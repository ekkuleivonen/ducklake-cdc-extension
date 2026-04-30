# SQL API

This is the public SQL surface for `ducklake_cdc`. The extension is still
early; prefer clear examples and small compatibility promises over elaborate
contracts.

## Conventions

- Every `cdc_*` function is also registered under a `ducklake_cdc_*` alias
  (for example `ducklake_cdc_window` is the same call as `cdc_window`). Pick
  whichever namespace style suits your codebase.
- All functions take an explicit `catalog` argument: the attached DuckLake
  catalog name, for example `'lake'` after `ATTACH ... AS lake`.
- Every entry below is a DuckDB table function and must be called from a
  `FROM` clause (for example `SELECT * FROM cdc_window('lake', 'demo')`).
  The only scalar function is `cdc_version()`. DuckDB table functions also
  do not accept subqueries as positional arguments — capture intermediates
  into a session variable, as the `cdc_commit` example below shows.
- Example result tables are illustrative; the column names and types are
  exact.

## Index

- Build: [`cdc_version`](#cdc_version)
- Lifecycle: [`cdc_consumer_create`](#cdc_consumer_create),
  [`cdc_consumer_reset`](#cdc_consumer_reset),
  [`cdc_consumer_drop`](#cdc_consumer_drop),
  [`cdc_consumer_heartbeat`](#cdc_consumer_heartbeat),
  [`cdc_consumer_force_release`](#cdc_consumer_force_release),
  [`cdc_consumer_list`](#cdc_consumer_list),
  [`cdc_consumer_subscriptions`](#cdc_consumer_subscriptions)
- Cursor primitives: [`cdc_window`](#cdc_window),
  [`cdc_commit`](#cdc_commit), [`cdc_wait`](#cdc_wait)
- Typed event sugars: [`cdc_changes`](#cdc_changes),
  [`cdc_ddl`](#cdc_ddl), [`cdc_events`](#cdc_events)
- Stateless helpers: [`cdc_recent_changes`](#cdc_recent_changes),
  [`cdc_recent_ddl`](#cdc_recent_ddl),
  [`cdc_schema_diff`](#cdc_schema_diff),
  [`cdc_range_events`](#cdc_range_events),
  [`cdc_range_ddl`](#cdc_range_ddl),
  [`cdc_range_changes`](#cdc_range_changes)
- Observability: [`cdc_consumer_stats`](#cdc_consumer_stats),
  [`cdc_audit_recent`](#cdc_audit_recent), [`cdc_doctor`](#cdc_doctor)

---

## Build

### `cdc_version`

```sql
SELECT cdc_version();
```

Build stamp for the loaded extension. The value comes from
`git tag --points-at HEAD` when releasing, or the short SHA on untagged
builds.

**Use cases:** confirm the loaded extension build, embed the build stamp
in support tickets, bench result files, or CI output.

**Returns:** scalar `VARCHAR`.

```text
┌──────────────────────┐
│    cdc_version()     │
├──────────────────────┤
│ ducklake_cdc v0.2.0  │
└──────────────────────┘
```

---

## Consumer lifecycle

### `cdc_consumer_create`

```sql
SELECT * FROM cdc_consumer_create(
    catalog,
    name,
    start_at              := 'now',   -- 'now' | 'beginning' | 'oldest' | snapshot id
    subscriptions         := [
      {
        'scope_kind': 'table',          -- catalog | schema | table
        'schema_name': 'main',          -- name sugar, resolved at start_at
        'table_name': 'orders',
        'event_category': 'dml',        -- * | dml | ddl
        'change_type': '*'              -- * | insert | update_preimage | update_postimage | delete
      },
      {
        'scope_kind': 'schema',
        'schema_id': 12,                -- identity input, validated at start_at
        'event_category': 'ddl',
        'change_type': '*'
      }
    ],
    stop_at_schema_change := TRUE     -- BOOLEAN; stop the cursor on schema boundaries
);
```

Creates a named consumer cursor and persists it in the metadata-catalog
`__ducklake_cdc_consumers` state table. The consumer's routing rules are
stored as normalized rows in `__ducklake_cdc_consumer_subscriptions`.

Subscriptions are identity-first. Qualified names are accepted only as
creation-time sugar and are resolved at `start_at` to DuckLake object ids.
A table subscription follows the resolved `table_id` across table renames;
a schema subscription follows the resolved `schema_id` across schema
renames. Drop + recreate with the same name is a new identity and is not
matched by the old subscription.

**Use cases:** register a durable cursor before reading any windows;
configure per-consumer subscriptions once; return resolved ids that an
application can persist in its own control plane.

**Returns:**
`consumer_name VARCHAR, consumer_id BIGINT, last_committed_snapshot BIGINT,
subscription_id BIGINT, scope_kind VARCHAR, schema_id BIGINT,
table_id BIGINT, event_category VARCHAR, change_type VARCHAR,
original_qualified_name VARCHAR, current_qualified_name VARCHAR`.

```text
┌───────────────┬─────────────┬─────────────────────────┬─────────────────┬────────────┬───────────┬──────────┬────────────────┬─────────────┬─────────────────────────┬────────────────────────┐
│ consumer_name │ consumer_id │ last_committed_snapshot │ subscription_id │ scope_kind │ schema_id │ table_id │ event_category │ change_type │ original_qualified_name │ current_qualified_name │
├───────────────┼─────────────┼─────────────────────────┼─────────────────┼────────────┼───────────┼──────────┼────────────────┼─────────────┼─────────────────────────┼────────────────────────┤
│ orders_sink   │           1 │                       7 │               1 │ table      │         0 │        5 │ dml            │ insert      │ main.orders             │ main.orders            │
│ orders_sink   │           1 │                       7 │               2 │ table      │         0 │        5 │ dml            │ delete      │ main.orders             │ main.orders            │
└───────────────┴─────────────┴─────────────────────────┴─────────────────┴────────────┴───────────┴──────────┴────────────────┴─────────────┴─────────────────────────┴────────────────────────┘
```

`subscriptions` is required. Use explicit `'*'` wildcards rather than
`NULL`: `event_category := '*'` means both DML and DDL, and
`change_type := '*'` means every DML change type. `NULL` is not a
wildcard; it is accepted only where a field is not applicable to the
chosen `scope_kind` (for example `table_id` on a schema subscription).
Wildcard entries may expand into multiple persisted subscription rows.

Common subscription shapes:

```text
catalog + ddl + *             -- all DDL in the catalog
schema  + dml + insert        -- inserts into any table in one schema identity
schema  + dml + *             -- all DML for all tables in one schema identity
table   + dml + *             -- all DML for one table identity
table   + *   + *             -- DML and DDL for one table identity
```

### `cdc_consumer_reset`

```sql
SELECT * FROM cdc_consumer_reset(catalog, name, to_snapshot := NULL);
```

Repositions the cursor and clears the lease. `to_snapshot := NULL` resets
to the current head; pass an explicit snapshot id to replay from an older
point or fast-forward past already-handled work.

**Use cases:** re-sync a downstream sink after a corruption rebuild; pin
a consumer to a known good snapshot during incident recovery; create a
separate backfill consumer that replays `[to_snapshot, current_head]`
while the live consumer keeps serving current time.

**Returns:** `consumer_name VARCHAR, consumer_id BIGINT, previous_snapshot BIGINT, new_snapshot BIGINT`.

```text
┌───────────────┬─────────────┬───────────────────┬──────────────┐
│ consumer_name │ consumer_id │ previous_snapshot │ new_snapshot │
├───────────────┼─────────────┼───────────────────┼──────────────┤
│ orders_sink   │           1 │                42 │            7 │
└───────────────┴─────────────┴───────────────────┴──────────────┘
```

### `cdc_consumer_drop`

```sql
SELECT * FROM cdc_consumer_drop(catalog, name);
```

Deletes the consumer row (and its lease) from the metadata-catalog
`__ducklake_cdc_consumers` state table and writes a `consumer_drop` audit
entry.

**Use cases:** retire a consumer that no longer has a downstream sink.

**Returns:** `consumer_name VARCHAR, consumer_id BIGINT, last_committed_snapshot BIGINT`
(the values observed immediately before the row was deleted).

```text
┌───────────────┬─────────────┬─────────────────────────┐
│ consumer_name │ consumer_id │ last_committed_snapshot │
├───────────────┼─────────────┼─────────────────────────┤
│ orders_sink   │           1 │                      42 │
└───────────────┴─────────────┴─────────────────────────┘
```

### `cdc_consumer_heartbeat`

```sql
SELECT * FROM cdc_consumer_heartbeat(catalog, name);
```

Extends the current holder's owner-token lease without reading more data.

**Use cases:** keep a long-running reader's lease alive between idle
windows; pair with `cdc_window` loops that may sit on `cdc_wait` for a
while.

**Returns:** `heartbeat_extended BOOLEAN`. `false` means the caller did
not hold the lease at the moment of the call.

```text
┌────────────────────┐
│ heartbeat_extended │
├────────────────────┤
│ true               │
└────────────────────┘
```

### `cdc_consumer_force_release`

```sql
SELECT * FROM cdc_consumer_force_release(catalog, name);
```

Operator escape hatch: clears the lease without checking the owner token,
and writes a `consumer_force_release` audit entry.

**Use cases:** reclaim a consumer after the previous holder died hard
(killed process, network partition) and you don't want to wait for the
lease to time out.

**Returns:** `consumer_name VARCHAR, consumer_id BIGINT, previous_token VARCHAR`
(the token that was forcibly cleared, or `NULL` if no holder existed).

```text
┌───────────────┬─────────────┬──────────────────────────────────────┐
│ consumer_name │ consumer_id │ previous_token                       │
├───────────────┼─────────────┼──────────────────────────────────────┤
│ orders_sink   │           1 │ 8f3a3c2e-…-b2c1                      │
└───────────────┴─────────────┴──────────────────────────────────────┘
```

### `cdc_consumer_list`

```sql
SELECT * FROM cdc_consumer_list(catalog);
```

Enumerates every registered consumer, ordered by `consumer_id`.

**Use cases:** drive admin/inspector UIs; audit which filters and lease
state each consumer is configured with.

**Returns:**
`consumer_name VARCHAR, consumer_id BIGINT, subscription_count BIGINT,
subscriptions_active BIGINT, subscriptions_renamed BIGINT,
subscriptions_dropped BIGINT, stop_at_schema_change BOOLEAN,
last_committed_snapshot BIGINT, last_committed_schema_version BIGINT,
owner_token UUID, owner_acquired_at TIMESTAMPTZ,
owner_heartbeat_at TIMESTAMPTZ, lease_interval_seconds INTEGER,
created_at TIMESTAMPTZ, created_by VARCHAR, updated_at TIMESTAMPTZ,
metadata VARCHAR`.

```text
┌───────────────┬─────────────┬────────────────────┬──────────────────────┬───────────────────────┬───────────────────────┬───────────────────────┬─────────────────────────┬───────────────────────────────┬───────────────────────┬─────────────────────────┬─────────────────────────┬────────────────────────┬─────────────────────────┬────────────┬─────────────────────────┬──────────┐
│ consumer_name │ consumer_id │ subscription_count │ subscriptions_active │ subscriptions_renamed │ subscriptions_dropped │ stop_at_schema_change │ last_committed_snapshot │ last_committed_schema_version │ owner_token           │ owner_acquired_at       │ owner_heartbeat_at      │ lease_interval_seconds │ created_at              │ created_by │ updated_at              │ metadata │
├───────────────┼─────────────┼────────────────────┼──────────────────────┼───────────────────────┼───────────────────────┼───────────────────────┼─────────────────────────┼───────────────────────────────┼───────────────────────┼─────────────────────────┼─────────────────────────┼────────────────────────┼─────────────────────────┼────────────┼─────────────────────────┼──────────┤
│ orders_sink   │           1 │                  2 │                    2 │                     0 │                     0 │ true                  │                      42 │                             3 │ 8f3a3c2e-…-b2c1       │ 2026-04-30 09:30:51+00  │ 2026-04-30 09:31:08+00  │                     30 │ 2026-04-30 08:00:00+00  │ ekku       │ 2026-04-30 09:31:08+00  │ NULL     │
└───────────────┴─────────────┴────────────────────┴──────────────────────┴───────────────────────┴───────────────────────┴───────────────────────┴─────────────────────────┴───────────────────────────────┴───────────────────────┴─────────────────────────┴─────────────────────────┴────────────────────────┴─────────────────────────┴────────────┴─────────────────────────┴──────────┘
```

### `cdc_consumer_subscriptions`

```sql
SELECT * FROM cdc_consumer_subscriptions(catalog, name := NULL);
```

Enumerates normalized subscription rows. This is the durable inspection
surface for routing intent; applications that persist their own
subscription control plane should store `schema_id` / `table_id` from
this output, together with the catalog identity.

**Use cases:** inspect resolved subscription identities; detect table or
schema renames; reconcile an application's stored human-readable filters
with the current DuckLake names.

**Returns:**
`consumer_name VARCHAR, consumer_id BIGINT, subscription_id BIGINT,
scope_kind VARCHAR, schema_id BIGINT, table_id BIGINT,
event_category VARCHAR, change_type VARCHAR,
original_qualified_name VARCHAR, current_qualified_name VARCHAR,
status VARCHAR`.

```text
┌───────────────┬─────────────┬─────────────────┬────────────┬───────────┬──────────┬────────────────┬─────────────┬─────────────────────────┬────────────────────────┬─────────┐
│ consumer_name │ consumer_id │ subscription_id │ scope_kind │ schema_id │ table_id │ event_category │ change_type │ original_qualified_name │ current_qualified_name │ status  │
├───────────────┼─────────────┼─────────────────┼────────────┼───────────┼──────────┼────────────────┼─────────────┼─────────────────────────┼────────────────────────┼─────────┤
│ orders_sink   │           1 │               1 │ table      │         0 │        5 │ dml            │ insert      │ main.orders             │ main.orders_v2         │ renamed │
│ schema_watch  │           2 │               3 │ schema     │        12 │     NULL │ ddl            │ *           │ sales                   │ sales_archive          │ renamed │
└───────────────┴─────────────┴─────────────────┴────────────┴───────────┴──────────┴────────────────┴─────────────┴─────────────────────────┴────────────────────────┴─────────┘
```

---

## Cursor primitives

### `cdc_window`

```sql
SELECT * FROM cdc_window(catalog, name, max_snapshots := 100);
```

Acquires or refreshes the consumer lease and returns the current read
window. Calling `cdc_window` repeatedly from the same connection returns
the same window until `cdc_commit` advances the cursor. A different
connection trying to read the same consumer receives `CDC_BUSY`.

**Use cases:** the entry point of every cursor loop — discover the
snapshot range the consumer should now process, and refresh the lease in
the same call.

**Returns:** `start_snapshot BIGINT, end_snapshot BIGINT, has_changes BOOLEAN, schema_version BIGINT, schema_changes_pending BOOLEAN`.

```text
┌────────────────┬──────────────┬─────────────┬────────────────┬────────────────────────┐
│ start_snapshot │ end_snapshot │ has_changes │ schema_version │ schema_changes_pending │
├────────────────┼──────────────┼─────────────┼────────────────┼────────────────────────┤
│              7 │           42 │ true        │              3 │ false                  │
└────────────────┴──────────────┴─────────────┴────────────────┴────────────────────────┘
```

### `cdc_commit`

```sql
SET VARIABLE end_snapshot = (
  SELECT end_snapshot FROM cdc_window('lake', 'demo')
);
SELECT * FROM cdc_commit('lake', 'demo', getvariable('end_snapshot'));
```

Advances the cursor after downstream work succeeds. The caller must
still hold the owner-token lease; otherwise the call raises `CDC_BUSY`.
Subqueries cannot be passed directly as positional arguments to a table
function, so capture `end_snapshot` into a session variable first.

**Use cases:** the trailing edge of every cursor loop — once the sink
has durably absorbed `[start_snapshot, snapshot_id]`, advance the cursor
so the next `cdc_window` call moves forward.

**Returns:** `consumer_name VARCHAR, committed_snapshot BIGINT, schema_version BIGINT`.

```text
┌───────────────┬────────────────────┬────────────────┐
│ consumer_name │ committed_snapshot │ schema_version │
├───────────────┼────────────────────┼────────────────┤
│ orders_sink   │                 42 │              3 │
└───────────────┴────────────────────┴────────────────┘
```

### `cdc_wait`

```sql
SELECT * FROM cdc_wait(catalog, name, timeout_ms := 30000);
```

Long-polls until a new external DuckLake snapshot exists after the
consumer's cursor, or returns one row with `snapshot_id := NULL` on
timeout. `cdc_wait` holds the DuckDB connection for the duration of the
call, so use a dedicated connection. Always read it from a `FROM`
clause, never as a scalar.

**Use cases:** streaming consumers that want to block until new data is
available rather than spin on `cdc_window`.

**Returns:** `snapshot_id BIGINT`.

```text
┌─────────────┐
│ snapshot_id │
├─────────────┤
│          43 │
└─────────────┘
```

---

## Typed event sugars (within the current consumer window)

These sugars internally call `cdc_window` and then project events out of
the resulting `[start_snapshot, end_snapshot]` range. They apply the
consumer's normalized subscriptions by object identity, not by current
qualified name.

### `cdc_changes`

```sql
SELECT * FROM cdc_changes(
  catalog,
  name,
  table_id      := NULL, -- preferred identity form
  table_name    := NULL, -- name sugar, resolved inside the current window
  max_snapshots := 100
);
```

Typed, row-level DML for one subscribed table inside the current window.
Pass `table_id` when the caller has persisted the DuckLake identity.
Pass `table_name` for human-facing SQL; the name is resolved to a
`table_id` and then checked against the consumer's subscription rows. For
a consumer with exactly one active table-scope DML subscription, both
`table_id` and `table_name` may be omitted.

The middle of the column list mirrors DuckLake's `table_changes` output
for the resolved table; the ends add snapshot and commit metadata.

**Use cases:** the typical "give me the rows" call in a sink loop — e.g.
mirroring `lake.orders` into a downstream warehouse table.

**Returns (variable shape):**
`snapshot_id BIGINT, rowid BIGINT, change_type VARCHAR,
<user columns from the table…>, snapshot_time TIMESTAMPTZ,
author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR,
next_snapshot_id BIGINT`.

```text
┌─────────────┬───────┬─────────────┬────┬────────┬─────────────────────────┬────────┬────────────────┬───────────────────┬──────────────────┐
│ snapshot_id │ rowid │ change_type │ id │ status │ snapshot_time           │ author │ commit_message │ commit_extra_info │ next_snapshot_id │
├─────────────┼───────┼─────────────┼────┼────────┼─────────────────────────┼────────┼────────────────┼───────────────────┼──────────────────┤
│          42 │     0 │ insert      │  2 │ paid   │ 2026-04-30 09:30:51+00  │ NULL   │ NULL           │ NULL              │               43 │
│          42 │     1 │ insert      │  3 │ new    │ 2026-04-30 09:30:51+00  │ NULL   │ NULL           │ NULL              │               43 │
└─────────────┴───────┴─────────────┴────┴────────┴─────────────────────────┴────────┴────────────────┴───────────────────┴──────────────────┘
```

### `cdc_ddl`

```sql
SELECT * FROM cdc_ddl(catalog, name, max_snapshots := 100);
```

Typed schema events inside the current consumer window. Consumers
should apply DDL before DML for the same snapshot. Rows are filtered by
the consumer's DDL subscription rows. Table and schema renames preserve
`schema_id` / `object_id`; the current names in `schema_name` and
`object_name` are the names as of the event snapshot.

**Use cases:** drive a schema-aware sink (apply column adds before
re-reading rows); power schema-only watchers configured with
DDL subscription rows.

**Returns:**
`snapshot_id BIGINT, snapshot_time TIMESTAMPTZ, event_kind VARCHAR
(created | altered | dropped), object_kind VARCHAR (schema | table | view),
schema_id BIGINT, schema_name VARCHAR, object_id BIGINT,
object_name VARCHAR, details VARCHAR (JSON text)`.

```text
┌─────────────┬─────────────────────────┬────────────┬─────────────┬───────────┬─────────────┬───────────┬─────────────┬───────────────────────────────┐
│ snapshot_id │ snapshot_time           │ event_kind │ object_kind │ schema_id │ schema_name │ object_id │ object_name │ details                       │
├─────────────┼─────────────────────────┼────────────┼─────────────┼───────────┼─────────────┼───────────┼─────────────┼───────────────────────────────┤
│          11 │ 2026-04-30 09:25:00+00  │ created    │ table       │         0 │ main        │         5 │ orders      │ {"columns":["id","status"]}   │
│          17 │ 2026-04-30 09:27:14+00  │ altered    │ table       │         0 │ main        │         5 │ orders      │ {"add_column":"customer_id"}  │
└─────────────┴─────────────────────────┴────────────┴─────────────┴───────────┴─────────────┴───────────┴─────────────┴───────────────────────────────┘
```

### `cdc_events`

```sql
SELECT * FROM cdc_events(catalog, name, max_snapshots := 100);
```

Raw snapshot-level event stream for the current consumer window — one
row per snapshot, not per data row. The stream is filtered by the
consumer's subscriptions: a snapshot is returned when its DDL/DML work
touches at least one subscribed identity and event/change type. For typed
DML rows, use `cdc_changes`; for typed DDL rows, use `cdc_ddl`.

**Use cases:** outbox-style dispatch on `commit_extra_info`; lake
dashboards that want one row per snapshot with author and message.
The extension exposes `commit_extra_info` unchanged; producer teams and
clients own any JSON envelope, versioning, or routing convention they put
inside it.

**Returns:**
`snapshot_id BIGINT, snapshot_time TIMESTAMPTZ, changes_made VARCHAR,
author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR,
next_snapshot_id BIGINT, schema_version BIGINT,
schema_changes_pending BOOLEAN`.

```text
┌─────────────┬─────────────────────────┬────────────────────────────┬────────┬────────────────┬─────────────────────┬──────────────────┬────────────────┬────────────────────────┐
│ snapshot_id │ snapshot_time           │ changes_made               │ author │ commit_message │ commit_extra_info   │ next_snapshot_id │ schema_version │ schema_changes_pending │
├─────────────┼─────────────────────────┼────────────────────────────┼────────┼────────────────┼─────────────────────┼──────────────────┼────────────────┼────────────────────────┤
│          42 │ 2026-04-30 09:30:51+00  │ inserted_into_table:orders │ NULL   │ NULL           │ {"outbox":"orders"} │               43 │              3 │ false                  │
│          43 │ 2026-04-30 09:31:08+00  │ inserted_into_table:orders │ NULL   │ NULL           │ NULL                │             NULL │              3 │ false                  │
└─────────────┴─────────────────────────┴────────────────────────────┴────────┴────────────────┴─────────────────────┴──────────────────┴────────────────┴────────────────────────┘
```

---

## Stateless helpers

These do not create or advance a durable consumer. Use them for
exploration, debugging, screenshots, and one-off checks.

### `cdc_recent_changes`

```sql
SELECT * FROM cdc_recent_changes(catalog, table_name, since_seconds := 300);
```

The same row shape as `cdc_changes` (minus `next_snapshot_id`), but
sourced from the recent past with no consumer required.

**Use cases:** ad-hoc "what did this table do recently" queries; demo
screenshots; sanity-check a producer without standing up a consumer.

**Returns (variable shape):**
`snapshot_id BIGINT, rowid BIGINT, change_type VARCHAR,
<user columns from the table…>, snapshot_time TIMESTAMPTZ,
author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR`.

```text
┌─────────────┬───────┬─────────────┬────┬────────┬─────────────────────────┬────────┬────────────────┬───────────────────┐
│ snapshot_id │ rowid │ change_type │ id │ status │ snapshot_time           │ author │ commit_message │ commit_extra_info │
├─────────────┼───────┼─────────────┼────┼────────┼─────────────────────────┼────────┼────────────────┼───────────────────┤
│          42 │     0 │ insert      │  2 │ paid   │ 2026-04-30 09:30:51+00  │ NULL   │ NULL           │ NULL              │
└─────────────┴───────┴─────────────┴────┴────────┴─────────────────────────┴────────┴────────────────┴───────────────────┘
```

### `cdc_recent_ddl`

```sql
SELECT * FROM cdc_recent_ddl(catalog, since_seconds := 86400, for_table := NULL);
```

Stateless flavour of `cdc_ddl`, with the same row shape. Optionally
filter to a single fully-qualified table via `for_table := 'main.orders'`.

**Use cases:** quick "did anything just change?" schema audit; producer
debugging without registering a consumer.

**Returns:**
`snapshot_id BIGINT, snapshot_time TIMESTAMPTZ, event_kind VARCHAR,
object_kind VARCHAR, schema_id BIGINT, schema_name VARCHAR,
object_id BIGINT, object_name VARCHAR, details VARCHAR`.

```text
┌─────────────┬─────────────────────────┬────────────┬─────────────┬───────────┬─────────────┬───────────┬─────────────┬───────────────────────────────┐
│ snapshot_id │ snapshot_time           │ event_kind │ object_kind │ schema_id │ schema_name │ object_id │ object_name │ details                       │
├─────────────┼─────────────────────────┼────────────┼─────────────┼───────────┼─────────────┼───────────┼─────────────┼───────────────────────────────┤
│          17 │ 2026-04-30 09:27:14+00  │ altered    │ table       │         0 │ main        │         5 │ orders      │ {"add_column":"customer_id"}  │
└─────────────┴─────────────────────────┴────────────┴─────────────┴───────────┴─────────────┴───────────┴─────────────┴───────────────────────────────┘
```

### `cdc_schema_diff`

```sql
SELECT * FROM cdc_schema_diff(catalog, table_name, from_snapshot, to_snapshot);
```

Diffs one table's column shape between two snapshots
(`0 <= from_snapshot <= to_snapshot`). Renames and re-typings appear as
explicit rows, not as a drop+add pair.

**Use cases:** build a migration plan for downstream sinks before
applying a schema-boundary window; produce a human-readable changelog
between two known-good snapshots.

**Returns:**
`snapshot_id BIGINT, snapshot_time TIMESTAMPTZ, change_kind VARCHAR,
column_id BIGINT, old_name VARCHAR, new_name VARCHAR,
old_type VARCHAR, new_type VARCHAR, old_default VARCHAR,
new_default VARCHAR, old_nullable BOOLEAN, new_nullable BOOLEAN,
parent_column_id BIGINT`.

```text
┌─────────────┬─────────────────────────┬──────────────┬───────────┬──────────┬─────────────┬──────────┬──────────┬─────────────┬─────────────┬──────────────┬──────────────┬──────────────────┐
│ snapshot_id │ snapshot_time           │ change_kind  │ column_id │ old_name │ new_name    │ old_type │ new_type │ old_default │ new_default │ old_nullable │ new_nullable │ parent_column_id │
├─────────────┼─────────────────────────┼──────────────┼───────────┼──────────┼─────────────┼──────────┼──────────┼─────────────┼─────────────┼──────────────┼──────────────┼──────────────────┤
│          17 │ 2026-04-30 09:27:14+00  │ column_added │         3 │ NULL     │ customer_id │ NULL     │ INTEGER  │ NULL        │ NULL        │ NULL         │ true         │             NULL │
└─────────────┴─────────────────────────┴──────────────┴───────────┴──────────┴─────────────┴──────────┴──────────┴─────────────┴─────────────┴──────────────┴──────────────┴──────────────────┘
```

### `cdc_range_events`

```sql
SELECT * FROM cdc_range_events(
  catalog,
  from_snapshot,
  to_snapshot := NULL
);
```

Stateless snapshot-level events over an explicit snapshot range.
`to_snapshot := NULL` means the current catalog head. This does not create
or advance a consumer and does not apply subscription filters.

**Use cases:** export or inspect lake activity between two snapshots;
debug what a future consumer would have seen; build bounded backfill
plans without touching the live consumer cursor.

**Returns:** the same row shape as `cdc_events`.

### `cdc_range_ddl`

```sql
SELECT * FROM cdc_range_ddl(
  catalog,
  from_snapshot,
  to_snapshot := NULL
);
```

Stateless DDL events over an explicit snapshot range. `to_snapshot :=
NULL` means the current catalog head. This does not create or advance a
consumer and does not apply subscription filters.

**Use cases:** audit schema evolution across a bounded range; prepare a
schema migration plan before replaying data into a downstream sink.

**Returns:** the same row shape as `cdc_ddl`.

### `cdc_range_changes`

```sql
SELECT * FROM cdc_range_changes(
  catalog,
  from_snapshot,
  to_snapshot   := NULL,
  table_id      := NULL, -- preferred identity form
  table_name    := NULL  -- name sugar, resolved for the requested range
);
```

Stateless, row-level DML for one table over an explicit snapshot range.
`to_snapshot := NULL` means the current catalog head. This does not create
or advance a consumer and does not apply subscription filters.

Snapshot ids are the canonical range boundary. Timestamp sugar may be
added later, but must define whether a timestamp maps to the first
snapshot after the timestamp or the latest snapshot at or before it.

**Use cases:** bounded backfills, one-off exports, sink dry-runs, and
debugging replay plans while a separate live consumer continues to run.

**Returns (variable shape):**
`snapshot_id BIGINT, rowid BIGINT, change_type VARCHAR,
<user columns from the table…>, snapshot_time TIMESTAMPTZ,
author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR`.

---

## Observability

### `cdc_consumer_stats`

```sql
SELECT * FROM cdc_consumer_stats(catalog, consumer := NULL);
```

One row per consumer (filtered by `consumer :=` if provided). Reports
cursor lag, gap risk versus the oldest available snapshot, subscription
health, and lease state.

**Use cases:** dashboards and alerts — `lag_snapshots`, `lag_seconds`,
`gap_distance`, `lease_alive`, `subscriptions_dropped`, and
`subscriptions_renamed` are the columns most operators wire into
monitors.

**Returns:**
`consumer_name VARCHAR, consumer_id BIGINT,
last_committed_snapshot BIGINT, current_snapshot BIGINT,
lag_snapshots BIGINT, lag_seconds DOUBLE,
oldest_available_snapshot BIGINT, gap_distance BIGINT,
subscription_count BIGINT, subscriptions_active BIGINT,
subscriptions_renamed BIGINT, subscriptions_dropped BIGINT,
owner_token UUID, owner_acquired_at TIMESTAMPTZ,
owner_heartbeat_at TIMESTAMPTZ, lease_interval_seconds INTEGER,
lease_alive BOOLEAN`.

```text
┌───────────────┬─────────────┬─────────────────────────┬──────────────────┬───────────────┬─────────────┬───────────────────────────┬──────────────┬────────────────────┬──────────────────────┬───────────────────────┬───────────────────────┬───────────────────┬─────────────────────────┬─────────────────────────┬────────────────────────┬─────────────┐
│ consumer_name │ consumer_id │ last_committed_snapshot │ current_snapshot │ lag_snapshots │ lag_seconds │ oldest_available_snapshot │ gap_distance │ subscription_count │ subscriptions_active │ subscriptions_renamed │ subscriptions_dropped │ owner_token       │ owner_acquired_at       │ owner_heartbeat_at      │ lease_interval_seconds │ lease_alive │
├───────────────┼─────────────┼─────────────────────────┼──────────────────┼───────────────┼─────────────┼───────────────────────────┼──────────────┼────────────────────┼──────────────────────┼───────────────────────┼───────────────────────┼───────────────────┼─────────────────────────┼─────────────────────────┼────────────────────────┼─────────────┤
│ orders_sink   │           1 │                      42 │               43 │             1 │       12.4  │                         5 │           37 │                  2 │                    2 │                     0 │                     0 │ 8f3a3c2e-…-b2c1   │ 2026-04-30 09:30:51+00  │ 2026-04-30 09:31:08+00  │                     30 │ true        │
└───────────────┴─────────────┴─────────────────────────┴──────────────────┴───────────────┴─────────────┴───────────────────────────┴──────────────┴────────────────────┴──────────────────────┴───────────────────────┴───────────────────────┴───────────────────┴─────────────────────────┴─────────────────────────┴────────────────────────┴─────────────┘
```

### `cdc_audit_recent`

```sql
SELECT * FROM cdc_audit_recent(catalog, since_seconds := 86400, consumer := NULL);
```

Recent lifecycle events from the metadata-catalog `__ducklake_cdc_audit`
state table — consumer create, reset, drop, force release, lease
force-acquire, and so on.

**Use cases:** post-incident forensic timeline; audit trail for who/what
touched a consumer in the last day.

**Returns:**
`ts TIMESTAMPTZ, audit_id BIGINT, actor VARCHAR, action VARCHAR,
consumer_name VARCHAR, consumer_id BIGINT, details VARCHAR`.

```text
┌─────────────────────────┬──────────┬────────┬─────────┬───────────────┬─────────────┬──────────────────────┐
│ ts                      │ audit_id │ actor  │ action  │ consumer_name │ consumer_id │ details              │
├─────────────────────────┼──────────┼────────┼─────────┼───────────────┼─────────────┼──────────────────────┤
│ 2026-04-30 08:00:00+00  │        1 │ ekku   │ create  │ orders_sink   │           1 │ {"start_at":"now"}   │
│ 2026-04-30 09:31:08+00  │        2 │ ekku   │ commit  │ orders_sink   │           1 │ {"snapshot":42}      │
└─────────────────────────┴──────────┴────────┴─────────┴───────────────┴─────────────┴──────────────────────┘
```

### `cdc_doctor`

```sql
SELECT * FROM cdc_doctor(catalog, consumer := NULL);
```

Read-only operational diagnostics for a catalog, optionally scoped to one
consumer. `cdc_doctor` packages the checks that operators would otherwise
assemble manually from `cdc_consumer_list`, `cdc_consumer_subscriptions`,
`cdc_consumer_stats`, and `cdc_audit_recent`.

**Use cases:** preflight a Python client or long-running consumer; power a
CLI health check; give support issues a compact snapshot of CDC state.

**Returns:**
`severity VARCHAR, code VARCHAR, consumer_name VARCHAR, message VARCHAR,
details VARCHAR`.

```text
┌──────────┬─────────────────────────┬───────────────┬────────────────────────────────────┬────────────────────────────┐
│ severity │ code                    │ consumer_name │ message                            │ details                    │
├──────────┼─────────────────────────┼───────────────┼────────────────────────────────────┼────────────────────────────┤
│ info     │ CDC_METADATA_OK         │ NULL          │ CDC metadata tables are present    │ NULL                       │
│ warning  │ CDC_CONSUMER_LAG        │ orders_sink   │ Consumer is 120 snapshots behind   │ {"lag_snapshots":120}      │
│ error    │ CDC_SUBSCRIPTION_DROPPED│ orders_sink   │ A subscription target was dropped  │ {"subscription_id":3}      │
└──────────┴─────────────────────────┴───────────────┴────────────────────────────────────┴────────────────────────────┘
```

Initial checks should cover catalog compatibility, CDC metadata presence,
gap risk, stale/live leases, dropped or renamed subscriptions, pending
schema boundaries, suspicious lag, and recent force-release, reset, or
drop audit events.

---

## Filter rules

- Subscriptions are normalized rows, not JSON/list filters on the consumer
  row.
- `scope_kind` is `catalog`, `schema`, or `table`.
- `event_category` is `dml`, `ddl`, or explicit wildcard `'*'`.
- `change_type` is `insert`, `update_preimage`, `update_postimage`,
  `delete`, or explicit wildcard `'*'`.
- `change_type` applies only to DML. For DDL subscriptions it must be
  `'*'`.
- `NULL` is not a wildcard. Wildcards must be written as `'*'`.
- Names are resolved at `start_at`; persisted matching uses
  `schema_id` / `table_id`.
- Schema subscriptions match all current and future tables belonging to
  the same `schema_id`. Table subscriptions match the same `table_id`
  across renames.
- Drop + recreate with the same name creates a new identity and is not
  matched by an older subscription unless the caller creates a new
  subscription.

## Ordering

- Snapshot order is ascending by `snapshot_id`.
- DDL should be applied before DML for the same snapshot.
- DML rows preserve DuckLake's `table_changes` row identity and
  `change_type`.

## Replay and Backfill

There are two replay modes:

- Durable replay uses a named consumer. Reset or create a consumer at the
  desired start snapshot, process normal windows, and commit only after
  downstream work succeeds. A live consumer can keep running while a
  separate backfill consumer replays an older range.
- Stateless range reads use `cdc_range_events`, `cdc_range_ddl`, and
  `cdc_range_changes`. They are for inspection, export, dry-runs, and
  bounded backfill planning. They do not acquire a lease and do not move a
  cursor.

The extension provides at-least-once replay mechanics, not exactly-once
sink semantics. Clients and sinks own idempotency, retries, validation,
quarantine/dead-letter policy, and any decision to skip or reprocess
external side effects.

## Storage tables

The extension creates these metadata-catalog state tables on first use:

- `__ducklake_cdc_consumers`
- `__ducklake_cdc_consumer_subscriptions`
- `__ducklake_cdc_audit`

`__ducklake_cdc_consumers` stores cursor, lease, and consumer-level
configuration. `__ducklake_cdc_consumer_subscriptions` stores one row per
resolved routing rule, keyed by `consumer_id` plus `subscription_id` and
DuckLake object identities. Human-readable original names are retained for
audit and reconciliation, but they are not the source of truth for
matching.

DuckDB and PostgreSQL metadata catalogs place them under
`__ducklake_metadata_<catalog>.__ducklake_cdc`. SQLite metadata catalogs do
not support schemas, so the same table names live in
`__ducklake_metadata_<catalog>.main`.

## Related docs

- [Design notes](./design.md)
- [Errors](./errors.md)
- [`cdc_wait`](#cdc_wait)
- [`cdc_consumer_stats`](#cdc_consumer_stats)
- [Hazard log](./hazard-log.md)
