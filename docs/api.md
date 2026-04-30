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
  [`cdc_consumer_list`](#cdc_consumer_list)
- Cursor primitives: [`cdc_window`](#cdc_window),
  [`cdc_commit`](#cdc_commit), [`cdc_wait`](#cdc_wait)
- Typed event sugars: [`cdc_changes`](#cdc_changes),
  [`cdc_ddl`](#cdc_ddl), [`cdc_events`](#cdc_events)
- Stateless helpers: [`cdc_recent_changes`](#cdc_recent_changes),
  [`cdc_recent_ddl`](#cdc_recent_ddl),
  [`cdc_schema_diff`](#cdc_schema_diff)
- Observability: [`cdc_consumer_stats`](#cdc_consumer_stats),
  [`cdc_audit_recent`](#cdc_audit_recent)

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
    tables                := NULL,    -- NULL or VARCHAR[] of 'schema.table'
    change_types          := NULL,    -- NULL or subset of insert | update_preimage | update_postimage | delete
    event_categories      := NULL,    -- NULL or subset of ddl | dml
    stop_at_schema_change := TRUE     -- BOOLEAN; stop the cursor on schema boundaries
);
```

Creates a named consumer cursor and persists it in the metadata-catalog
`__ducklake_cdc_consumers` state table.

**Use cases:** register a durable cursor before reading any windows;
configure per-consumer filters once.

**Returns:** `consumer_name VARCHAR, consumer_id BIGINT, last_committed_snapshot BIGINT`.

```text
┌───────────────┬─────────────┬─────────────────────────┐
│ consumer_name │ consumer_id │ last_committed_snapshot │
├───────────────┼─────────────┼─────────────────────────┤
│ orders_sink   │           1 │                       7 │
└───────────────┴─────────────┴─────────────────────────┘
```

### `cdc_consumer_reset`

```sql
SELECT * FROM cdc_consumer_reset(catalog, name, to_snapshot := NULL);
```

Repositions the cursor and clears the lease. `to_snapshot := NULL` resets
to the current head; pass an explicit snapshot id to rewind or fast-forward.

**Use cases:** re-sync a downstream sink after a corruption rebuild; pin
a consumer to a known good snapshot during incident recovery.

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
`consumer_name VARCHAR, consumer_id BIGINT, tables VARCHAR[],
change_types VARCHAR[], event_categories VARCHAR[],
stop_at_schema_change BOOLEAN, dml_blocked_by_failed_ddl BOOLEAN,
last_committed_snapshot BIGINT, last_committed_schema_version BIGINT,
owner_token UUID, owner_acquired_at TIMESTAMPTZ,
owner_heartbeat_at TIMESTAMPTZ, lease_interval_seconds INTEGER,
created_at TIMESTAMPTZ, created_by VARCHAR, updated_at TIMESTAMPTZ,
metadata VARCHAR`.

```text
┌───────────────┬─────────────┬───────────────┬──────────────┬──────────────────┬────────────────────────┬──────────────────────────┬─────────────────────────┬───────────────────────────────┬───────────────────────┬─────────────────────────┬─────────────────────────┬────────────────────────┬─────────────────────────┬────────────┬─────────────────────────┬──────────┐
│ consumer_name │ consumer_id │ tables        │ change_types │ event_categories │ stop_at_schema_change  │ dml_blocked_by_failed_ddl│ last_committed_snapshot │ last_committed_schema_version │ owner_token           │ owner_acquired_at       │ owner_heartbeat_at      │ lease_interval_seconds │ created_at              │ created_by │ updated_at              │ metadata │
├───────────────┼─────────────┼───────────────┼──────────────┼──────────────────┼────────────────────────┼──────────────────────────┼─────────────────────────┼───────────────────────────────┼───────────────────────┼─────────────────────────┼─────────────────────────┼────────────────────────┼─────────────────────────┼────────────┼─────────────────────────┼──────────┤
│ orders_sink   │           1 │ [main.orders] │ [insert]     │ [dml]            │ true                   │ false                    │                      42 │                             3 │ 8f3a3c2e-…-b2c1       │ 2026-04-30 09:30:51+00  │ 2026-04-30 09:31:08+00  │                     30 │ 2026-04-30 08:00:00+00  │ ekku       │ 2026-04-30 09:31:08+00  │ NULL     │
└───────────────┴─────────────┴───────────────┴──────────────┴──────────────────┴────────────────────────┴──────────────────────────┴─────────────────────────┴───────────────────────────────┴───────────────────────┴─────────────────────────┴─────────────────────────┴────────────────────────┴─────────────────────────┴────────────┴─────────────────────────┴──────────┘
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
consumer's `change_types` and `event_categories` filters.

### `cdc_changes`

```sql
SELECT * FROM cdc_changes(catalog, name, table_name, max_snapshots := 100);
```

Typed, row-level DML for one fully-qualified table inside the current
window. The middle of the column list mirrors DuckLake's `table_changes`
output for that table; the ends add snapshot and commit metadata.

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
should apply DDL before DML for the same snapshot.

**Use cases:** drive a schema-aware sink (apply column adds before
re-reading rows); power schema-only watchers configured with
`event_categories := ['ddl']`.

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
row per snapshot, not per data row. For typed DML rows, use
`cdc_changes`; for typed DDL rows, use `cdc_ddl`.

**Use cases:** outbox-style dispatch on `commit_extra_info`; lake
dashboards that want one row per snapshot with author and message.

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

---

## Observability

### `cdc_consumer_stats`

```sql
SELECT * FROM cdc_consumer_stats(catalog, consumer := NULL);
```

One row per consumer (filtered by `consumer :=` if provided). Reports
cursor lag, gap risk versus the oldest available snapshot, table
filters, and lease state.

**Use cases:** dashboards and alerts — `lag_snapshots`, `lag_seconds`,
`gap_distance`, `lease_alive`, `tables_unresolved` are the columns most
operators wire into monitors.

**Returns:**
`consumer_name VARCHAR, consumer_id BIGINT,
last_committed_snapshot BIGINT, current_snapshot BIGINT,
lag_snapshots BIGINT, lag_seconds DOUBLE,
oldest_available_snapshot BIGINT, gap_distance BIGINT,
tables VARCHAR[], tables_unresolved VARCHAR[], change_types VARCHAR[],
owner_token UUID, owner_acquired_at TIMESTAMPTZ,
owner_heartbeat_at TIMESTAMPTZ, lease_interval_seconds INTEGER,
lease_alive BOOLEAN`.

```text
┌───────────────┬─────────────┬─────────────────────────┬──────────────────┬───────────────┬─────────────┬───────────────────────────┬──────────────┬───────────────┬───────────────────┬──────────────┬───────────────────┬─────────────────────────┬─────────────────────────┬────────────────────────┬─────────────┐
│ consumer_name │ consumer_id │ last_committed_snapshot │ current_snapshot │ lag_snapshots │ lag_seconds │ oldest_available_snapshot │ gap_distance │ tables        │ tables_unresolved │ change_types │ owner_token       │ owner_acquired_at       │ owner_heartbeat_at      │ lease_interval_seconds │ lease_alive │
├───────────────┼─────────────┼─────────────────────────┼──────────────────┼───────────────┼─────────────┼───────────────────────────┼──────────────┼───────────────┼───────────────────┼──────────────┼───────────────────┼─────────────────────────┼─────────────────────────┼────────────────────────┼─────────────┤
│ orders_sink   │           1 │                      42 │               43 │             1 │       12.4  │                         5 │           37 │ [main.orders] │ []                │ [insert]     │ 8f3a3c2e-…-b2c1   │ 2026-04-30 09:30:51+00  │ 2026-04-30 09:31:08+00  │                     30 │ true        │
└───────────────┴─────────────┴─────────────────────────┴──────────────────┴───────────────┴─────────────┴───────────────────────────┴──────────────┴───────────────┴───────────────────┴──────────────┴───────────────────┴─────────────────────────┴─────────────────────────┴────────────────────────┴─────────────┘
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

---

## Filter rules

- `event_categories` is the top-level stream filter.
- `tables` filters table-scoped DDL and DML.
- `change_types` filters DML only.
- Filters compose with AND semantics.
- Matching is exact on DuckLake's qualified table names.

## Ordering

- Snapshot order is ascending by `snapshot_id`.
- DDL should be applied before DML for the same snapshot.
- DML rows preserve DuckLake's `table_changes` row identity and
  `change_type`.

## Storage tables

The extension creates these metadata-catalog state tables on first use:

- `__ducklake_cdc_consumers`
- `__ducklake_cdc_audit`
- `__ducklake_cdc_dlq`

DuckDB and PostgreSQL metadata catalogs place them under
`__ducklake_metadata_<catalog>.__ducklake_cdc`. SQLite metadata catalogs do
not support schemas, so the same table names live in
`__ducklake_metadata_<catalog>.main`.

The DLQ table exists so the schema is stable, but DLQ helper APIs are
not shipped yet.

## Related docs

- [Design notes](./design.md)
- [Errors](./errors.md)
- [Lease operations](./operational/lease.md)
- [Wait operations](./operational/wait.md)
- [Hazard log](./hazard-log.md)
