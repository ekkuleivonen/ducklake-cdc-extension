# SQL API

This document describes the desired public SQL contract for `ducklake_cdc`.
The project is still greenfield; treat this file as the target surface the
implementation, Python client, and examples should converge on before a stable
release.

## Conventions

- Every public function is registered as a DuckDB table function and is called
  from a `FROM` clause, except `cdc_version()`, which is scalar.
- All functions take an explicit `catalog` argument: the attached DuckLake
  catalog name, for example `'lake'` after `ATTACH ... AS lake`.
- Stateful functions operate on a named consumer cursor. A consumer owns a
  durable `last_committed_snapshot`, a lease, and normalized subscriptions.
- DDL and DML are separate stream types. DDL consumers watch catalog/schema/table
  structure. DML consumers watch concrete table identities.
- `listen` means "wait up to a deadline, then return available work."
- `read` means "return currently available work without waiting."
- `query` means "stateless historical query; no consumer, no lease, no commit."
- `ticks` are the cheapest eventful stream: snapshot/touch metadata only, no
  row payloads and no expanded DDL payloads.
- `changes` are payload streams: parsed DDL changes or DML rows.
- `auto_commit` is always optional and always defaults to `FALSE`.

## Index

- General:
  [`cdc_version`](#cdc_version),
  [`cdc_doctor`](#cdc_doctor),
  [`cdc_list_consumers`](#cdc_list_consumers),
  [`cdc_list_subscriptions`](#cdc_list_subscriptions),
  [`cdc_consumer_stats`](#cdc_consumer_stats),
  [`cdc_audit_events`](#cdc_audit_events)
- Consumer lifecycle:
  [`cdc_ddl_consumer_create`](#cdc_ddl_consumer_create),
  [`cdc_dml_consumer_create`](#cdc_dml_consumer_create),
  [`cdc_consumer_reset`](#cdc_consumer_reset),
  [`cdc_consumer_drop`](#cdc_consumer_drop),
  [`cdc_consumer_heartbeat`](#cdc_consumer_heartbeat),
  [`cdc_consumer_force_release`](#cdc_consumer_force_release)
- Cursor:
  [`cdc_window`](#cdc_window),
  [`cdc_commit`](#cdc_commit)
- Stateful DDL:
  [`cdc_ddl_changes_listen`](#cdc_ddl_changes_listen),
  [`cdc_ddl_changes_read`](#cdc_ddl_changes_read),
  [`cdc_ddl_ticks_listen`](#cdc_ddl_ticks_listen),
  [`cdc_ddl_ticks_read`](#cdc_ddl_ticks_read)
- Stateful DML:
  [`cdc_dml_changes_listen`](#cdc_dml_changes_listen),
  [`cdc_dml_changes_read`](#cdc_dml_changes_read),
  [`cdc_dml_ticks_listen`](#cdc_dml_ticks_listen),
  [`cdc_dml_ticks_read`](#cdc_dml_ticks_read)
- Typed table DML:
  [`cdc_dml_table_changes_listen`](#cdc_dml_table_changes_listen),
  [`cdc_dml_table_changes_read`](#cdc_dml_table_changes_read)
- Stateless:
  [`cdc_ddl_changes_query`](#cdc_ddl_changes_query),
  [`cdc_dml_changes_query`](#cdc_dml_changes_query),
  [`cdc_ddl_ticks_query`](#cdc_ddl_ticks_query),
  [`cdc_dml_ticks_query`](#cdc_dml_ticks_query),
  [`cdc_dml_table_changes_query`](#cdc_dml_table_changes_query)

---

## General

### `cdc_version`

```sql
SELECT cdc_version();
```

Returns the loaded extension version as a scalar `VARCHAR`.

Use it in support tickets, CI logs, benchmark output, and migration checks.

### `cdc_doctor`

```sql
SELECT *
FROM cdc_doctor(catalog, consumer := NULL);
```

Read-only operational diagnostics for a catalog, optionally scoped to one
consumer.

Returns:

```text
severity VARCHAR
code VARCHAR
consumer_name VARCHAR
message VARCHAR
details VARCHAR
```

Expected checks include catalog compatibility, CDC state table presence, cursor
lag, gap risk, stale leases, dropped subscriptions, recent force releases, and
schema-boundary hazards.

### `cdc_list_consumers`

```sql
SELECT *
FROM cdc_list_consumers(catalog, consumer := NULL);
```

Lists registered consumers and their lease/cursor state.

Returns:

```text
consumer_name VARCHAR
consumer_kind VARCHAR        -- ddl | dml
consumer_id BIGINT
subscription_count BIGINT
last_committed_snapshot BIGINT
last_committed_schema_version BIGINT
owner_token UUID
owner_acquired_at TIMESTAMPTZ
owner_heartbeat_at TIMESTAMPTZ
lease_interval_seconds INTEGER
created_at TIMESTAMPTZ
created_by VARCHAR
updated_at TIMESTAMPTZ
metadata VARCHAR
```

### `cdc_list_subscriptions`

```sql
SELECT *
FROM cdc_list_subscriptions(catalog, consumer := NULL);
```

Lists normalized subscription rows. These rows are the durable routing contract.

DDL subscriptions may be catalog-, schema-, or table-scoped. DML subscriptions
are concrete table identities. Name inputs are creation-time sugar only; matching
uses DuckLake ids.

Returns:

```text
consumer_name VARCHAR
consumer_kind VARCHAR        -- ddl | dml
consumer_id BIGINT
subscription_id BIGINT
scope_kind VARCHAR           -- catalog | schema | table
schema_id BIGINT
table_id BIGINT
schema_name VARCHAR
table_name VARCHAR
original_qualified_name VARCHAR
current_qualified_name VARCHAR
status VARCHAR               -- active | renamed | dropped
created_at TIMESTAMPTZ
metadata VARCHAR
```

### `cdc_consumer_stats`

```sql
SELECT *
FROM cdc_consumer_stats(catalog, consumer := NULL);
```

One row per consumer with lag, gap risk, subscription health, and lease health.

Returns:

```text
consumer_name VARCHAR
consumer_kind VARCHAR
consumer_id BIGINT
last_committed_snapshot BIGINT
current_snapshot BIGINT
lag_snapshots BIGINT
lag_seconds DOUBLE
oldest_available_snapshot BIGINT
gap_distance BIGINT
subscription_count BIGINT
subscriptions_active BIGINT
subscriptions_renamed BIGINT
subscriptions_dropped BIGINT
owner_token UUID
owner_acquired_at TIMESTAMPTZ
owner_heartbeat_at TIMESTAMPTZ
lease_interval_seconds INTEGER
lease_alive BOOLEAN
```

### `cdc_audit_events`

```sql
SELECT *
FROM cdc_audit_events(
  catalog,
  since_seconds := 86400,
  consumer      := NULL,
  action        := NULL
);
```

Queries the CDC audit log. Audit rows cover lifecycle actions, resets, drops,
force releases, commits, and lease force-acquires.

Returns:

```text
ts TIMESTAMPTZ
audit_id BIGINT
actor VARCHAR
action VARCHAR
consumer_name VARCHAR
consumer_id BIGINT
details VARCHAR
```

---

## Consumer Lifecycle

### `cdc_ddl_consumer_create`

```sql
SELECT *
FROM cdc_ddl_consumer_create(
  catalog,
  name,
  start_at      := 'now',       -- 'now' | 'beginning' | 'oldest' | snapshot id
  schemas       := NULL,        -- optional LIST<VARCHAR> name sugar
  schema_ids    := NULL,        -- optional LIST<BIGINT>
  table_names   := NULL,        -- optional LIST<VARCHAR>
  table_ids     := NULL,        -- optional LIST<BIGINT>
  metadata      := NULL
);
```

Creates a durable DDL consumer. With no filters, the consumer watches all DDL in
the catalog. Schema filters watch DDL for that schema identity. Table filters
watch DDL touching that table identity.

DDL consumers are the discovery primitive. Applications that want to auto-start
work for new tables should consume DDL, apply their own policy, and then create
or configure DML consumers themselves.

Returns the same row shape as `cdc_list_subscriptions`, limited to the created
consumer.

### `cdc_dml_consumer_create`

```sql
SELECT *
FROM cdc_dml_consumer_create(
  catalog,
  name,
  start_at      := 'now',       -- 'now' | 'beginning' | 'oldest' | snapshot id
  table_names   := NULL,        -- LIST<VARCHAR>; name sugar resolved at start_at
  table_ids     := NULL,        -- LIST<BIGINT>; preferred stable identity input
  change_types  := ['*'],       -- * | insert | update_preimage | update_postimage | delete
  metadata      := NULL
);
```

Creates a durable DML consumer for one or more concrete table identities.

DML consumers do not dynamically subscribe to future tables through schema or
catalog filters. That policy belongs in application code, usually driven by a
separate DDL consumer.

Name inputs are resolved at `start_at` and persisted as `table_id`. A table
subscription follows the same `table_id` across renames. Drop + recreate with
the same name is a new identity and requires a new subscription/consumer.

Returns the same row shape as `cdc_list_subscriptions`, limited to the created
consumer.

### `cdc_consumer_reset`

```sql
SELECT *
FROM cdc_consumer_reset(catalog, name, to_snapshot := 'now');
```

Repositions the cursor and clears the lease.

`to_snapshot` accepts `'now'`, `'beginning'`, `'oldest'`, or an explicit snapshot
id. Resetting can replay older data, fast-forward a broken consumer, or recover
from a retention gap.

Returns:

```text
consumer_name VARCHAR
consumer_id BIGINT
previous_snapshot BIGINT
new_snapshot BIGINT
```

### `cdc_consumer_drop`

```sql
SELECT *
FROM cdc_consumer_drop(catalog, name);
```

Drops a consumer, its subscriptions, and its lease state. Writes an audit event.

Returns:

```text
consumer_name VARCHAR
consumer_id BIGINT
last_committed_snapshot BIGINT
```

### `cdc_consumer_heartbeat`

```sql
SELECT *
FROM cdc_consumer_heartbeat(catalog, name);
```

Extends the current holder's lease without reading more data.

Returns:

```text
heartbeat_extended BOOLEAN
```

### `cdc_consumer_force_release`

```sql
SELECT *
FROM cdc_consumer_force_release(catalog, name);
```

Operator escape hatch. Clears the lease without checking the owner token and
writes an audit event.

Use only when the previous holder is known dead or wedged.

Returns:

```text
consumer_name VARCHAR
consumer_id BIGINT
previous_token UUID
```

---

## Cursor

### `cdc_window`

```sql
SELECT *
FROM cdc_window(catalog, name, max_snapshots := 100);
```

Acquires or refreshes the consumer lease and returns the next available cursor
window. Repeated calls from the same lease holder return the same window until
`cdc_commit` advances the cursor.

Returns:

```text
consumer_name VARCHAR
consumer_kind VARCHAR
start_snapshot BIGINT
end_snapshot BIGINT
has_changes BOOLEAN
schema_version BIGINT
schema_changes_pending BOOLEAN
```

### `cdc_commit`

```sql
SELECT *
FROM cdc_commit(catalog, name, snapshot_id);
```

Advances the consumer cursor after downstream work succeeds. The caller must
hold the lease.

This is the safe at-least-once contract:

```text
listen/read -> write sink durably -> cdc_commit
```

Returns:

```text
consumer_name VARCHAR
committed_snapshot BIGINT
schema_version BIGINT
```

---

## Stateful DDL

### `cdc_ddl_changes_listen`

```sql
SELECT *
FROM cdc_ddl_changes_listen(
  catalog,
  name,
  timeout_ms    := 30000,
  max_snapshots := 100,
  auto_commit   := FALSE
);
```

Waits until DDL changes matching the consumer are available or the deadline is
reached, then returns parsed DDL changes.

`auto_commit := TRUE` advances the cursor after the extension produces the rows.
It is useful for dashboards and best-effort consumers, but not the safe default
for durable sinks.

Returns the same row shape as `cdc_ddl_changes_read`.

### `cdc_ddl_changes_read`

```sql
SELECT *
FROM cdc_ddl_changes_read(
  catalog,
  name,
  start_snapshot := NULL,
  end_snapshot   := NULL,
  max_snapshots  := 100,
  auto_commit    := FALSE
);
```

Reads currently available DDL changes for the consumer without waiting. If
`start_snapshot` and `end_snapshot` are omitted, the function resolves the
consumer's current window. If they are supplied, the function uses that explicit
range and does not re-resolve the window.

Returns:

```text
consumer_name VARCHAR
start_snapshot BIGINT
end_snapshot BIGINT
snapshot_id BIGINT
snapshot_time TIMESTAMPTZ
event_kind VARCHAR       -- created | altered | dropped | renamed
object_kind VARCHAR      -- schema | table | view
schema_id BIGINT
schema_name VARCHAR
object_id BIGINT
object_name VARCHAR
details VARCHAR          -- JSON text
```

### `cdc_ddl_ticks_listen`

```sql
SELECT *
FROM cdc_ddl_ticks_listen(
  catalog,
  name,
  timeout_ms    := 30000,
  max_snapshots := 100,
  auto_commit   := FALSE
);
```

Waits for DDL-relevant snapshot ticks and returns snapshot metadata only.

Use ticks for cheap triggers: cache invalidation, "wake a batch job after N
source snapshots", or lightweight orchestration.

Returns the same row shape as `cdc_ddl_ticks_read`.

### `cdc_ddl_ticks_read`

```sql
SELECT *
FROM cdc_ddl_ticks_read(
  catalog,
  name,
  start_snapshot := NULL,
  end_snapshot   := NULL,
  max_snapshots  := 100,
  auto_commit    := FALSE
);
```

Reads DDL-relevant snapshot ticks for the consumer without waiting.

Returns:

```text
consumer_name VARCHAR
start_snapshot BIGINT
end_snapshot BIGINT
snapshot_id BIGINT
snapshot_time TIMESTAMPTZ
schema_version BIGINT
changes_made VARCHAR
schema_ids BIGINT[]
table_ids BIGINT[]
change_count BIGINT
```

---

## Stateful DML

Consumer-level DML functions are generic and multi-table. They return a stable
schema regardless of the subscribed tables. Use typed table DML functions when
the caller wants DuckDB-typed user columns for one table.

### `cdc_dml_changes_listen`

```sql
SELECT *
FROM cdc_dml_changes_listen(
  catalog,
  name,
  timeout_ms    := 30000,
  max_snapshots := 100,
  auto_commit   := FALSE
);
```

Waits until DML rows matching the consumer are available or the deadline is
reached, then returns generic DML changes for all subscribed tables.

Returns the same row shape as `cdc_dml_changes_read`.

### `cdc_dml_changes_read`

```sql
SELECT *
FROM cdc_dml_changes_read(
  catalog,
  name,
  start_snapshot := NULL,
  end_snapshot   := NULL,
  max_snapshots  := 100,
  auto_commit    := FALSE
);
```

Reads generic DML changes for all tables subscribed by the consumer. If an
explicit range is supplied, the function uses it directly. Otherwise it resolves
the current consumer window.

Returns:

```text
consumer_name VARCHAR
start_snapshot BIGINT
end_snapshot BIGINT
snapshot_id BIGINT
snapshot_time TIMESTAMPTZ
schema_id BIGINT
schema_name VARCHAR
table_id BIGINT
table_name VARCHAR
rowid BIGINT
change_type VARCHAR       -- insert | update_preimage | update_postimage | delete
values JSON               -- stable generic payload for the table row
author VARCHAR
commit_message VARCHAR
commit_extra_info VARCHAR
```

For `insert` and `update_postimage`, `values` is the emitted row. For
`update_preimage` and `delete`, `values` is the previous row version.

### `cdc_dml_ticks_listen`

```sql
SELECT *
FROM cdc_dml_ticks_listen(
  catalog,
  name,
  timeout_ms    := 30000,
  max_snapshots := 100,
  auto_commit   := FALSE
);
```

Waits for DML-relevant snapshot ticks and returns snapshot/table metadata only.

Returns the same row shape as `cdc_dml_ticks_read`.

### `cdc_dml_ticks_read`

```sql
SELECT *
FROM cdc_dml_ticks_read(
  catalog,
  name,
  start_snapshot := NULL,
  end_snapshot   := NULL,
  max_snapshots  := 100,
  auto_commit    := FALSE
);
```

Reads DML-relevant snapshot ticks for the consumer without waiting.

Returns:

```text
consumer_name VARCHAR
start_snapshot BIGINT
end_snapshot BIGINT
snapshot_id BIGINT
snapshot_time TIMESTAMPTZ
schema_version BIGINT
table_ids BIGINT[]
insert_count BIGINT
update_count BIGINT
delete_count BIGINT
change_count BIGINT
```

---

## Typed Table DML

Typed table DML functions return one table's user columns with their DuckDB
types. They are best for application code that joins, validates, or transforms a
small set of known tables.

### `cdc_dml_table_changes_listen`

```sql
SELECT *
FROM cdc_dml_table_changes_listen(
  catalog,
  name,
  table_id      := NULL,
  table_name    := NULL,
  timeout_ms    := 30000,
  max_snapshots := 100,
  auto_commit   := FALSE
);
```

Waits for DML changes for one subscribed table and returns typed rows.

Returns the same variable row shape as `cdc_dml_table_changes_read`.

### `cdc_dml_table_changes_read`

```sql
SELECT *
FROM cdc_dml_table_changes_read(
  catalog,
  name,
  table_id       := NULL,
  table_name     := NULL,
  start_snapshot := NULL,
  end_snapshot   := NULL,
  max_snapshots  := 100,
  auto_commit    := FALSE
);
```

Reads typed DML rows for one subscribed table. `table_id` is preferred.
`table_name` is name sugar resolved inside the requested range.

Returns a variable schema:

```text
consumer_name VARCHAR
start_snapshot BIGINT
end_snapshot BIGINT
snapshot_id BIGINT
rowid BIGINT
change_type VARCHAR
<user columns from the table...>
snapshot_time TIMESTAMPTZ
author VARCHAR
commit_message VARCHAR
commit_extra_info VARCHAR
```

---

## Stateless Queries

Stateless functions do not create a consumer, acquire a lease, or advance a
cursor. Use them for debugging, bounded backfills, exports, and support tools.

### `cdc_ddl_changes_query`

```sql
SELECT *
FROM cdc_ddl_changes_query(
  catalog,
  from_snapshot,
  to_snapshot := NULL,
  schemas     := NULL,
  schema_ids  := NULL,
  table_names := NULL,
  table_ids   := NULL
);
```

Returns parsed DDL changes over an explicit range. `to_snapshot := NULL` means
the current catalog head.

Returns the same payload columns as `cdc_ddl_changes_read`, without
`consumer_name`, `start_snapshot`, or `end_snapshot`.

### `cdc_dml_changes_query`

```sql
SELECT *
FROM cdc_dml_changes_query(
  catalog,
  from_snapshot,
  to_snapshot := NULL,
  table_names := NULL,
  table_ids   := NULL,
  change_types := ['*']
);
```

Returns generic multi-table DML changes over an explicit range. Use
`cdc_dml_table_changes_query` for typed rows from one table.

Returns the same payload columns as `cdc_dml_changes_read`, without
`consumer_name`, `start_snapshot`, or `end_snapshot`.

### `cdc_ddl_ticks_query`

```sql
SELECT *
FROM cdc_ddl_ticks_query(
  catalog,
  from_snapshot,
  to_snapshot := NULL,
  schemas     := NULL,
  schema_ids  := NULL,
  table_names := NULL,
  table_ids   := NULL
);
```

Returns DDL-relevant snapshot ticks over an explicit range.

Returns the same payload columns as `cdc_ddl_ticks_read`, without
`consumer_name`, `start_snapshot`, or `end_snapshot`.

### `cdc_dml_ticks_query`

```sql
SELECT *
FROM cdc_dml_ticks_query(
  catalog,
  from_snapshot,
  to_snapshot := NULL,
  table_names := NULL,
  table_ids   := NULL
);
```

Returns DML-relevant snapshot/table ticks over an explicit range.

Returns the same payload columns as `cdc_dml_ticks_read`, without
`consumer_name`, `start_snapshot`, or `end_snapshot`.

### `cdc_dml_table_changes_query`

```sql
SELECT *
FROM cdc_dml_table_changes_query(
  catalog,
  from_snapshot,
  to_snapshot := NULL,
  table_id    := NULL,
  table_name  := NULL
);
```

Returns typed DML rows for one table over an explicit range. This is the
stateless sibling of `cdc_dml_table_changes_read`.

Returns the same variable row shape as `cdc_dml_table_changes_read`, without
`consumer_name`, `start_snapshot`, or `end_snapshot`.

---

## Delivery Semantics

The safe default is at-least-once:

```text
listen/read with auto_commit := FALSE
write downstream sink durably
cdc_commit(catalog, consumer, end_snapshot)
```

If the process crashes before `cdc_commit`, the same window may be replayed.
Sinks should be idempotent by `(consumer_name, snapshot_id, table_id, rowid,
change_type)` or an application-specific key.

`auto_commit := TRUE` advances the cursor after the extension produces the
result rows, before the application has necessarily written them to its sink.
Use it for best-effort streams, dashboards, demos, or sinks whose own semantics
make early cursor advancement acceptable. It is not the durable default.

## Subscription Contract

- DDL subscriptions are discovery/control-plane streams. They may be scoped to
  catalog, schema, or table identities.
- DML subscriptions are data-plane streams. They are concrete table identities.
- DML schema/catalog expansion is not dynamic. If an application wants "all
  future tables in schema X", it should create a DDL consumer, watch for table
  creation, apply its own policy, and then create or configure DML consumers.
- Names are accepted as creation/query sugar. Durable matching uses DuckLake
  `schema_id` and `table_id`.
- Drop + recreate with the same name creates a new identity.

## Ordering

- Snapshot order is ascending by `snapshot_id`.
- DDL should be applied before DML for the same snapshot when both streams are
  consumed by the same application.
- DML row order is stable within a table by `(snapshot_id, rowid)`.
- Cross-table ordering should use `(snapshot_id, table_id, rowid)` unless a sink
  defines a stricter application order.

## Storage Tables

The extension creates CDC state tables in the DuckLake metadata catalog:

- `__ducklake_cdc_consumers`
- `__ducklake_cdc_consumer_subscriptions`
- `__ducklake_cdc_audit`

`__ducklake_cdc_consumers` stores cursor and lease state.
`__ducklake_cdc_consumer_subscriptions` stores normalized routing identities.
`__ducklake_cdc_audit` stores lifecycle and operational events.

DuckDB and PostgreSQL metadata catalogs place these under
`__ducklake_metadata_<catalog>.__ducklake_cdc`. SQLite metadata catalogs may
store them under `__ducklake_metadata_<catalog>.main` when schemas are not
available.

## Related Docs

- [Design notes](./design.md)
- [Errors](./errors.md)
- [Performance](./performance.md)
- [Roadmap](./roadmap.md)
- [Hazard log](./hazard-log.md)
