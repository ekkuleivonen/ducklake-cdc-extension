# SQL API

This is the public SQL surface for `ducklake_cdc`. The extension is still
early; prefer clear examples and small compatibility promises over elaborate
contracts.

All functions take an explicit `catalog` argument: the attached DuckLake catalog
name, for example `'lake'` after `ATTACH ... AS lake`.

## Durable Consumer Loop

### `cdc_consumer_create`

```sql
SELECT * FROM cdc_consumer_create(
    catalog,
    name,
    start_at := 'now',
    tables := NULL,
    change_types := NULL,
    event_categories := NULL
);
```

Creates a named consumer cursor.

- `start_at`: `'now'`, `'beginning'`, `'oldest'`, or an explicit snapshot id.
- `tables`: `NULL` for all tables, or a list of fully-qualified
  `schema.table` names.
- `change_types`: `NULL` for all DML change types, or a subset of
  `insert`, `update_preimage`, `update_postimage`, `delete`.
- `event_categories`: `NULL` for both streams, or a subset of `ddl`, `dml`.

### `cdc_window`

```sql
SELECT * FROM cdc_window(catalog, name, max_snapshots := 100);
```

Acquires or refreshes the consumer lease and returns the current read window:

```text
start_snapshot BIGINT
end_snapshot BIGINT
has_changes BOOLEAN
schema_version BIGINT
schema_changes_pending BOOLEAN
```

Calling `cdc_window` repeatedly from the same connection returns the same
window until `cdc_commit` advances the cursor. A different connection trying to
read the same consumer receives `CDC_BUSY`.

### `cdc_changes`

```sql
SELECT * FROM cdc_changes(catalog, name, table_name, max_snapshots := 100);
```

Returns DML rows for one table inside the current consumer window. The row
shape is DuckLake's `table_changes` output plus snapshot metadata. The
consumer's `change_types` and `event_categories` filters are applied.

### `cdc_ddl`

```sql
SELECT * FROM cdc_ddl(catalog, name, max_snapshots := 100);
```

Returns typed DDL events inside the current consumer window:

```text
snapshot_id BIGINT
event_kind VARCHAR       -- created | altered | dropped
object_kind VARCHAR      -- schema | table | view
schema_id BIGINT
schema_name VARCHAR
object_id BIGINT
object_name VARCHAR
details VARCHAR          -- JSON text
```

Consumers should apply DDL before DML for the same snapshot.

### `cdc_commit`

```sql
SELECT * FROM cdc_commit(catalog, name, snapshot_id);
```

Advances the cursor after downstream work succeeds. The caller must still hold
the owner-token lease; otherwise the call raises `CDC_BUSY`.

### `cdc_wait`

```sql
SELECT * FROM cdc_wait(catalog, name, timeout_ms := 30000);
```

Long-polls until a new external DuckLake snapshot exists after the consumer's
cursor, or returns `NULL` on timeout. `cdc_wait` holds the DuckDB connection for
the duration of the call, so use a dedicated connection.

## Consumer Management

```sql
SELECT * FROM cdc_consumer_list(catalog);
SELECT * FROM cdc_consumer_reset(catalog, name, to_snapshot := NULL);
SELECT * FROM cdc_consumer_drop(catalog, name);
SELECT * FROM cdc_consumer_heartbeat(catalog, name);
SELECT * FROM cdc_consumer_force_release(catalog, name);
```

- `cdc_consumer_reset` repositions a cursor and clears the lease.
- `cdc_consumer_heartbeat` extends the current holder's lease.
- `cdc_consumer_force_release` is an operator escape hatch for dead holders.

## Stateless Helpers

These do not create or advance a durable consumer.

```sql
SELECT * FROM cdc_recent_changes(catalog, table_name, since_seconds := 300);
SELECT * FROM cdc_recent_ddl(catalog, since_seconds := 86400, for_table := NULL);
SELECT * FROM cdc_schema_diff(catalog, table_name, from_snapshot, to_snapshot);
```

Use these for exploration, debugging, screenshots, and one-off checks.

## Lake-Level Events

```sql
SELECT * FROM cdc_events(catalog, name, max_snapshots := 100);
```

Returns raw snapshot-level change information for the current consumer window.
This is useful for inspection and dashboards. For typed DML rows, use
`cdc_changes`; for typed DDL rows, use `cdc_ddl`.

## Observability

```sql
SELECT * FROM cdc_consumer_stats(catalog, consumer := NULL);
SELECT * FROM cdc_audit_recent(catalog, since_seconds := 86400, consumer := NULL);
```

`cdc_consumer_stats` reports cursor lag, gap risk, table filters, and lease
state. `cdc_audit_recent` shows recent lifecycle events such as consumer
create, reset, drop, force release, and lease force-acquire.

## Filter Rules

- `event_categories` is the top-level stream filter.
- `tables` filters table-scoped DDL and DML.
- `change_types` filters DML only.
- Filters compose with AND semantics.
- Matching is exact on DuckLake's qualified table names.

## Ordering

- Snapshot order is ascending by `snapshot_id`.
- DDL should be applied before DML for the same snapshot.
- DML rows preserve DuckLake's `table_changes` row identity and `change_type`.

## Storage Tables

The extension creates these DuckLake tables on first use:

- `__ducklake_cdc_consumers`
- `__ducklake_cdc_audit`
- `__ducklake_cdc_dlq`

The DLQ table exists so the schema is stable, but DLQ helper APIs are not
shipped yet.

## Related Docs

- [Design notes](./design.md)
- [Errors](./errors.md)
- [Hazard log](./hazard-log.md)
