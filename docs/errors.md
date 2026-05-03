# Errors and Notices

`ducklake-cdc` uses stable, searchable error codes in user-facing messages.
Messages may gain detail over time, but the code prefix is the part scripts
should key on.

## Errors

### `CDC_GAP`

The consumer cursor points at a snapshot that DuckLake has expired.

Typical recovery:

```sql
SELECT * FROM cdc_consumer_reset(
  'lake',
  'consumer_name',
  to_snapshot := 'oldest_available'
);
```

This skips expired history. To avoid gaps, run consumers more frequently than
your DuckLake retention window.

### `CDC_BUSY`

Another connection owns the consumer lease, or the caller lost its lease before
commit.

Use `cdc_consumer_stats` to inspect the holder. Use
`cdc_consumer_force_release` only when you know the holder is dead.

### `CDC_MAX_SNAPSHOTS_EXCEEDED`

`max_snapshots` exceeded the extension's hard cap. Use smaller batches unless
you are deliberately doing a large catch-up read.

### `CDC_INCOMPATIBLE_CATALOG`

The attached DuckLake catalog is outside the catalog format range this
extension knows how to read safely.

### `CDC_SCHEMA_TERMINATED`

A DML consumer is pinned to the schema shape of its subscribed tables at
creation time. The shape is the column set those tables have at the
consumer's `last_committed_snapshot`. Once any subscribed table is
altered or dropped, the consumer terminates: it stops returning DML and
its cursor is parked at the snapshot before the schema change.

This error fires when a caller tries to drive the cursor past the boundary:

- `cdc_commit(catalog, name, snapshot_id)` with `snapshot_id >= boundary`.
- `cdc_consumer_reset(catalog, name, to_snapshot)` to a target on the other
  side of any boundary in the cursor-to-target range. Same-shape rewinds
  are still allowed.

Recovery: create a fresh DML consumer with `start_at` at or after the
boundary snapshot and let it consume the post-change shape. Drive the
orchestration from a DDL consumer that surfaces the boundary event. See
[`cdc_dml_consumer_create`](./api.md#cdc_dml_consumer_create).

## Notices and Warnings

### `CDC_SCHEMA_BOUNDARY`

`cdc_window` returned a window with `schema_changes_pending = true`. Consumers
that process both stream types should read/apply DDL with
`cdc_ddl_changes_read` before applying DML from the same snapshot range.

### `CDC_WAIT_TIMEOUT_CLAMPED`

A listen function was called with a timeout above the extension cap, so the
timeout was clamped.

### `CDC_WAIT_SHARED_CONNECTION`

Listen functions hold a DuckDB connection for the duration of the wait. Use a
dedicated connection for long-polling so shared pools, notebooks, or request
handlers are not blocked.

## Related Docs

- [API reference](./api.md)
- [Hazard log](./hazard-log.md)
