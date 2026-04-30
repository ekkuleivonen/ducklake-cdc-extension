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

## Notices and Warnings

### `CDC_SCHEMA_BOUNDARY`

`cdc_window` returned a window with `schema_changes_pending = true`. Commit the
current window, then read DDL with `cdc_ddl` before applying DML from the next
window.

### `CDC_WAIT_TIMEOUT_CLAMPED`

`cdc_wait` was called with a timeout above the extension cap, so the timeout was
clamped.

### `CDC_WAIT_SHARED_CONNECTION`

`cdc_wait` holds a DuckDB connection for the duration of the wait. Use a
dedicated connection for long-polling so shared pools, notebooks, or request
handlers are not blocked.

## Related Docs

- [API reference](./api.md)
- [Hazard log](./hazard-log.md)
