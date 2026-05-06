# 01 &mdash; Streamable Datalake Schemas

Turn your datalake's evolving schemas into live git diffs. Row consumers stay
durable, stop at schema boundaries, let you migrate the sink, then resume on the
next schema version without guessing what shape a row has.

![Schema-safe consumer UI](./demo.gif)

## Python Client

```python
from ducklake_cdc_client import DMLConsumer


consumer = DMLConsumer(lake, "orders_dml_v1", table="orders", mode="changes").open()

while True:
    for batch in consumer.batches(
        timeout_ms=1_000,
        max_snapshots=20,
        idle_timeout=1.0,
    ):
        with batch.transaction() as tx:
            apply_batch(tx, batch.changes)

    if consumer.window(max_snapshots=20).schema_changes_pending:
        migrate_sink(consumer.schema_diff())
        next_consumer = consumer.successor("orders_dml_v2")
        consumer.close()
        consumer = next_consumer.open()
```

## API References

- [`cdc_dml_consumer_create`](../../docs/api.md#cdc_dml_consumer_create): create a DML consumer pinned to one table shape.
- [`cdc_dml_changes_listen`](../../docs/api.md#cdc_dml_changes_listen): long-poll row changes for the active shape.
- [`cdc_window`](../../docs/api.md#cdc_window): detect the terminal schema boundary.
- [`cdc_schema_diff`](../../docs/api.md#cdc_schema_diff): read line-by-line schema changes at the boundary.
- [`cdc_commit`](../../docs/api.md#cdc_commit): advance the durable cursor after the sink transaction commits.
- [`cdc_ddl_consumer_create`](../../docs/api.md#cdc_ddl_consumer_create) and [`cdc_ddl_changes_listen`](../../docs/api.md#cdc_ddl_changes_listen): watch source DDL when the app wants a separate DDL stream.
