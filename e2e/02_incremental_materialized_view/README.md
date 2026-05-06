# 02 &mdash; Materialized Views Without Full Refreshes

Keep derived DuckLake tables current by applying changed rows, not rescanning
the lake. Durable DML consumers turn every insert, update, and delete into delta
math for aggregates, indexes, caches, or serving tables.

![Incremental materialized view UI](./demo.gif)

## Python Client

```python
from ducklake_cdc_client import DMLConsumer


consumer = DMLConsumer(lake, "daily_revenue_mv", table="orders", mode="changes").open()

for batch in consumer.batches(timeout_ms=1_000, max_snapshots=20):
    with batch.transaction() as tx:
        apply_deltas(tx, batch.changes)
```

`batch.transaction()` keeps the aggregate update and `cdc_commit` in one
transaction, so replay cannot double-apply a committed delta.

## API References

- [`cdc_dml_consumer_create`](../../docs/api.md#cdc_dml_consumer_create): create the durable row-change consumer.
- [`cdc_dml_changes_listen`](../../docs/api.md#cdc_dml_changes_listen): long-poll changed rows for the next window.
- [`cdc_commit`](../../docs/api.md#cdc_commit): advance the cursor after the aggregate update commits.
- [`cdc_window`](../../docs/api.md#cdc_window): inspect the current durable window and committed cursor.
