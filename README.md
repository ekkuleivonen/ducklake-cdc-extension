# ducklake-cdc

Turn DuckLake snapshots into durable change streams, so you can build realtime
sinks, cache invalidation, and lakehouse automation from SQL or Python.

![Demo](./docs/assets/demo.gif)

> **Pre-alpha.** The current public API is in good shape, but names and row
> shapes may still change.

## Who This Is For

`ducklake-cdc` is for DuckLake users who want to react to changes instead of
polling tables by hand.

Use it if you want to:

- Build downstream sinks from DuckLake tables.
- Invalidate caches when data or schemas change.
- Run lightweight lakehouse automation from SQL or Python.
- Keep durable consumer cursors without adding Kafka, Debezium, or a separate
  state store.
- Experiment with CDC workflows while staying inside DuckDB.

### Use Cases

- [Streamable datalake schemas](./e2e/01_schema_safe_consumer/README.md):
  turn evolving schemas into live diffs, stop row consumers at schema
  boundaries, migrate, then resume safely.
- [Materialized views without full refreshes](./e2e/02_incremental_materialized_view/README.md):
  maintain derived DuckLake tables by applying changed rows instead of
  rescanning the lake.
- [Durable catchup after restarts](./e2e/03_backfill_live_catchup/README.md):
  let producers keep writing while a consumer restarts, then drain exactly the
  missed snapshots.
- [Refresh signals, not row streams](./e2e/04_cache_refresh/README.md):
  wake caches, search indexes, vector indexes, or service-local materializations
  with metadata-only ticks.
- [CDC-native pipeline DAGs](./e2e/05_pipeline_dag/README.md):
  build multi-stage lakehouse pipelines where each node owns a durable cursor.

## What This Is Not

This is not trying to be a high-throughput streaming broker or an ultra-low
latency replication system.

If you need hundreds of thousands of events per second, or consistent sub-50ms
end-to-end latency, you probably want something built for that job.

## Use It From SQL

Create a durable DML consumer and read row changes:

```sql
SELECT *
FROM cdc_dml_consumer_create(
  'lake',
  'orders_sink',
  table_name := 'main.orders',
  change_types := ['insert', 'update_postimage', 'delete']
);

SELECT *
FROM cdc_dml_changes_read('lake', 'orders_sink');

-- After your sink write succeeds, commit the batch's end_snapshot.
SELECT *
FROM cdc_commit('lake', 'orders_sink', <batch_end_snapshot>);
```

Create one catalogue-wide cursor when only schema-independent DML ticks are
needed:

```sql
SELECT *
FROM cdc_dml_consumer_create(
  'lake',
  'catalog_dml_ticks',
  start_at := 'now'
);

SELECT *
FROM cdc_dml_ticks_listen(
  'lake',
  'catalog_dml_ticks',
  timeout_ms := 1000
);
```

Each tick carries the touched `table_ids`. The same cursor follows future
tables and crosses DDL boundaries; fan-out belongs downstream.

Listen for schema changes:

```sql
SELECT *
FROM cdc_ddl_consumer_create('lake', 'schema_watch', schemas := ['main']);

SELECT *
FROM cdc_ddl_changes_listen('lake', 'schema_watch', timeout_ms := 30000);
```

## Learn More

- [SQL API](./docs/api.md)
- [Design notes](./docs/design.md)
- [Python client](https://pypi.org/project/ducklake-cdc-client/)
- [End-to-end demos](./e2e/README.md)

Developer setup, builds, and tests live in [Development](./docs/development.md).
