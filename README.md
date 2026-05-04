# ducklake-cdc

A DuckDB extension that adds change-data-capture cursors on top of
[DuckLake](https://ducklake.select).


![Demo](./docs/assets/demo.gif)

> **Status: greenfield CDC contract.** The extension exposes separate DDL and
> DML consumer streams, explicit read/listen/query modes, durable cursor state,
> and lightweight tick streams. The API is still pre-adoption, so this repository
> intentionally removes old SQL names instead of keeping compatibility shims.

## What Works Today

- Consumer lifecycle: `cdc_ddl_consumer_create`, `cdc_dml_consumer_create`,
  `cdc_consumer_reset`, `cdc_consumer_drop`, `cdc_consumer_heartbeat`, and
  `cdc_consumer_force_release`.
- Cursor primitives: `cdc_window` and `cdc_commit`.
- Stateful streams: `cdc_ddl_changes_*`, `cdc_ddl_ticks_*`,
  `cdc_dml_changes_*` (single typed payload API; one consumer = one
  table), and `cdc_dml_ticks_*`.
- Stateless queries: `cdc_ddl_changes_query`, `cdc_ddl_ticks_query`,
  `cdc_dml_changes_query` (single-table typed lookback), and
  `cdc_dml_ticks_query`.
- Schema inspection: `cdc_schema_diff`.
- Observability: `cdc_version`, `cdc_doctor`, `cdc_list_consumers`,
  `cdc_list_subscriptions`, `cdc_consumer_stats`, and `cdc_audit_events`.
- A thin Python client in `clients/python`.

The full SQL contract and row shapes are documented in
[`docs/api.md`](./docs/api.md).

## Build

```bash
git clone --recursive https://github.com/<this-repo>.git
cd ducklake-cdc-extension
make release
./build/release/duckdb -unsigned -c "SELECT cdc_version();"
```

Development and test details live in [`docs/development.md`](./docs/development.md).

## Quickstart

```sql
INSTALL ducklake;
LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:my.ducklake' AS lake (DATA_PATH 'my_data');

CREATE TABLE lake.orders(id INTEGER, status VARCHAR);
INSERT INTO lake.orders VALUES (1, 'new');

SELECT *
FROM cdc_dml_consumer_create(
  'lake',
  'orders_sink',
  table_name := 'main.orders',
  change_types := ['insert', 'update_postimage', 'delete'],
  start_at := 'now'
);

INSERT INTO lake.orders VALUES (2, 'paid');

-- The pinned table identity is implicit: `cdc_dml_changes_read`
-- projects the row's native columns at the top level alongside
-- `snapshot_id`, `rowid`, `change_type`, `table_id`, `table_name`,
-- and the snapshot's `author` / `commit_message` / `commit_extra_info`.
SELECT *
FROM cdc_dml_changes_read('lake', 'orders_sink');

SET VARIABLE end_snapshot = (
  SELECT end_snapshot FROM cdc_window('lake', 'orders_sink')
);

SELECT *
FROM cdc_commit('lake', 'orders_sink', getvariable('end_snapshot'));
```

For low-latency consumers, use the listen variants:

```sql
SELECT *
FROM cdc_dml_changes_listen(
  'lake',
  'orders_sink',
  timeout_ms := 30000,
  auto_commit := false
);
```

For cache invalidation or orchestration, use ticks:

```sql
SELECT *
FROM cdc_dml_ticks_listen('lake', 'orders_sink', timeout_ms := 30000);
```

DDL consumers are separate:

```sql
SELECT *
FROM cdc_ddl_consumer_create('lake', 'schema_watch', schemas := ['main']);

SELECT *
FROM cdc_ddl_changes_listen('lake', 'schema_watch', timeout_ms := 30000);
```

## Python

Single consumer (DML consumers are pinned to a single table by contract):

```python
from ducklake import DuckLake
from ducklake_cdc import DMLConsumer, StdoutDMLSink

lake = DuckLake("ducklake:my.ducklake", alias="lake")

with DMLConsumer(
    lake,
    "orders_sink",
    table="main.orders",
    change_types=["insert", "update_postimage", "delete"],
    sinks=[StdoutDMLSink()],
) as consumer:
    consumer.run(infinite=True)
```

Multiple consumers in one process — `CDCApp` runs them concurrently with
shared lifecycle and `SIGINT` / `SIGTERM` handling:

```python
from ducklake import DuckLake
from ducklake_cdc import CDCApp, DMLConsumer, StdoutDMLSink

lake = DuckLake("ducklake:my.ducklake", alias="lake")

consumers = [
    DMLConsumer(lake, f"sink_{table.name}", table=f"{table.schema_name}.{table.name}",
                sinks=[StdoutDMLSink()])
    for table in lake.tables()
]

with CDCApp(consumers=consumers) as app:
    app.run(infinite=True)  # blocks until Ctrl+C; drains in-flight batches on exit
```

For raw extension access, the low-level mirror lives at
`ducklake_cdc.lowlevel.CDCClient` and exposes `cdc_dml_consumer_create`,
`cdc_dml_changes_read` / `_listen` / `_query`, `cdc_window`, `cdc_commit`,
and friends as direct methods.

## Tests

The focused new-contract SQL test is:

```bash
./build/release/test/unittest "test/sql/new_api_contract.test"
```

`make test_release_default` runs the active default SQL set. Older SQL suites
that reference removed APIs are intentionally excluded until they are migrated
or deleted.

## Backends

DuckLake supports DuckDB, SQLite, and PostgreSQL metadata catalogs. This project
has smoke coverage for those backends, but non-DuckDB metadata paths should
still be treated as young.

## More

- [`docs/api.md`](./docs/api.md) - public SQL contract
- [`docs/design.md`](./docs/design.md) - state model and design notes
- [`docs/errors.md`](./docs/errors.md) - structured errors and notices
- [`docs/hazard-log.md`](./docs/hazard-log.md) - known risks
- [`VISION.md`](./VISION.md) - product direction
