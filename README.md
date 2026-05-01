# ducklake-cdc

A DuckDB extension that adds change-data-capture cursors on top of
[DuckLake](https://ducklake.select).

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
  `cdc_dml_changes_*`, `cdc_dml_ticks_*`, and typed
  `cdc_dml_table_changes_*`.
- Stateless queries: `cdc_ddl_changes_query`, `cdc_ddl_ticks_query`,
  `cdc_dml_changes_query`, `cdc_dml_ticks_query`, and
  `cdc_dml_table_changes_query`.
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
  table_names := ['main.orders'],
  change_types := ['insert', 'update_postimage', 'delete'],
  start_at := 'now'
);

INSERT INTO lake.orders VALUES (2, 'paid');

SELECT *
FROM cdc_dml_table_changes_read(
  'lake',
  'orders_sink',
  table_name := 'main.orders'
);

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

```python
from ducklake import DuckLake
from ducklake_cdc import CDCClient

lake = DuckLake("ducklake:my.ducklake", alias="lake")
cdc = CDCClient(lake)

cdc.dml_consumer_create(
    "orders_sink",
    table_names=["main.orders"],
    change_types=["insert", "update_postimage", "delete"],
)

rows = cdc.dml_table_changes_read_rows("orders_sink", table_name="main.orders")
```

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
