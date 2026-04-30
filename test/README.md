# Testing this extension
This directory contains the test surfaces for `ducklake_cdc`.

- `sql/` holds extension behavior tests written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html).
- `smoke/` holds extension smoke probes that need Python/C++ harnesses.
- `catalog_matrix/` holds backend smoke tests for DuckDB, SQLite, and
  Postgres DuckLake catalogs.
- `upstream/` holds DuckDB/DuckLake contract probes that do not load this extension.
- `client_py/` is reserved for a future Python client.

The root makefile contains targets for the SQLLogicTests. To run them:
```bash
make test
```
or 
```bash
make test_debug
```

Local debug loops can use narrower targets:

```bash
make test_debug_smoke    # extension load + DuckLake catalog compatibility
make test_debug_default  # skips the three slowest integration files
make test_debug_full     # alias for the full debug SQL suite
```

The slowest debug files are the broad DuckLake integration fixtures
(`sql/always_breaks.test`, `sql/consumer_state.test`, and
`sql/sugar.test`), not intentional sleep/timeout assertions.

Run Python smoke probes after `make debug`:

```bash
uv run python test/smoke/compat_warning_smoke.py
uv run python test/smoke/lease_multiconn_smoke.py
uv run python test/smoke/cdc_wait_interrupt_smoke.py
uv run python test/smoke/toctou_expire_smoke.py
```

Run the catalog-matrix smoke harness after `make debug`:

```bash
uv run python test/catalog_matrix/catalog_matrix_smoke.py
docker compose -f test/catalog_matrix/docker-compose.yml up -d --wait
uv run python test/catalog_matrix/catalog_matrix_smoke.py --backends duckdb sqlite postgres
docker compose -f test/catalog_matrix/docker-compose.yml down -v
```

Run upstream DuckLake contract checks:

```bash
uv run python test/upstream/enumerate_changes_map.py --check
```
