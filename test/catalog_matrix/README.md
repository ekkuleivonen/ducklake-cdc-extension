# Catalog Matrix Smoke Tests

Phase 2 starts by proving that the Phase 1 extension semantics hold across
DuckLake's supported catalog backends: embedded DuckDB, SQLite, and Postgres.

This smoke harness is deliberately narrower than the full SQLLogic suite. It
checks the README-style DDL + DML consumer flow, schema-boundary behavior, and
owner-token lease rejection on each backend. The full Phase 1 test suite still
needs a backend-matrix runner before Phase 2 can close.

Build the debug extension first:

```bash
make debug
```

Run the local embedded backends:

```bash
uv run python test/catalog_matrix/catalog_matrix_smoke.py
```

Run all backends, including Postgres:

```bash
docker compose -f test/catalog_matrix/docker-compose.yml up -d --wait
uv run python test/catalog_matrix/catalog_matrix_smoke.py --backends duckdb sqlite postgres
docker compose -f test/catalog_matrix/docker-compose.yml down -v
```

The Postgres DSN defaults to the local compose fixture. Override it with
`DLCDC_CATALOG_PG_DSN` or `--postgres-dsn` when running against another
catalog.
