# Smoke tests

Extension smoke tests that need a richer harness than SQLLogicTest (stderr
notice assertions, multiple concurrent connections, DuckDB interrupts,
optional C++ multiconn harnesses).

Build the release extension first (and `make prepare_tests` for probes that
load the official DuckLake artifact):

```bash
make release
make prepare_tests
```

Run CI’s Python probes from the repo root (`ducklake-cdc-tests` uv project):

```bash
uv run python e2e/smoke/compat_warning_smoke.py
uv run python e2e/smoke/lease_multiconn_smoke.py
uv run python e2e/smoke/cdc_wait_interrupt_smoke.py
```

Optional adaptive listen probe (uses `ducklake-client` for attach):

```bash
uv run python e2e/smoke/adaptive_listen_coalesce_smoke.py
```

## DuckLake `snapshots().changes` MAP contract

`enumerate_changes_map.py` pins DuckDB/DuckLake behaviour this extension depends on
but does not load `ducklake_cdc`. It records the shape of DuckLake's
`snapshots().changes` MAP across catalog backends.

Run the default DuckDB + SQLite check from the repository root:

```bash
uv run python e2e/smoke/enumerate_changes_map.py --check
```

For the full backend matrix, start Postgres first (shared with other e2e workloads):

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
uv run python e2e/smoke/enumerate_changes_map.py --backends duckdb sqlite postgres --check
```

The script sets `DATA_INLINING_ROW_LIMIT = 10` explicitly (DuckLake's spec default)
so inlined-data boundary checks stay deterministic across local runs and CI.
