# Upstream Probes

This directory contains executable probes for DuckDB/DuckLake behavior that
`ducklake_cdc` depends on but does not implement.

These are not extension tests in the usual sense: they do not load
`ducklake_cdc`. They pin upstream contract assumptions, such as the shape of
DuckLake's `snapshots().changes` MAP across catalog backends.

Run the default DuckDB + SQLite check from the shared test Python project:

```bash
cd test
uv run python upstream/enumerate_changes_map.py --check
```

For the full backend matrix, start Postgres from this directory first:

```bash
cd test/upstream
docker compose up -d
cd ..
uv run python upstream/enumerate_changes_map.py --backends duckdb sqlite postgres --check
```

## Caveats

`enumerate_changes_map.py` sets `DATA_INLINING_ROW_LIMIT = 10` explicitly.
That is DuckLake's spec default and keeps inlined-data boundary checks
deterministic across local runs and CI.
