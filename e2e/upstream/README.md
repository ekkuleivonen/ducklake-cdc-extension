# Upstream Probes

This directory contains executable probes for DuckDB/DuckLake behavior that
`ducklake_cdc` depends on but does not implement.

These are not extension tests in the usual sense: they do not load
`ducklake_cdc`. They pin upstream contract assumptions, such as the shape of
DuckLake's `snapshots().changes` MAP across catalog backends.

Run the default DuckDB + SQLite check from the root Python project:

```bash
uv run python e2e/upstream/enumerate_changes_map.py --check
```

For the full backend matrix, start Postgres from the repository root first (shared with other e2e workloads):

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
uv run python e2e/upstream/enumerate_changes_map.py --backends duckdb sqlite postgres --check
```

## Caveats

`enumerate_changes_map.py` sets `DATA_INLINING_ROW_LIMIT = 10` explicitly.
That is DuckLake's spec default and keeps inlined-data boundary checks
deterministic across local runs and CI.
