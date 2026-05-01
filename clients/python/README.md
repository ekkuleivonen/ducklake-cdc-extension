# ducklake-cdc Python Client

Python client helpers for DuckLake and the `ducklake_cdc` DuckDB extension.

This package is scaffolded for early client work and is not published yet.

## DuckLake Quickstart

```python
from ducklake import DuckLake

lake = DuckLake.open(
    catalog="postgresql://user:pw@host/db",
    storage="s3://my-bucket?endpoint=https://s3.example.com&region=us-east-1",
    duckdb_settings={
        "enable_http_metadata_cache": True,
        "enable_object_cache": True,
    },
)

rows = lake.sql("SELECT * FROM events WHERE ts > $cutoff", cutoff="2025-01-01").list()
```

`DuckLake.open()` is lazy: it validates configuration immediately, then creates
the DuckDB connection and attaches DuckLake on the first query. Use
`lake.raw_connection()` when you need the underlying `duckdb.DuckDBPyConnection`.

`duckdb_settings` mirrors DuckDB runtime settings directly. Each entry is applied
as `SET name = value` before DuckLake is attached.

## Development

```bash
cd clients/python
uv sync
uv run pytest
uv run ruff check .
uv run mypy
```

Build the local package with:

```bash
uv build
```
