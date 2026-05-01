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

## Demo

Run the demo scripts from this directory:

```bash
uv run python demo/producer.py
uv run python demo/consumer.py
```

`producer.py` is a placeholder for now. `consumer.py` creates a local DuckLake
catalog under `demo/.work/`, loads `ducklake_cdc`, and calls `cdc_doctor`.

Until the latest extension build is available from DuckDB community extensions,
load a local build by path. By default the demo looks for:

```text
../../build/release/extension/ducklake_cdc/ducklake_cdc.duckdb_extension
```

Override it with:

```bash
DUCKLAKE_CDC_EXTENSION=/path/to/ducklake_cdc.duckdb_extension \
  uv run python demo/consumer.py
```

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
