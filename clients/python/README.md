# ducklake-cdc Python Client

Python client helpers for DuckLake and the `ducklake_cdc` DuckDB extension.

This package is scaffolded for early client work and is not published yet.

## DuckLake Quickstart

```python
from ducklake import DuckDBConfig, DuckLake

lake = DuckLake(
    catalog="postgresql://user:pw@host/db",
    storage="s3://my-bucket?endpoint=https://s3.example.com&region=us-east-1",
    duckdb=DuckDBConfig(
        threads=8,
        memory_limit="8GB",
        max_temp_directory_size="20GB",
        settings={"enable_http_metadata_cache": True},
    ),
)

rows = lake.sql("SELECT * FROM events WHERE ts > $cutoff", cutoff="2025-01-01").list()
df = lake.execute("SELECT 1 AS ok").df()

with lake.transaction() as tx:
    tx.sql("INSERT INTO events VALUES ($id, $payload)", id=1, payload="hello").list()
    tx.sql("UPDATE events SET seen = true WHERE id = $id", id=1).list()
```

`DuckLake(...)` is lazy: it validates configuration immediately, then creates
the DuckDB connection and attaches DuckLake on the first query. It exposes common
DuckDB connection methods directly, including `execute(...)`, and delegates other
connection attributes to the underlying `duckdb.DuckDBPyConnection`. Use
`lake.raw_connection()` when you need the underlying `duckdb.DuckDBPyConnection`.
Use `lake.transaction()` to group multiple statements into one DuckLake commit;
it commits on success and rolls back when the block raises.

`DuckDBConfig` mirrors DuckDB runtime setting names directly. Common settings
such as `threads`, `memory_limit`, `max_temp_directory_size`, and
`s3_uploader_max_filesize` are first-class fields; extra `settings` entries are
applied as `SET name = value` before DuckLake is attached.

## Demo

Run the demo scripts from this directory:

```bash
uv run python demo/consumer.py
uv run python demo/producer.py
```

`consumer.py` loads `ducklake_cdc` against the local catalog, creates a
catalog-wide CDC subscription, and streams DDL, snapshot events, row-level DML,
and commits as JSON lines on stdout. Run it first, then run `producer.py` in
another terminal.

By default the demo uses the included Postgres-backed DuckLake metadata catalog
on `localhost:5435` plus local data files under `demo/.work/`. Start the demo
catalog first:

```bash
docker compose up -d --wait
```

Use `--catalog-backend sqlite` to opt into the local SQLite catalog. You can
still override either process with `--catalog`/`--storage` or the
`DUCKLAKE_DEMO_CATALOG` and `DUCKLAKE_DEMO_STORAGE` environment variables.

`consumer.py` resets the demo catalog and removes local demo data files before
it opens the CDC connections, so each demo run starts from a clean catalog and
Parquet directory. `producer.py` generates schemas, tables, inserts, updates,
and deletes over a requested duration. `--batch_min` and `--batch_max` control
how many actions are grouped into each DuckLake transaction. `producer.py --reset`
is still available for one-off producer-only runs, but do not use it while
`consumer.py` is attached.

```bash
uv run python demo/producer.py \
  --schemas 2 \
  --tables 3 \
  --inserts 100 \
  --update 25 \
  --delete 10 \
  --duration 30 \
  --profile ramp \
  --batch_min 5 \
  --batch_max 50
```

```bash
docker compose up -d --wait
export DUCKLAKE_DEMO_STORAGE='s3://my-demo-bucket/ducklake-demo'
uv run python demo/consumer.py --analytics
uv run python demo/producer.py --schemas 2 --tables 3 --inserts 100
```

To fully reset the local demo Postgres container and volume outside the normal
consumer startup reset, run `docker compose down -v`.

For a benchmark-like exploratory run, collect aggregate analytics from the
consumer while keeping the live event stream on stdout:

```bash
uv run python demo/consumer.py \
  --analytics \
  --summary-output demo/.work/summary.json \
  --max-windows 100
```

Then run the producer in another terminal with the workload you want to observe.
The consumer exits after 5 seconds without a new snapshot by default and prints
a summary table with throughput, CDC call counts, operation timing percentiles,
and end-to-end latency for changes that carry the demo `produced_ns` column.
Use `--idle-timeout 0` to keep it running indefinitely. `--summary-output`
writes the same summary as JSON for scripts. This is useful for client/demo
iteration; `bench/runner.py` remains the stricter benchmark harness.

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
