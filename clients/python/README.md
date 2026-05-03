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

The demo has two scripts: a long-running `consumer.py` that observes
everything the lake produces and a short-lived `producer.py` that
generates a workload. The consumer is intentionally knob-free — the
library's job is to absorb whatever the producer throws at it efficiently
with sensible defaults — so all the workload knobs live on the producer.

```bash
# one terminal: start the consumer (Ctrl+C when you want the summary)
uv run python demo/consumer.py

# another terminal: run a workload
uv run python demo/producer.py --schemas 1 --tables 2 --inserts 1000
```

By default the demo uses the included Postgres-backed DuckLake metadata
catalog on `localhost:5435` plus local data files under `demo/.work/`.
Start the demo catalog first:

```bash
docker compose up -d --wait
```

Use `--catalog-backend sqlite` (on either script) to opt into the local
SQLite catalog. You can still override either process with
`--catalog`/`--storage` or the `DUCKLAKE_DEMO_CATALOG` and
`DUCKLAKE_DEMO_STORAGE` environment variables.

`consumer.py` resets the demo catalog and removes local demo data files
before it starts, then waits for producer-created tables. This keeps
results comparable across runs while preserving the intended flow:
start the consumer first, run the producer second, then stop the
consumer to print the summary.

`producer.py` generates schemas, tables, inserts, updates, and deletes
over a requested duration. `--batch_min` and `--batch_max` control how
many actions are grouped into each DuckLake transaction.

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

`consumer.py` discovers the lake's tables, builds one `DMLConsumer` per
table, and runs them concurrently under a single `CDCApp`. On `Ctrl+C`
(or `SIGTERM`) it drains the in-flight batch, prints a summary of
throughput and end-to-end latency, and optionally writes the same
summary as JSON via `--summary-output`. The only flags it accepts are
infrastructure (`--catalog`, `--catalog-backend`, `--storage`) and
`--summary-output` — there are no workload knobs on the consumer side.

```bash
docker compose up -d --wait
export DUCKLAKE_DEMO_STORAGE='s3://my-demo-bucket/ducklake-demo'
uv run python demo/consumer.py --summary-output demo/.work/summary.json
# in another terminal
uv run python demo/producer.py --schemas 2 --tables 3 --inserts 100
# back in the consumer terminal: Ctrl+C
```

To fully reset the local demo Postgres container and volume outside the
normal consumer-time reset, run `docker compose down -v`.

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
