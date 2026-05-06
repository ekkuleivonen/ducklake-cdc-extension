# 04 &mdash; Cache / Search Refresh

A metadata-only change tap for systems that should re-read from DuckLake on
demand: caches, search indexes, vector indexes, CDN tags, or service-local
materializations.

## Story

A producer commits lightweight updates to a watched DuckLake table. The
consumer waits in `cdc_dml_ticks_listen`, receives one tick per matching
snapshot, records commit-to-tick latency, and advances the cursor. It never
reads row payloads.

```
raw_ticks  ->  cdc_dml_ticks_listen  ->  refresh signal
```

## Why This Is Fourth

Ticks are useful, but they are not the core product claim. This demo exists to
show the cheap wakeup primitive that other systems can use after the first
three demos have established durable, schema-aware consumption.

## Limitations

This is not a microsecond event bus. Postgres catalogs use the snapshot
`LISTEN`/`NOTIFY` fast path; DuckDB and SQLite catalogs use polling. The
visible latency includes DuckLake commit cost and metadata work. This demo
sets `coalesce=False` because it measures first-tick latency rather than
throughput-oriented batching.

## Run

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release

uv run --project e2e python e2e/04_cache_refresh/app.py
uv run --project e2e python e2e/04_cache_refresh/app.py --headless --duration 30
uv run --project e2e python e2e/04_cache_refresh/app.py --headless --duration 30 --catalog duckdb
uv run --project e2e python e2e/04_cache_refresh/app.py --headless --duration 30 --catalog sqlite
```

The lake is reset before and after each run.

## Reading The Numbers

```
producer_rows=N     -> rows the producer inserted
producer_commits=N  -> commits/snapshots the producer made
ticks=N             -> ticks the consumer delivered
rate=N/s            -> ticks delivered per second
p50, p95, p99       -> commit -> tick latency percentiles
hist=[a,b,c,d,e]    -> latency bins: <5ms, 5-10ms, 10-25ms, 25-100ms, >100ms
errors=N            -> unhandled exceptions
```

A healthy run has `producer_commits == ticks` or is off by one tick at
shutdown, with `errors=0`.
