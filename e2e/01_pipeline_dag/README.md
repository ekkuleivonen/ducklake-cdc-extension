# 01 &mdash; In-process pipeline DAG

A multi-stage CDC pipeline running entirely inside one Python
process, on top of one DuckLake. Two normalize stages and one
snapshot-lookup join, exactly-once at every stage edge.

```
  raw_purchases  →  normalize_purchases  →  clean_purchases ─┐
                                                              ├→  join_orders  →  joined_orders
  raw_refunds    →  normalize_refunds    →  clean_refunds   ─┘
```

## Real-life use case

The "platform team builds a streaming enrichment pipeline without
adopting Kafka + Flink" story. A SaaS app emits two source streams
(orders + refunds); a downstream service needs a joined view
(orders attributed with the largest matching refund). Normalisation
and join run as separate stages so each can fail / restart / scale
independently, but everything lives in one process and one storage
substrate.

The snapshot-lookup join is the realistic shape: refunds that arrive
*after* a purchase has already been joined leave `refund_amount`
NULL forever. That's the trade-off you accept in exchange for "the
join target is just another DuckLake table; consistency comes for
free." A real pipeline that needs different semantics swaps the
join transform's aggregation (MAX → SUM, fan out, repair-sweep,
etc.); the surrounding plumbing is unchanged.

## Limitations

**Postgres catalog only.** The DAG runs five concurrent writers
(2 producer threads + 3 stage consumers, each on its own DuckDB
connection). They all commit through DuckLake's catalog flush,
which is a single-writer operation. On postgres that's fine. On
sqlite or embedded duckdb it's a global file-lock contention storm
that the lib's `retry_on_transient` policy can't recover from.

If you want to validate sequential CDC primitives against
sqlite/duckdb catalogs, that's `e2e/catalog_matrix/`. This example
is the multi-writer story.

## Run

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release

# live TUI, runs until Ctrl-C
uv run --project e2e python e2e/01_pipeline_dag/app.py

# unattended: no TUI, periodic stderr summary, runs for N seconds
uv run --project e2e python e2e/01_pipeline_dag/app.py --headless --duration 30

# S3 storage (Garage local default; ./e2e/setup-garage.sh once first)
uv run --project e2e python e2e/01_pipeline_dag/app.py --storage s3
```

The lake is reset before and after each run. Nothing is stored to
disk past the process; the printed summary line is the result.

## Reading the numbers

The final stderr line (and the live TUI) report:

```
rawP=N    rawR=N      → rows the producers emitted into raw_purchases / raw_refunds
cleanP=N  cleanR=N    → rows each normalize stage wrote into clean_purchases / clean_refunds
joined=N              → rows the join stage wrote into joined_orders
throughput=N rows/s   → total rows committed per second across all writers (producers + stages)
p99=N ms              → 99th-percentile per-batch apply latency across all stages
errors=N              → unhandled exceptions from any producer / stage transform
```

A healthy run:

- `rawP == cleanP == joined` and `rawR == cleanR` &mdash; the strict
  1:1 invariant. Every emitted row made it through the DAG by the
  time the drain pass finished.
- `errors=0`.
- `p99` in the hundreds of ms to low seconds. The join stage's
  per-batch cost grows with `clean_refunds` size (its hash-join
  scan target), so longer runs trend higher.
- `throughput` around 2.5&ndash;3k rows/s on a laptop-class machine.
  The ceiling is the postgres catalog's commits/s budget shared
  across all 5 writers, not Python or DuckDB.

If `cleanP < rawP` (or similar), the consumer didn't drain in time
&mdash; the producer is offering more rows than the pipeline can
absorb. Bump `PRODUCER_BATCH_INTERVAL_S` in `app.py`, or shorten
`--duration` so the drain pass has room.
