# 05 &mdash; Multi-Stage Pipeline DAG

A larger example showing several CDC consumers maintaining derived DuckLake
tables inside one process.

```
raw_purchases  ->  normalize_purchases  ->  clean_purchases --.
                                                              +-> join_orders -> joined_orders
raw_refunds    ->  normalize_refunds    ->  clean_refunds   --'
```

## Real-Life Use Case

A data platform team wants lakehouse DAG orchestration without adopting a
separate streaming runtime. Each stage has its own durable CDC cursor and
commits only after its output table update succeeds.

This is intentionally last in the suite. It is useful once the primitives are
understood, but it should not be the first product story: the real wedge is
safe, incremental snapshot consumption.

## Limitations

**Postgres catalog only.** This demo runs five concurrent writers: two
producers and three stage consumers. SQLite and embedded DuckDB catalogs
serialize too much of that write path and turn the demo into a lock-contention
test rather than a CDC demo.

## Run

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release

uv run --project e2e python e2e/05_pipeline_dag/app.py
uv run --project e2e python e2e/05_pipeline_dag/app.py --headless --duration 30
uv run --project e2e python e2e/05_pipeline_dag/app.py --storage s3
```

The lake is reset before and after each run. Nothing is stored past the
process; the printed summary is the result.

## Reading The Numbers

```
rawP=N    rawR=N      -> rows emitted into raw_purchases / raw_refunds
cleanP=N  cleanR=N    -> rows written into clean_purchases / clean_refunds
joined=N              -> rows written into joined_orders
throughput=N rows/s   -> rows committed per second across all writers
p99=N ms              -> p99 per-batch apply latency
errors=N              -> unhandled producer or stage errors
```

A healthy run has `rawP == cleanP == joined`, `rawR == cleanR`, and
`errors=0`. The throughput ceiling is the shared Postgres catalog commit
budget, not Python row generation.
