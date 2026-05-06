# 01 &mdash; In-process pipeline DAG

> *"Materialize-class semantics, in your app's process."*

The flagship example. A multi-stage data-processing pipeline running entirely
inside one Python process, on top of one DuckLake. Each stage is a CDC
consumer that reads changes from upstream tables, transforms them, and writes
into a downstream table &mdash; which the next stage's consumer in turn picks
up. Exactly-once at every stage edge via the transactional `cdc_commit` boundary.

This example is the conceptual heart of the suite. If it's not beautiful, the
product isn't ready.

## The pipeline shape

A small but representative DAG &mdash; small enough to run on a laptop, broad
enough to exercise every interesting CDC behavior:

```
  raw_events_{1..4}     ─┐
                         ├─►  clean_events_{1..4}   ─┐
                         │       (decode + enrich)    ├─► joined_orders   ─┐
  raw_orders            ─┘                            │                     ├─► hourly_revenue
                                                      │                     │
                                                      └────────────────────┘
```

Five raw tables &rarr; five clean tables &rarr; one joined table &rarr; one
aggregate. Each arrow is a CDC consumer. The whole DAG runs in one process.

The producer is a separate concern: it can be either an external simulator
(reuses `e2e/benchmark/producer.py`) or in-process. The example runs both
modes so the demo viewer sees the *application* of the pattern (in-process
DAG) regardless of where the source data comes from.

## API surface used

- `cdc_dml_consumer_create` &mdash; one per stage edge
- `cdc_ddl_consumer_create` &mdash; one per stage; surfaces schema boundaries
- `cdc_window` &mdash; per-cycle snapshot probe
- `cdc_dml_changes_read` &mdash; payload read inside the transformation transaction
- `cdc_commit` &mdash; cursor advance, atomic with the sink write
- `cdc_ddl_changes_read` &mdash; for the schema-boundary handler

Schema-boundary handling matters here in a way it doesn't for the simpler
examples: an `ALTER TABLE` upstream terminates the affected DML consumer at
the boundary; the DAG must stand a successor consumer up at the same
snapshot under the new shape, and downstream stages must reconcile too. The
example demonstrates an automated boundary handler so the DAG self-heals.

## Demo visualization

Bespoke. A live DAG diagram in the terminal:

```
   raw_events_1  ──► clean_events_1  ─┐
       42 r/s         42 r/s ▓▓▓▓▓░  │
                                      ├─► joined_orders ──► hourly_revenue
   raw_events_2  ──► clean_events_2  │      28 r/s ▓▓▓░     1.2 commits/min
       38 r/s         38 r/s ▓▓▓▓░   │      lag p99 412 ms   lag p99 4.1 s
                                      │
   raw_orders    ─────────────────────┘
       12 r/s
```

Per-edge: throughput, fill bar showing relative load, p99 lag. Aggregated at
top: total commits/sec across all stages, current lake commit budget
utilization, snapshot pointer pressure.

## Acceptance criteria

In addition to the suite-wide criteria in `e2e/examples/README.md`:

- [ ] Runs the full DAG against the Postgres catalog with sustained
      throughput &ge; 5,000 raw rows/s while keeping joined-stage lag
      p99 &le; 1&nbsp;s.
- [ ] Same DAG runs against the embedded `duckdb` and `sqlite` catalogs in
      single-process mode (no separate producer subprocess), proving the
      in-process pattern is catalog-portable.
- [ ] An `ALTER TABLE` injected mid-run on any raw table is handled
      automatically: the DDL consumer detects it, the affected DML consumer
      terminates cleanly at its schema boundary, a successor is created, and
      the downstream `joined_orders` and `hourly_revenue` stages converge to
      the post-ALTER shape within 30&nbsp;s.
- [ ] Killing the example process mid-pipeline and restarting it advances
      every consumer past the same point with no duplicates and no gaps.
      (Exactly-once via durable cursor &mdash; this is the whole point.)
- [ ] Numbers in the README update from real measurements on a known machine
      class (Apple M-series laptop *and* a small Linux VM).

## Talk story

> "Most teams reach for Kafka + Flink to build something like this. We built
> it on a single DuckDB process. Same exactly-once guarantee. Watch."

The talk demo runs the pipeline live and injects an `ALTER TABLE` mid-stream
to show schema-boundary handling. Then it kills the process to show
exactly-once recovery.

## Open questions

1. **Threading model.** Async tasks vs. one OS thread per stage? Async is
   cleaner and lighter; threads simplify the per-stage DuckDB cursor
   management. Decide during build of `app.py` &mdash; benchmark both.
2. **DAG declaration.** Is the DAG defined in code (decorators on python
   functions) or in a YAML/JSON file the runtime interprets? Code reads
   better as a doc; YAML is closer to what an "operations layer" would look
   like. Pick one and commit.
3. **Lake-shard threshold.** When the joined-stage lag approaches budget,
   the example should narrate "you'd shard the lake here." Worth building
   a *measured* shard-decision aid into the demo, or out of scope?

## Running this example

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release

# default: live TUI, runs until Ctrl-C
uv run --project e2e python e2e/01_pipeline_dag/app.py

# CI: same workload, no TUI, asserts + writes metrics JSON
uv run --project e2e python e2e/01_pipeline_dag/app.py --headless --catalog postgres
```

Catalog support: `duckdb`, `sqlite`, `postgres` &mdash; CI fans this example
across all three (matches the `e2e/catalog_matrix/` portability gate) since
the in-process design works against every backend.

Storage: `--storage disk` (default) or `--storage s3`. The README's measured
numbers should report both so disk *and* S3 commit-cost shapes are visible.

## Status

TODO. Build first; nothing else in the suite blocks on this.
