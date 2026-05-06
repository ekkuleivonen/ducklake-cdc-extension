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
- `cdc_dml_changes_listen` &mdash; long-poll wait+fetch of typed change rows
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

1. **Threading model.** ~~Async tasks vs. one OS thread per stage?~~
   *Resolved:* one OS thread per stage. DuckDB releases the GIL during
   query execution and has no async API, so `run_in_executor` would
   defeat the point of async. For the &le;10-stage shape this example
   targets, threads are simpler and the per-thread cursor pattern is
   the DuckDB-native one. Revisit if/when we want to demo 100&ndash;200
   units in one process.
2. **DAG declaration.** ~~Code (decorators) vs YAML?~~ *Resolved:*
   code. The DAG *is* the example; YAML would be premature
   abstraction. As stages multiply (v2+), the per-stage transforms
   move to `stages.py` so `app.py` stays declarative.
3. **Lake-shard threshold.** When joined-stage lag approaches budget,
   the example should narrate "you'd shard the lake here." Out of scope
   for v1&ndash;v3; revisit once joined-stage lag is the actual
   bottleneck on the measured numbers, not the catalog commit ceiling.

## Running this example

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release

# default: live TUI, runs until Ctrl-C
uv run --project e2e python e2e/01_pipeline_dag/app.py

# CI: same workload, no TUI, runs for --duration seconds, writes metrics JSON
uv run --project e2e python e2e/01_pipeline_dag/app.py --headless --duration 60
```

Catalog and storage flags follow the suite-wide convention. v1 ships with
`--catalog postgres --storage disk` only; v2 adds `sqlite` and `duckdb`
(matching `e2e/catalog_matrix/` portability), v3 adds `--storage s3` once
Garage has been exercised manually.

Metrics land at `e2e/.results/01_pipeline_dag-<catalog>-<storage>.json`
in both demo (Ctrl-C) and headless modes -- the data is the same; only
the live rendering differs.

## Status

**v1 (current).** Smallest possible end-to-end pipeline, validating the
plumbing before stage count grows.

- `lake.raw_events` &rarr; `normalize` consumer &rarr; `lake.clean_events`.
- The consumer goes through the headline Python API, not raw SQL:
  ```python
  with DMLConsumer(lake, name, table="raw_events", mode="changes") as consumer:
      for batch in consumer.batches(stop_event=stop, timeout_ms=1000):
          with batch.transaction() as tx:
              tx.executemany(
                  "INSERT INTO sink VALUES (...)",
                  [transform(c) for c in batch],
              )
  ```
  `DMLConsumer` owns *every* connection-management concern: it derives
  and pins a dedicated DuckDB connection (so the CDC lease and the
  cursor advance always agree on which connection holds the lease),
  creates the consumer with the H-022 first-bootstrap retry, drives
  `cdc_dml_changes_listen` (one server roundtrip per batch, no
  ticks-then-read dance), adapts the snapshot window, manages the
  lease, and bounds shutdown via `stop_event`. The example owns the
  business-logic transform (plain Python on `change.values["..."]`)
  and the transactional sink write inside `batch.transaction()`.
- Exactly-once is preserved by `batch.transaction()`: it `BEGIN`s on
  enter, runs `cdc_commit` + `COMMIT` atomically on success, and
  `ROLLBACK`s on exception. With `auto_commit := false` on the listen
  call, the consumer cursor stays parked until `cdc_commit` runs
  inside the transaction. ROLLBACK undoes both the INSERT and the
  cursor advance, so a crash before COMMIT replays the same batch
  &rarr; exactly-once at the consumer's view of the clean stream is
  already in place at this size.
- The consumer thread signals "ready" before the main thread starts
  the producer, so `cdc_dml_consumer_create`'s catalog bootstrap (a
  one-time per fresh database operation) doesn't race with concurrent
  INSERTs on other connections (H-022).
- Why long-poll vs `cdc_window` + `time.sleep`: a 50 ms busy-poll cycles
  roughly 100 postgres-scanner connection acquisitions/second on the
  catalog, which (combined with producer INSERT churn) exhausts the
  duckdb postgres-scanner pool under sustained load and surfaces as
  `Connection pool timeout: all 64 connections in use`. Long-polling
  holds one connection idle server-side for the wait window instead.
- TUI two-pane layout (producer / consumer) under demo mode; periodic
  stderr summary under `--headless`.
- Two-stage signal handling: 1st SIGINT requests graceful stop
  (writes metrics, runs cleanup); 2nd SIGINT hard-exits via
  `os._exit(130)`. The escape hatch matters because `uv run` 0.7.3
  occasionally swallows the first SIGINT when delivery bypasses the
  process group.
- Hermetic boundaries: every run resets the catalog and clears the
  per-example storage at startup *and* at clean exit, so back-to-back
  runs never inherit dirty state and `git status` stays clean after a
  run.
- Metrics JSON shape matches `e2e/_lib/README.md`.

Measured locally (Apple M-series, postgres catalog via pgbouncer, disk
storage), 30 s headless run: 18,800 rows produced and 18,800 rows
consumed (1:1, no drain gap), ~619 rows/s sustained, apply p99 ~274 ms,
zero errors. Short runs (e.g. `--duration 8`) sometimes show a 200-row
tail because the producer's last batch lands after the consumer's last
listen returns; addressing that is a v2 acceptance item (graceful
drain on shutdown). v1 is plumbing-correct, not perf-tuned; the
throughput target lives in v3.

**v2 (next).**

- Add a second stage: `clean_events` &rarr; `enrich` consumer &rarr;
  `enriched_events` (or jump straight to a join). This is where the
  decorator-based DAG declaration earns its keep.
- Validate against `--catalog sqlite` and `--catalog duckdb`.
- Wire the DDL consumer + automated schema-boundary handler.
- Graceful drain on shutdown so the headless metrics show
  `producer_rows_total == consumer_rows_total` exactly.
- Headless correctness assertion: assert raw count == clean count
  post-drain, exit non-zero on mismatch.

**v3 (acceptance).**

- Full DAG shape from "The pipeline shape" above (or a reduced form
  that hits the same shape patterns).
- `--storage s3` against Garage.
- Perf tuning to hit &ge; 5,000 raw rows/s with joined-stage lag p99
  &le; 1 s on the postgres catalog.
- Kill+restart correctness pass demonstrating exactly-once.
- README numbers updated from real measurements on Apple M-series + a
  small Linux VM.
