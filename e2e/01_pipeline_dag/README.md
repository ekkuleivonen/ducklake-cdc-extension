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

**v3 (current).** v2's multi-stage topology, now with graceful
shutdown drain, kill+restart correctness coverage, and a refined
join semantic that surfaces the user-owned multi-match choice.

Topology (5 tables, 3 stages, 2 producers, all in one process):

```
  raw_purchases  ─►  normalize_purchases  ─►  clean_purchases  ─┐
                                                                 ├─►  join_orders  ─►  joined_orders
  raw_refunds    ─►  normalize_refunds    ─►  clean_refunds   ──┘   (driver: clean_purchases,
                                                                     lookup: clean_refunds inside tx)
```

What a DAG author writes
------------------------

A stage is just a function `transform(batch, tx) -> rows_written`:

```python
def normalize_purchases(batch, tx) -> int:
    """Subscribed via INSERTS_ONLY -- every change is already an insert."""
    cleaned = [...]                                            # transform Python-side
    tx.executemany("INSERT INTO lake.clean_purchases VALUES ...", cleaned)
    return len(cleaned)

def join_orders(batch, tx) -> int:
    """Driver: clean_purchases. Lookup: clean_refunds inside the tx."""
    if not batch.changes:
        return 0
    tx.execute("""
        INSERT INTO lake.joined_orders
        SELECT d.purchase_id, d.kind, d.amount, r.refund_amount,
               ?, now()
        FROM (VALUES ...) AS d(purchase_id, kind, amount)
        LEFT JOIN lake.clean_refunds r ON r.purchase_id = d.purchase_id
    """, [batch.end_snapshot, *driver_params])
    return len(batch.changes)
```

The DAG topology is one declaration in one place. `change_types`
forwards to `cdc_dml_consumer_create` so the *cursor itself* skips
update / delete events -- updates and deletes never even cross the
listen boundary, so the transform doesn't filter and the listen call
doesn't ship rows the sink would drop:

```python
STAGES = (
    Stage("normalize_purchases", source="raw_purchases",   transform=normalize_purchases, change_types=INSERTS_ONLY),
    Stage("normalize_refunds",   source="raw_refunds",     transform=normalize_refunds,   change_types=INSERTS_ONLY),
    Stage("join_orders",         source="clean_purchases", transform=join_orders,         change_types=INSERTS_ONLY),
)

with StageRunner(lake, STAGES, stop=stop, recorder=recorder):
    ...                  # producers run here, TUI renders, Ctrl-C, etc.
```

`StageRunner` is the only piece of glue that wasn't already in
`ducklake-cdc-client`: it gives each stage one OS thread + one
`DMLConsumer` (which derives a dedicated, lease-pinned DuckDB
connection and forwards `change_types` for extension-level
subscription filtering), runs every batch's transform inside
`batch.transaction()` so the sink write and `cdc_commit` land
atomically, and starts the stages serially so each per-stage
`cdc_dml_consumer_create` finishes its catalog bootstrap before the
next stage begins (H-022 race avoidance).

Exactly-once + replay
---------------------

- Each stage's cursor advances inside the same BEGIN as its sink
  write. ROLLBACK on a transform exception undoes both, the next
  listen replays the same batch &rarr; exactly-once at every stage
  edge.
- The cursor is durable in `__ducklake_cdc.consumer_state`. A killed
  process restarts every stage at the snapshot it last committed.
- Exactly-once is *per stage*, not pipeline-wide. A row that's been
  normalized into `clean_purchases` but not yet joined is durable in
  the catalog &mdash; the join stage's own cursor will pick it up
  next time it runs.

Join semantics (driver + snapshot lookup)
-----------------------------------------

- `clean_purchases` drives, one batch at a time. The batch carries
  the driver columns (`id`, `kind`, `amount` projected onto
  `Change.values`), so we don't re-read `clean_purchases` &mdash;
  the batch *is* the driver.
- The lookup against `clean_refunds` runs on the consumer's
  connection inside `batch.transaction()`. DuckLake gives us
  snapshot-isolated reads inside the transaction, so the lookup sees
  a consistent point-in-time view of `clean_refunds`. (The honest
  answer to "where do you put the join state?": you don't. The
  other table *is* the state.)
- *Multi-match folding (user-owned).* The refunds producer
  intentionally targets random purchase IDs, so a single purchase
  can have N matching refunds in `clean_refunds`. How to fold
  N refunds into one `joined_orders.refund_amount` is a domain
  decision, not a CDC one. The example uses
  `MAX(refund_amount)` in a scalar subquery so every driver row
  produces exactly one output row and the example has a clean 1:1
  invariant (`joined_orders.count == clean_purchases.count`,
  `count(DISTINCT purchase_id) == count(*)`). Real pipelines might
  pick `SUM`, `MIN`, the most-recent, or fan out to one row per
  refund &mdash; the surrounding plumbing doesn't change.
- *Append-only by contract.* A refund that arrives **after** its
  purchase has already been joined leaves `refund_amount` as NULL
  in `joined_orders` *forever*. This is not a bug to repair, it
  is the contract of the snapshot-lookup join model: at the
  moment the join ran, that refund did not exist. Whether to
  repair it &mdash; with a periodic UPDATE sweep, an UPSERT-driven
  repair stage, or not at all &mdash; is also a domain decision the
  example deliberately doesn't make for you. The CDC layer gives
  you the durable cursor and the atomic write boundary; what you
  do with late-arriving lookup data is yours.

Graceful shutdown (drain) and kill+restart
------------------------------------------

The example demonstrates two distinct shutdown shapes that real
pipelines need:

- *Polite shutdown* (timeout elapses, or operator hits Ctrl-C
  once). Producers are told to stop emitting; `StageRunner.__exit__`
  then triggers a *drain pass* on every stage's `DMLConsumer`.
  Each consumer keeps long-polling `cdc_dml_changes_listen` until
  it's been idle for `drain_idle_timeout` seconds (default 2 s),
  then exits. After drain, headless metrics show
  `producer_total == clean_total == joined_total` exactly &mdash;
  no "single-batch shutdown tail" left over.
- *Panic shutdown* (operator hits Ctrl-C twice in quick succession).
  Drain is skipped; consumers exit on their next listen check;
  daemons die on interpreter exit if even that hangs.

Drain is wired through two new pieces:

- `DMLConsumer.batches(drain_event=..., drain_idle_timeout=...)`
  in the lib: a polite "finish what's in flight, then exit" knob
  separate from the panic `stop_event`.
- `StageRunner` owns a `_drain_event` internally and sets it on
  `__exit__` *unless* `stop` is already set; the runner then waits
  up to `drain_timeout_s` (default 5 s) for stages to finish
  naturally before force-stopping anything still alive.

Restart correctness is exercised by `test_restart.sh`. Two
back-to-back runs against the same lake (`--keep-state` skips the
hermetic catalog reset between them) should:

- Continue producer ID space from the existing high-water mark in
  `raw_purchases` / `raw_refunds`, so run 2 doesn't collide with
  run 1's primary keys.
- Pick up every consumer cursor from the snapshot run 1 last
  committed (verified via `cdc_consumer_stats`).
- Maintain the within-run 1:1 invariants on the cumulative tables.
- Show strict cursor monotonicity across the boundary
  (`run2.cursor > run1.cursor` for every consumer).

The script asserts all of the above and refuses to clean up on
failure so the dirty lake is available for inspection. Run it
manually with `bash 01_pipeline_dag/test_restart.sh`.

H-022 mitigation in the example. Empirically the lib's
`retry_on_transient` policy doesn't reliably recover the H-022
race when it fires against a *populated* postgres catalog (the
canonical "kill+restart" shape). The example pre-warms by issuing
one `SELECT cdc_version()` on `lake.connection` immediately after
`LOAD ducklake_cdc` (see `_lib/config.py::load_cdc_extension`),
which avoids the race entirely on the first cdc_dml_*/cdc_*-stats
table-function call. Out-of-band stats reads (e.g.
`inspect_lake.py`) additionally use a derived cursor for the
cdc_* call so a parent connection that's already touched lake
metadata doesn't re-arm the race. Both workarounds are
documented under H-022 in
`ducklake-cdc-extension/docs/hazard-log.md`.

Why this shape and not some others
----------------------------------

- *Decorator-based DAG DSL* &mdash; cute, but hides where the threads
  / connections / cursors come from. Debugging would mean reading
  framework internals; we want users to see plain Python stack
  traces.
- *Symmetric stream-stream join with watermarks* &mdash; powerful,
  but you'd be re-implementing Flink. The driver+lookup model
  covers the common case ("for each fact in this batch, look up
  its dimension") and stays in one screen of code.
- *One DuckDB connection shared across all stages* &mdash; would
  serialize every stage's writes through one mutex. Each stage's
  `DMLConsumer` derives its own connection so they truly run in
  parallel.

The boring-but-important stuff (carried over from v1)
-----------------------------------------------------

- Two-stage signal handling: 1st SIGINT requests graceful stop
  (writes metrics, runs cleanup); 2nd SIGINT hard-exits via
  `os._exit(130)`. The escape hatch matters because `uv run` 0.7.3
  occasionally swallows the first SIGINT when delivery bypasses the
  process group.
- Hermetic boundaries: every run resets the catalog and clears the
  per-example storage at startup *and* at clean exit, so back-to-back
  runs never inherit dirty state and `git status` stays clean after a
  run.
- TUI: producers panel + per-stage stages panel under demo mode;
  periodic stderr summary under `--headless`. Re-rendered at 4 Hz so
  per-stage rows / batches / last snapshot reflect live state.
- Metrics JSON shape matches `e2e/_lib/README.md`. Per-stage detail
  keys: `stage_<name>_rows`, `stage_<name>_batches`,
  `stage_<name>_last_snapshot`. Per-producer detail keys:
  `producer_purchases_total`, `producer_refunds_total`.

Measured locally
----------------

Apple M-series, postgres catalog via pgbouncer, disk storage,
single 10 s headless run with drain enabled:

| signal | value |
|---|---|
| `producer_purchases_total` | 5,800 |
| `stage_normalize_purchases_rows` | 5,800 |
| `producer_refunds_total` | 1,600 |
| `stage_normalize_refunds_rows` | 1,600 |
| `stage_join_orders_rows` | 5,800 |
| `errors` | 0 |
| apply p99 | ~320 ms |
| aggregate throughput | ~1,030 rows/s (sum across stages) |

Strict 1:1 across every edge &mdash; no "single-batch shutdown
tail" anymore (v2's `producer_total - clean_total = 200` is gone).
Drain on shutdown is what closes the gap.

Cumulative across a kill+restart pair (`bash test_restart.sh`,
two 8 s runs back-to-back):

| signal | run 1 | run 2 (cumulative) |
|---|---|---|
| `raw_purchases` rows | 4,800 | 9,200 |
| `clean_purchases` rows | 4,800 | 9,200 |
| `joined_orders` rows | 4,800 | 9,200 |
| `joined_orders` distinct purchase IDs | 4,800 | 9,200 |
| `cursor_join_orders` (snapshot id) | 126 | 239 |

`joined_orders.count == count(DISTINCT purchase_id)` across both
runs is the exactly-once-across-restart invariant: no consumer
replayed a batch after restart, no inserts duplicated under the
producer's primary key. Cursor monotonicity (run 2 > run 1) shows
the durable cursor genuinely picked up where the previous process
left off rather than rewinding to "now."

Known noise: `CDC_WAIT_SHARED_CONNECTION` fires once per stage on
startup. The advisory is unconditional in the extension and is a
false positive on this code path because `DMLConsumer` does own a
dedicated connection. Tracked as a follow-up under H-006 in
`ducklake-cdc-extension/docs/hazard-log.md`. Filter it out of run
output with `2>&1 | grep -v CDC_WAIT_SHARED_CONNECTION` until the
extension-side suppression lands.

**v4 -- next, planned in slices.**

What's left after v3 lands. Slices ordered by what most improves
the example's *story* before what extends its surface; pick
whichever one fits the next session's budget.

*v4a -- catalog / storage portability*

- **`--catalog sqlite` and `--catalog duckdb`** so the example
  matches `e2e/catalog_matrix/` parity. Most of the work is in
  the example's `_lib/config.py` wiring; the consumer/transform
  code already runs catalog-agnostic.
- **`--storage s3` against Garage.** New
  `e2e/garage/docker-compose.yml` spins Garage up; example reads
  S3 creds from `e2e/.env`; storage helper picks `s3` over `disk`
  based on the flag.

*v4b -- schema evolution*

- **DDL consumer + automated schema-boundary handler.** New
  fourth stage that subscribes to schema events on `raw_*` tables;
  when a column is added/renamed/dropped, the DDL consumer fires
  the appropriate "rebind affected DML consumer" handler so the
  pipeline self-heals. The talk-story payoff is "inject `ALTER
  TABLE` mid-stream and watch nothing break."

*v4c -- perf + published numbers*

- **Perf tuning** to hit &ge; 5,000 raw rows/s with joined-stage
  lag p99 &le; 1 s on the postgres catalog. Likely candidates:
  bigger producer batches, parallel transforms inside a stage,
  longer `LISTEN_MAX_SNAPSHOTS` window per batch, postgres
  catalog tuning.
- **Published README numbers** from real measurements on Apple
  M-series + a small Linux VM. Replace the current "measured
  locally" table.
