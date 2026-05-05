# Performance, honestly

`ducklake-cdc` is designed for durable, low-friction CDC over DuckLake
snapshots, not for OLTP-log firehose rates. The current single-table Python demo
shows sub-second fresh-row latency with a few concurrent producers; multi-table
and multi-consumer numbers are still being established.

Idle consumers should be cheap by design: listen functions should combine a
level check ("is there already work?") with a notification/wait path rather than
busy-polling.

## What we measure (and what we don't)

The project tracks four measurement categories:

- **Fresh latency** (`p50`, `p95`, `p99`, `max`, `mean`) — time from a
  producer-side insert/update-postimage timestamp to consumer observation.
  Preimages and deletes are tracked separately because they intentionally carry
  older row-version timestamps.
- **Producer-to-snapshot latency** — producer transaction / visibility cost.
- **Snapshot-to-consumer latency** — the consumer/extension-owned part of the
  pipeline.
- **Events / sec** — total rows delivered by the consumer divided by run
  duration.
- **Catalog QPS** — every stateful read/listen/commit call counts as one
  round-trip from the consumer's perspective.
- **Lag drift** — `producer.snapshots_produced - consumer.commits`,
  sampled per consumer iteration. Producer-truth because
  `cdc_consumer_stats.lag_snapshots` over-counts internal
  lease/commit UPDATEs (those bump the lake snapshot id but are
  invisible to user-data consumers).

Consumer CPU / RSS, sink-side throughput, and sink-side dropped /
retried event counts are deferred to the client and sink work, where
they can be measured against real sinks rather than a stdout-equivalent
in-process consumer.

## Where to read the numbers

The smoke harness lives in `e2e/benchmark/runner.py`, with the first workload
descriptor in `e2e/benchmark/light.yaml`. The manual benchmark workflow
downloads the `linux_amd64` extension artifact from a successful CI run,
executes that descriptor through the published Python packages against the
supported official DuckDB release, and uploads the result JSON as an artifact.
Selected baselines can be committed under `e2e/benchmark/results/` once there
is enough history to make a trajectory meaningful.

## What we are bad at

We are a **poor fit** for:

- **Sub-10ms latency.** Even with notification-based listen functions, latency
  is bounded by DuckLake snapshot creation, query planning, row materialization,
  and consumer commit/checkpoint work. If you need sub-10ms OLTP invalidation,
  Debezium against your source database is the right tool.
- **OLTP CDC.** We read DuckLake snapshots; we do not tap a write-ahead
  log. Latency is bounded below by snapshot frequency.
- **Globally-distributed sub-second cache invalidation.** The lease
  model assumes the consumer holds an actual DuckDB connection to the
  catalog. Cross-region polling against a Postgres catalog is fine for
  reverse ETL but not for cache invalidation in the critical path of a
  user request.

## Python Client-Loop Baseline

The Python benchmark summary separates responsibility:

- `e2e_p{50,95,99}_ms` (in `e2e_latency_ms`) is the headline latency for
  fresh insert and update-postimage events: producer emit ➝ delivered to sink.
- `stage_latency_ms.producer_p95` is the producer-side cost (commit + publish)
  and `stage_latency_ms.pipeline_p95` is the consumer/extension-owned slice;
  the two roughly sum to `e2e_p95_ms`.
- `pipeline_breakdown.extension_listen_ms_p95` /
  `pipeline_breakdown.python_build_ms_p95` decompose the pipeline by who
  introduced the time (extension SQL vs. Python materialization).
- `post_delivery_ms.{sink_p95, extension_commit_p95}` track the user sink
  callback and `cdc_commit` after sink success — both run *after* the
  delivery point, so they affect throughput, not e2e latency.
- `health.rows_excluded_from_e2e` counts preimage and delete rows whose
  timestamp represents the previous row version, not the current action.

Recent local runs show the single-table path is no longer dominated by the old
duplicate window resolution. The next performance questions are multi-table
single-consumer fan-out and multi-consumer catalog pressure. The target API
captures those as generic consumer-level DML reads/listens plus typed
single-table reads for application processing.

## The four axes

Performance discussions on this project always frame against four axes:

1. **Throughput** (events/sec).
2. **Latency** (fresh producer event → snapshot → consumer observation/commit).
3. **Catalog load** (QPS, connection count).
4. **Footprint** (consumer process CPU / RSS, lag drift).

A change that improves one axis at the cost of another is a tradeoff
worth documenting in commit / PR text. A change that improves all four
is rare and worth celebrating.

## Current benchmark discipline

The manual benchmark gate is **"the harness ran and produced a number"**.
Soft gates print `::warning::` annotations when:

- `lag_snapshots_max > 0` over the 60-second light smoke run, or
- `catalog_qps_avg > 5` (one consumer at default backoff).

Latency thresholds are recorded but not gated. The absolute target
(`p99 < 1s` for the `light` workload) is a design target, not a hard
contract. The useful discipline for now is "no surprising regression vs.
previous run for this workload + commit-relative hardware label" — the
trajectory matters more than the absolute number until the project has enough
history.

The benchmark workflow already runs after the extension distribution
matrix by downloading the matrix-built artifact. The likely future regular
CI gate is a 5-minute `medium` workload on every platform the matrix
builds and every supported catalog backend available there. Long soaks, heavy
workloads, and variable-load profiles are not part of the default PR loop.
