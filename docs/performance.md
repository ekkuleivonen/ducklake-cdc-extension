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

The user-facing demos double as the current smoke-level performance checks.
Each demo can emit a final `--json-summary`, and
`e2e/ci_demo_assertions.py` applies conservative floors for the tick and DAG
workloads. Those gates are meant to catch obvious regressions, not publish
stable benchmark claims.

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

## The four axes

Performance discussions on this project always frame against four axes:

1. **Throughput** (events/sec).
2. **Latency** (fresh producer event → snapshot → consumer observation/commit).
3. **Catalog load** (QPS, connection count).
4. **Footprint** (consumer process CPU / RSS, lag drift).

A change that improves one axis at the cost of another is a tradeoff
worth documenting in commit / PR text. A change that improves all four
is rare and worth celebrating.

## Current CI discipline

The CI gate is **"the demos still prove their operating property"**:

- correctness assertions are hard gates for every demo (`errors=0`,
  invariants, drained row counts);
- performance assertions are conservative hard gates only where the demo story
  depends on them (`04_cache_refresh` tick rate/latency and `05_pipeline_dag`
  throughput/apply latency).

The thresholds live in `e2e/ci_demo_assertions.py`. They should move slowly and
stay intentionally below good local numbers so they catch broken paths rather
than normal CI variance.
