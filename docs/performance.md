# Performance, honestly

`ducklake-cdc` is designed for **~100–10k events/sec per consumer** with
**sub-second to seconds latency**, against a Postgres catalog that
comfortably serves **~50 consumers** without connection pooling
(~500 with PgBouncer). Idle consumers are cheap by design — `cdc_wait`
backs off from 100ms to a 10s cap and resets on activity, so 50 idle
consumers are 5 catalog queries / sec total.

## What we measure (and what we don't)

The project tracks four measurement categories:

- **End-to-end latency** (`p50`, `p95`, `p99`, `max`, `mean`) — time
  from a producer-side INSERT landing as a snapshot to the consumer's
  `cdc_commit` for that snapshot id. Computed via a monotonic-time
  ledger so cross-process clock skew is irrelevant.
- **Events / sec** — total rows surfaced via `cdc_changes` divided by
  the run duration.
- **Catalog QPS** — every `cdc_window` / `cdc_changes` / `cdc_commit`
  call counts as one round-trip from the consumer's perspective.
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

No benchmark harness is committed in this clean-slate tree yet. ADR 0011
defines the planned light / medium / heavy workloads and the publication
policy. Until the harness lands, this page records the design targets and
the shape of the numbers we intend to publish.

## What we are bad at

We are a **poor fit** for:

- **Sub-10ms latency.** The polling backoff bottoms out at 100ms; the
  ratchet up means a typical `cdc_wait` returns within 100–300ms of
  the producer commit, not microseconds. If you need sub-10ms,
  Debezium against your OLTP database is the right tool.
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
2. **Latency** (snapshot landing → consumer commit).
3. **Catalog load** (QPS, connection count).
4. **Footprint** (consumer process CPU / RSS, lag drift).

A change that improves one axis at the cost of another is a tradeoff
worth documenting in commit / PR text. A change that improves all four
is rare and worth celebrating.

## Soft gates today, hard gates after Phase 5

The planned benchmark CI gate is **"the harness ran and produced a
number"**. Once the harness exists, soft gates should print
`::warning::` annotations when:

- `lag_snapshots_max > 0` over the 5-minute CI run, or
- `catalog_qps_avg > 5` (one consumer at default backoff).

Latency thresholds are recorded but not gated. The absolute target
(`p99 < 1s` for the `light` workload, per ADR 0011) only becomes a
hard CI gate after Phase 5 ratifies the production number on
representative hardware. The interim discipline is "no regression vs
previous run for this workload + commit-relative hardware label" — the
trajectory matters more than the absolute number until the absolute
number has been measured on representative hardware.
