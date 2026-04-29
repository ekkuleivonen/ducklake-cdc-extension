# ADR 0011 — Performance model + benchmark workloads

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

`docs/performance.md` commits the project to
four performance axes (latency, throughput, catalog load, per-consumer
cost) and to a "good fit / poor fit" workload contract. Those principles
are user-visible promises; without measurable workloads, the promises
are aspiration. Without ratified hard caps, users can self-DOS the
extension by passing degenerate arguments. This ADR ratifies both:

1. The **three benchmark workloads** (light, medium, heavy) that
   measure the four axes, and the publish-honest discipline that goes
   with them.
2. The **`max_snapshots` hard cap** that prevents
   `cdc_window(consumer, max_snapshots := 10_000_000)` from asking
   DuckLake to scan the entire lake history.
3. The **`max_dml_dlq_per_consumer` pre-approval** that lands alongside
   the Phase 2 DLQ feature and is recorded in ADR 0009 as a pre-approved
   addition to the consumer schema.

The discipline that distinguishes this ADR from a vendor's hero-numbers
deck: every absolute target is a **design target until benchmark history
ratifies it as a measured contract**. The clean-slate tree does not yet
commit a benchmark harness; when it lands, the CI gate is "ran
successfully + no regression vs previous run", not the absolute number.

Why: committing to absolute numbers in Phase 1 means the first
benchmark miss reads as a broken promise; ratifying in Phase 5 after
three phases of measurement reads as "here's what it actually does."
The same discipline applies to the prototype implementation's hot path:
it can sustain a busy producer in the small, but it was measured on one
machine in one configuration. That is a starting point, not a contract.

## Decision

### Status of the numbers in this ADR

Every absolute target below is a **design target** until benchmark
history ratifies it as a measured contract. A future harness should
publish the light workload first, then add medium across the catalog
matrix, then add heavy sustained-load runs. Until then, the CI gate is
"ran successfully + no regression vs previous run", not the absolute
target.

This is the `docs/performance.md` commitment to publish honest benchmark
numbers, not hero numbers, applied to the
targets themselves: a design target is a goal, a measured number is a
contract, and the project does not conflate the two.

### Latency

**Stated target on default polling backoff: p50 ~200ms, p99 ~1.5s.**

Aggressive polling (`SET ducklake_cdc_wait_max_interval_ms = 100`) gets
**p50 ~75ms / p99 ~250ms** at the cost of catalog QPS. Sub-100ms p50
requires the upstream notification hook (post-1.0) and is **not** a
v0.1 fight.

Document this position **loudly** — users coming from Debezium's
10-100ms latency expectations will otherwise be disappointed silently.
The README, `docs/performance.md`, and the Phase 5 launch post all
state the design target with the qualifier "default polling backoff".

### Throughput per consumer

Bounded by `table_changes` Parquet scan rate (~1-2M rows/sec/thread for
inserts on local NVMe, ~200MB/s on S3) and the sink's own write rate.
Realistic ranges (design targets, not measured contracts):

| Sink | Throughput |
| --- | --- |
| Stdout | ~100-500k rows/sec (Python overhead dominates; Go ~5-10× faster) |
| Webhook (sync, no batching) | ~1-5k rows/sec |
| Webhook (with batching) | ~10-50k rows/sec |
| Kafka | ~100k-1M rows/sec (batch + compression dependent) |
| Postgres mirror (COPY) | ~50-200k rows/sec |
| Postgres mirror (INSERT) | ~5-20k rows/sec |

The bottleneck is almost always the sink, not the extension or the read
path. The design guarantee that protects this is: cost is bounded by
changed tables, not lake size.

### Catalog load

Per-consumer steady-state cost (design targets):

- **Idle, default backoff:** ~0.1-0.2 catalog QPS (one `cdc_wait` poll
  every 5-10s once backoff has saturated).
- **Active, 10 batches/sec:** ~30-50 catalog QPS (window + commit +
  heartbeat).
- **`LakeWatcher`-amortised** (Phase 3 / 4): N consumers in one
  process share the snapshot-polling QPS. 50 consumers in one process
  ≈ 1 process's worth of `cdc_wait` cost, not 50.

### Per-consumer cost

Real bottleneck is **catalog connection count** — each consumer holds
one dedicated connection. Recommended limits:

- **~50 consumers per Postgres catalog** without connection pooling
  (Postgres' default `max_connections = 100`, leaving headroom for
  OLTP).
- **~500 consumers** with PgBouncer / pgcat in front of the catalog.
- **`LakeWatcher` amortisation is in-process only.** N consumers in
  *one* process share one polling loop. N consumers across N
  *separate* processes (one-per-pod, one-per-tenant SaaS, serverless)
  do **not** share — that's N× the polling load, no matter the
  per-process count. Cross-process amortisation is out of scope for
  v0.1.

Operational workaround for the cross-process case (documented in
`docs/performance.md`, Phase 1): ship a sidecar that polls
`current_snapshot()` and fans out the result to peer consumer
processes, or accept the fan-out cost and raise the `cdc_wait`
backoff cap. Stating this honestly in the ADR avoids the surprise
where a multi-pod deployment treats the in-process number as the
cross-process number.

### `max_snapshots` hard cap

`max_snapshots` is user-controllable on `cdc_window`. Without a hard
cap, a user can self-DOS by passing `max_snapshots := 10_000_000` and
asking DuckLake to scan the entire lake history. The cap:

- **Default `max_snapshots`:** 100. (Set by the
  `cdc_window(consumer, max_snapshots := 100)` signature default.)
- **Hard cap:** 1000, configurable per session via
  `SET ducklake_cdc_max_snapshots_hard_cap = ...`. Above the cap,
  `cdc_window` rejects the call with `CDC_MAX_SNAPSHOTS_EXCEEDED`
  (locked in `docs/errors.md`).
- **Tuning knob documented** in `docs/performance.md` and surfaced in
  the `cdc_consumer_stats()` view as "current cap" for operators who
  raise the session-wide cap and want to verify it took effect.

The cap composes with the schema-version bounding (ADR 0006): if the
schema-bounded `end_snapshot` is closer than `start + max_snapshots`,
the schema bound wins; the `max_snapshots` cap only applies when no
relevant schema change exists in the requested range.

### `cdc_wait` timeout hard cap

`cdc_wait(consumer, timeout_ms := 30000)` returns the new
`current_snapshot()` or NULL on timeout. Without a hard cap, a user can
hold a connection open for hours waiting for a poll that never
satisfies. The cap:

- **Hard cap:** 5 minutes (300_000 ms), configurable per session via
  `SET ducklake_cdc_wait_max_timeout_ms = ...`. Larger values are
  **clamped with a `CDC_WAIT_TIMEOUT_CLAMPED` notice** (not an error)
  so the call still returns a value the caller can act on; the
  notice carries the requested and applied timeouts so the operator
  can tell what happened.

### `max_dml_dlq_per_consumer` (Phase 2 pre-approval)

ADR 0009 § "Pre-approved Phase 2 addition" promises:
`max_dml_dlq_per_consumer INTEGER NOT NULL DEFAULT 1000` lands on the
consumer row alongside the Phase 2 DLQ feature.

This ADR ratifies the **default 1000** as part of the performance
model: the DLQ opt-out (`dml_blocked_by_failed_ddl := false`) is unsafe
without a per-consumer cap, and shipping the opt-out three phases
ahead of the cap is the kind of split that produces real users with
100k+ DLQ rows and a doctor command that takes 10s to render. The
default-1000 satisfies the locking-discipline requirement (safe
default, no migration break for Phase 1 consumers) and the operational
requirement (1000 rows is enough to debug a steady-state failure mode
without overwhelming the table).

When the cap is hit, the consumer halts via the
`consumer_halt_dlq_overflow` audit action (ADR 0010); operators
resolve via `cdc_dlq_clear` / `cdc_dlq_replay` / `cdc_dlq_acknowledge`
(Phase 2).

### Three planned benchmark workloads

| Workload | Snapshots/min | Rows/snapshot | Consumers | Sinks | Latency p99 design target | Throughput design target |
| --- | --- | --- | --- | --- | --- | --- |
| **light** | 30 | 100 | 1 | stdout | < 1s | matches producer over a 60s smoke run |
| **medium** | 100 | 1 000 | 5 | mix (webhook + Kafka) | < 5s | matches producer at steady state |
| **heavy** | 1 000 | 10 000 | 20 | Kafka | < 30s | matches producer at steady state |

Each workload should be committed as a YAML descriptor when the benchmark
harness lands. The descriptor is the source of truth: the harness reads
it, the README links it, and release notes embed the most recent
measurement.

The intended rollout is a short, constant-rate `light` smoke benchmark
first, medium across the catalog matrix next, then heavy, variable-load
profiles, and sustained-load runs once the implementation has enough
history for the numbers to be credible. The runner should take duration,
snapshot rate, rows per snapshot, consumer count, and `max_snapshots` as
parameters from the workload descriptor so later phases can add richer
load profiles without replacing the harness.

The mature CI shape is different from Phase 1's local rebuild: run the
benchmark **after** the extension distribution matrix has produced its
platform artifacts, and benchmark those exact artifacts. The likely
default gate is a 5-minute `medium` run on every platform the matrix
builds, across the supported catalog backends available on that runner.
That may be sufficient as the regular CI performance signal; longer soak,
heavy, and variable-load runs can stay scheduled or release-gated.

### Honest publication policy

Numbers go in `docs/performance.md` with **date + commit SHA +
hardware label** (one row per measurement). Numbers are **not**
removed when they regress — they are explained in a `notes` column
that links to the issue or commit causing the regression.

**Numbers without history are flat; numbers with history are a story.**
A regression that's documented and explained reads as
"the project is paying attention"; a regression that's quietly removed
reads as "the project is hiding things." The latter is the failure
mode this policy prevents.

The same discipline applies to the design targets above: when the
Phase 5 measurement shows that `medium` p99 is 7s instead of the design
target's 5s, the doc updates the **published target** (not the design
target) and the README's "what works" line says "5-10s p99 for medium
workload". The design target stays in this ADR as historical context;
the doc says what the project actually delivers.

### Workloads we are good at / poorly suited to

`docs/performance.md` states the poor-fit workloads. This ADR ratifies
the good-fit / poor-fit split as a contract; reviewers reject features
that imply a poor-fit workload is something the project plans to be good
at. The contract:

**Good fit:**

- Reverse ETL and analytics fan-out (~100-10k rows/sec, latency budget
  seconds-to-minutes). Sweet spot.
- Search index hydration (~1-100k rows/sec, latency budget seconds).
- Audit logging and compliance (any throughput, latency budget hours).
  Excellent fit; the DLQ design + DDL events make it nearly trivial.
- Schema-as-code workflows (tens of DDL events/day, latency budget
  minutes). Genuinely differentiated.
- Cache invalidation, single-region (~100-10k events/sec, sub-second
  wanted). Marginal on default polling; good with aggressive polling;
  ideal once the upstream notification hook lands.
- Small-to-medium ML feature stores (daily/hourly retraining loops).

**Poor fit (loud about it):**

- Sub-10ms latency / HFT.
- Massive fan-out (10k+ consumers per lake; soft cap ~500).
- OLTP CDC.
- Sub-second cross-region cache invalidation.
- Workloads with extreme write skew + many consumers on the hot table
  (no shared-read coalescing in v0.1).

## Consequences

- **Phase impact.**
  - **Phase 1** ships:
    - The benchmark design and publication policy. The harness itself
      lives in `bench/runner.py` and runs the `bench/light.yaml` smoke
      workload in Full CI.
    - The `max_snapshots` hard cap (default 100, hard cap 1000)
      enforced inside `cdc_window` with `CDC_MAX_SNAPSHOTS_EXCEEDED`
      raised on violation.
    - The `cdc_wait` timeout hard cap (5min) enforced with
      `CDC_WAIT_TIMEOUT_CLAMPED` notice on clamp.
    - `docs/performance.md` scaffold with empty measurement table and
      the "honest publication policy" front-matter.
    - `docs/performance.md` with the cross-process polling
      cost discussion.
  - **Later phases** add benchmark descriptors for medium / heavy, run
    them across the catalog matrix, publish production numbers in
    `docs/performance.md`, and only then consider hard CI gates.
  - **Phase 3 / Phase 4 bindings** are exempt from the benchmark
    targets (they're about the extension surface, not the binding-side
    `tail()`-loop overhead). Bindings document their own per-binding
    overhead when those packages land.
- **Reversibility.**
  - **One-way doors:** the `max_snapshots` default (100) and hard cap
    (1000) — users have built dashboards around the default; raising
    the default is a minor bump but not a no-op; lowering it would
    break consumers that rely on larger windows; raising the hard cap
    is benign; lowering it is a major-version bump because it
    invalidates `SET ducklake_cdc_max_snapshots_hard_cap = N` calls
    that previously succeeded.
  - **Two-way doors:** the design-target numbers themselves; the
    workload definitions; the publication-policy specifics (date + SHA
    + hardware is the minimum, can be expanded).
  - **Strictly forbidden:** removing a number from
    `docs/performance.md` because it regressed. The policy exists
    precisely to make this temptation impossible to act on without a
    visible PR review.
- **Open questions deferred.**
  - **Per-table read coalescing.** N consumers on one hot table
    currently re-read the same Parquet files. v1.0+ optimisation;
    listed as a "poor fit" in the workload contract above so users
    know v0.1 doesn't solve it.
  - **Cross-process snapshot polling amortisation.** Currently
    in-process only via `LakeWatcher`. Cross-process needs the
    upstream notification hook (post-1.0) or a project-shipped sidecar;
    the latter is documented as a workaround in
    `docs/performance.md` but is not a v0.1 deliverable.
  - **GPU / SIMD acceleration of the row-event JSON serialisation.**
    Out of scope for v0.1 / v1.0 — the bottleneck is the sink, not
    the serialiser.
  - **Per-sink benchmark fingerprints.** A future version may publish
    per-sink benchmark numbers (Postgres-mirror p99, Kafka p99,
    webhook p99) instead of just aggregate workload numbers. Phase 5
    decides based on what users ask for in pre-launch issues.

## Alternatives considered

- **Publish benchmark numbers as hard CI gates from Phase 1.**
  Rejected for the design-target-vs-measured-contract reason in
  Context. The first benchmark run in a green-field harness on a
  fresh CI worker is going to be wrong by some factor; gating on it
  produces noise and resets the project's credibility on every
  unrelated CI flake.
- **No `max_snapshots` hard cap — trust the user.** Rejected: trivial
  self-DOS surface (`max_snapshots := 10_000_000` against a lake with
  millions of historical snapshots reads megabytes of catalog state
  in one transaction). The cap is one integer and a notice; the
  cost-benefit is overwhelmingly in favour of the cap.
- **Hard cap = 100 (the default).** Rejected: too restrictive for
  back-fill scenarios (consumers catching up after a long pause want
  to advance hundreds of snapshots per call). 1000 is comfortably
  above any reasonable steady-state and cheap to raise per-session
  for legitimate batch workloads.
- **`max_dml_dlq_per_consumer` ships in Phase 5 alongside the doctor
  command.** Rejected: the DLQ feature lands in Phase 2; the cap
  must land with the feature, not three phases later. ADR 0009's
  pre-approval is the locking discipline for "this column lands in
  Phase 2 with this default and this name"; this ADR ratifies the
  number.
- **Use percentiles other than p50 / p99.** Considered (p95, p999).
  Rejected for v0.1: p99 is the right granularity for the workloads
  the project targets (sub-second to seconds; pillar 1 of the
  performance principles). p999 starts to matter at sub-100ms scales
  the project explicitly does not promise.
- **Define the workloads in code (Python harness with magic
  constants) instead of YAML descriptors.** Rejected: YAML descriptors
  are reviewable in PR diffs without running code; bindings (Phase 3 /
  4) can read them too if they ever ship binding-side perf tests; the
  workload definition is design data, not implementation.

## References

- `docs/performance.md` — the four-axis performance model and the
  good-fit / poor-fit contract.
- ADR 0006 — Schema-version boundaries (the bound that composes with
  `max_snapshots` inside `cdc_window`).
- ADR 0007 — Concurrency model (the lease cost is one of the
  per-consumer cost components measured by `medium` and `heavy`).
- ADR 0009 — Consumer-state schema (the
  `max_dml_dlq_per_consumer` pre-approval is recorded there;
  this ADR ratifies the default).
- ADR 0010 — Audit log (the `consumer_halt_dlq_overflow` action that
  fires when the cap is hit).
- `docs/performance.md` (scaffold in Phase 1) — the published numbers
  that this ADR's policies govern.
- `docs/performance.md` (Phase 1) — the cross-process polling
  workaround.
- `docs/errors.md` — `CDC_MAX_SNAPSHOTS_EXCEEDED`,
  `CDC_WAIT_TIMEOUT_CLAMPED`.
