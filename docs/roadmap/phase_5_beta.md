# Phase 5 — Beta hardening & release

**Goal:** ship `v0.1.0-beta.1` of the extension and both clients with enough operational polish that early adopters can run them in non-critical pipelines.

The previous Phase 5 was bloated to a year of work. It's been split. **Phase 5 (this document) is the engineering hardening + the launch.** The pieces that aren't engineering blockers — the Grafana / Prometheus assets, the upstream notification-hook proposal, the schema-as-code recipe write-up, the docs site's full subsection list — are tracked here for visibility but explicitly **not beta-launch gates**. They land continuously after `v0.1.0-beta.1` ships, on the cadence in the post-beta section below.

The five primitives, sugar wrappers, owner-token lease + single-reader enforcement (Phase 1), catalog matrix, schema-diff, DLQ with DDL-blocks-DML semantics, special-type handling, and the doctor command (all earlier phases) are done by the time we get here. This phase is about observability surface, distribution, docs that ship, proving stability under load, and saying the launch words.

## Beta-launch gates (the things that actually block `v0.1.0-beta.1`)

### Operability

- [ ] Structured logging from the extension (configurable via `SET ducklake_cdc_log_level = ...`). Hook into DuckLake's own logging surface where it exists (see the spec's [Logging](https://ducklake.select) section).
- [ ] Extend `cdc_consumer_stats()` (already shipped in Phase 1, audit-aware in Phase 2) with the additional fields needed for ops dashboards:
  - `compaction_gaps_total`, `schema_changes_total`, `wait_calls_total`, `wait_timeouts_total`, `dlq_pending`, `dlq_pending_ddl`, `dlq_pending_dml`, `dlq_total`
  - `lease_acquisitions_total`, `lease_force_acquires_total`, `lease_busy_rejections_total` — visibility into the pillar-6 enforcement path.
- [ ] Optional: surface `ducklake_snapshot_changes.author` (already in `snapshots()`) as a metrics label for multi-tenant lakes.

### Distribution

- [ ] Submit extension to the [DuckDB Community Extensions](https://www.duckdb.org/community_extensions/) repository.
- [ ] Verify `INSTALL ducklake_cdc FROM community;` works on Linux (x86_64, arm64), macOS (x86_64, arm64), Windows (x86_64).
- [ ] First public Python release on PyPI (`0.1.0b1`).
- [ ] First public Go release tagged (`clients/go/v0.1.0-beta.1`).
- [ ] Standalone Go binary published as a GitHub release artifact for the operator persona. Homebrew formula is post-launch (next section).
- [ ] **Visible-cadence release-tag discipline (demoted from Phase 1).** At least four release tags between the `v0.0.x` series and `v0.1.0-beta.1`, with honest "what works / what doesn't" notes for each. Phase 1 proved the release pipeline; Phase 5 is where the cadence discipline actually matters because (a) there is now real feature flow to tag against (catalog matrix in Phase 2, language clients in Phase 3 / 4), and (b) the project's first public users are reading release frequency as a liveness signal. The job is to demonstrate the discipline once, not to manufacture it before there is something to tag.

### Documentation that ships with beta

The full docs site is post-beta — it does not block. Beta ships with a minimum that makes the project usable.

- [ ] **README quickstart** — already shipped in Phase 1; refresh to reflect any v0.1.0-beta-only changes. Add the honest performance one-liner with link to `docs/performance.md`.
- [ ] **`docs/api.md`** — already shipped in Phase 0/1 with the primitive vs. sugar classifications and the filter composition matrix.
- [ ] **`docs/errors.md`** — every error code with example messages.
- [ ] **Operational docs refresh** — keep `docs/operational/lease.md`, `docs/operational/wait.md`, `docs/operational/audit-limitations.md`, and `docs/operational/inlined-data-edge-cases.md` current; add isolation and scaling guidance covering:
  - Recommended consumer counts per catalog backend (~50 without pooling, ~500 with PgBouncer; per ADR 0011).
  - **Multi-process catalog-load guidance.** `LakeWatcher` amortises N consumers in *one process* into one polling loop; it does **not** amortise across processes. Deployments with ≥10 processes each holding one consumer (separate Kubernetes pods, serverless functions, multi-tenant SaaS hosting one consumer per customer) get N independent polling loops and the active-consumer load is N× the single-process number. Recommended pattern: **share a `LakeWatcher` via a sidecar** — one process polls `current_snapshot()` and broadcasts to peer consumer processes via a local socket / shared FD / pub-sub channel; consumer processes subscribe instead of polling. If sidecar isn't available, accept the polling fan-out cost and budget for it (e.g. raise `cdc_wait` backoff cap). Without this guidance, users hit a self-inflicted catalog-load problem and blame the project; cross-process amortisation is a real engineering project we are not opening in v0.1, but the workaround is documentable in one section.
- [ ] **`docs/security.md`** — refresh threat model (drafted Phase 0); add SQL-injection review checklist for the lease-related new SQL paths.
- [ ] **`docs/python.md`, `clients/go/README.md`** — both already exist by this phase; just refresh against the final API.
- [ ] **`docs/performance.md` is a beta-launch gate.** This is the doc that converts curious senior engineers from "interesting" to "deployed." Sections:
  1. The four axes (latency, throughput, catalog load, per-consumer cost) with rough rules of thumb (lifted from `README.md` "Performance principles" → contracts).
  2. **Current numbers from `bench/results/` for all three workloads** (light / medium / heavy), including hardware label and commit SHA. This is an autogenerated section; the markdown points at `bench/README.md` for the trajectory chart.
  3. The "well-suited / poorly-suited" workload lists (lifted from `README.md`).
  4. **Tuning knobs:** `cdc_wait` backoff cap, `max_snapshots`, `change_types` filter for cheaper consumers, `lease_interval_seconds` for slower-batch sinks, `LakeWatcher` opt-out.
  5. **Scaling math:** "I have N consumers and M tables, what should I expect?" — formulas, not vague claims.
- [ ] **One "what's actually in beta" page** — a single page listing v0.1.0-beta scope vs. known limitations vs. v0.2 plans. Replaces a docs site for now.

### Quality gate

- [ ] **Heavy benchmark workload (`bench/heavy.yaml`) ratified in ADR 0011 lands in this phase.** Flat profile, 16.67 target snapshots/sec, 10 000 target rows/snapshot, 20 consumers, Kafka sink. Targets: p99 latency < 30s, throughput matches producer at steady state. Goes into the `bench/results/` history alongside light (since Phase 1) and medium (since Phase 2).
- [ ] **Ratify ADR 0011 design targets as production contract.** Every absolute number in ADR 0011 ("Performance model & benchmark workloads") was a design target through Phases 1-4 and a soft CI gate ("no regression vs previous run"). Phase 5, with three phases of measurement on representative hardware in `bench/results/`, converts the design targets into **hard CI gates from `v0.1.0-beta.1` forward**: any release that misses a ratified number fails the build. If the measured number for a workload is materially different from the design target, **update ADR 0011 with the measured number and a one-paragraph rationale** before flipping the gate — never silently soften a target by editing the ADR without explanation. This is the moment the project's published performance numbers stop being aspirational and start being a contract.
- [ ] **Soak test against `medium` workload:** run for 72h (chaos events injected — see below). No lag drift, no memory growth. **Plus a catalog-load assertion:** average catalog query rate over the 72h run stays under 50 QPS for the 5-consumer medium workload (the ADR 0011 active-consumer cost model says ~10-50 QPS per active consumer with `LakeWatcher` amortisation; this asserts the model holds in practice).
- [ ] **Sustained-load test against `heavy` workload:** run for 24h at producer-matching throughput. No lag drift, no memory growth. **Catalog-load assertion:** average catalog query rate stays under 100 QPS for the 20-consumer heavy workload.
- [ ] **Chaos test:** kill the consumer mid-batch, verify no duplicate commits, no dropped events. Repeat with kill mid-commit, mid-schema-change, mid-DDL-apply, mid-DML-write, mid-DLQ-write, mid-heartbeat.
- [ ] **Lease-loss chaos:** kill the lease-holder process; another process acquires after the heartbeat times out; the killed process's recovery script tries to commit and is correctly rejected with `CDC_BUSY`.
- [ ] **Multi-consumer test:** 10 consumers tailing the same lake with different `tables` and `event_categories` filters (mix of DDL-only, DML-only, and both); verify isolation and that lake-level discovery cost is shared (not multiplied).
- [ ] **Catalog-isolation test:** kill the Postgres catalog backend mid-poll; consumer recovers cleanly on restart.
- [ ] **Single-reader contention test:** 10 workers under one consumer name from 10 different connections; the lease holds; only one progresses; the others receive `CDC_BUSY` cleanly. (This is the lease, exercised hard. The mechanism shipped in Phase 1; this phase soak-tests it.)
- [ ] **DDL-storm test:** 100 ALTER statements in 60 seconds; verify `cdc_ddl` keeps up, schema-version-bounded windows do not stall, lag returns to baseline within `max_snapshots * (typical poll interval)`.
- [ ] **DDL-blocks-DML test under load:** intentionally fail an ALTER apply in a Phase 2 sink; verify the consumer halts DML for the affected table; operator acknowledges; consumer resumes; under the opt-out (`dml_blocked_by_failed_ddl := false`), DML proceeds and the expected DML DLQ flood is bounded by `max_dml_dlq_per_consumer` (per-consumer cap shipped in Phase 2 alongside the DLQ; default 1000 entries before the consumer halts entirely with `consumer_halt_dlq_overflow`). Test asserts both the bounded-flood case and the post-cap consumer-halt case.
- [ ] **Security review:** check for SQL-injection vectors in consumer-name / table-name handling, and in any new lease-related parameter binding. Refresh `docs/security.md` (drafted in Phase 0).
- [ ] **Doctor smoke test:** intentionally break things (drop a table referenced by a consumer; expire snapshots past a cursor; run a consumer past `expire_older_than`; cause DDL DLQ entries; let a lease time out); `ducklake-cdc doctor` (which has been usable since Phase 2) flags each one with the right recommendation.

### Launch words

- [ ] Announcement post (blog + DuckDB Discord + r/dataengineering + Hacker News).
- [ ] Issue templates, GitHub Discussions categories, triage rotation.
- [ ] Show-and-tell example: end-to-end demo repo wiring DuckLake → ducklake-cdc → Kafka → ClickHouse, runnable via `docker compose up`.

## Post-beta deliverables (not launch gates)

These were previously listed as Phase 5 work that "must" land before `v0.1.0-beta.1`. They don't. Pulling them out of the beta gate is what makes the gate achievable. Each lands on its own rhythm after launch; the project is still alive and visibly shipping.

### Docs site (months 1-3 post-beta)

The full docs site (mdBook or Docusaurus) lands as a sequence of post-beta releases. Beta itself ships with the README plus the markdown docs set above; the docs site collects them into a navigable form and adds the recipe pages over time.

- [ ] **What we are and what we are not** page: lead with the non-overlap principle. List DuckLake's own functions and where ours pick up.
- [ ] Concepts section: snapshots, change feed, the lake-vs-table iterator split, schema-version boundaries, outbox metadata, DLQ, the lease, audit log.
- [ ] API reference for all three artifacts, with each function classified as `primitive` / `observability` / `acknowledged sugar`.
- [ ] Recipes (one per page, see "Recipes" below).
- [ ] Operational guide: catalog backend selection, retention vs. CDC lag, encrypted lakes, multi-tenant deployments, single-reader enforcement (the lease in operational language), DDL-blocks-DML rationale.
- [ ] **Migration guide from hand-rolled patterns** (`docs/migration-from-handrolled.md`). Take `the prototype implementation` as the canonical "before"; show the line-by-line "after" using the extension and clients. Crucially: cursor-in-a-file users need explicit instructions to port their state.
- [ ] Architecture diagram showing extension ↔ catalog ↔ clients, with the discovery (`snapshots()`) / read (`table_changes`) split called out.
- [ ] Public roadmap from beta → 1.0.

### Recipes (months 1-6 post-beta)

Each recipe is a one-screen example that lives in `examples/recipes/<slug>/` *and* as a blog post. The write-up for each recipe drives the slow-burn SEO. Beta ships with **at least three** runnable recipes; the rest land continuously.

Beta-month recipes (block on launch):

- [ ] **Subscribe to deletes only on one table → log to stdout.**
- [ ] **Subscribe to all events on a schema → POST to a webhook.**
- [ ] **Backfill via time travel → write to a Parquet file.**

Months 1-3 post-beta:

- [ ] **`ducklake-cdc` → Discord / Slack webhook.** Lightweight notifier — every change in the lake posts to a channel. Useful as a debugging tool *and* as a "big production change" alarm.
- [ ] **`ducklake-cdc` → Redis cache invalidation.** Push deletes / updates to Redis on every relevant change.
- [ ] **`ducklake-cdc` → Meilisearch.** Index hydration; one of the most common "I just want a search index updated" paths.
- [ ] **Schema-as-code:** DDL-only consumer (`event_categories := ['ddl']`) → commit each schema change as a file in a git repo, with snapshot id as commit metadata. ~50 lines of Python. Falls out of `cdc_ddl` for free; **nobody else in the lakehouse space ships this — it's the differentiator that earns the Hacker News thread.** It is intentionally *not* a beta-launch gate (engineering done; the narrative write-up pays off in the launch month and the months after, not in the beta tag).

Months 3-6 post-beta:

- [ ] **`ducklake-cdc` → ClickHouse.** Analytics mirror.
- [ ] **Schema notifier:** DDL-only consumer → Slack with the diff inline.
- [ ] **Schema-registry sync:** DDL-only consumer → Confluent Schema Registry / Avro / Protobuf.
- [ ] **Vector store hydration:** DML consumer → embed `commit_extra_info.event` payloads → upsert into Qdrant / Pinecone / pgvector.

### Observability assets (stretch — Grafana / Prometheus, ship if time)

Operators will end up writing the same Grafana dashboard. Shipping it is high-value. It's stretch because if it slips, the project still ships — `cdc_consumer_stats()` is queryable as a SQL view and operators can build their own dashboards from that surface for the first month.

- [ ] **Grafana dashboard JSON** covering the full `cdc_consumer_stats()` surface — lag, throughput, gaps, schema changes, DLQ pending, lease busy-rejections.
- [ ] **Prometheus alert rules** for: lag > threshold, DLQ growth, DDL DLQ pending > 0, gap rate, schema-change rate (informational, not critical), wait timeouts > rate, lease force-acquire rate (operational concern).
- [ ] **"What does healthy look like"** doc page with screenshots of the dashboard under normal operation, mid-snapshot-burst, post-compaction.

### Upstream notification-hook proposal (month +3 post-beta)

Phase 0 deferred this. We previously planned to file at beta launch. **Filing later is better:** real users have run for months by then, the polling-load metrics are concrete (not handwaved), and the DuckLake maintainers are talking to a project with users, not a project with vapourware.

- [ ] **At month +3:** file the proposal as an issue / discussion in the DuckLake repo. Body covers:
  - The use case: `cdc_wait` in a CDC consumer wanting to react within milliseconds of a snapshot commit, not the 100ms-10s polling latency we ship today.
  - The current implementation: exponential-backoff polling against `current_snapshot()`. Catalog-load implications at scale, **with real numbers from the production users we have by then.**
  - The minimal proposal: a single hook fired on snapshot commit, signature `void on_snapshot_commit(catalog_id, snapshot_id, snapshot_time)`. Backends that support `LISTEN/NOTIFY` (Postgres) can additionally surface a notification channel.
  - **Reference our beta-period dashboards showing the polling load**, so this is data, not a hypothesis.
- [ ] If the proposal lands, plan the post-1.0 work to switch `cdc_wait` from polling to event-driven. If it doesn't, document the rationale and keep polling honest.

### Distribution-extras (continuous post-beta)

- [ ] Homebrew formula for the standalone `ducklake-cdc` Go binary.
- [ ] Pre-built Docker image of the operator CLI.
- [ ] OS package repos (deb / rpm) — month +3 if there's pull from users.

### Post-beta DDL plan (announced, not shipped)

Already documented in Phase 3 / 4 sink matrices; restated here for the public roadmap.

- **Postgres-mirror DDL handling (v0.2 plan).** `apply_ddl` translates each `cdc_ddl` event into target-side DDL: `created.table` → `CREATE TABLE ...`, `altered.table` → one or more `ALTER TABLE ... ADD/DROP/RENAME/SET TYPE ...` statements driven by the event's `details` payload, `dropped.table` → `DROP TABLE ...`. Hard to get right (type promotion edge cases, default-value translation, nested types).
- **Kafka DDL routing (v0.2 plan).** Optional separate `--ddl-topic` (or same topic with `event_type` field). Schema Registry integration.
- **Redis Streams DDL routing (v0.2 plan).** Separate stream per category by default.
- **Glob and tag-based table selection (v0.2 plan).** `tables := ['analytics.*']` and `tables_tagged := {'cdc.enabled': 'true'}`. Subtle because tables and tags themselves have versioned begin/end snapshots.
- **Leased windows for work-sharing (v1.0 plan).** `cdc_window_lease(consumer, worker_id, lease_ms)` for proper multi-worker fan-out. Note: distinct from the Phase-1 owner-token lease, which is single-reader enforcement; a Phase-1 lease holder reads serially or fans out within one connection. v1.0 leased windows enable multiple connections to share work explicitly.

## Exit criteria

- All three artifacts (extension, Python, Go) installable from their official channels with one command.
- Standalone `ducklake-cdc` Go binary distributed via GitHub releases.
- 72h soak and chaos tests pass on the medium workload (including DDL events and lease-loss chaos), with the catalog-load assertion green.
- 24h sustained-load test on the heavy workload green; throughput matches producer; catalog-load assertion green.
- All three benchmark workloads (light / medium / heavy) have published numbers in `bench/results/` and `docs/performance.md`.
- DDL-storm test green.
- DDL-blocks-DML behaviour green under load with both default and opt-out semantics.
- Single-reader contention test green at scale (the lease, soak-tested).
- Doctor command (which has been used since Phase 2) catches every failure mode in the chaos suite.
- At least one external user has run the README quickstart end-to-end and filed feedback. Schema-as-code recipe is **not** a beta-launch gate (it lands in the launch month, not the beta tag).
- "What's actually in beta" page published with v0.1 scope, known limitations, v0.2 plans.
- `docs/performance.md` published with the four-axis discussion, current numbers from all three workloads, well-suited / poorly-suited workload lists, tuning knobs, and the scaling math. **Beta-launch gate.**
- v0.2 / v1.0 plans visible in the public roadmap.
- `v0.1.0-beta.1` tagged, announced, and listed on the DuckDB Community Extensions page.

The Grafana dashboard, Prometheus alerts, full docs site, schema-as-code recipe, and upstream notification-hook proposal are **not** exit criteria for this phase. They land on the post-beta cadence above.
