# Roadmap

Path from concept to first public beta. Grounded in the [DuckLake v1.0 spec](https://ducklake.select/docs/stable/specification/introduction).

| Phase | Goal | Status |
| --- | --- | --- |
| [0 — Discovery & Spike](./phase_0_discovery.md) | Audit overlap with DuckLake, freeze the (small) primitive surface, lock concurrency / schema-boundary / threat-model / consumer-state-schema / audit-log decisions, decide language and license | **complete** (closed 2026-04-28; eleven ADRs merged, API frozen, all exit criteria satisfied) |
| [1 — Extension MVP](./phase_1_extension_mvp.md) | Primitives + `cdc_consumer_*` lifecycle + **owner-token lease for single-reader enforcement** + the SQL walkthrough + the matching README quickstart, against the embedded DuckDB catalog | **in progress** (core extension, docs, tests, CI, README screenshot walkthrough, examples, and benchmark smoke harness are present; release artifacts remain open) |
| [2 — Catalog matrix, DDL coverage, DLQ, doctor](./phase_2_backend_agnostic.md) | Identical semantics on DuckDB / SQLite / Postgres catalogs (DuckLake's three supported metadata backends); `cdc_schema_diff` exhaustively tested; DLQ with **DDL-blocks-DML semantics**; `variant` / `geometry` serialization conventions; **doctor command** (moved up from Phase 5 so it's usable during testing) | not started |
| [3 — Python client](./phase_3_python_client.md) | `ducklake-cdc` PyPI package: composes our primitives with DuckLake builtins; iterator + Sink protocol + reference sinks | not started |
| [4 — Go client](./phase_4_go_client.md) | `ducklake-cdc/go` module mirroring the Python surface | not started |
| [5 — Beta hardening & release](./phase_5_beta.md) | Observability, packaging, soak / chaos tests, public `v0.1.0-beta.1` release. **Grafana / Prometheus assets, full docs site, schema-as-code recipe, and the upstream notification-hook proposal are post-beta deliverables, not launch gates.** | not started |

Each phase has explicit exit criteria. A phase is "done" only when every checkbox is ticked.

## Design pillars

These are non-negotiable. Every PR is reviewed against them.

1. **Non-overlap with DuckLake.** Anything DuckLake already does, we use directly — `snapshots()`, `current_snapshot()`, `last_committed_snapshot()`, `table_changes`, `set_commit_message`, time travel, conflict resolution, compaction maintenance. We add only what's missing: cursors, gap detection, long-poll, schema-diff computation, observability, sink-side serialization conventions. Every API addition has to justify itself against this list.
2. **Discovery is via `snapshots()`, reads are via `table_changes`.** A consumer poll learns what moved by reading `snapshots()` (already returns `changes` as a structured map) for the bounded window we computed. Then it drills into specific tables via `table_changes` so we inherit inlined-data, encryption and schema-evolution handling for free.
3. **Cursors and consumer state live in the same catalog.** Stored as regular DuckLake tables, committed in the same transaction as reads. No external state store, no drift, transactionally consistent regardless of which catalog backend is in use. Conflict resolution between concurrent consumers is inherited from DuckLake's own snapshot-id arbitration.
4. **Read and commit are always separate operations.** `cdc_window` returns bounds and never advances the cursor. `cdc_commit` advances it. Read-and-advance in one call destroys at-least-once semantics; we never do it. The acknowledged sugar wrappers (`cdc_events`, `cdc_changes`) inherit this — they don't auto-commit either.
5. **`cdc_window` is idempotent until commit, on the holding connection.** Calling it again from the same connection that opened the window returns the same `(start, end, schema_version, schema_changes_pending)` row. Once a window is open, **one connection drives the cursor**; that single orchestrator connection may itself fan out `cdc_changes` / `table_changes` reads to a worker pool because the underlying `table_changes` reads are ordinary stateless `SELECT`s and the cursor only advances on `cdc_commit`. Concurrent calls to `cdc_window` from a *different* connection under the same consumer name are rejected with `CDC_BUSY` per pillar 6. This combination — same-connection idempotence + reject-other-connection — is what makes interactive SQL-CLI exploration safe and parallel-read fan-out from one orchestrator easy, without quietly enabling work-sharing.
6. **One consumer name = one logical reader.** Work-sharing across multiple workers under the same consumer name is explicitly unsupported in v0.1, and the ownership lease (pillar 5) actively rejects it. See the "Concurrency model" section in Phase 0.
7. **Compaction is a partner, not an enemy.** Gap detection is an existence check on `ducklake_snapshot`; recovery is explicit (`cdc_consumer_reset(to_snapshot := 'oldest_available')`), never silent. The error message tells the operator exactly what command to type.
8. **Schema changes are window boundaries by default, not surprises.** When `stop_at_schema_change` is set on the consumer (default `true`), `cdc_window` never returns a window that crosses a `schema_version` boundary. The window result includes `schema_version` and a `schema_changes_pending` flag. Reactive (after-the-fact) schema-diff handling is unsafe because `table_changes` already reconciled the data to the end-snapshot schema. Power users who can tolerate cross-schema batches can opt out per-consumer.
9. **DDL is a first-class event stream, not a sidecar.** `ducklake_snapshot_changes.changes_made` already separates DDL (`created_*`, `altered_*`, `dropped_*`) from DML (`inserted_into_*`, `deleted_from_*`) and from maintenance (`compacted_*`). Per spec, RENAME folds into `altered_*` (no separate `renamed_*` keys). Our `cdc_ddl(consumer)` primitive surfaces typed DDL events alongside `cdc_changes(consumer, table)` for DML. Within a single snapshot, **DDL events are delivered before DML events**; across snapshots, snapshot-id ordering is preserved. **Two-stage extraction:** `cdc_ddl` reads `snapshots().changes` to discover *which* objects changed in each snapshot, then queries the versioned catalog tables (`ducklake_table`, `ducklake_column`, `ducklake_view`, `ducklake_schema`) using their `begin_snapshot` / `end_snapshot` ranges to reconstruct the typed `details` payload (added/dropped/renamed columns, type promotions, default changes, view-definition diffs, table renames). Stage 1 lives over `snapshots()`; stage 2 lives over the catalog version-range tables. `cdc_schema_diff` reuses the same stage-2 extractor. Pillar 9 is therefore **not** "one query against `ducklake_snapshot_changes`"; it is "discover via `snapshots()`, reconstruct via the versioned catalog."
10. **Serializers live in the language clients, not the extension.** The extension exposes raw DuckLake values. Variant→JSON, geometry→GeoJSON, blob→base64 etc. happen in Python and Go. This keeps the C++/Rust extension small and avoids forcing it to depend on JSON / GeoJSON / etc. libraries.
11. **All four change types are available; filtering is consumer policy.** `insert`, `update_preimage`, `update_postimage`, `delete` are all available to consumers. Default consumer config returns all four. Operators can scope a consumer to a subset via the `change_types` argument to `cdc_consumer_create` (a per-consumer policy, recorded in `__ducklake_cdc_consumers` and applied as a `WHERE` clause inside the sugar wrappers); further filtering is the user's own `WHERE`. The extension never invents implicit filters that aren't either (a) declared at consumer-create time, or (b) written by the caller.
12. **The extension is the canonical surface.** Its reason to exist is not magic in `cdc_wait` (which is honest exponential-backoff polling) — it's that one SQL surface works identically from the `duckdb` CLI, Python, Go, R, Rust, Java, Node, and any future binding without re-implementation. **This commits the project to a multi-language future:** if only Python and Go ever ship, the extension is overhead the project doesn't need. The extension wins when binding #3 is contemplated and the cost of "do the same thing in C++ that's already in two other languages" hits. ADR 0005 (in Phase 0) carries the long version of this reasoning and now supersedes the older "extension wins on `cdc_wait`" framing.

## Performance principles

Sibling section to design pillars. Same status: every PR is reviewed against these. They define what users can rely on, *and* what they shouldn't.

The system is reasoned about along four axes. They are mostly independent and the architecture has different bottlenecks on each:

1. **End-to-end latency** — producer commit to sink delivery. Dominated by `cdc_wait` poll interval. Steady-state target on the default backoff is **200ms p50, ~1.5s p99**; sub-100ms p50 requires the upstream notification hook (post-1.0) and is explicitly *not* a v0.1 fight. Honest published target: **sub-second to seconds**, not Debezium-comparable milliseconds.
2. **Throughput** — events/sec a single consumer can sustain. Bounded by `table_changes` Parquet scan rate and the sink's own write rate. Realistic per-consumer ranges: **~100k-1M rows/sec** for stdout/Kafka with batching, **~50-200k rows/sec** for Postgres mirror via COPY, **~1-50k rows/sec** for sync webhook (HTTP-roundtrip-bound). The bottleneck is almost always the sink, not us.
3. **Catalog load** — pressure on the Postgres / SQLite / DuckDB catalog from polling. Idle consumers are designed to be cheap (the `cdc_wait` backoff caps at 10s by default; 50 idle consumers cost ~5 catalog QPS, not 50). Active consumers cost ~10-50 QPS each. Multi-consumer deployments inside one process **share** snapshot-polling via `LakeWatcher` (Phase 3 / 4) — N consumers in one process do not produce N times the catalog load.
4. **Per-consumer cost** — what each additional consumer costs in catalog connections, rows, query rate. Real bottleneck is **catalog connection count**: each consumer holds one dedicated connection for `cdc_wait`. With Postgres' default `max_connections=100`, this caps at roughly **50 consumers per Postgres catalog**, scaling to **~500 with PgBouncer / connection pooling**. Documented operational guidance, not a silent cliff.

The contracts these axes imply, stated as principles:

1. **Polling intervals default to favouring catalog load over latency.** `cdc_wait` backs off from 100ms to 10s on idle. Users who want sub-second freshness opt in by lowering `ducklake_cdc_wait_max_interval_ms`; users who don't care get cheap idle consumers by default.
2. **Cost is bounded by changed tables, not lake size.** A consumer scoped to `tables := ['orders']` pays per-poll cost proportional to `orders` activity, not the lake's total table count. `table_changes` already enforces this; we do not regress it.
3. **`max_snapshots` is hard-capped.** Default 100, hard cap 1000 (configurable to a higher value per session via `SET ducklake_cdc_max_snapshots_hard_cap = ...`, but never silently). Stops users self-DOSing on `max_snapshots := 1000000` reads of the entire lake history. Phase 0 ratifies the cap.
4. **Multiple consumers in one process share snapshot polling via `LakeWatcher`.** One goroutine / async task per `(catalog_uri)` polls `current_snapshot()` and broadcasts to local consumers. N consumers ≠ N times the polling load. Phase 3 / 4 client-side optimisation; the extension surface is unchanged.
5. **The compaction contract bounds maximum lag, and that's the operator's choice.** `expire_older_than` has to exceed worst-case consumer lag. We surface lag in `cdc_consumer_stats()`; we do not negotiate with `ducklake_expire_snapshots`. Pillar 7 is the design-side companion to this.
6. **We publish honest benchmark numbers, not hero numbers.** Three benchmark workloads (light / medium / heavy, ratified in Phase 0) are the intended release measurement set. Phase 1 runs the short `light` smoke benchmark in Full CI; `docs/performance.md` records the targets and publication policy while longer histories accumulate.

### Workloads we are good at, workloads we are not

Stated explicitly so users can self-select in 30 seconds. Saying no to bad-fit workloads earns more credibility than overpromising.

**Good fit:**

- **Reverse ETL and analytics fan-out.** ~100-10k rows/sec, latency budget seconds-to-minutes. Sweet spot. Replaces SaaS pipelines.
- **Search index hydration** (Meilisearch, Typesense, Elasticsearch). ~1-100k rows/sec, latency budget seconds.
- **Audit logging and compliance.** Throughput is whatever the producer does; latency budget hours. Excellent fit; the DLQ design + DDL events make it nearly trivial.
- **Schema-as-code workflows.** Tens of DDL events/day; latency budget minutes. Genuinely differentiated — nobody else ships this.
- **Cache invalidation, single-region.** ~100-10k events/sec, sub-second wanted. Marginal on default polling; good with aggressive polling; ideal once the upstream notification hook lands.
- **Small-to-medium ML feature stores.** Daily/hourly retraining loops fit cleanly; online-serving sub-second is marginal.

**Poor fit (loud about it):**

- **Sub-10ms latency / HFT.** Polling caps at ~100ms p50 best case. Users who need single-digit ms need Kafka.
- **Massive fan-out (10k+ consumers per lake).** Operational weight (cursors, DLQ partitions, metric labels) breaks well before the architecture does. Soft cap: ~500 consumers per lake.
- **OLTP CDC.** "Postgres → Kafka" is Debezium's. We do "DuckLake → Kafka." Composing `pg_duckpipe` + `ducklake-cdc` adds DuckLake-commit latency to every event — unacceptable for OLTP fan-out.
- **Sub-second cross-region cache invalidation.** Latency floor is poll interval + catalog round-trip; cross-region adds 100ms+ network. Use pub-sub.
- **Workloads with extreme write skew + many consumers on the hot table.** No shared-read coalescing in v0.1; 50 consumers on one hot table all read the same Parquet repeatedly. Per-table read coalescing is a v1.0+ optimisation.

## Non-goals (v0.1)

We will get asked for these. Saying no early is cheaper than removing them later.

- **Work-sharing across multiple workers under one consumer name.** One name = one reader. The escape hatch is "create N consumers with disjoint table filters." Leased windows (`cdc_window_lease(consumer, worker, lease_ms)`) is a v1.0 design exercise.
- **Server-side row predicates.** No `cdc_filter_predicate := 'amount > 1000'` parameter. Users write `WHERE amount > 1000` in their own SQL. DuckDB's predicate engine is the predicate engine.
- **Glob / tag-based table selection.** `tables := ['analytics.*']` and `tables_tagged := {'cdc.enabled': 'true'}` are v0.2. v0.1 takes a hard list of fully-qualified table names. Glob / tag semantics over a versioned catalog have subtle edge cases (tables / tags themselves have begin / end snapshot ranges) that aren't worth solving before we know users want them.
- **Multi-sink fan-out per consumer.** "Send to Kafka and Postgres from one consumer" is two consumers with two cursors in v0.1. Atomicity across multiple sink writes is a real distributed-systems problem we are not opening in MVP.
- **Auto-commit-on-sink-success as the default.** Reference sinks support it as opt-in (so users get the safe behaviour by default in `tail()`-style sugar) but the lead-line API in every client is the explicit iterator + commit pattern. Users with debugging needs must be able to see and control the cursor.
- **Replacing DuckLake time travel.** `AT (VERSION => N)` is DuckLake's. We expose a `cdc_replay` CLI command in Phase 5 as a thin wrapper, but it's `table_changes` with bounds — we don't reinvent it.
- **Notifications from the catalog.** `cdc_wait` polls. The right long-term fix is an upstream DuckLake hook on snapshot commit; we'll file that issue in Phase 5 once we have a working demo to motivate it. v0.1 ships honest polling.
- **DDL handling for non-trivial reference sinks.** v0.1 reference sinks for Postgres-mirror, Kafka, and Redis are **DML-only**. They surface DDL events but do not translate them into target-side DDL. Doing it correctly per sink is months of work and a v0.1 should not block on it. Users who need DDL→Postgres in v0.1 implement `Sink.apply_ddl` against the documented protocol. Stdout, file, and webhook reference sinks pass DDL through trivially as JSONL with an `event_type` discriminator.
