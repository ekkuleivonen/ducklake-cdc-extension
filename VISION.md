# Vision — what `ducklake-cdc` intends to become

> **This is the destination, not the current state.** For what works
> today, read [`README.md`](./README.md) — the SQL extension is real but
> single-backend, there is no language client, no published binary, and
> no reference sink. This document describes the target shape of the
> project at v0.1.0-beta.1 (end of Phase 5 in
> [`docs/roadmap/`](./docs/roadmap/)).

**Stream changes from your DuckLake — to webhook, Postgres, Redis,
Kafka, your own code, anywhere. No JVM. No coordinator. No SaaS bill.**

Postgres has logical replication. Iceberg has table-changes.
[DuckLake](https://ducklake.select) has both — `ducklake-cdc` aims to
give you the daemon to consume them, with a SQL surface that works
identically from the `duckdb` CLI, Python, Go, and any future binding.

## Use cases the project is designed for

- **Outbox without dual-writes.** Apps call
  `set_commit_message(extra_info => '{"event":"OrderPlaced"}')`;
  downstream services tail the lake and route on `commit_extra_info`.
  The dual-writes-to-Kafka problem you've been working around — gone.
- **Reverse ETL.** Push row-level deltas to Kafka, NATS, Redis Streams,
  search indexes, vector stores, SaaS APIs. The Hightouch / Census
  shape, on top of a lakehouse you already have.
- **Cache invalidation.** Fan out updates to CDNs and application caches
  with at-least-once delivery.
- **ML feature stores.** Stream feature updates from the offline
  lakehouse to online serving stores.
- **Audit & compliance.** A durable, snapshot-precise change log
  decoupled from the producer.

## Schema-as-code (the differentiator)

Most CDC systems hand you DDL events as an afterthought. This project
promotes them to first-class. A consumer subscribed with
`event_categories := ['ddl']` will see a typed event for every `CREATE`
/ `ALTER` / `DROP` in the lake — column adds, column drops, type
promotions, table renames, view-definition changes — with a structured
`details` payload that downstream sinks can act on.

The intended Python recipe (Phase 3, **does not exist yet**):

```python
import ducklake_cdc, subprocess, json

with ducklake_cdc.consumer("ducklake:lake.duckdb", name="schema-watcher",
                            event_categories=["ddl"]) as c:
    for batch in c.poll():
        for ddl in batch.ddl:
            path = f"schema/{ddl.schema_name}/{ddl.object_name}.json"
            with open(path, "w") as f:
                json.dump({"snapshot_id": ddl.snapshot_id,
                           "event_kind": ddl.event_kind,
                           "details": ddl.details}, f, indent=2)
            subprocess.run(["git", "add", path])
            subprocess.run(["git", "commit", "-m",
                            f"{ddl.event_kind} {ddl.object_name} @ snapshot {ddl.snapshot_id}"])
        c.commit(batch.snapshot_id)
```

Fifty lines of Python and your lake's schema history is a git repo.
Same primitive feeds Slack notifications when columns drop, Confluent
Schema Registry sync, or a "who changed what" audit trail. No Spark
cluster. No Databricks bill.

The *typed DDL primitive* underneath this recipe (`cdc_ddl`) is shipped
today — what is missing is the Python wrapper that makes it ergonomic.

## How this project relates to existing tools

`ducklake-cdc` is the **consumer** side of a DuckLake — complementary to
producer-side tools like `pg_duckpipe` (Postgres → DuckLake), and
orthogonal to OLTP CDC (Debezium's home turf). If you already have data
landing in a DuckLake, this is what reads it out as a change stream.

## What `ducklake-cdc` adds vs DuckLake itself

| What `ducklake-cdc` adds | Why DuckLake doesn't have it | Status |
| --- | --- | --- |
| Persistent consumer state (named cursors stored as a regular DuckLake table) | DuckLake has snapshots, but no "subscriber" concept | shipped (Phase 1) |
| `cdc_window(consumer)` — atomic cursor read + gap-detected snapshot range, schema-version-bounded, single-reader-enforced via an owner-token lease | Has to be combined with a state table that DuckLake doesn't ship with | shipped (Phase 1) |
| `cdc_wait(consumer, timeout)` — long-poll until a new snapshot appears | DuckLake exposes `current_snapshot()` but no blocking subscribe | shipped (Phase 1) |
| `cdc_ddl(consumer)` — typed DDL event stream | `ducklake_snapshot_changes.changes_made` separates DDL from DML at the catalog level, but no typed surface exists | shipped (Phase 1) |
| `cdc_consumer_stats()` — lag (snapshots + seconds), throughput, gap counters, DLQ pending | Pure-SQL is possible but unstandardised | partial — cursor / lease columns shipped (Phase 1); DLQ counters land in Phase 2 |
| Wire-format conventions for `variant` and `geometry` (JSON, GeoJSON) | DuckLake stores them; downstream sinks need a contract | planned (Phase 2) |
| Reference sinks (stdout, file, webhook, Kafka, Redis Streams, Postgres mirror) with retry, backoff, idempotency keys, and a DLQ | DuckLake stops at the read API | planned (Phases 3–5) |
| Thin Python and Go clients that wrap these primitives and compose them with DuckLake's own functions | — | planned (Phases 3 and 4) |
| `doctor` command — meta-diagnostic for catalog reachability, consumer lag, DLQ pending, lease state, gap risk | — | planned (Phase 2) |

## What this project explicitly does *not* reinvent

This is a small project on purpose. Anything DuckLake already does, the
extension uses directly. The non-overlap principle is what keeps it
shippable.

- **`table_changes` / `table_insertions` / `table_deletions`** already
  give you row-level inserts, update pre/post-images and deletes, with
  snapshot-id *or* timestamp bounds. Inlined data, encrypted files,
  partial files and post-compaction reads are handled transparently.
  These are used directly.
- **`snapshots()`** already returns the per-snapshot change manifest as
  a structured map plus `author`, `commit_message`, `commit_extra_info`.
  Multi-table change discovery is one `SELECT` away. Not shadowed.
- **Time travel** (`AT (VERSION => ...)` / `AT (TIMESTAMP => ...)` /
  `ATTACH ... (SNAPSHOT_VERSION ...)`) is built in. Snapshot ids and
  timestamps are passed through; no separate "replay" feature beyond
  thin glue in the sink or client.
- **Schema evolution** on the read path is built in — `table_changes`
  reads using the schema as of the end snapshot, with default values
  for added columns. Typed *DDL events* are added on top for downstream
  sinks; the read semantics are not reimplemented.
- **Outbox metadata** is already first-class on the producer side via
  `CALL <lake>.set_commit_message('author', 'msg', extra_info => '{...}')`.
  It is surfaced on every batch and a JSON convention is recommended
  for `extra_info`.
- **Compaction maintenance** (`ducklake_expire_snapshots`,
  `ducklake_merge_adjacent_files`, ...) is DuckLake's job. The
  extension's role is detecting when compaction expired a consumer's
  cursor and raising a structured `CDC_GAP` with the recovery command
  in the error message.

## Backend agnostic by construction

The extension talks to DuckLake; DuckLake talks to your catalog.
**DuckDB, SQLite, and PostgreSQL** catalogs are all targets — DuckLake's
three supported metadata backends. Storage can be anything DuckDB can
read.

> **Today only the embedded DuckDB catalog has been validated.** SQLite
> and PostgreSQL coverage is the work item of Phase 2 — see
> [`docs/roadmap/phase_2_backend_agnostic.md`](./docs/roadmap/phase_2_backend_agnostic.md).

## Performance, honestly

Designed for **~100–10k events/sec per consumer** with **sub-second to
seconds latency**, on a Postgres catalog that is intended to comfortably
serve **~50 consumers** without connection pooling (~500 with
PgBouncer). Idle consumers are cheap by design (10s polling backoff).
Multiple consumers in one process are intended to share snapshot polling
via `LakeWatcher` (Phases 3 / 4), so N consumers ≠ N times the catalog
load.

The project is a poor fit for sub-10ms latency, OLTP CDC, and
globally-distributed sub-second cache invalidation. The four-axis
discussion and the honest "what we're bad at" list are in
[`docs/performance.md`](./docs/performance.md). The cross-backend
numbers behind the claims above will land as `bench/medium.yaml` runs
on the Phase 2 catalog matrix; today only the `bench/light.yaml` smoke
on embedded DuckDB exists.

## Roadmap

The phase plan, with explicit exit criteria for each phase, lives under
[`docs/roadmap/`](./docs/roadmap/):

| Phase | Goal | Status |
| --- | --- | --- |
| [0 — Discovery & Spike](./docs/roadmap/phase_0_discovery.md) | Audit overlap with DuckLake, freeze the primitive surface, lock concurrency / schema-boundary / threat-model / consumer-state-schema / audit-log decisions, decide language and license | complete |
| [1 — Extension MVP](./docs/roadmap/phase_1_extension_mvp.md) | Primitives + `cdc_consumer_*` lifecycle + owner-token lease + the SQL walkthrough + the README quickstart, against the embedded DuckDB catalog | in progress |
| [2 — Catalog matrix, DDL coverage, DLQ, doctor](./docs/roadmap/phase_2_backend_agnostic.md) | Identical semantics on DuckDB / SQLite / Postgres catalogs; `cdc_schema_diff` exhaustively tested; DLQ with DDL-blocks-DML semantics; `variant` / `geometry` serialization conventions; doctor command | not started |
| [3 — Python client](./docs/roadmap/phase_3_python_client.md) | `ducklake-cdc` PyPI package: composes the primitives with DuckLake builtins; iterator + Sink protocol + reference sinks | not started |
| [4 — Go client](./docs/roadmap/phase_4_go_client.md) | `ducklake-cdc/go` module mirroring the Python surface | not started |
| [5 — Beta hardening & release](./docs/roadmap/phase_5_beta.md) | Observability, packaging, soak / chaos tests, public `v0.1.0-beta.1` release | not started |

The design pillars that govern every phase (non-overlap with DuckLake,
read-and-commit-are-separate, one-consumer-name-is-one-reader, DDL is
first-class, etc.) are in [`docs/roadmap/README.md`](./docs/roadmap/README.md).
