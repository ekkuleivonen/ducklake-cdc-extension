# Roadmap

This roadmap describes where `ducklake-cdc` is trying to go. It is not a
release checklist, and it is not a promise that every possible edge case must
be closed before the next tag.

Known risks live in the [hazard log](./hazard-log.md). Older phase files were
useful design history, but the surviving docs are now the canonical definition
of what exists and what is intentionally deferred.

## Where We Are

`ducklake-cdc` is a released DuckDB extension that adds durable consumer
cursors, long-polling, typed DDL events, schema-boundary handling, and
row-level change reads on top of DuckLake. The current repository release is
`v0.3.2`; DuckDB community-extension distribution for that release is green
and waiting on upstream merge.

What exists today:

- The SQL extension surface: `cdc_window`, `cdc_commit`, `cdc_wait`,
  `cdc_ddl`, `cdc_changes`, `cdc_events`, `cdc_recent_changes`,
  `cdc_recent_ddl`, `cdc_range_events`, `cdc_range_ddl`,
  `cdc_range_changes`, `cdc_schema_diff`, `cdc_doctor`,
  `cdc_consumer_*`, `cdc_consumer_stats`, and `cdc_audit_recent`.
- Owner-token leasing for single-reader-per-consumer enforcement.
- GitHub releases and full DuckDB extension matrix validation, proven through
  the `v0.3.2` line.
- Community-extension packaging for `v0.3.2`, with SQL tests disabled in the
  community repo because this extension's DuckLake-dependent integration suite
  runs in its own CI.
- CI smoke coverage for DuckDB, SQLite, and PostgreSQL DuckLake catalogs.
- A lightweight benchmark harness for smoke-level performance tracking,
  including both normal and empty-window workloads.

What is still intentionally thin:

- No Python client package.
- No reference sinks.
- No extension-owned sink failure queue or validation framework. Sink retries,
  failure classification, quarantine policy, and exactly-once-ish semantics
  belong in clients and sinks.
- Backend coverage is smoke-level, not exhaustive certification.
- `UPDATE ... RETURNING` fast paths are currently enabled only for DuckDB
  metadata catalogs. SQLite and PostgreSQL stay on the portable path because
  DuckDB's attached-table scanners reject `UPDATE ... RETURNING` there today.
- Performance numbers are early signal, not production contracts.

## Direction

### Just Finished: Verified SQL Surface and Hot Path Sweep

The SQL surface has been checked and locked in with tests against the main use
cases that motivate the project: ad-hoc SQL inspection, managed subscribers,
streaming pipeline consumers, catalog/schema replication, replay/backfill, and
operational diagnostics. The `v0.3.2` sweep also tightened the single-round hot
path and release packaging.

Decisions from this pass:

- Keep `cdc_changes`, `cdc_ddl`, and `cdc_events` focused on the current
  leased consumer window.
- Added explicit stateless range helpers for bounded replay/export/debug work:
  `cdc_range_events`, `cdc_range_ddl`, and `cdc_range_changes`.
- Added `cdc_doctor` as the common health-check surface for SQL users, support
  requests, and the future Python client.
- Treat operational replay as a named-consumer workflow: use a separate
  backfill consumer when live processing must continue independently.
- Do not impose a JSON envelope on DuckLake `commit_extra_info`; producers and
  clients own those conventions.
- Do not expose an extension-owned sink failure queue. The extension provides
  at-least-once replay mechanics; clients and sinks own idempotency, retries,
  validation, quarantine policy, and external side-effect semantics.
- Keep `UPDATE ... RETURNING` gated to DuckDB metadata catalogs until
  DuckDB's SQLite/PostgreSQL scanners support it for attached tables.
- Push schema-change filtering into SQL where possible and keep the
  fast/slow-path asymmetry documented.
- Track both non-empty and empty `cdc_window` behaviour in the benchmark
  harness so optimisations do not only target the happy path.

### Now: Python Client

The next chapter is the first client, because it is the shortest path from the
SQL extension to people actually trying the project in scripts, notebooks, and
small services. The SQL layer should now be stable enough to support that
client without inventing a second cursor model.

Likely work:

- Explicit iterator + commit API first.
- A small `tail()` helper once the explicit API feels right.
- A replay/backfill helper that can create or use a separate consumer for
  bounded historical work.
- Stdout/file/webhook-style examples before heavier sinks.
- Client-side handling for dedicated wait connections and heartbeats.
- Client-owned retry/idempotency/failure handling. Do not push sink-specific
  quarantine semantics back into the extension.

### Next: Client Examples and Feedback

Once the Python client can drive the core consumer loop, use examples and real
feedback to decide what deserves to become product surface.

Likely work:

- End-to-end examples for DDL-only watching, catalog replication, and table
  change processing.
- A small operational recipe around `cdc_doctor`, stale leases, lag, and replay.
- Tighten docs around client-owned idempotency, retry, and dead-letter handling
  without adding extension-owned sink semantics.

### Later: Only If Demand Appears

These are not near-term plans:

- Other language clients.
- Kafka, Redis Streams, or Postgres mirror sinks.
- Large validation or quarantine machinery.
- Long soak-test programs.
- A full docs site.

They become interesting when real users or new maintainers show up.

## Principles

- Use DuckLake's own primitives wherever possible: `snapshots()`,
  `table_changes`, time travel, commit metadata, and compaction tooling.
- Keep the extension surface small. Add SQL only when it gives users a durable
  cursor, safety boundary, or useful introspection they cannot easily compose
  themselves.
- Preserve at-least-once semantics: read windows and commits stay separate.
- Treat schema changes as first-class events. DDL is not a footnote to row
  changes.
- Prefer honest limitations over elaborate promises. A known unhandled hazard
  is better than a hidden release gate.

## More Detail

- [Design notes](./design.md)
- [Hazard log](./hazard-log.md)
