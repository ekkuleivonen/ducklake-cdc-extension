# Roadmap

This roadmap describes where `ducklake-cdc` is trying to go. It is not a
release checklist, and it is not a promise that every possible edge case must
be closed before the next tag.

Known risks live in the [hazard log](./hazard-log.md). Older phase files were
useful design history, but the surviving docs are now the canonical definition
of what exists and what is intentionally deferred.

## Where We Are

`ducklake-cdc` is a community-published DuckDB extension that adds durable
consumer cursors, long-polling, typed DDL events, schema-boundary handling, and
row-level change reads on top of DuckLake.

What exists today:

- The SQL extension surface: `cdc_window`, `cdc_commit`, `cdc_wait`,
  `cdc_ddl`, `cdc_changes`, `cdc_events`, `cdc_recent_changes`,
  `cdc_recent_ddl`, `cdc_schema_diff`, `cdc_consumer_*`,
  `cdc_consumer_stats`, and `cdc_audit_recent`.
- Owner-token leasing for single-reader-per-consumer enforcement.
- Community extension publishing, proven through the `v0.2.0` line.
- CI smoke coverage for DuckDB, SQLite, and PostgreSQL DuckLake catalogs.
- A lightweight benchmark harness for smoke-level performance tracking.

What is still intentionally thin:

- No Python client package.
- No reference sinks.
- No extension-owned DLQ or validation framework. Sink retries, failure
  classification, quarantine/dead-letter policy, and exactly-once-ish
  semantics belong in clients and sinks.
- Backend coverage is smoke-level, not exhaustive certification.
- Performance numbers are early signal, not production contracts.

## Direction

### Just Finished: Verify the Extension Surface

The SQL surface has been checked against the main use cases that motivate the
project: ad-hoc SQL inspection, managed subscribers, streaming pipeline
consumers, catalog/schema replication, replay/backfill, and operational
diagnostics.

Decisions from this pass:

- Keep `cdc_changes`, `cdc_ddl`, and `cdc_events` focused on the current
  leased consumer window.
- Add explicit stateless range helpers for bounded replay/export/debug work:
  `cdc_range_events`, `cdc_range_ddl`, and `cdc_range_changes`.
- Add `cdc_doctor` as the common health-check surface for SQL users, support
  requests, and the future Python client.
- Treat operational replay as a named-consumer workflow: use a separate
  backfill consumer when live processing must continue independently.
- Do not impose a JSON envelope on DuckLake `commit_extra_info`; producers and
  clients own those conventions.
- Do not expose an extension-owned DLQ. The extension provides at-least-once
  replay mechanics; clients and sinks own idempotency, retries, validation,
  quarantine/dead-letter policy, and external side-effect semantics.

### Now: TDD the Verified Surface

The next chapter is implementing the small SQL additions and removals that fell
out of the surface pass.

Likely work:

- Remove the no-op DLQ table/schema path and any docs that imply extension-owned
  DLQ semantics.
- Add tests for `cdc_doctor` diagnostics before implementing the table
  function.
- Add tests for stateless range helpers, including gap handling and
  `to_snapshot := NULL` as current-head sugar.
- Keep the existing consumer-window semantics crisp: range reads must not
  acquire leases, apply subscription filters, or move cursors.

### Next: Python Client

The first client should be Python, because it is the shortest path from the SQL
extension to people actually trying the project in scripts, notebooks, and
small services.

Likely work:

- Explicit iterator + commit API first.
- A small `tail()` helper once the explicit API feels right.
- A replay/backfill helper that can create or use a separate consumer for
  bounded historical work.
- Stdout/file/webhook-style examples before heavier sinks.
- Client-side handling for dedicated wait connections and heartbeats.
- Client-owned retry/idempotency/failure handling. Do not push sink-specific
  dead-letter semantics back into the extension.

### Later: Only If Demand Appears

These are not near-term plans:

- Other language clients.
- Kafka, Redis Streams, or Postgres mirror sinks.
- Large validation, quarantine, or DLQ machinery.
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
