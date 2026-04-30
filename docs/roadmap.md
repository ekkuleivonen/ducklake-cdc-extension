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
- No DLQ helper API beyond the schema created by the extension.
- Backend coverage is smoke-level, not exhaustive certification.
- Performance numbers are early signal, not production contracts.

## Direction

### Just Finished: Clean Up the Docs

The docs have been reduced to the pieces a single weekend maintainer should be
able to keep current.

What remains:

- Keep the README, API reference, examples, roadmap, and hazard log.
- Keep focused operational docs that explain current behavior.
- Keep design notes short and tied to shipped metadata/catalog behavior.
- Keep `INSTALL ducklake_cdc FROM community` working.

### Now: Verify the Extension Surface

The next chapter is a couple focused passes on the extension itself.

Likely work:

- Check that the SQL API covers the use cases that motivated the project.
- Add targeted tests for hazards that are real and reproducible.
- Look for simple performance wins in the hot paths.
- Keep the catalog-matrix smoke small and meaningful.
- Improve examples where the SQL surface feels awkward.

### Next: Python Client

The first client should be Python, because it is the shortest path from the SQL
extension to people actually trying the project in scripts, notebooks, and
small services.

Likely work:

- Explicit iterator + commit API first.
- A small `tail()` helper once the explicit API feels right.
- Stdout/file/webhook-style examples before heavier sinks.
- Client-side handling for dedicated wait connections and heartbeats.

### Later: Only If Demand Appears

These are not near-term plans:

- Other language clients.
- Kafka, Redis Streams, or Postgres mirror sinks.
- Large DLQ machinery.
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
