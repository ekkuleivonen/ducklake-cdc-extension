# Hazard Log

This log records known project risks. It is deliberately lightweight: a hazard
can be handled by tests/docs/code, partially handled, not handled yet, or
accepted as a known limitation.

The hazard log is not the roadmap. The roadmap says where the project wants to
go; this file says what can hurt users or maintainers on the way there.

## Status Values

- `handled` - covered by shipped behavior, tests, and/or clear docs.
- `partially handled` - there is meaningful coverage, but known gaps remain.
- `not handled` - known risk with no real mitigation yet.
- `accepted` - intentionally left as a limitation for now.

## Hazards

### H-001: Catalog Backend Parity

- Risk: DuckDB, SQLite, and PostgreSQL DuckLake catalogs may diverge in metadata
  encoding, transaction behavior, or extension-visible semantics.
- Status: partially handled.
- Handling: `test/catalog_matrix/catalog_matrix_smoke.py` runs a DDL + DML
  cursor flow and lease rejection flow across DuckDB, SQLite, and PostgreSQL in
  CI.
- Next action: Add targeted backend tests only when a concrete portability bug
  appears or a user-reported workflow depends on it.

### H-002: Lease Correctness

- Risk: Two readers could process or commit the same consumer window if the
  owner-token lease is wrong.
- Status: partially handled.
- Handling: `test/smoke/lease_multiconn_smoke.py`,
  `test/sql/consumer_state.test`, and the catalog matrix smoke cover
  same-connection idempotence, second-reader rejection, force release, and
  stolen-lease commit failure.
- Notes: `cdc_consumer_force_release` is only for a holder that is
  demonstrably dead. A longer `lease_interval_seconds` gives long batches more
  room, but also makes dead holders take longer to clear.
- Next action: Keep new lease tests focused on real bugs: timeout precision,
  backend-specific timestamp behavior, and recovery after process death.

### H-003: Schema Boundary Ordering

- Risk: A consumer can apply rows under the wrong downstream schema if DDL and
  DML are delivered in the wrong order or a window crosses a schema boundary
  unexpectedly.
- Status: handled for the SQL extension surface.
- Handling: `cdc_window` exposes `schema_changes_pending`,
  `cdc_ddl_changes_read` emits typed DDL events, and
  `test/sql/always_breaks.test` plus
  `test/sql/ddl_stage2.test` cover schema boundaries and DDL-before-DML
  ordering.
- Next action: Revisit when a client library interleaves DDL and DML into one
  stream.

### H-004: Typed DDL Extraction Edge Cases

- Risk: Rename, nested-column, or combined schema changes can produce confusing
  or duplicate DDL events.
- Status: partially handled.
- Handling: `test/sql/ddl_stage2.test` covers the `snapshots().changes` MAP
  source, rename deduplication, snapshot-bound object lookup, and nested-column
  parent/child ordering.
- Next action: Add examples before adding more exhaustive cross-products. The
  goal is confidence in common migrations, not a full DuckLake spec clone.

### H-005: Compaction Gap Recovery

- Risk: A lagging consumer may point at snapshots expired by DuckLake
  maintenance and silently miss changes.
- Status: handled for the main gap path.
- Handling: `cdc_window` raises `CDC_GAP`, `cdc_consumer_reset` supports
  recovery, and `test/smoke/toctou_expire_smoke.py` covers the compaction gap
  path. Planned stateless range helpers must apply the same explicit gap
  handling when `from_snapshot` is older than the oldest available snapshot.
- Next action: Keep operator docs clear that retention must exceed expected
  consumer lag.

### H-006: Listen Connection Starvation

- Risk: Long-polling holds a DuckDB connection and can starve shared pools or
  interactive sessions.
- Status: handled by docs and warning.
- Handling: listen functions emit `CDC_WAIT_SHARED_CONNECTION`, clamp excessive
  timeouts, use level-triggered checks before waiting, and
  `test/smoke/cdc_wait_interrupt_smoke.py` covers interruptibility.
- Notes: SQL users should hold a dedicated connection for listen calls. Batch
  jobs that wake up on a schedule should call `cdc_window` directly instead of
  long-polling.
- Next action: Client libraries should hide this by using dedicated wait
  connections.

### H-007: Sink Failure Semantics

- Risk: Failed sink writes, especially failed DDL, need a durable operator flow;
  otherwise a single bad event can either halt work or create a flood of bad
  downstream writes.
- Status: intentionally client-owned.
- Handling: The extension exposes at-least-once windows and only advances a
  cursor when `cdc_commit` succeeds. It does not persist a generic failure
  queue because only the client/sink can classify whether a failure belongs to
  a window, snapshot, table, DDL event, row, or user callback.
- Next action: Let the Python client and reference sinks prove retry,
  idempotency, validation, and quarantine patterns before adding any shared
  failure persistence.

### H-008: No Client Libraries or Reference Sinks

- Risk: Users must compose the SQL primitives themselves, including commit
  timing, DDL/DML ordering, heartbeats, and sink failure behavior.
- Status: accepted.
- Handling: The SQL API and examples are the supported surface today.
- Next action: Let actual usage decide whether the first client should be a
  Python package or a smaller command-line helper.

### H-009: Performance Claims

- Risk: Early benchmark numbers can be mistaken for production promises.
- Status: partially handled.
- Handling: `bench/runner.py`, `bench/light.yaml`, and `bench/README.md`
  provide smoke-level measurements and explain how to read them.
- Next action: Publish numbers as observations with commit/hardware context;
  avoid hard performance contracts until repeated runs justify them.

### H-010: Public Docs Drift

- Risk: The README, roadmap, examples, and actual community release state can
  drift, giving users a false first impression.
- Status: partially handled.
- Handling: The roadmap now points to this hazard log, and the README should be
  refreshed whenever a release changes what users can install or try.
- Next action: Treat stale public status text as a release bug.

### H-011: Inlined DML Discovery

- Risk: DuckLake's inlined-data path uses `inlined_insert` /
  `inlined_delete` keys instead of the regular DML map keys. Consumers that
  discover work from `snapshots().changes` and forget those keys can silently
  skip small batches.
- Status: handled for the extension surface.
- Handling: The extension's discovery logic checks the inlined keys, and
  `test/upstream/enumerate_changes_map.py` tracks the observed DuckLake key
  set.
- Next action: Keep any new discovery/filtering code covered by the upstream
  key probe or a focused SQL test.

### H-012: Inline-on-Inline Delete Visibility

- Risk: DuckLake can represent deletes against inlined rows by updating catalog
  row visibility rather than advertising a normal DML key. A consumer that uses
  DML ticks alone as "is there row payload work?" can miss rows that DML changes
  reads would still return.
- Status: partially handled.
- Handling: DML changes reads remain the source of truth for row payloads. The
  conservative rule is: do not skip a DML changes read just because a tick
  stream looks empty.
- Next action: Add a targeted test only if this shows up as a real user-facing
  confusion point.

### H-013: Inlining Limit Assumptions

- Risk: Tests or examples copied from old prototypes may assume
  `DATA_INLINING_ROW_LIMIT = 100`, but DuckLake's default is 10. That can make
  tests miss the inlined-data path entirely.
- Status: handled in upstream probes.
- Handling: `test/upstream/enumerate_changes_map.py` sets
  `DATA_INLINING_ROW_LIMIT = 10` explicitly.
- Next action: Set `DATA_INLINING_ROW_LIMIT` explicitly in any test that cares
  about inlined-vs-materialized behavior.

### H-014: Dropped Columns in Same-Snapshot DML

- Risk: If one DuckLake commit both drops a column and deletes or updates rows
  in the same table, typed table DML reads use the end-snapshot schema and do
  not include the dropped column's old values.
- Status: accepted.
- Handling: This is DuckLake `table_changes` behavior, not an extension bug.
  The recovery path for audit use cases is DuckLake time travel: read the table
  at the snapshot before the drop and join the preserved column values back to
  the affected DML events.
- Notes: Schema-boundary handling does not help when the DDL and DML are in the
  same snapshot. Consumers that need the old values must recover them before
  retention expires the pre-drop snapshot.
- Next action: Keep this as a known limitation unless a real audit workflow
  needs a helper around the time-travel recipe.

### H-015: Sibling Metadata Schema State

- Risk: Moving CDC persistence out of DuckLake-managed tables and into a
  sibling schema in the metadata catalog removes DuckLake snapshot overhead, but
  it also means CDC state is no longer protected by DuckLake's table DDL,
  catalog-versioning, or data-type translation layer.
- Status: partially handled.
- Handling: CDC state now lives in the metadata catalog, using
  `__ducklake_metadata_<catalog>.__ducklake_cdc` where schemas are supported and
  a prefixed-table fallback for SQLite. Repeated string fields use portable JSON
  text because SQLite does not preserve DuckDB LIST columns through the scanner
  layer.
- Notes: DuckDB and SQLite are covered by the catalog matrix smoke. PostgreSQL
  still needs the same focused probe before this is treated as fully portable.
- Next action: Extend the backend matrix probe to PostgreSQL and add explicit
  cleanup/migration coverage for the state tables.

### H-016: CDC State and DuckLake Snapshot Atomicity

- Risk: Once CDC state lives outside DuckLake-managed tables, a `cdc_commit`
  updates metadata-catalog state without creating a DuckLake snapshot. This is
  the performance goal, but it weakens the current "all state is ordinary
  DuckLake data" consistency story and can expose ordering bugs around producer
  commits, consumer commits, rollbacks, and retries.
- Status: partially handled.
- Handling: Hot-path state writes are intentionally narrow metadata-catalog
  updates, and `cdc_window` / `cdc_commit` continue resolving committed
  snapshot ids against DuckLake before returning or advancing the cursor.
- Notes: SQLite cannot safely hold a direct metadata write transaction open
  while DuckLake reads snapshot metadata from the same backend, so lease and
  commit writes are not grouped with the later snapshot scan. This preserves the
  at-least-once SQL contract but weakens the old "one DuckLake table
  transaction" story.
- Next action: Add restart/race tests for commit idempotence, stolen-lease
  rejection, and recovery after process death.

### H-017: CDC State Discoverability and Migration

- Risk: Users and operators can currently inspect CDC state as DuckLake tables
  under `main`. A sibling metadata schema changes where state lives, which can
  break ad-hoc runbooks, backups, and upgrades from earlier releases.
- Status: partially handled.
- Handling: Preserve the public table functions (`cdc_consumer_stats`,
  `cdc_audit_events`, and lifecycle functions) as the supported inspection
  surface instead of asking users to query storage tables directly.
- Notes: Existing `__ducklake_cdc_*` DuckLake tables may exist in early user
  catalogs. The migration path must be explicit: either one-way copy into the
  sibling schema with clear ownership, or a documented reset path for pre-1.0
  catalogs.
- Next action: Add an upgrade test that starts from the old DuckLake-table
  layout.

### H-018: Metadata State Retention

- Risk: CDC-owned metadata tables are no longer DuckLake-managed data, so
  DuckLake cleanup, compaction, and snapshot expiration will not prune them.
  `__ducklake_cdc_audit` can grow without bound.
- Status: partially handled.
- Handling: `__ducklake_cdc_consumers` is bounded by the number of named
  consumers. The current unbounded table is `__ducklake_cdc_audit`, which
  appends lifecycle and lease-recovery events.
- Notes: Heartbeats and commits should remain updates to
  `__ducklake_cdc_consumers`, not append-only audit events, unless there is a
  retention policy in place. Any future shared failure-persistence path must
  ship with acknowledge/replay/prune semantics.
- Next action: Add an explicit maintenance surface, such as audit pruning by
  age and observability for CDC metadata table row counts, before treating
  long-lived catalogs as production-ready.

### H-019: Stateless Query Semantics

- Risk: `*_query` functions may appear equivalent to consumer reads, but they
  intentionally do not acquire leases, apply consumer subscription filters, move
  cursors, or enforce consumer schema-boundary policy.
- Status: not handled.
- Handling: The API docs distinguish durable replay from stateless queries.
- Next action: Add TDD coverage for query gap handling, `to_snapshot := NULL`,
  DDL/DML ordering, and proof that stateless queries do not mutate consumer
  state.

### H-020: Diagnostic False Confidence

- Risk: `cdc_doctor` can make operators trust an incomplete health report,
  especially if new hazards are added without updating doctor checks.
- Status: not handled.
- Handling: `cdc_doctor` is planned as an advisory table function, not a proof
  of correctness.
- Next action: Keep doctor checks tied to concrete hazards: gap risk, stale
  leases, dropped or renamed subscriptions, metadata presence, catalog
  compatibility, suspicious lag, and recent reset or force-release audit events.

### H-021: High-Level Auto Commit Bypasses Sink Gating

- Risk: Python high-level consumers may expose `auto_commit=True` while also
  delivering to sinks. In that mode the SQL listen/read function can commit
  before required sinks acknowledge the batch, so the sink layer no longer
  controls at-least-once delivery.
- Status: accepted.
- Handling: This is a known limitation for the first Python client shape. The
  draft documents that `auto_commit=True` passes through to SQL commit behavior
  and bypasses sink-gated commit safety.
- Next action: Revisit only if users are likely to trip over this in the high
  level API; possible mitigations include forbidding `auto_commit=True` with
  required sinks or renaming it to make the unsafe ordering obvious.

### H-022: Cross-Connection Metadata Lock Handoff on First Bootstrap Write

- Risk: The first cdc_* call against a catalog in a DuckDB process is
  the one that bootstraps `__ducklake_cdc.*` (`CREATE SCHEMA IF NOT
  EXISTS` + `CREATE TABLE IF NOT EXISTS` × 3) against the attached
  metadata database (`__ducklake_metadata_<catalog>`). When the
  immediately-preceding user statement is a read on that same attached
  metadata database — the canonical shape being `SET VARIABLE x =
  (SELECT max(snapshot_id) FROM __ducklake_metadata_<catalog>
  .ducklake_snapshot)` followed by `cdc_*_consumer_create(..., start_at
  := getvariable('x'))` — the same caller thread ends up running the
  extension's bootstrap writes on a newly-opened internal
  `duckdb::Connection` against the same attached database whose catalog
  machinery the outer `ClientContext` still holds state on. Release-matrix
  MSVC has also failed when the first bootstrap used `start_at := 'now'` with
  no user metadata read immediately before; the minimal trigger is not fully
  characterised — treat any first write-path `cdc_*` after DuckLake activity on
  the outer connection as suspicious on MSVC until stack evidence exists. The two
  platforms surface this differently:
  - Windows MSVC's error-checking `std::mutex::lock()` detects the
    resulting same-thread re-entry of a shared catalog/attachment
    mutex and throws `std::system_error(errc::
    resource_deadlock_would_occur, "resource deadlock would occur")`,
    which the test harness prints as
    `Invalid Error: resource deadlock would occur: resource deadlock
    would occur` (doubled-message shape is the MSVC STL fingerprint of
    `std::system_error`).
  - Windows MinGW's C runtime historically surfaced a related file-
    locking variant — `LockFileEx` / `ERROR_POSSIBLE_DEADLOCK` mapped
    to `EDEADLK`, printed as `Resource deadlock avoided` — on
    SQLite-backed metadata. Linux, macOS, and Wasm don't error-check
    `std::mutex` re-entry on the same thread, so the same code path
    slides past silently on those platforms.
  The most plausible shared mutex is inside the auto-loaded DuckLake
  extension's catalog (it owns the attached metadata database and
  sees both the outer read and the inner bootstrap write against it);
  secondary candidates are `DatabaseManager::databases_lock` and the
  dependency-manager write lock on the same `AttachedDatabase`. A
  debug-build MSVC stack trace at the `std::system_error` throw site
  would pin which one exactly; we haven't invested in that yet because
  no real user has hit this outside the regression-test pattern
  described below.
- Status: partially handled.
- Handling (source side, kept): `CheckCatalogOrThrow` and
  `BootstrapConsumerStateOrThrow` have `Connection&` overloads, and
  every cdc_* function opens one internal connection up front
  (`duckdb::Connection conn(*context.db); ConfigureCdcInternalConnection
  (conn);`) and threads it through the compat probe, the bootstrap
  CREATEs, and all follow-up reads/writes. That closes the separate
  3-internal-connection MinGW variant (the original shape of this
  hazard). It does not close the outer-conn ↔ inner-conn handoff, which
  requires upstream (DuckDB + DuckLake) lock-ordering work to fix
  in-source.
- Handling (release matrix — temporary): Windows MSVC (`DUCKDB_PLATFORM
  == windows_amd64`) does not export `CDC_RUN_H022_SENSITIVE_TESTS`.
  That file gates on `require-env CDC_RUN_H022_SENSITIVE_TESTS`, so it
  is skipped entirely on that triplet until stabilisation captures an
  MSVC stack trace at the throw site and fixes the mutex ordering.
  The extension root `Makefile` exports `CDC_RUN_H022_SENSITIVE_TESTS
  := 1` on every other platform CI matrix entry; local `make
  test_release` thus runs the regression unchanged. Developers who run
  `unittest` manually without Make must export
  `CDC_RUN_H022_SENSITIVE_TESTS=1` on non MSVC-Windows hosts. The test
  body keeps `SET VARIABLE x = (SELECT max(snapshot_id) FROM
  __ducklake_metadata_<catalog>.ducklake_snapshot)` + `start_at :=
  getvariable('x')` — the intentional API-shape coverage; MSVC does
  not run it via this gate (`start_at := 'now'` proved insufficient on
  MSVC CI — the deadlock still surfaced there).
- Notes: DuckLake's documented `META_JOURNAL_MODE 'WAL'` and
  `META_BUSY_TIMEOUT` ATTACH options improve concurrency on
  SQLite-backed catalogs in general (see duckdb/ducklake#128) and are
  orthogonal to this hazard.
- User-facing guidance: On Windows MSVC, avoid first write-path `cdc_*`
  against a catalog in-process until stabilisation confirms a safe recipe.
  Tentative mitigations (empirical, not validated on MSVC CI): drive a read-
  only `cdc_*` call first (`cdc_dml_ticks_query`, etc.), use a separate
  DuckDB process for the preceding metadata peek, or open the catalog on an
  already-bootstraped path (`start_at := 'now'` alone did **not** clear the
  failure in MSVC release-matrix logs).
- Next action: If we see this hit by a real user outside the
  regression-test pattern, capture an MSVC debug-build stack trace at
  the `std::system_error(resource_deadlock_would_occur)` throw site to
  identify the specific shared mutex, then either serialize the
  outer-to-inner transition in our code or file an upstream issue on
  the offending lock's ordering contract.
