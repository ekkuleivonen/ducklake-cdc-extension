# Phase 1 — Extension MVP

**Goal:** ship the primitives (`cdc_window`, `cdc_commit`, `cdc_wait`, `cdc_ddl`, the `cdc_consumer_*` lifecycle including the **owner-token lease** for single-reader enforcement), the structured-error contract, the acknowledged sugar wrappers, the audit table, and the canonical README quickstart. Validated against the embedded DuckDB metadata catalog only.

The MVP is small on purpose. Anything DuckLake already exposes (`snapshots()`, `current_snapshot()`, `last_committed_snapshot()`, `table_changes`, `set_commit_message`, time travel) is consumed directly. Our code only ships the missing primitives: cursors, the lease, gap detection, long-poll, **typed DDL extraction**, schema-diff computation.

This phase ships both the demo *and* the README quickstart that reflects it. If a SQL-literate reader can't go from `INSTALL` to seeing both DDL and DML events in under a minute by following the README verbatim, the phase isn't done.

## Work items

### Scaffolding

- [x] Scaffold from the official DuckDB extension template (per ADR 0001 — C++ over `duckdb-rs`). The template's files (`Makefile`, `CMakeLists.txt`, `extension_config.cmake`, `vcpkg.json`, `src/`, `test/sql/`, `.clang-format`/`.clang-tidy`/`.editorconfig` symlinks) live at the repo root. `LOAD 'build/debug/extension/ducklake_cdc/ducklake_cdc.duckdb_extension'; SELECT cdc_version();` reports `ducklake_cdc <build-stamp>` end-to-end. The build-time `EXT_VERSION_DUCKLAKE_CDC` macro is what stamps the SHA or tag — any wild binary is therefore traceable to source. Smoke test: `test/sql/ducklake_cdc.test`.
- [x] **CI: build + test pipeline wired** via `.github/workflows/full-ci.yml` and `.github/workflows/light-ci.yml`. Full CI calls the canonical reusable workflow `duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@v1.5.1` with `extension_name: ducklake_cdc` and `duckdb_version: v1.5.1`; light CI runs format/tidy plus a Linux debug smoke lane. **Outstanding follow-ups before this is closed end-to-end:**
  - [ ] Sanitiser jobs (ASan / UBSan / TSan) — the local debug build already runs with `-fsanitize=address -fsanitize=undefined`; promote that to a CI matrix entry (per ADR 0001's "every CI build runs at least one pass under sanitiser" commitment).
  - [ ] Verify the upstream matrix actually goes green for `ducklake_cdc` on first push — the matrix builds we get for free are only worth the disk space if they pass. First push to a feature branch reveals which platforms need toolchain tweaks.
  - [x] A separate fast-feedback workflow for quick iteration cycles (`.github/workflows/light-ci.yml`), so the canonical multi-platform pipeline doesn't gate every feature-branch push.
- [x] Catalog-version compatibility check on `LOAD ducklake_cdc;` — if a loaded DuckLake catalog's format version (per `docs/compatibility.md`) is outside the hardcoded supported set in `SUPPORTED_DUCKLAKE_CATALOG_VERSIONS` (`src/compat_check.cpp`), emit a `CDC_INCOMPATIBLE_CATALOG` notice (per `docs/errors.md`) per incompatible catalog at extension-load time. Don't let users discover incompatibility through cryptic runtime errors. The call-time half is also live now: catalog-touching `cdc_*` functions call `CheckCatalogOrThrow` before writes and raise the same structured `CDC_INCOMPATIBLE_CATALOG` message. `cdc_version()` remains exempt because it is a build stamp that must stay callable for debugging. The planned `extension_init_warning` audit row is deferred indefinitely: writing an audit row into an incompatible catalog would require exactly the untrusted catalog write the guard exists to prevent. SQL silent-paths test under `test/sql/compat_check.test`; notice/error text smoke under `test/smoke/compat_warning_smoke.py`.
- [x] **Promote `test/upstream/enumerate_changes_map.py` from Phase 0 spike to a recurring CI job.** Per ADR 0008: the probe runs on full CI, dumps the `snapshots().changes` MAP key set, and diffs against the reference table committed to `docs/api.md#snapshots-changes-map-reference`. **A diff is a hard build break.** Reference outputs are committed under `test/upstream/output/`; the full CI `upstream-contract-check` job runs `uv run --locked python test/upstream/enumerate_changes_map.py --check`.

### Release cadence — tag early, tag often

Visible release cadence is what turns a project that "looks alive" from one that "looks dead."

- [ ] First release tag. The current clean-slate repo has release automation (`.github/workflows/release.yml`) but no `v0.0.*` tags yet.
- [ ] Subsequent tags every time a meaningful primitive lands. Target a visible cadence between the first `v0.0.x` tag and the beta cut.
- [ ] Releases are built and published as GitHub release artifacts with a one-line "what's new" body and links to matching docs or examples.

### Consumer state — `cdc_consumer_*`

The on-disk schema is the single source of truth in **ADR 0009**. This section enumerates the Phase 1 implementation; any column drift between this list and ADR 0009 is a bug in this list.

- [x] On first use, create `__ducklake_cdc_consumers` as a regular DuckLake table inside the attached catalog, with the columns from ADR 0009. **Implementation note:** DuckLake v1.5.1 rejects `PRIMARY KEY`, `UNIQUE`, default expressions, indexes, and the JSON extension type in regular DuckLake tables, so the Phase-1 foundation slice creates the full column set, lowers `metadata` to `VARCHAR`, inserts explicit default values, and enforces duplicate consumer names in `cdc_consumer_create` until Phase 2's backend-matrix work revisits physical constraints:
  - `consumer_name VARCHAR PRIMARY KEY`
  - `consumer_id BIGINT NOT NULL UNIQUE`
  - `tables VARCHAR[]` (NULL = all tables; hard list of fully-qualified names; no globs/tags in v0.1)
  - `change_types VARCHAR[]` (NULL = all four; subset of `{insert, update_preimage, update_postimage, delete}`)
  - `event_categories VARCHAR[]` (NULL = both; subset of `{ddl, dml}`)
  - `stop_at_schema_change BOOLEAN NOT NULL DEFAULT TRUE`
  - `dml_blocked_by_failed_ddl BOOLEAN NOT NULL DEFAULT TRUE`
  - `last_committed_snapshot BIGINT`
  - `last_committed_schema_version BIGINT`
  - `owner_token UUID`
  - `owner_acquired_at TIMESTAMPTZ`
  - `owner_heartbeat_at TIMESTAMPTZ`
  - `lease_interval_seconds INTEGER NOT NULL DEFAULT 60`
  - `created_at TIMESTAMPTZ NOT NULL`
  - `created_by VARCHAR`
  - `updated_at TIMESTAMPTZ NOT NULL`
  - `metadata JSON` (extension-reserved, free-form for clients; **audit history does NOT live here** — see `__ducklake_cdc_audit`)
- [x] On first use, create `__ducklake_cdc_audit` per ADR 0010. The Phase-1 foundation slice creates the full column set and writes `consumer_create`; later lifecycle slices add their own actions. `details` is lowered to `VARCHAR` for the same DuckLake v1.5.1 DDL limitation noted above. Phase 1 emits these `action`s:
  - Foundation slice emits `consumer_create`.
  - Later lifecycle slices emit `consumer_drop`, `consumer_reset`, `consumer_force_release`, and `lease_force_acquire`.
  - `extension_init_warning` is deferred indefinitely; writing an audit row into an incompatible catalog would require the untrusted write the compat guard exists to prevent.
  - Insert-only; operators clean up via the documented retention SQL.
- [x] Also create `__ducklake_cdc_dlq` schema in this phase (table is empty; the *feature* lands in Phase 2 but the schema is locked now so we don't migrate later). `payload` is lowered to `VARCHAR` for now:
  - `consumer_name VARCHAR`
  - `consumer_id BIGINT` (denormalised so DLQ rows survive consumer drops)
  - `snapshot_id BIGINT`
  - `event_kind VARCHAR` (`'ddl'` or `'dml'`; populated by Phase 3/4 client code)
  - `table_name VARCHAR` (NULL for non-table-scoped DDL)
  - `object_kind VARCHAR` (NULL for DML; one of `schema`, `table`, `view` for DDL)
  - `object_id BIGINT` (NULL for DML)
  - `rowid BIGINT` (NULL for DDL)
  - `change_type VARCHAR` (NULL for DDL)
  - `failed_at TIMESTAMPTZ`
  - `attempts INT`
  - `last_error VARCHAR`
  - `payload JSON` (the event payload at time of failure; lowered to `VARCHAR` until DuckLake supports JSON in regular DuckLake tables)
  - Intended PRIMARY KEY `(consumer_name, snapshot_id, COALESCE(table_name, ''), COALESCE(rowid, -1), COALESCE(event_kind, 'dml'))`; not physically emitted yet because DuckLake v1.5.1 rejects primary keys on regular DuckLake tables.
- [x] Implement `cdc_consumer_create(catalog, name, start_at := 'now')` foundation slice:
  - Assigns `consumer_id = COALESCE(MAX(consumer_id), 0) + 1` inside the create transaction (backend-portable, doesn't depend on `SERIAL` / `IDENTITY`).
  - `'now'` → cursor = `current_snapshot()`.
  - `'beginning'` → cursor = oldest available snapshot (smallest `snapshot_id` in `ducklake_snapshot`).
  - `<BIGINT>` → cursor = that snapshot, validated against `ducklake_snapshot`.
  - `<TIMESTAMP>` → cursor = `(SELECT max(snapshot_id) FROM ducklake_snapshot WHERE snapshot_time <= :ts)`. Pure passthrough to DuckLake's existing snapshot/timestamp resolution; we don't reinvent time travel. **Deferred follow-up:** timestamp-shaped `start_at` currently raises `CDC_FEATURE_NOT_YET_IMPLEMENTED` so its parsing surface can get explicit tests.
  - [x] Validate `tables` at create-time. Each entry must resolve to an existing table in the catalog *as of the resolved start cursor*. Raises `CDC_INVALID_TABLE_FILTER` (per `docs/errors.md`) listing the unresolved names, the snapshot id, and the actual table list at that snapshot. Empty `tables` list is rejected as well — it would deny everything.
  - [x] Validate `change_types` at create-time. Each entry must be in `{insert, update_preimage, update_postimage, delete}` (the literal values DuckLake's `table_changes` emits). Empty list is rejected.
  - [x] Validate `event_categories` at create-time. Each entry must be in `{ddl, dml}`. Empty list is rejected. The filter values round-trip into the consumer row (visible on `cdc_consumer_stats`) and gate `cdc_changes` / `cdc_ddl` post-window: a `['ddl']` consumer sees zero rows from `cdc_changes`, a `['dml']` consumer sees zero rows from `cdc_ddl`. Cursor still advances.
  - Records a `consumer_create` audit row with the full create-time parameters in `details`.
- [x] Implement `cdc_consumer_drop(catalog, name)`, `cdc_consumer_list(catalog)`, `cdc_consumer_reset(catalog, name, to_snapshot := NULL)`. `cdc_consumer_list(catalog)` is shipped by the foundation slice (returns the consumer table ordered by `consumer_id ASC`); `drop` and `reset` are shipped by the consumer-lifecycle slice. `NULL` = oldest available; an explicit value goes through the same validation as `start_at`. Each mutating call writes the corresponding audit row. Explicit `catalog` arguments are the shipped v0.1 call form until qualified-call sugar is solved for this extension.
- [x] Implement `cdc_consumer_heartbeat(catalog, name)` — single UPDATE setting `owner_heartbeat_at = now() WHERE consumer_name = :name AND owner_token = :calling_token`. Returns `heartbeat_extended BOOLEAN` (`TRUE` on success) and raises `CDC_BUSY` if the lease has been lost / force-released or the calling connection never acquired it. Explicit `catalog` remains the shipped v0.1 call form until qualified-call sugar is solved. **Library client guidance (binding into Phase 3 / 4):** the Python `tail()` and Go `Tail()` helpers always start a background heartbeat thread/goroutine on `lease_interval_seconds / 3` cadence, regardless of expected batch duration; the heartbeat is a no-op cost (one tiny UPDATE) if the batch finishes before the next tick. Detecting "batch will exceed half the lease interval" reliably is harder than just paying the always-on cost — and the always-on cost is a single UPDATE per `lease_interval_seconds / 3` (default 20s). Bindings document this as the contract; SQL-CLI users without a library still call `cdc_consumer_heartbeat(catalog, name)` manually for long batches.
- [x] Implement `cdc_consumer_force_release(catalog, name)` — `UPDATE __ducklake_cdc_consumers SET owner_token = NULL, owner_acquired_at = NULL, owner_heartbeat_at = NULL WHERE consumer_name = :name` and writes a `consumer_force_release` audit row capturing the previous holder's token, acquired-at, and last-heartbeat. Operator-only escape hatch; documented as such. Explicit `catalog` arguments remain the shipped v0.1 call form until qualified-call sugar is solved for this extension.

### Primitive: `cdc_window`

This is the most-touched primitive in the system. Get its contract right or pay forever.

- [x] `cdc_window(catalog, consumer, max_snapshots := 100)` foundation slice returns one row (explicit `catalog` is the shipped v0.1 call form until qualified-call sugar is solved for this extension):
  ```text
  start_snapshot BIGINT,
  end_snapshot BIGINT,
  has_changes BOOLEAN,
  schema_version BIGINT,
  schema_changes_pending BOOLEAN
  ```
- [x] **Implementation, single transaction (foundation slice):**
  1. **Acquire / extend the lease** via the conditional UPDATE described in ADR 0007:
     ```sql
     UPDATE __ducklake_cdc_consumers
     SET    owner_token        = COALESCE(owner_token, :new_token_for_this_call),
            owner_acquired_at  = CASE WHEN owner_token IS NULL THEN now() ELSE owner_acquired_at END,
            owner_heartbeat_at = now()
     WHERE  consumer_name = :name
       AND  (owner_token IS NULL
             OR owner_heartbeat_at < now() - make_interval(secs => lease_interval_seconds)
             OR owner_token = :token_held_by_this_connection)
     RETURNING owner_token, last_committed_snapshot, last_committed_schema_version,
               stop_at_schema_change, tables /* etc. */;
     ```
     DuckLake v1.5.1 does not support `UPDATE ... RETURNING` for DuckLake tables, so the shipped foundation uses the same lease predicate split across read/update/re-read inside one transaction. If the predicate fails, it reads the existing holder's `(owner_token, owner_acquired_at, owner_heartbeat_at, lease_interval_seconds)` and raises structured `CDC_BUSY` (see Phase 0 error format). If a non-NULL `owner_token` is overwritten because its heartbeat timed out, it writes a `lease_force_acquire` audit row. The connection caches its own `owner_token` per `(catalog, consumer)` in process-local connection state, so subsequent `cdc_window` calls from this connection re-pass the same token (idempotence contract; see below).
  2. With the lease held, re-read the consumer's other state fields from `__ducklake_cdc_consumers`.
  3. Verify `last_committed_snapshot` still exists in `ducklake_snapshot`. If not → raise structured `CDC_GAP` (see error format in Phase 0).
  4. Compute `start` and `end_uncapped` from external DuckLake snapshots only. Because the CDC state tables are regular DuckLake tables, the shipped foundation filters snapshots whose `changes_made` only touches `__ducklake_cdc_*` tables so lease/audit writes do not appear as user data changes. For a schema-bound yield-before window, `end = start - 1` and `has_changes = false`.
  5. Read `consumer.tables` (NULL = whole-lake; otherwise a list of `schema.table` strings resolved to `table_id`s at consumer-create time and stored alongside).
  6. Find `next_schema_change` = the smallest `begin_snapshot > last_committed_snapshot` from `ducklake_schema_versions` filtered by:
     - whole-lake consumer: any row (a global schema-version change includes schema-level DDL).
     - table-filtered consumer: rows where `table_id IN (consumer.table_ids)`. **A schema change to an unrelated table does not bound the window.** This is the per-table correctness ADR 0006 calls out.
     Effective `schema_version` for the window = the `schema_version` of `start` (look up via `ducklake_snapshot.schema_version` for the lake-wide view, or via `ducklake_schema_versions` per-table when the filter is set).
  7. If `stop_at_schema_change` is `true` AND `next_schema_change <= end_uncapped`: bound `end = next_schema_change - 1`, set `schema_changes_pending = true`. Note: if `next_schema_change = start`, then `end = start - 1` (empty window) and the first event of the *next* window will be the DDL at `next_schema_change`.
  8. Else if `next_schema_change <= end_uncapped` (cross-boundary opt-in): `end = end_uncapped`, `schema_changes_pending = true` (informational only — the consumer opted into crossing).
  9. Else: `end = end_uncapped`, `schema_changes_pending = false`.
  10. `has_changes = end >= start`.
- [x] **Idempotence contract — same-connection.** Calling `cdc_window` N times without an intervening `cdc_commit` from the **same connection** returns the same `(start, end, schema_version, schema_changes_pending)` row. Covered in `test/sql/consumer_state.test`.
- [x] **Single-reader contract — different-connection.** Calling `cdc_window` from a *different* connection while a lease is held raises `CDC_BUSY`. Covered by `test/smoke/lease_multiconn_smoke.py`, which compiles a tiny C++ harness against the local debug libduckdb and opens two `duckdb::Connection` handles in one process.
- [x] **TOCTOU defense.** The foundation slice keeps the lease check/update, cursor existence check, external-snapshot scan, and schema-version lookups inside one transaction. The deterministic `ducklake_expire_snapshots` interleaving test ships in `test/smoke/toctou_expire_smoke.py` (embedded DuckDB single-writer interleaving — A reads a window, B expires the cursor, A's next call raises `CDC_GAP` cleanly and `cdc_consumer_reset` recovers). Phase 2 re-runs the same harness on the Postgres / SQLite catalog matrix.
- [x] **TOCTOU race test (deterministic interleaving).** Embedded-DuckDB lane shipped in `test/smoke/toctou_expire_smoke.py`:
  1. Connection A and B share one `DuckDB` instance; A creates a consumer pinned to a known external snapshot and reads `cdc_window`.
  2. Connection B calls `ducklake_expire_snapshots` with a cutoff that includes A's cursor.
  3. A's next `cdc_window` raises structured `CDC_GAP`; `cdc_consumer_reset` then succeeds against the oldest available snapshot. Recovery command in the error message is the contract the test pins.

  Backend coverage:
  - **Embedded DuckDB:** shipped (spike harness above; single-writer model serialises B behind A's transaction).
  - **Postgres backend:** deferred to Phase 2's catalog matrix (Postgres lane needs `pg_advisory_xact_lock` barrier + two `*sql.Conn`s to deterministically interleave the lease UPDATE and the expire commit).
  - **SQLite backend:** deferred to Phase 2 (same shape as embedded DuckDB; assert B's `ducklake_expire_snapshots` blocks until A's transaction releases).

  Each backend has its own test file; the test interleavings are deterministic, not timing-dependent. Phase 1 ships the embedded-DuckDB lane; Phase 2 re-runs the same harness on the catalog matrix.
- [x] **Schema-changes-pending UX.** When `schema_changes_pending = true`, `cdc_window` also emits a `CDC_SCHEMA_BOUNDARY` stderr notice (per `docs/errors.md` reference message) carrying the consumer name, the window's `end_snapshot` and `schema_version`, and the next snapshot's id and `schema_version`. Detection of the schema boundary is now structural — looks for actual external DDL tokens in `changes_made` rather than comparing physical `schema_version` numbers — so bootstrap-time CREATE TABLE on `__ducklake_cdc_*` no longer falsely flags subsequent INSERTs as schema boundaries.

### Primitive: `cdc_commit`

- [x] `cdc_commit(catalog, consumer, snapshot_id)` advances the cursor in its own transaction. Explicit `catalog` is the shipped v0.1 call form until qualified-call sugar is solved for this extension. Implementation: conditional update that:
  - Asserts `owner_token = :calling_token` (the caller's cached lease token). If the predicate fails, raise `CDC_BUSY` referring to the new holder's token — the caller's lease was lost (timeout / force-released).
  - Asserts `snapshot_id >= last_committed_snapshot`, `snapshot_id <= current_snapshot()`, and that `snapshot_id` exists in `ducklake_snapshot`.
  - Sets `last_committed_snapshot = :snapshot_id`, refreshes `last_committed_schema_version` from `ducklake_snapshot`, **and refreshes `owner_heartbeat_at = now()`** so the act of committing doubles as a lease extension.
  - `updated_at = now()`.
  - Returns `(consumer_name, committed_snapshot, schema_version)` for SQL assertions. SQLLogic coverage validates successful commit, cursor advancement, post-commit `cdc_window`, behind-cursor rejection, missing cached lease (`CDC_BUSY`), missing consumer, and incompatible catalog guard.
- [x] Apps that want exactly-once-on-commit batch the read and the commit (and any DuckLake-resident sink writes) inside one transaction; this is inherited from DuckLake's snapshot isolation, not added by us. The lost-lease commit path is covered by `test/smoke/lease_multiconn_smoke.py`. A deeper exactly-once sink-transaction example lands with the binding / client slices in Phases 3 / 4 — that is where the wire protocol for an in-DuckLake sink table is decided.

### Primitive: `cdc_wait`

- [x] `cdc_wait(catalog, consumer, timeout_ms := 30000)` blocks until an external DuckLake snapshot exists after `last_committed_snapshot`, then returns the new current external snapshot. Returns `NULL` on timeout. Explicit `catalog` is the shipped v0.1 call form until qualified-call sugar is solved for this extension.
- [x] **Hard cap on `timeout_ms`: 5 minutes (300 000 ms)**. Larger values are clamped to the cap and the call emits a `CDC_WAIT_TIMEOUT_CLAMPED` stderr notice carrying the requested value, the cap, and the `SET ducklake_cdc_wait_max_timeout_ms = ...` recovery line per `docs/errors.md`. Session-wide configurability of the cap itself remains a follow-up; today the cap is fixed at 300 000 ms. Why a cap: users will pass `0` or `INT_MAX` thinking "wait forever," and a long-poll-forever in a busy DuckDB process holding a connection is exactly the connection-starvation foot gun documented below. **Default of 30s is conservative on purpose.**
- [x] **Polling backoff curve:** start at 100ms, double on idle, cap at 10s. (Not 1s as previously specified — at 50 idle consumers polling every second, that's 50 catalog queries per second forever. 10s cap is a much better default.) Session configurability via `SET ducklake_cdc_wait_max_interval_ms = ...` is deferred.
- [x] **Reset to 100ms on each wake** so the next `cdc_wait` after activity is responsive.
- [x] **Best-effort shared-connection warning.** On the first `cdc_wait` call from a given DuckDB connection, the extension emits a one-time `CDC_WAIT_SHARED_CONNECTION` notice via `Printer::Print` carrying the call's `timeout_ms` and the recovery guidance from `docs/operational/wait.md`. Tracked per-connection through a process-local `unordered_set<int64_t>` so a long-running connection that re-enters `cdc_wait` does not spam the notice. The catch is intentionally broad — we cannot read DuckDB's connection-state introspection from inside the extension reliably enough to fire only on truly-shared connections, so we always emit on first use and document that as the contract.
- [x] **Interrupt handling is the most likely subtle bug in the whole MVP.** Every poll iteration checks DuckDB's interrupt flag, not just the SQL boundary. Dedicated interrupt test ships in `test/smoke/cdc_wait_interrupt_smoke.py`: a worker thread starts `cdc_wait('lake', 'iw_consumer', timeout_ms => 60000)`; the main thread calls `Connection::Interrupt()` after 200ms; the wait must return with an interrupt-shaped error within `WAIT_DEADLINE_MS = 3000`. Last measured locally: 156ms (interrupt → return).
- [x] Document explicitly in `docs/operational/wait.md`:
  - **TOP-OF-PAGE WARNING:** `cdc_wait` holds a DuckDB connection for its entire duration (up to the timeout cap). **Hold a dedicated connection for `cdc_wait`. Calling it from a pooled / shared connection can starve other queries on the same pool.** The Python and Go clients open a dedicated connection internally; SQL-CLI users must take responsibility for this themselves.
  - Default timeout (30s) is intentionally conservative. The hard cap (5 min) is also intentional.
  - Postgres-backed catalogs: each poll is a `SELECT max(snapshot_id) FROM ducklake_snapshot`. Compute load is bounded but real.
  - The honest truth: when DuckLake gains a notification mechanism upstream, we'll switch from polling to subscribing. That's tracked in `docs/upstream-asks.md`; not in MVP. The truly-async escape hatch (`cdc_wait_async` returning a poll-able handle) is a v1.0 API discussion, not v0.1.

### Primitive: `cdc_ddl`

This is the new primitive promoted from Phase 2 per ADR 0008. It cannot be expressed as a one-line composition over DuckLake builtins because typed reconstruction of `created_table` / `altered_table` / etc. requires querying `ducklake_table` / `ducklake_column` / `ducklake_schema` / `ducklake_view` with version-range awareness.

**Two-stage extraction (per pillar 9):** the implementation has two distinct query stages, and reviewers should expect to see them as separate code paths:

1. **Stage 1 — discover.** Read `snapshots().changes` for each snapshot in the window. Inspect the MAP for the discovery keys (`schemas_created`, `tables_created`, `views_created`, `tables_altered`, `views_altered`, `schemas_dropped`, `tables_dropped`, `views_dropped` — exact set locked by the Phase 0 enumeration spike). This stage tells us *which* objects changed in *which* snapshot.
2. **Stage 2 — reconstruct.** For each `(snapshot_id, object_id)` discovered in stage 1, query the versioned catalog tables (`ducklake_table`, `ducklake_column`, `ducklake_view`, `ducklake_schema`) using their `begin_snapshot` / `end_snapshot` ranges to compute the typed `details` payload. Column-level diff for `altered.table` is computed by comparing `ducklake_column` rows valid at `snapshot_id - 1` and at `snapshot_id`. Table-level rename is detected by comparing `ducklake_table.table_name` for the same `table_id` across the two snapshots.

A reader of pillar 9 might assume `cdc_ddl` is one query against one source. It is not. Stage 2 is what makes it a primitive (and not sugar) — that's the version-range-aware reconstruction that no one-liner can express.

- [x] `cdc_ddl(catalog, consumer, max_snapshots := 100)` foundation table function returning typed DDL events for the consumer's window. Explicit `catalog` is the shipped v0.1 call form until qualified-call sugar is solved for this extension:

  ```text
  snapshot_id   BIGINT
  event_kind    VARCHAR  -- created | altered | dropped  (per spec: RENAME = ALTER)
  object_kind   VARCHAR  -- schema | table | view
  schema_id     BIGINT   -- NULL for created.schema
  schema_name   VARCHAR
  object_id     BIGINT
  object_name   VARCHAR  -- post-event name (for an altered.table that includes a rename, this is the new name; the old name is in details.old_table_name)
  details       JSON     -- shape varies by (event_kind, object_kind); locked in ADR 0008
  ```

- [x] **Implementation foundation + rich Stage-2 payloads.** Stage 1: per-snapshot scan of `ducklake_snapshot_changes.changes_made` for `created_schema` / `created_table` / `altered_table` / `created_view` / `dropped_schema` / `dropped_table` / `dropped_view` tokens. Stage 2: typed `details` JSON payloads for the events implemented in the current tree:
  1. **`created.table`** — `{"columns":[{"id":N,"order":N,"name":"...","type":"...","nullable":bool,"default":...,"parent_column":N?},...]}` enumerating live columns at the create snapshot via `LookupTableColumnsAt`.
  2. **`altered.table`** — `{"old_table_name":"...","new_table_name":"...","added":[...],"dropped":[...],"renamed":[...],"type_changed":[...],"default_changed":[...],"nullable_changed":[...]}`. Empty groups are omitted, so the typical single-column ALTER produces a small payload. The diff is computed by `LookupTableColumnsAt(snapshot_id - 1)` vs `LookupTableColumnsAt(snapshot_id)` and matched on `column_id` (DuckLake preserves the id across renames / type changes / default changes / nullable flips).
  3. **`created.view`** — `{"definition":"...","dialect":"duckdb","column_aliases":"..."}` from `ducklake_view`. `altered.view` is not emitted by DuckLake — `CREATE OR REPLACE VIEW` shows up as a paired `dropped.view` + `created.view` in the same snapshot, which is arguably more correct.
  4. **Finding-1 rename detection (ADR 0008)** — a `created_table:"X"."Y"` token whose `table_id` already had a different name in any earlier snapshot is surfaced as `altered.table` (not `created.table`) with the rename pair populated, so downstream consumers see one continuous identity per table across renames.
  5. **DuckLake's `end_snapshot` is exclusive** — the snapshot at which a row becomes invisible. Our predicates use `begin <= N AND (end IS NULL OR end > N)` to match `SELECT ... AT (VERSION => N)` semantics. Type names and DEFAULT expressions in `details` come from the metadata catalog verbatim (lowercase types like `int32`/`varchar`; DEFAULT expressions stored without surrounding quotes).
  6. **Schema-boundary surfacing** — when `cdc_window` returns an empty yield-before-boundary window (`schema_changes_pending = true`), `cdc_ddl` still emits the boundary snapshot so callers can read and commit the DDL event.
- [ ] Switch Stage 1 from `ducklake_snapshot_changes.changes_made` text scan to the typed `<lake>.snapshots().changes` MAP form once we have a Phase-1-stable upstream contract for the MAP shape.
- [x] Order results within a snapshot deterministically per ADR 0002 / 0008. The shipped extractor sorts each snapshot's DDL rows by a stable `(DdlObjectKindRank(event_kind, object_kind), object_id)` key — `created` events emit `schema → view → table`; `dropped` events reverse the order so children disappear before their parents in the row stream. `test/sql/ddl_stage2.test` pins the contract with a `BEGIN; CREATE SCHEMA; CREATE TABLE; COMMIT;` snapshot.
- [ ] Performance shortcut — Stage 2 only. If `ducklake_schema_versions` shows no per-relevant-table schema change in the window, the column-diff reconstruction for `altered.table` can be skipped. Stage 1 is never gated on `ducklake_schema_versions` (created/dropped of schemas/views can land without bumping schema versions).
- [x] Honors the consumer's `event_categories` filter: returns zero rows if `event_categories = ['dml']` (and `cdc_changes` returns zero rows for `event_categories = ['ddl']`). The cursor still advances through the window because `cdc_window` runs ahead of the filter — the projection is post-hoc.
- [x] **Excludes `compacted_table`** from output (it's maintenance, not user-relevant DDL). The Stage-1 token scan only recognises `created_*` / `altered_*` / `dropped_*` for `{schema, table, view}`; `compacted_table:<id>` and the MAP-side `merge_adjacent` token are intentionally absent from the prefix list. Operators monitoring compactions read `cdc_events`. Pinned by `test/sql/ddl_stage2.test`'s "compaction" section, which runs `ducklake_merge_adjacent_files` mid-stream and asserts only the `created.table` event surfaces.
- [ ] Handle nested-column diffs (where `parent_column IS NOT NULL`) inside `altered.table.details` — currently emitted as a flat `parent_column` field in each ColumnInfo JSON, but the diff itself does not yet normalise parent/child reordering.

### Acknowledged sugar wrappers

These exist for ergonomics. Each is documented in `docs/api.md` as "1-line composition over X and Y". They do **not** auto-commit. They **do** apply the consumer's `change_types` filter as a `WHERE` clause when set.

- [x] `cdc_events(catalog, consumer, max_snapshots := 100)` — composes `cdc_window` with `__ducklake_metadata_<lake>.ducklake_snapshot` JOIN `ducklake_snapshot_changes`, so the row shape is `(snapshot_id, snapshot_time, changes_made, author, commit_message, commit_extra_info, next_snapshot_id, schema_version, schema_changes_pending)`. The shipped foundation reads `changes_made` as raw text (the typed `<lake>.snapshots().changes` MAP form lands once we switch the upstream source). It acquires/extends the lease the same way `cdc_window` does, filters out internal `__ducklake_cdc_*` writes so consumer-state bootstrap rows never appear, and applies the consumer's `tables` filter per-snapshot when set (translating `inserted_into_table:<id>` / `tables_inserted_into:<id>` / `inlined_insert:<id>` / `created_table:"schema"."name"` / similar tokens to fully-qualified table names against `ducklake_table`/`ducklake_schema`). Does **not** auto-commit. SQLLogic coverage in `test/sql/sugar.test`.
- [x] `cdc_changes(catalog, consumer, table, max_snapshots := 100)` — dynamic-schema sugar over the underlying DuckLake `<lake>.table_changes(...)`. The shipped foundation discovers the table's columns via a `LIMIT 0` probe at `current_snapshot()` during Bind so the planner sees the real schema, then issues the materialised query at Init from the lease-held window. Returns the underlying `table_changes` columns `(snapshot_id, rowid, change_type, <table cols>)` followed by `(snapshot_time, author, commit_message, commit_extra_info, next_snapshot_id)`. Honors the consumer's `change_types` filter as a `WHERE` clause when set; **never** filters `update_preimage` implicitly. Calls against a table that is not in the consumer's `tables` filter fail loudly with `cdc_changes: table 'X' is not in consumer 'Y' tables filter` rather than returning silently empty results. Does **not** auto-commit. SQLLogic coverage in `test/sql/sugar.test`.
- [x] `cdc_recent_changes(catalog, table, since_seconds := 300)` — stateless passthrough to the upstream `<lake>.table_changes(...)` over the snapshot range `[max(snapshot_id WHERE epoch(snapshot_time) <= epoch(now()) - since_seconds), current_snapshot()]`. Reuses the dynamic-schema probe pattern from `cdc_changes` — `LIMIT 0` at `current_snapshot()` discovers the table's columns at Bind time. Does **not** require the CDC state tables (no bootstrap). Returns `(snapshot_id, rowid, change_type, <table cols>, snapshot_time, author, commit_message, commit_extra_info)`; there is no `next_snapshot_id` because there is no consumer cursor. The `INTERVAL` form is deferred (DuckDB has no `-(TIMESTAMP_TZ, INTERVAL)` binder; the function uses epoch-second math instead). SQLLogic coverage in `test/sql/recent_sugar.test`.
- [x] `cdc_schema_diff(catalog, table, from_snapshot, to_snapshot)` — stateless inspection helper that resolves the `schema.table` filter to the *set* of `table_id` values that ever bore that qualified name within `[from_snapshot, to_snapshot]`, then runs the same Stage-1 token scan as `cdc_ddl` and matches by `table_id` (not by name) so renames keep the timeline coherent. Returns one row per column-level change with `(snapshot_id, snapshot_time, change_kind, column_id, old_name, new_name, old_type, new_type, old_default, new_default, old_nullable, new_nullable)`. Pure CREATE TABLE events surface as a series of `added` rows; table renames as a `table_rename` row with old/new names in the name fields. Negative or inverted ranges are rejected at bind. Does **not** require the CDC state tables. SQLLogic coverage in `test/sql/ddl_stage2.test`.
- [x] `cdc_recent_ddl(catalog, since_seconds := 86400, for_table := NULL)` — stateless DDL companion to `cdc_recent_changes`, reusing the same Stage-1/Stage-2 DDL extractor as `cdc_ddl` so the rich payloads land here at the same time. The optional table filter is named `for_table` (not `table`) because `table` is reserved in DuckDB's parser; unqualified names auto-qualify to `main.<name>`. Schema-level events are dropped when `for_table` is set. Does **not** require the CDC state tables. SQLLogic coverage in `test/sql/recent_sugar.test`.

### `max_snapshots` hard cap (per ADR 0011)

- [x] `cdc_window`'s `max_snapshots` arg is hard-capped at the compile-time constant `HARD_MAX_SNAPSHOTS = 1000`. Values above the cap raise `CDC_MAX_SNAPSHOTS_EXCEEDED` (per `docs/errors.md`); covered in `test/sql/consumer_state.test` (`max_snapshots = 1001 → CDC_MAX_SNAPSHOTS_EXCEEDED`). Session-tunable form (`SET ducklake_cdc_max_snapshots_hard_cap = ...`) is deferred until the session-settings/notice helper lands; the error message already advertises the planned `SET` for forward-compat.

### Benchmark harness

Numbers without history are flat; numbers with history are a story. Phase 1 ships the first fast benchmark harness so every main CI run proves the CDC path is measurable before later phases add longer runs.

- [x] **Harness skeleton** in `bench/`:
  - `bench/runner.py` Python harness: seeds a DuckLake with a controllable producer, runs a consumer with workload parameters `(duration_seconds, snapshots_per_minute, rows_per_snapshot, consumers, max_snapshots, polling_config)`, captures **end-to-end latency p50/p95/p99/max/mean** (via a monotonic-time ledger so cross-process clock skew is irrelevant), **events/sec**, **catalog QPS**, **lag drift over the run** (producer-truth lag, since `cdc_consumer_stats.lag_snapshots` over-counts internal lease/commit UPDATEs). Consumer CPU/RSS is deferred to a future revision — we get the headline metrics this phase needs without a `psutil` dependency. The runner's shape must make load profiles easy to add later; Phase 1 only needs constant-rate parameters.
  - `bench/light.yaml` workload descriptor — a fast smoke benchmark, not a soak: 60 seconds, 30 snapshots/min, 100 rows/snapshot, 1 consumer. The CI run uses this descriptor as-is; longer runs and variable-load profiles land after the harness has earned trust.
  - `bench/results/` — JSON result location with `(run_id, timestamp_utc, commit, hardware_label, workload, measurements)`. CI uploads each result as an artifact; maintainers commit selected baselines when they want an auditable trajectory. Per ADR 0011: committed regressions are explained in commit messages, not deleted.
- [x] **CI gate (Phase 1, against light only) — soft, not a build break.**
  - **Hard requirement (build-break):** the harness ran successfully and produced a number. Wired into Full CI as `benchmark-smoke` — the workflow runs `make release` then `uv run --locked python bench/runner.py --build release --workload bench/light.yaml`, fails the build if the runner exits non-zero, and uploads the result file as a workflow artifact even on success.
  - **Soft requirements (warn, don't fail):** the runner emits `::warning::` annotations when `lag_snapshots_max > 0` or `catalog_qps_avg > 5`. These never fail the build in Phase 1; reviewers read the annotations and the result file's `soft_gates` block to decide whether to act.
  - **Latency:** recorded in every result (`p50` / `p95` / `p99` / `max` / `mean`) but not gated. Per the ADR 0011 + Phase 5 hand-off: the absolute target only becomes a hard CI gate after Phase 5 ratifies the production number on representative hardware.
  - Why this discipline: if Phase 1 committed to a 1s p99 hard gate and the first run got 1.2s, the release notes would read "we said p99 < 1s, we got 1.2s" on day one. Soft-gate now, ratify in Phase 5, treat as contract from beta forward.
- [x] **Bench-history page (manual for now):** `bench/README.md` is hand-written for now (covers the harness, the workload, the metrics, and how to read the JSON). Auto-generation from `bench/results/*.json` — trajectory charts per metric per workload — lands once there are enough committed data points to chart. Phase 5 hardens the auto-generation; Phase 1 keeps it manual because two data points are not yet a trajectory.
- [ ] Phase 2 adds `bench/medium.yaml` and runs it across the catalog matrix. Phase 5 adds `bench/heavy.yaml`, variable-load profiles, and the long-running soak / sustained-load jobs.

### Observability

- [x] `cdc_consumer_stats(catalog, consumer := NULL)` — read-only table
  function. Computed in a single SQL pass that LEFT JOINs
  `__ducklake_cdc_consumers` to `ducklake_snapshot` for `lag_seconds`,
  then derives `tables_unresolved` and `lease_alive` per row in the
  C++ scan. Never acquires the consumer lease, never advances the
  cursor, never writes beyond the implicit consumer-state bootstrap.
  Implemented in the current tree.
  - `consumer_name`, `consumer_id` from the consumer row.
  - `last_committed_snapshot`, `current_snapshot`, `lag_snapshots`,
    `oldest_available_snapshot`, `gap_distance` for cursor / `CDC_GAP`
    monitoring.
  - `lag_seconds` (DOUBLE; NULL when the cursor predates the catalog
    or has never been committed).
  - `tables` / `change_types` mirror the consumer's filter
    (NULL = no filter).
  - `tables_unresolved` is the per-consumer subset of `tables` not
    present at `current_snapshot` (ADR 0009).
  - `owner_token`, `owner_acquired_at`, `owner_heartbeat_at`,
    `lease_interval_seconds`, `lease_alive` for lease state.
  - **Deferred to PR 4 / Phase 2:** `lag_human`, `events_consumed_total`,
    `commits_total`, `gaps_total`, `schema_changes_total`,
    `wait_calls_total`, `wait_timeouts_total`, `last_poll_at`,
    `last_commit_at`, DLQ-derived counters,
    `current_max_snapshots_cap`.
- [x] `cdc_audit_recent(catalog, since_seconds := 86400, consumer := NULL)`
  — read-only window over `__ducklake_cdc_audit`. Returns one row per
  lifecycle event (`consumer_create`, `consumer_reset`,
  `consumer_drop`, `consumer_force_release`, `lease_force_acquire`)
  newest first; rejects negative `since_seconds`. Implemented in the current tree.

### Tests

Structured as "primitive in isolation" + "sugar wrapper composition" + "the things that always break".

- [x] **Primitive in isolation:**
  - `cdc_window` returns the right bounds at the start, in the middle of a stream, and at end-of-stream — `test/sql/consumer_state.test`.
  - `cdc_window` is **idempotent**: two consecutive calls without `cdc_commit` return identical rows — `test/sql/consumer_state.test:81-89`.
  - `cdc_window` raises `CDC_GAP` after compaction expires the cursor; `cdc_consumer_reset` recovers; the error message includes the recovery command verbatim — `test/sql/consumer_state.test` + `test/smoke/toctou_expire_smoke.py`.
  - `cdc_window` **bounds at schema-version boundaries** and sets `schema_changes_pending` correctly when the next snapshot is at a different `schema_version` — `test/sql/consumer_state.test:97-100` + `test/sql/always_breaks.test` (both `stop_at_schema_change` modes).
  - `cdc_window` **TOCTOU race**: parallel `ducklake_expire_snapshots` while a consumer is mid-stream does not produce silently wrong results — either it succeeds with consistent data or it raises `CDC_GAP`. Embedded-DuckDB lane shipped in `test/smoke/toctou_expire_smoke.py`; Phase 2 carries the same harness onto the Postgres / SQLite catalog lanes.
  - `cdc_wait` returns promptly when a new snapshot lands; returns `NULL` on timeout — `test/sql/consumer_state.test`.
  - `cdc_wait` is **interruptible** within hundreds of milliseconds via DuckDB's interrupt mechanism — `test/smoke/cdc_wait_interrupt_smoke.py` (last measured: 156ms interrupt → return).
  - `cdc_wait` backoff curve: start 100ms, cap 10s, reset on activity. Implementation pinned in `WaitForSnapshot`; explicit timing-assertion test deferred (test would be flaky in CI; the implementation is small enough to read at face value).
  - `cdc_ddl` returns typed events for every DDL kind: `created.{schema,table,view}`, `altered.{table,view}` (covering RENAME TABLE, RENAME COLUMN, ADD/DROP COLUMN, SET TYPE, default changes, view-def changes), `dropped.{schema,table,view}`. `details` JSON shape matches the ADR 0008 spec — `test/sql/ddl_stage2.test`. `altered.view` is not emitted by DuckLake (CREATE OR REPLACE shows up as paired drop+create).
  - `cdc_ddl` does **not** emit `compacted_table` events — `test/sql/ddl_stage2.test` "compaction" section runs `ducklake_merge_adjacent_files` mid-stream and asserts only the `created.table` event surfaces.
  - `cdc_ddl` honors `event_categories := ['dml']` (zero DDL rows) and `event_categories := ['ddl']` (returns events; `cdc_changes` returns zero) — `test/sql/notices_validation.test`.
  - `cdc_ddl` orders events within a snapshot deterministically: `(object_kind_rank, object_id)`, `created.schema → created.view → created.table`, `dropped` reversed — `test/sql/ddl_stage2.test` "DDL ordering" section pins the contract with a `BEGIN; CREATE SCHEMA; CREATE TABLE; COMMIT;` snapshot.
  - `cdc_schema_diff` (sugar) correctly identifies add / drop / rename / type-change for top-level columns — `test/sql/ddl_stage2.test`. Nested-column diffs remain a follow-up (called out under `cdc_ddl` above).
  - `cdc_schema_diff` is fast (zero `ducklake_column` scan) when `from`/`to` share a `schema_version`. Performance shortcut for Stage 2 is the open follow-up under `cdc_ddl`; the diff API itself works correctly for both shared and differing `schema_version`s today.
- [x] **The things that always break:**
  - **Inlined-data path.** `test/sql/always_breaks.test` exercises `DATA_INLINING_ROW_LIMIT 100`, asserts `cdc_events` discovers the inlined snapshot and `cdc_changes` returns the rows, then forces a materialised flush in a follow-up snapshot and asserts both surface seamlessly.
  - **All four change types pass through unfiltered.** `test/sql/always_breaks.test` covers the default consumer (4 events: insert / update_preimage / update_postimage / delete) plus a `change_types := ['delete']` consumer (1 event). No implicit `update_preimage` filtering.
  - **Mixed inlined + materialised in one window.** Same `inline_consumer` section in `test/sql/always_breaks.test`.
  - **Schema change mid-window (default `stop_at_schema_change=true`).** `test/sql/always_breaks.test` "schema_default" section: first window bounded before the ALTER, `schema_changes_pending=true`, `cdc_changes` returns OLD-schema rows; commit, then the next window starts AT the ALTER snapshot, `cdc_ddl` returns the typed `altered.table` event, `cdc_changes` returns NEW-schema rows.
  - **Schema change with `stop_at_schema_change=false`.** "schema_optout" section in `test/sql/always_breaks.test`: one window spans both schema versions; `cdc_ddl` and `cdc_changes` both return rows; documented behaviour pinned.
  - **DDL+DML in one snapshot, ordering enforced.** "ddl_dml" section in `test/sql/always_breaks.test`: `BEGIN; ALTER; INSERT; COMMIT;` lands as one snapshot id; `cdc_ddl` and `cdc_changes` both report that snapshot id; the typed-DDL row is sorted ahead of the DML row inside the snapshot per the `(object_kind_rank, object_id)` contract.
  - **Column-drop audit limitation, documented behaviour.** "drop_audit" section in `test/sql/always_breaks.test`: drop+delete in one snapshot; `cdc_changes` returns the deletes without the dropped column; test pins the absence so we don't silently regress. `docs/operational/audit-limitations.md` documents the recovery query.
  - **DDL-only consumer / DML-only consumer.** `event_categories := ['ddl']` and `event_categories := ['dml']` covered in `test/sql/notices_validation.test`; cursor still advances; the filter is a post-hoc projection.
  - **Dropped-table policy.** "dropped-table" section in `test/sql/always_breaks.test`: consumer with `tables := ['main.orders']`, table dropped, consumer keeps advancing; `cdc_consumer_stats().tables_unresolved` lists `main.orders`; `cdc_ddl` returns the `dropped.table` event.
  - **`commit_message` / `commit_extra_info`** end-to-end propagation pinned by the "commit_meta" section in `test/sql/always_breaks.test` (both `cdc_events` and `cdc_changes` carry the fields per row).
- [x] **Multi-consumer:**
  - Multiple consumers on the same lake advance independently — "multi-consumer" section in `test/sql/always_breaks.test` (consumer A advances; consumer B's cursor stays put).
  - Two consumers writing to `__ducklake_cdc_consumers` concurrently — DuckLake's own snapshot-id arbitration handles it; no extension-side coordination needed. Phase 2 expands this to the Postgres / SQLite catalog matrix where the arbitration is a different code path.
- [x] **Single-reader-per-consumer (enforced via the owner-token lease):**
  - **Same-connection idempotence** — `test/sql/consumer_state.test`: two consecutive `cdc_window` calls without `cdc_commit` return identical rows; the lease UPDATE is a no-op on the second call because `owner_token = :calling_token` is already true.
  - **Different-connection rejection** — `test/smoke/lease_multiconn_smoke.py`: A holds the lease via `cdc_window`; B's `cdc_window` raises structured `CDC_BUSY`; the message exposes A's `owner_token`, `owner_acquired_at`, and `owner_heartbeat_at`.
  - **Force release + stolen-lease commit fails** — `test/smoke/lease_multiconn_smoke.py`: B's `cdc_consumer_force_release` succeeds while A holds the lease; A's later `cdc_commit` raises `CDC_BUSY`; cursor unchanged; audit row landed.
  - **Lease timeout / heartbeat extension / symmetric stolen-lease commit / orchestrator fan-out** — implementation pinned in `src/consumer_state.cpp` (lease predicate is symmetric; heartbeat is a single UPDATE with the cached token; orchestrator-fan-out reuses the same connection's cached token). Dedicated multi-process spike harnesses for the three time-pressure scenarios are deferred to Phase 2's catalog matrix work, where the same harness has to run against Postgres / SQLite anyway.

### The SQL walkthrough + the README quickstart (NEW exit gate)

This phase doesn't ship until the SQL walkthrough runs end-to-end **and the README quickstart matches it verbatim**. The README is what you point people at when you ask for early feedback. If the README is wrong (or aspirational) until Phase 5, every reader between now and then is forming their first impression from a misleading description. Phase 5's previous "60-second README quickstart" work item is moved here.

**Honest preconditions** must be in the README and the walkthrough. The previous "INSTALL ducklake_cdc FROM community; LOAD ducklake_cdc; ATTACH ... CALL ..." undersold the actual ceremony, which includes installing and loading the `ducklake` extension itself. Don't lie about line counts.

The honest minimum:

```sql
-- Preconditions: DuckDB >= <our supported floor>, ducklake extension available in the
-- community repo (it is), ducklake_cdc available in the community repo (it will be after
-- Phase 5 distribution; for Phase 1 use the local build).
INSTALL ducklake;
INSTALL ducklake_cdc FROM community;  -- or `LOAD 'build/release/ducklake_cdc.duckdb_extension';` during MVP
LOAD ducklake;
LOAD ducklake_cdc;
ATTACH 'ducklake:demo.ducklake' AS lake;

-- Stateless first taste — no consumer needed.
SELECT * FROM cdc_recent_ddl('lake', since_seconds => 86400);          -- sugar over cdc_ddl with bare snapshot bounds
SELECT * FROM cdc_recent_changes('lake', 'orders', since_seconds => 300); -- sugar over table_changes with bare snapshot bounds

-- Durable consumer with both DDL and DML.
SELECT * FROM cdc_consumer_create('lake', 'demo', start_at => 'now');

-- In another terminal:
--   INSERT INTO lake.main.orders VALUES (1, 100), (2, 200);
--   ALTER TABLE lake.main.orders ADD COLUMN region VARCHAR DEFAULT 'UNKNOWN';
--   INSERT INTO lake.main.orders VALUES (3, 300, 'EU');

-- Back here:
SELECT * FROM cdc_window('lake', 'demo');
-- → start, end, schema_version, schema_changes_pending=true (DDL on next window)

SELECT * FROM cdc_changes('lake', 'demo', 'orders');
SELECT * FROM cdc_commit('lake', 'demo', /* end_snapshot from cdc_window above */);

-- Next window starts at the ALTER snapshot under v_schema=N+1:
SELECT * FROM cdc_window('lake', 'demo');
SELECT * FROM cdc_ddl('lake', 'demo');                 -- typed altered.table event with details
SELECT * FROM cdc_changes('lake', 'demo', 'orders');   -- the EU insert under the new schema
SELECT * FROM cdc_commit('lake', 'demo', /* end_snapshot above */);

-- Long-poll for the next change
SELECT cdc_wait('lake', 'demo', timeout_ms => 30000);
```

**Test the walkthrough on a clean machine** — fresh DuckDB install, no prior extension cache, no environment hand-holding. If a developer who hasn't seen the project before can't go from `INSTALL` to seeing both DDL and DML events by following the README verbatim, the walkthrough isn't ready.

The README's headline-style `cdc_changes('lake', 'demo', 'orders')` four-line teaser (lede block) is also valid SQL — it requires the same preconditions as the full quickstart, and the README must say so in a sentence right after it. Don't pretend the four lines stand alone; readers will copy-paste them and bounce when they fail.

`cdc_recent_changes` and `cdc_recent_ddl` in this walkthrough are **sugar passthroughs**, not new SQL primitives — they expand to `table_changes(...)` and the stage-2 DDL extractor with bare snapshot bounds. Phase 0 / 1 docs label them as such; the CLI sections in Phase 3 / 4 must do the same and not present them as new functions.

## Exit criteria

- [ ] `INSTALL` from a local build works in `duckdb` CLI.
- [ ] The SQL walkthrough above runs end-to-end against a fresh embedded DuckLake.
- [x] **The README quickstart exists and matches the local-build preconditions.** A cold-machine test still needs to confirm it works in under a minute including extension installs.
- [x] `examples/01_basic_consumer.sql` (uses `cdc_window` + `table_changes` + `cdc_commit` directly).
- [x] `examples/02_outbox_demo.sql` (uses sugar `cdc_events` to dispatch on `commit_extra_info`).
- [x] `examples/03_long_poll.sql` (uses `cdc_wait` for a streaming consumer).
- [x] `examples/04_schema_change.sql` (demonstrates `schema_changes_pending`, `cdc_ddl`, and the schema-boundary flow end-to-end).
- [x] `examples/05_recent_changes.sql` (stateless ad-hoc exploration with `cdc_recent_changes` and `cdc_recent_ddl`).
- [x] `examples/06_parallel_readers.sql` (orchestrator-fans-out pattern: one connection holds the lease + calls `cdc_window`, two more connections run `table_changes` reads in the same window range, single commit).
- [x] `examples/07_ddl_only_consumer.sql` (consumer with `event_categories := ['ddl']` for the schema-watcher pattern).
- [x] `examples/08_cross_schema_window.sql` (consumer with `stop_at_schema_change := false` for the throughput-over-safety case; demonstrates the documented behaviour).
- [x] `examples/09_lease_recovery.sql` (kill the holder mid-stream; show another connection acquiring after the heartbeat times out; show the audit row).
- [ ] All tests green on CI, including:
  - **The TOCTOU race test, deterministic interleaving** (per the spelled-out work item above).
  - **The lease test suite** (same-connection idempotence, different-connection rejection, lease timeout, stolen-lease commit fails, force release, heartbeat extension, orchestrator fan-out).
  - The inlined-data path test.
  - The schema-version-boundary test (both `stop_at_schema_change` modes).
  - The DDL+DML same-snapshot ordering test.
  - The column-drop audit-limitation documented-behaviour test.
  - The DDL-only and DML-only consumer tests.
  - The interrupt-during-`cdc_wait` test.
  - The `cdc_wait` timeout-clamp test.
- [x] **Two-screenshot DDL-discovery + row-level CDC walkthrough committed under `docs/demo/phase1/`.** This is the README-sized Phase 1 asset: one producer-side screenshot showing inserts, one update, and one delete; and one consumer-side screenshot showing `cdc_recent_ddl` discovering the table and durable `cdc_changes` returning the row-level events for that table. The full cursor loop (`cdc_window` / `cdc_changes` / `cdc_commit`) stays in the quickstart and examples; the README image uses the smallest stable SQL surface. A moving GIF/video is deferred until Phase 3 or 4, when a Python or Go client can stream ordered events to JSONL and make the live experience visually obvious.
- [ ] **At least four release tags between the first `v0.0.x` tag and the Phase 1 cut**, with honest "what works / what doesn't" release notes for each. Demonstrates the visible-cadence discipline that Phase 5 launch depends on.
- [x] Zero runtime dependency on Python.
- [x] `docs/api.md` lists every function with its classification (`primitive` / `observability` / `acknowledged sugar`) and, for sugar, the SQL recipe it composes. `cdc_ddl` is listed as a primitive; `cdc_schema_diff`, `cdc_recent_changes`, `cdc_recent_ddl` are listed as sugar.
- [x] `docs/errors.md` lists every error code (`CDC_GAP`, `CDC_INVALID_TABLE_FILTER`, `CDC_BUSY` (lease), `CDC_SCHEMA_BOUNDARY` notice, `CDC_WAIT_TIMEOUT_CLAMPED` notice, `CDC_WAIT_SHARED_CONNECTION` warning) with example messages.
- [x] `docs/operational/audit-limitations.md` documents the column-drop-with-DML-in-same-snapshot loss. **Leads with a runnable worked example** showing the workload, what `cdc_changes` returns, the `tbl AT (VERSION => snapshot_before_drop)` recovery query, and the JOIN that reconciles the dropped column back onto the delete events. Recovery comes before rationale.
- [x] `docs/operational/lease.md` documents the owner-token lease, heartbeat semantics, force-release escape hatch.
- [x] `docs/operational/wait.md` documents the `cdc_wait` connection-starvation foot gun and the timeout cap.
- [x] `bench/runner.py` + `bench/light.yaml` ship and run on every Full CI build against embedded DuckDB. CI uploads result JSON as an artifact; selected baselines can be committed to `bench/results/` once there is a trajectory worth preserving. **The harness is the deliverable** — bad numbers are acceptable in Phase 1 because we now have a way to show improvement.
- [x] `__ducklake_cdc_dlq` table schema is created (empty, feature lands Phase 2).
- [x] `__ducklake_cdc_audit` table is created and emits Phase-1 actions (`consumer_create`, `consumer_drop`, `consumer_reset`, `consumer_force_release`, `lease_force_acquire`).
