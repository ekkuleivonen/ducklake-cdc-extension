# Phase 2 — Catalog matrix, DDL-event coverage, special types, DLQ, doctor

**Goal:** the same extension behaves identically against every DuckLake-supported catalog backend, our `cdc_ddl` primitive (and the `cdc_schema_diff` sugar that shares its extraction logic) is exhaustively tested against the spec's own schema-evolution rules, the dead-letter-queue feature lands on top of the schema we locked in Phase 1 (with the explicit DDL-vs-DML failure-mode policy), special types (`variant`, `geometry`) have a documented serialization contract for downstream sinks, and the **`doctor` command** ships so the rest of the project can use it during testing.

The doctor command was previously in Phase 5. It moves here because it pays for itself the moment there are any users at all (or even any developers debugging the catalog matrix). Building it early means it's a real tool by the time Phase 5's chaos suite needs it; building it late means Phase 5 testers debug by hand for months.

This phase is deliberately not about reimplementing schema evolution, encryption, or compaction handling. DuckLake handles all of those on the read path through `table_changes`. Our job is to (a) prove we don't break that handling on each catalog backend, (b) prove typed DDL extraction returns identical typed events on every backend, (c) ship the small typed helpers (special-type serializers) that downstream consumers genuinely need, (d) give consumers a way to keep moving when individual events fail downstream **without letting one bad DDL cause a DLQ flood**, and (e) give operators the doctor command early.

## Work items

### Phase 1 deferrals folded in

These three items were deferred from Phase 1 with rationale captured in the Phase 1 closure note. They are tracked here so the next-phase reviewer doesn't have to cross-reference Phase 1 to find them — each one is reinforced by an existing Phase 2 work item below, but having the deferrals listed up front prevents them from being silently lost.

- [x] **Stage-1 `snapshots().changes` MAP switch.** `cdc_ddl`, `cdc_recent_ddl`, and `cdc_schema_diff` now scan `<lake>.snapshots().changes` via `UNNEST(map_entries(...))` and dispatch on the typed MAP key (`schemas_created` / `tables_created` / `tables_altered` / `views_created` / `schemas_dropped` / `tables_dropped` / `views_dropped`). The text-scan path stays alive only for `cdc_events` (its surface still emits `changes_made` verbatim) and for `ChangesTouchConsumerTables` (per-snapshot consumer-filter eval). Backend-agnostic by construction — the upstream probe in `test/upstream/output/` confirms all three DuckLake backends emit identical MAP key sets. Test coverage in `test/sql/ddl_stage2.test` § `Phase 2 deferral 1`.
- [x] **Stage-2 perf shortcut.** `BuildAlteredTableDetails` and `cdc_schema_diff` consult `ducklake_schema_versions` before issuing the two `LookupTableColumnsAt` + `DiffColumns` reconstructions; when no row exists for `(table_id, begin_snapshot=snapshot_id)` the diff is provably empty and the lookups are skipped. Empirically every Stage-1-relevant `altered_table:<id>` token under DuckDB 1.5.x / DuckLake 4.x is accompanied by a `ducklake_schema_versions` row at that snapshot, so this is *defensively correct* on this DuckLake version rather than a measurable perf win — kept because the cost is one small index lookup and the predicate is the right hook if a future DuckLake ever emits noisy `altered_table:<id>` tokens. Test coverage in `test/sql/ddl_stage2.test` § `Phase 2 deferral 2`.
- [x] **Nested-column diff parent/child reordering normalisation.** `ColumnDiff` carries `parent_column_id`, `DiffsToJson` and the `cdc_schema_diff` per-column rows emit it for `ADDED` / `DROPPED` kinds, and `NormaliseDiffParentChildOrdering` stable-sorts ADDED parents-before-children / DROPPED children-before-parents (mirroring the within-snapshot rank rule cdc_ddl already uses for top-level objects per ADR 0008). Phase 1's emitted JSON field on `created.table.details.columns` was renamed from `parent_column` → `parent_column_id` for consistency with the spec text in ADR 0008 and with the new `cdc_schema_diff.parent_column_id` BIGINT column. Test coverage in `test/sql/ddl_stage2.test` § `Phase 2 deferral 3`, including the cross-product stress test (rename + type change + nested-field add in one transaction) called out below.

### Pre-existing Stage-1 extraction artefacts (surfaced during deferral 3)

The cross-product test for deferral 3 surfaced two pre-existing Stage-1 quirks worth chasing in their own follow-up — neither was correctness-breaking for the column-diff payload, but both confused downstream consumers building per-table identity tracking. Both fixes landed alongside the Phase 1 deferrals because they fall in the same code path and the dual-emission one would start mattering as soon as a downstream consumer keys on `(snapshot_id, object_kind, object_id)`.

- [x] **Dual altered emission at rename snapshots — fixed.** Stage-1 (`ExtractDdlRows`) and `cdc_schema_diff` now order the MAP-entry scan so `tables_created` keys are processed before `tables_altered` within a snapshot (`ORDER BY snapshot_id ASC, CASE e.key WHEN 'tables_created' THEN 0 ELSE 1 END`). When the Finding-1 path promotes a `tables_created` value to `altered.table`, it inserts the resolved `table_id` into a per-snapshot `unordered_set<int64_t>`; the ID-resolved `tables_altered:<id>` branch consults the set and skips emission when it's a hit. The Finding-1 row carries the strictly larger payload (`old_table_name + new_table_name + column diff`), so dropping the ID-resolved sibling loses no information. The set is reset on every snapshot boundary so dedup is local to a single snapshot. Test coverage: new hermetic block in `test/sql/ddl_stage2.test` § `Phase 2 follow-ups #1 (dedup) and #2 (snapshot-bound LookupTableById)` asserts exactly one `altered.table` row at a cross-product snapshot, plus the deferral-3 cross-product block now asserts the same count contract.
- [x] **`LookupTableById` returns post-rename name from "wrong" side — fixed.** `LookupTableById`, `LookupSchemaById`, and `LookupViewById` now take a `snapshot_id` parameter and apply `(begin_snapshot <= snap AND (end_snapshot IS NULL OR end_snapshot > snap))` to both the object row and its joined schema row — the same predicate `LookupTableColumnsAt` already uses for column rows. ALTERED events pass `snapshot_id` (the table is alive at this snapshot, post-ALTER state); DROPPED events pass `snapshot_id - 1` (the row alive immediately before the drop, the one whose `end_snapshot = drop_snap`). The dead `AddDdlRowsForToken` text-token dispatcher was removed in the same commit (it was orphaned after the deferral-1 MAP switch and was the last remaining caller of the snapshot-unbounded form). Test coverage: deferral 3 DROP test now expects `'orders_v2'` (post-rename name) where it previously asserted `'orders'`; deferral 2 ADD COLUMN test likewise; the new follow-ups block asserts both ALTERED-after-rename and DROP-after-rename name resolution explicitly.

### Catalog matrix

- [x] **Catalog matrix smoke wired into CI** (`test/catalog_matrix/catalog_matrix_smoke.py`, runs on every push / PR). Covers the Phase 1 cursor loop (`cdc_consumer_create` → DML → `cdc_window` → `cdc_changes` → `cdc_commit` → ALTER + new-schema DML → `cdc_ddl` → `cdc_changes` over the new schema → final commit) **and** the single-reader lease (`cdc_window` blocks the second reader with `CDC_BUSY`; `cdc_consumer_force_release` invalidates the holder's owner-token so its commit also fails with `CDC_BUSY`) on **DuckDB / SQLite / PostgreSQL** in one CI step. The Postgres leg uses the `test/catalog_matrix/docker-compose.yml` fixture (port 5434 to coexist with the upstream contract probe). The full SQLLogic suite still needs a backend-matrix runner; that's the next bullet.
- [ ] Run the **full Phase 1 SQL test suite** against each DuckLake-supported catalog backend (the smoke is an early signal; the SQLLogic matrix is the actual exit criterion):
  - [ ] DuckDB (embedded)
  - [ ] SQLite
  - [ ] PostgreSQL
- [x] Containerised CI fixtures (`docker compose` for Postgres) — `test/catalog_matrix/docker-compose.yml`.
- [ ] Verify that `__ducklake_cdc_consumers`, `__ducklake_cdc_dlq`, and `__ducklake_cdc_audit` are stored using each backend's documented inlined-type encoding (per the spec's PostgreSQL and SQLite encoding tables) — no DuckDB-only assumptions leak through. Particular attention to `UUID` (`owner_token`) — store as native `uuid` on Postgres, `TEXT(36)` on SQLite where native UUID isn't available, with a portable extraction helper.
- [ ] **Re-run the Phase 1 TOCTOU race test on each backend.** Phase 1's deterministic-interleaving test infrastructure already supports per-backend barriers (Postgres advisory lock, SQLite serial assertion, DuckDB single-writer assertion). Postgres needs `SERIALIZABLE` or `REPEATABLE READ + SELECT FOR SHARE`; SQLite is serial; DuckDB embedded is single-writer. Document the per-backend isolation requirement in `docs/operational/isolation.md`. **No new race-test code in Phase 2** — same code, three backend configs.
- [ ] **Re-run the Phase 1 lease test suite on each backend.** Lease lifetime semantics are now portable (single conditional UPDATE, no per-backend lock primitive). The matrix verifies portability empirically: lease timeout, force release, heartbeat extension, stolen-lease commit-fail. If a backend exposes anomalies (e.g. SQLite's text-encoded TIMESTAMPTZ comparison interacting with the heartbeat predicate), document and adjust the lease comparison precision (always use `>=` with a known floor, never `=`).
- [ ] Document which catalogs support which transaction-isolation guarantees and how that affects exactly-once-on-commit semantics in `docs/operational/isolation.md`. Note that conflict resolution between concurrent consumers writing to `__ducklake_cdc_consumers` is inherited from DuckLake's own `ducklake_snapshot` PRIMARY KEY arbitration.
- [ ] **Catalog-version compatibility matrix** in `docs/compatibility.md`: rows = ducklake-cdc versions, columns = DuckLake catalog-schema versions, cells = supported / unsupported / EOL. Updated every release. Loaded at extension init time as a hard guard (Phase 1 work item).

### DDL-event exhaustive tests

These test both `cdc_ddl` (primitive, full event surface) and `cdc_schema_diff` (sugar, scoped to ALTER-on-table). Both share the same typed-extraction code; both must pass identically on every backend.

- [ ] Test `cdc_ddl` returns correct typed events for every category. Per spec, RENAME = ALTER, so all rename forms surface as `altered.{table,view}` with rename info inside `details`:
  - `created.schema`
  - `created.table` — `details.columns` matches the table's actual columns at the snapshot
  - `created.view` — `details.definition` matches
  - `dropped.schema`, `dropped.table`, `dropped.view`
  - `altered.table` for every operation:
    - `ADD COLUMN` (with and without default) → `details.added`
    - `ADD COLUMN nested.field` (struct field add) → `details.added` with `parent_column_id` set
    - `DROP COLUMN` (top-level and nested) → `details.dropped`
    - `RENAME COLUMN` (top-level and nested field) → `details.renamed`
    - `RENAME TABLE` → `details.{old_table_name, new_table_name}` populated; all column-level arrays empty
    - `ALTER ... SET TYPE` (every promotion in the spec's promotion table: int8→int16/32/64, uint variants, float32→float64) → `details.type_changed`
    - `SET / DROP NOT NULL` → `details.nullable_changed`
    - Default changes → `details.default_changed`
    - **Combined** (e.g. `ALTER TABLE t RENAME TO t2; ALTER TABLE t2 ADD COLUMN c INT;` in one transaction) — single `altered.table` event with multiple populated `details` arrays plus the rename fields.
    - **Cross-product stress test** (the realistic schema-migration shape, not a corner case): in one transaction, rename the table, change the type of an existing column, and add a nested struct field on a third column — `BEGIN; ALTER TABLE orders RENAME TO orders_v2; ALTER TABLE orders_v2 ALTER COLUMN amount SET TYPE BIGINT; ALTER TABLE orders_v2 ADD COLUMN customer.tier VARCHAR; COMMIT;`. Single `altered.table` event must populate `details.{old_table_name, new_table_name}`, `details.type_changed[amount]`, and `details.added[customer.tier]` with the correct `parent_column_id`. **All three populated simultaneously.** Migrations doing this combination are common; getting any one of the three wrong silently is the failure mode.
  - `altered.view` — `details.{old_definition, new_definition, old_view_name, new_view_name}` correct (any subset may differ)
- [ ] Test that `cdc_ddl` (and `cdc_schema_diff`) correctly skips work when `schema_version` is unchanged between snapshots (using `ducklake_schema_versions`) — performance shortcut applies only to `altered.table` (per Phase 1 implementation note); `created.schema` / `dropped.schema` still need full snapshot-changes scan.
- [ ] **Schema-version-boundary end-to-end (full sugar+primitive composition; no spec change).** The contract — `cdc_window` returning `schema_version` and `schema_changes_pending`, never crossing a `schema_version` boundary by default — was locked and shipped in Phase 1, not added here. Phase 2 only re-runs the end-to-end flow on each catalog backend: insert at v1, alter, insert at v2 → consumer reads v1 window with `schema_changes_pending=true` → commits at last v1 snapshot → next window starts at the schema-change snapshot under v2 → `cdc_ddl` returns the typed `altered.table` event → consumer applies to mock sink → `cdc_changes` returns the v2 inserts. Across all three catalog backends. Run with both `stop_at_schema_change=true` and `=false`.
- [ ] **DDL-before-DML ordering enforced per snapshot.** Insert + alter + insert in one transaction; verify `cdc_ddl` and `cdc_changes` results, when interleaved by snapshot id, place DDL before DML. (Library-level interleaving lives in Phase 3 / 4; the Phase 2 test asserts the extension's ordering primitives produce data the library can interleave correctly.)
- [ ] **Column-drop audit limitation, per backend.** The deletes-after-drop test from Phase 1 runs on every backend. Asserts the documented data loss is identical across backends — this is a DuckLake property, not a ducklake-cdc property.
- [ ] Document explicitly: read-side schema reconciliation (omitting dropped columns, defaulting added columns) is **DuckLake's** behaviour, not ours. Our `cdc_ddl` and `cdc_schema_diff` are metadata-only — they tell downstream sinks *what* the schema change was so they can react before the next batch arrives.

### Compaction & file-rewrite — trust nothing, test everything

The spec says `ducklake_merge_adjacent_files` and `ducklake_rewrite_data_files` are transparent to readers. We don't trust the spec. We test it.

- [ ] Test that consumers continue to read correctly after `ducklake_merge_adjacent_files` is run mid-stream. Specifically: window of 100 snapshots, run merge between snapshot 50 and snapshot 51, verify `cdc_changes` returns identical event sequence to a control run without merge.
- [ ] Test that consumers continue to read correctly after `ducklake_rewrite_data_files` (including with a delete-threshold that triggers actual rewrite) is run mid-stream. Verify `update_preimage` / `update_postimage` events for ranges that crossed the rewrite still appear correctly.
- [ ] Test that consumers continue to read correctly after `ducklake_flush_inlined_data` is run mid-stream — events that started inlined and got flushed mid-window still appear once.
- [ ] **Compaction-gap recovery flow:** `ducklake_expire_snapshots` → consumer's cursor is gone → next `cdc_window` raises `CDC_GAP` → error message includes the recovery command verbatim → `cdc_consumer_reset(to_snapshot := 'oldest_available')` succeeds → consumer resumes without losing newer events.
- [ ] Test interaction with `ducklake_files_scheduled_for_deletion` and the `delete_older_than` setting: data files queued for deletion remain readable until the deletion grace period passes.

### DLQ feature (schema landed Phase 1, semantics land here)

- [ ] `cdc_dlq_record(consumer, snapshot_id, event_kind, table, object_kind, object_id, rowid, change_type, error, payload)` — write an entry to `__ducklake_cdc_dlq`. Called by language clients when a sink permanently rejects an event after `max_attempts` retries. `event_kind` is `'ddl'` or `'dml'`; the table / object / rowid / change_type fields are populated per the schema in Phase 1.
- [ ] `cdc_dlq_list(consumer := NULL, event_kind := NULL, since := NULL, limit := 100)` — table function for inspecting failures, filterable by kind so operators can see DDL failures separately from DML failures (DDL failures are usually more critical).
- [ ] `cdc_dlq_replay(consumer, snapshot_id, event_kind, ...)` — return the original payload for a failed event so a downstream tool can retry it manually. Does not auto-delete the DLQ entry (operator decides).
- [ ] `cdc_dlq_acknowledge(consumer, snapshot_id, event_kind, ...)` — operator marks an entry as resolved (writes a `dlq_acknowledge` row to `__ducklake_cdc_audit`; does not delete the DLQ row — keep for history). Required for the DDL-blocks-DML semantics below.
- [ ] `cdc_dlq_clear(consumer, before_snapshot := NULL)` — operator-driven cleanup; writes a `dlq_clear` audit row.

**DLQ contract — DML and DDL diverge on purpose.** "Advance past one bad row" is the right default for individual DML rows (one bad row halting the entire pipeline is the most common production failure mode of CDC systems). "Advance past one bad DDL" is **catastrophic** because every subsequent DML in the affected table will then DLQ for shape mismatch — one bad ALTER becomes 10 000 DLQ entries. The policy:

- **DML failures.** Cursor advances past the failed row. The DLQ entry is an alert ("ops, look at this"), not a stop signal.
- **DDL failures.** Default behaviour (`dml_blocked_by_failed_ddl = true` on the consumer; see ADR 0009): when a DDL apply for snapshot N permanently fails, the consumer **halts DML for the affected table** (or the whole consumer, for schema-level DDL) until an operator calls `cdc_dlq_acknowledge` on the failed DDL entry. The cursor does not advance past the DDL snapshot for that table's DML. Other tables' DML continues normally for table-scoped DDL; whole-consumer halt only for schema-level DDL. The doctor command surfaces the halt reason loudly.

  **"The affected table" — explicit definition (locks here, both bindings inherit).** The halt set is computed per `cdc_ddl` event using the `details` payload:
  - `created.table` / `dropped.table`: halt DML for the single table named by `object_name`.
  - `altered.table` with column-level changes only: halt DML for the single table named by `object_name`.
  - `altered.table` with `details.{old_table_name, new_table_name}` populated (a rename): **halt DML for both `old_table_name` and `new_table_name`** until acknowledgement. Rationale: a downstream sink that failed to apply the rename is in an inconsistent state — it doesn't know whether subsequent rows belong to the old name (because the rename never landed) or the new name (because we logically committed it). Halting both names is the only safe answer; halting one risks silently writing rows to the wrong table on the sink. Additional rows for the old name during the halt are themselves DLQ candidates for "DML against unacknowledged-rename source table" if they arrive (the producer has been told the new name exists, but in practice rare-but-possible if the producer is split-brained).
  - `altered.view`: halt is a no-op for DML purposes (views don't carry DML); the DDL itself stays in the DLQ until acknowledged.
  - `created.schema` / `dropped.schema` / any failure where `tables := NULL` was the consumer scope: whole-consumer halt.

  Documented loudly in a future DLQ semantics operational guide with the table-rename worked example (sink raises on the `RENAME TO`; consumer halts DML for both old and new name; operator either fixes the sink and replays, or `cdc_dlq_acknowledge`s the entry and accepts the inconsistency, or runs `cdc_consumer_reset` to restart from a known-good snapshot). Both halt names appear in `cdc_doctor()` output.

- **Opt-out (`dml_blocked_by_failed_ddl := false`).** Power-user mode for sinks that don't care about schema (e.g. blob-passthrough audit). Cursor advances past the failed DDL; DML for the affected table will likely DLQ at scale; operators take responsibility. **The DML DLQ flood under opt-out is bounded by `max_dml_dlq_per_consumer` (next bullet), not unbounded.**

This split is enforced **client-side** in Phase 3 / 4 (the client's `Tail` loop checks for unacknowledged DDL DLQ entries before proceeding with DML for the same table) because the extension doesn't know which DML rows belong to which DDL-affected table without re-reading the catalog. The extension exposes `cdc_dlq_pending_ddl(consumer, table := NULL)` as a single-row SQL helper (returns the oldest unacknowledged DDL DLQ entry per table) so clients don't have to re-implement the join.

- [ ] DLQ growth as an SLO: surface `dlq_pending`, `dlq_pending_ddl`, `dlq_pending_dml` counts in `cdc_consumer_stats()`. Operators alert on `dlq_pending_ddl` reaching 1 (because of the halt semantics) and on `dlq_pending_dml` crossing a higher threshold.
- [ ] **`max_dml_dlq_per_consumer` cap (ships with the DLQ feature in this phase, not later).** Per-consumer hard cap on DML DLQ entries (default `1000`). When the cap is hit, the consumer halts entirely (writes a `consumer_halt_dlq_overflow` audit row; `cdc_consumer_stats()` flags it; the doctor surfaces it loudly) until an operator clears DLQ entries or raises the cap. Adds a `max_dml_dlq_per_consumer INTEGER NOT NULL DEFAULT 1000` column to the consumer-state schema (Phase 1 already locked the schema; this is the only post-Phase-1 column addition allowed because it has a safe default per ADR 0009's locking discipline). Configurable per consumer at create time and per session via `SET ducklake_cdc_default_max_dml_dlq = ...`. **Why this ships in Phase 2 and not Phase 5:** without it, a consumer with `dml_blocked_by_failed_ddl := false` whose DDL apply silently fails generates one DLQ row per DML event for the affected table — at typical write rates that's 100k+ DLQ rows before anyone notices. Phase 5's safety net is too late if Phase 2 ships the opt-out without the cap.

### Special types

- [ ] Default serializers (configurable via `SET ducklake_cdc_serializer_<type> = ...`):
  - `variant` → JSON. Use shredded sub-fields (`ducklake_file_variant_stats`) where present for typed access; fall back to dynamic JSON otherwise.
  - `geometry` → GeoJSON. Bounding box surfaced as a separate column for sink-side filtering.
  - `blob` → base64 by default; passthrough binary as an option.
  - `decimal` → exact string; preserve precision/scale.
  - Nested types (`list`, `struct`, `map`) → JSON.
- [ ] Document per-backend caveats from the spec (PostgreSQL catalogs cannot store `variant` inline; SQLite collapses many types to `TEXT`).
- [ ] **Reminder of the design pillar:** these serializers run on the client side, not in the extension. The extension exposes raw values; the Python and Go clients apply the serialization.

### Encryption

- [ ] Verify `table_changes` transparently decrypts encrypted files (the `encryption_key` column on `ducklake_data_file` / `ducklake_delete_file` is consumed by DuckLake itself; we just have to not break it).
- [ ] CI test with `encrypted := true` enabled at the catalog level. Run on at least Postgres and embedded DuckDB.

### Benchmark — `bench/medium.yaml` runs across the catalog matrix

ADR 0011 ratifies three benchmark workloads (light / medium / heavy). Phase 1 ships the harness running `light` against embedded DuckDB. Phase 2 adds `medium` and runs it on every catalog backend (DuckDB / SQLite / Postgres — DuckLake's three supported metadata backends).

- [ ] `bench/medium.yaml` workload descriptor — 5-minute CI run at medium load: flat profile, 1.67 target snapshots/sec, 1 000 target rows/snapshot, 5 consumers, mix of webhook + Kafka sinks. Longer soak runs and variable-load profiles stay out of the regular CI path until Phase 5.
- [ ] Mature benchmark CI should run **after** the extension distribution matrix has built its platform artifacts, then reuse those exact artifacts instead of rebuilding. The medium benchmark should run on every platform the matrix produced (Linux x86_64/arm64, macOS x86_64/arm64, Windows x86_64 where available) and every supported catalog backend that can be exercised on that runner. This is the likely long-term main CI confidence gate: "the matrix-built binaries run the 5-minute medium workload and produce numbers."
- [ ] CI matrix runs `medium` against each backend/platform combination; numbers committed to `bench/results/<platform>/<backend>/medium-<sha>.json` when maintainers want to preserve a baseline. The append-only history makes per-backend and per-platform regressions visible.
- [ ] Backend-specific gates (informational, not pass/fail in Phase 2): per-backend p99 latency target = ADR 0011 medium target × 1.5 (allows for backend overhead). The hard gate is "no regression vs. previous run."

### Doctor command (moved from Phase 5)

`ducklake-cdc doctor` is the meta-diagnostic users will love and operators will reach for first when something's wrong. It pays for itself the moment there are any users at all — including the maintainers debugging Phase 2's catalog matrix. Building it now means it's a real tool by the time Phase 5's chaos suite needs it.

It does **not** ship as a SQL function — it ships in the CLIs (Python `python -m ducklake_cdc doctor`, Go `ducklake-cdc doctor`). Phase 2 ships a SQL helper view that the CLIs both consume:

- [ ] `cdc_doctor()` table function returns one row per (category, finding, severity, recommendation, details_json):
  - **`catalog`** — reachability, backend type, ducklake extension version vs. supported catalog-schema range (per `docs/compatibility.md`).
  - **`consumers`** — per-consumer rows: lag (snapshots + human time), distance to `expire_older_than`, `tables_unresolved`, lease holder + age + heartbeat freshness, last commit age.
  - **`dlq`** — per-consumer DLQ pending counts, broken down by `event_kind`. Highlights any consumer with `dlq_pending_ddl > 0` (since that means DML is halted under default semantics).
  - **`leases`** — any orphaned leases (heartbeat older than 5 × `lease_interval_seconds` — these should never happen with the `lease_force_acquire` path; if they do, something is wrong).
  - **`audit_recent`** — last 10 audit rows for visibility.
  - **`gap_risk`** — consumers within 10% of `expire_older_than` (gap incoming).
- [ ] CLI-side rendering (Python and Go) groups by category, colour-codes severity (info / warn / error), prints recommended next actions inline.
- [ ] Phase 2 deliverable is the SQL helper plus a minimal Python rendering. The Go-binary rendering ships with the rest of the Go CLI in Phase 4. Phase 5's chaos suite uses the doctor command extensively — by then it's been used in real testing for two phases.

## Exit criteria

- CI matrix green across all three supported catalog backends, including:
  - Phase 1 test suite verbatim.
  - Phase 1 lease test suite verbatim.
  - Per-backend TOCTOU race test (same code, three configs).
  - Per-backend isolation level documented and verified.
  - Per-backend column-drop audit-limitation test (consistent loss).
  - Per-backend DDL-before-DML ordering test.
- `cdc_ddl` test suite covers every DDL category and shape in the ADR 0008 spec, on top-level and nested columns.
- `cdc_schema_diff` (sugar) shares extraction logic with `cdc_ddl`; identical test coverage on the column-level subset.
- Schema-version-boundary end-to-end flow passes on all three backends, with both `stop_at_schema_change` modes.
- Backend-portability test: the Phase 1 demo runs unchanged after only changing the `ATTACH 'ducklake:...'` URI.
- Compaction, file-rewrite and inline-flush operations exercised mid-stream against an active consumer; no regressions vs `table_changes` baseline.
- `variant` and `geometry` round-trip through CDC with byte-exact semantics on read-back into a fresh DuckLake.
- DLQ feature shipped: write, list, replay, acknowledge, clear, stats integration. Tested across all three backends. **DDL-blocks-DML semantics tested:** failing DDL halts DML for the affected table; table-rename DDL halts both old and new names; `cdc_dlq_acknowledge` resumes; `dml_blocked_by_failed_ddl := false` consumer opts out and the DML DLQ flood is bounded by `max_dml_dlq_per_consumer` (default 1000); cap-overflow halts the consumer cleanly with a `consumer_halt_dlq_overflow` audit row.
- `cdc_doctor()` SQL helper shipped with Python rendering; Go rendering deferred to Phase 4. Usable by Phase-2 maintainers debugging the catalog matrix.
- `__ducklake_cdc_audit` emits Phase-2 actions (`dlq_acknowledge`, `dlq_replay`, `dlq_clear`, `consumer_halt_dlq_overflow`).
- `docs/compatibility.md` populated and CI guards extension init against unsupported catalog versions.
- `bench/medium.yaml` shipped and run per-backend; numbers committed; trajectory visible in `bench/README.md`.
