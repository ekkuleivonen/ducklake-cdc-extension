# Phase 0 — Discovery & Spike

**Goal:** audit overlap with DuckLake's existing API exhaustively, decide whether an extension is the right vehicle (or whether a SQL-helper library would do), freeze a small primitive surface that justifies itself against the non-overlap principle, lock the design decisions that are too expensive to change later (concurrency model, schema-version boundaries, threat model, error-format contracts), and lock implementation language and license.

This phase produces no shipping code. Its output is decisions. The cost of getting any of them wrong is paid in Phases 1-5; the cost of getting them right here is two weeks of writing.

## Work items

### Overlap audit (the most important deliverable of this phase)

- [x] Catalogue every DuckLake function and metadata table relevant to consumer-side change capture. Already known to exist:
  - `<lake>.snapshots()` — returns `(snapshot_id, snapshot_time, schema_version, changes [structured map], author, commit_message, commit_extra_info)`.
  - `<lake>.current_snapshot()` — latest snapshot id.
  - `<lake>.last_committed_snapshot()` — connection-scoped last commit.
  - `CALL <lake>.set_commit_message(author, msg, extra_info => ...)` — producer-side outbox metadata API.
  - `table_changes(t, start, end)` / `table_insertions` / `table_deletions` — accept snapshot ids OR timestamps; transparently handle inlining, encryption, partial files, post-compaction reads, and schema evolution on the read path.
  - `tbl AT (VERSION => N)` / `AT (TIMESTAMP => ...)` and `ATTACH ... (SNAPSHOT_VERSION ...)` — full time travel.
  - `ducklake_expire_snapshots`, `ducklake_merge_adjacent_files`, `ducklake_cleanup_old_files`, `ducklake_delete_orphaned_files`, `ducklake_rewrite_data_files`, `ducklake_flush_inlined_data` — maintenance.
  - `ducklake_list_files(catalog, table, ...)` — file listing.
  - Conflict resolution via `ducklake_snapshot_changes` — automatic concurrent-writer arbitration.
  - `ducklake_inlined_data_*` — small batches stay in catalog tables instead of parquet; `snapshots()` reports them as `inlined_insert` / `inlined_delete` change keys (not `tables_inserted_into` / `tables_deleted_from`). Critical for any test matrix.
- [x] For each item we proposed, label it `keep`, `demote-to-sugar`, or `drop` based on the audit. Document in `docs/decisions/0004-overlap-audit.md`.
- [x] Re-validate that the prior-art search ("consumer-side CDC for DuckLake is empty space") is still accurate.

### Architecture decision: extension vs SQL-helper library

- [x] Document the trade-off in `docs/decisions/0005-extension-vs-library.md`. **The extension is primary.** The justification is *not* that `cdc_wait` is magic — `cdc_wait` is honest exponential-backoff polling and a library could implement the same loop. The justification is:
  - **One canonical SQL surface** that works identically from `duckdb` CLI, Python, Go, R, Rust, Java, Node, and any future binding without re-implementation.
  - **The SQL CLI persona is the cleanest demo path**: `INSTALL ducklake_cdc FROM community; LOAD ducklake_cdc; CALL cdc_consumer_create(...)` is a better 60-second demo than any `pip install` story.
  - **Future pushdown opportunities** for change-feed reads that wouldn't be possible from a pure SQL helper.
- [x] **Honest cost statement (must appear in the ADR).** This justification is only winning if we ship and maintain bindings beyond Python and Go. If only Python and Go ever ship, the extension is overhead — both could call `snapshots()` and `table_changes()` natively and reach the same place. The extension wins when binding #3 (Rust? R? Node? Java?) is contemplated and the cost of "do the same thing in C++ that's already in two other languages" hits. Adopting the extension as the canonical surface (pillar 12) **commits the project to a multi-language future**. If that appetite isn't there, switch to a Python-first plan and revisit how the project is framed for contributors and users. Phase 0 must explicitly answer: which third binding will we ship within 12 months of v0.1.0? (Recommended: R, because the DuckDB R community overlaps the analytics audience cleanly and an R binding takes weeks not months.) Record the answer in this ADR.
- [x] **Pillar-12 reconciliation.** ADR 0005 (this document) and pillar 12 in `README.md` must say the same thing. The older "extension wins because of `cdc_wait` magic" framing is dead. The new framing — "one canonical SQL surface across many bindings" — is the binding one and lives in both places.
- [x] Counter-argument to engage with explicitly in the ADR: every primitive must also be expressible as a documented pure-SQL recipe so that (a) library implementations stay viable as a fallback for environments where extension install is blocked, and (b) we can prove the extension is sugar over `snapshots()` + `table_changes` + a state table, not magic.

### Architecture decision: hybrid discovery + read

- [x] Document and ratify in `docs/decisions/0002-hybrid-discovery-read.md`:
  - Discovery: read `snapshots()` filtered by the cursor window. We do *not* parse `ducklake_snapshot_changes.changes_made` ourselves — DuckLake already returns it as a typed map.
  - Read: call `table_changes` / `table_insertions` / `table_deletions` so we inherit inlined-data, encryption and schema-evolution handling for free.
  - Consumer state: write to a regular table in the catalog, transactional with the read.
  - **Inlined data**: discovery must check `inlined_insert` / `inlined_delete` keys in `snapshots().changes` *in addition to* `tables_inserted_into` / `tables_deleted_from`. A snapshot with only inlined writes will otherwise be silently skipped. Reference: `the prototype implementation` lines 244-255 already gets this right; we must not regress.

### Architecture decision: concurrency model (NEW — `docs/decisions/0007-concurrency-model.md`)

There are three distinct "parallel" patterns and the roadmap previously conflated them. Lock them down now.

- **(a) Multiple independent consumers on the same lake.** Each has its own row in `__ducklake_cdc_consumers`. They don't interact except via DuckLake's snapshot-id arbitration on writes to that table. **Supported in v0.1.**
- **(b) Multiple workers sharing one consumer name (work-sharing fan-out).** Two workers both call `cdc_window('indexer')` from different connections. **Explicitly unsupported in v0.1.** The naive behaviour (both get the same window, both process the same rows) is wrong, and the right answer (a coordinator with leased windows) is a v1.0 design exercise. v0.1 enforces "one consumer name = one logical reader" via the **owner-token lease** locked below — *not* via per-backend SQL advisory locks. The escape hatch for genuine work-sharing needs is "create N consumers with disjoint table filters." Future v1.0: `cdc_window_lease(consumer, worker_id, lease_ms)` for proper work-sharing.
- **(c) Parallel `table_changes` reads within one window driven by one orchestrator connection for one consumer.** `the prototype implementation` already does this — the orchestrator calls `cdc_window` once, then fans out `table_changes` queries to a worker pool, then commits once at end. **Supported in v0.1** — pillar 5 explicitly carves this out: same-connection idempotence + reject-other-connection. The fan-out workers run plain `SELECT`s; only the orchestrator holds the lease.

#### Single-reader enforcement: the owner-token lease (lands in Phase 1, not Phase 5)

The earlier per-backend advisory-lock plan (Postgres `pg_advisory_xact_lock`, etc.) is dropped. Two reasons it doesn't work:

1. **Lifetime mismatch.** `pg_advisory_xact_lock` is transaction-scoped and releases when `cdc_window`'s transaction commits — milliseconds later — not when the holder commits. Two consumers can serially acquire, both think they own the cursor, both call `cdc_commit`, and the second silently overwrites the first. This is exactly the race pillar 6 is trying to prevent.
2. **Hash collisions and per-backend divergence.** `hashtext(consumer_name)` is `int4`; with hundreds of consumers, name collisions block unrelated consumers from each other. And the three supported backends (DuckDB / SQLite / Postgres) each have different lock primitives with different lifetime semantics — SQLite has none at all.

**The portable design.** The lease is the consumer row itself, three columns added to `__ducklake_cdc_consumers`:

```text
owner_token         UUID           -- NULL = no holder
owner_acquired_at   TIMESTAMPTZ    -- when the current holder took the lease
owner_heartbeat_at  TIMESTAMPTZ    -- last heartbeat from the holder
```

`cdc_window` performs a single conditional UPDATE inside its transaction. **The exact SQL below is the canonical form; Phase 1's implementation must match it byte-for-byte (modulo backend dialect for `make_interval`).** Schema drift between this ADR and the Phase 1 SQL is the failure mode ADR 0009 already calls out for the consumer-state schema; the same discipline applies to the lease SQL itself, because divergent predicates between the design and the implementation are how the race we're defending against gets re-opened by a refactor.

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

`:token_held_by_this_connection` is the connection's session-cached token for this consumer (NULL on the first `cdc_window` call from this connection). `:new_token_for_this_call` is a freshly-generated UUID used only when the row's `owner_token` is currently NULL — the `COALESCE` makes the lease-acquired path single-statement and atomic. The connection caches the resulting `owner_token` from the `RETURNING` clause and re-passes it as `:token_held_by_this_connection` on subsequent calls (this is what makes `cdc_window` idempotent on the holding connection).

If the UPDATE returns zero rows, raise `CDC_BUSY` with the existing holder's token, acquired-at, and heartbeat-at in the message (per the structured-error format below).

**Heartbeat extension.** `cdc_commit` refreshes `owner_heartbeat_at = now()` as part of its own UPDATE. Long-running orchestrators that read for many minutes between commits call `cdc_consumer_heartbeat(name)` (a Phase 1 primitive — single UPDATE setting `owner_heartbeat_at = now() WHERE owner_token = :token`). Library `tail()` / `Tail()` helpers heartbeat automatically.

**Lease loss.** If the holder dies and the lease times out (`owner_heartbeat_at < now() - lease_interval`), the next caller acquires cleanly. The previous holder cannot then commit because its `cdc_commit` UPDATE will fail the `owner_token = :stale_token` predicate. Document this in `docs/operational/lease.md`.

**Why this is a Phase 1 work item and not Phase 5.** The advisory-lock approach was deferred because it required per-backend implementation work across DuckDB / SQLite / Postgres (and SQLite has no native advisory-lock primitive at all). The owner-token approach is one UPDATE statement, portable across all three backends with no per-backend code, and survives connection death gracefully via the heartbeat timeout. There is no good reason to ship Phase 1 without it — the no-enforcement window between v0.0.x and v0.1.0-beta would be the period where users are most likely to hit the race and remember it.

**`cdc_consumer_force_release(name)`** becomes trivial: `UPDATE __ducklake_cdc_consumers SET owner_token = NULL WHERE consumer_name = :name RETURNING owner_token` — and emits an audit-table entry per the audit work item below.

**Default lease interval:** 60 seconds. Configurable per consumer via `lease_interval_seconds` on `cdc_consumer_create`. Configurable session-wide as a fallback via `SET ducklake_cdc_lease_interval_seconds = ...`.

Lock all of this in ADR 0007.

### Architecture decision: schema-version boundaries (NEW — `docs/decisions/0006-schema-version-boundaries.md`)

This is the sharpest technical decision in Phase 0 and the one most likely to be wrong if not explicitly designed.

**The problem.** `table_changes(t, A, B)` reads using the schema as-of `B`. So if a column was dropped between A and B, historical insert events from before the drop come back without that column. If a column was added, historical events come back with the default value substituted. (Spec: "Field Identifiers".)

**The implication.** A consumer that "applies the schema diff first, then reads data" is correct. A consumer that "reads data, then applies the schema diff" reads data already reconciled to the new schema and then applies the diff *to its sink* — divergent state.

**The decision.** `cdc_window` honors a per-consumer `stop_at_schema_change BOOLEAN` flag (default `true`). When true, `cdc_window` **never returns a window that crosses a `schema_version` boundary**. The window result includes:

- `schema_version BIGINT` — the schema version active throughout the window
- `schema_changes_pending BOOLEAN` — true if the very next snapshot the consumer would read is at a different `schema_version`

The contract (with `stop_at_schema_change = true`, the default):

1. Consumer calls `cdc_window` → gets `(start, end, schema_version, schema_changes_pending)`. The window's snapshots all share `schema_version`.
2. Consumer reads DML via `cdc_changes` and DDL via `cdc_ddl`. Per ADR 0008, DDL events are delivered before DML events within each snapshot.
3. Consumer calls `cdc_commit(end)`.
4. If `schema_changes_pending` was true: the next `cdc_window` call returns a window starting at the schema-change snapshot. The DDL events at that snapshot (which produced the new `schema_version`) are the first events the consumer sees.

**Opting out.** Consumers created with `stop_at_schema_change := false` can have windows that cross schema boundaries. Use cases: high-throughput sinks that don't care about column-level shape (stdout JSONL, blob-passthrough audit), or consumers that handle their own schema reconciliation. Documented as a power-user knob; the safe default is on.

**Why proactive (yield-before) and not reactive (yield-after).** Reactive yields data already reconciled to the new schema; the diff arrives after the consumer has already written wrong shapes downstream. Proactive yield bounds the window so all reads happen under one consistent schema.

**Implementation note — per-table awareness.** `ducklake_schema_versions` is a per-table table: `(begin_snapshot, schema_version, table_id)`. `ducklake_snapshot.schema_version` is global and advances whenever *any* table or schema changes. Consequently:

- For a consumer with **no table filter** (subscribes to the whole lake), `cdc_window` bounds at any `ducklake_snapshot.schema_version` change. (Conservative; correct.)
- For a consumer with a **table filter** (`tables := ['orders']`), `cdc_window` bounds only at snapshots whose `ducklake_schema_versions.table_id` intersects the filter set. A schema change to an unrelated `customers` table does not trigger a boundary.
- Implementation: a single `JOIN` between `ducklake_snapshot` (in window range) and `ducklake_schema_versions` (filtered by `table_id IN (...)`), find the smallest `begin_snapshot` strictly greater than `last_committed_snapshot`. If none in `[last_committed+1, current_snapshot]`, the whole range is one schema version.
- Cost: `ducklake_schema_versions` is small (one row per `(table, schema_version)` change, typically a handful per table over a lake's lifetime).

### Architecture decision: DDL as a first-class event stream (NEW — `docs/decisions/0008-ddl-events.md`)

#### Two surfaces, one source of truth

DuckLake exposes change information in two related but **distinct** forms:

1. **`ducklake_snapshot_changes.changes_made`** — a `VARCHAR` column on the catalog table holding a comma-separated list of *singular* keys with the format `key:<id-or-name>`. Per spec, the documented values are exactly:

   ```text
   created_schema:<schema_name>
   created_table:<table_name>            (table_name is qualified, e.g. main.orders)
   created_view:<view_name>
   inserted_into_table:<table_id>
   deleted_from_table:<table_id>
   compacted_table:<table_id>
   dropped_schema:<schema_id>
   dropped_table:<table_id>
   dropped_view:<view_id>
   altered_table:<table_id>
   altered_view:<view_id>
   ```

   **Note:** there is no `renamed_table` or `renamed_view` in the spec. `RENAME TABLE` and `RENAME COLUMN` are real DuckLake operations but are recorded as `altered_table:<id>`. The conflict-resolution rules confirm this view: ALTER and RENAME are one category. Our `cdc_ddl` event_kind enum follows the spec — `created`, `altered`, `dropped` — and exposes rename information *inside* the `details` payload of an `altered` event.

   `changes_made` does **not** contain `inlined_insert` / `inlined_delete` keys.

2. **`<lake>.snapshots().changes`** — a typed `MAP` column returned by the DuckLake extension's `snapshots()` table function. Keys are *plural-past-tense* and values are arrays. Confirmed examples from the spec:

   ```text
   {schemas_created=[main]}            ← schema names
   {tables_created=[main.people]}      ← qualified table names
   {inlined_insert=[1]}                ← table ids
   ```

   The reference implementation (`the prototype implementation`) additionally relies on `tables_inserted_into=[<table_id>]`, `tables_deleted_from=[<table_id>]`, and `inlined_delete=[<table_id>]`. These are observed-in-the-wild keys; the spec does not exhaustively enumerate the MAP-side names.

   **Phase 0 spike (work item, MUST run before Phase 1 starts):** write a spike (`test/upstream/enumerate_changes_map.py`) that exercises every DDL / DML / compaction operation against a real DuckLake and dumps `snapshots().changes` for each, producing a definitive table mapping `changes_made` text → `snapshots().changes` map keys. Commit the table to `docs/api.md` as the canonical reference. We cannot lock the `cdc_events` sugar wrapper's table-filter SQL until this is empirically verified.

   **Promote the spike to a recurring CI job in Phase 1, do not retire it.** A 6-key portion of the MAP enumeration is observed-in-the-wild rather than spec-confirmed. If DuckLake silently changes a key between v1.0 and v1.1, our DDL extractor breaks silently. The mitigation is to make the spike output a build artifact: re-run it against every DuckLake version in the supported window on every CI build, diff the produced key-set against the committed `docs/api.md#snapshots-changes-map-reference`, and **fail the build on any diff**. Operators get the diff in the failure message and decide whether to bump the supported-version floor, add a new key to the extractor, or reject the upstream change. Lock this in the ADR; Phase 1's CI scaffolding work item picks it up.

#### Categorisation our `cdc_ddl` primitive uses

Independent of the surface, the *categories* the consumer cares about are:

- **DDL** — `created_*`, `altered_*`, `dropped_*` for `schema`, `table`, `view`. (RENAME folds into `altered`.)
- **DML** — `inserted_into_table`, `deleted_from_table`, plus the inlined variants `inlined_insert`, `inlined_delete`.
- **Maintenance** — `compacted_table`. Excluded from `cdc_ddl` (operators see it via `cdc_events`).

`table_changes()` is a DML-only function. There is no DuckLake-provided equivalent for DDL — yet downstream sinks frequently need to react to schema lifecycle events (a Postgres mirror needs `CREATE TABLE` on its side; a search index needs to provision; a schema registry needs the new shape).

**The decision.** Ship `cdc_ddl(consumer)` as a Phase 1 **primitive** (not sugar) returning typed DDL events:

| column | type | notes |
| --- | --- | --- |
| `snapshot_id` | BIGINT | the snapshot the DDL happened in |
| `event_kind` | VARCHAR | one of `created`, `altered`, `dropped`. (Per spec: RENAME = ALTER. There is no separate `renamed` kind.) |
| `object_kind` | VARCHAR | one of `schema`, `table`, `view` |
| `schema_id` | BIGINT | NULL for `event_kind = created` of a schema |
| `schema_name` | VARCHAR | |
| `object_id` | BIGINT | |
| `object_name` | VARCHAR | the *current* (post-event) name. For an `altered.table` event that includes a table rename, this is the new name; the old name is in `details`. |
| `details` | JSON | shape varies by `(event_kind, object_kind)` — locked below |

`details` shapes locked in `docs/api.md`:

- `created.table`: `{columns: [{column_id, name, type, default, nullable, parent_column_id}, ...], partition_by: [...], properties: {...}}`
- `created.view`: `{definition: "..."}`
- `created.schema`: `{}`
- `altered.table`: `{old_table_name: <name-or-null>, new_table_name: <name-or-null>, added: [{column_id, name, type, default, nullable, parent_column_id}, ...], dropped: [{column_id, name}, ...], renamed: [{column_id, old_name, new_name}, ...], type_changed: [{column_id, old_type, new_type}, ...], default_changed: [...], nullable_changed: [...]}`. A pure table rename surfaces as `{old_table_name, new_table_name}` populated and all other arrays empty. A column rename surfaces in `renamed[]`.
- `altered.view`: `{old_definition, new_definition, old_view_name, new_view_name}`
- `dropped.table` / `dropped.view` / `dropped.schema`: `{}`

**Out of `cdc_ddl` scope:** `compacted_table` is maintenance, not user-relevant DDL. It does not appear in `cdc_ddl`. Operators who want to monitor compactions read `cdc_events` (which exposes the raw `changes` map) and look for `compacted_table` keys.

**Why this is a primitive, not sugar.** Typed reconstruction requires querying `ducklake_table` / `ducklake_column` / `ducklake_schema` / `ducklake_view` with version-range awareness (each row carries `begin_snapshot` / `end_snapshot`). It cannot be expressed as a one-line composition over DuckLake builtins. Hiding this in client-side code would mean every binding (Python, Go, R, future) reimplements the typed extraction. One canonical implementation lives in the extension.

**Per-snapshot delivery ordering contract.**

- Within a snapshot: **DDL events before DML events**. Within DDL, sort by `(object_kind, object_id)` deterministically. Within DML, sort by `(table_id, rowid)` per ADR 0002.
- Across snapshots: strict `snapshot_id` ascending.
- The library (Python / Go / future) enforces this when interleaving `cdc_ddl` and `cdc_changes` outputs into a single batch. If a user calls them separately and applies in the wrong order, that is a user bug and the docs name it as such.

**Reframing `cdc_schema_diff` as acknowledged sugar.** With `cdc_ddl` as the typed-extraction primitive, `cdc_schema_diff(table, from, to)` is a stateless inspection helper equivalent to "collect every `altered.table` (and the initial `created.table` if `from` precedes table creation) `cdc_ddl` event for this table between these snapshot bounds, then merge the `details` payloads into a flat per-column delta." Same underlying logic, different surface. Lives in `docs/api.md` as sugar.

**Event-category filter on consumer creation.** Consumers can subscribe to a subset of `{ddl, dml}` via `event_categories` on `cdc_consumer_create`. Default `NULL` = both. Patterns this enables:

- One schema-watcher daemon (`event_categories := ['ddl']`) updates downstream schemas; many DML consumers fan out without worrying about schema management.
- An audit pipeline subscribes to DDL only and ships every schema change to git / Slack / a registry.

**The unfixable edge case — column-drop in the same snapshot as DML.** Consider:

```sql
BEGIN;
ALTER TABLE orders DROP COLUMN customer_email;
DELETE FROM orders WHERE id < 100;
COMMIT;
```

`table_changes()` reads the snapshot under the **end-snapshot schema**, so the deletes come back without `customer_email`. For most sinks (Postgres mirror, search index) this is correct — apply the DROP, apply the DELETEs, world is consistent. For audit sinks that want the original pre-drop column values, **the data is gone from the read path**. We cannot fix this — DuckLake itself doesn't preserve pre-drop column data on the read path.

Document loudly in `docs/operational/audit-limitations.md`. The doc **must lead with a runnable worked example**, not bury the recovery in a closing paragraph: take the exact `BEGIN; ALTER TABLE orders DROP COLUMN customer_email; DELETE FROM orders WHERE id < 100; COMMIT;` workload, show what `cdc_changes` returns (deletes without the column), then show the recovery query verbatim using `tbl AT (VERSION => snapshot_before_drop)` to fetch the pre-drop rows, then show the JOIN that reconciles them with the delete events. Audit-pipeline operators are exactly who hits this and who blames the project — give them the recipe before they reach for the issue tracker.

Lock all of this in the ADR.

### Architecture decision: consumer-state schema (NEW — `docs/decisions/0009-consumer-state-schema.md`)

This is the load-bearing on-disk schema for `__ducklake_cdc_consumers`. **All other phase docs reference this ADR; nothing else defines columns.** Schema drift across the planning docs guarantees schema drift in implementation.

| column | type | notes |
| --- | --- | --- |
| `consumer_name` | `VARCHAR PRIMARY KEY` | user-supplied; SQL-injection-safe quoting required wherever it's interpolated |
| `consumer_id` | `BIGINT NOT NULL UNIQUE` | monotonic id assigned at create time. Used as the `object_id` for any future 2-arg advisory-lock paths and as a stable identifier in audit-table rows when consumer rows are dropped |
| `tables` | `VARCHAR[]` | hard list of fully-qualified `schema.table` names; `NULL` = whole-lake. Globs/tags out of scope (v0.2) |
| `change_types` | `VARCHAR[]` | subset of `{insert, update_preimage, update_postimage, delete}`; `NULL` = all four. DML-only filter (see filter composition below) |
| `event_categories` | `VARCHAR[]` | subset of `{ddl, dml}`; `NULL` = both. Top-level switch (see filter composition below) |
| `stop_at_schema_change` | `BOOLEAN NOT NULL DEFAULT TRUE` | per-consumer schema-boundary opt-out; ADR 0006 |
| `dml_blocked_by_failed_ddl` | `BOOLEAN NOT NULL DEFAULT TRUE` | when a permanently-failed DDL apply lands in DLQ, the consumer's DML for that table halts until operator resolution. Setting to `false` opts into "advance past failed DDL" (rare). See Phase 2 DLQ semantics |
| `last_committed_snapshot` | `BIGINT` | the cursor; `NULL` only between create and first commit |
| `last_committed_schema_version` | `BIGINT` | cached for fast pillar-8 checks; updated alongside `last_committed_snapshot` on each `cdc_commit` |
| `owner_token` | `UUID` | the lease — see ADR 0007. `NULL` = no holder |
| `owner_acquired_at` | `TIMESTAMPTZ` | when the current holder took the lease |
| `owner_heartbeat_at` | `TIMESTAMPTZ` | last heartbeat (extended by `cdc_window`, `cdc_commit`, `cdc_consumer_heartbeat`) |
| `lease_interval_seconds` | `INTEGER NOT NULL DEFAULT 60` | per-consumer override of the lease timeout |
| `created_at` | `TIMESTAMPTZ NOT NULL` | |
| `created_by` | `VARCHAR` | nullable; populated from session user / `current_role` when available |
| `updated_at` | `TIMESTAMPTZ NOT NULL` | bumped on every metadata write |
| `metadata` | `JSON` | extension-reserved for forward-compatible additions; clients may store free-form data here. **Audit history does NOT live here** — see the audit table |

**Locking discipline:** any future addition to this table is a major-version bump for the extension. Adding a column without a default value is forbidden (would break catalog migrations). New optional columns must have safe defaults.

**Pre-approved Phase 2 addition:** `max_dml_dlq_per_consumer INTEGER NOT NULL DEFAULT 1000` ships alongside the DLQ feature in Phase 2, not Phase 5. The DLQ opt-out (`dml_blocked_by_failed_ddl := false`) is unsafe without a per-consumer cap on DML DLQ growth, and shipping the opt-out three phases ahead of the cap is the kind of split that produces real users with 100k+ DLQ rows. The default-1000 satisfies the locking-discipline requirement (safe default, no migration break for Phase 1 consumers).

`consumer_id` is assigned by `(SELECT COALESCE(max(consumer_id), 0) + 1 FROM __ducklake_cdc_consumers)` inside the create transaction. We do **not** depend on backend `SERIAL` / `IDENTITY` semantics because they diverge across DuckDB / SQLite / Postgres.

`tables_unresolved` (referenced by Phase 2's dropped-table policy) is **derived**, not stored: it's computed by `cdc_consumer_stats()` as `tables \ (existing tables in the catalog at current_snapshot())`. Stored values would drift; derived values can't.

### Architecture decision: audit log (NEW — `docs/decisions/0010-audit-log.md`)

Stuffing audit history into `__ducklake_cdc_consumers.metadata` is wrong: history is dropped when the consumer is dropped, the row grows unbounded for active operators, and every `cdc_consumer_list` query gets slower as a result. Use a dedicated table.

```sql
__ducklake_cdc_audit (
    audit_id        BIGINT PRIMARY KEY,           -- monotonic, assigned on insert
    ts              TIMESTAMPTZ NOT NULL,
    actor           VARCHAR,                       -- session user or 'unknown'
    action          VARCHAR NOT NULL,              -- enum below
    consumer_name   VARCHAR,                       -- nullable for system-level events
    consumer_id     BIGINT,                        -- denormalised so post-drop rows still identify the consumer
    details         JSON
);
```

**`action` enum (closed set, locked here):**

- `consumer_create`
- `consumer_drop`
- `consumer_reset`
- `consumer_force_release`
- `dlq_acknowledge` — operator marks a DLQ entry as resolved
- `dlq_replay` — operator triggers a manual replay
- `dlq_clear`
- `lease_force_acquire` — when the heartbeat-timeout path triggers; `details.previous_holder_token` records who got pre-empted
- `consumer_halt_dlq_overflow` — Phase 2: emitted when a consumer hits `max_dml_dlq_per_consumer` and halts; `details.{dlq_count, max_dlq, last_table}` carries the context
- `extension_init_warning` — version-incompatibility or other refusal-to-start (`consumer_name` NULL)

Insert-only, append-only. Operators clean up via a documented retention SQL recipe (`DELETE FROM __ducklake_cdc_audit WHERE ts < now() - INTERVAL '90 days'`). Schema is cheap to write to: single insert, no scan. Indexed on `(consumer_name, ts)` for the doctor command.

`audit_id` is assigned via the same `COALESCE(max + 1)` pattern as `consumer_id` — backend-portable.

The `__ducklake_cdc_audit` table is created lazily on first audit-emitting call (consumer create is usually the first), the same way `__ducklake_cdc_consumers` is. Schema is locked here; feature scope per phase:

- Phase 1 ships the table and emits `consumer_create`, `consumer_drop`, `consumer_reset` rows.
- Phase 1 also emits `lease_force_acquire` rows because the lease lands in Phase 1.
- Phase 2 adds DLQ-related actions and `consumer_halt_dlq_overflow` (alongside the `max_dml_dlq_per_consumer` cap).
- Phase 2's `cdc_doctor()` helper reads from this table.

### Architecture decision: performance model & benchmark workloads (NEW — `docs/decisions/0011-performance-model.md`)

The four-axis performance model in `README.md` ("Performance principles") is ratified here with the contract numbers and the benchmark workloads that prove them. **This ADR makes commitments visible to users; every release validates them in CI.**

**Status of the numbers in this ADR:** every absolute target in the tables below is a **design target** until Phase 5 ratifies it as a measured contract. Phase 1 ships the harness and the workload descriptors; Phase 2 runs `medium` across the catalog matrix; Phase 5 publishes the production numbers and turns the design targets into hard CI gates from `v0.1.0-beta.1` onward. Until then, the CI gate is "ran successfully + no regression vs previous run", not the absolute target. Why: committing to absolute numbers in Phase 1 means the first benchmark miss reads as a broken promise; ratifying in Phase 5 after three phases of measurement reads as "here's what it actually does." See the "Honest publication policy" below — the same logic applies to the targets themselves, not just the published results.

**Latency.** Stated target on default polling backoff: **p50 ~200ms, p99 ~1.5s**. Aggressive polling (100ms cap) gets p50 ~75ms / p99 ~250ms at the cost of catalog QPS. Sub-100ms p50 requires the upstream notification hook (post-1.0) and is not a v0.1 fight. Document this position **loudly** — users coming from Debezium's 10-100ms latency expectations will otherwise be disappointed silently.

**Throughput per consumer.** Bounded by `table_changes` Parquet scan rate (~1-2M rows/sec/thread for inserts on local NVMe, ~200MB/s on S3) and the sink's own write rate. Realistic ranges:

| Sink | Throughput |
| --- | --- |
| Stdout | ~100-500k rows/sec (Python overhead dominates; Go ~5-10× faster) |
| Webhook (sync, no batching) | ~1-5k rows/sec |
| Webhook (with batching) | ~10-50k rows/sec |
| Kafka | ~100k-1M rows/sec (batch + compression dependent) |
| Postgres mirror (COPY) | ~50-200k rows/sec |
| Postgres mirror (INSERT) | ~5-20k rows/sec |

**Catalog load.** Per-consumer steady-state cost:

- **Idle, default backoff:** ~0.1-0.2 catalog QPS (one `cdc_wait` poll every 5-10s once backoff has saturated).
- **Active, 10 batches/sec:** ~30-50 catalog QPS (window + commit + heartbeat).
- **`LakeWatcher`-amortised** (Phase 3 / 4): N consumers in one process share the snapshot-polling QPS. 50 consumers in one process ≈ 1 process's worth of `cdc_wait` cost, not 50.

**Per-consumer cost.** Real bottleneck is **catalog connection count** — each consumer holds one dedicated connection. Recommended limits:

- **~50 consumers per Postgres catalog** without connection pooling (Postgres' default `max_connections = 100`, leaving headroom for OLTP).
- **~500 consumers** with PgBouncer / pgcat in front of the catalog.
- **`LakeWatcher` amortisation is in-process only.** N consumers in *one* process share one polling loop. N consumers across N *separate* processes (one-per-pod, one-per-tenant SaaS, serverless) do **not** share — that's N× the polling load, no matter the per-process count. Cross-process amortisation is out of scope for v0.1. Operational workaround documented in `docs/operational/scaling.md`: ship a sidecar that polls `current_snapshot()` and fans out the result to peer consumer processes, or accept the fan-out cost and raise the `cdc_wait` backoff cap. Stating this honestly in the ADR avoids the surprise where a multi-pod deployment treats the in-process number as the cross-process number.
- Documented in `docs/operational/scaling.md`.

#### `max_snapshots` hard cap

`max_snapshots` is user-controllable. Without a hard cap a user can self-DOS by passing `max_snapshots := 10_000_000` and asking the engine to scan the entire lake. The cap:

- Default `max_snapshots`: 100.
- **Hard cap: 1000**, configurable per session via `SET ducklake_cdc_max_snapshots_hard_cap = ...`. Above that, the call is rejected with `CDC_MAX_SNAPSHOTS_EXCEEDED` (a new error code added to `docs/errors.md`).
- Documented as one of the tuning knobs in `docs/performance.md`.

#### Three benchmark workloads (ratified here, run in CI from Phase 1 onward)

| Workload | Snapshots/min | Rows/snapshot | Consumers | Sinks | Latency p99 target | Throughput target |
| --- | --- | --- | --- | --- | --- | --- |
| **light** | 10 | 100 | 1 | stdout | < 1s | matches producer; 0% lag drift over 24h |
| **medium** | 100 | 1 000 | 5 | mix (webhook + Kafka) | < 5s | matches producer at steady state |
| **heavy** | 1 000 | 10 000 | 20 | Kafka | < 30s | matches producer at steady state |

Each workload should be committed as a YAML descriptor when the benchmark harness lands. Phase 1 owns the light harness as an open follow-up in the current clean-slate tree. Phase 2 adds `medium` across the catalog matrix. Phase 5 adds `heavy` and the 72h soak runs both `medium` (chaos) and `heavy` (sustained-load).

**Honest publication policy:** numbers go in `docs/performance.md` with date + commit SHA + hardware. Numbers are not removed when they regress — they are explained. **Numbers without history are flat; numbers with history are a story.**

#### Workloads we are good at / poorly suited to

Stated in `README.md` "Performance principles" section. ADR 0011 ratifies it as a contract; reviewers reject features that imply a poor-fit workload is something we plan to be good at.

### Public API freeze

- [x] Lock the **5 primitives** (write SQL stubs in `docs/api.md`):
  - `cdc_consumer_create(name, tables := NULL, change_types := NULL, event_categories := NULL, stop_at_schema_change := true, dml_blocked_by_failed_ddl := true, lease_interval_seconds := 60, start_at := 'now')`, `cdc_consumer_drop`, `cdc_consumer_list`, `cdc_consumer_reset(name, to_snapshot := NULL)`, `cdc_consumer_heartbeat(name)`, `cdc_consumer_force_release(name)`.
    - On-disk schema for `__ducklake_cdc_consumers` is the single source of truth in ADR 0009. Every parameter here corresponds to a column there.
    - `tables` is a hard list of fully-qualified table names. Globs and tags are out of scope for v0.1 (see Non-goals in `README.md`).
    - `change_types` is a list filtering DML (`insert`, `update_preimage`, `update_postimage`, `delete`). Default `NULL` = all four. **The extension never invents implicit DML filters**; the consumer declares the policy at create time (recorded on the consumer row, applied as a `WHERE` clause inside `cdc_changes` sugar) or filters in user-side `WHERE`.
    - `event_categories` is a list filtering by category: subset of `{ddl, dml}`. Default `NULL` = both. See ADR 0008 + filter composition below.
    - `stop_at_schema_change` (default `true`) controls whether `cdc_window` bounds at schema-version boundaries. See ADR 0006.
    - `dml_blocked_by_failed_ddl` (default `true`) controls whether a permanently-failed DDL apply halts DML for the affected table. See Phase 2 DLQ semantics.
    - `lease_interval_seconds` (default 60) — see ADR 0007.
  - `cdc_window(consumer, max_snapshots := 100)` → returns `(start_snapshot BIGINT, end_snapshot BIGINT, has_changes BOOLEAN, schema_version BIGINT, schema_changes_pending BOOLEAN)`. Validates cursor against `ducklake_snapshot`; raises structured `CDC_GAP` if expired by compaction; raises `CDC_BUSY` if a different connection holds the consumer's lease (see ADR 0007); raises `CDC_MAX_SNAPSHOTS_EXCEEDED` if `max_snapshots` exceeds the session-wide hard cap (default 1000; see ADR 0011). **Idempotent until commit on the holding connection.** Honors per-consumer `stop_at_schema_change` (see ADR 0006).
  - `cdc_commit(consumer, snapshot_id)` — advances the cursor and refreshes `owner_heartbeat_at`. Verifies `owner_token` matches the calling connection's lease before committing; raises `CDC_BUSY` if the lease was lost.
  - `cdc_wait(consumer, timeout_ms := 30000)` → returns `BIGINT` (new `current_snapshot()`) or `NULL` on timeout. Honest polling implementation. **Hard cap on `timeout_ms`: 5 minutes** (configurable via `SET ducklake_cdc_wait_max_timeout_ms`); larger values are clamped with a notice.
  - `cdc_ddl(consumer, max_snapshots := 100)` → table function returning typed DDL events for the consumer's window. Schema and `details` payload locked in ADR 0008. Honors the consumer's `event_categories` filter (returns zero rows if `event_categories = ['dml']`).
- [x] Lock the **observability primitive**: `cdc_consumer_stats()` → table function with lag (snapshots + seconds), throughput counters, gap counters, schema-change counters, wait-call counters, DLQ-pending counts.
- [x] Lock the **acknowledged sugar wrappers** (each documented as a 1-line composition over the primitives + DuckLake builtins):
  - `cdc_events(consumer, max_snapshots := 100)` ≡ `SELECT * FROM <lake>.snapshots() WHERE snapshot_id BETWEEN cdc_window.start AND cdc_window.end` plus `tables` filter. Returns the raw structured `changes` map including DDL, DML, and `compacted_table` maintenance entries — for inspection / dashboards, not for typed sink processing.
  - `cdc_changes(consumer, table, max_snapshots := 100)` ≡ `table_changes(table, cdc_window.start, cdc_window.end) JOIN <lake>.snapshots() USING (snapshot_id)`. Applies the consumer's `change_types` filter as a `WHERE` if set. DML rows for one table.
  - `cdc_schema_diff(table, from_snapshot, to_snapshot)` ≡ "filter `cdc_ddl` to `altered.table` events for this table, between these snapshot bounds, and project the `details` payload to a flat row format." Stateless inspection helper. No consumer required.
  - `cdc_recent_changes(table, since := INTERVAL '5 minutes')` — **stateless**, no consumer required. Pure passthrough to `table_changes(table, max(snapshot_id WHERE snapshot_time < now() - since), current_snapshot())`. Exists because the SQL-tinkerer persona's first 30 seconds shouldn't require creating durable state. Sugar only — no cursor, no commit, no gap detection.
  - `cdc_recent_ddl(since := INTERVAL '1 day', table := NULL)` — stateless DDL companion. "What schema changes happened in the last day?" Sugar over `cdc_ddl` extraction logic with bare snapshot bounds.
  - None auto-commit; durable consumers still call `cdc_commit(consumer, batch.next_snapshot_id)`.
- [x] Decide tag conventions for opt-in / routing using `ducklake_tag` and `ducklake_column_tag`: `cdc.enabled`, `cdc.topic`, `cdc.schema_registry_subject`. (No new functions needed; just documented conventions. v0.1 reads tags but doesn't implement tag-based selection — that's v0.2.)

#### Filter composition rules (lock here, all bindings inherit)

Three filter knobs (`tables`, `change_types`, `event_categories`) compose. Without locking the precedence and combination logic explicitly, every binding will guess differently and Python and Go will diverge on day one. The rules:

- **`event_categories` is a top-level switch.** It selects which *streams* the consumer reads at all.
  - `NULL` (default) or `['ddl', 'dml']`: both `cdc_ddl` and `cdc_changes` return rows.
  - `['dml']`: `cdc_ddl` returns zero rows; `cdc_changes` operates normally.
  - `['ddl']`: `cdc_changes` returns zero rows; `cdc_ddl` operates normally.
- **`tables` filters both DDL and DML events**, when set, to those whose `(schema, name)` is in the list.
  - On DDL: an `altered.table` event fires only if the affected table's qualified name is in `tables`. A `created.schema` event with `tables` set is **not** emitted (schemas have no qualified table name to match) — schema-level DDL is implicitly suppressed for table-filtered consumers. A `created.table` event for a brand-new table fires only if that table's qualified name is in `tables` (the create-time validation of `tables` happens against snapshots reachable from the start cursor; new tables added after consumer creation that aren't in the list are simply ignored).
  - On DML: behaves as today — `cdc_changes(consumer, table)` is naturally per-table; `cdc_events` filters the structured map keys per the Phase 0 enumeration spike.
  - For consumers that legitimately need schema-level DDL (`create_schema`, `drop_schema`), they must subscribe with `tables := NULL`.
- **`change_types` filters DML only.** DDL events have no `change_type` field; `change_types` is silently ignored when reading `cdc_ddl`. Documented loudly so readers don't expect DDL filtering by `change_types`.
- **All three filters compose with AND semantics where they overlap.** A consumer with `tables := ['main.orders']`, `change_types := ['delete']`, `event_categories := ['ddl', 'dml']` sees:
  - DDL: `created.table`, `altered.table`, `dropped.table` events for `main.orders` (the create event arrives if `start_at` precedes the table's creation snapshot).
  - DML: only `delete` events on `main.orders`.
- **Matching is exact**, case-sensitive, on the qualified name as DuckLake stores it (`schema.table`). No glob, no regex, no fuzzy matching.

Document this table in `docs/api.md#filter-composition` as the canonical reference. Both Python and Go test suites assert this matrix verbatim.

### Structured error format (NEW)

User-facing error messages are UX. Lock the format in Phase 0 so Phase 1 implements it correctly the first time.

- [x] `CDC_GAP` error must carry: consumer name, current cursor, oldest available snapshot, suggested recovery command. Reference message:

  ```text
  CDC_GAP: consumer 'my_session' is at snapshot 42, but the oldest
  available snapshot is 100 (snapshots 42-99 were expired by
  ducklake_expire_snapshots). To recover and skip the gap:
    CALL cdc_consumer_reset('my_session', to_snapshot => 'oldest_available');
  To preserve all events, run consumers more frequently than your
  expire_older_than setting.
  ```

- [x] `CDC_BUSY` (lands Phase 1, with the lease) — message includes the consumer name, the existing `owner_token`, `owner_acquired_at`, `owner_heartbeat_at`, and the lease interval. Suggests `cdc_consumer_force_release(name)` when the holder's heartbeat is older than `2 × lease_interval_seconds` (likely dead). Reference message:

  ```text
  CDC_BUSY: consumer 'indexer' is currently held by token
  a1b2c3d4-... (acquired 2026-04-28T12:01:00Z, last heartbeat
  2026-04-28T12:01:42Z; lease interval 60s).
  The holder appears alive; wait for it to release, or run
    CALL cdc_consumer_force_release('indexer');
  if you know it has died.
  ```
- [x] `CDC_SCHEMA_BOUNDARY` (informational, not error) — emitted via DuckDB `Notice` when a `cdc_window` call returns `schema_changes_pending = true`. Includes the affected `schema_version` transition.
- [x] `CDC_INVALID_TABLE_FILTER` — at `cdc_consumer_create` time when `tables` references a non-existent table. Fail loud at create-time, not silently at first-poll time.
- [x] `CDC_MAX_SNAPSHOTS_EXCEEDED` — when `cdc_window`'s `max_snapshots` arg exceeds the session-wide hard cap (default 1000; see ADR 0011). Message names the cap and the `SET ducklake_cdc_max_snapshots_hard_cap = ...` raise-the-cap escape hatch.

### Special-type and edge-case policy

- [x] Document default serializers for non-portable types so downstream sinks have a contract:
  - `variant` → JSON (using DuckDB's variant cast). Use shredded sub-fields from `ducklake_file_variant_stats` where present.
  - `geometry` → GeoJSON. Bounding box surfaced as a separate column.
  - `blob` → base64 by default; passthrough binary as an option.
  - `decimal` → exact string (preserve precision/scale).
  - Nested types (`list`, `struct`, `map`) → JSON.
- [x] **Promote to design pillar (already done in `README.md`):** these serializers run in language clients, *not* in the extension. The extension exposes raw DuckLake values. This keeps the C++/Rust extension small and avoids forcing it to depend on JSON / GeoJSON / Avro / etc. libraries.
- [x] Document per-backend caveats from the spec (PostgreSQL catalogs cannot store `variant` inline; SQLite collapses many types to `TEXT`).
- [x] Document explicitly: encryption, inlined-data and partial-file handling are inherited from `table_changes`; we test that we do not break them, but we do not claim them as features.

### Producer-side outbox metadata convention (NEW)

If we don't pick a recommended shape, every project will invent its own JSON in `commit_extra_info` and the sink ecosystem fragments before it exists. Recommended (not enforced):

```json
{
  "event": "OrderPlaced",
  "trace_id": "01HX...",
  "schema": "orderplaced.v1",
  "actor": "user:42",
  "extra": { ... }
}
```

- `event` is the only recommended-required field.
- All others are optional but standardized when present.
- The library helpers in Phase 3 / 4 (`ducklake_cdc.outbox(...)`, `ducklakecdc.WithOutbox(...)`) accept these as named arguments and serialize them. Callers can still pass arbitrary JSON via `extra` — we don't enforce the shape, we recommend it.
- Reference sinks (webhook, kafka) route on `event` and propagate `trace_id` when present.

Document in `docs/conventions/outbox-metadata.md`.

### Delivery ordering contract (NEW)

`the prototype implementation` line 367 sorts by `(snapshot_id, table_id, rowid)`. We need to commit to a delivery order so consumers can rely on it.

- [x] Document in `docs/api.md`:
  - **Across snapshots:** strict `snapshot_id` ascending. Always.
  - **Within a snapshot, between categories:** **DDL events before DML events** (per ADR 0008). A sink that needs to e.g. provision a new column before receiving rows under that column relies on this.
  - **Within a snapshot, within DDL:** by `(object_kind, object_id)` ascending. `object_kind` sort order: `schema, view, table` (creates schemas first, drops tables before schemas).
  - **Within a snapshot, within DML, across tables:** by `table_id` ascending. (Cheap, deterministic, gives consumers a stable order they can hash.)
  - **Within a snapshot, within DML, within a table:** by `rowid` ascending.
- [x] These are guarantees of the sugar wrappers (`cdc_changes`, `cdc_events`, `cdc_ddl`) and the language clients (which interleave `cdc_ddl` and `cdc_changes` per snapshot). Raw `table_changes` callers get whatever DuckLake returns; we don't reorder for them. Same for `snapshots().changes` — it's a structured map and has no inherent order.

### Threat model (NEW — `docs/security.md`)

- [x] State explicitly: `__ducklake_cdc_consumers` is world-readable to anyone with catalog read access. Consumer names should not encode secrets. In multi-tenant catalogs, tenant isolation is the catalog's responsibility (per-schema GRANTs); we don't add a layer.
- [x] SQL-injection surface in consumer-name and table-name handling: bind every user-supplied identifier through DuckDB's quoting helpers, never string-concatenate into SQL. Reference: `the prototype implementation:_q` is the right pattern.
- [x] `set_commit_message`'s `extra_info` is consumer-trusted input. Document that downstream sinks should treat it as untrusted (validate JSON shape, length-limit, escape before logging).

### Operational policies (NEW)

- [x] **Backpressure semantics.** What happens when a consumer can't keep up and `cdc_window` keeps returning the same `(start, end)` because `max_snapshots` is hit every cycle? Document: lag grows monotonically until `expire_older_than` catches the cursor and the next poll returns `CDC_GAP`. Recommend `expire_older_than > max_expected_lag` and point at `cdc_consumer_stats()` for monitoring.
- [x] **Dropped-table policy.** A consumer with `tables := ['orders']` and then `DROP TABLE orders`. Decision: the consumer continues advancing (snapshots without `orders` changes are no-ops); `cdc_consumer_stats()` flags it as `tables_unresolved = ['orders']`. The consumer is not killed; if the user wanted to halt-on-drop, they read the stats. Documented.
- [x] **Catalog-version compatibility.** The extension pins to a `ducklake` catalog-schema version range. We publish a compatibility matrix at `docs/compatibility.md`. Promise: we lag DuckLake major-version releases by at most 4 weeks. Breaking catalog changes get a major version bump in the extension.

### Project-level decisions

- [x] Implementation language: C++ (canonical extension template) vs. Rust (`duckdb-rs` extension support). ADR `docs/decisions/0001-extension-language.md`.
- [x] License: **default to Apache-2.0** (current). ADR `docs/decisions/0003-license.md` should ratify this and document the rationale: the explicit patent grant matters if the project ever goes commercial (private maintainer notes §11 explicitly defers that decision); switching from Apache-2.0 → MIT later is one-way-easy (relax), MIT → Apache-2.0 is hard (tighten, requires re-licensing all contributions). The DuckDB / DuckLake ecosystem leans MIT, but the only thing that should override the default is an explicit request from the DuckDB team to align for a specific reason (e.g. listing on community-extensions). **In the absence of that request, ship Apache-2.0** and remove "MIT alignment is being reconsidered" wording from the README.
- [x] Project skeleton: build system, CI, code of conduct, contribution guide, issue templates. (License + governance + non-template skeleton parts ship in Phase 0; the C++ build scaffold lands in Phase 1's first work item per ADR 0001 § "Phase 0 vs Phase 1 skeleton scope".)

### Deferred to Phase 5 (do not start in Phase 0, but track now)

- [x] **Upstream DuckLake notification-hook feature request.** `cdc_wait` is polling because DuckLake has no commit-notification mechanism. The right long-term fix is a hook that fires on snapshot commit so the extension can subscribe instead of polling. We will file the issue / draft a discussion in Phase 5 *after* the demo exists, because (a) the demo is the proof that the feature is worth shipping, and (b) we don't want to ask DuckLake maintainers to design something for a project that hasn't shipped a single user-facing artifact yet. Tracked in `docs/upstream-asks.md`.

- [x] **Spec-vs-implementation enumeration spike (`test/upstream/enumerate_changes_map.py`).** The DuckLake spec documents `ducklake_snapshot_changes.changes_made` (text) exhaustively but only partially documents `<lake>.snapshots().changes` (the typed MAP returned by the extension). Confirmed-from-spec MAP keys: `schemas_created`, `tables_created`, `inlined_insert`. Confirmed-from-reference-impl MAP keys: `tables_inserted_into`, `tables_deleted_from`, `inlined_delete`. Unverified: the MAP keys for `dropped_*`, `altered_*`, `compacted_table`, `created_view`, `dropped_view`, `altered_view`. The spike runs every DDL/DML/maintenance op against a real DuckLake (each backend) and dumps the resulting MAP per snapshot, producing a definitive `changes_made` text → `snapshots().changes` MAP key reference. Output committed to `docs/api.md#snapshots-changes-map-reference`. Phase 1 `cdc_events` filter SQL and `cdc_ddl` extraction rules **block on this**. **The spike is then promoted to a recurring CI job (Phase 1 work item) that diffs the key-set against every supported DuckLake version on every build and fails on diff.** Treat a key-set diff as a hard build break — silent upstream changes to the MAP keys are the single highest-impact silent failure mode in the DDL extractor.

- [x] **Document the `DATA_INLINING_ROW_LIMIT` default = 10 correction.** The reference implementation `the prototype implementation` line 248 comments `default 100` — that's wrong; the spec is 10. Make sure the test fixtures set `DATA_INLINING_ROW_LIMIT` explicitly so behavior is deterministic across DuckLake versions, and call out the discrepancy in the Phase 5 migration guide so users porting from `the prototype implementation` don't get surprised by an off-by-an-order-of-magnitude inlining threshold.

- [x] **Document the inline-on-inline delete invisibility.** Per spec ("Deletion Inlining" note): a delete that targets an inlined-insert row is not recorded as an `inlined_delete`; instead, `end_snapshot` is set on the inlined-insert row directly. From `snapshots().changes` this snapshot looks like there was no DML. `table_changes()` still returns the right rows (because it reads from the inlined storage with snapshot ranges). So `cdc_events` may show "empty" snapshots that nonetheless yield rows from `cdc_changes`. Document this in `docs/operational/inlined-data-edge-cases.md` so consumers don't write logic that assumes "empty `cdc_events` → no work".

## Exit criteria

**Status: closed 2026-04-28.** Every criterion below is satisfied in
tree. Phase 1 (`./phase_1_extension_mvp.md`) is unblocked.

- [x] Eleven ADRs merged in `docs/decisions/`:
  - [x] 0001 extension language
  - [x] 0002 hybrid discovery / read
  - [x] 0003 license
  - [x] 0004 overlap audit
  - [x] 0005 extension-vs-library (with the corrected "one canonical SQL surface" justification, the multi-language commitment statement, and the named third binding)
  - [x] 0006 schema-version boundaries (with `stop_at_schema_change` opt-out)
  - [x] 0007 concurrency model + the **owner-token lease** as the single-reader enforcement mechanism (replaces the per-backend advisory-lock plan)
  - [x] 0008 DDL as a first-class event stream (with `cdc_ddl` primitive, schema_diff demoted to sugar, two-stage extraction explicit, DDL-before-DML ordering, column-drop audit limitation)
  - [x] 0009 consumer-state schema — single source of truth for `__ducklake_cdc_consumers`, including the lease columns and `dml_blocked_by_failed_ddl`
  - [x] 0010 audit log — schema and `action` enum for `__ducklake_cdc_audit`
  - [x] 0011 performance model + benchmark workloads (light / medium / heavy), `max_snapshots` hard cap, "good fit / poor fit" workload contract
- [x] API surface stubs in `docs/api.md`, reviewed and frozen, with each entry classified as `primitive`, `observability`, or `acknowledged sugar`. `details` JSON shapes for `cdc_ddl` event kinds are locked here.
- [x] Filter composition matrix (`tables` × `change_types` × `event_categories`) published in `docs/api.md#filter-composition`.
- [x] Structured error formats locked in `docs/errors.md`, including `CDC_BUSY` (lease lands in Phase 1, so the format ships now).
- [x] Producer outbox metadata convention published in `docs/conventions/outbox-metadata.md`.
- [x] Delivery ordering contract documented in `docs/api.md`, including DDL-before-DML within a snapshot.
- [x] Threat model published in `docs/security.md`.
- [x] Audit-sink limitations (column-drop in same snapshot as DML) documented in `docs/operational/audit-limitations.md`.
- [x] Inlined-data edge cases (inline-on-inline deletes, default-10 vs reference-impl-100 inlining limit) documented in `docs/operational/inlined-data-edge-cases.md`.
- [x] `snapshots().changes` MAP key reference (output of `test/upstream/enumerate_changes_map.py`) committed to `docs/api.md#snapshots-changes-map-reference`. Phase 1 SQL is unblocked by this.
- [x] Catalog-version compatibility matrix scaffolded in `docs/compatibility.md`.
- [x] Spike script promoted into `test/upstream/enumerate_changes_map.py` with reference outputs under `test/upstream/output/`.
- [x] `docs/upstream-asks.md` lists the DuckLake notification-hook proposal with rationale; the formal upstream issue is filed **3 months post-beta**, not in Phase 5 (we want real users' polling-load data to motivate it).
- [x] Project skeleton (build, CI, license, governance) on `main`. License (Apache-2.0), governance (`CONTRIBUTING.md` + `CODE_OF_CONDUCT.md` + `.github/ISSUE_TEMPLATE/`), and docs CI are in tree; the C++ build scaffold lands in Phase 1's first work item per ADR 0001 § "Phase 0 vs Phase 1 skeleton scope".
