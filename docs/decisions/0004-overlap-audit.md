# ADR 0004 — Overlap audit: small surface, no shadowing

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

The non-overlap principle (`docs/roadmap/README.md` — pillar 1)
is the project's reason to exist: anything DuckLake already does, we use
directly. ADR 0005 will argue the extension *form* on the "one canonical SQL
surface" axis (pillar 12); this ADR tests that argument empirically by
labeling every function in scope.

Two surfaces are audited:

1. **DuckLake's existing consumer-relevant API** — the catalogue from
   `docs/roadmap/`. Each entry is labeled
   `use-directly` (the project depends on it; no wrapper) or `not-relevant`
   (out of consumer-side scope).
2. **The reference Python implementation** (the prototype implementation) — what
   we already shipped against an early DuckLake without the extension. Each
   function is labeled `keep` (lifts into the extension as-is or as
   internals), `demote-to-sugar` (becomes a documented composition or a
   language-client helper), or `drop` (replaced by a DuckLake builtin or
   pushed out of scope).

Both surfaces inform the third tally — our **proposed extension surface** —
which must show `keep` (primitive) outnumbering `demote-to-sugar` for the
extension form to deserve to exist. If `demote` outnumbers `keep`, the
SQL-helper-library answer wins and ADR 0005 must reflect that.

## Decision

### DuckLake's existing surface (16 logical entries / 18 functions)

Every entry is labeled `use-directly`. The project adds **no shadow** of any
DuckLake builtin.

| # | DuckLake function / object | Role | Label | How we use it |
| --- | --- | --- | --- | --- |
| 1 | `<lake>.snapshots()` | per-snapshot change manifest with structured `changes` MAP | `use-directly` | Stage-1 discovery (pillar 2) and `cdc_events` sugar. We do not parse `ducklake_snapshot_changes.changes_made` ourselves — `snapshots().changes` already returns it as a typed MAP. |
| 2 | `<lake>.current_snapshot()` | latest snapshot id | `use-directly` | `cdc_wait` polls this; `cdc_window` reads it as the upper bound of the uncapped range. |
| 3 | `<lake>.last_committed_snapshot()` | connection-scoped last commit | `use-directly` | Available to orchestrators for in-process bookkeeping. We do not wrap. |
| 4 | `CALL <lake>.set_commit_message(author, msg, extra_info => ...)` | producer-side outbox metadata API | `use-directly` | Producers call this directly. We document a JSON convention for `extra_info` in `docs/conventions/outbox-metadata.md` and surface the value on every batch — we do not wrap the call itself. |
| 5 | `table_changes(t, start, end)` | row-level DML across a snapshot range | `use-directly` | Read surface (pillar 2). Inherits inlined-data, encryption, partial-file and post-compaction handling, plus schema-evolution-on-read, for free. |
| 6 | `table_insertions(t, start, end)` | inserts only | `use-directly` | Specialised form of `table_changes`. Available to users; we do not wrap. |
| 7 | `table_deletions(t, start, end)` | deletes only | `use-directly` | Specialised form of `table_changes`. Available to users; we do not wrap. |
| 8 | `tbl AT (VERSION => N)` / `tbl AT (TIMESTAMP => ...)` / `ATTACH ... (SNAPSHOT_VERSION ...)` | full time travel (3 surface forms) | `use-directly` | Audit-recovery example in `docs/operational/audit-limitations.md` invokes `AT (VERSION => snapshot_before_drop)` directly. We do not add a `cdc_replay`-style wrapper in v0.1; Phase 5 may ship a thin CLI helper. |
| 9 | `ducklake_expire_snapshots` | maintenance: snapshot expiry | `use-directly` (operator-facing) | We detect the gap it creates and surface `CDC_GAP` with the recovery `CALL` (pillar 7). We do not run it ourselves. |
| 10 | `ducklake_merge_adjacent_files` | maintenance: small-file compaction | `use-directly` (operator-facing) | Compaction is a partner, not an enemy (pillar 7). |
| 11 | `ducklake_cleanup_old_files` | maintenance | `use-directly` (operator-facing) | Operator runs as needed. |
| 12 | `ducklake_delete_orphaned_files` | maintenance | `use-directly` (operator-facing) | Operator runs as needed. |
| 13 | `ducklake_rewrite_data_files` | maintenance | `use-directly` (operator-facing) | Operator runs as needed. |
| 14 | `ducklake_flush_inlined_data` | maintenance: flush inlined storage | `use-directly` (operator-facing) | Operator runs as needed. Discovery already handles `inlined_insert` / `inlined_delete` keys (pillar 2). |
| 15 | `ducklake_list_files(catalog, table, ...)` | file listing | `use-directly` | Available to users; out of consumer-side scope. |
| 16 | `ducklake_snapshot_changes` (catalog table) and `ducklake_inlined_data_*` (catalog tables) | conflict resolution / `changes_made` VARCHAR / inlined-batch storage | `use-directly` (transparent) | Conflict resolution is inherited automatically (pillar 1). `ducklake_snapshot_changes.changes_made` is enumerated in ADR 0008 for completeness, but stage-1 discovery uses the typed MAP variant `snapshots().changes`. `table_changes` reads inlined batches transparently. |

**Result:** 16/16 entries `use-directly`. The non-overlap principle holds —
the project ships zero wrappers around DuckLake builtins.

### Reference implementation (the prototype implementation, 14 functions / classes / constants)

| # | Reference symbol | What it does | Label | Disposition |
| --- | --- | --- | --- | --- |
| 1 | `_q(name)` | quote-identifier helper | `lifted-as-pattern` | Not a function we ship; the identifier-quoting *pattern* lifts into the extension. `docs/security.md` (threat model) names it as the canonical defense against the SQL-injection surface in user-supplied consumer / table names. Excluded from the keep / demote / drop tally below because it does not occupy a callable slot in either surface. |
| 2 | `RowEvent` dataclass | typed per-row event shape | `demote-to-sugar` | Equivalent shape lives in language clients (Phase 3 Python `Event`, Phase 4 Go `Event`). The extension returns the underlying columns natively via `cdc_changes`. |
| 3 | `load_cursor()` / `save_cursor()` | atomic file-write cursor (`/tmp/.dlcdc_cursor`) | `drop` | Cursor lives in `__ducklake_cdc_consumers.last_committed_snapshot` (ADR 0009), transactional with the read. The file-cursor was the only-Postgres-reference-impl shortcut; it cannot survive multi-consumer or multi-process deployments. |
| 4 | `pg_head()` | latest snapshot id via direct `psycopg.connect(...)` `SELECT max(snapshot_id) FROM public.ducklake_snapshot` | `drop` | Replaced by `current_snapshot()` (entry 2 of Table 1). The reference impl bypassed DuckDB to save a round-trip; the extension calls the builtin and is therefore backend-portable. |
| 5 | `pg_table_map()` | `table_id → (schema_id, schema_name, table_name)` via direct Postgres `SELECT` | `drop` | Same — bypasses DuckLake. The extension joins `ducklake_table` + `ducklake_schema` through DuckDB, backend-portably across DuckDB / SQLite / Postgres catalogs (Phase 2). |
| 6 | `write_outbox_batch()` | `COPY` events into a Postgres `cdc_event` table | `drop` | Sink-side, not extension-side. Equivalent reference sinks live in Phase 3 (Python) and Phase 4 (Go) clients. The extension returns events; sinks consume them. |
| 7 | `_make_duck()` / `DuckPool` (class with `acquire` / `release` / `close`) | DuckDB connection pool with `ATTACH` + S3 secret per connection | `drop` | Connection pooling is the language-client's job (`LakeWatcher` in Phase 3 / 4). Extension users bring their own connection. The "one connection per `cdc_wait` call" guidance lives in `docs/operational/wait.md`. |
| 8 | `snapshot_changes(conn, start_exc, end_inc)` | `SELECT snapshot_id, snapshot_time, changes FROM <cat>.snapshots() WHERE snapshot_id > ? AND snapshot_id <= ? ORDER BY snapshot_id` | `keep` (as `cdc_window` stage-1 + `cdc_events` sugar) | This is exactly stage-1 discovery (pillar 9). Lifted into the extension so all bindings inherit it without re-implementation. |
| 9 | `_AFFECTED_CHANGE_KEYS` constant | hard-coded tuple of 4 MAP keys (`tables_inserted_into`, `tables_deleted_from`, `inlined_insert`, `inlined_delete`) | `keep` (as the upstream probe output) | The Phase 0 upstream probe (`test/upstream/enumerate_changes_map.py`) replaces the hard-code with an empirically-verified, version-stamped key set. Promoted to a recurring CI job in Phase 1 per ADR 0008. |
| 10 | `affected_table_ranges(snap_rows)` | collapse per-snapshot `table_id` lists into `(min_snapshot, max_snapshot)` ranges across the batch | `keep` (as `cdc_window` internal range-coalescing) | Internal optimisation that lets the orchestrator fan out fewer `table_changes` calls. Not user-visible. |
| 11 | `_table_row_events(...)` | per-table `table_changes(?, ?, ?)` query with `USE` schema dance, returns list of `RowEvent` | `demote-to-sugar` (`cdc_changes`) | This is `cdc_changes(consumer, table)` sugar plus the language-client's row-to-event conversion (`RowEvent` build). The extension exposes the SQL composition; the conversion lives in Python / Go. |
| 12 | `fetch_all_row_events(pool, table_map, table_ranges)` | `ThreadPoolExecutor` fan-out across tables, per-table `_table_row_events`, sort by `(snapshot_id, table_id, rowid)` | `drop` (from extension) / `demote-to-sugar` (in language clients) | Pillar 5 carves out the "orchestrator fans out `table_changes` reads from a pool" pattern as language-client territory, not extension code. The extension exposes the primitives that make this pattern safe (idempotent `cdc_window` until `cdc_commit`). The sort is the delivery-ordering contract (ADR 0008). |
| 13 | `poll_once(pool, cursor)` | one poll cycle: `pg_head` → `snapshot_changes` → `affected_table_ranges` → `pg_table_map` → `fetch_all_row_events` | `demote-to-sugar` | Reconstructed as `cdc_window` + `cdc_changes` per affected table + client-side sort. The reconstruction is the canonical composition documented for `cdc_changes` in `docs/api.md`. |
| 14 | `main()` | polling loop with signal handling, exponential-backoff on consecutive failures, `create_alert_sync` on poll-stall, `time.sleep(POLL_S)` between cycles, `save_cursor` after each successful batch | `demote-to-sugar` | Becomes the language-client `tail()` / `Tail()` helper (Phase 3 / 4) with `cdc_wait` replacing `time.sleep` and `cdc_consumer_stats()` replacing the manual stall-alert (the operator queries stats; the daemon doesn't reach into an alerting system). |

**Result:** 14 reference symbols, labeled as **3 keep, 3 demote-to-sugar, 8 drop, 1 lifted-as-pattern (`_q`).** Only one constant is `keep-as-upstream-probe-output`; the rest collapse cleanly. The 14 symbols sum as 3 + 3 + 8 + 1 = 14 — the bottom-line headline is the keep/demote ratio against the eight drops, not a count that absorbs the pattern row.

The drops are concentrated in the four classes the reference impl had to invent because the extension didn't yet exist:

- direct Postgres bypasses (`pg_head`, `pg_table_map`) — eliminated by going through DuckDB,
- file-based cursor (`load_cursor`, `save_cursor`) — eliminated by the catalog-resident consumer row,
- sink machinery (`write_outbox_batch`) — moved to language clients,
- connection pooling (`_make_duck`, `DuckPool`) — moved to language clients.

### Proposed extension surface (10 primitive callables + 1 observability + 5 sugar = 16 total)

The phase doc groups these as "5 primitives" by SQL-CALL signature family.
At callable-count granularity:

**Primitives (10 callables across 5 families):**

| Family | Callables | Why a primitive (not sugar) |
| --- | --- | --- |
| Consumer lifecycle | `cdc_consumer_create`, `cdc_consumer_drop`, `cdc_consumer_list`, `cdc_consumer_reset`, `cdc_consumer_heartbeat`, `cdc_consumer_force_release` (6) | Backend-portable single-row INSERT / UPDATE / DELETE / SELECT against `__ducklake_cdc_consumers` (ADR 0009). No DuckLake equivalent. The lease columns make `force_release` and `heartbeat` non-trivial (ADR 0007). |
| `cdc_window` | 1 | Composes `snapshots()` + `current_snapshot()` + the conditional lease UPDATE + a per-table-aware schema-version boundary scan — five reads and one write inside one transaction (ADR 0007 + ADR 0006). Cannot be one query. |
| `cdc_commit` | 1 | Single conditional UPDATE that asserts `owner_token`, advances the cursor, refreshes `last_committed_schema_version`, and bumps `owner_heartbeat_at`. Cannot be a DuckLake builtin (ADR 0007). |
| `cdc_wait` | 1 | Honest exponential-backoff polling against `current_snapshot()` with cooperative-cancellation semantics. A library could implement the polling loop, but having the canonical surface in the extension means the SQL-CLI persona's first 60 seconds works without a binding (pillar 12). |
| `cdc_ddl` | 1 | Two-stage extraction: stage-1 over `snapshots().changes`, stage-2 over the versioned catalog tables (`ducklake_table`, `ducklake_column`, `ducklake_view`, `ducklake_schema`) using `begin_snapshot` / `end_snapshot` ranges to reconstruct the typed `details` payload (ADR 0008). Cannot be expressed as a one-line composition. |

**Observability (1 callable):**

| Function | Why a primitive |
| --- | --- |
| `cdc_consumer_stats` | Aggregations over `__ducklake_cdc_consumers` + `ducklake_snapshot` joined; returns lag (snapshots + seconds), throughput, gap counters, schema-change counters, wait-call counters, DLQ-pending counts. Pure SQL but multi-table; lifting it into the extension means every binding gets the same dashboard query. |

**Acknowledged sugar (5 callables):** `cdc_events`, `cdc_changes`, `cdc_schema_diff`, `cdc_recent_changes`, `cdc_recent_ddl`. Each has its canonical one-line composition documented in `docs/roadmap/` and (when populated) in `docs/api.md`. `cdc_schema_diff` and `cdc_recent_ddl` reuse the `cdc_ddl` stage-2 extractor — one extractor in C++ / Rust, three surface functions.

### The ratio

**10 primitive callables vs 5 acknowledged sugar = 10 : 5 (2.0×).**
**16 callables total** = 10 primitive + 1 observability + 5 sugar.

The threshold the project committed to is "primitive outnumbers
demote-to-sugar"; the observability function answers a different question
(operator dashboards, not the read path) and is excluded from both sides
of that ratio for the threshold test. The non-overlap principle survives
on its own terms: 2× as many primitives as sugar, with no primitive
shadowing a DuckLake builtin.

At the family level the ratio is 5 primitive families : 5 sugar — a tie
on names but not on substance, because each primitive family carries
multiple callables (consumer-lifecycle alone has 6) and the sugar wrappers
are each one callable. The dominant failure mode the threshold is
designed to catch — "everything-is-sugar masquerading as an extension" —
is ruled out at either granularity.

The extension form deserves to exist on the surface-count axis. ADR 0005
picks up the broader extension-vs-library argument (the third-binding
commitment, the SQL-CLI persona, future pushdown opportunities).

### Prior-art revalidation

The "consumer-side CDC for DuckLake is empty space" claim is re-validated as
of 2026-04-28: no DuckLake-specific change-feed consumer ships in the public
ecosystem. Adjacent tools surveyed:

- **`pyiceberg`** reads Iceberg `table_changes` directly but has no
  DuckLake-specific cursor / wait / lease / DDL surface, and Iceberg's
  catalog model differs from DuckLake's enough that the cross-port is not
  free.
- **Debezium and OLTP-CDC variants** address Postgres → Kafka / Postgres →
  Kinesis. DuckLake is not their substrate; the four-axis performance model
  (ADR 0011) explicitly differentiates from sub-10 ms OLTP CDC.
- **DuckLake-side producer tools** (`pg_duckpipe` and similar) write *into*
  a DuckLake; this project reads *out of* one. They are complementary, not
  competitive.

The empty-space claim stands.

## Consequences

- **Phase impact.** Phase 1 implements 10 primitives + 1 observability + 5
  sugar wrappers (16 callables total). Phase 2 extends them to the
  three-backend matrix (DuckDB / SQLite / Postgres — DuckLake's three
  supported metadata backends). Phase 3 and Phase 4 wrap them in
  Python and Go clients. The 5 sugar functions are
  tested in Phase 1 against their canonical compositions — divergence
  between the wrapper SQL and the documented composition is a build
  break, so the sugar contract stays inspectable.
- **Reversibility.** All 11 non-sugar function names (10 primitives + 1
  observability) are **one-way doors**. Once they ship in `v0.1.0` they
  are renameable only via a major-version bump per the locking discipline
  in ADR 0009. The 5 sugar names are softer (sugar is documented
  composition; users who built directly on the underlying composition
  aren't affected by a sugar rename) but should still be treated as
  one-way doors for ergonomic continuity.
- **Third-binding budget.** Pillar 12 commits the project to a third
  binding (ADR 0005 will name it; recommended R). The 16-callable count
  is the implementation budget that third binding must clear: 6
  consumer-lifecycle wrappers (CRUD-shaped, trivial), 4 read-path wrappers
  (`cdc_window` / `cdc_commit` / `cdc_wait` / `cdc_ddl`), 1 stats wrapper,
  plus 5 sugar passthroughs. R-binding effort estimate: ~2 weeks.
- **Discipline locked here.** The audit table in this ADR is the
  authoritative record of what we ship and what we don't. Any future PR
  that adds a wrapper around a DuckLake builtin (e.g. proposes a
  `cdc_table_changes` or a `cdc_snapshots`) is rejected by reference to
  this document. Any future PR that proposes a new primitive must justify
  it against the same `keep` / `demote-to-sugar` / `drop` framework and
  add a row to the proposed-surface table.
- **the prototype implementation lifespan.** The reference impl stays at the
  repo root, gitignored, throughout the rest of Phase 0 — we read it
  constantly during ADRs 0006–0009. At Phase 0 exit, per the exit criteria,
  it is un-gitignored as the "what we want to make obsolete" baseline.

## Alternatives considered

- **SQL-helper library only (no extension).** Rejected: every binding
  re-implements `cdc_window`'s lease UPDATE + gap detection + schema-version
  bounding (ADR 0007 + ADR 0006), and `cdc_ddl`'s two-stage extraction
  (ADR 0008). Re-implementing those in Python *and* Go *and* R *and* Rust
  *and* Java *and* Node is the cost the extension form is designed to
  avoid (pillar 12). ADR 0005 carries the long version of this argument
  and is the document where the library-only counterfactual is honestly
  re-tested.
- **Smaller primitive surface (3 primitives + everything else as sugar).**
  Rejected on two axes:
  - `cdc_ddl`'s stage-2 reconstruction cannot be one-line sugar (ADR 0008
    is the proof: column-level diff via `ducklake_column.begin_snapshot`
    range arithmetic is not a composition).
  - The consumer-lifecycle CRUD has no DuckLake equivalent and cannot be
    omitted; demoting it to "users hand-write `INSERT INTO
    __ducklake_cdc_consumers`" defeats the durability and lease
    invariants.
  The 11 non-sugar callables (10 primitive + 1 observability) are a
  lower bound, not a target.
- **Mirror Debezium's surface.** Rejected: Debezium's surface assumes Kafka
  Connect framing and per-database connector semantics. We are a SQL
  surface against a lakehouse; the shapes don't transfer. The "good fit /
  poor fit" workload contract (ADR 0011) explicitly differentiates from
  OLTP CDC.
- **Wrap a few DuckLake builtins with `cdc_*`-prefixed aliases for
  discoverability** (e.g. `cdc_table_changes := table_changes`). Rejected:
  that's the shadowing pillar 1 forbids. Discoverability is the SQL CLI's
  `\df cdc*` tab-completion job, not the project's.

## References

- `docs/roadmap/`
- `docs/roadmap/README.md` — pillars 1 (non-overlap), 2
  (hybrid discovery + read), 5 (idempotence), 9 (DDL as first-class), 12
  (canonical surface)
- the prototype implementation — reference implementation, labeled
  function-by-function in Table 2 above
- ADR 0005 — Extension vs library (forthcoming; picks up the third-binding
  commitment and the SQL-CLI persona argument)
- ADR 0006 — Schema-version boundaries (forthcoming; locks the
  `cdc_window` schema-bounding logic)
- ADR 0007 — Concurrency model + owner-token lease (forthcoming; locks the
  `cdc_window` and `cdc_commit` lease SQL)
- ADR 0008 — DDL as first-class events (forthcoming; locks the `cdc_ddl`
  primitive and proves stage-2 cannot be sugar)
- ADR 0009 — Consumer-state schema (forthcoming; locks the
  `__ducklake_cdc_consumers` columns referenced by every primitive in this
  audit)
- DuckLake spec — Snapshots, Snapshot Changes, Field Identifiers, Inlined
  Data, Conflict Resolution
