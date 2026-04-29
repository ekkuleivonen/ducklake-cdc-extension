# ADR 0002 — Hybrid discovery + read: `snapshots()` for what moved, `table_changes` for the rows

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

A consumer-side CDC poll has to answer two questions on every cycle:

1. **What moved?** Which tables were inserted into, deleted from,
   altered, created, dropped, or had inlined writes between the cursor
   and `current_snapshot()`?
2. **Give me the rows.** For each affected table, return the per-row
   change events (insert / delete / update_preimage / update_postimage)
   in the requested snapshot range.

These are different problems with different cost shapes and different
DuckLake primitives, and the project's pre-extension reference
implementation (the prototype implementation) already arrived at the same
two-call answer empirically. ADR 0004 ratified the non-overlap
principle (every existing DuckLake function we audited is
`use-directly`); this ADR locks the *two specific functions* the read
path consumes and the discipline that follows.

The two questions admit different bad answers:

- A consumer that **only reads `table_changes` per known table** has to
  guess which tables are interesting on every poll. With no `tables`
  filter that's an `information_schema` walk per poll
  (per-backend-syntax, expensive). With a `tables` filter it works but
  then misses any newly-created table and any DDL events entirely.
- A consumer that **only reads `snapshots()` and parses
  `changes_made`** gets a flat per-snapshot summary but no row-level
  data. It would also have to re-implement inlined-data handling,
  encryption handling, partial-file handling, and post-compaction
  reads — all of which `table_changes` already does correctly.

The hybrid pattern is therefore not a "design choice" so much as the
only correct one: discover via `snapshots()`, drill via `table_changes`.
This ADR ratifies the pattern, locks the inlined-data discipline that
the prototype implementation already gets right, and makes the cursor-state
home explicit (the catalog, not a file).

The pattern is also pillar 2 of `docs/roadmap/README.md`
and shows up implicitly in every ADR downstream of this one (ADR 0006's
schema-bound is a `snapshots()` walk; ADR 0007's lease lives in the
catalog state; ADR 0008's `cdc_ddl` reads `snapshots().changes` for
stage 1 and DuckLake's catalog tables for stage 2). This ADR is the
canonical statement of the rule that the others depend on.

## Decision

### Discovery: `<lake>.snapshots()` filtered by the cursor window

For each poll cycle, the discovery query is one `SELECT` against the
DuckLake `snapshots()` table function:

```sql
SELECT snapshot_id,
       snapshot_time,
       schema_version,
       changes,         -- the typed MAP, per ADR 0008
       author,
       commit_message,
       commit_extra_info
FROM   <lake>.snapshots()
WHERE  snapshot_id >  :last_committed_snapshot
  AND  snapshot_id <= :end_snapshot
ORDER BY snapshot_id;
```

`:end_snapshot` is the bound chosen by `cdc_window`'s schema-version
logic (ADR 0006) and `max_snapshots` cap (ADR 0011). Discovery
**never** parses
`__ducklake_metadata_<lake>.ducklake_snapshot_changes.changes_made`
ourselves — the typed `MAP` returned by `snapshots().changes` is the
higher-fidelity surface (it carries the `inlined_insert` /
`inlined_delete` keys; the text `changes_made` does not — see ADR 0008
§ "Two empirical findings").

Stage-1 discovery in `cdc_ddl` consumes the same `changes` MAP. Sugar
wrappers `cdc_events` and `cdc_recent_changes` consume the same query
shape with light filtering on top. One discovery query, many surface
consumers.

### Read: `table_changes` / `table_insertions` / `table_deletions`

For each table the discovery step flagged as affected, the row-level
read goes through DuckLake's own table function:

```sql
SELECT *  -- snapshot_id, change_type, rowid, *original columns
FROM   table_changes('<schema>.<table>',
                     :start_snapshot,
                     :end_snapshot)
ORDER BY snapshot_id, rowid;
```

`table_changes` **transparently** handles:

- **Inlined data** — small batches stored in `ducklake_inlined_data_*`
  tables instead of Parquet. `table_changes` reads from both backing
  stores and presents them under one shape.
- **Encryption** — DuckLake's encryption-at-rest is handled inside
  `table_changes`; the consumer never touches a key.
- **Partial files** — partial-write recovery, partial-Parquet reads
  during compaction transitions. Bounded by `table_changes`.
- **Post-compaction reads** — when `ducklake_merge_adjacent_files`
  rewrites underlying Parquet, the snapshot range still reads
  correctly.
- **Schema-evolution-on-read** — fields added between `:start_snapshot`
  and `:end_snapshot` come back with the default value substituted;
  fields dropped come back missing. ADR 0006's schema-version bound
  is what makes this correct (the consumer never reads across a
  schema boundary, so the as-of-end-snapshot reconciliation is always
  a no-op for the data the consumer actually sees).

`table_insertions` and `table_deletions` are specialised forms of
`table_changes` (inserts only, deletes only). Available to users; the
extension's sugar wrappers prefer `table_changes` because the change
discrimination is per-row and we want to show the consumer that path.

### Cursor and consumer state: regular DuckLake table in the catalog

The consumer's cursor and configuration live in
`__ducklake_cdc_consumers` (ADR 0009, the canonical schema). The
audit log lives in `__ducklake_cdc_audit` (ADR 0010). Both are
**regular DuckLake tables** in the attached catalog. Consequences:

- **Transactional with the read.** `cdc_commit` is a single UPDATE
  inside a transaction; the cursor advance is committed in the same
  catalog transaction whose isolation level guarantees the read
  cannot drift from the cursor.
- **No external state store.** No Redis, no etcd, no file. Pillar 3
  of `docs/roadmap/README.md` rules this out
  affirmatively; this ADR ratifies it.
- **Conflict resolution between concurrent consumers is inherited.**
  Two consumers writing to `__ducklake_cdc_consumers` are arbitrated
  by DuckLake's snapshot-id mechanism (pillar 1) — the same mechanism
  that arbitrates concurrent writes to user data tables. The lease
  layer (ADR 0007) sits on top of that.
- **Backend-portable.** A regular DuckLake table works on every
  catalog backend DuckLake supports (DuckDB, SQLite, Postgres). The
  Phase 2 catalog matrix re-runs every test against every backend;
  the cursor mechanism does not need per-backend code.

The reference implementation the prototype implementation keeps the cursor
in a file (`/tmp/.dlcdc_cursor` written via atomic rename). That works
for one process polling one lake; it does not scale to multi-consumer
or multi-process and gives up the "transactional with the read"
guarantee. The catalog table is the obsoletion target — this is what
ADR 0004 labels the file-cursor as `drop`.

### Inlined-data discipline (the easy thing to forget)

DuckLake stores small write batches in catalog tables
(`ducklake_inlined_data_*`) instead of Parquet. The
`snapshots().changes` MAP has **distinct keys** for inlined writes:

- `inlined_insert: [<table_id>, ...]` for inlined inserts
- `inlined_delete: [<table_id>, ...]` for inlined delete tombstones

These are **in addition to** `tables_inserted_into` /
`tables_deleted_from`. **Discovery must look at all four keys.** A
snapshot with only inlined writes that is not inspected for
`inlined_insert` / `inlined_delete` is silently skipped — the consumer
advances its cursor past data it never read.

the prototype change-key probe already gets this right
(it's hard-coded to the four keys observed in the wild). The Phase 0
upstream probe (`test/upstream/enumerate_changes_map.py`) replaced the hard-code with
an empirically-verified, version-stamped key set; ADR 0008 promotes it
to a recurring CI gate. **This ADR is the rule the gate enforces**:
discovery considers the union of `{tables_inserted_into,
tables_deleted_from, inlined_insert, inlined_delete}` for DML
attribution — never a subset.

The inline-on-inline edge case (a delete targeting an inlined-insert
row, recorded by setting `end_snapshot` on the inlined row instead of
emitting an `inlined_delete`) is documented in
`docs/operational/inlined-data-edge-cases.md` (Phase 0 deferred to
Phase 5 per the phase doc): `cdc_events` may show "empty" snapshots
that nonetheless yield rows from `cdc_changes`. Consumers must not
write logic that assumes "empty `cdc_events` → no work".

### What this ADR does NOT decide

- **Per-snapshot ordering of DDL vs DML events.** That's ADR 0008's
  contract (DDL before DML within a snapshot).
- **Schema-version boundary placement of `cdc_window` results.**
  That's ADR 0006.
- **Single-reader enforcement of the cursor.** That's ADR 0007.
- **The MAP key set itself** (which keys exist, what they mean).
  That's ADR 0008 + the upstream probe output committed in
  `docs/api.md#snapshots-changes-map-reference`.
- **The `details` JSON shapes returned by `cdc_ddl`.** Also ADR 0008.

This ADR is the architectural ratification that the read path is
two-call hybrid, that the cursor lives in the catalog, and that the
inlined-data keys are non-optional. Everything else is downstream.

## Consequences

- **Phase impact.**
  - **Phase 1** ships `cdc_window` reading `snapshots()` for the
    discovery side and `cdc_changes` (sugar) reading `table_changes`
    for the read side. No code re-implements either DuckLake function.
    Tests verify the inlined-data discipline by exercising at least
    one snapshot with only inlined writes and asserting it is not
    skipped.
  - **Phase 2** (catalog matrix) re-runs the read-path tests against
    SQLite and Postgres backends. Because the read path goes through
    DuckLake table functions and not through backend-specific SQL,
    the test matrix has minimal per-backend code (only the initial
    `ATTACH ... (TYPE ducklake, ...)` differs).
  - **Phase 3 / Phase 4 bindings** consume the same SQL surface;
    there is no binding-side re-implementation of the read path.
- **Reversibility.**
  - **One-way doors:** the rule "discovery via `snapshots()`, read
    via `table_changes`" (changing this would force every binding to
    re-implement the inlined / encrypted / partial-file path);
    the rule "cursor lives in the catalog" (changing this breaks
    pillar 3 and every operational doc).
  - **Two-way doors:** the specific MAP keys discovery consults
    (additive — the upstream probe's recurring CI gate detects new keys and we
    add them); the choice between `table_changes` vs the more-specific
    `table_insertions` / `table_deletions` for any given internal
    use (currently `table_changes` for all; could specialise without
    user-visible change).
- **Open questions deferred.**
  - **Streaming `table_changes` reads.** Currently
    `table_changes` returns a query result; the consumer materialises
    it. For large windows on cold S3 data, a streaming variant
    (`table_changes` returning a `STREAM` table function with
    backpressure) would be a v1.0+ optimisation. Out of v0.1 scope;
    `max_snapshots` cap (ADR 0011) bounds the worst case for now.
  - **Push-based discovery.** `snapshots()` is a poll. The upstream
    notification-hook ask (`docs/upstream-asks.md`, Phase 5) is the
    long-term fix. Out of v0.1 scope. ADR 0011's latency design
    target reflects this honestly.
  - **Discovery batching across consumers.** `LakeWatcher` (Phase 3 /
    4 in-process amortisation) is the v0.1 answer; cross-process is
    deferred per ADR 0011.

## Alternatives considered

- **Read-only via `table_changes` per known table; skip discovery.**
  Rejected: requires the consumer to know its table set ahead of
  time (no DDL discovery, no new-table awareness), and a poll cycle
  with no candidate tables to read still costs N table-function
  invocations to "check for changes" — vs the one `snapshots()` query
  we'd otherwise do. The discovery cost is amortised across all
  affected tables.
- **Discovery only via `snapshots()`; parse `changes_made` for the
  text representation.** Rejected: the text key is missing inlined
  variants entirely (ADR 0008 § "Findings worth flagging"), and
  parsing `key:value` strings out of comma-joined text is fragile
  (table names with quotes, schema qualifiers, `,` in identifiers).
  The typed MAP is unambiguous; the text key is for human reading.
- **Cursor in an external state store (Redis / etcd / Postgres
  outside the lake catalog).** Rejected for pillar-3 reasons:
  introduces operational burden the project is explicitly positioned
  away from (Debezium-shaped problems), breaks the "transactional
  with the read" guarantee, and makes multi-backend support a
  per-backend integration story rather than a SQL story.
- **Cursor in a file** (the prototype implementation baseline).
  Rejected: ADR 0004 labels this `drop`. Single-process; loses
  transactionality; can't survive lease-protected reads.
- **Read inlined data through a separate primitive
  (`cdc_inlined_changes(table, start, end)`).** Rejected:
  `table_changes` already reads from both backing stores transparently
  (DuckLake spec, "Deletion Inlining" + "Inlined Data" sections).
  Adding a separate primitive would shadow `table_changes` (pillar 1
  violation) and create an inconsistency surface where users have to
  remember which primitive to call when.
- **Cache `current_snapshot()` per process to amortise discovery.**
  Considered as a v0.1 optimisation. Rejected for v0.1 in favour of
  the in-process `LakeWatcher` amortisation in Phase 3 / 4. Adding
  caching to the extension would either be incorrect (consumers
  expect a fresh `current_snapshot()`) or require invalidation
  primitives that don't exist. The library-side amortisation is the
  right layer.

## References

- `docs/roadmap/README.md` — pillar 2 (discovery via
  `snapshots()`, reads via `table_changes`); pillar 3 (cursors and
  consumer state in the catalog).
- `docs/roadmap/` — the long-form discussion this ADR formalises.
- ADR 0004 — Overlap audit (lists `snapshots()`, `current_snapshot()`,
  `table_changes`, `table_insertions`, `table_deletions` as
  `use-directly`; lists the file-cursor as `drop`; lists the
  inlined-data hard-code as `keep` (as the upstream probe output)).
- ADR 0006 — Schema-version boundaries (the bound that limits
  `:end_snapshot` in the discovery query).
- ADR 0007 — Concurrency model (the lease that protects the
  cursor-in-catalog write).
- ADR 0008 — DDL as first-class events (the typed-extraction
  primitive that consumes the discovery `changes` MAP).
- ADR 0009 — Consumer-state schema (the canonical cursor / config
  shape for `__ducklake_cdc_consumers`).
- ADR 0011 — Performance model (the `max_snapshots` cap on
  `:end_snapshot`).
- DuckLake spec — Snapshots, Snapshot Changes, Inlined Data,
  Field Identifiers, Conflict Resolution.
- the prototype implementation — the reference implementation that
  arrived at the hybrid pattern empirically; lines 244-255 are the
  inlined-data discipline this ADR canonicalises.
- `test/upstream/enumerate_changes_map.py` /
  `test/upstream/output/snapshots_changes_map_reference.md` — the empirical
  enumeration of the MAP keys discovery consults.
