# ADR 0006 — Schema-version boundaries: proactive yield-before

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

DuckLake's `table_changes(t, A, B)` reads using the schema as-of the
**end** of the requested window (snapshot `B`). Per the spec ("Field
Identifiers"):

- A column dropped between A and B causes historical insert events from
  before the drop to come back **without** that column.
- A column added between A and B causes historical events to come back
  **with** the new column's default substituted.
- A column type-promoted between A and B causes historical events to
  come back cast to the new type.

This is the right behaviour for read-time backfills (the rows you read
match the schema you'd see if you re-ran the historical query today),
but it produces a subtle correctness bug for change-data consumers: if
the consumer "reads data via `table_changes`, then applies the DDL diff
to its sink," the data it just read is already reconciled to the new
schema, but its sink hasn't seen the DDL yet. Result: the sink writes
rows under the wrong shape, then receives the DDL after the fact, then
either silently discards the now-mismatched rows (Postgres-mirror with
strict mode) or accumulates a per-event reconciliation cost forever
(Kafka with schema registry).

The reverse order — apply the DDL diff first, then read data — is
correct: the sink's schema matches the data's schema by the time the
data lands. The only way to guarantee that order is for the consumer
to **never** read a window that crosses a schema-version boundary.

DuckLake exposes the per-table schema-version history in the
`ducklake_schema_versions` catalog table:
`(begin_snapshot, schema_version, table_id)`. Every schema change to
table T appends one row. The global `ducklake_snapshot.schema_version`
advances whenever **any** table or schema in the lake changes, but the
per-table history lets a consumer with a table filter ignore schema
changes to unrelated tables. The bounding logic must respect this:
otherwise a consumer subscribed only to `orders` would yield-and-restart
on every `customers` ALTER, which is correct-but-pointless and
imposes per-DDL pause latency on every consumer in the lake.

`stop_at_schema_change BOOLEAN NOT NULL DEFAULT TRUE` is the per-consumer
opt-out. ADR 0009 owns the column; this ADR locks the *behaviour* under
both settings.

## Decision

### Default behaviour (`stop_at_schema_change = TRUE`)

`cdc_window` **never returns a window that crosses a `schema_version`
boundary** for any table the consumer is subscribed to. The window
result includes:

- `schema_version BIGINT` — the schema version active throughout the
  window (single value; not nullable on a non-empty window).
- `schema_changes_pending BOOLEAN` — TRUE iff the very next snapshot
  the consumer would read is at a different `schema_version`. (TRUE on
  the last snapshot before a boundary; FALSE elsewhere, including on
  the snapshot **at** the boundary itself.)

The contract for one consumer batch with the default setting:

1. Consumer calls `cdc_window` and gets
   `(start, end, has_changes, schema_version, schema_changes_pending)`.
   All snapshots in `[start, end]` share `schema_version`. If
   `has_changes` is FALSE, the window is empty and the cursor is at
   the latest committed snapshot — there is nothing to do.
2. Consumer reads DML via `cdc_changes` (per ADR 0008's
   sugar-wrapper composition over `table_changes`) and DDL via
   `cdc_ddl`. Per ADR 0008's per-snapshot ordering rule, DDL events
   are delivered before DML events within each snapshot. **In a
   default-setting window, all DDL events have `event_kind ∈ {created,
   altered, dropped}` only when those events did not change the
   `schema_version` for any subscribed table** — concretely, table
   creates and drops outside the consumer's filter, and column-level
   creates/drops/renames within the same `schema_version`-tick that
   produced this window's bound. If the window covers `schema_version
   = N` exclusively, the only DDL events present are events whose
   schema-version effect is contained within `N`.
3. Consumer calls `cdc_commit(end)`.
4. If `schema_changes_pending` was TRUE: the next `cdc_window` call
   returns a window starting at the schema-change snapshot. The DDL
   events at that snapshot (which produced the new `schema_version`)
   are the **first events the consumer sees**. The contract is
   "yield-before, deliver-the-DDL-first-on-resume."
5. If `schema_changes_pending` was FALSE and `has_changes` was TRUE:
   the cursor advanced through one or more snapshots without crossing
   a schema boundary. The next `cdc_window` may return more data, an
   empty window, or hit a boundary later — no special handling.

`CDC_SCHEMA_BOUNDARY` is emitted via DuckDB `Notice` (not an error)
when a `cdc_window` call returns `schema_changes_pending = TRUE`. This
gives operators a logged signal that a schema change is pending without
disrupting the contract; the message includes the affected
`schema_version` transition and is locked in `docs/errors.md`.

### Opt-out (`stop_at_schema_change = FALSE`)

Consumers created with `stop_at_schema_change := FALSE` can have windows
that cross schema boundaries. Use cases:

- **High-throughput sinks that don't care about column-level shape.**
  Stdout JSONL, blob-passthrough audit, write-everything-to-S3-as-Parquet
  — they read whatever DuckLake hands back without column-name validation.
- **Consumers that handle their own schema reconciliation.** The user
  maintains an in-process schema registry, reconciles inside their
  `Sink.apply()` callback, and explicitly does not want the extension
  to bound their windows.

The result row still carries `schema_version` (the value at `end_snapshot`)
and `schema_changes_pending` (the value at `end_snapshot + 1`). Both are
informational under the opt-out; the consumer chooses whether to do
anything with them.

Documented as a power-user knob; the safe default is on. The opt-out
is recorded as a column on the consumer row (ADR 0009), not as a
session setting, so a long-running daemon cannot accidentally reset
the consumer's behaviour by reconnecting.

### Why proactive (yield-before) and not reactive (yield-after)

Reactive ("read the snapshot, then yield") has the consumer's `Sink`
receive rows whose shape was already reconciled to the new schema, then
asks the consumer to apply the DDL diff after the fact. The sink writes
mis-shaped rows in the window between "rows arrived under new shape"
and "DDL was applied to sink." This produces:

- **Postgres mirror, strict mode:** rows rejected because `extra_col`
  doesn't exist in the target. Hours of operator time recovering from
  the rejected DLQ.
- **Postgres mirror, lenient mode:** silently dropped columns. Worse:
  data loss without a logged signal.
- **Kafka with schema registry:** schema-evolution writes from the
  sink's perspective, with the schema registered *after* messages
  carrying the new shape. Some Kafka consumers refuse to deserialise
  the messages until they refresh the registry; some accept and
  silently apply defaults; the behaviour depends on the consumer
  library and the operator does not get to choose.
- **Search index hydration:** docs indexed under the old schema,
  reranking model trained on the new schema, query-time mismatch.

Proactive yield bounds the window so all reads happen under one
consistent schema and the DDL arrives at the *start* of the next
window. The cost is one extra `cdc_window` round-trip per schema
change — in practice, far below the latency budget of the workloads
this project targets (the performance principles in `docs/performance.md`:
sub-second to seconds, not Debezium-comparable milliseconds).

### Per-table awareness

`ducklake_schema_versions` is a per-table table:
`(begin_snapshot, schema_version, table_id)`. `ducklake_snapshot.schema_version`
is global and advances whenever *any* table or schema in the lake
changes. The bounding logic respects this distinction:

- For a consumer with **no table filter** (`tables IS NULL`,
  subscribes to the whole lake), `cdc_window` bounds at any
  `ducklake_snapshot.schema_version` change. Conservative; correct.
  This is what the prototype implementation
  would have to do (it has no per-table awareness).
- For a consumer with a **table filter** (e.g.
  `tables := ['main.orders']`), `cdc_window` bounds only at snapshots
  whose `ducklake_schema_versions.table_id` intersects the filter set.
  A schema change to an unrelated `customers` table does **not**
  trigger a boundary. This matches the workload pattern from
  the performance principles in `docs/performance.md` point 2: cost is
  bounded by the consumer's interest, not the lake's total schema
  churn.

The per-table-aware lookup is one JOIN on small tables.
`ducklake_schema_versions` carries a handful of rows per table over a
lake's lifetime (one row per `(table_id, schema_version)` change), so
the JOIN cost is trivial even on large lakes.

### The bounding query (canonical form)

Phase 1 ships this byte-for-byte modulo three backend-dialect items
that diverge across DuckDB / SQLite / Postgres:

1. **Parameter markers.** `?` (DuckDB / SQLite), `$1 / $2 / …`
   (Postgres). Mechanical at the binding layer.
2. **`UNNEST(:tables)` over an array parameter.** DuckDB and Postgres
   accept `UNNEST(?)` over an array literal directly. SQLite has no
   array type — Phase 2's SQLite implementation receives `tables` as a
   `JSON` text and substitutes
   `(SELECT value FROM json_each(:tables))` for the `UNNEST(:tables)`
   subquery; ADR 0009 already lowers `VARCHAR[]` to `TEXT (JSON
   array)` on SQLite, so the binding is consistent. Phase 2 work item.
3. **`make_interval`** is not used in this query (only in ADR 0007's
   lease predicate); listed here for cross-reference because the same
   per-backend lowering applies whenever a snapshot-window query needs
   one.

Drift between this query's *predicate* and the implementation
re-opens the cross-boundary-window class of bugs. Drift in the
per-backend dialect is mechanical and tested by Phase 2's catalog
matrix.

```sql
-- Inputs:
--   :consumer_name   the consumer name
--   :tables          NULL or the consumer.tables array
--   :start_excl      consumer.last_committed_snapshot (exclusive lower bound)
--   :current_top     current_snapshot() (inclusive upper bound, before max_snapshots cap)
-- Output:
--   end_snapshot:                the schema-bound upper bound for the window
--   schema_version:              the single schema_version active across the window
--   schema_changes_pending:      TRUE iff a relevant schema change exists at end_snapshot+1

WITH relevant_table_ids AS (
    -- The set of table_ids the consumer cares about. If `tables` is NULL,
    -- the consumer subscribes to every table in the catalog at the current
    -- snapshot; otherwise we resolve the qualified names through ducklake_table.
    SELECT t.table_id
    FROM   ducklake_table t
    JOIN   ducklake_schema s ON s.schema_id = t.schema_id
    WHERE  t.end_snapshot IS NULL
      AND  (
                :tables IS NULL
             OR (s.schema_name || '.' || t.table_name) IN (SELECT UNNEST(:tables))
           )
),
relevant_boundaries AS (
    -- For consumers with a table filter, bound only on schema-version
    -- changes affecting subscribed tables. For unfiltered consumers,
    -- bound on the global schema_version (every advance is relevant).
    SELECT begin_snapshot AS boundary_snapshot
    FROM   ducklake_schema_versions
    WHERE  begin_snapshot > :start_excl
      AND  begin_snapshot <= :current_top
      AND  (
                :tables IS NULL
             OR table_id IN (SELECT table_id FROM relevant_table_ids)
           )
)
SELECT
    -- The schema-bound upper bound: the snapshot just before the next
    -- relevant boundary, or :current_top if there is no boundary in range.
    COALESCE(MIN(boundary_snapshot) - 1, :current_top) AS end_snapshot,
    -- The schema_version active across the window: read from
    -- ducklake_snapshot at the chosen end_snapshot.
    (SELECT schema_version
       FROM ducklake_snapshot
      WHERE snapshot_id = COALESCE(MIN(boundary_snapshot) - 1, :current_top)
    ) AS schema_version,
    -- schema_changes_pending: TRUE iff there is at least one relevant
    -- boundary at-or-before :current_top that we excluded from the window.
    (MIN(boundary_snapshot) IS NOT NULL) AS schema_changes_pending
FROM relevant_boundaries;
```

The query always produces **exactly one row** — including when
`relevant_boundaries` is empty. `SELECT MIN(...) FROM empty_cte`
returns one row whose aggregates are NULL, and the
`COALESCE(MIN(boundary_snapshot) - 1, :current_top)` collapses the
empty case to `:current_top` for `end_snapshot`; the
`schema_version` subquery then reads `ducklake_snapshot.schema_version`
at `:current_top`; and `MIN(boundary_snapshot) IS NOT NULL` resolves
to FALSE, so `schema_changes_pending` is FALSE in the empty case as
intended. The calling code does **not** need substitution logic; the
query handles the empty-boundaries case inline.

This query runs **inside the same transaction** as the lease UPDATE
(ADR 0007), so a parallel `ducklake_expire_snapshots` cannot interleave
between the boundary scan and the existence check. The combined
transaction is the TOCTOU defense.

### Behaviour when `stop_at_schema_change = FALSE`

The query above is skipped entirely. `end_snapshot = :current_top`
(modulo the `max_snapshots` cap from ADR 0011). `schema_version` is
read from `ducklake_snapshot` at `end_snapshot`. `schema_changes_pending`
is computed as a simple existence check on `ducklake_schema_versions`
filtered by `relevant_table_ids` — TRUE iff at least one relevant
boundary exists in `(start_excl, current_top]`, FALSE otherwise. The
opt-out consumer gets the same informational signal but no bounding.

### `last_committed_schema_version` upkeep

ADR 0009 commits `last_committed_schema_version BIGINT` on the consumer
row. `cdc_commit` refreshes it from
`ducklake_snapshot.schema_version` at the committed snapshot id (per
ADR 0007's `cdc_commit` UPDATE). The cached value lets future
`cdc_window` calls compare cheaply against `current_snapshot()`'s
schema_version without re-reading the row. The cached value is **never**
used as the schema_version returned to the user — the returned
`schema_version` always comes from a fresh `ducklake_snapshot` lookup
inside the lease transaction.

## Consequences

- **Phase impact.**
  - **Phase 1** ships the bounding query above as part of `cdc_window`,
    plus the `CDC_SCHEMA_BOUNDARY` notice format
    (`docs/errors.md`), plus the schema-change tests enumerated in
    `docs/roadmap/` (schema change
    mid-window with default `stop_at_schema_change=TRUE`; same workload
    with the opt-out; per-table-aware bounding asserted with two
    consumers, one filtered to `orders` and one to the whole lake,
    against a schema change to `customers`).
  - **Phase 2** (catalog matrix) re-runs the bounding-query tests
    against SQLite and Postgres backends. The expected outcome is
    "identical bounding" — `ducklake_schema_versions` is a regular
    DuckLake catalog table whose contents are backend-independent.
  - **Phase 3 / Phase 4 bindings** surface
    `schema_changes_pending` to user code as a typed field; no client-side
    behaviour changes (the contract is fully extension-side).
- **Reversibility.**
  - **One-way doors:** the contract that `cdc_window` "never returns a
    window that crosses a `schema_version` boundary" under the default
    setting (loosening it would silently put pre-existing consumers
    back in the reactive-yield-after failure mode); the
    "DDL-first-on-resume" delivery on the boundary snapshot
    (consumers may have hard-coded "the first event after a
    `schema_changes_pending=TRUE` window is a DDL event").
  - **Two-way doors:** the per-table-aware bounding (could be relaxed
    to global bounding in a patch release for unfiltered consumers,
    though there is no reason to). The `CDC_SCHEMA_BOUNDARY` notice
    (could be promoted to an error in a major version bump if user
    feedback indicates operators want the boundary as a hard signal).
- **Open questions deferred.**
  - **Multi-statement DDL with mixed table targets in one snapshot.**
    `ALTER TABLE orders ADD COLUMN x; ALTER TABLE customers ADD COLUMN
    y;` in one transaction produces one snapshot with one
    `ducklake_snapshot.schema_version` advance, but two
    `ducklake_schema_versions` rows (one per table_id). The bounding
    query above handles this correctly (the per-table-filter consumer
    bounds on the relevant table_id; the unfiltered consumer bounds on
    the global advance). No additional logic required, but documented
    as a worked example in `docs/decisions/0006-schema-version-boundaries.md`.
  - **Schema changes that DuckLake records as no-op for a particular
    consumer.** The current spec does not have schema changes that
    DuckLake records and then revokes; if it ever does, the
    `schema_changes_pending = TRUE → next window starts at the
    boundary` contract may yield empty windows that were unnecessary.
    Acceptable cost; not a correctness issue.
  - **Per-row-level schema reconciliation hooks.** The
    `Sink.apply_ddl_then_dml` interface in Phase 3 / Phase 4 is the
    deliberate-by-design interface; an extension-side
    "auto-apply DDL to a registered sink" hook is out of v0.1 scope
    (and arguably out of v1.0 scope — sink-side reconciliation is the
    sink's concern, not the extension's).

## Alternatives considered

- **Reactive (yield-after) with a `Sink.apply_ddl(diff)` callback.**
  Rejected for the silent-data-corruption reasons in Decision § "Why
  proactive..." above. Even with the callback, the rows in the
  current batch have already been reconciled to the new schema by
  `table_changes`, so the sink has to choose between dropping them
  (data loss) or trying to un-reconcile them (impossible — DuckLake
  doesn't preserve pre-drop column data on the read path; ADR 0008
  documents this as the unfixable column-drop edge case).
- **Bound at every `ducklake_snapshot.schema_version` advance for all
  consumers (no per-table awareness).** Rejected: causes consumers
  scoped to one table to yield-and-restart on every unrelated DDL in
  the lake. In a multi-team lake (the workload pattern reverse-ETL and
  schema-as-code consumers target), this means an `orders` consumer
  pauses and resumes on every `customers` ALTER. The per-table-aware
  bounding is one additional JOIN against a small table; the cost is
  trivial and the operational difference is the difference between a
  feature operators love and a feature operators turn off.
- **Per-snapshot schema_version checks (consumer reads each snapshot
  individually and bails when `schema_version` changes).** Rejected:
  inverts the contract surface (consumers who didn't care about
  schema versions now have to care because `cdc_window` returns
  one-snapshot windows); doesn't compose with `max_snapshots`; loses
  the batching benefit of multi-snapshot windows on quiescent schemas.
- **`schema_change_strategy` enum (`bound | warn | strict-fail`).**
  Rejected as scope creep for v0.1. The two settings (`TRUE` =
  proactive bound, `FALSE` = no bounding, raw informational signal)
  cover the user research. A third "fail loudly on any schema change"
  mode is an opinion that doesn't earn the surface complexity in v0.1
  — users who want it can detect `schema_changes_pending=TRUE` and
  raise their own exception. Revisit in v0.2 if there's demand.
- **Separate `cdc_window_for_ddl` and `cdc_window_for_dml` primitives.**
  Rejected: makes the contract harder to reason about ("which window
  is ahead?"), doubles the surface, and breaks the per-snapshot
  ordering contract from ADR 0008 (DDL-before-DML *within a snapshot*
  requires both kinds of events to come from the same window).

## References

- `docs/roadmap/` — the long-form discussion this ADR locks down,
  including the per-table-aware design note this ADR formalises in SQL.
- `docs/roadmap/`
  (the bounding query lands inside `cdc_window`'s implementation),
  § "Test matrix" (the schema-change test scenarios consume this ADR).
- `docs/roadmap/README.md` — pillar 8 (schema changes
  are window boundaries by default).
- ADR 0007 — Concurrency model + owner-token lease (the same
  transaction wraps both the lease UPDATE and this ADR's bounding
  query as the TOCTOU defense).
- ADR 0008 — DDL as first-class events (the DDL-first-on-resume
  contract delivers the DDL events that produced the new
  `schema_version` at the start of the next window).
- ADR 0009 — Consumer-state schema (defines `stop_at_schema_change`,
  `last_committed_schema_version`).
- ADR 0011 — Performance model (the `max_snapshots` cap composes with
  this ADR's `end_snapshot`).
- DuckLake spec — Field Identifiers, Snapshots, Schema Versions.
- the prototype implementation — the reference implementation has no
  per-table-aware bounding (it pre-dates the extension's typed schema
  awareness), demonstrating the failure mode this ADR avoids.
