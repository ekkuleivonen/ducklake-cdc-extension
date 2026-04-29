# Public API surface

> **Status: frozen at end of Phase 0.** This document is the binding
> public API surface for `ducklake_cdc`. Phase 1 implements against
> this document; deviations require a new ADR and a minor or major
> version bump per the rules below.

## How to read this document

Every entry on this page is classified as one of:

- **primitive** — first-class extension function, lives in C++ (per
  ADR 0001), has its own ADR. Adding a primitive is a minor version
  bump; renaming or removing one is a major version bump.
- **observability** — a primitive whose role is metrics /
  introspection rather than the read path. Same versioning rules as
  primitives.
- **acknowledged sugar** — documented as a one-line composition over
  primitives + DuckLake builtins. Sugar entries include the canonical
  composition so the SQL-helper-library escape hatch (ADR 0005) stays
  viable. Sugar wrappers may be added or refactored as minor bumps as
  long as the underlying composition is documented.

Identifier-quoting discipline (per `docs/security.md`) applies to
every primitive that accepts a `consumer_name`, `table`, or other
user-supplied identifier: the implementation binds these as parameters
or quotes them with the canonical `_q` pattern; never string-concats
into SQL.

Primitive signatures use DuckDB-style named-argument syntax with
`:=` defaults. Bindings (Phase 3 / 4) generate language-native
wrappers off these signatures.

## Primitives

### `cdc_consumer_create`

```sql
CALL cdc_consumer_create(
    catalog                    VARCHAR,
    name                       VARCHAR,
    tables                  := NULL,                    -- VARCHAR[]
    change_types            := NULL,                    -- VARCHAR[]
    event_categories        := NULL,                    -- VARCHAR[]
    stop_at_schema_change   := TRUE,                    -- BOOLEAN
    dml_blocked_by_failed_ddl := TRUE,                  -- BOOLEAN
    lease_interval_seconds  := 60,                      -- INTEGER
    start_at                := 'now',                   -- VARCHAR or BIGINT
    created_by              := NULL                     -- VARCHAR (override of session user)
);
```

Creates a row in `__ducklake_cdc_consumers` (canonical schema in
ADR 0009) and emits a `consumer_create` row to `__ducklake_cdc_audit`
(ADR 0010).

Phase-1 implementation status: the shipped form is

```sql
SELECT * FROM cdc_consumer_create(
    catalog            VARCHAR,
    name               VARCHAR,
    start_at         := 'now',
    tables           := NULL,           -- VARCHAR[]
    change_types     := NULL,           -- VARCHAR[]
    event_categories := NULL            -- VARCHAR[]
);
```

`catalog` is the attached DuckLake catalog name (`'lake'` for
`ATTACH ... AS lake`). The planned `<catalog>.cdc_consumer_create(...)`
qualified form is deferred until DuckDB exposes that dispatch hook for
non-DuckLake extension functions; today callers pass the catalog name
explicitly.

`tables`, `change_types`, and `event_categories` are validated at
create time (v0.0.7+):

- `tables`: every entry must be a fully-qualified `<schema>.<table>`
  that exists in the catalog at the resolved `start_at` snapshot;
  otherwise the call raises `CDC_INVALID_TABLE_FILTER` with the
  supplied list, the missing names, the snapshot id, and the actual
  table list at that snapshot.
- `change_types`: every entry must be one of
  `{insert, update_postimage, update_preimage, delete}`. Empty list is
  rejected (would deny everything; the caller almost certainly meant
  to omit the parameter).
- `event_categories`: every entry must be one of `{dml, ddl}`. Empty
  list is rejected for the same reason.

Filter values round-trip into the consumer row and surface on
`cdc_consumer_stats`. `cdc_changes` and `cdc_ddl` honor
`event_categories` post-window: a `['ddl']` consumer sees zero rows
from `cdc_changes`; a `['dml']` consumer sees zero rows from `cdc_ddl`.
The cursor still advances through every snapshot in the window
regardless of the filter — `event_categories` is a *projection*, not a
window-bounding decision. `lease_interval_seconds`, `created_by`, and
`metadata` are API-locked but implemented in follow-up slices when
bindings first consume them.

Implementation note for the validated DuckDB v1.5.1 DuckLake tuple:
regular DuckLake tables reject `PRIMARY KEY`, `UNIQUE`, default
expressions, indexes, and the JSON extension type in catalog-resident
user tables. The extension creates the full column set now, lowers JSON
payload columns to `VARCHAR`, and enforces duplicate consumer names in
`cdc_consumer_create` until the Phase 2 backend-matrix slice can
revisit physical constraints.

- `name` — primary key. Stable across `cdc_consumer_drop` + recreate
  iff the user reuses the name. SQL-injection-safe quoting required
  on every interpolation site (`docs/security.md`).
- `tables` — `NULL` (default) means whole-lake; otherwise a hard
  list of fully-qualified `schema.table` names. Globs and tag-based
  selection are out of scope for v0.1 (see "Non-goals" in
  `docs/roadmap/README.md`). Validated against the catalog as of the
  resolved `start_at` snapshot; raises `CDC_INVALID_TABLE_FILTER`
  on miss (`docs/errors.md`).
- `change_types` — `NULL` (default) means all four
  (`{insert, update_preimage, update_postimage, delete}`); otherwise
  a subset. DML-only filter; the extension never invents implicit
  DML filters (`docs/roadmap/README.md`).
- `event_categories` — `NULL` (default) means both `{ddl, dml}`;
  otherwise a subset. Top-level switch — see
  [Filter composition](#filter-composition).
- `stop_at_schema_change` — when `TRUE` (default), `cdc_window`
  never returns a window that crosses a `schema_version` boundary.
  See ADR 0006.
- `dml_blocked_by_failed_ddl` — when `TRUE` (default), a
  permanently-failed DDL apply in the DLQ halts the consumer's DML
  for that table until operator resolution. Setting `FALSE` opts
  into "advance past failed DDL"; rare, and unsafe without the
  Phase 2 `max_dml_dlq_per_consumer` cap (which lands alongside
  this opt-out per ADR 0011).
- `lease_interval_seconds` — per-consumer override of the
  owner-token lease timeout (ADR 0007). Default 60. Bindings drive
  heartbeat cadence at `lease_interval_seconds / 3`.
- `start_at` — initial cursor position. Resolved at create time:
  - `'now'` (default) → `current_snapshot()` at the create
    transaction.
  - `'beginning'` / `'oldest'` → `MIN(snapshot_id) FROM ducklake_snapshot`.
  - `<bigint>` → used verbatim; must exist in `ducklake_snapshot`
    or the call raises.
  - `<timestamp>` is deferred; the current implementation raises
    `CDC_FEATURE_NOT_YET_IMPLEMENTED` for timestamp-shaped strings.
  The resolved value is what's written to
  `last_committed_snapshot`; the original literal is preserved on
  the audit row so operators can answer "did the user say `'now'`
  or pin a specific snapshot?" later.
- `created_by` — `NULL` defaults to `current_user` /
  `current_role` if the backend exposes it; bindings may override
  for service-account attribution.

ADR(s): 0009 (column ownership), 0008 (filter composition + DDL
filter rules).

### `cdc_consumer_drop`

```sql
CALL cdc_consumer_drop(
    catalog VARCHAR,
    name    VARCHAR
);
```

Hard `DELETE` from `__ducklake_cdc_consumers`. Emits a
`consumer_drop` audit row carrying the consumer's `consumer_id` (so
post-drop audit / DLQ rows still resolve back to a recognisable
identifier per ADR 0010).

`catalog` is the attached DuckLake catalog name (`'lake'` for
`ATTACH ... AS lake`). The planned `<catalog>.cdc_consumer_drop(...)`
qualified form is deferred with the other qualified-call sugar.

No soft-delete tombstone in v0.1; the audit log is the system of
record for "this consumer ever existed" (ADR 0009 § "Open questions
deferred").

### `cdc_consumer_list`

```sql
SELECT * FROM cdc_consumer_list(catalog VARCHAR);
```

Table function. Returns one row per consumer with every column from
`__ducklake_cdc_consumers` (ADR 0009), ordered by `consumer_id ASC`.
The Phase-1 foundation slice does **not** yet add the derived
`tables_unresolved VARCHAR[]` column; that lands with the table-filter
validation/statistics slice that first has to resolve table filters at
runtime.

### `cdc_consumer_reset`

```sql
CALL cdc_consumer_reset(
    catalog      VARCHAR,
    name         VARCHAR,
    to_snapshot  := NULL    -- 'oldest_available' | 'now' | <bigint>
);
```

Repositions the cursor. Emits `consumer_reset` to the audit table.
Recovery target accepted in either of three forms:

- `NULL` / omitted / `'oldest_available'` — `MIN(snapshot_id) FROM
  ducklake_snapshot` at the moment of the call. The `CDC_GAP` recovery
  message (`docs/errors.md`) recommends this string verbatim.
- `'beginning'` / `'oldest'` — aliases for `'oldest_available'`.
- `'now'` — `current_snapshot()` at the moment of the call.
- `<bigint>` — explicit snapshot id; must exist in
  `ducklake_snapshot`.

The function clears the owner-token lease columns as part of the reset,
refreshes `updated_at`, and emits a `consumer_reset` audit row carrying
the old and new cursor. `catalog` is explicit for the same reason as
`cdc_consumer_create`: qualified-call sugar is deferred.

### `cdc_consumer_heartbeat`

```sql
SELECT * FROM cdc_consumer_heartbeat(
    catalog VARCHAR,
    name    VARCHAR
);
```

Single `UPDATE __ducklake_cdc_consumers SET owner_heartbeat_at =
now() WHERE owner_token = :token` (where `:token` is the calling
connection's session-cached lease token). Used by long-running
orchestrators that read for many minutes between commits; library
`tail()` / `Tail()` helpers heartbeat automatically.

Raises `CDC_BUSY` if the calling connection's cached token no longer
matches the row's `owner_token` (lease lost via timeout or
`cdc_consumer_force_release`).

Phase-1 implementation status: the shipped call form is
`cdc_consumer_heartbeat(catalog, consumer)`, with explicit `catalog`
for the same qualified-call limitation documented on
`cdc_consumer_create`. It returns one row with
`heartbeat_extended BOOLEAN`, currently always `TRUE` on success; lost
or missing leases raise `CDC_BUSY`.

ADR: 0007.

### `cdc_consumer_force_release`

```sql
CALL cdc_consumer_force_release(
    catalog VARCHAR,
    name    VARCHAR
);
```

Operator escape hatch when the lease holder is known dead. Single
`UPDATE __ducklake_cdc_consumers SET owner_token = NULL,
owner_acquired_at = NULL, owner_heartbeat_at = NULL WHERE
consumer_name = :name`. Emits `consumer_force_release` to the audit
table with the previous holder's token, acquisition time, and heartbeat
time in `details` (ADR 0010). Returns
`(consumer_name, consumer_id, previous_token)`.

`catalog` is explicit until qualified-call sugar is solved for this
extension.

The recommended diagnostic ladder before force-releasing is in the
`CDC_BUSY` reference message (`docs/errors.md`): wait for the
holder to release; if `now() - owner_heartbeat_at > 2 ×
lease_interval_seconds`, the holder is likely dead and force-release
is appropriate.

### `cdc_window`

```sql
SELECT * FROM cdc_window(
    catalog        VARCHAR,
    consumer       VARCHAR,
    max_snapshots  := 100   -- BIGINT, hard-capped (see ADR 0011)
);
```

Returns a single row:

| column | type | notes |
| --- | --- | --- |
| `start_snapshot` | `BIGINT` | inclusive lower bound for the next external DuckLake snapshot after the cursor |
| `end_snapshot` | `BIGINT` | inclusive upper bound, computed per ADR 0006 (schema-bounded) and ADR 0011 (`max_snapshots`-bounded); may be `< start_snapshot` for an empty or yield-before-schema-change window |
| `has_changes` | `BOOLEAN` | `FALSE` iff there are no external user changes in the returned range |
| `schema_version` | `BIGINT` | the schema version active throughout the window (single value when `has_changes`) |
| `schema_changes_pending` | `BOOLEAN` | `TRUE` iff the next snapshot the consumer would read is at a different `schema_version` |

Phase-1 implementation status: the shipped call form is
`cdc_window(catalog, consumer, max_snapshots := 100)`, with explicit
`catalog` for the same qualified-call limitation documented on
`cdc_consumer_create`. The foundation slice implements owner-token
lease acquisition, same-connection idempotence, `CDC_BUSY`,
`CDC_MAX_SNAPSHOTS_EXCEEDED`, and global schema-bound windows for
whole-lake consumers. Because the CDC state tables live in DuckLake,
the implementation filters snapshots whose `changes_made` only touch
`__ducklake_cdc_*` internal tables so lease/audit writes do not appear
as user data windows.

Side effects (single transaction):

1. **Lease acquisition** — runs the ADR 0007 conditional lease
   predicate adapted for the validated DuckDB v1.5.1 DuckLake tuple,
   which does not support `UPDATE ... RETURNING` on DuckLake tables.
   The acquire/extend logic stays in one transaction, raises
   `CDC_BUSY` on lease conflict, and caches the acquired `owner_token`
   on the connection for idempotent repeat calls.
2. **Cursor validation** — checks `last_committed_snapshot` exists
   in `ducklake_snapshot`; raises `CDC_GAP` (`docs/errors.md`) if
   compaction expired the cursor.
3. **`max_snapshots` validation** — raises `CDC_MAX_SNAPSHOTS_EXCEEDED`
   if the requested cap exceeds the session-wide hard cap (default
   1000 per ADR 0011).
4. **Schema-bound computation** — currently global/whole-lake only:
   `stop_at_schema_change = TRUE` bounds the window before the first
   external snapshot whose `schema_version` differs from the cursor's
   schema version. Per-table-aware boundaries are deferred until
   table filters are accepted by `cdc_consumer_create`.
5. **Notice emission** — `CDC_SCHEMA_BOUNDARY` notice emission is still
   deferred; the row already carries `schema_changes_pending = TRUE`.

**Idempotence contract.** Calling `cdc_window` repeatedly from the
same connection that holds the lease returns the same row until
`cdc_commit` advances the cursor. A second connection's call (under
the same consumer name) is rejected with `CDC_BUSY`. This combination
— same-connection idempotence + reject-other-connection — is
`docs/roadmap/README.md`.

ADR(s): 0006 (schema bounding), 0007 (lease), 0011 (`max_snapshots`
cap).

### `cdc_commit`

```sql
SELECT * FROM cdc_commit(
    catalog     VARCHAR,
    consumer    VARCHAR,
    snapshot_id BIGINT
);
```

Advances `last_committed_snapshot` to `snapshot_id`, refreshes
`last_committed_schema_version` from `ducklake_snapshot`, and
refreshes `owner_heartbeat_at = now()` — all in a single
transaction.

Verifies `owner_token` matches the calling connection's cached
lease before committing; raises `CDC_BUSY` if the lease was lost.
The current implementation returns
`(consumer_name, committed_snapshot, schema_version)` so SQL callers
can assert the committed cursor directly.

Phase-1 implementation status: the shipped call form is
`cdc_commit(catalog, consumer, snapshot_id)`, with explicit `catalog`
for the same qualified-call limitation documented on
`cdc_consumer_create`. `snapshot_id` must exist in
`ducklake_snapshot`, must be `>= last_committed_snapshot`, and must be
`<= current_snapshot()`.

The commit is the only operation that advances the cursor; reads
never advance it (`docs/roadmap/README.md`). Bindings call
`cdc_commit(catalog, consumer, batch.end_snapshot)` after their sink confirms
durability.

ADR(s): 0007, 0009.

### `cdc_wait`

```sql
SELECT cdc_wait(
    catalog     VARCHAR,
    consumer    VARCHAR,
    timeout_ms  := 30000   -- BIGINT, hard-capped at 5 min per ADR 0011
);
```

Honest exponential-backoff polling — backoff defaults to 100ms
floor, 10s cap (per the performance principles in `docs/performance.md`),
session-tunable via `SET ducklake_cdc_wait_max_interval_ms`.

Returns:

- `BIGINT` — the new `current_snapshot()` (when `current_snapshot()`
  has advanced past the consumer's `last_committed_snapshot`).
- `NULL` — when `timeout_ms` elapses without a new snapshot.

`timeout_ms` over the session-wide cap (default 300000ms) is
**clamped, not rejected** — the call still returns. Notice emission via
`CDC_WAIT_TIMEOUT_CLAMPED` is still deferred until the extension has a
session-settings/notice helper shared by wait and schema-boundary UX.

Phase-1 implementation status: the shipped call form is
`cdc_wait(catalog, consumer, timeout_ms := 30000)`, with explicit
`catalog` for the same qualified-call limitation documented on
`cdc_consumer_create`. It polls external DuckLake snapshots only, so
CDC state-table writes do not wake waiters. The backoff curve is the
roadmap default: 100ms floor, doubling, 10s cap. `timeout_ms = 0` is a
non-blocking check.

ADR: 0011.

### `cdc_ddl`

```sql
SELECT * FROM cdc_ddl(
    catalog        VARCHAR,
    consumer       VARCHAR,
    max_snapshots  := 100   -- BIGINT, same hard cap as cdc_window
);
```

Table function. Returns typed DDL events for the consumer's window.
Schema, `details` payloads, and the rename-detection rule are locked
in ADR 0008.

| column | type | notes |
| --- | --- | --- |
| `snapshot_id` | `BIGINT` | the snapshot the DDL happened in |
| `snapshot_time` | `TIMESTAMPTZ` | wall-clock commit time, sourced from `ducklake_snapshot.snapshot_time` (carried so audit timelines need no extra JOIN) |
| `event_kind` | `VARCHAR` | one of `created`, `altered`, `dropped` |
| `object_kind` | `VARCHAR` | one of `schema`, `table`, `view` |
| `schema_id` | `BIGINT` | `NULL` for `event_kind = created` of a schema; otherwise the containing schema id |
| `schema_name` | `VARCHAR` | qualified-name source-of-truth |
| `object_id` | `BIGINT` | `schema_id` for schema events; `table_id` for tables; `view_id` for views |
| `object_name` | `VARCHAR` | the *current* (post-event) name; for `altered.table` with rename, the new name (old name is in `details`) |
| `details` | `VARCHAR` | JSON text; detailed payload shapes are deferred beyond the foundation slice |

Phase-1 implementation status: the shipped foundation call form is
`cdc_ddl(catalog, consumer, max_snapshots := 100)`, with explicit
`catalog` for the same qualified-call limitation documented on
`cdc_consumer_create`. It emits typed rows for
`created`/`altered`/`dropped` schema, table, and view events by
reading `ducklake_snapshot_changes.changes_made` and reconstructing
names from DuckLake metadata tables. The shipped row shape is the
table at the top of this section; the **`details` payload now ships
typed Stage-2 content** for the events listed below.

Stage-2 `details` JSON payload shapes (v0.0.5+):

| event | payload shape |
| --- | --- |
| `created.table` | `{"columns":[{"id":N,"order":N,"name":"...","type":"...","nullable":bool,"default":...,"parent_column":N?},...]}` |
| `altered.table` | `{"old_table_name":"...","new_table_name":"...","added":[...],"dropped":[...],"renamed":[{"id":N,"old_name":"...","new_name":"..."}],"type_changed":[{"id":N,"name":"...","old_type":"...","new_type":"..."}],"default_changed":[{"id":N,"name":"...","old_default":...,"new_default":...}],"nullable_changed":[{"id":N,"name":"...","old_nullable":bool,"new_nullable":bool}]}`. Empty groups are omitted, so the typical single-column ALTER produces a small payload. The rename-pair fields appear only when the table itself was renamed (Finding-1). |
| `created.view` | `{"definition":"...","dialect":"duckdb","column_aliases":"..."}` |
| `dropped.schema` / `dropped.table` / `dropped.view` | `{}` (objects are gone; consumers know what was dropped from the row's `object_id` / `object_name` columns) |
| `created.schema` | `{}` |
| `altered.view` | not emitted by DuckLake — `CREATE OR REPLACE VIEW` shows up as a paired `dropped.view` + `created.view` in the same snapshot |

Type names in `details` come from DuckLake's metadata catalog and
are lowercase (`int32`, `varchar`, `int64`, …), not the DuckDB SQL
spellings. Default values are stored as the raw SQL expression from
the `DEFAULT` clause without surrounding quotes (e.g. `new`, not
`'new'`). Column ids and column orders both start at 1.

Finding-1 rename detection: a `created_table:"X"."Y"` token whose
`table_id` already had a different name in any earlier snapshot is
surfaced as `altered.table` with the rename pair populated, **not**
as `created.table`. This keeps the timeline coherent across renames
so downstream consumers see one continuous identity per table.

Honors the consumer's `event_categories` filter (returns zero rows
if `event_categories = ['dml']`) and `tables` filter
(`created.schema` and `dropped.schema` events are implicitly
suppressed for table-filtered consumers — see
[Filter composition](#filter-composition)).

Per ADR 0008's two-stage extraction:

- **Stage 1** — the foundation reads
  `__ducklake_metadata_<lake>.ducklake_snapshot_changes.changes_made`
  for the consumer's window. The later full extractor will switch to
  `<lake>.snapshots().changes` (the typed MAP) once the full MAP payload
  handling is implemented. Keys/tokens consulted now: created/dropped
  schema, table, and view. Other keys (DML, `merge_adjacent`,
  `altered_table`) are ignored by the foundation.
- **Stage 2** — for each candidate event, query versioned catalog
  tables (`ducklake_table`, `ducklake_column`, `ducklake_view`,
  `ducklake_schema`) using their `begin_snapshot` / `end_snapshot`
  ranges to build the typed `details` payload. The
  Finding-1 rename detection rule (ADR 0008) is part of stage 2.

ADR: 0008.

## Observability

### `cdc_consumer_stats`

```sql
SELECT * FROM cdc_consumer_stats(
    consumer  := NULL   -- VARCHAR; NULL = all consumers
);
```

Table function. Computes one row per consumer with **live, derived**
counters (nothing materialised on `__ducklake_cdc_consumers`):

| column | type | notes |
| --- | --- | --- |
| `consumer_name` | `VARCHAR` | from the consumer row |
| `consumer_id` | `BIGINT` | from the consumer row |
| `last_committed_snapshot` | `BIGINT` | the cursor |
| `current_snapshot` | `BIGINT` | `current_snapshot()` at call time |
| `lag_snapshots` | `BIGINT` | `current_snapshot - last_committed_snapshot`; the canonical "how far behind is this consumer?" measure |
| `lag_seconds` | `DOUBLE` | `EXTRACT(EPOCH FROM now() - committed_snapshot.snapshot_time)`; sourced via JOIN against `<lake>.snapshots()` |
| `oldest_available_snapshot` | `BIGINT` | `MIN(snapshot_id) FROM ducklake_snapshot` — the `CDC_GAP` floor |
| `gap_distance` | `BIGINT` | `last_committed_snapshot - oldest_available_snapshot`; negative ⇒ consumer is already gapped (next `cdc_window` will raise `CDC_GAP`) |
| `tables_unresolved` | `VARCHAR[]` | `tables \ existing-tables-in-catalog-at-current_snapshot()` — derived per ADR 0009 |
| `owner_token` | `UUID` | NULL = no holder |
| `owner_acquired_at` | `TIMESTAMPTZ` | from the lease row |
| `owner_heartbeat_at` | `TIMESTAMPTZ` | from the lease row |
| `lease_alive` | `BOOLEAN` | `now() - owner_heartbeat_at < lease_interval_seconds` |
| `dlq_pending_dml` | `BIGINT` | Phase 2; count of DLQ DML rows for this consumer in non-resolved state |
| `dlq_pending_ddl` | `BIGINT` | Phase 2; same for DDL |
| `dlq_capacity_used_pct` | `DOUBLE` | Phase 2; `dlq_pending_dml × 100.0 / max_dml_dlq_per_consumer` |
| `wait_calls_total` | `BIGINT` | counter — total `cdc_wait` calls served for this consumer |
| `wait_returns_snapshot_total` | `BIGINT` | counter — `cdc_wait` returns a non-NULL value |
| `wait_returns_null_total` | `BIGINT` | counter — `cdc_wait` returns NULL (timeout) |
| `gap_total` | `BIGINT` | counter — `CDC_GAP` raises observed |
| `schema_boundary_total` | `BIGINT` | counter — `CDC_SCHEMA_BOUNDARY` notices emitted |
| `current_max_snapshots_cap` | `BIGINT` | session value of `ducklake_cdc_max_snapshots_hard_cap` (operators who raise the cap can confirm it took effect) |

Counters reset to zero on extension load (per-connection); they are
not persisted. Phase 1 surfaces `wait_*`, `gap_total`, and
`schema_boundary_total`; the DLQ-derived counters land in Phase 2
alongside the DLQ feature.

ADR(s): 0009 (`tables_unresolved` derivation), 0007 (lease columns),
0011 (`current_max_snapshots_cap`).

Phase-1 implementation status: shipped in v0.0.6 as
`cdc_consumer_stats(catalog, consumer := NULL)`. The function never
acquires the consumer lease, never advances any cursor, and writes
nothing beyond the implicit consumer-state bootstrap. Computed in a
single SQL pass that LEFT JOINs `__ducklake_cdc_consumers` to
`ducklake_snapshot` for `lag_seconds`, then derives `tables_unresolved`
and `lease_alive` per-row in the C++ scan.

Shipped row shape (other counters from the spec table land in PR 4 /
Phase 2):

| column | type | notes |
| --- | --- | --- |
| `consumer_name` | `VARCHAR` | from the consumer row |
| `consumer_id` | `BIGINT` | from the consumer row |
| `last_committed_snapshot` | `BIGINT` | the cursor |
| `current_snapshot` | `BIGINT` | `current_snapshot()` at call time |
| `lag_snapshots` | `BIGINT` | `current_snapshot - last_committed_snapshot` |
| `lag_seconds` | `DOUBLE` | seconds since `last_committed_snapshot.snapshot_time`; NULL when the cursor predates the catalog (or has never been committed) |
| `oldest_available_snapshot` | `BIGINT` | `MIN(snapshot_id)` from `ducklake_snapshot` |
| `gap_distance` | `BIGINT` | `last_committed_snapshot - oldest_available_snapshot` |
| `tables` | `VARCHAR[]` | mirror of the consumer's `tables` filter; NULL = no filter |
| `tables_unresolved` | `VARCHAR[]` | per-consumer subset of `tables` not present at `current_snapshot`; empty when the filter is empty/NULL |
| `change_types` | `VARCHAR[]` | mirror of the consumer's `change_types` filter; NULL = no filter |
| `owner_token` | `UUID` | NULL = no holder |
| `owner_acquired_at` | `TIMESTAMPTZ` | from the lease row |
| `owner_heartbeat_at` | `TIMESTAMPTZ` | from the lease row |
| `lease_interval_seconds` | `INTEGER` | from the lease row (configured per consumer at create time, defaults to 60s) |
| `lease_alive` | `BOOLEAN` | `now() - owner_heartbeat_at < lease_interval_seconds`; false when nobody is holding the lease |

### `cdc_audit_recent`

```sql
SELECT * FROM cdc_audit_recent(
    catalog        VARCHAR,
    since_seconds  := 86400,
    consumer       := NULL
);
```

Read-only window over `__ducklake_cdc_audit`. Returns one row per
audited lifecycle event in the lookback window, newest first. Useful
for dashboards that surface "who reset what when" without granting
direct read access to the audit table.

| column | type | notes |
| --- | --- | --- |
| `ts` | `TIMESTAMPTZ` | when the event was recorded |
| `audit_id` | `BIGINT` | monotonically-increasing per-catalog id |
| `actor` | `VARCHAR` | `current_user` at the time of the write |
| `action` | `VARCHAR` | one of `consumer_create`, `consumer_reset`, `consumer_drop`, `consumer_force_release`, `lease_force_acquire` |
| `consumer_name` | `VARCHAR` | the targeted consumer |
| `consumer_id` | `BIGINT` | the targeted consumer's stable id |
| `details` | `VARCHAR` | per-action JSON payload (resolved snapshot, previous lease state, etc) |

`since_seconds` is enforced via `epoch(ts) >= epoch(now()) -
since_seconds`; negative values raise `Invalid Input Error`. The
default is 24h. Calling with `since_seconds => 0` returns rows whose
`ts` is in the future (i.e. an empty slice on a sane clock) and is the
recommended way to probe "anything in the last N seconds" with a tight
lookback. Filtering by `consumer` is an exact match on
`consumer_name`. Both helpers reject catalogs that fail the
`CDC_INCOMPATIBLE_CATALOG` probe.

## Acknowledged sugar wrappers

Each entry below is a **one-line composition** over primitives plus
DuckLake builtins. The composition is normative — the SQL-helper
library escape hatch (ADR 0005) re-implements these as ordinary SQL
recipes. Sugar entries do not auto-commit; durable consumers still
call `cdc_commit(consumer, batch.next_snapshot_id)`.

### `cdc_events`

```sql
SELECT * FROM cdc_events(
    consumer       VARCHAR,
    max_snapshots  := 100
);
```

**Composition:**

```sql
WITH w AS (SELECT * FROM cdc_window(consumer, max_snapshots))
SELECT  s.snapshot_id,
        s.snapshot_time,
        s.schema_version,
        s.changes,            -- the raw MAP, see § snapshots-changes-map-reference
        s.author,
        s.commit_message,
        s.commit_extra_info
FROM    <lake>.snapshots() s, w
WHERE   s.snapshot_id BETWEEN w.start_snapshot AND w.end_snapshot
  AND   ( consumer.tables IS NULL
          OR  -- map-key filter against the consumer's tables list per ADR 0008 finding 1
              -- (tables_created / tables_altered / tables_dropped /
              --  tables_inserted_into / tables_deleted_from /
              --  inlined_insert / inlined_delete) referencing a
              --  qualified name or table_id in the consumer's filter
              keys_overlap(s.changes, consumer.tables)
        )
  AND   ( -- maintenance-only snapshots are still emitted; operators
          --  filter further on (s.changes->'merge_adjacent') OR
          --  (s.changes->'compacted_table'). Per ADR 0008 finding 2,
          --  cdc_events SQL must look for both spellings.
          TRUE
        )
ORDER BY s.snapshot_id;
```

Returns the raw structured `changes` map plus snapshot metadata. For
inspection / dashboards, **not** for typed sink processing — typed
sinks use `cdc_changes` (DML) and `cdc_ddl` (DDL).

Phase-1 implementation status: the shipped foundation form is
`cdc_events(catalog, consumer, max_snapshots := 100)`, with explicit
`catalog` for the same qualified-call limitation documented on
`cdc_consumer_create`. The shipped row shape is:

| column | type | notes |
| --- | --- | --- |
| `snapshot_id` | `BIGINT` | from `ducklake_snapshot` |
| `snapshot_time` | `TIMESTAMPTZ` | from `ducklake_snapshot` |
| `changes_made` | `VARCHAR` | the raw token list from `ducklake_snapshot_changes.changes_made`; the typed MAP form lands once we switch to `<lake>.snapshots().changes` |
| `author` | `VARCHAR` | from `ducklake_snapshot_changes` |
| `commit_message` | `VARCHAR` | from `ducklake_snapshot_changes` |
| `commit_extra_info` | `VARCHAR` | from `ducklake_snapshot_changes`; the outbox-metadata convention lives downstream |
| `next_snapshot_id` | `BIGINT` | the window's `end_snapshot`, projected onto every row so `cdc_commit(consumer, next_snapshot_id)` is one obvious call |
| `schema_version` | `BIGINT` | `cdc_window`'s `schema_version` (the version the rows belong to) |
| `schema_changes_pending` | `BOOLEAN` | `cdc_window`'s `schema_changes_pending` flag |

Acquires/extends the consumer's owner-token lease via the same path
`cdc_window` uses, so concurrent calls from a different connection
raise `CDC_BUSY`. Internal `__ducklake_cdc_*` writes (consumer-state
bootstrap / audit / lease columns) are filtered out so they never
appear here. The `tables` filter is applied per-snapshot when set
(snapshots whose `changes_made` does not reference any table in the
filter are dropped). Does **not** auto-commit.

### `cdc_changes`

```sql
SELECT * FROM cdc_changes(
    consumer       VARCHAR,
    table          VARCHAR,
    max_snapshots  := 100
);
```

**Composition:**

```sql
WITH w AS (SELECT * FROM cdc_window(consumer, max_snapshots))
SELECT  s.snapshot_id,
        s.snapshot_time,
        s.schema_version,
        s.author,
        s.commit_message,
        s.commit_extra_info,
        c.*
FROM    <lake>.table_changes(table, w.start_snapshot, w.end_snapshot) c
JOIN    <lake>.snapshots() s USING (snapshot_id)
WHERE   ( consumer.change_types IS NULL
          OR c.change_type = ANY(consumer.change_types) )
ORDER BY s.snapshot_id, table_id, rowid;
```

DML rows for one table. Inherits inlined-data, encryption,
schema-evolution handling from `table_changes` (`docs/roadmap/README.md`
pillar 2).

The `WHERE` clause for `change_types` is the only filter the
extension applies on top of `table_changes` — the consumer's policy,
declared at create time on the consumer row (`docs/roadmap/README.md`
pillar 11). User-side `WHERE` further narrows.

Phase-1 implementation status: the shipped foundation form is
`cdc_changes(catalog, consumer, table, max_snapshots := 100)`, with
explicit `catalog` for the same qualified-call limitation documented
on `cdc_consumer_create`. The function probes the underlying
`<lake>.table_changes(...)` schema with a `LIMIT 0` query at
`current_snapshot()` during Bind so the planner sees the table's
actual columns, then issues the real query at `Init` time. The shipped
row shape is the underlying `table_changes` columns:

```text
snapshot_id BIGINT
rowid       BIGINT
change_type VARCHAR
<...table columns as of the END snapshot...>
```

…plus extras appended after the table columns:

| column | type | notes |
| --- | --- | --- |
| `snapshot_time` | `TIMESTAMPTZ` | from `ducklake_snapshot` |
| `author` | `VARCHAR` | from `ducklake_snapshot_changes` |
| `commit_message` | `VARCHAR` | from `ducklake_snapshot_changes` |
| `commit_extra_info` | `VARCHAR` | from `ducklake_snapshot_changes` |
| `next_snapshot_id` | `BIGINT` | the window's `end_snapshot`, projected onto every row |

Acquires/extends the consumer's owner-token lease via the same path
`cdc_window` uses. Honors the consumer's `change_types` filter when
set (no implicit `update_preimage` filtering). When the consumer has a
`tables` filter that does not include the requested table, the call
fails loudly with `cdc_changes: table 'X' is not in consumer 'Y'
tables filter` rather than returning silently empty results. Does
**not** auto-commit.

### `cdc_schema_diff`

```sql
SELECT * FROM cdc_schema_diff(
    table         VARCHAR,
    from_snapshot BIGINT,
    to_snapshot   BIGINT
);
```

**Composition:**

```sql
-- Stateless: no consumer required.
-- Equivalent to: collect every cdc_ddl event with object_kind = 'table'
-- and (event_kind = 'altered' OR (event_kind = 'created' AND from_snapshot < creation_snapshot))
-- for this qualified table name between [from_snapshot, to_snapshot],
-- then merge the details payloads into a flat per-column delta.
SELECT  -- columns derived from merged details: column_id, name, type,
        -- default, nullable, parent_column_id, action ∈ {added,dropped,renamed,type_changed,default_changed,nullable_changed}
        ...
FROM    <stage-2 extractor reused from cdc_ddl, bounded by [from_snapshot, to_snapshot]>;
```

Stateless inspection helper. Same stage-2 extractor as `cdc_ddl`,
different surface. Reuses ADR 0008's typed payload reconstruction.

Phase-1 implementation status: the shipped foundation form is
`cdc_schema_diff(catalog, table, from_snapshot, to_snapshot)`. The
function resolves the `schema.table` filter to the *set* of
`table_id` values that ever bore that qualified name within
`[from_snapshot, to_snapshot]`, then matches Stage-1 tokens by
`table_id` rather than by name so renames keep the timeline coherent.
Returns one row per column-level change with the columns:

| column | type | populated when |
| --- | --- | --- |
| `snapshot_id` | `BIGINT` | always |
| `snapshot_time` | `TIMESTAMPTZ` | always |
| `change_kind` | `VARCHAR` | one of `added`, `dropped`, `renamed`, `type_changed`, `default_changed`, `nullable_changed`, `table_rename` |
| `column_id` | `BIGINT` | populated for column-level rows; `NULL` for `table_rename` |
| `old_name` / `new_name` | `VARCHAR` | populated per change kind (for `table_rename`, these carry the old/new table names) |
| `old_type` / `new_type` | `VARCHAR` | populated for `added` (new only), `dropped` (old only), `type_changed` (both) |
| `old_default` / `new_default` | `VARCHAR` | populated for `added` (new only), `dropped` (old only), `default_changed` (both) |
| `old_nullable` / `new_nullable` | `BOOLEAN` | populated for `added` (new only), `dropped` (old only), `nullable_changed` (both) |

Pure CREATE TABLE events (i.e. not a Finding-1 rename) surface as a
series of `added` rows — one per column in the initial schema. Negative
or inverted snapshot ranges are rejected at bind. Does **not** require
the CDC state tables. SQLLogic coverage in `test/sql/ddl_stage2.test`.

### `cdc_recent_changes`

```sql
SELECT * FROM cdc_recent_changes(
    table  VARCHAR,
    since  := INTERVAL '5 minutes'
);
```

**Composition:**

```sql
-- Stateless: no consumer required.
SELECT  c.*
FROM    <lake>.table_changes(
            table,
            (SELECT max(snapshot_id)
             FROM   <lake>.snapshots()
             WHERE  snapshot_time < now() - since),
            (SELECT current_snapshot())
        ) c
ORDER BY snapshot_id, rowid;
```

Pure passthrough to `table_changes` with a bare snapshot bound.
Exists for the SQL-tinkerer persona's first 30 seconds — no cursor,
no commit, no gap detection. Sugar only.

Phase-1 implementation status: the shipped foundation form is
`cdc_recent_changes(catalog, table, since_seconds := 300)` (300s = 5
minutes default). The function uses the same dynamic-schema probe
pattern as `cdc_changes` — a `LIMIT 0` query at `current_snapshot()`
in Bind discovers the table's columns; the real query runs at Init
with the resolved `[start_snapshot, current_snapshot()]` range. The
shipped row shape is the underlying `table_changes` columns
`(snapshot_id, rowid, change_type, <table cols>)` followed by
`(snapshot_time, author, commit_message, commit_extra_info)` — there
is no `next_snapshot_id` because there is no consumer cursor to
commit at. **Does not require the CDC state tables**: it reads
upstream catalog state directly. `since_seconds` of `INTERVAL` form is
deferred until DuckDB has a `-(TIMESTAMP_TZ, INTERVAL)` binder; for
now epoch-second math is the contract. `since_seconds < 0` raises
`InvalidInputException`. Calls against an unknown table fail loudly
during the `LIMIT 0` probe.

### `cdc_recent_ddl`

```sql
SELECT * FROM cdc_recent_ddl(
    since   := INTERVAL '1 day',
    table   := NULL   -- VARCHAR; NULL = whole-lake
);
```

**Composition:**

```sql
-- Stateless: no consumer required.
-- Same stage-2 extractor as cdc_ddl, bounded by:
--    from_snapshot = max(snapshot_id WHERE snapshot_time < now() - since)
--    to_snapshot   = current_snapshot()
-- and optionally filtered by table.
SELECT  ...
FROM    <stage-1+2 extraction reused from cdc_ddl>;
```

"What schema changes happened in the last day?" Sugar over `cdc_ddl`
extraction logic with bare snapshot bounds. No consumer required.

Phase-1 implementation status: the shipped foundation form is
`cdc_recent_ddl(catalog, since_seconds := 86400, for_table := NULL)`
(86400s = 1 day default). It reuses the same Stage-1 / Stage-2
extractor as `cdc_ddl` so the rich payloads will land here at the
same time. The optional table filter parameter is named `for_table`
rather than the roadmap's `table := ...` because `table` is a
reserved word in DuckDB's parser — quoting it (`"table" = '...'`)
would force every caller into awkward syntax. Unqualified table
names auto-qualify to `main.<name>`. Schema-level events
(`created.schema` / `dropped.schema`) are dropped when `for_table` is
set because they have no table identity. **Does not require the CDC
state tables**: it reads upstream catalog state directly.
`since_seconds < 0` raises `InvalidInputException`.

## Filter composition

<a id="filter-composition"></a>

The three filter knobs (`tables`, `change_types`, `event_categories`)
compose deterministically. The matrix below is the **binding
reference**; both the Python (Phase 3) and Go (Phase 4) test suites
assert it verbatim.

### Per-knob semantics

- **`event_categories` is a top-level switch.** It selects which
  *streams* the consumer reads at all.
  - `NULL` (default) or `['ddl', 'dml']` — both `cdc_ddl` and
    `cdc_changes` return rows.
  - `['dml']` — `cdc_ddl` returns zero rows; `cdc_changes` operates
    normally.
  - `['ddl']` — `cdc_changes` returns zero rows; `cdc_ddl` operates
    normally.

- **`tables` filters both DDL and DML events**, when set, to those
  whose qualified name (`schema.table`) is in the list.
  - On DDL: an `altered.table` event fires only if the affected
    table's qualified name is in `tables`. A `created.schema` /
    `dropped.schema` event with `tables` set is **not** emitted —
    schemas have no qualified table name to match, so schema-level
    DDL is implicitly suppressed for table-filtered consumers. A
    `created.table` event for a brand-new table fires only if that
    table's qualified name is in `tables`. Per the rename-detection
    rule (ADR 0008 Finding 1), an `altered.table` event synthesized
    from a `tables_created` MAP entry uses the *new* table name for
    the filter check — so a rename `orders → orders_v2` with
    `tables := ['main.orders']` does **not** fire (the new name is
    not in the filter); the operator should add `'main.orders_v2'`
    to the filter before the rename to avoid a silent drop. See the
    `cdc_consumer_stats().tables_unresolved` column for the
    detection signal.
  - On DML: behaves naturally — `cdc_changes(consumer, table)` is
    per-table; `cdc_events` filters the structured map keys per the
    `keys_overlap(...)` predicate in the composition above.
  - Consumers that legitimately need schema-level DDL
    (`created.schema`, `dropped.schema`) must subscribe with
    `tables := NULL`.

- **`change_types` filters DML only.** DDL events have no
  `change_type` field; `change_types` is **silently ignored** when
  reading `cdc_ddl`. Documented loudly so callers don't expect DDL
  filtering by `change_types`.

### Composition rules

- All three filters compose with **AND semantics where they
  overlap**. A consumer with `tables := ['main.orders']`,
  `change_types := ['delete']`, `event_categories := ['ddl', 'dml']`
  sees:
  - DDL: `created.table`, `altered.table`, `dropped.table` events
    for `main.orders` (the create event arrives only if `start_at`
    precedes the table's creation snapshot).
  - DML: only `delete` events on `main.orders`.
- **Matching is exact, case-sensitive**, on the qualified name as
  DuckLake stores it (`schema.table`). No glob, no regex, no fuzzy
  matching.

### The matrix

`tables` × `change_types` × `event_categories` collapsed into the
table that bindings assert:

| `tables` | `change_types` | `event_categories` | `cdc_ddl` returns | `cdc_changes(t)` returns |
| --- | --- | --- | --- | --- |
| `NULL` | `NULL` | `NULL` (≡ both) | every DDL event for every object in the lake | every DML row for `t`, all four `change_type` values |
| `NULL` | `NULL` | `['dml']` | (zero rows) | every DML row for `t`, all four `change_type` values |
| `NULL` | `NULL` | `['ddl']` | every DDL event for every object in the lake | (zero rows) |
| `NULL` | `['delete']` | `NULL` | every DDL event for every object | DML rows for `t` where `change_type = 'delete'` |
| `['main.orders']` | `NULL` | `NULL` | every DDL event for `main.orders` (table-scope only); schema-level DDL suppressed | DML rows for `t` where `t = 'main.orders'`, all four `change_type` values; `t ≠ 'main.orders'` returns zero rows |
| `['main.orders']` | `['insert']` | `['dml']` | (zero rows) | DML rows for `main.orders` where `change_type = 'insert'` |
| `['main.orders']` | `NULL` | `['ddl']` | DDL events for `main.orders` (table-scope only); schema-level DDL suppressed | (zero rows) |
| `['main.orders']` | `['delete']` | `['ddl', 'dml']` | DDL events for `main.orders` (table-scope only) | DML rows for `main.orders` where `change_type = 'delete'` |

Implementation note: the `change_types` filter is applied as the
`WHERE c.change_type = ANY(consumer.change_types)` clause in the
`cdc_changes` composition above. The `tables` filter on `cdc_ddl`
is applied at stage 1 (filter the candidate event set by qualified
name) and at stage 2 (skip events whose resolved object resolves
outside the filter). The `event_categories` filter is a top-of-query
short-circuit (`WHERE FALSE` substituted in when the relevant
category is excluded).

ADR(s): 0008 (filter rules), 0009 (column ownership).

## Delivery ordering contract

The order in which `cdc_changes`, `cdc_ddl`, and `cdc_events`
deliver rows is a binding API contract — sinks downstream of these
wrappers depend on it. Bindings (Phase 3 / 4) enforce this when
interleaving streams; raw `table_changes` callers get whatever
DuckLake returns and the project does not reorder for them.

### Across snapshots

Strict `snapshot_id` ascending. Always.

### Within a snapshot, between categories

**DDL events before DML events.** A sink that needs to provision a
new column before receiving rows under that column relies on this.
Per ADR 0008 § "Per-snapshot delivery ordering contract".

### Within a snapshot, within DDL

Sort by `(object_kind, object_id)` ascending. `object_kind` sort
order: `schema, view, table` — creates schemas before views before
tables; drops in the opposite order. (ADR 0008 sort rule reproduced
here for the binding test suites.)

### Within a snapshot, within DML, across tables

Sort by `table_id` ascending. Cheap, deterministic, gives consumers
a stable order they can hash.

### Within a snapshot, within DML, within a table

Sort by `rowid` ascending. Per ADR 0002.

### Notes for binding authors

- `snapshots().changes` is a structured map and has no inherent
  order; bindings that materialise the raw map per `cdc_events`
  do not promise an ordering on map keys.
- A user calling `cdc_ddl` and `cdc_changes` separately and applying
  results in the wrong order is a user bug, not a binding bug. The
  binding-side `tail()` / `Tail()` helpers interleave correctly
  under this contract; users who roll their own loop should follow
  the same order.
- The `snapshot_time` column on every wrapper output (`cdc_ddl`,
  `cdc_changes`, `cdc_events`) is sourced from
  `ducklake_snapshot.snapshot_time` and is consistent across all
  three streams — sinks rendering one timeline can sort on
  `(snapshot_id, [the contract above])` without re-joining.

ADR: 0008.

## `cdc_ddl` `details` payload shapes

<a id="cdc_ddl-details-payload-shapes"></a>

The `details` JSON payload returned by `cdc_ddl` varies by
`(event_kind, object_kind)`. **The shapes are normative** — Phase 1
emits them byte-for-byte, bindings (Phase 3 / 4) generate typed
models off them. Adding a key to a payload is a minor version bump;
renaming or removing a key is a major version bump.

Locked in ADR 0008; reproduced here as the binding reference.

### `created.schema`

```json
{}
```

No payload — a schema has no metadata at create time beyond its
name, which is already in the row's `object_name`.

### `created.table`

```json
{
  "columns": [
    {
      "column_id":        123,
      "name":             "customer_email",
      "type":             "VARCHAR",
      "default":          null,
      "nullable":         true,
      "parent_column_id": null
    }
  ],
  "partition_by": [],
  "properties":   {}
}
```

`columns[]` is sourced from `ducklake_column` rows whose
`begin_snapshot = <this snapshot>` for the new `table_id`.
`parent_column_id` is non-NULL only for nested struct fields.
`partition_by` may be an empty array. `properties` is whatever
DuckLake stores in `ducklake_tag` rows scoped to this table at this
snapshot (commonly empty in v0.1; `cdc.enabled`, `cdc.topic`, and
`cdc.schema_registry_subject` are the project's documented tag
conventions).

### `created.view`

```json
{
  "definition": "SELECT id, name FROM main.users WHERE active"
}
```

The view's `view_definition` from `ducklake_view`.

### `altered.table`

```json
{
  "old_table_name":   "main.orders",
  "new_table_name":   "main.orders_v2",
  "added": [
    { "column_id": 7, "name": "shipped_at", "type": "TIMESTAMP",
      "default": null, "nullable": true, "parent_column_id": null }
  ],
  "dropped": [
    { "column_id": 4, "name": "legacy_status" }
  ],
  "renamed": [
    { "column_id": 3, "old_name": "amt", "new_name": "amount" }
  ],
  "type_changed": [
    { "column_id": 5, "old_type": "INTEGER", "new_type": "BIGINT" }
  ],
  "default_changed": [
    { "column_id": 6, "old_default": null, "new_default": "0" }
  ],
  "nullable_changed": [
    { "column_id": 8, "old_nullable": true, "new_nullable": false }
  ]
}
```

A pure table rename surfaces with `{old_table_name, new_table_name}`
populated and **all other arrays empty** — this is the marker shape
for ADR 0008's stage-2 rename detection (Finding 1). A pure column
rename surfaces with
`{old_table_name: null, new_table_name: null, renamed: [{...}]}`
and other arrays empty. Multi-effect alters populate multiple
arrays. `default_changed` and `nullable_changed` are populated only
when the column is otherwise unchanged (no rename, no type change);
otherwise the change is folded into the `renamed[]` /
`type_changed[]` entry to keep one row-per-column-per-snapshot.

### `altered.view`

```json
{
  "old_definition": "SELECT id, name FROM main.users WHERE active",
  "new_definition": "SELECT id, name, email FROM main.users WHERE active",
  "old_view_name":  "main.user_v",
  "new_view_name":  "main.user_v"
}
```

`CREATE OR REPLACE VIEW` (the only `ALTER VIEW`-equivalent DuckDB
supports) surfaces as a dropped+created pair on the MAP side per the
upstream probe's `altered_view` row; stage-2 collapses them into one
`altered.view` event when the same view name reappears within one
snapshot.

### `dropped.table`, `dropped.view`, `dropped.schema`

```json
{}
```

No payload. The pre-drop shape is recoverable via
`tbl AT (VERSION => <snapshot_id - 1>)` — operators who need it use
the recipe in
[`docs/operational/audit-limitations.md`](./operational/audit-limitations.md).

### Known v0.1 limitations on `details`

- **`variant`-typed columns** surface only the surface-level type
  (`type: "variant"`) in `created.table.columns[]` and the
  `added` / `type_changed` arrays of `altered.table`. Shredded
  sub-field exposure (sourced from `ducklake_file_variant_stats`)
  is deferred to v0.2. ADR 0008 § "Open questions deferred".
- **Per-column tag changes** (`ducklake_column_tag` rows) are
  **not** surfaced in `altered.table` `details` in v0.1; they are
  visible via `cdc_events` raw map output. Promote to a
  `tags_added` / `tags_removed` array in v0.2 if there is demand.

## Tag conventions

`v0.1` reads but does not implement tag-based selection (that is
v0.2; see the good-fit / poor-fit guidance in `docs/performance.md`). The project commits
to the following tag namespace conventions so producers and sinks
agree before tag selection ships:

- `cdc.enabled` — `'true'` / `'false'`. Producer hint: "include
  this table in default consumers." Selection on this tag is a v0.2
  feature; v0.1 surfaces it via `cdc_events`'s raw map and
  `cdc_consumer_stats()` exposes the tag in a future column.
- `cdc.topic` — string. Producer hint: "route changes from this
  table to this Kafka / NATS / Redis Streams topic." Reference sinks
  read it; the extension does not.
- `cdc.schema_registry_subject` — string. Producer hint: "this is
  the subject name to register the new schema under in Confluent /
  Apicurio." Reference sinks read it; the extension does not.

Set with the standard DuckLake catalog `ducklake_tag` /
`ducklake_column_tag` writes. Documented as conventions, not
enforced primitives.

## `snapshots().changes` MAP key reference

<a id="snapshots-changes-map-reference"></a>

DuckLake's `snapshots()` table function returns a typed `MAP` column
named `changes`. The full set of keys observed across the supported
DuckLake versions is enumerated by `test/upstream/enumerate_changes_map.py`
(Phase 0 upstream probe, promoted to recurring CI in Phase 1 per ADR 0008).
This section is the canonical, version-stamped table of those keys.

<!-- BEGIN: snapshots-changes-map-reference (auto-generated) -->

<!-- Generated by test/upstream/enumerate_changes_map.py — do not edit by hand. -->

Captured against DuckDB `1.5.2` + DuckLake extension `415a9ebd` with `DATA_INLINING_ROW_LIMIT = 10`.

Run provenance: `captured_at=2026-04-28T11:29:46Z` · `git_sha=43de1523f919` · `platform=darwin-arm64`.

Each row is one operation kind from the upstream probe script. The `changes_made` column shows the per-spec text key as it appears in `__ducklake_metadata_<lake>.ducklake_snapshot_changes`. The MAP-key columns show the keys observed in `<lake>.snapshots().changes` for the same snapshot, per backend.

| Kind | Example `changes_made` | `snapshots().changes` keys (duckdb) | `snapshots().changes` keys (postgres) | `snapshots().changes` keys (sqlite) |
| --- | --- | --- | --- | --- |
| `_seed_attach_initial_snapshot` | `created_schema:"main"` | `schemas_created` | `schemas_created` | `schemas_created` |
| `created_schema` | `created_schema:"test_s"` | `schemas_created` | `schemas_created` | `schemas_created` |
| `created_table_main` | `created_table:"main"."t_main"` | `tables_created` | `tables_created` | `tables_created` |
| `created_table_qualified` | `created_table:"test_s"."t_sub"` | `tables_created` | `tables_created` | `tables_created` |
| `inserted_into_table_regular` | `inserted_into_table:2` | `tables_inserted_into` | `tables_inserted_into` | `tables_inserted_into` |
| `inserted_into_table_inlined` | `inlined_insert:2` | `inlined_insert` | `inlined_insert` | `inlined_insert` |
| `deleted_from_table_small` | `inlined_delete:2` | `inlined_delete` | `inlined_delete` | `inlined_delete` |
| `deleted_from_table_large` | `deleted_from_table:2` | `tables_deleted_from` | `tables_deleted_from` | `tables_deleted_from` |
| `inlined_delete_inline_on_inline` | `inlined_delete:2` | `inlined_delete` | `inlined_delete` | `inlined_delete` |
| `inserted_into_table_more` | `inserted_into_table:2` | `tables_inserted_into` | `tables_inserted_into` | `tables_inserted_into` |
| `_setup_compact_table` | `created_table:"main"."t_compact"` | `tables_created` | `tables_created` | `tables_created` |
| `_setup_compact_files_1` | `inserted_into_table:4` | `tables_inserted_into` | `tables_inserted_into` | `tables_inserted_into` |
| `_setup_compact_files_2` | `inserted_into_table:4` | `tables_inserted_into` | `tables_inserted_into` | `tables_inserted_into` |
| `_setup_compact_files_3` | `inserted_into_table:4` | `tables_inserted_into` | `tables_inserted_into` | `tables_inserted_into` |
| `compacted_table` | `merge_adjacent:4` | `merge_adjacent` | `merge_adjacent` | `merge_adjacent` |
| `altered_table_add_column` | `altered_table:2` | `tables_altered` | `tables_altered` | `tables_altered` |
| `altered_table_drop_column` | `altered_table:2` | `tables_altered` | `tables_altered` | `tables_altered` |
| `altered_table_rename_column` | `altered_table:2` | `tables_altered` | `tables_altered` | `tables_altered` |
| `altered_table_change_type` | `altered_table:2` | `tables_altered` | `tables_altered` | `tables_altered` |
| `altered_table_rename_table` | `created_table:"main"."renamed_main"` | `tables_created` | `tables_created` | `tables_created` |
| `created_view` | `created_view:"main"."v1"` | `views_created` | `views_created` | `views_created` |
| `altered_view` | `dropped_view:5,created_view:"main"."v1"` | `views_created`, `views_dropped` | `views_created`, `views_dropped` | `views_created`, `views_dropped` |
| `dropped_view` | `dropped_view:6` | `views_dropped` | `views_dropped` | `views_dropped` |
| `dropped_table` | `dropped_table:2` | `tables_dropped` | `tables_dropped` | `tables_dropped` |
| `dropped_table_qualified` | `dropped_table:3` | `tables_dropped` | `tables_dropped` | `tables_dropped` |
| `dropped_schema` | `dropped_schema:1` | `schemas_dropped` | `schemas_dropped` | `schemas_dropped` |

### Per-backend health

| Backend | Ops with snapshot | Hard failures | No-snapshot ops |
| --- | ---: | ---: | ---: |
| `duckdb` | 26 | 0 | 0 |
| `postgres` | 26 | 0 | 0 |
| `sqlite` | 26 | 0 | 0 |

Hard failures indicate a DuckLake-on-backend bug; the operation does not produce a snapshot and the backend is excluded from divergence comparison for that operation.

### Backend divergence (only backends that ran the op)

All 3 backends (duckdb, postgres, sqlite) emit the same MAP key set for every operation kind. Stage-1 discovery code is therefore backend-agnostic at the MAP-key level.

<!-- END: snapshots-changes-map-reference (auto-generated) -->

**Findings worth flagging (locked into ADR 0008):**

- **Table renames surface as `tables_created`, not `tables_altered`.** The
  spec note (and the prior text framing in `docs/roadmap/`)
  says RENAME TABLE folds into `altered_table:<id>`. Empirically (DuckDB
  1.5.2 / DuckLake `415a9ebd`), an `ALTER TABLE x RENAME TO y` produces a
  snapshot whose `changes_made` is `created_table:"schema"."y"` and whose
  `snapshots().changes` MAP has `tables_created: ['schema.y']`. The
  underlying `__ducklake_metadata_<lake>.ducklake_table` row preserves
  `table_id` across the rename — the old row gets `end_snapshot` set, a
  new row with the same `table_id` and the new name is inserted with
  `begin_snapshot` set. Stage-2 of `cdc_ddl` (per ADR 0008) MUST detect
  this case (a `tables_created` event for an already-known `table_id`)
  and synthesize an `altered.table` event with `details.old_table_name` /
  `details.new_table_name` populated, **not** a plain `created.table`
  event. Column renames (`RENAME COLUMN`) DO surface as `tables_altered`
  / `altered_table` and require no special-case handling.
- **`compacted_table` (text) ↔ `merge_adjacent` (MAP).** The spec
  documents the `changes_made` text key as `compacted_table:<id>`. The
  typed MAP uses `merge_adjacent: [<id>]`. The `cdc_events` filter SQL
  must look for both spellings; ADR 0008's filter logic is locked
  accordingly.
- **Backend coverage is explicit.** DuckLake supports three catalog
  backends — DuckDB, SQLite, Postgres — and the upstream probe can run
  against all three. Full CI runs the DuckDB + SQLite check; run the
  Postgres leg before changing the committed reference.

## Special-type serialization conventions

Default serializers for non-portable types so downstream sinks have a
contract. Per `docs/roadmap/README.md` and ADR 0001, **these
serializers run in language clients (Phase 3 / 4), not in the
extension.** The extension exposes raw DuckLake values; the sink-side
contract is documented here so all bindings agree:

| Type | Serialization | Notes |
| --- | --- | --- |
| `variant` | JSON (DuckDB's `variant_to_json` cast) | Use shredded sub-fields from `ducklake_file_variant_stats` where present (v0.2 enhancement; v0.1 ships the surface-level cast). |
| `geometry` | GeoJSON | Bounding box surfaced as a separate column on the binding-side `Event` model. |
| `blob` | base64 by default | Passthrough binary opt-in via the binding's sink config. |
| `decimal` | exact string | Preserve precision and scale; never down-cast to `DOUBLE`. |
| `list`, `struct`, `map` | JSON | Recurse into element / field types using these same rules. |
| `timestamp`, `timestamptz` | ISO-8601 string in UTC (`Z` suffix on tz-aware) | The extension surfaces `TIMESTAMPTZ` natively; bindings stringify on serialization. |
| `uuid` | string (`hex-hex-hex-hex-hex` lowercase) | Matches Postgres / standard UUID rendering. |

Per-backend caveats (from the DuckLake spec + ADR 0009 type
mapping):

- **PostgreSQL catalogs** cannot store `variant` inline in the
  catalog metadata tables; the producer-side `set_commit_message`
  `extra_info` field can carry a JSON payload but cannot carry a
  `variant`. (Data files are unaffected.)
- **SQLite catalogs** collapse many types to `TEXT`; the
  `__ducklake_cdc_consumers.tables` array, for instance, is stored
  as a JSON-text column on SQLite per ADR 0009's type-mapping
  table. Binding-side serialization is unaffected — the catalog
  storage shape is internal to the extension.

Encryption, inlined-data and partial-file handling on the read path
are inherited from `table_changes` (`docs/roadmap/README.md`).
The project tests that we do not break them; we do not claim them as
features.

ADR(s): 0001 (extension language; serializers in bindings), 0009
(type mapping per backend), 0010 (audit log).

## Producer outbox metadata

See [`docs/conventions/outbox-metadata.md`](./conventions/outbox-metadata.md)
for the recommended JSON shape for `commit_extra_info`. Documented
as a convention, not enforced by the extension.

## Operational policies

These policies describe steady-state behaviour at the consumer
boundary — useful for operators sizing catalogs and writing
runbooks. They are referenced from `docs/roadmap/`
§ "Operational policies" and from the Phase 0 exit criteria.

### Backpressure: what happens when a consumer can't keep up

A consumer that consistently can't drain its window inside
`max_snapshots` (default 100, hard cap 1000 per ADR 0011) sees lag
grow **monotonically**. The extension does not throttle the
producer, does not drop snapshots, and does not silently advance
the cursor. Specifically:

- `cdc_window` keeps returning a window bounded by `max_snapshots`
  ahead of the cursor.
- `cdc_consumer_stats().lag_snapshots` and `lag_seconds` rise on
  every poll until the consumer catches up.
- If `ducklake_expire_snapshots` runs with an
  `expire_older_than` that crosses the cursor before the consumer
  catches up, the next `cdc_window` raises `CDC_GAP`
  (`docs/errors.md`) — the consumer must reset to an available
  snapshot, dropping the gap range.

**Operator recommendation:** keep `expire_older_than >
max_expected_lag` (with comfortable headroom). Monitor
`cdc_consumer_stats().lag_seconds` per consumer; alert when it
crosses 50% of the `expire_older_than` budget. The pillar is
"compaction is a partner, not an enemy" (`docs/roadmap/README.md`
pillar 7) — the project guarantees gap detection and explicit
recovery, not silent gap traversal.

### Dropped-table policy

A consumer with `tables := ['main.orders']` and then `DROP TABLE
main.orders`:

- The consumer **continues advancing**. Snapshots without
  `main.orders` changes are no-ops (the filter matches nothing);
  snapshots with the `dropped_table` event yield one
  `dropped.table` event from `cdc_ddl` (and zero rows from
  `cdc_changes`).
- `cdc_consumer_stats().tables_unresolved` is updated to
  `['main.orders']` on the next call. This is derived live from
  `tables \ existing-tables-in-catalog-at-current_snapshot()`
  per ADR 0009; nothing is materialised.
- The consumer is **not** killed. Operators who want halt-on-drop
  semantics read the stats and act on them.
- The rename-detection corollary (per
  [Filter composition](#filter-composition)): a rename
  `orders → orders_v2` registers as a `dropped.table` followed by
  an `altered.table` (with `details.old_table_name` /
  `details.new_table_name` populated) — and the consumer's filter
  silently stops matching the renamed table because the new name
  isn't in `tables`. Operators who anticipate a rename should
  add the new name to the filter **before** the rename or accept
  the silent drop. `tables_unresolved` is the post-hoc detection
  signal.

### Catalog-version compatibility

The extension pins to a `ducklake` catalog-schema version range.
The full matrix is in [`docs/compatibility.md`](./compatibility.md).
The promise: we lag DuckLake major-version releases by **at most 4
weeks**; breaking catalog changes get a major-version bump in the
extension. The runtime guard refuses to register the `cdc_*`
functions when the loaded DuckLake's catalog-schema version is
outside the supported range — operators get a structured warning at
`LOAD ducklake_cdc;` time, not a cryptic runtime error mid-poll.

## See also

- [`docs/errors.md`](./errors.md) — every structured error and
  notice this surface raises, with reference messages and recovery
  commands.
- [`docs/security.md`](./security.md) — threat model, identifier
  quoting discipline, multi-tenant isolation guidance.
- [`docs/conventions/outbox-metadata.md`](./conventions/outbox-metadata.md)
  — recommended `commit_extra_info` shape.
- [`docs/operational/audit-limitations.md`](./operational/audit-limitations.md)
  — column-drop-with-DML edge case + recovery recipe.
- [`docs/operational/inlined-data-edge-cases.md`](./operational/inlined-data-edge-cases.md)
  — inline-on-inline delete invisibility + the
  `DATA_INLINING_ROW_LIMIT` default.
- [`docs/compatibility.md`](./compatibility.md) — DuckLake
  catalog-schema version compatibility matrix.
- [`docs/upstream-asks.md`](./upstream-asks.md) — open issues we
  want DuckLake to fix.
- ADR index: [`docs/decisions/`](./decisions/).
