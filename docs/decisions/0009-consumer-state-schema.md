# ADR 0009 — Consumer-state schema: `__ducklake_cdc_consumers`

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

Pillar 3 (`docs/roadmap/README.md`) commits the project to
storing consumer cursor state as a regular DuckLake table inside the
attached catalog — no external state store, no drift, transactionally
consistent regardless of catalog backend. This ADR is the **canonical
schema** for that table. ADRs 0006, 0007, 0008, and 0010 reference its
columns; Phase 1 ships a `CREATE TABLE` statement that matches it
byte-for-byte; bindings (Phase 3 / Phase 4) generate typed models off it.

Drift between this ADR and any of the above is the failure mode
`docs/roadmap/` calls out repeatedly. Defining columns in
multiple places (e.g. ADR 0007 redefining `owner_token`'s type, ADR 0008
adding a new filter column) is exactly the rework Phase 0's dependency
graph is structured to prevent. Hence: this ADR alone names every
column; downstream ADRs *consume* the names but never *re-define* them.

The reference implementation (the prototype implementation lines 82–93)
persists cursor as a single integer in a file written via atomic
rename. That works for one process polling one lake; it does not
scale to multi-consumer / multi-process / lease-protected reads, and it
gives up the "transactionally consistent with the catalog" guarantee
that makes pillar 3 worth committing to. The extension form replaces
the file with this catalog table.

## Decision

### Table definition (canonical SQL)

The extension creates this table on first use inside the attached
catalog. Column names, types, NULL-ability, defaults, and the
`PRIMARY KEY` constraint are normative. Phase 1's `CREATE TABLE`
statement is a verbatim transliteration of this block (only the
backend-specific timestamp / array spelling may differ — see "Type
mapping per backend" below):

```sql
CREATE TABLE __ducklake_cdc_consumers (
    -- Identity
    consumer_name                 VARCHAR     PRIMARY KEY,
    consumer_id                   BIGINT      NOT NULL UNIQUE,

    -- Filter configuration (NULL = no filter; see ADR 0008 for
    -- composition semantics and create-time validation rules)
    tables                        VARCHAR[],
    change_types                  VARCHAR[],
    event_categories              VARCHAR[],

    -- Behaviour configuration
    stop_at_schema_change         BOOLEAN     NOT NULL DEFAULT TRUE,
    dml_blocked_by_failed_ddl     BOOLEAN     NOT NULL DEFAULT TRUE,

    -- Cursor state (advanced atomically by cdc_commit; see ADR 0007)
    last_committed_snapshot       BIGINT,
    last_committed_schema_version BIGINT,

    -- Owner-token lease (single-reader enforcement; see ADR 0007)
    owner_token                   UUID,
    owner_acquired_at             TIMESTAMPTZ,
    owner_heartbeat_at            TIMESTAMPTZ,
    lease_interval_seconds        INTEGER     NOT NULL DEFAULT 60,

    -- Provenance (write-once except updated_at)
    created_at                    TIMESTAMPTZ NOT NULL,
    created_by                    VARCHAR,
    updated_at                    TIMESTAMPTZ NOT NULL,

    -- Extension-reserved free-form storage for clients
    -- (see "What does NOT live here" below — not the audit log,
    --  not failure history, not bag-of-counters)
    metadata                      JSON
);
```

### Per-column rationale

| Column | Owner ADR | Why this column exists |
| --- | --- | --- |
| `consumer_name` | 0009 | User-visible identifier. `PRIMARY KEY`. Stable across `cdc_consumer_drop` + recreate iff the user reuses the name. |
| `consumer_id` | 0009 | Stable BIGINT denormalisation of `consumer_name` for FKs in `__ducklake_cdc_audit` and `__ducklake_cdc_dlq` rows that must outlive a `cdc_consumer_drop`. Assigned `COALESCE(MAX(consumer_id), 0) + 1` inside the create transaction. `UNIQUE` is enforced. **Not** auto-incremented via SERIAL/IDENTITY — see Alternatives. |
| `tables` | 0008 | Filter: NULL = all tables; otherwise hard list of fully-qualified names. No globs / tag selectors in v0.1. Validated at create time against the catalog as of `start_at` (raises `CDC_INVALID_TABLE_FILTER`). |
| `change_types` | 0008 | Filter: NULL = all four; otherwise subset of `{insert, update_preimage, update_postimage, delete}`. Pillar 11. |
| `event_categories` | 0008 | Filter: NULL = both; otherwise subset of `{ddl, dml}`. Drives the `cdc_events` UNION. |
| `stop_at_schema_change` | 0006 | When TRUE (default), `cdc_window` never returns a window crossing a `schema_version` boundary. Pillar 8. |
| `dml_blocked_by_failed_ddl` | 0008 | When TRUE (default), a failed DDL event in `__ducklake_cdc_dlq` blocks subsequent `cdc_window` calls until either the DDL is acked / re-attempted or the consumer is reset. The "schema-as-code" use case (pillar 9) leans on this. |
| `last_committed_snapshot` | 0007 | The cursor. Initialised at create time from the `start_at` argument to `cdc_consumer_create`: `'now'` (default) resolves to `current_snapshot()` at create-transaction time; `'oldest'` resolves to `MIN(snapshot_id) FROM ducklake_snapshot`; `<bigint>` is used verbatim (must exist in `ducklake_snapshot` or `cdc_consumer_create` raises). The resolved value is what's written to this column on insert; `start_at` itself is **not** stored (the resolved snapshot id is the canonical record). The audit row written by `cdc_consumer_create` (ADR 0010) carries the original `start_at` literal alongside the resolved value, for the diagnostic case where an operator wants to know "did the user say `'now'` or pin a specific snapshot?". `cdc_window`'s lower bound is `last_committed_snapshot` (always non-NULL after create; set to the start-time cursor by `cdc_consumer_create`, advanced by `cdc_commit` thereafter). |
| `last_committed_schema_version` | 0006 | Refreshed by `cdc_commit` from `ducklake_snapshot.schema_version` for the committed snapshot id. Used by ADR 0006's schema-bounding logic to detect cross-version windows. |
| `owner_token` | 0007 | UUID generated by the calling connection on first `cdc_window`; held by the connection until `cdc_consumer_force_release` or lease timeout. The single-reader enforcement predicate. |
| `owner_acquired_at` | 0007 | Set when the lease is first acquired by a token; preserved across heartbeats so the holder's wall-clock acquisition time is observable in `CDC_BUSY` errors. |
| `owner_heartbeat_at` | 0007 | Updated on every `cdc_window`, `cdc_commit`, and `cdc_consumer_heartbeat` call from the lease holder. Lease is considered live iff `now() - owner_heartbeat_at < lease_interval_seconds`. |
| `lease_interval_seconds` | 0007 | Per-consumer override of the lease timeout. Default 60. Bindings drive heartbeat cadence at `lease_interval_seconds / 3`. |
| `created_at` | 0009 | Set once by `cdc_consumer_create`; never mutated. |
| `created_by` | 0009 | NULL-able. Populated from `current_user` / `current_role` if the backend exposes it; bindings may override via `cdc_consumer_create(..., created_by => ...)` for service-account attribution. |
| `updated_at` | 0009 | Refreshed by every `cdc_commit` / `cdc_consumer_heartbeat` / lease-extension UPDATE. Operational visibility — `cdc_consumer_list()` exposes it. |
| `metadata` | 0009 | JSON. Free-form for clients (e.g. consumer-binding registers its language + version). The extension never reads it; the extension never writes it after `cdc_consumer_create`. **Not** the audit log; **not** the failure log; **not** a bag of counters. See "What does NOT live here" below. |

### What does NOT live here

These belong to other tables locked by other ADRs. Listing them here
because each was specifically pitched at some point as "just stick it
in `metadata`":

| Concern | Lives in (ADR) | Why not here |
| --- | --- | --- |
| Audit history (`consumer_create`, `consumer_force_release`, `lease_force_acquire`, …) | `__ducklake_cdc_audit` (ADR 0010) | Insert-only, queryable by time range; querying it via `metadata->>'audit_log'` is awful. |
| Failed-event retry queue | `__ducklake_cdc_dlq` (ADR 0010 schema, Phase 2 feature) | DLQ rows have their own PRIMARY KEY for ack idempotence. |
| Per-snapshot lag / throughput metrics | `cdc_consumer_stats()` view (Phase 1) | Computed live from `current_snapshot()` − `last_committed_snapshot`; not stored. |
| `cdc_wait` poll-interval hyperparameters | session settings (`SET ducklake_cdc_*`) | Tuning knobs are connection-scoped, not consumer-scoped. |
| Per-binding "is this consumer healthy" snapshots | client-side (Phase 3 / 4) | Not the extension's concern. |

### Type mapping per backend

The canonical SQL above uses Postgres-flavoured types. The extension
emits the equivalent type per backend at `CREATE TABLE` time:

| Canonical | DuckDB | SQLite | Postgres |
| --- | --- | --- | --- |
| `VARCHAR` | `VARCHAR` | `TEXT` | `TEXT` |
| `BIGINT` | `BIGINT` | `INTEGER` | `BIGINT` |
| `BOOLEAN` | `BOOLEAN` | `INTEGER` (0/1) | `BOOLEAN` |
| `INTEGER` | `INTEGER` | `INTEGER` | `INTEGER` |
| `UUID` | `UUID` | `TEXT` (36 chars) | `UUID` |
| `TIMESTAMPTZ` | `TIMESTAMP WITH TIME ZONE` | `TEXT` (ISO-8601 UTC) | `TIMESTAMPTZ` |
| `JSON` | `JSON` (DuckDB JSON extension type) | `TEXT` with `CHECK(json_valid(metadata))` | `JSONB` |
| `VARCHAR[]` | `VARCHAR[]` | `TEXT` (JSON array) | `TEXT[]` |

Phase 1's CREATE TABLE per backend is a mechanical lowering of this
table; the cross-backend reference in `docs/compatibility.md` will
quote it verbatim.

### Index DDL per-backend lowering (project-wide invariant)

`__ducklake_cdc_consumers` does not require any non-PK index in v0.1
(every operational query is keyed on `consumer_name`, which is the
PRIMARY KEY). The audit table (ADR 0010) does carry one composite
index, and any future PR adding an index to either catalog-resident
table follows the same lowering pattern as the type-mapping table
above:

| Canonical | DuckDB | SQLite | Postgres |
| --- | --- | --- | --- |
| `CREATE INDEX <name> ON <table> (<col>, ...)` | identical | identical | identical |
| `CREATE UNIQUE INDEX <name> ON <table> (<col>, ...)` | identical | identical | identical |
| Partial index (`WHERE` clause) | supported | supported | supported |
| Functional index (`(LOWER(col))`) | supported | supported | supported |

The standard B-tree composite index DDL is identical across all three
supported backends in v0.1; partial / functional indexes are not used
by any v0.1 schema. Any future ADR proposing a partial or functional
index must include the per-backend lowering inline (the same discipline
as the type-mapping table). This note is the project-wide answer to
"how do I express an index in a backend-agnostic way?"; downstream
ADRs cross-reference it instead of re-stating the rule.

## Consequences

- **Phase impact.**
  - **Phase 1** ships `cdc_consumer_create`, `cdc_consumer_drop`,
    `cdc_consumer_list`, `cdc_consumer_reset`, `cdc_consumer_heartbeat`,
    `cdc_consumer_force_release`, `cdc_window`, `cdc_commit` against
    these columns. Drift between Phase 1's `CREATE TABLE` and this ADR
    is a Phase 1 bug, not an ADR omission.
  - **ADR 0006** (schema boundaries) consumes
    `last_committed_schema_version` and `stop_at_schema_change`. Does
    not add columns.
  - **ADR 0007** (concurrency + owner-token lease) consumes
    `owner_token`, `owner_acquired_at`, `owner_heartbeat_at`, and
    `lease_interval_seconds`, and locks the canonical UPDATE statement
    that mutates them. Does not add columns.
  - **ADR 0008** (DDL as first-class events) consumes `tables`,
    `change_types`, `event_categories`, and `dml_blocked_by_failed_ddl`,
    and locks the filter-composition rules. Does not add columns.
  - **ADR 0010** (audit log) FKs `consumer_name` and denormalises
    `consumer_id` so audit rows survive `cdc_consumer_drop`. Does not
    add columns to *this* table.
  - **Phase 3 / Phase 4 bindings** generate typed models off this table.
- **Reversibility.**
  - **One-way doors:** column names, column types, the `PRIMARY KEY`
    on `consumer_name`, the `UNIQUE` constraint on `consumer_id`. The
    table is on-disk in user catalogs from `v0.1` onward; renames are
    a `v1 → v2` migration, not a patch release.
  - **Two-way doors:** default values (e.g. `lease_interval_seconds =
    60`), and *adding* new columns (with defaults) in later versions.
    The extension's startup migration story (Phase 2) handles those.
  - **Strictly forbidden:** repurposing an existing column for a new
    meaning. `dml_blocked_by_failed_ddl` always means what ADR 0008
    says it means; if a future feature wants different semantics, it
    gets a new column.
- **Open questions deferred.**
  - **Per-consumer access control / row-level security.** Out of v0.1.
    Operators rely on backend-level GRANTs against `__ducklake_cdc_*`
    tables. Tracked under Phase 2 (catalog matrix work).
  - **YAML/TOML "consumer profile" import.** Out of v0.1. The Phase 5
    polish work decides whether this is a binding feature or a
    `cdc_consumer_create_from_yaml(...)` SQL function.
  - **Soft delete.** `cdc_consumer_drop` does a hard `DELETE`. We do
    not carry a `dropped_at TIMESTAMPTZ` tombstone column in v0.1; the
    audit log is the system of record for "this consumer ever
    existed." Revisit if users ask.
  - **Per-consumer counters / cumulative stats.** Live computation via
    `cdc_consumer_stats()` is the v0.1 answer. Materialising them into
    columns here is a "we measured idle-consumer cost and it's bad"
    optimisation, not a v0.1 problem.
  - **Read-only catalogs.** The extension's consumer-state table
    (`__ducklake_cdc_consumers`) and audit table
    (`__ducklake_cdc_audit`, ADR 0010) require **write access to the
    catalog** — every consumer-create, lease-acquire, heartbeat, and
    commit is a catalog write. A catalog `ATTACH`-ed read-only (a
    common pattern for analytics replicas, or for the
    pyiceberg-shaped consumer-of-a-downstream-lake use case) is not a
    supported configuration in v0.1: the first `cdc_consumer_create`
    will fail at `CREATE TABLE __ducklake_cdc_consumers (...)` with a
    backend-specific permission error. The supported workaround is
    "use a read-write replica of the catalog for the cursor table"
    (the data files can stay read-only on the consumer side; only the
    catalog needs to be writable). v1.0+ may revisit with an
    external-state-store option (a `cdc_consumer_create_external(...,
    state_uri := 'sqlite:///path/to/cursor.db')` style escape hatch),
    but the v0.1 contract is "the catalog is where state lives"
    (`docs/roadmap/README.md`) and that contract requires write
    access. This is documented inline in `docs/performance.md`
    when that file lands in Phase 1; flagging it here prevents an
    early user from finding it via a bug report.

## Alternatives considered

- **Cursor-in-file (the reference implementation, the prototype implementation).**
  Rejected: single-process; no multi-consumer support; no lease; no
  schema-version awareness; not transactional with the catalog (cursor
  and `cdc_event` writes can drift on crash). Pillar 3 is the
  affirmative case; this is the negative case being obsoleted.
- **One table per consumer (`__ducklake_cdc_consumer_<name>`).**
  Rejected: every backend's catalog DDL becomes O(N consumers);
  enumeration requires `information_schema` queries that vary per
  backend; the audit log can't FK to a single table; renaming a
  consumer becomes a `RENAME TABLE`. Aggregate operations
  (`cdc_consumer_list`, lag dashboards) become per-table fan-outs.
- **Composite PK `(consumer_name, consumer_id)`.** Rejected:
  `consumer_name` already uniquely identifies a consumer. Making both
  PK columns muddies which one is the user-facing identifier and lets
  rows exist with mismatched (name, id) pairs after a future rename.
  `consumer_id` is a denormalisation for audit/DLQ FK survivability,
  not a co-identifier.
- **Auto-increment `consumer_id` via `SERIAL` / `IDENTITY`.** Rejected:
  not portable. `SERIAL` is Postgres-only spelling; `IDENTITY` is
  Postgres / DuckDB but with per-backend syntax variants; SQLite
  prefers `INTEGER PRIMARY KEY AUTOINCREMENT`. The
  `consumer_id = COALESCE(MAX(consumer_id), 0) + 1` idiom inside the
  create transaction is identical SQL on every backend and races
  cleanly under DuckLake's snapshot-id arbitration (pillar 1's
  conflict resolution inheritance).
- **Store filter configuration in `metadata` JSON.** Rejected: filter
  composition (`docs/api.md#filter-composition`) requires the
  extension to read `tables` / `change_types` / `event_categories` per
  call; reading via JSON path is per-backend syntax (`->>` vs
  `JSON_EXTRACT` vs DuckDB JSON functions). Typed columns also let
  `cdc_consumer_create` validate filters at create-time, which is
  ADR 0008's `CDC_INVALID_TABLE_FILTER` early-fail story.
- **Use `JSONB` (Postgres) instead of `JSON` for `metadata`.**
  Rejected: not all DuckLake catalog backends have JSONB. `JSON` is the
  canonical column type in this ADR; the per-backend mapping below
  lowers it to the best native JSON type on each backend (DuckDB's
  `JSON` extension type, Postgres' `JSONB`, SQLite's
  text-with-`json_valid` constraint). Phase 1 emits the per-backend
  spelling at `CREATE TABLE` time without changing the SQL surface
  bindings see.
- **Advisory-lock-based concurrency (Postgres `pg_advisory_lock`)
  instead of `owner_token` lease columns.** Rejected in ADR 0007;
  documented here for cross-reference because it would remove the
  four `owner_*` / `lease_*` columns. Advisory locks are
  connection-scoped on Postgres and absent on SQLite / embedded
  DuckDB; recovery from a dead holder requires guessing which session
  held what. The owner-token row column lease is portable, observable
  (`SELECT owner_* FROM __ducklake_cdc_consumers WHERE consumer_name =
  ...`), and recoverable (`cdc_consumer_force_release` writes a single
  audit row).
- **Store `last_committed_snapshot_time` alongside
  `last_committed_snapshot`.** Rejected: derivable via `JOIN
  ducklake_snapshot USING (snapshot_id)` for the diagnostic case;
  storing it duplicates a value DuckLake already owns and creates a
  drift opportunity if `ducklake_expire_snapshots` removes the row.
  `cdc_consumer_stats()` (Phase 1) does the join lazily.
- **Track failure counts (`failed_dml_events`, `failed_ddl_events`)
  here.** Rejected: failure history lives in `__ducklake_cdc_dlq`
  (ADR 0010 schema). Counters here would either drift from the DLQ
  (if updated separately) or be redundant (if rederived on every
  read). `cdc_consumer_stats()` exposes the DLQ-derived counters.

## References

- `docs/roadmap/README.md` — pillar 3 (cursors live in
  the catalog), pillar 4 (read and commit are separate), pillar 5
  (idempotence on holding connection), pillar 6 (one consumer name =
  one logical reader).
- `docs/roadmap/` and
  § "Consumer-state schema" — the long-form discussion these columns
  encode.
- `docs/roadmap/` — the implementation that has to match this ADR
  byte-for-byte.
- the prototype implementation lines 82–93 (`load_cursor` / `save_cursor`)
  — the file-based baseline being obsoleted.
- ADR 0004 — Overlap audit (establishes that `__ducklake_cdc_consumers`
  has no DuckLake builtin to defer to; consumer-state CRUD is "missing
  from DuckLake" and lives here).
- ADR 0006 — Schema-version boundaries (forthcoming; consumes
  `last_committed_schema_version` and `stop_at_schema_change`).
- ADR 0007 — Concurrency model + owner-token lease (forthcoming; locks
  the `owner_token` UPDATE statement that mutates four of these
  columns).
- ADR 0008 — DDL as first-class events (forthcoming; locks the filter
  semantics of `tables`, `change_types`, `event_categories`, and the
  block semantics of `dml_blocked_by_failed_ddl`).
- ADR 0010 — Audit log schema (forthcoming; FK relationship from
  `__ducklake_cdc_audit.consumer_name` and the
  `__ducklake_cdc_dlq.consumer_id` denormalisation).
- DuckLake spec — Snapshot Conflict Resolution (the inherited
  arbitration that makes cross-backend `consumer_id` assignment safe).
