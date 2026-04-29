# ADR 0008 — DDL as a first-class event stream

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

DuckLake exposes change information through two related but **distinct**
surfaces:

1. **`ducklake_snapshot_changes.changes_made`** — a `VARCHAR` column on
   the catalog metadata table. Per spec, the documented values are a
   comma-separated list of singular `key:<id-or-name>` tuples like
   `created_table:"main"."orders"`, `inserted_into_table:2`,
   `dropped_view:5`. `changes_made` does **not** contain
   `inlined_insert` / `inlined_delete` keys (those exist only on the
   typed MAP surface).

2. **`<lake>.snapshots().changes`** — a typed `MAP` column returned by
   the DuckLake `snapshots()` table function. Keys are
   plural-past-tense (e.g. `tables_created`, `tables_inserted_into`,
   `tables_altered`) and values are arrays. The reference
   implementation (the prototype change-key probe) hard-coded
   four keys it observed in the wild; the spec only exhaustively
   documents three. The Phase 0 upstream probe (`test/upstream/enumerate_changes_map.py`)
   replaced the hard-code with an empirical enumeration committed to
   `docs/api.md#snapshots-changes-map-reference`.

The discovery layer of `cdc_ddl` reads the typed MAP — it's the higher-fidelity
surface and is what `docs/roadmap/README.md` pillar 2
calls "discovery via `snapshots()`." But the upstream probe produced **two
empirical findings that contradict the spec text** and are the most
load-bearing outputs of Phase 0. They are recorded in the upstream probe output
(`docs/api.md#snapshots-changes-map-reference` "Findings worth flagging")
and they lead this ADR rather than being buried as footnotes.

`table_changes()` is a DML-only function. There is no DuckLake-provided
equivalent for DDL — yet downstream sinks frequently need to react to
schema lifecycle events (a Postgres mirror needs `CREATE TABLE` on its
side; a search index needs to provision; a schema registry needs the
new shape). This is the gap `cdc_ddl` fills, and it is the gap that
makes the project's "10 primitive callables vs 5 sugar" ratio (ADR 0004)
hold — a sugar-only library cannot do typed DDL extraction in
one-line compositions.

## Decision

### The two empirical findings the upstream probe proved (lead with these)

These are the upstream probe's most valuable output and are the reason
`cdc_ddl`'s stage-2 extractor exists. Any future PR that proposes to
"simplify" the extractor by removing one of these special cases is
rejected by reference to this section.

#### Finding 1 — Table renames surface as `tables_created`, not `tables_altered`

The spec note (and an earlier draft of `docs/roadmap/`)
said `RENAME TABLE` folds into `altered_table:<id>`. **Empirically
(DuckDB 1.5.2 / DuckLake `415a9ebd`, all three working backends), it
does not.** An `ALTER TABLE x RENAME TO y` produces a snapshot whose
`changes_made` text is `created_table:"<schema>"."y"` and whose
`snapshots().changes` MAP has `tables_created: ['<schema>.y']` —
exactly as if a fresh table had been created.

The underlying `__ducklake_metadata_<lake>.ducklake_table` row preserves
`table_id` across the rename: the old row gets `end_snapshot` set, a
new row with the **same `table_id`** and the new name is inserted with
`begin_snapshot` set. This is what makes detection possible at all.

**Stage-2 extraction rule (locked here, ships in Phase 1):** when
stage-1 sees `tables_created: [<qualified_name>]`, stage-2 looks up
the `table_id` and queries `ducklake_table` for any prior row with the
same `table_id` and a now-set `end_snapshot`. If found, the event is
emitted as `altered.table` with
`details = {old_table_name, new_table_name, ...all-other-altered-arrays-empty}`,
**not** `created.table`. The detection is one query and is exact —
`table_id` reuse does not happen for any other operation.

Column renames (`RENAME COLUMN`) DO surface as `tables_altered` /
`altered_table:<id>` and require no special-case handling — they are
detected via the standard "two `ducklake_column` rows with the same
`column_id` and different names" diff in stage 2.

This finding is non-negotiable. Without the upstream probe, `cdc_ddl`'s stage-2
would have looked for `tables_altered:<id>` for every rename and
produced wrong DDL events for every table rename forever.

#### Finding 2 — `compacted_table` (text) ↔ `merge_adjacent` (MAP)

The spec documents the `changes_made` text key as `compacted_table:<id>`
for `ducklake_merge_adjacent_files`. **The typed MAP uses
`merge_adjacent: [<id>]`.** The text and MAP keys disagree on this
operation alone, and the upstream probe confirmed all three working backends
emit the MAP-side spelling.

**Stage-1 filter rule (locked here):** `cdc_events` and any internal
"is this snapshot maintenance-only?" check must look for **both
spellings** — `merge_adjacent` on the MAP side and `compacted_table`
on the text side — when filtering or attributing the maintenance
category. `cdc_ddl` does not emit maintenance events at all (operators
read `cdc_events` for those), but the filter SQL inside `cdc_events`
sugar must handle both spellings.

If a future DuckLake version unifies the spelling, the upstream probe's
recurring-CI gate catches the change (the MAP key set diverges from the
committed reference) and the diff message points the operator at this
ADR's filter list. The handling cost of "MAP key changed" is one-line
edit; the handling cost of "MAP key changed and we didn't notice" is
weeks of misclassified operator dashboards.

### The categorisation `cdc_ddl` uses (independent of surface)

Independent of the underlying spelling on either surface, the
*categories* the consumer cares about are:

- **DDL** — `created_*`, `altered_*`, `dropped_*` for `schema`, `table`,
  `view`. RENAME folds into `altered` per Finding 1's stage-2 rule.
- **DML** — `tables_inserted_into`, `tables_deleted_from`, plus the
  inlined variants `inlined_insert`, `inlined_delete`. (DML is the
  domain of `cdc_changes`, not `cdc_ddl`.)
- **Maintenance** — `merge_adjacent` (MAP) / `compacted_table` (text).
  Excluded from `cdc_ddl`. Operators see it via `cdc_events`.

### The `cdc_ddl` table function

Ship `cdc_ddl(consumer, max_snapshots := 100)` as a Phase 1
**primitive** (not sugar). Returns one row per typed DDL event in the
consumer's window:

| column | type | notes |
| --- | --- | --- |
| `snapshot_id` | `BIGINT` | the snapshot the DDL happened in |
| `snapshot_time` | `TIMESTAMPTZ` | wall-clock commit time of the snapshot (sourced from `ducklake_snapshot.snapshot_time`). Carried in the row so operators can render audit timelines without an extra JOIN against `<lake>.snapshots()`. The cost is one column-projection in the inner query against `ducklake_snapshot`; ergonomic gain is large, additional read cost is zero. |
| `event_kind` | `VARCHAR` | one of `created`, `altered`, `dropped`. Per Finding 1, RENAME = ALTER. There is no separate `renamed` kind. |
| `object_kind` | `VARCHAR` | one of `schema`, `table`, `view` |
| `schema_id` | `BIGINT` | NULL for `event_kind = created` of a schema (the schema doesn't have an id yet, or rather the id is on the new row that this event describes — `schema_id` here means the *containing* schema for tables/views) |
| `schema_name` | `VARCHAR` | the qualified-name source-of-truth |
| `object_id` | `BIGINT` | `schema_id` for schema events; `table_id` for table events; `view_id` for view events |
| `object_name` | `VARCHAR` | the *current* (post-event) name. For an `altered.table` event that includes a rename, this is the new name; the old name is in `details.old_table_name`. |
| `details` | `JSON` | shape varies by `(event_kind, object_kind)`; locked below |

**Wall-clock-time consistency across the read surface.** `cdc_changes`
and `cdc_events` (the sugar wrappers, per `docs/roadmap/`
§ "Acknowledged sugar wrappers") project the same `snapshot_time`
column on every output row, sourced via the same JOIN against
`<lake>.snapshots()` that the wrappers already perform for `changes`
map access. The contract is: every row from `cdc_ddl`, `cdc_changes`,
or `cdc_events` carries both `snapshot_id` and `snapshot_time`, so a
downstream sink mixing the three streams can render one timeline
without per-stream JOIN gymnastics. Phase 1 work item: assert this
column is present on all three wrappers' outputs in the test matrix.

Honors the consumer's `event_categories` filter (returns zero rows if
`event_categories = ['dml']`) and `tables` filter (returns only events
for tables in the filter; `created.schema` / `dropped.schema` events are
implicitly suppressed for table-filtered consumers per the
filter-composition rules in `docs/api.md#filter-composition`).

### `details` JSON shapes (locked here, populates `docs/api.md`)

The shapes are normative. Phase 1 emits these shapes byte-for-byte;
bindings (Phase 3 / Phase 4) generate typed models off them. Adding a
key is a minor version bump; renaming or removing a key is a major
version bump.

#### `created.schema`

```json
{}
```

No payload — a schema has no metadata at create time beyond its name,
which is already in the row's `object_name`.

#### `created.table`

```json
{
  "columns": [
    {
      "column_id":         <bigint>,
      "name":              "<column_name>",
      "type":              "<duckdb_type>",
      "default":           "<sql_expression-or-null>",
      "nullable":          true,
      "parent_column_id":  <bigint-or-null>
    }
  ],
  "partition_by": ["<column_name>", ...],
  "properties": { "<key>": "<value>", ... }
}
```

`columns[]` is sourced from `ducklake_column` rows whose
`begin_snapshot = <this snapshot>` for the new `table_id`.
`parent_column_id` is non-NULL only for nested struct fields.
`partition_by` may be an empty array. `properties` is whatever
DuckLake stores in `ducklake_tag` rows scoped to this table at this
snapshot (commonly empty).

#### `created.view`

```json
{
  "definition": "<sql_select_statement>"
}
```

The view's `view_definition` from `ducklake_view`.

#### `altered.table`

```json
{
  "old_table_name":   "<schema.name-or-null>",
  "new_table_name":   "<schema.name-or-null>",
  "added": [
    { "column_id": <bigint>, "name": "<n>", "type": "<t>",
      "default": "<expr-or-null>", "nullable": true, "parent_column_id": null }
  ],
  "dropped": [
    { "column_id": <bigint>, "name": "<n>" }
  ],
  "renamed": [
    { "column_id": <bigint>, "old_name": "<n_before>", "new_name": "<n_after>" }
  ],
  "type_changed": [
    { "column_id": <bigint>, "old_type": "<t_before>", "new_type": "<t_after>" }
  ],
  "default_changed": [
    { "column_id": <bigint>, "old_default": "<expr-or-null>",
      "new_default": "<expr-or-null>" }
  ],
  "nullable_changed": [
    { "column_id": <bigint>, "old_nullable": <bool>, "new_nullable": <bool> }
  ]
}
```

A pure table rename surfaces as `{old_table_name, new_table_name}`
populated and **all other arrays empty** — this is the marker shape for
the Finding-1 stage-2 path. A pure column rename (no table rename)
surfaces with `{old_table_name: null, new_table_name: null, renamed:
[{...}]}` and the other arrays empty. Multi-effect alters (e.g.
`ADD COLUMN x INT` + `DROP COLUMN y` in the same `ALTER TABLE`) populate
multiple arrays. `default_changed` and `nullable_changed` are populated
only when the column is otherwise unchanged (no rename, no type change);
otherwise the change is folded into the `renamed[]` / `type_changed[]`
entry to keep one row-per-column-per-snapshot.

#### `altered.view`

```json
{
  "old_definition":   "<sql-or-null>",
  "new_definition":   "<sql-or-null>",
  "old_view_name":    "<schema.name-or-null>",
  "new_view_name":    "<schema.name-or-null>"
}
```

`CREATE OR REPLACE VIEW` (the only `ALTER VIEW`-equivalent DuckDB
supports) surfaces as the dropped+created pair on the MAP side per the
upstream probe's `altered_view` row; stage-2 collapses them into one
`altered.view` event when the same view name reappears within one
snapshot.

#### `dropped.table`, `dropped.view`, `dropped.schema`

```json
{}
```

No payload. The pre-drop shape is recoverable via `tbl AT (VERSION =>
<snapshot_id - 1>)` — operators who need it use the recipe in
`docs/operational/audit-limitations.md`.

### Two-stage extraction (why this is a primitive, not sugar)

**Stage 1 — discovery.** Read `<lake>.snapshots().changes` (the typed
MAP) for the consumer's window. For each snapshot, project the relevant
keys to the candidate event set. The MAP keys consulted (locked from
the upstream probe output committed at `docs/api.md#snapshots-changes-map-reference`):

- `schemas_created`, `schemas_dropped`
- `tables_created`, `tables_altered`, `tables_dropped`
- `views_created`, `views_dropped`

`merge_adjacent` is consulted only for the maintenance-classification
side-table (used by `cdc_events`); `cdc_ddl` ignores it. The
`tables_inserted_into` / `tables_deleted_from` / `inlined_insert` /
`inlined_delete` keys are DML and are also ignored by `cdc_ddl`.

**Stage 2 — typed reconstruction.** For each candidate event, query
the versioned catalog tables to build the typed `details` payload:

- `ducklake_table` — for table/view events, `(table_id,
  begin_snapshot, end_snapshot, table_name, schema_id)`.
  - Finding 1's rename detection: a `tables_created`-emitting snapshot
    with a `table_id` whose prior row has `end_snapshot` set →
    `altered.table` with rename details.
- `ducklake_column` — for table-altered events, the `(column_id,
  begin_snapshot, end_snapshot, name, type, default, nullable,
  parent_column_id)` rows scoped to the affected `table_id`. The diff
  between the row set at `<snapshot - 1>` and `<snapshot>` produces
  `added`, `dropped`, `renamed`, `type_changed`, `default_changed`,
  `nullable_changed`.
- `ducklake_view` — for view events, `(view_id, begin_snapshot,
  end_snapshot, view_definition, view_name, schema_id)`.
- `ducklake_schema` — for schema events.

Every query is bounded by the snapshot range from stage 1; no full
catalog scans. The cost is proportional to the number of events in the
window, not the catalog's total size.

This composition cannot be expressed as one-line sugar over DuckLake
builtins. Hiding the column-level diff in client-side code would mean
every binding (Python, Go, R, future) reimplements the typed extraction
— the cost the extension form is designed to avoid (`docs/roadmap/README.md`
pillar 12, ADR 0005).

### Per-snapshot delivery ordering contract

- **Within a snapshot:** DDL events are delivered **before** DML events.
  Within DDL, sort by `(object_kind, object_id)` deterministically;
  `object_kind` sort order is `schema, view, table` (creates schemas
  first; drops tables before schemas). Within DML, sort by
  `(table_id, rowid)` per ADR 0002.
- **Across snapshots:** strict `snapshot_id` ascending.
- The library (Python / Go / future) enforces this when interleaving
  `cdc_ddl` and `cdc_changes` outputs into a single batch. If a user
  calls them separately and applies them in the wrong order, that is a
  user bug and the docs name it as such (`docs/api.md` § "Delivery
  ordering contract").

### `cdc_schema_diff` is acknowledged sugar over `cdc_ddl`

With `cdc_ddl` as the typed-extraction primitive,
`cdc_schema_diff(table, from_snapshot, to_snapshot)` is a **stateless
inspection helper** equivalent to: "collect every `altered.table` (and
the initial `created.table` if `from_snapshot` precedes table creation)
`cdc_ddl` event for this table between these snapshot bounds, then
merge the `details` payloads into a flat per-column delta." Same
underlying logic, different surface. Lives in `docs/api.md` as sugar.
Reuses the stage-2 extractor from `cdc_ddl`.

`cdc_recent_ddl(since := INTERVAL '1 day', table := NULL)` is the
stateless companion sugar — same extractor, bare snapshot bounds, no
consumer required.

### Event-category filter

Consumers can subscribe to a subset of `{ddl, dml}` via
`event_categories` on `cdc_consumer_create` (column on ADR 0009).
Default `NULL` = both. The filter composes per
`docs/api.md#filter-composition`:

- `event_categories = ['ddl']`: `cdc_ddl` operates normally;
  `cdc_changes` returns zero rows.
- `event_categories = ['dml']`: `cdc_changes` operates normally;
  `cdc_ddl` returns zero rows.
- `event_categories = NULL` or `['ddl', 'dml']`: both operate.

Patterns this enables:

- One schema-watcher daemon (`event_categories := ['ddl']`) updates
  downstream schemas; many DML consumers fan out without worrying about
  schema management.
- An audit pipeline subscribes to DDL only and ships every schema
  change to git / Slack / a registry.

### The unfixable column-drop edge case

```sql
BEGIN;
ALTER TABLE orders DROP COLUMN customer_email;
DELETE FROM orders WHERE id < 100;
COMMIT;
```

`table_changes()` reads the snapshot under the **end-snapshot schema**,
so the deletes come back without `customer_email`. For most sinks
(Postgres mirror, search index) this is correct — apply the DROP, apply
the DELETEs, world is consistent. **For audit sinks that want the
original pre-drop column values, the data is gone from the read path.**
This is a DuckLake property, not a project bug; we cannot fix it.

The recovery path uses time travel:

```sql
-- Pre-drop snapshot (reachable via DuckLake AT VERSION) carries the
-- column. Audit sinks fetch it directly.
SELECT id, customer_email
FROM   orders AT (VERSION => :snapshot_before_drop)
WHERE  id < 100;
```

`docs/operational/audit-limitations.md` (Phase 1 doc) **leads with the
runnable worked example**: the exact `ALTER ... DROP COLUMN; DELETE;`
workload, what `cdc_changes` returns (deletes without the column), the
recovery `SELECT ... AT (VERSION => N)` query, and the JOIN that
reconciles the recovered rows with the delete events. Audit-pipeline
operators are exactly who hits this and who blames the project; give
them the recipe before they reach for the issue tracker.

### Recurring CI gate (Phase 1 implementation, locked here)

The `test/upstream/enumerate_changes_map.py` script is **promoted to a
recurring CI job in Phase 1**, not retired. In CI, the default embedded
backend check runs on every protected branch build. Before changing the
committed reference, maintainers run the full DuckDB + SQLite + Postgres
matrix locally. The script:

- Runs against DuckLake's catalog backends, with DuckDB + SQLite as the
  CI default and Postgres available through `test/upstream/docker-compose.yml`.
- Diffs the produced MAP key set against the committed reference at
  `docs/api.md#snapshots-changes-map-reference`.
- **Fails the build on any diff.**

The diff message is the operator-readable failure: it names the
operation, the old key set, the new key set, the captured_at /
git_sha / platform of the comparing run (per the provenance fields
added to each per-backend JSON), and the path to this ADR for the
classification rules. Operators decide whether to bump the supported
DuckLake floor, add a new key to the extractor, or reject the upstream
change.

Treating a key-set diff as a hard build break is the only mitigation
for the silent-extractor-breakage failure mode: if DuckLake quietly
changes a key between v1.0 and v1.1 and we don't fail the build, the
DDL extractor is silently wrong against the new version forever.

## Consequences

- **Phase impact.**
  - **Phase 1** ships `cdc_ddl` byte-for-byte against the schema and
    `details` shapes above, plus the per-snapshot DDL-before-DML
    ordering contract, plus the recurring CI gate that diffs the upstream probe
    output. The two empirical findings above are the lead test cases:
    (a) every rename test asserts `event_kind = 'altered'` with
    `details.old_table_name` populated, **never** `event_kind =
    'created'`; (b) every `cdc_events` filter test exercises both
    `merge_adjacent` and `compacted_table` spellings.
  - **Phase 2** (catalog matrix) re-runs the upstream probe and the `cdc_ddl`
    test suite against SQLite and Postgres backends. Backend
    divergence on MAP keys (the failure mode the upstream probe's exit-code-2
    case detects) is a Phase 2 release blocker.
  - **Phase 3 / Phase 4 bindings** generate typed `Event` and
    `DdlEvent` models off the `details` shapes locked in this ADR.
    Drift between this ADR's shapes and the binding-side typed models
    is a Phase 3 / Phase 4 build break.
- **Reversibility.**
  - **One-way doors:** the `cdc_ddl` table-function shape (column names
    and types); the `details` shapes per `(event_kind, object_kind)`
    (renaming or removing a key is a major-version bump); the
    DDL-before-DML per-snapshot ordering contract (sinks may rely on
    DDL arriving before they receive their first row under the new
    shape); the rename-detection rule in stage 2 (the project promises
    renames as `altered.table`, not `created.table`).
  - **Two-way doors:** the implementation choice between MAP-key
    discovery and text-key discovery (we picked MAP; could fall back
    to text without changing the public surface, though the upstream probe
    confirms MAP is more reliable); adding new fields to `details`
    payloads (additive minor version bumps).
- **Open questions deferred.**
  - **VARIANT-typed columns in the `details.added[]` shape.**
    DuckLake's `variant` type may carry shredded sub-fields via
    `ducklake_file_variant_stats`. Phase 1 surfaces the surface-level
    type (`type: "variant"`); shredded sub-field exposure is deferred
    to v0.2 if there is demand. Documented in `docs/api.md` as a
    known v0.1 limitation.
  - **Cross-DDL-statement ordering inside one transaction.** A single
    `BEGIN; CREATE TABLE a; CREATE TABLE b; COMMIT;` produces one
    snapshot with two `tables_created` entries. The
    `(object_kind, object_id)` sort within the snapshot gives a
    stable order (`a` and `b` get sequential `table_id`s; the
    smaller wins). This is documented as the contract; it is not
    "the order the SQL was written."
  - **Per-column tag changes.** `ducklake_column_tag` rows have their
    own `begin_snapshot`/`end_snapshot`. Phase 1 does not surface
    column-level tag changes in the `altered.table` `details` payload;
    they are visible via `cdc_events` raw map output. Promote to a
    `tags_added` / `tags_removed` `details` array in v0.2 if there is
    demand from the schema-as-code crowd.
  - **Compacted-table maintenance event surfacing in
    `cdc_consumer_stats()`.** Operators want to see "this consumer's
    window included N maintenance snapshots in the last hour" as a
    counter. `cdc_consumer_stats()` (Phase 1) exposes this; the
    counter is sourced from the same `merge_adjacent` /
    `compacted_table` filter list locked above.

## Alternatives considered

- **Skip `cdc_ddl`; let consumers parse `cdc_events` themselves.**
  Rejected: shifts the typed-extraction burden onto every binding,
  which is exactly the multi-language tax pillar 12 / ADR 0005 is
  trying to avoid. Also: makes the column-drop limitation (`audit
  sinks lose pre-drop data`) much harder to document because
  consumers don't have a typed event boundary to anchor the recovery
  recipe to.
- **Generate one DDL event per affected column / schema object
  instead of one event per `(snapshot, table)`.** Rejected: explodes
  the row count for any `ALTER TABLE ADD COLUMN a, ADD COLUMN b, ADD
  COLUMN c, ...` (DuckDB supports multi-clause ALTER), and forces
  sinks to know how to coalesce N "added column" events back into one
  schema-evolve operation. The `details.added[]` array is the
  natural composite shape; one event per snapshot per altered table
  matches the way schema-evolve sinks (Postgres ALTER, search-index
  reindex) batch their work.
- **Surface RENAME as a separate `event_kind = 'renamed'`.**
  Rejected: contradicts the DuckLake spec (which categorises ALTER
  and RENAME together for conflict resolution) and breaks the
  three-value enum that maps cleanly to every sink's DDL surface.
  The rename is **inside** the `altered` event via `details.old_*` /
  `details.new_*` fields. Sinks that need to detect renames specially
  pattern-match on those fields; sinks that don't need to care.
- **Use the `changes_made` text key for stage-1 discovery instead of
  the typed MAP.** Rejected: parsing comma-separated `key:<id>`
  strings is fragile (table names with `,` in them break it; the spec
  doesn't promise quoting), the text key is missing the inlined-data
  variants entirely, and the upstream probe's all-three-backends-agree
  observation only holds on the MAP side. Stage 2 still consults the
  text key for the `compacted_table` cross-check (Finding 2).
- **Inline the column-drop audit recovery as a built-in
  `cdc_recover_dropped_column(table, snapshot)` primitive.** Rejected
  for v0.1: the recovery is one `SELECT ... AT (VERSION => N)`,
  documented in the operational doc, not a primitive. Adding a
  primitive for it would balloon the surface count without earning
  the surface complexity, and the recipe pattern works for any
  column or pre-snapshot recovery the operator might want — not just
  drops.
- **Defer recurring CI of the upstream probe to Phase 2.** Rejected: the
  silent-extractor-breakage failure mode is highest-impact in Phase
  1 (when the test suite is smallest and operator surface area is
  highest). Phase 1 ships the diff job; Phase 2 extends it to the
  catalog matrix.

## References

- `docs/roadmap/` — the long-form discussion this ADR
  formalises, with the two findings now leading rather than buried.
- `docs/roadmap/` — the
  implementation work item that consumes this ADR's typed shapes.
- `docs/roadmap/README.md` — pillar 9 (DDL as first-class
  event stream); pillar 2 (discovery via `snapshots()`); pillar 12
  (extension as canonical surface; one extractor in C++ / Rust, three
  surface functions: `cdc_ddl`, `cdc_schema_diff`, `cdc_recent_ddl`).
- ADR 0009 — Consumer-state schema (defines `event_categories`,
  `tables`, `change_types`, `dml_blocked_by_failed_ddl`, all of which
  this ADR's filter composition consumes).
- ADR 0006 — Schema-version boundaries (the DDL-first-on-resume
  contract specified there is satisfied by this ADR's
  per-snapshot ordering rule).
- ADR 0007 — Concurrency model (the lease transaction wrapping
  `cdc_window` is the same transaction that bounds the schema lookup
  in ADR 0006; `cdc_ddl` reads happen outside the lease transaction
  on the holding connection).
- ADR 0010 — Audit log (events emitted from the failed-DDL DLQ flow,
  Phase 2; this ADR's `dml_blocked_by_failed_ddl` semantics are
  consumed there).
- `test/upstream/enumerate_changes_map.py` and
  `test/upstream/output/snapshots_changes_map_reference.md` — the empirical
  evidence behind the two findings above. The upstream probe is committed
  alongside this ADR; the recurring CI gate is the Phase 1 work item
  that keeps the probe findings honest as DuckLake evolves.
- `docs/api.md` § `cdc_ddl` `details` payload shapes (populated from
  the shapes locked in this ADR); § "Delivery ordering contract"
  (consumes the per-snapshot DDL-before-DML rule); § "Filter
  composition" (consumes the `event_categories` rules).
- `docs/operational/audit-limitations.md` (Phase 1 doc) — the
  worked-example recovery recipe for the column-drop edge case.
- DuckLake spec — Snapshots, Snapshot Changes, Field Identifiers,
  Schema Versions, Conflict Resolution.
