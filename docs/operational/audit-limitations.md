# Audit-sink limitations: column-drop with same-snapshot DML

> **Status: Phase 0 deliverable.** This document leads with a
> runnable worked example because audit-pipeline operators are exactly
> who hits this and who blames the project — give them the recipe
> before they reach for the issue tracker
> (`docs/roadmap/`).

## TL;DR

If a single DuckLake commit contains both `ALTER TABLE ... DROP
COLUMN` and `DELETE FROM` (or any DML on the same table), the
`cdc_changes` events arrive **without the dropped column's values**.
For most sinks this is correct (apply DROP, apply DELETEs, the world
is consistent). For audit sinks that need the original pre-drop
values, the data is gone from the read path — but **it is recoverable
via DuckLake time travel**.

This document is the recovery recipe.

## The exact workload

```sql
ATTACH 'ducklake:lake.duckdb' AS lake;

-- Initial state: an orders table with a customer_email column.
CREATE TABLE lake.orders (
    id              INTEGER PRIMARY KEY,
    customer_id     INTEGER,
    customer_email  VARCHAR,    -- the column the audit sink cares about
    total_cents     BIGINT
);
INSERT INTO lake.orders VALUES
    (1, 42, 'alice@example.com', 9999),
    (2, 43, 'bob@example.com',  4999),
    (3, 44, 'carol@example.com', 14999);

-- The problem commit: drop and delete in one transaction.
BEGIN;
    ALTER TABLE lake.orders DROP COLUMN customer_email;
    DELETE FROM lake.orders WHERE id < 100;
COMMIT;
```

After the `COMMIT`:

- The lake has one new snapshot whose `changes_made` is something
  like `altered_table:1,deleted_from_table:1` (or the inlined-delete
  spelling depending on row count).
- `__ducklake_metadata_lake.ducklake_column` shows the
  `customer_email` row with its `end_snapshot` set to this snapshot
  id.

## What `cdc_changes` returns

```sql
SELECT * FROM lake.cdc_changes('audit_consumer', 'main.orders');
```

| `snapshot_id` | `change_type` | `id` | `customer_id` | `total_cents` |
| ---: | --- | ---: | ---: | ---: |
| (new snapshot) | `delete` | 1 | 42 | 9999 |
| (new snapshot) | `delete` | 2 | 43 | 4999 |
| (new snapshot) | `delete` | 3 | 44 | 14999 |

**No `customer_email` column.** This is `table_changes`'s documented
behaviour (per spec § "Field Identifiers"): the read uses the
**end-snapshot schema**, and the column was dropped at that
snapshot. ADR 0006 explains the broader implications and the
`stop_at_schema_change` opt-in that prevents this in the
*window-bounding* sense — but ADR 0006's bounding does **not** help
here because the DROP and the DELETE are in the *same* snapshot, so
no boundary exists between them.

This is a DuckLake property, not a project bug. We cannot fix it
without DuckLake itself preserving pre-drop column data on the read
path, which it deliberately does not.

## What `cdc_ddl` returns (at the same snapshot)

```sql
SELECT * FROM lake.cdc_ddl('audit_consumer');
```

| `snapshot_id` | `event_kind` | `object_kind` | `object_name` | `details` |
| ---: | --- | --- | --- | --- |
| (new snapshot) | `altered` | `table` | `main.orders` | `{"old_table_name": null, "new_table_name": null, "added": [], "dropped": [{"column_id": 3, "name": "customer_email"}], "renamed": [], "type_changed": [], "default_changed": [], "nullable_changed": []}` |

Per ADR 0008's per-snapshot ordering rule, this `altered.table`
event is delivered **before** the three `delete` events from
`cdc_changes`. Bindings interleave correctly via the wrappers; users
who roll their own `cdc_ddl` + `cdc_changes` loop must apply the
order themselves.

## The recovery recipe

The pre-drop column values still exist in DuckLake — you can read
them via time travel against the snapshot **before** the
DROP+DELETE commit. Two queries plus a JOIN:

### Step 1: identify the pre-drop snapshot id

```sql
SELECT snapshot_id - 1 AS snapshot_before_drop
FROM   lake.cdc_ddl('audit_consumer')
WHERE  event_kind  = 'altered'
  AND  object_kind = 'table'
  AND  object_name = 'main.orders'
  AND  json_extract(details, '$.dropped') @> '[{"name": "customer_email"}]'
LIMIT  1;
```

(Replace the JSON predicate with the DuckDB-native equivalent if
the project's binding doesn't expose `@>`; bindings ship a typed
helper in Phase 3 / 4 that flattens this to one line.)

### Step 2: fetch the pre-drop column values for the affected rows

```sql
SELECT id, customer_email
FROM   lake.orders AT (VERSION => :snapshot_before_drop)
WHERE  id < 100;
```

This uses DuckLake's built-in time travel (`AT (VERSION => N)`).
The snapshot **before** the DROP+DELETE commit still has the
`customer_email` column on the read path with the original values.

### Step 3: reconcile against the delete events

```sql
WITH preserved AS (
    SELECT id, customer_email
    FROM   lake.orders AT (VERSION => :snapshot_before_drop)
    WHERE  id < 100
),
deletes AS (
    SELECT *
    FROM   lake.cdc_changes('audit_consumer', 'main.orders')
    WHERE  change_type = 'delete'
      AND  snapshot_id = :snapshot_at_drop
)
SELECT  d.snapshot_id,
        d.id,
        d.customer_id,
        p.customer_email,        -- the recovered, pre-drop value
        d.total_cents,
        d.change_type
FROM    deletes  d
JOIN    preserved p USING (id);
```

The result is a delete event row enriched with the dropped column
value — exactly what the audit sink wants.

## What this recipe assumes (and when it stops working)

- **The pre-drop snapshot is still in the catalog.** If
  `ducklake_expire_snapshots` has run since the DROP+DELETE commit
  with an `expire_older_than` that crosses
  `:snapshot_before_drop`, the recovery snapshot is gone — `AT
  (VERSION => :snapshot_before_drop)` returns an error. **Operators
  with audit pipelines must coordinate `expire_older_than` with the
  audit consumer's lag** (`docs/roadmap/README.md` point 5; `cdc_consumer_stats().lag_seconds`).
- **The column was dropped, not type-changed in place.** A type
  change between A and B is also lossy in the same way — the
  pre-change values exist in `AT (VERSION => :A)`. Adapt the recipe
  by reading the column twice (at A's type and at B's type) and
  comparing.
- **The sink receives the DDL event and the DML events from the
  same snapshot together.** Bindings (Phase 3 / 4) do this
  automatically via the per-snapshot ordering rule (ADR 0008). Users
  rolling their own loop must read `cdc_ddl` and `cdc_changes`
  windowed by the same `(start_snapshot, end_snapshot)` and apply
  DDL events first.

## Why this isn't fixable inside the extension

The extension reads `table_changes` for DML. `table_changes` reads
under the end-snapshot schema. The pre-drop values are in the
producer's data files (the snapshot before the drop preserves them)
but `table_changes` does not surface them — it surfaces values cast
to / projected through the end-snapshot schema, which means a
dropped column is gone.

To "fix" this, the extension would have to either:

1. **Re-read every DML event under the start-snapshot schema and
   diff against the end-snapshot schema** — this would change the
   read semantics for every consumer to optimise the audit case,
   degrading the more-common Postgres-mirror / search-index case
   that explicitly *wants* end-snapshot-schema reads. Rejected on
   surface impact.
2. **Add a `cdc_recover_dropped_column(table, snapshot)` primitive**
   — rejected for v0.1 (ADR 0008 § "Alternatives considered"): the
   recovery is one `SELECT ... AT (VERSION => N)`, documented in
   this operational doc, not a primitive. Adding a primitive would
   balloon the surface count without earning the surface complexity,
   and the recipe pattern works for any column or pre-snapshot
   recovery the operator might want — not just drops.

So: documented limitation + recovery recipe + clear narration of
why we picked this trade-off. We are not promising to fix this; we
are promising not to surprise users.

## See also

- ADR 0006 — schema-version boundaries (the window-level fix that
  prevents *cross-snapshot* DDL+DML mixing; doesn't help with
  *same-snapshot* mixing, which is what this doc covers).
- ADR 0008 — DDL as first-class event stream (the per-snapshot
  ordering rule that delivers DDL before DML; defines the recovery
  hook).
- `docs/api.md#cdc_ddl-details-payload-shapes` — the
  `dropped: [{column_id, name}]` shape used in step 1.
- `docs/operational/inlined-data-edge-cases.md` — sibling doc
  covering the inline-on-inline delete invisibility (different edge
  case, similar "documented loudly" pattern).
- DuckLake spec — Field Identifiers, Time Travel.
