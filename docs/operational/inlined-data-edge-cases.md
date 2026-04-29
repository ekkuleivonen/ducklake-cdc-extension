# Inlined-data edge cases

> **Status: Phase 0 deliverable.** Two edge cases that bite consumers
> who reason about DuckLake's inlined-data path naively. Both are
> documented loudly so users don't write logic that assumes
> "empty `cdc_events` ⇒ no work" or "default inlining limit is 100."

DuckLake's "inlined data" optimization keeps small INSERT batches in
catalog tables instead of materialising them as Parquet files. From
the consumer's perspective this is mostly transparent — `table_changes`
still returns the right rows whether they came from Parquet or from
inlined storage. But three places leak through:

1. **The `snapshots().changes` MAP** uses different keys
   (`inlined_insert` / `inlined_delete`) than the regular
   (`tables_inserted_into` / `tables_deleted_from`) DML keys.
2. **The `cdc_events` raw map** can show "empty" snapshots that
   nonetheless yield rows from `cdc_changes`, because of the
   inline-on-inline delete invisibility (covered below).
3. **The `DATA_INLINING_ROW_LIMIT` default is 10**, not 100 — the
   prototype implementation had the wrong value as a code comment, and
   tests inheriting from it will silently miss the inlining boundary.

## Edge case 1: discovery must check `inlined_*` keys

Consumers that filter `snapshots().changes` only on the canonical DML
keys (`tables_inserted_into`, `tables_deleted_from`) will **silently
skip** any snapshot whose only DML was inlined. The prototype
implementation gets this right, and the project commits to not
regressing it (ADR 0002 § "Inlined data").

The discovery rule:

```python
# Pseudocode for the discovery filter (Phase 1 will implement in C++)
DML_MAP_KEYS_TO_CHECK = {
    "tables_inserted_into",
    "tables_deleted_from",
    "inlined_insert",        # easy to forget; do not omit
    "inlined_delete",        # easy to forget; do not omit
}
```

The full key reference (every key the upstream probe has observed) is
committed at
[`docs/api.md#snapshots-changes-map-reference`](../api.md#snapshots-changes-map-reference).
Phase 1's CI gate (per ADR 0008) diffs the upstream probe output on every
build to keep the key set honest as DuckLake evolves.

## Edge case 2: the inline-on-inline delete invisibility

### What the spec says

The DuckLake spec § "Deletion Inlining" notes:

> A delete that targets an inlined-insert row is not recorded as an
> `inlined_delete`; instead, `end_snapshot` is set on the
> inlined-insert row directly.

The implication: from the `snapshots().changes` MAP perspective, this
snapshot looks like there was **no DML at all** — neither
`inlined_delete`, nor `tables_deleted_from`, nor anything else. The
catalog row's `end_snapshot` quietly moves; no DML key appears.

`table_changes()` still returns the right rows (because it reads from
the inlined storage with snapshot ranges), so any sink that goes
through `cdc_changes` sees the delete events normally. But:

### What this means for `cdc_events` consumers

Any consumer that:

1. Reads `cdc_events` to discover "is there work to do?",
2. Filters out snapshots with no DML keys, and
3. Skips calling `cdc_changes` on those snapshots

— silently drops the inline-on-inline deletes. **Don't do step 3.**
Either always call `cdc_changes` for the window's bounds (the cost is
already proportional to the changed rows, not the snapshot count;
the performance principles in `docs/performance.md` point 2), or special-case
the empty-`cdc_events`-but-non-empty-`cdc_changes` outcome in the
sink's "no work this batch" reporting.

### What the upstream probe empirically shows

The Phase 0 upstream probe (`test/upstream/enumerate_changes_map.py`)
attempts an inline-on-inline delete in the
`inlined_delete_inline_on_inline` operation. Per the script's inline
note:

> Phase 0 doc cites a spec note predicting "this snapshot looks like
> there was no DML" because `end_snapshot` is set on the inlined-insert
> row directly. **Empirically (DuckDB 1.5.2 / DuckLake `415a9ebd`)
> this is FALSE for the cross-transaction case** — the delete still
> produces a snapshot with `inlined_delete`. The spec note may apply
> only to single-transaction `INSERT` + `DELETE`; not yet verified by
> the upstream probe.

So:

- **Cross-transaction case** (insert in transaction A, delete in
  transaction B against the inlined row from A): the upstream probe shows
  the delete *does* produce an `inlined_delete` MAP entry — the
  visibility loss is not observed.
- **Same-transaction case** (insert and delete in the **same**
  transaction): the spec note still predicts loss; the upstream probe does
  not currently exercise this path. The conservative consumer-side
  rule (always call `cdc_changes`) holds for both cases regardless
  of which one DuckLake actually loses visibility on.

A future upstream-probe pass should explicitly test the same-transaction
case so the answer can be either confirmed or retracted in this
doc. Until then: **don't trust the MAP-only signal for "is there
DML?".**

### Recovery / detection

If your sink's "no work" log line matters (operations dashboards,
SLA reporting), make `cdc_changes` the source of truth:

```sql
WITH w AS (SELECT * FROM lake.cdc_window('my_consumer'))
SELECT  COUNT(*)                                          AS dml_row_count,
        EXISTS(SELECT 1 FROM lake.cdc_events('my_consumer'))
                                                          AS has_event_row
FROM    lake.cdc_changes('my_consumer', 'main.orders');
```

A row with `dml_row_count > 0` and `has_event_row = false` is a
**diagnostic signal** for the inline-on-inline visibility loss —
log it with the affected snapshot range and treat it as "we found
DML the MAP didn't advertise."

## Edge case 3: `DATA_INLINING_ROW_LIMIT` default is 10, not 100

The prototype implementation had this code comment:

```python
# DATA_INLINING_ROW_LIMIT (default 100, configurable per ATTACH)
```

**That comment is wrong.** The DuckLake spec sets
`DATA_INLINING_ROW_LIMIT = 10` as the default — an order of
magnitude lower. Consumers that copy the reference implementation's
boundary tests with `100` will:

- Insert batches of 11-99 rows,
- Expect them to land in **inlined storage** (per the wrong comment),
- Actually have them land in **Parquet files** (per the real
  default of 10),
- Pass the test (because `cdc_changes` returns the right rows
  regardless), and
- Silently never exercise the inlined-data MAP-key handling code.

The Phase 0 upstream probe sets `DATA_INLINING_ROW_LIMIT = 10` explicitly
(`test/upstream/README.md` § "Caveats") to make the inlining boundary
deterministic across DuckLake versions. **Phase 1 test fixtures and
all downstream tests must do the same.**

The recommended pattern, copy-pasteable into any new test fixture:

```sql
-- At the top of every test that exercises inlined-vs-not boundary
-- behaviour. Re-asserts the spec default explicitly so tests don't
-- silently follow whatever DuckLake's compiled-in default is on the
-- CI worker.
ATTACH 'ducklake:test.duckdb' AS lake (DATA_INLINING_ROW_LIMIT 10);
```

The Phase 5 migration guide (forthcoming) flags this discrepancy
explicitly so users porting test code from the prototype implementation
to the v0.1 surface don't inherit the off-by-an-order-of-magnitude
bug.

## See also

- ADR 0002 — hybrid discovery + read (the
  "discovery must check `inlined_*` keys" rule lives here as a
  decision; this doc is the operational expansion).
- ADR 0008 — DDL as first-class event stream (Finding 1 / Finding 2,
  same "spec disagrees with reality" pattern as edge case 2).
- `docs/api.md#snapshots-changes-map-reference` — the canonical key
  set the upstream probe enumerates.
- `test/upstream/README.md` § "Caveats" — the
  `DATA_INLINING_ROW_LIMIT = 10` discipline applied to the upstream probe
  itself.
- `docs/operational/audit-limitations.md` — sibling doc covering
  the column-drop-with-DML edge case.
- DuckLake spec — Deletion Inlining note; `DATA_INLINING_ROW_LIMIT`
  default.
