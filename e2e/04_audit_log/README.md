# 04 &mdash; Durable audit log

> *"Every change to every row, queryable forever."*

A consumer that subscribes to all changes (DML and DDL) across one or more
schemas and writes them into a long-lived `audit_events` table inside the
same lake. Once captured, the audit log is queryable like any other DuckLake
table: time-travel a row, list all schema changes in a window, prove
compliance, debug a production incident.

This example is small in code and big in narrative. Compliance, regulated
industries, and platform teams all want this story; few systems give it to
them as a side effect of normal CDC machinery.

## Why this example matters separately from the others

Examples 01, 02, 03, 05 all *consume* changes and do something with them.
This one *retains* them. The shape is different:

- **Append-only.** No upserts, no deletes against the audit table itself.
- **Long retention.** Months or years, not the working set.
- **DDL is first-class.** Most other examples treat DDL as an exception to
  handle; here, schema events are part of the product.
- **Read pattern is point-lookup-by-row-id, not stream tail.** The example
  also ships a small `rewind` tool that demonstrates the read side.

It's the cheapest example to build (no external services, no new extension
work) and the most useful for proving the *durability* posture of the system
to a skeptical audience.

## API surface used

- `cdc_dml_consumer_create` &mdash; one consumer covering all watched tables
- `cdc_ddl_consumer_create` &mdash; **central to this example**; captures
  every `CREATE`/`ALTER`/`DROP` and writes a structured row
- `cdc_window`, `cdc_dml_changes_read`, `cdc_ddl_changes_read`, `cdc_commit`
  &mdash; standard read-and-advance loop
- DuckLake's snapshot system on the read side &mdash; the `rewind` tool
  uses `cdc_dml_changes_read(start_snapshot, end_snapshot)` to reconstruct
  a row's state at a given moment

The audit table schema is intentionally generic:

```sql
CREATE TABLE audit_events (
    occurred_at      TIMESTAMP,
    snapshot_id      BIGINT,
    schema_name      VARCHAR,
    table_name       VARCHAR,
    change_type      VARCHAR,   -- insert | update_preimage | update_postimage | delete | ddl
    pk               JSON,      -- canonical primary-key dict
    before           JSON,      -- nullable; populated for update_preimage / delete
    after            JSON,      -- nullable; populated for insert / update_postimage
    ddl_kind         VARCHAR,   -- nullable; populated for change_type = 'ddl'
    ddl_text         VARCHAR    -- nullable; the textual DDL statement
);
```

JSON keeps the audit table catalog-portable (no per-source-table column
sprawl). Cost: queries are slightly less ergonomic, mitigated by a few
helper views the example ships.

## Demo visualization

Bespoke. A "rewind" UI in the terminal:

```
   audit_log: orders.id = 42100
   ─────────────────────────────────────────────────────────────────────
   2026-05-04 11:42:18.103  insert       status='new'      total=120.00
   2026-05-04 11:43:01.892  ddl          ALTER TABLE orders ADD COLUMN
                                         tax DOUBLE
   2026-05-04 11:43:54.218  update       status='paid'     total=120.00
                                         (pre)             tax=NULL
                                         (post)            tax=9.60
   2026-05-04 11:51:09.776  delete       status='paid'     total=120.00
   ─────────────────────────────────────────────────────────────────────
   3 DML events  ·  1 DDL event  ·  spans snapshots 4172..4198
```

Pick a row, see its full history including the schema change that happened
mid-life. That's the demo.

A second view shows DDL events as a timeline across all watched schemas
&mdash; useful for incident postmortems.

## Acceptance criteria

In addition to the suite-wide criteria:

- [ ] Captures 100% of DML and DDL events across a 10-minute mixed
      workload (driven by the benchmark's `mutation_heavy.yaml`); zero
      dropped events verified by counting against the producer's own
      tally.
- [ ] Schema changes are visible in the audit table within one
      `cdc_commit` of when they happen (not deferred until next DML
      flush).
- [ ] The `rewind` tool reconstructs a row's full history correctly
      across at least one `ALTER TABLE` boundary, including the column-
      addition case (post-ALTER updates show the new column as `after`
      with old rows correctly showing it as `NULL` in `before`).
- [ ] Audit-table size and write rate documented in the README from a
      real run, so an operator can size retention.
- [ ] Headless run passes against `duckdb`, `sqlite`, **and** `postgres`
      catalogs (this example is the same shape regardless of catalog).

## Talk story

> "We did nothing special to add this. It's the DDL consumer + the DML
> consumer + an append-only table. Two hundred lines of Python and your
> compliance officer is happy."

The talk demo runs a mixed workload, then calls the `rewind` tool on a row
the audience picks. Then runs a `DROP TABLE`, shows it in the DDL timeline,
demonstrates the audit table survives even though the source table didn't.

## Open questions

1. **Compaction.** Audit tables grow forever. Do we ship a compaction
   helper (e.g. roll old months into Parquet files outside the lake) in
   this example, or note it as out-of-scope and link to a docs page?
   Probably out of scope; compaction is its own conversation.
2. **Pseudonymization.** Some events contain PII. Add an optional
   transformation hook (`before`/`after` JSON gets passed through a
   user-supplied function before insert), or leave it to the operator?
   Pick one and commit.
3. **DDL text fidelity.** The DDL consumer surfaces structured event
   types (`altered_table`, `added_column`, &hellip;) &mdash; do we also
   capture the original SQL text or reconstruct it from the structured
   form? Original text is more debuggable; reconstruction is more
   normalized. Probably both columns.

## Running this example

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release

# default: rewind-UI TUI (pick a row, see its full history)
uv run --project e2e python e2e/04_audit_log/app.py

# CI: same workload, no TUI, asserts + writes metrics JSON
uv run --project e2e python e2e/04_audit_log/app.py --headless --catalog postgres
```

Catalog support: `duckdb`, `sqlite`, `postgres` &mdash; the audit consumer
is single-process by design, so all three apply.

Storage: `--storage disk` (default) or `--storage s3`.

## Status

TODO. Build fourth. Cheap, no new infra, exercises DDL consumer hard.
