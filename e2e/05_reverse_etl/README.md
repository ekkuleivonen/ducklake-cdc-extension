# 05 &mdash; Reverse ETL (DuckLake &rarr; Postgres serving)

> *"OLAP-to-OLTP, no separate vendor."*

A consumer that reads CDC from a DuckLake table and keeps a row-shaped copy
in a Postgres serving database via `INSERT ... ON CONFLICT DO UPDATE` and
`DELETE WHERE id IN (...)`. The serving Postgres is a separate database
from the catalog Postgres &mdash; the example treats it as an external OLTP
target the application reads from.

This is the "reverse ETL" category that vendors like Hightouch and Census
have built businesses on: take analytical data, push it to operational
systems where users actually interact with it. We do it as a 200-line
script on top of CDC primitives.

## Why this is distinct from example 03 (publish-Redis)

03 publishes to a **log-shaped sink** (Redis Streams, Kafka): append-only,
ordering preserved, idempotency via entry IDs. Each change becomes one
stream entry forever.

05 publishes to a **table-shaped sink** (Postgres): updates collapse,
deletes remove rows, primary-key conflicts must be resolved. The sink
contract is fundamentally different: idempotency is via the destination
PK, not via an ever-growing event ID.

Same CDC reader; different sink semantics. Worth a separate example
because the sink-side code is non-trivial and the failure modes are
different.

## API surface used

- `cdc_dml_consumer_create` &mdash; one consumer per source table
- `cdc_window`, `cdc_dml_changes_read`, `cdc_commit` &mdash; standard loop
- DDL consumer **not** used by default in this example. A schema change to
  the source table requires operator action on the destination Postgres
  schema; the README explains the pattern, but the example doesn't ship
  automatic schema migration to OLTP. (This is the right place to *not*
  over-promise; OLTP migrations are scary.)

The serving-side write is one transaction per CDC window, batched:

```sql
BEGIN;

INSERT INTO orders (id, status, total, updated_at)
VALUES (...), (...), (...)
ON CONFLICT (id) DO UPDATE SET
    status = EXCLUDED.status,
    total = EXCLUDED.total,
    updated_at = EXCLUDED.updated_at
WHERE orders.snapshot_id < EXCLUDED.snapshot_id;  -- monotonic guard

DELETE FROM orders WHERE id = ANY(ARRAY[...]);

COMMIT;
```

The `WHERE orders.snapshot_id < EXCLUDED.snapshot_id` guard is the
idempotency mechanism: re-applying a window after a crash is safe because
older snapshot data won't overwrite newer rows.

## Demo visualization

Bespoke. Two tables side by side, same row IDs, watch them stay in sync:

```
   ── DuckLake.orders                   ── serving.orders (Postgres)
   id     status   total    snap        id     status   total    snap
   42100  paid     120.00   4218        42100  paid     120.00   4218
   42101  new       89.50   4220        42101  new       89.50   4220
   42102  shipped  205.00   4221        42102  shipped  205.00   4221
   42103  paid      45.00   4222        42103  paid      45.00   4222

   sync lag p50 14 ms  ·  p99 38 ms  ·  divergence 0 rows
```

The killer demo: kill the consumer mid-window, watch the divergence column
go non-zero, restart the consumer, watch it converge back to zero. Then
write a row to the OLTP serving table directly (simulate operator drift),
show that the consumer's monotonic guard prevents the source from
overwriting the operator's intent (and explain why that *is* the right
behavior &mdash; the source of truth is the lake, but the guard prevents
silent regressions).

## Acceptance criteria

In addition to the suite-wide criteria:

- [ ] Sustains 5,000 row-updates/sec into the serving Postgres for a
      5-minute run; sync-lag p99 &le; 200&nbsp;ms.
- [ ] After a forced consumer kill+restart, the serving table converges
      to bit-identical state with the source within one CDC window of
      the next write; no manual intervention required.
- [ ] Reapplying a window (simulated by hand-setting the cursor backward
      one snapshot) does **not** corrupt the serving table; the
      monotonic guard prevents older snapshots from overwriting newer
      ones, and the CDC commit advances cleanly.
- [ ] The `divergence` count in the live UI hits non-zero during the
      kill window and returns to zero within 5 seconds of restart.
- [ ] The reusable sink contract from example 03 is reused unchanged for
      the at-least-once delivery + idempotency-key shape, even though
      the serving-side write strategy is different.

## Talk story

> "We're going to keep an OLTP serving table in sync with a DuckLake
> analytical table. Updates flow live. Now I'll kill the sync. Watch the
> divergence climb. I'll bring it back. Watch it converge. No manual
> reconciliation, no Hightouch, no Census."

The kill-and-recover demo is the punch. Steady-state sync is unsurprising
(a hundred vendors do it); recovery without manual intervention is what
sells the pattern.

## Open questions

1. **Schema migration to OLTP.** Out of scope for this example, but
   the README should sketch the recommended pattern: capture the DDL
   event in a separate consumer (like example 04), run an operator-
   approved migration on the OLTP side, then resume the sync at the
   schema boundary.
2. **Hard deletes vs. soft deletes on the serving side.** Some teams
   want `DELETE` from CDC to set `deleted_at = now()` on the serving
   row instead of removing it. Probably configurable per consumer
   (a callback on the delete code path) but default to hard delete.
3. **Cross-database transactions.** We can't atomically commit a CDC
   cursor advance and a Postgres write across databases without a
   2PC. We accept at-least-once over the wire and rely on the
   monotonic guard for idempotency. Document this clearly so no one
   thinks they're getting a distributed transaction.

## Running this example

```bash
docker compose -f e2e/docker-compose.yml up -d --wait   # adds serving-postgres service
make release

# default: side-by-side OLAP vs OLTP tables, kill+converge demo
uv run --project e2e python e2e/05_reverse_etl/app.py

# CI: same workload, no TUI, asserts + writes metrics JSON
uv run --project e2e python e2e/05_reverse_etl/app.py --headless --catalog postgres
```

Catalog support: `postgres` (typical multi-process deployment), `sqlite`
(WAL multi-process), `duckdb` only when `--in-process` is set.

Storage: `--storage disk` (default) or `--storage s3`.

## Status

TODO. Build last. Requires a secondary Postgres service in
`e2e/docker-compose.yml`; add it alongside this example.
