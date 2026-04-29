# Catalog Backend Isolation

`ducklake-cdc` stores consumer state in regular DuckLake tables inside the
attached catalog. The extension's exactly-on-commit behavior therefore depends
on DuckLake's catalog transaction semantics, not on a separate state store.

Phase 2 verifies the same Phase 1 contracts on every supported catalog backend:

- `cdc_window` acquires or extends the owner-token lease and computes the
  window inside one transaction.
- `cdc_commit` only advances the cursor when the caller still owns the lease.
- `cdc_consumer_heartbeat` only extends the current holder's lease.
- Snapshot expiry races either produce a consistent window or a structured
  `CDC_GAP`; they must never produce a torn read.

## Backend Notes

### DuckDB Catalog

The embedded DuckDB catalog serializes catalog writes inside the DuckDB process.
The Phase 1 TOCTOU harness covers the relevant interleaving: one connection
opens a window, another expires snapshots, and the first connection's next
window call fails with `CDC_GAP` if its cursor disappeared.

### SQLite Catalog

SQLite-backed DuckLake catalogs are also effectively serialized for writes.
Phase 2 runs the same window, lease, and expiry smoke flows against SQLite to
catch type-lowering issues, especially timestamp comparison precision in the
lease heartbeat predicate.

### Postgres Catalog

Postgres-backed DuckLake catalogs add networked transaction isolation and a
backend connection pool. Phase 2's catalog-matrix smoke fixture starts with
the same user-visible contracts as the embedded backends, then grows into the
deterministic TOCTOU and lease-timeout harnesses.

When a deterministic Postgres interleaving needs to hold a catalog read stable
while another connection attempts expiry, use `SERIALIZABLE` or
`REPEATABLE READ` plus explicit row locks around the catalog rows under test.
Do not replace the owner-token lease with Postgres advisory locks; the lease is
portable state in `__ducklake_cdc_consumers` and must behave the same on all
backends.

## Operator Guidance

Use one logical reader per consumer name. A second connection calling
`cdc_window` for the same consumer receives `CDC_BUSY` while the first holder's
lease is alive. If the holder died, wait for the heartbeat timeout or use
`cdc_consumer_force_release` and inspect `cdc_audit_recent` afterward.

Set snapshot retention so `expire_older_than` exceeds worst-case consumer lag.
If a cursor falls behind retention, `cdc_window` raises `CDC_GAP`; recover with
the `cdc_consumer_reset` command included in the error message.
