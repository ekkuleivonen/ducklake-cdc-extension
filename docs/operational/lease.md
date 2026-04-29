# Owner-token lease

The single-reader-per-consumer rule (ADR 0007) is enforced by an
**owner-token lease**: a UUID stored on `__ducklake_cdc_consumers`
that every mutating cdc\_\* call checks against the calling
connection's cached token. The lease is the foundation of correctness
for cdc_window, cdc_commit, cdc_changes, and cdc_ddl: without it, two
processes could each commit different snapshot ids and corrupt the
cursor.

## Lifecycle

1. **First call to cdc_window** on a free consumer (`owner_token IS
   NULL`) writes a fresh UUID to the row, stamps `owner_acquired_at =
   now()` and `owner_heartbeat_at = now()`, and caches the UUID in
   the per-connection token cache (key:
   `(connection_id, catalog, consumer)`).
2. **Subsequent cdc_\* calls on the same connection** find their
   cached token already matches the row and proceed (idempotent
   acquire).
3. **A different connection** calling cdc_window on the same consumer
   while the lease is held and not expired raises `CDC_BUSY` with the
   current holder's token, acquired-at, and last-heartbeat (per
   `docs/errors.md`).
4. **cdc_consumer_heartbeat** updates `owner_heartbeat_at = now()`,
   keeping the lease alive past the next `lease_interval_seconds`
   tick. Bindings drive heartbeats at `lease_interval_seconds / 3`
   cadence by default.
5. **Lease expiry** — when `now() - owner_heartbeat_at >
   lease_interval_seconds`, the next caller from a different
   connection succeeds in `cdc_window`, gets a fresh token, and writes
   a `lease_force_acquire` audit row with the previous holder's
   forensic state. The original holder's next cdc_commit raises
   `CDC_BUSY` because its cached token no longer matches the row.
6. **cdc_consumer_force_release** is the operator escape hatch: nukes
   the lease unconditionally, writes a `consumer_force_release` audit
   row, leaves cursors and filters intact. Use when the holder is
   demonstrably dead and its lease is still alive.

## Default parameters

| parameter | default | configured at | notes |
| --- | --- | --- | --- |
| `lease_interval_seconds` | 60 | consumer-create time, on the row | API-locked but not yet a named param on `cdc_consumer_create` (lands in PR 6); manual UPDATE on the consumer row works in the meantime. |
| Heartbeat cadence | `lease_interval_seconds / 3` (=20s) | binding | Always-on cost: one tiny UPDATE per tick. Detecting "batch will exceed half the lease" reliably is harder than just paying the always-on cost. |
| Lease-expired threshold | `now() - owner_heartbeat_at > lease_interval_seconds` | extension | After this, *any* connection's next cdc_window steals the lease. |
| Likely-dead threshold | `now() - owner_heartbeat_at > 2 * lease_interval_seconds` | extension | Influences the `CDC_BUSY` reference message ("the holder appears dead, run force_release if you know it has died"). |

## Single-reader rule, in detail

The rule is "single *writer* of the cursor", not "single reader of the
data". Workers can read DuckLake's `table_changes()` directly off the
window range the holder captured — see ADR 0007's parallel-reader pattern.
The carve-out matters because parallelism is the natural way to chew
through a wide window on a multi-core machine. Honor the rule at the
boundary (lease holder owns cdc_window + cdc_commit) and you can fan
out arbitrarily inside it.

## Operator playbook

- **"My consumer is stuck (CDC_BUSY)."** Check
  `cdc_consumer_stats('lake', consumer => 'X')`:
  - `lease_alive = true` — someone is heartbeating. Find the process
    via your binding's connection id; do not force_release.
  - `lease_alive = false` and the lease is recent — owner died, the
    *next* cdc_window from any connection will silently take over and
    audit it. Run cdc_consumer_force_release if you can't wait the
    next `lease_interval_seconds`.
  - `lease_alive = false` and the lease is ancient — owner long-dead.
    Force-release without hesitation.
- **"I want to know when the lease was force-acquired."**
  `cdc_audit_recent('lake', consumer => 'X')`. The
  `lease_force_acquire` rows include the previous holder's token and
  heartbeat for postmortem.
- **"The bindings' heartbeat thread is fighting with my batch
  processing."** Lower `lease_interval_seconds / 3` heartbeat cadence
  by raising `lease_interval_seconds` (default 60s, which gives a 20s
  cadence). Counter-tradeoff: a longer lease means a dead process
  takes longer to be detected.

## Tests

The full lease test matrix is in `test/sql/consumer_state.test`
(same-connection idempotence, force-release, force-acquire after
heartbeat lapse) and `test/smoke/lease_multiconn_smoke.py` (the
multi-connection lease handover that's awkward in SQL alone). The
matrix backs the documented contract above.
