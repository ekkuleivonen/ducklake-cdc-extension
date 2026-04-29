# ADR 0010 — Audit log: `__ducklake_cdc_audit`

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

Operators of CDC pipelines need a system-of-record for "who did what to
which consumer, when": who created a consumer, who force-released a
lease, when an automatic lease force-acquire fired (and who got
pre-empted), when a DLQ overflow halted a consumer. Without a dedicated
record, this information either lives in operator-side log aggregation
(which is fragile, requires log-search infrastructure, and disappears
on log rotation) or in the consumer row itself (which means the row
grows unbounded, every `cdc_consumer_list` query gets slower, and
audit history is silently lost when a consumer is dropped).

Stuffing audit history into `__ducklake_cdc_consumers.metadata` is
specifically wrong:

- **History is dropped when the consumer is dropped.** The "who
  created and dropped the `prod_indexer` consumer last week" trace is
  exactly the trace the post-incident reviewer wants and exactly the
  trace `metadata` deletes.
- **The row grows unbounded for active operators.** A consumer that
  has been force-released and recreated and reset 30 times has a
  `metadata` field that's 50KB and slows every `cdc_consumer_list`
  call.
- **JSON-path queries are per-backend syntax.** Filtering "show me
  every `lease_force_acquire` event in the last 24h across all
  consumers" via `metadata->>'audit_log'` is unwritable in
  backend-portable SQL.

A dedicated table side-steps all three. The schema is small,
insert-only, indexed for the queries operators actually run, and
survives `cdc_consumer_drop` because rows reference the dropped
consumer by name and by the denormalised `consumer_id` (so post-drop
rows still uniquely identify which consumer they describe).

ADR 0009 explicitly carves audit history out of the consumer row's
`metadata` column and points at this ADR. The Phase 1 lease ADR
(ADR 0007) and the Phase 2 DLQ feature both depend on the action enum
and the table being live by the time their flows fire.

## Decision

### Table definition (canonical SQL)

The extension creates this table on first audit-emitting call (which is
typically `cdc_consumer_create`), the same way `__ducklake_cdc_consumers`
is created on first lifecycle call. The schema below is normative;
Phase 1's `CREATE TABLE` matches it byte-for-byte modulo the per-backend
type lowering documented in ADR 0009 § "Type mapping per backend".

```sql
CREATE TABLE __ducklake_cdc_audit (
    audit_id        BIGINT      PRIMARY KEY,   -- monotonic, assigned at insert
    ts              TIMESTAMPTZ NOT NULL,
    actor           VARCHAR,                   -- session user / current_role; NULL = 'unknown'
    action          VARCHAR     NOT NULL,      -- enum below
    consumer_name   VARCHAR,                   -- nullable for system-level events
    consumer_id     BIGINT,                    -- denormalised so post-drop rows still identify
    details         JSON
);

CREATE INDEX __ducklake_cdc_audit_consumer_ts
    ON __ducklake_cdc_audit (consumer_name, ts);
```

The standard B-tree composite index DDL is identical across all three
supported backends in v0.1; the project-wide per-backend index-DDL
lowering rules live in ADR 0009 § "Index DDL per-backend lowering"
rather than being re-stated here.

`audit_id` is assigned via the same `COALESCE(MAX(audit_id), 0) + 1`
idiom used for `consumer_id` in ADR 0009. Backend-portable; races
cleanly under DuckLake's snapshot-id arbitration; never depends on
`SERIAL` / `IDENTITY` whose syntax diverges per backend.

The `(consumer_name, ts)` index serves the doctor command (Phase 2)
and operator-friendly time-range queries
("show me every action against `indexer` in the last hour"). The
table is queried far less often than it is written; one composite
index is enough.

### `action` enum — closed set, locked here

Adding a new `action` value is a minor-version bump for the extension
(forward-compatible: old clients just see an unknown action string and
should pass it through). Renaming or removing a value is a major-version
bump. The closed set:

| `action` | When emitted | `details` shape | Phase |
| --- | --- | --- | --- |
| `consumer_create` | `cdc_consumer_create` succeeds | `{ tables, change_types, event_categories, stop_at_schema_change, dml_blocked_by_failed_ddl, lease_interval_seconds, start_at, start_at_resolved_snapshot }` (a snapshot of the create-time arguments; `start_at` carries the literal the user passed — `'now'` / `'oldest'` / `<bigint>` — and `start_at_resolved_snapshot` carries the BIGINT actually written to `last_committed_snapshot`. ADR 0009 explains why both are recorded) | Phase 1 |
| `consumer_drop` | `cdc_consumer_drop` succeeds | `{ last_committed_snapshot, last_committed_schema_version }` (the cursor state at drop time) | Phase 1 |
| `consumer_reset` | `cdc_consumer_reset` succeeds | `{ from_snapshot, to_snapshot, reset_kind }` where `reset_kind ∈ {explicit, oldest_available, latest}` | Phase 1 |
| `consumer_force_release` | `cdc_consumer_force_release` releases a held lease | `{ previous_token, previous_acquired_at, previous_heartbeat_at }` (the holder context at release time) | Phase 1 |
| `lease_force_acquire` | `cdc_window` acquires a lease whose previous heartbeat had timed out | `{ previous_token, previous_acquired_at, previous_heartbeat_at, lease_interval_seconds }` (the pre-empted holder's context) | Phase 1 |
| `extension_init_warning` | extension load detects a version-incompatibility or other refusal-to-start condition | `{ reason, detail }` (one of: `unsupported_ducklake_version`, `unsupported_backend`, `migration_required`, …) | Phase 1 |
| `dlq_acknowledge` | operator acks a DLQ entry | `{ dlq_id, table_name, snapshot_id, ack_reason }` | Phase 2 |
| `dlq_replay` | operator triggers a manual replay of a DLQ entry | `{ dlq_id, table_name, snapshot_id, replay_outcome }` | Phase 2 |
| `dlq_clear` | operator drops one or more DLQ entries | `{ count, oldest_dlq_id, newest_dlq_id, scope }` (`scope ∈ {single, consumer, all}`) | Phase 2 |
| `consumer_halt_dlq_overflow` | a consumer hits `max_dml_dlq_per_consumer` and halts | `{ dlq_count, max_dlq, last_table }` | Phase 2 |

The `consumer_name` column is **nullable** because
`extension_init_warning` is system-level and not associated with any
specific consumer. Every other action populates `consumer_name` and
`consumer_id`. The `consumer_id` denormalisation is the post-drop
identification story: after `cdc_consumer_drop` deletes the
consumer row, the audit row still says
`consumer_id = 17` so the post-incident reviewer can tell which
consumer the row describes even if a new consumer with the same name
is created later (which would get a different `consumer_id`).

### Insert-only, append-only

The extension never updates or deletes audit rows. Operators clean
up via a documented retention recipe in `docs/decisions/0010-audit-log.md`
(Phase 1 doc):

```sql
DELETE FROM __ducklake_cdc_audit
WHERE  ts < now() - INTERVAL '90 days';
```

The recipe is documented but not automated; operators choose retention
windows. The `(consumer_name, ts)` index serves the recipe efficiently.

### Single-statement insert pattern

Every audit-emitting primitive ships its audit row as part of the same
transaction as the operation. For lifecycle calls this is simple: the
operation runs and the audit row inserts in the same transaction. For
`lease_force_acquire`, the audit row inserts inside the same
transaction as the lease UPDATE (ADR 0007), so a force-acquire that
gets rolled back by some downstream failure does not leave a stale
audit row claiming a force-acquire happened.

The atomicity is non-negotiable: an audit row that exists for an
operation that was rolled back is worse than no audit row, because it
misleads the post-incident reviewer.

### Lazy table creation

The audit table is created lazily on the first audit-emitting call
(usually `cdc_consumer_create` is the first such call in a fresh
catalog). The pattern is the same one ADR 0009 uses for
`__ducklake_cdc_consumers`. The Phase 1 implementation gates the
`CREATE TABLE` on a single `IF NOT EXISTS` — there is no init phase to
fail early on; if the create fails, the audit-emitting primitive that
triggered it surfaces the error.

### `audit_id` assignment

Inside the create transaction:

```sql
INSERT INTO __ducklake_cdc_audit
    (audit_id, ts, actor, action, consumer_name, consumer_id, details)
VALUES (
    (SELECT COALESCE(MAX(audit_id), 0) + 1 FROM __ducklake_cdc_audit),
    now(),
    :actor,
    :action,
    :consumer_name,
    :consumer_id,
    :details
);
```

Same pattern as `consumer_id` in ADR 0009. Rationale (also from
ADR 0009): `SERIAL` / `IDENTITY` syntax diverges across DuckDB / SQLite
/ Postgres, the `COALESCE(MAX + 1)` idiom is identical SQL on every
backend, and concurrent inserts are arbitrated by DuckLake's
snapshot-id mechanism (pillar 1) — last writer to claim `MAX + 1`
wins the row, the loser retries inside the same outer transaction.

For `lease_force_acquire` and `consumer_halt_dlq_overflow`, the audit
row is the **only** thing that can fail in its surrounding transaction;
losing the race and retrying once is acceptable and bounded.

## Consequences

- **Phase impact.**
  - **Phase 1** ships:
    - The `CREATE TABLE` (lazy, on first audit-emitting call) plus the
      single-index DDL.
    - Audit emission for `consumer_create`, `consumer_drop`,
      `consumer_reset`, `consumer_force_release`, `lease_force_acquire`,
      `extension_init_warning`.
    - A documented retention recipe in
      `docs/decisions/0010-audit-log.md`.
  - **Phase 2** adds DLQ-related actions (`dlq_acknowledge`,
    `dlq_replay`, `dlq_clear`, `consumer_halt_dlq_overflow`) plus the
    `cdc_doctor()` helper that reads `__ducklake_cdc_audit` for its
    timeline view. The Phase 2 DLQ feature is gated on the audit table
    being live (which Phase 1 satisfies) — there is no audit-table
    migration in Phase 2.
  - **Phase 3 / Phase 4 bindings** generate typed
    `AuditEvent` / `AuditAction` enums off the action set locked
    here. Drift between this ADR's enum and the binding-side typed
    enum is a Phase 3 / Phase 4 build break.
- **Reversibility.**
  - **One-way doors:** the table name (`__ducklake_cdc_audit`) and
    column shape; the closed `action` enum (renaming or removing a
    value is a major-version bump); the insert-only / append-only
    contract (operators may have built dashboards that assume rows
    never disappear except via the documented retention recipe);
    `consumer_id` denormalisation (post-drop identification depends
    on this column being present and populated).
  - **Two-way doors:** the index strategy (additional indexes can be
    added in patch releases); the `details` JSON shape (additive
    fields are minor; removing a field from a known-action shape is
    major); the retention recipe specifics (operators choose their
    retention window; the recipe is a doc, not an enforced policy).
- **Open questions deferred.**
  - **Per-row PII redaction in `details`.** The `details` payload may
    include user-supplied configuration (`tables`, `actor`). v0.1 does
    not redact; operators with PII concerns can transform the audit
    table via the same retention recipe pattern. v0.2 may add a
    redaction hook if there is demand.
  - **Cross-consumer audit views.** A `cdc_audit_recent()` table
    function that returns the last N audit rows pretty-printed is a
    candidate for Phase 1 polish or Phase 2; the `cdc_doctor()`
    command in Phase 2 partially covers it. Tracked in Phase 2.
  - **Audit row export to external sinks.** Operators may want audit
    rows shipped to their existing log aggregation. v0.1 says "set
    the ducklake_cdc consumer to subscribe to
    `__ducklake_cdc_audit` itself with `event_categories := ['dml']`
    and a Sink of choice." Documented in
    `docs/decisions/0010-audit-log.md`. No new primitive required.
  - **Schema migration for the audit table itself.** If we ever add a
    new column to `__ducklake_cdc_audit`, the Phase 2 catalog
    migration story handles it the same way it handles
    `__ducklake_cdc_consumers` evolution. Adding a column with a
    default is forward-compatible; renaming or dropping is a major
    version bump.

## Alternatives considered

- **Stuff audit history into `__ducklake_cdc_consumers.metadata`.**
  Rejected for the three reasons in Context above — drops on
  consumer-drop, grows the row unboundedly, JSON-path queries diverge
  per backend.
- **One audit table per consumer
  (`__ducklake_cdc_audit_<consumer_name>`).** Rejected for the same
  reason ADR 0009 rejected one consumer table per consumer:
  enumeration becomes O(N consumers); cross-consumer queries
  ("show me every `lease_force_acquire` in the last hour") become
  per-table fan-outs; consumer rename becomes a `RENAME TABLE`
  cascade; the doctor command's timeline view becomes unwritable.
- **A dedicated `dlq_event` table, separate from the audit table.**
  Considered; rejected as scope creep for v0.1 / v0.2. Audit rows
  *describing* DLQ operator actions belong here. The DLQ rows
  themselves (the failed events with retry state) live in
  `__ducklake_cdc_dlq` (Phase 2 work item, separate ADR if needed).
  The split keeps the audit table small and keeps the DLQ table
  shaped around its own concerns (retry counts, last-error,
  etc.).
- **Open `action` enum (any string allowed).** Rejected: the closed
  set is what makes binding-side typed enums viable (Phase 3 / 4) and
  what lets the doctor command pre-canonicalise actions. A
  forward-compatible "see also: extension version" pattern (clients
  pass through unknown actions) is the right pattern for evolution
  *and* the closed set; we get both.
- **Composite primary key `(audit_id, ts)`.** Rejected: `audit_id`
  alone is monotonic and unique; adding `ts` to the PK adds nothing
  except making the index larger. The `(consumer_name, ts)` index is
  the operational-query index and is enough.
- **Use UUID for `audit_id` instead of monotonic BIGINT.** Rejected:
  monotonic BIGINTs make the doctor command's "show me events 100
  through 200" pagination trivial and make the table size predictable;
  UUIDs would require an extra timestamp index for any
  operationally-useful query and would inflate row size by ~12 bytes
  for no benefit. UUID is the right choice for `owner_token`
  (per-connection, security-relevant, non-guessable) and the wrong
  choice for `audit_id` (per-row, auditing, ordered).

## References

- `docs/roadmap/`
  — the long-form discussion this ADR formalises.
- `docs/roadmap/` § "Extension
  lifecycle and state" — the implementation work items that consume
  this ADR.
- `docs/roadmap/` — the DLQ
  feature work item that adds `dlq_*` and `consumer_halt_dlq_overflow`
  audit emissions.
- ADR 0007 — Concurrency model + owner-token lease (defines the
  `lease_force_acquire` and `consumer_force_release` events; this
  ADR locks the action names and `details` shapes).
- ADR 0008 — DDL as first-class events
  (`dml_blocked_by_failed_ddl` semantics; the Phase 2 DLQ flows that
  feed `dlq_*` audit events come from the failed-DDL path).
- ADR 0009 — Consumer-state schema (defines `consumer_id`
  denormalisation that survives `cdc_consumer_drop`; defines the
  `metadata` column carve-out that points at this ADR).
- ADR 0011 — Performance model (the `max_dml_dlq_per_consumer` cap
  that triggers `consumer_halt_dlq_overflow`).
- `docs/decisions/0010-audit-log.md` (Phase 1 doc) — the retention recipe
  and the "subscribe to the audit table itself" pattern.
