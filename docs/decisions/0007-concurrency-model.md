# ADR 0007 — Concurrency model + owner-token lease

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

Three "parallel" patterns were conflated by earlier drafts of
`docs/roadmap/`. They have distinct correctness
requirements and distinct enforcement costs, and bundling them under one
"concurrency story" is exactly how a v0.1 ships with a silent
last-writer-wins race on the cursor. This ADR separates them and locks
the single-reader enforcement mechanism for v0.1.

The three patterns:

1. **Multiple independent consumers on the same lake.** Each has its own
   row in `__ducklake_cdc_consumers` (ADR 0009). They never interact
   except via DuckLake's snapshot-id arbitration (pillar 1) on writes to
   that table. **Supported in v0.1.** No new mechanism required —
   DuckLake already arbitrates concurrent writes via the conflict-resolution
   path baked into every catalog backend it supports.
2. **Multiple workers sharing one consumer name (work-sharing fan-out).**
   Two workers both call `cdc_window('indexer')` from different
   connections. **Explicitly unsupported in v0.1** (`docs/roadmap/README.md`
   pillar 6, "Non-goals (v0.1)"). The naive behaviour (both workers see
   the same window, both process the same rows, both commit) is wrong.
   The correct answer (a coordinator with leased windows) is a v1.0
   design exercise (`cdc_window_lease(consumer, worker, lease_ms)`). This
   ADR's job is the **enforcement** that turns "explicitly unsupported"
   from a documentation-only discipline into a runtime-rejected request.
3. **Parallel `table_changes` reads within one window driven by one
   orchestrator connection for one consumer.** The reference
   implementation (`the prototype implementation:fetch_all_row_events`) already
   does this — the orchestrator calls `cdc_window` once, fans out
   `table_changes` queries to a worker pool, then commits once at end.
   **Supported in v0.1.** Pillar 5 explicitly carves it out:
   same-connection idempotence on `cdc_window` + reject-other-connection.
   The fan-out workers run plain stateless `SELECT`s; only the
   orchestrator holds the lease.

The earlier per-backend advisory-lock plan (Postgres
`pg_advisory_xact_lock`, etc.) is dropped for two reasons that any one
of which would be fatal:

- **Lifetime mismatch.** `pg_advisory_xact_lock` is transaction-scoped
  and releases when `cdc_window`'s transaction commits — milliseconds
  later — not when the holder eventually calls `cdc_commit`. Two
  consumers on different connections can serially acquire, both think
  they own the cursor, both call `cdc_commit`, and the second silently
  overwrites the first. This is the exact race pillar 6 forbids.
- **Hash collisions and per-backend divergence.** `hashtext(consumer_name)`
  is `int4`; with hundreds of consumers, name collisions block unrelated
  consumers from each other. SQLite has no advisory lock; embedded
  DuckDB has no equivalent; each remaining backend has different
  lifetime semantics. Phase 2 (catalog matrix) would have to paper
  over divergent primitives across DuckDB / SQLite / Postgres —
  exactly the per-backend implementation tax pillar 12 is trying to
  avoid.

The portable design ratified here uses **the consumer row itself** as
the lease, with three columns ADR 0009 already provisions:
`owner_token UUID`, `owner_acquired_at TIMESTAMPTZ`,
`owner_heartbeat_at TIMESTAMPTZ`, plus the per-consumer
`lease_interval_seconds INTEGER`.

## Decision

### What v0.1 supports and what it doesn't

| Pattern | v0.1 status | Enforcement |
| --- | --- | --- |
| Multiple independent consumers on the same lake | **supported** | DuckLake snapshot-id arbitration (inherited; no new code) |
| Same-connection re-call of `cdc_window` until `cdc_commit` (idempotent) | **supported** | The `owner_token = :calling_token` predicate makes the lease UPDATE a no-op |
| Orchestrator fan-out of `table_changes` reads from one connection that holds the lease | **supported** | The fan-out workers run stateless `SELECT`s; only the orchestrator holds the lease |
| Different-connection re-call of `cdc_window` under the same consumer name | **rejected with `CDC_BUSY`** | The owner-token lease (this ADR) |
| Work-sharing across N workers under one consumer name | **rejected with `CDC_BUSY`** (same mechanism as above) | Escape hatch: create N consumers with disjoint table filters |

### The lease columns

Defined canonically in ADR 0009; reproduced here for cross-reference
only — this ADR consumes the columns, it does not redefine them:

```text
owner_token              UUID            -- NULL = no holder
owner_acquired_at        TIMESTAMPTZ     -- when the current holder took the lease
owner_heartbeat_at       TIMESTAMPTZ     -- last heartbeat from the holder
lease_interval_seconds   INTEGER NOT NULL DEFAULT 60
```

If a future ADR proposes to change the type or NULL-ability of any of
these, the change lands in ADR 0009 first; this ADR follows.

### The canonical lease UPDATE (Phase 1 ships this byte-for-byte)

`cdc_window` performs exactly one conditional UPDATE inside its
transaction. **Phase 1's implementation must match this SQL
byte-for-byte** modulo backend dialect for `make_interval` and parameter
markers. Schema drift between this ADR and Phase 1's SQL is the failure
mode `docs/roadmap/` repeatedly calls out for the consumer
schema; the same discipline applies to the lease SQL itself, because
divergent predicates between the design and the implementation are how
the race we're defending against gets re-opened by a later refactor.

```sql
UPDATE __ducklake_cdc_consumers
SET    owner_token        = COALESCE(owner_token, :new_token_for_this_call),
       owner_acquired_at  = CASE WHEN owner_token IS NULL THEN now() ELSE owner_acquired_at END,
       owner_heartbeat_at = now()
WHERE  consumer_name = :name
  AND  (owner_token IS NULL
        OR owner_heartbeat_at < now() - make_interval(secs => lease_interval_seconds)
        OR owner_token = :token_held_by_this_connection)
RETURNING owner_token, last_committed_snapshot, last_committed_schema_version,
          stop_at_schema_change, tables, change_types, event_categories,
          dml_blocked_by_failed_ddl, lease_interval_seconds;
```

Parameter contract:

- `:name` — the consumer name from the `cdc_window` call.
- `:token_held_by_this_connection` — the connection's session-cached
  `owner_token` for this consumer (NULL on the first `cdc_window` call
  from this connection in this session).
- `:new_token_for_this_call` — a freshly-generated UUIDv4, used **only**
  when the row's `owner_token` is currently NULL. The `COALESCE`
  collapses the lease-acquired path to a single statement and keeps the
  acquire+heartbeat atomic.

Behaviour by case:

| Row state at UPDATE time | `:token_held_by_this_connection` | Outcome |
| --- | --- | --- |
| `owner_token IS NULL` | (any) | Lease acquired with `:new_token_for_this_call`. `owner_acquired_at = now()`. RETURNING returns one row. Connection caches the new token. |
| `owner_token IS NOT NULL` and `owner_heartbeat_at` within lease | NULL or `≠ owner_token` | Zero rows. Caller raises `CDC_BUSY`. |
| `owner_token IS NOT NULL` and `owner_heartbeat_at` within lease | `= owner_token` | Lease re-extended (no-op on `owner_token`, `owner_heartbeat_at = now()`, RETURNING returns one row). Idempotent. |
| `owner_token IS NOT NULL` and `owner_heartbeat_at` past lease | NULL or `≠ owner_token` | Lease force-acquired with `:new_token_for_this_call`. `owner_acquired_at = now()`. RETURNING returns one row. Caller writes a `lease_force_acquire` audit row capturing the previous holder's token, acquired-at, and last-heartbeat. |
| `owner_token IS NOT NULL` and `owner_heartbeat_at` past lease | `= owner_token` | Lease re-extended on its own row (the heartbeat self-revives the lease, no force-acquire audit row). Idempotent. |

### The post-acquisition reads

After the lease UPDATE returns one row (i.e., the lease is held by this
connection), `cdc_window` continues, in the same transaction, to:

1. Read `current_snapshot()` for the upper bound of the candidate range.
2. Validate the cursor against `ducklake_snapshot` (gap detection per
   pillar 7) — if `last_committed_snapshot` no longer exists, raise
   `CDC_GAP` with the recovery `cdc_consumer_reset` command.
3. Apply the schema-version boundary lookup per ADR 0006 (per-table-aware
   when `tables IS NOT NULL`, full-snapshot when `tables IS NULL`).
4. Cap the range at `:max_snapshots` and the session-wide
   `ducklake_cdc_max_snapshots_hard_cap` (raises `CDC_MAX_SNAPSHOTS_EXCEEDED`
   per ADR 0011).
5. Return `(start_snapshot, end_snapshot, has_changes, schema_version,
   schema_changes_pending)`.

The single transaction wrapping the lease UPDATE through step 5 is the
TOCTOU defense: a parallel `ducklake_expire_snapshots` cannot interleave
between the lease UPDATE and the existence check at the chosen
isolation level (Phase 1 work item per
`docs/roadmap/`).

### The `cdc_commit` lease assertion

`cdc_commit` is a single conditional UPDATE in its own transaction:

```sql
UPDATE __ducklake_cdc_consumers
SET    last_committed_snapshot       = :snapshot_id,
       last_committed_schema_version = (SELECT schema_version
                                          FROM ducklake_snapshot
                                         WHERE snapshot_id = :snapshot_id),
       owner_heartbeat_at            = now(),
       updated_at                    = now()
WHERE  consumer_name = :name
  AND  owner_token   = :calling_token;
```

If zero rows updated, raise `CDC_BUSY` referring to the new holder
(re-fetch `owner_*` to populate the message). The combined
"assert-lease-and-advance" predicate is what blocks the
silent-last-writer-wins race the advisory-lock plan could not block.
Refreshing `owner_heartbeat_at` here means a successful `cdc_commit`
implicitly extends the lease for the next batch — the common case
("read, commit, read again") never needs an explicit heartbeat call.

### Heartbeat extension

Long-running orchestrators that read for many minutes between commits
call `cdc_consumer_heartbeat(name)`:

```sql
UPDATE __ducklake_cdc_consumers
SET    owner_heartbeat_at = now(),
       updated_at         = now()
WHERE  consumer_name = :name
  AND  owner_token   = :calling_token
RETURNING TRUE;
```

Returns `BOOLEAN` — TRUE if the row was updated (lease still held),
FALSE if it was not (lease lost; `cdc_consumer_heartbeat` does not
raise, the daemon decides whether to fail or retry). Library `tail()` /
`Tail()` helpers spawn a background heartbeat at `lease_interval_seconds
/ 3` cadence regardless of expected batch duration; the always-on cost
is one tiny UPDATE every ~20s on the default 60s lease, far cheaper than
trying to predict whether a batch will overrun half the lease.

### Lease loss

If a holder dies and the lease times out
(`now() - owner_heartbeat_at > lease_interval`), the next caller from a
different connection acquires cleanly via the `owner_heartbeat_at <
now() - make_interval(...)` branch of the lease UPDATE predicate, and
writes a `lease_force_acquire` audit row (ADR 0010). The previous holder
cannot then commit because its `cdc_commit` UPDATE will fail the
`owner_token = :stale_token` predicate. The previous holder learns of
its lease loss only when it next calls `cdc_commit` or
`cdc_consumer_heartbeat`. This is documented in
`docs/operational/lease.md` (Phase 1 doc) so operators do not assume
holders learn about lease loss instantaneously.

### `cdc_consumer_force_release` (operator escape hatch)

```sql
UPDATE __ducklake_cdc_consumers
SET    owner_token        = NULL,
       owner_acquired_at  = NULL,
       owner_heartbeat_at = NULL,
       updated_at         = now()
WHERE  consumer_name = :name
RETURNING owner_token AS previous_token,
          owner_acquired_at AS previous_acquired_at,
          owner_heartbeat_at AS previous_heartbeat_at;
```

The RETURNING clause lets the caller construct the
`consumer_force_release` audit row (ADR 0010) with the pre-release
holder context. The structured `CDC_BUSY` error message points operators
to this command when the holder's heartbeat is older than `2 ×
lease_interval_seconds` (likely dead).

### Default lease interval

**60 seconds.** Per-consumer override via `lease_interval_seconds` on
`cdc_consumer_create`. Session-wide fallback via
`SET ducklake_cdc_lease_interval_seconds = ...` (consulted only when
the consumer's column is NULL — and the column is `NOT NULL DEFAULT 60`,
so this path is reserved for some hypothetical migration story; v0.1
never reads the session setting in practice).

The 60s default is chosen so the heartbeat cadence (`lease_interval / 3
= 20s`) is comfortably above pillar-1 catalog-poll noise but well below
any expected batch latency outside of degenerate workloads. Operators
processing 1M+ row batches over slow sinks raise the per-consumer
interval at create time. The hard cap `lease_interval_seconds <= 3600`
(1h) is a Phase 1 implementation detail to keep the worst-case
"holder died but heartbeat extends silently" window bounded.

### Why this lands in Phase 1, not Phase 5

The advisory-lock plan was deferred because it required per-backend
implementation work and per-backend test matrices across the three
supported backends (and SQLite has no advisory-lock primitive at all).
The owner-token approach is **one UPDATE statement**, portable across
all three backends with zero per-backend code, and survives connection
death gracefully via the heartbeat timeout. The cost reduction is so
large that there is no good reason to ship Phase 1 without it — the
no-enforcement window between `v0.0.x` and `v0.1.0-beta.1` is the
period where users are most likely to hit the race and remember it.

The Phase 1 work item lives in
`docs/roadmap/` and the
test list in § "Single-reader-per-consumer".

## Consequences

- **Phase impact.**
  - **Phase 1** ships the lease UPDATE byte-for-byte as written above,
    plus `cdc_consumer_heartbeat`, `cdc_consumer_force_release`, the
    `CDC_BUSY` error format (also locked in `docs/errors.md`), and the
    `lease_force_acquire` / `consumer_force_release` audit rows
    (ADR 0010). The full single-reader test suite enumerated in
    `docs/roadmap/`
    is gated on this ADR being correct — every test asserts a behaviour
    that this ADR specifies.
  - **Phase 2** (catalog matrix) re-runs the full lease test suite
    against SQLite and Postgres backends. The expected outcome is
    "identical behaviour"; per-backend dialect differences are confined
    to `make_interval` (Postgres / DuckDB native, SQLite needs
    `datetime(..., '+N seconds')`) and parameter marker style. The
    lease *predicate* never changes.
  - **Phase 3 / Phase 4 bindings** add `tail()` / `Tail()` helpers that
    own the heartbeat cadence and surface `CDC_BUSY` as a typed
    exception with the holder context attached (binding-side parses
    the structured error message per `docs/errors.md`).
- **Reversibility.**
  - **One-way doors:** the lease predicate above (drift = race
    re-opened); the `owner_token` semantic of "single-reader
    enforcement" (loosening it would silently break sinks that depend on
    monotonic per-consumer cursors); the `cdc_consumer_force_release`
    NULL-ing of `owner_acquired_at` and `owner_heartbeat_at` (operators
    who script around the audit row depend on the post-release row
    shape).
  - **Two-way doors:** the default `lease_interval_seconds = 60`, the
    heartbeat cadence (`lease_interval / 3`), the choice of UUIDv4 for
    `owner_token`. None of these are part of the public surface and
    can be tuned in a patch release.
- **Open questions deferred.**
  - **Cross-process work-sharing for one consumer name.** The v1.0
    `cdc_window_lease(consumer, worker_id, lease_ms)` design is out of
    v0.1 scope. The escape hatch — "create N consumers with disjoint
    table filters" — is documented in `docs/performance.md`
    (Phase 1) and explicitly named in `docs/roadmap/README.md`. Anyone proposing work-sharing in v0.1 is pointed at this
    ADR; the answer is "two consumers, two cursors."
  - **Lease handoff without heartbeat timeout.** If a holder wants to
    cleanly hand off the lease to another connection without waiting
    for the timeout, v0.1 says "call `cdc_consumer_force_release` then
    let the new holder acquire on its next `cdc_window`." A dedicated
    `cdc_consumer_handoff(name, to_connection_token)` primitive could
    do this in one UPDATE; deferred to v1.0 because no demand.
  - **Lease observability via `cdc_consumer_stats`.** The stats view
    surfaces `owner_token`, `owner_acquired_at`, `owner_heartbeat_at`,
    `lease_interval_seconds`, and a derived `lease_status` column
    (`held` / `expired` / `unowned`). Phase 1 work item.

## Alternatives considered

- **Per-backend advisory locks (Postgres `pg_advisory_xact_lock` /
  `pg_advisory_lock`, SQLite serial transactions, embedded DuckDB
  single-writer).** Rejected for the lifetime-mismatch and
  per-backend-divergence reasons in Context above. The fatal one is
  the lifetime mismatch on `pg_advisory_xact_lock` — the lock
  releases at the end of `cdc_window`'s transaction, *not* at
  `cdc_commit`'s, leaving an unbounded window during which two
  connections can serially acquire and both commit. Session-scoped
  advisory locks (`pg_advisory_lock`/`pg_advisory_unlock`) avoid the
  lifetime issue but introduce connection-leak surface (a holder that
  crashes without unlocking blocks the consumer until the connection
  times out, which is configured in postgresql.conf, not by us) and
  still leave SQLite needing a different primitive.
- **Optimistic concurrency control on `last_committed_snapshot` only
  (no `owner_*` columns).** `cdc_commit` would `UPDATE … WHERE
  last_committed_snapshot = :previously_observed_value`. Rejected:
  works for the cursor advance but does not prevent two connections
  from both calling `cdc_window` and reading the same window
  (rendering pillar 5's same-connection-idempotence contract
  meaningless when both holders think their idempotence applies).
  Also: it tells the loser of the race "the cursor moved" but cannot
  explain *who* moved it — operators end up grepping logs to figure out
  which other process was running. The lease columns make the holder
  observable in the consumer row itself.
- **Connection-scoped DuckDB session variables instead of a per-row
  `owner_token`.** Couldn't survive connection death cleanly (the next
  connection has no way to know the lease is stale without a
  catalog-resident timestamp), and would require per-backend handling
  of the session-variable storage. The catalog-resident lease is one
  schema for all three supported backends.
- **Two-phase commit / external lock service (etcd, ZooKeeper).**
  Rejected as a non-goal: pillar 3 says state lives in the catalog,
  not in an external service. An external lock service would re-introduce
  the operational burden Debezium has and the project explicitly
  positions away from (`docs/roadmap/README.md`).
- **Tying the lease to the DuckLake snapshot system itself.** Tempting
  because it would inherit DuckLake's conflict resolution. Rejected:
  the lease is **about the consumer cursor, not lake data**, and writes
  to the consumer-state table are arbitrated by DuckLake already
  (pillar 1) — which is what blocks the OS-level "two `cdc_commit`s
  with the same predicate succeed" race. The owner-token lease is
  exactly the per-consumer-name semantic layer on top of that, and
  there is no DuckLake primitive that does it.

## References

- `docs/roadmap/` — the long-form discussion this ADR locks down.
- `docs/roadmap/`,
  § "Primitive: `cdc_commit`", § "Single-reader-per-consumer",
  § "TOCTOU defense" — the implementation work items that consume this
  ADR.
- `docs/roadmap/README.md` — pillar 5 (idempotence on
  holding connection), pillar 6 (one consumer name = one logical
  reader); § "Non-goals (v0.1)" (work-sharing across multiple workers
  is explicitly excluded).
- ADR 0009 — Consumer-state schema (defines `owner_token`,
  `owner_acquired_at`, `owner_heartbeat_at`, `lease_interval_seconds`).
- ADR 0010 — Audit log (defines `lease_force_acquire`,
  `consumer_force_release` action enum values).
- ADR 0006 — Schema-version boundaries (the per-table-aware schema
  version lookup that runs inside the same `cdc_window` transaction as
  the lease UPDATE).
- ADR 0011 — Performance model (`max_snapshots` hard cap consulted
  inside the same transaction).
- `docs/errors.md` § `CDC_BUSY` — reference message format.
- `the prototype implementation:fetch_all_row_events` — reference
  implementation of the orchestrator-fan-out pattern that this ADR's
  same-connection idempotence rule keeps working.
- DuckLake spec — Snapshot Conflict Resolution (the inherited
  arbitration that arbitrates concurrent writes to
  `__ducklake_cdc_consumers` itself).
