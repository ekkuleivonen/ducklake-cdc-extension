# Structured errors

> Reference messages and recovery commands for every structured error
> (and notice) the extension raises. Locked in Phase 0 so Phase 1
> implements them correctly the first time. Each entry below maps to a
> Phase 0 work item in `docs/roadmap/` or to the ADR named below it.

User-facing error messages are UX. The contract is:

1. **Identifier prefix** — every error message starts with the error
   code (e.g. `CDC_GAP:`). Bindings parse on this prefix; do not
   change it without a major-version bump.
2. **Diagnostic body** — the consumer name, the cursor, and any
   relevant timestamps / tokens, so an operator can diagnose without
   re-querying.
3. **Recovery command** — a copy-pasteable `CALL ...` or `SET ...`
   line that resolves the error, when one exists.

Phase 1 implements every reference message below verbatim. Drift
between this document and what Phase 1 raises is a Phase 1 bug. Adding
a new code is a minor version bump; renaming or removing a code is a
major version bump (bindings parse on the prefix).

| Code | Severity | Source ADR |
| --- | --- | --- |
| `CDC_GAP` | error | (Phase 0 phase doc § "Structured error format"; ADR 0002) |
| `CDC_BUSY` | error | ADR 0007 |
| `CDC_SCHEMA_BOUNDARY` | notice | ADR 0006 |
| `CDC_INVALID_TABLE_FILTER` | error | ADR 0009 (validation at create time) |
| `CDC_MAX_SNAPSHOTS_EXCEEDED` | error | ADR 0011 |
| `CDC_WAIT_TIMEOUT_CLAMPED` | notice | ADR 0011 |
| `CDC_WAIT_SHARED_CONNECTION` | warning | ADR 0011 |
| `CDC_INCOMPATIBLE_CATALOG` | error (call-time) / notice (LOAD-time) | ADR 0001 |

## `CDC_GAP`

Raised by `cdc_window` when the consumer's `last_committed_snapshot`
no longer exists in `ducklake_snapshot` — typically because
`ducklake_expire_snapshots` removed the snapshot range the consumer
was about to read. Compaction is a partner, not an enemy
(`docs/roadmap/README.md`); the gap is detected and recovery is
explicit.

**Reference message:**

```text
CDC_GAP: consumer 'my_session' is at snapshot 42, but the oldest
available snapshot is 100 (snapshots 42-99 were expired by
ducklake_expire_snapshots). To recover and skip the gap:
  CALL cdc_consumer_reset('my_session', to_snapshot => 'oldest_available');
To preserve all events, run consumers more frequently than your
expire_older_than setting.
```

**Required fields in the diagnostic body:**

- consumer name (from the calling `cdc_window` argument)
- the consumer's current cursor (`last_committed_snapshot`)
- the oldest currently-available snapshot id
  (`MIN(snapshot_id) FROM ducklake_snapshot`)
- the recovery `CALL cdc_consumer_reset(...)` line, with the consumer
  name interpolated and `to_snapshot => 'oldest_available'` as the
  recommended argument

## `CDC_BUSY`

Raised by `cdc_window` when a different connection holds the
consumer's owner-token lease (ADR 0007), or by `cdc_commit` /
`cdc_consumer_heartbeat` when the calling connection's cached token
no longer matches the row's `owner_token` (lease lost via timeout or
`cdc_consumer_force_release`).

**Reference message:**

```text
CDC_BUSY: consumer 'indexer' is currently held by token
a1b2c3d4-e5f6-7890-abcd-ef1234567890 (acquired 2026-04-28T12:01:00Z,
last heartbeat 2026-04-28T12:01:42Z; lease interval 60s).
The holder appears alive; wait for it to release, or run
  CALL cdc_consumer_force_release('lake', 'indexer');
if you know it has died.
```

**Required fields in the diagnostic body:**

- consumer name
- the existing `owner_token` (UUID, full)
- `owner_acquired_at` (UTC, ISO-8601)
- `owner_heartbeat_at` (UTC, ISO-8601)
- `lease_interval_seconds`
- conditional liveness hint: when
  `now() - owner_heartbeat_at > 2 × lease_interval_seconds`, replace
  "The holder appears alive; wait for it to release" with "The
  holder's last heartbeat is older than 2× the lease interval; it is
  likely dead."
- the recovery `CALL cdc_consumer_force_release(...)` line, with the
  explicit catalog and consumer name interpolated

The `cdc_commit` / `cdc_consumer_heartbeat` variants of this message
substitute "the calling connection's lease was lost; the current
holder is …" for the first sentence to make clear that the caller
*had* the lease and lost it (vs being told their first call to
`cdc_window` was rejected).

## `CDC_SCHEMA_BOUNDARY`

**Notice, not error.** Emitted via DuckDB `Notice` when a `cdc_window`
call returns `schema_changes_pending = TRUE` (ADR 0006). The notice
gives operators a logged signal that a schema change is pending
without disrupting the window contract.

**Reference message:**

```text
CDC_SCHEMA_BOUNDARY: consumer 'mirror' window ends at snapshot 142
(schema_version 7); the next snapshot (143) is at schema_version 8.
The next cdc_window call will start at snapshot 143; DDL events for
the schema change will arrive first per the DDL-before-DML ordering
contract (ADR 0008).
```

**Required fields:**

- consumer name
- `end_snapshot` of the current window
- `schema_version` of the current window
- the next snapshot id (`end_snapshot + 1`)
- the next snapshot's `schema_version`

This notice is informational; bindings should surface it as a
`Notice` / `Warning` channel entry, not as a thrown exception.

## `CDC_INVALID_TABLE_FILTER`

Raised by `cdc_consumer_create` when `tables` references a
fully-qualified table name that does not exist in the catalog at the
resolved `start_at` snapshot (ADR 0009 § "tables", validated at
create time). Fail-loud-at-create-time, not silently at first-poll
time.

**Reference message:**

```text
CDC_INVALID_TABLE_FILTER: consumer 'orders_audit' was created with
tables := ['main.orders', 'main.ordres'] but 'main.ordres' does not
exist in the catalog at snapshot 42. Either:
  - correct the table name and retry cdc_consumer_create, or
  - omit the missing table; if it is created later, recreate the
    consumer with start_at => <its-creation-snapshot>.
Tables in the lake at snapshot 42:
  main.customers, main.orders, main.payments
```

**Required fields:**

- consumer name (from the create call)
- the full `tables` list as supplied
- the specific names that did not resolve
- the snapshot id at which validation ran (`start_at` resolved to a
  snapshot id; the message uses that id, not the literal `'now'`
  string)
- the closest-existing-table list (capped at the first 20 names so
  large lakes don't produce multi-page errors)

## `CDC_MAX_SNAPSHOTS_EXCEEDED`

Raised by `cdc_window` when its `max_snapshots` argument exceeds the
session-wide hard cap (default 1000; configurable per session via
`SET ducklake_cdc_max_snapshots_hard_cap = ...`). See ADR 0011.

**Reference message:**

```text
CDC_MAX_SNAPSHOTS_EXCEEDED: cdc_window was called with
max_snapshots => 50000, but the session hard cap is 1000. To raise
the cap for this session:
  SET ducklake_cdc_max_snapshots_hard_cap = 50000;
Caps above ~10000 are likely to read megabytes of catalog state in a
single transaction; consider committing in smaller batches instead.
```

**Required fields:**

- the requested `max_snapshots` value
- the current session hard cap
- the `SET ducklake_cdc_max_snapshots_hard_cap = <value>` raise-the-cap
  recovery line, with the requested value interpolated
- the soft advisory line about batches above ~10000 (ADR 0011's
  performance discussion); operators with legitimate batch needs
  ignore it, but the line keeps the surprise out of the failure mode

## `CDC_WAIT_TIMEOUT_CLAMPED`

**Notice, not error.** Emitted via DuckDB `Notice` when `cdc_wait`'s
`timeout_ms` argument exceeds the session-wide hard cap (default
300_000 ms = 5 minutes; configurable per session via
`SET ducklake_cdc_wait_max_timeout_ms = ...`). See ADR 0011 §
"`cdc_wait` timeout hard cap". The call still proceeds with the
clamped timeout — the notice exists so the caller can tell what
happened without `cdc_wait` returning early-NULL silently.

**Reference message:**

```text
CDC_WAIT_TIMEOUT_CLAMPED: cdc_wait was called with timeout_ms =>
1800000 but the session cap is 300000 (5 minutes); the call will
return after at most 300000ms. To raise the cap for this session:
  SET ducklake_cdc_wait_max_timeout_ms = 1800000;
Caps above 1 hour mean a single connection is blocked for that long;
prefer shorter waits with re-polling for long-lived consumers.
```

**Required fields:**

- the requested `timeout_ms` value
- the current session cap (the value `cdc_wait` will actually use)
- the `SET ducklake_cdc_wait_max_timeout_ms = <value>` raise-the-cap
  recovery line, with the requested value interpolated
- the advisory line about caps above 1 hour

This notice is informational; bindings should surface it as a
`Notice` / `Warning` channel entry, not as a thrown exception. The
call returns the same value (next snapshot id, or NULL on the
clamped timeout) as it would with the lower timeout.

## `CDC_WAIT_SHARED_CONNECTION`

**Warning, not error.** Emitted once per DuckDB connection on the first
`cdc_wait` call. DuckDB does not expose enough connection-state
introspection for the extension to detect truly shared pool handles,
notebook sessions, or request handlers, so Phase 1 emits this
best-effort warning unconditionally on first use and does not spam
later waits on the same connection.

**Reference message:**

```text
CDC_WAIT_SHARED_CONNECTION: cdc_wait holds this DuckDB connection for
up to 30000ms. Calling it from a connection that also serves other
queries (a pool handle, a shared notebook session, an HTTP request
handler) can starve them. Hold a dedicated connection for cdc_wait.
The Python and Go clients open one for you internally; SQL-CLI users
must do this themselves. See docs/operational/wait.md.
```

**Required fields:**

- the effective `timeout_ms` value for the call
- the guidance to hold a dedicated connection for `cdc_wait`
- the client-specific note that Python and Go open a dedicated
  connection internally, while SQL-CLI users must do so themselves
- a pointer to `docs/operational/wait.md`

Bindings should surface this as a warning / notice channel entry, not
as a thrown exception. The call proceeds normally.

## `CDC_INCOMPATIBLE_CATALOG`

Emitted by `LoadInternal` and re-checked by every `cdc_*` function
that touches the catalog when an attached DuckLake catalog's format version
(`__ducklake_metadata_<lake>.ducklake_metadata WHERE key='version'`)
is outside the set listed in `docs/compatibility.md`. Severity:
notice at LOAD time (`cdc_version()` is a build stamp and stays
callable regardless of catalog state); error at the per-function call
site before any catalog-touching `cdc_*` function writes state.

The probe is best-effort: if the catalog can't be queried (partially
initialised mid-ATTACH, transient lock, missing privilege), no
notice is emitted and the next `cdc_*` call surfaces the real
problem.

**Reference message:**

```text
CDC_INCOMPATIBLE_CATALOG: attached DuckLake catalog 'lake' reports
catalog format version '99.99' but ducklake_cdc <version> supports
{0.4}. Either downgrade your DuckLake extension to a version that
writes one of the supported versions, or upgrade ducklake_cdc to a
release that supports '99.99'. See docs/compatibility.md for the full
matrix.
```

`<version>` is whatever `cdc_version()` reports for that build — the
release tag for tagged builds, or the git short SHA on untagged
builds. See `docs/decisions/0013-versioning-and-release-automation.md`.

**Required fields in the diagnostic body:**

- attached catalog name (the `AS lake` from the user's `ATTACH ...`).
- observed catalog format version (verbatim string read from the
  metadata row).
- supported set, comma-separated inside `{}`. Matches
  `SUPPORTED_DUCKLAKE_CATALOG_VERSIONS` in
  `src/compat_check.cpp`.
- this extension's version literal (matches the build stamp returned
  by `cdc_version()`; the prefix is always `ducklake_cdc `, the
  segment after is the build's tag or short SHA).
- a pointer to `docs/compatibility.md` so the operator can read the
  full matrix without searching.
