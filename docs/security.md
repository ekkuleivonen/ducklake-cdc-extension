# Threat model

> **Status: Phase 0 deliverable.** Locked here so Phase 1 implements
> against an explicit threat model rather than discovering one through
> bug reports. The model covers what the extension defends against,
> what it explicitly does not, and what the operator owes the catalog.

`ducklake-cdc` is small on purpose (`docs/roadmap/README.md`). The threat surface is correspondingly small. This document
enumerates it exhaustively rather than trusting that "small" implies
"safe."

## Trust boundaries

```text
┌─────────────────────────────────────────────────────────────────┐
│                  Catalog (DuckDB / Postgres / SQLite)           │
│  __ducklake_cdc_consumers   __ducklake_cdc_audit                │
│  __ducklake_metadata_*      ducklake_*    (DuckLake's own)     │
│                              ▲                                  │
└──────────────────────────────│──────────────────────────────────┘
                               │ catalog GRANTs
                               │ (operator-owned)
                               │
┌──────────────────────────────┴──────────────────────────────────┐
│                  DuckDB process                                 │
│   ducklake (extension, upstream)                                │
│   ducklake_cdc (this project)                                   │
│   ┌──────────────────────────────────────────────────────┐      │
│   │ Per-connection state:                                │      │
│   │  - cached owner_token (per consumer name)            │      │
│   │  - session settings (ducklake_cdc_*)                 │      │
│   └──────────────────────────────────────────────────────┘      │
└──────────────────────────────┬──────────────────────────────────┘
                               │ language binding (Python / Go / …)
                               │
┌──────────────────────────────┴──────────────────────────────────┐
│                  Application code                               │
│   producer side:  CALL set_commit_message(...)                  │
│   consumer side:  cdc_consumer_create / cdc_window / cdc_commit │
│   sink side:      reference sinks (Phase 1+)                    │
└─────────────────────────────────────────────────────────────────┘
```

Trust assumptions:

- **The catalog is authoritative.** Anyone with catalog read access
  can read every `__ducklake_cdc_*` table; anyone with catalog write
  access can call every primitive. Catalog GRANTs are the operator's
  responsibility — the extension does not add a permission layer on
  top.
- **The DuckDB process is trusted by the application.** The
  extension does not defend against the SQL caller; the SQL caller
  is the user. (We do defend against SQL-injection in *user-supplied
  identifiers* — see § "SQL injection in identifier handling" below
  — because the user is not always the same as the operator.)
- **DuckLake is trusted upstream.** We do not validate
  `<lake>.snapshots()` rows for plausibility, do not re-check
  encryption / inlining behaviour, and do not assume DuckLake
  catalog integrity is compromised. Bugs in DuckLake are filed
  upstream (`docs/upstream-asks.md`), not worked around in this
  extension.

## What the extension defends against

### 1. SQL injection in identifier handling

**Surface.** Every primitive accepts user-supplied identifiers:
`consumer_name` on every consumer-lifecycle call,
`tables[]` on `cdc_consumer_create`, `table` on `cdc_changes` /
`cdc_recent_changes` / `cdc_schema_diff`. A naive implementation
that string-concatenates these into SQL is the classic injection
surface.

**Defense.** Bind every user-supplied identifier through DuckDB's
quoting helpers; never string-concatenate into SQL. The reference
implementation the prototype query helper is the canonical pattern
(quoted in ADR 0004 row 1 of the lifted-pattern table). Phase 1's
test matrix asserts:

- `cdc_consumer_create('valid"; DROP TABLE evil; --')` either
  rejects the name as invalid or stores it verbatim and uses
  it via parameter-binding everywhere — never as a string-concat
  into the catalog DDL.
- `cdc_changes(consumer, 'main.t"; DROP TABLE evil; --')` raises
  `CDC_INVALID_TABLE_FILTER` (or the equivalent table-validation
  error) at create time, and never reaches `table_changes` with
  the unquoted name.
- The `tables[]` filter elements are validated at create time
  against `ducklake_table` rows; rejection is loud
  (`CDC_INVALID_TABLE_FILTER`, `docs/errors.md`).

**Audit-log discipline.** The same quoting rules apply when the
audit-log writer interpolates `consumer_name` into the
`__ducklake_cdc_audit` insert; the test matrix exercises both the
primitive call site and the audit-log call site.

### 2. Cross-connection cursor takeover

**Surface.** Two concurrent connections both subscribe under the
same `consumer_name` and try to advance the cursor. Without
defense, the second `cdc_commit` silently overwrites the first.

**Defense.** The owner-token lease (ADR 0007). `cdc_window` acquires
the lease atomically inside its own transaction; `cdc_commit`
verifies the lease before advancing the cursor. The race the
earlier `pg_advisory_xact_lock` design failed to close (lease
released before the holder commits) is what the row-stored token
explicitly prevents — `cdc_commit`'s UPDATE predicate is
`WHERE owner_token = :token_held_by_this_connection`, which fails
if the holder has been displaced.

`CDC_BUSY` (`docs/errors.md`) is the user-visible signal.

### 3. Cursor-state corruption from interrupted commits

**Surface.** A `cdc_commit` partially completes — say, the cursor
update succeeds but the heartbeat refresh doesn't — leaving an
inconsistent state.

**Defense.** All cursor mutations are single-statement `UPDATE`s
inside a single transaction. The catalog's transactional guarantees
(DuckLake snapshot-id arbitration, Postgres / SQLite ACID, embedded
DuckDB single-writer) do the work; the extension does not split a
commit into multiple statements. Phase 1's test matrix injects connection drops mid-
commit and asserts the catalog state is one of "fully advanced"
or "fully un-advanced" — never partial.

### 4. Self-DOS via degenerate arguments

**Surface.** `cdc_window(consumer, max_snapshots := 10_000_000)`
asks DuckLake to scan the entire lake history in one transaction.
`cdc_wait(consumer, timeout_ms := 86_400_000)` holds a connection
open for a day.

**Defense.** Hard caps (ADR 0011):

- `max_snapshots` is hard-capped at 1000 (default 100, raisable per
  session via `SET ducklake_cdc_max_snapshots_hard_cap = ...`).
  Above the cap, `cdc_window` raises
  `CDC_MAX_SNAPSHOTS_EXCEEDED`.
- `timeout_ms` on `cdc_wait` is hard-capped at 5 minutes
  (`SET ducklake_cdc_wait_max_timeout_ms` to raise).
  Above the cap, the call is **clamped** with a
  `CDC_WAIT_TIMEOUT_CLAMPED` notice — not rejected. The notice
  approach prevents the failure mode where a consumer using
  `cdc_wait(timeout_ms => 600000)` silently returns after 5 minutes
  with `NULL` and the operator can't tell why.

### 5. Audit-log forgery via metadata field

**Surface.** A consumer-create call could put arbitrary JSON in
`metadata` to fake-corroborate an audit-log story.

**Defense.** Audit history does **not** live in
`__ducklake_cdc_consumers.metadata` (ADR 0009 § "What does NOT live
here", ADR 0010). It lives in the dedicated, append-only
`__ducklake_cdc_audit` table; the consumer cannot write directly to
it through the public surface. Audit rows are emitted by the
extension's own primitives (`cdc_consumer_create`, `_drop`,
`_reset`, `_force_release`, lease force-acquire on heartbeat
timeout, Phase 2 DLQ actions). Operators with raw catalog write
access can of course forge rows — that's a catalog-permissions
problem, not an extension problem.

## What the extension does NOT defend against (out-of-scope)

The project is small; saying no early is cheaper than saying it
later under bug-report pressure.

### 1. Multi-tenant isolation inside one catalog

`__ducklake_cdc_consumers` is **world-readable** to anyone with
catalog read access. Consumer names should not encode secrets.
Multi-tenant deployments handle isolation at the catalog layer:
per-schema GRANTs, per-tenant `__ducklake_metadata_<lake>` schemas,
or one DuckLake catalog per tenant. The extension does not add a
row-level security layer.

If you need consumer-name privacy, your catalog architecture should
already separate tenants. The extension would be a thin and
defeat-able veneer over a problem the catalog already solves
properly.

### 2. Encryption of consumer state at rest

Consumer rows are stored unencrypted in the catalog (modulo
catalog-level transparent encryption like Postgres + pgcrypto, or
a fully-encrypted SQLite). The cursor (`last_committed_snapshot`)
is a `BIGINT` — there's nothing in the row that needs to be
encrypted that isn't already covered by catalog-level encryption.

### 3. Authentication of `set_commit_message` content

Producers stamp `(author, message, extra_info)` on every commit.
The extension surfaces these verbatim. Whether `author` is
trustworthy is the producer's deployment problem (e.g. service
accounts with restricted catalog write access). Reference sinks
that route on `extra_info.event` should treat the value as
**untrusted** input — see `docs/conventions/outbox-metadata.md` §
"Validation discipline".

### 4. Denial-of-service via legitimate consumer load

A consumer that calls `cdc_wait` aggressively under
`SET ducklake_cdc_wait_max_interval_ms = 1` (1ms polling) imposes
real catalog QPS. The extension does not rate-limit; the operator
does, via catalog-level connection limits and `pg_stat_statements`
discipline. ADR 0011 § "Catalog load" gives the per-consumer cost
ranges so operators can size catalogs intentionally.

### 5. Sink-side delivery guarantees beyond at-least-once

`cdc_window` + `cdc_commit` give at-least-once at the cursor; the
sink's own write semantics determine end-to-end delivery. Idempotent
sinks (Postgres mirror with primary key, Kafka with producer-side
dedup) recover from re-delivery transparently; non-idempotent sinks
(append-only HTTP webhook with no `Idempotency-Key`) do not. The
project's reference sinks (Phase 1 / Phase 5) ship with
idempotency-key support; user-written sinks are the user's contract.

## `set_commit_message` `extra_info` is consumer-trusted input

Repeating this in its own section because reference sinks (which
*are* under project control) are the place this defense lands.

`extra_info` is a `JSON` column that the producer writes and the
consumer reads. The extension surfaces the value verbatim per
pillar 10 (serializers in clients). Downstream sinks treat it as
**untrusted**:

- **Validate JSON shape.** A producer can write
  `extra_info => 'not-json'`; the bindings parse and surface a
  `MalformedExtraInfo` warning rather than crashing the consumer
  loop.
- **Length-limit before logging.** Pathological payloads are legal
  SQL (`'"' || repeat('A', 1_000_000) || '"'`); sinks truncate
  before stamping into log lines or forwarding to systems with
  payload-size limits (Slack, PagerDuty).
- **Escape before rendering.** Webhook sinks that template fields
  into URL paths URL-encode them; HTML rendering HTML-escapes;
  `psql`-style logs do not interpret backslash escapes.

The full convention shape (recommended `event`, `trace_id`,
`schema`, `actor`, `extra` fields) is in
`docs/conventions/outbox-metadata.md`. Sinks that recognise the
convention can apply schema-aware validation (e.g. "if
`extra_info.schema` says `orderplaced.v1`, validate against the
registered Avro schema before forwarding"); sinks that don't simply
pass-through.

## SQL-injection review checklist (Phase 1 PR template)

Every Phase 1 PR that touches the extension's SQL-emission code
runs against this checklist (also referenced in `CONTRIBUTING.md`):

- [ ] Every interpolation of a user-supplied identifier
  (`consumer_name`, `table`, anything in `tables[]`,
  `change_types[]`, `event_categories[]`) uses `_q`-equivalent
  quoting or parameter binding.
- [ ] No identifier is string-concatenated into a `CREATE TABLE`,
  `ALTER TABLE`, or `INSERT` statement.
- [ ] User-supplied table names in `cdc_changes` /
  `cdc_recent_changes` / `cdc_schema_diff` are validated against
  `ducklake_table` before being passed through to `table_changes`.
- [ ] The `__ducklake_cdc_audit` writer applies the same quoting
  rules to `consumer_name`, `actor`, and `details` JSON.
- [ ] Reference sinks (Phase 1 stdout / file / webhook) escape
  user-controlled fields before rendering into output formats
  (URL paths, HTTP headers, log lines).

The Phase 5 security review (per `docs/roadmap/`)
revisits this checklist against the v0.1.0-beta.1 surface and
extends it with any lease-related new SQL paths.

## See also

- ADR 0007 — owner-token lease (defense against cursor takeover).
- ADR 0009 — `__ducklake_cdc_consumers` schema (what's stored,
  what isn't).
- ADR 0010 — audit log schema (where audit rows live; what they
  carry; why `metadata` is not the audit log).
- ADR 0011 — performance model + hard caps (defense against
  self-DOS).
- `docs/errors.md` — `CDC_BUSY`, `CDC_INVALID_TABLE_FILTER`,
  `CDC_MAX_SNAPSHOTS_EXCEEDED`, `CDC_WAIT_TIMEOUT_CLAMPED`.
- `docs/conventions/outbox-metadata.md` § "Validation discipline".
- `docs/roadmap/` — refresh checklist
  for v0.1.0-beta.1.
