# Upstream asks

> **Status: living document.** Tracks issues / feature requests that
> belong upstream in DuckLake (or DuckDB) rather than worked around
> here. Each entry has a status, a priority, and a "what we want
> them to do" framing — so we can hand the entry to a maintainer
> without it reading as a project-internal grievance.

## Why this file exists

The project's design pillars (`docs/roadmap/README.md`) commit us to a
small surface: "anything DuckLake already does, we use directly."
Sometimes DuckLake doesn't do the thing yet, and the right long-term
fix is to get it added upstream rather than re-implement it here.
This file is the staging ground for those conversations.

Two ground rules:

1. **No issue gets filed before we have a working demo.** Asking a
   maintainer to design a feature for a project that has shipped
   nothing reads as entitlement. The Phase 5 schedule
   (`docs/roadmap/`) explicitly defers the formal upstream
   filings until 3 months post-beta, so we have real-user data to
   motivate them with.
2. **Each entry says what success looks like.** A feature request
   without a measurable outcome is a wish. Each entry below has an
   "Acceptance" section that describes how we'd know the upstream
   change closed our gap.

## Open asks

### 1. Notification hook on snapshot commit (the polling-vs-subscribe gap)

- **Filed:** not yet (target: 3 months post-`v0.1.0-beta.1`).
- **Priority:** high.
- **Owner here:** ekku.
- **DuckLake side:** unowned (we'll find a maintainer when we file).

#### Context

`cdc_wait` is honest exponential-backoff polling because DuckLake has
no commit-notification mechanism. The polling backoff defaults to
100ms floor / 10s cap (`docs/roadmap/README.md`); the resulting steady-state p50 latency is ~200ms with
a p99 of ~1.5s on default settings (ADR 0011). That is **adequate
for the workloads we target** (sub-second to seconds; reverse ETL,
search index hydration, audit) and **inadequate for OLTP CDC**
(Debezium territory; 10-100ms expectations).

The right long-term fix is upstream: a hook that fires on snapshot
commit so the extension can subscribe to commit notifications
instead of polling for them. Polling as a fallback would still exist
(catalogs without the hook, brand-new lakes, cross-version
deployments), but the hook would close the gap to Debezium-class
latency on the workloads where users actually want it.

#### What we'd like

A DuckLake-level extension API along the lines of:

```cpp
// Pseudo-API; the real shape depends on DuckLake's extension model.
ducklake::Catalog::OnCommit(
    std::function<void(const SnapshotCommit&)> callback
);
```

Triggered when DuckLake commits a new snapshot to the catalog.
`SnapshotCommit` carries at least `snapshot_id`, `snapshot_time`,
and the `changes_made` blob; that's enough for `cdc_wait` to
short-circuit the backoff and return immediately.

For the Postgres catalog backend, the natural implementation sits on
`LISTEN`/`NOTIFY`. For DuckDB / SQLite (single-file catalogs), an
in-process callback is sufficient.

#### Acceptance

- `cdc_wait` p50 drops from ~200ms (polling) to ~10-50ms
  (notification-driven) on a configured-aggressive setting.
- `cdc_wait` p99 drops from ~1.5s to ~100ms.
- Catalog QPS for idle consumers drops to **near zero** (no polling
  loop on the holding connection — only the notification fire-and-poll
  to read the new state).
- Phase 5 launch post can re-write the latency one-liner from
  "sub-second to seconds" to "tens to hundreds of milliseconds"
  without overpromising.

#### Why we wait until 3 months post-beta

We want the issue to land with **production polling-load data**
attached: "here are the catalog QPS curves from real users; here's
what the notification path would replace; here's the throughput
ceiling we're hitting." That kind of evidence makes the ask read as
a measured engineering proposal, not a feature request.

### 2. (Reserved for future)

This file is the staging ground; new asks land here as the project
encounters them. Keep them short, keep them measurable, keep the
"what success looks like" front-and-centre.

## Closed asks

(Empty — the project is pre-alpha. Closed asks land here when they
ship upstream and we cut a corresponding release.)

## How to add a new ask

1. Open a PR that adds a numbered section here.
2. Include: filed-or-not status, priority, our owner, the upstream
   owner if known, context (why this is upstream's problem and not
   ours), what we want them to do, an Acceptance section with
   measurable outcomes, and a reproducer if applicable.
3. Cross-reference from any project doc / ADR / probe that triggered
   the ask, so the chain of reasoning is followable.
4. If the ask is filed upstream, link the issue.

## See also

- `docs/roadmap/` — the upstream-filing schedule (3
  months post-beta).
- ADR 0008 — the upstream probe + recurring CI gate that surfaces upstream
  changes; the place where future asks of the form "DuckLake
  changed a key, please put it back / give us a migration" will
  originate.
- `docs/compatibility.md` — the matrix that picks up the resolution
  of these asks (e.g. when DuckLake's notification-hook lands, the
  matrix cells reflect it).
