# 02 &mdash; Low-latency ticks

> *"Sub-50&nbsp;ms change-tap that doesn't move bytes."*

A consumer that wakes on every new snapshot, sends a tiny notification
("snapshot N is here for table T"), and **never reads the row payload**. The
recipient (a cache, a CDN, a microservice, a browser tab) re-reads its own
data when it cares to. Data isn't shuffled twice.

This is the latency-floor demo. It exists to prove a specific claim:
*"DuckLake CDC can notify a downstream system within milliseconds of a commit
without ever materializing the changes inside the CDC consumer."*

## API surface used

- `cdc_dml_consumer_create` &mdash; per watched table
- `cdc_window` &mdash; metadata-only snapshot probe (the **only** read on the
  hot path; no `cdc_dml_changes_read` ever called)
- `cdc_commit` &mdash; cursor advance after the notification is acknowledged

Notifications themselves go out over Server-Sent Events to a tiny HTML page.
SSE is the demo transport; the same shape works with WebSockets, Redis Pub/Sub,
HTTP webhooks, or just direct calls to a downstream service.

## Extension prerequisite (blocks this example)

Today's `cdc_window` is a polling primitive. Polling at 1&nbsp;ms gives
sub-2&nbsp;ms detection but burns CPU; polling at 100&nbsp;ms is cheap but
caps the demo at 100+&nbsp;ms latency floor. Neither is the story we want.

The example is **gated on adding a long-poll variant** to the extension:

```sql
SELECT * FROM cdc_window_wait(
    'lake', 'consumer_name',
    timeout_ms := 5000  -- block up to 5s; return immediately on new snapshot
);
```

Semantics: returns the same row shape as `cdc_window`. If a new snapshot is
already available, returns immediately. Otherwise blocks inside the extension
on a condition variable signaled by the catalog's snapshot-pointer update,
returning either when a snapshot arrives or when `timeout_ms` elapses (with
`has_changes := false`).

This is a contained extension change &mdash; one new table function, no
changes to the storage format or to existing primitives. The smoke probe in
`e2e/smoke/cdc_wait_interrupt_smoke.py` already proves DuckDB interrupts
propagate cleanly, which is the hardest part.

The example's `app.py` should not be merged until `cdc_window_wait` is in.
The README should ship now; it doubles as the spec for the extension work.

## Demo visualization

Bespoke. Two panes, side by side:

- **Left**: a tiny browser tab open to `localhost:8765`. Every tick, the page
  flashes briefly and shows the snapshot ID + the table name. Visceral
  proof of liveness.
- **Right**: a terminal histogram of *commit&nbsp;&rarr;&nbsp;tick* latency,
  measured by injecting writes whose timestamp is read back through the tick
  payload. p50, p99, max updated live.

```
  commit -> tick latency  (n = 8421)
   <  5ms ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓░░ 6312
  5-10ms ▓▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░░░░░ 1488
  10-25ms ▓▓░░░░░░░░░░░░░░░░░░░░░░░░░░░░  421
   >25ms ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  200

  p50 3.2 ms  ·  p99 18.4 ms  ·  max 41 ms
```

## Acceptance criteria

In addition to the suite-wide criteria:

- [ ] **`cdc_window_wait` primitive lands in the extension** with a
      SQLLogic test and a smoke probe (`e2e/smoke/cdc_window_wait_smoke.py`).
- [ ] On a known machine class (Apple M-series laptop), median
      commit&nbsp;&rarr;&nbsp;tick latency &le; 10&nbsp;ms; p99 &le; 50&nbsp;ms,
      sustained for a 10-minute run at 50 commits/sec.
- [ ] Idle CPU is &lt; 1% per watched table (the long-poll variant blocks
      inside the extension; no busy loop in Python).
- [ ] Killing and restarting the example resumes ticks from the last
      committed snapshot. At-least-once delivery is the target here, not
      exactly-once: the recipient is expected to be idempotent.
- [ ] Demo HTML page is openable on `localhost:8765` with no extra setup,
      and visibly flashes within one frame of every tick.

## Talk story

> "Most CDC systems force you to materialize every change before you can
> react to it. We don't. Here's a write to DuckLake. Here's a browser tab
> reacting to that write. Look at the timestamp difference."

10-second demo. Memorable. Closes the talk.

## Open questions

1. **Coalescing.** If 100 commits land in 5&nbsp;ms, do we send 100 ticks or
   one tick covering the snapshot range? Probably configurable per consumer:
   `coalesce: true` for cache invalidation (one tick is enough),
   `coalesce: false` for audit-style consumers that need to see every
   snapshot ID. Default: coalesce.
2. **Backpressure on the recipient.** If the SSE client falls behind, do we
   buffer or drop? For cache-invalidation semantics, dropping is fine
   (latest tick subsumes earlier ones). The example should make this
   explicit and let the operator choose.
3. **Ticks across schema boundaries.** A tick during an `ALTER TABLE`
   should still go out; the recipient should treat schema-change ticks
   as a stronger signal ("re-read everything, the shape changed"). Decide
   the API shape with the extension change.

## Running this example

(Once the extension primitive lands and `app.py` is built.)

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release

# default: live TUI + browser tab on localhost:8765
uv run --project e2e python e2e/02_ticks/app.py

# CI: same workload, no TUI, asserts + writes metrics JSON
uv run --project e2e python e2e/02_ticks/app.py --headless --catalog postgres
```

Catalog support: `duckdb`, `sqlite`, `postgres`. The tick consumer is
intrinsically single-process, so all three apply.

Storage: `--storage disk` (default) or `--storage s3`. Latency is
storage-sensitive here &mdash; the README's numbers should report both so the
"sub-50&nbsp;ms" claim is grounded in the realistic backend mix.

## Status

TODO &mdash; **blocked on extension `cdc_window_wait` primitive**. README is
the spec; safe to ship the README now and treat it as the design doc for the
extension work.
