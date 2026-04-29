# cdc_wait — long-poll, connection starvation, and the timeout cap

`cdc_wait(catalog, consumer, timeout_ms := 30000)` blocks the calling
connection until either:

- a new external snapshot lands past the consumer's
  `last_committed_snapshot` (returns the new snapshot id), or
- `timeout_ms` elapses (returns `NULL`).

It exists so consumers can stream without polling the catalog every
100ms. The connection-blocking is the implementation detail; treat
it as the load-bearing constraint.

## The connection-starvation foot gun

`cdc_wait` holds the calling connection for the full `timeout_ms` if
nothing happens. While held the connection cannot run other queries.
A naive consumer pool that sizes connections to "one per consumer"
and uses `cdc_wait(timeout_ms => 5*60_000)` per loop iteration will,
on a quiet lake, hold every connection in the pool for 5 minutes at
a time — and any concurrent transactional work on the same
connections will stall.

**Defense in depth:**

1. **Hard cap on `timeout_ms`: 5 minutes (300 000 ms).** Larger
   values are clamped to the cap and the call emits a
   `CDC_WAIT_TIMEOUT_CLAMPED` stderr notice carrying the requested
   value, the cap, and the recovery line. The cap exists so users
   who pass `0` or `INT_MAX` thinking "wait forever" do not lock a
   connection indefinitely.
2. **Default of 30 seconds.** Conservative on purpose. Bindings
   should re-poll on `NULL` rather than reach for a longer timeout.
3. **Use a dedicated long-poll connection per consumer**, separate
   from any connection that runs concurrent transactional work.
   Bindings provide a `tail()` helper that does this.
4. **Backoff curve inside cdc_wait** — initial 100ms, doubles up to
   10s. The first call after activity returns within 100ms; long
   stretches of inactivity poll the catalog at most once every 10s.
5. **Interruptible.** A standard DuckDB interrupt cancels the wait
   within hundreds of milliseconds. Bindings honor the interrupt
   path so a consumer's shutdown signal never has to wait the full
   `timeout_ms`.

## Pool sizing

For a process running N consumers, size connections as `N + M` where
M is the largest number of concurrent transactional queries you
expect. Don't share long-poll connections between consumers because
the inner sleep cannot multiplex.

For a Postgres-catalog deployment, the foot gun multiplies: each
long-poll consumer holds a Postgres backend on the catalog side too.
PgBouncer in transaction-pooling mode does not help here because
cdc_wait holds the connection between transactions. Use the
catalog-side connection floor in `docs/performance.md` ("comfortably
~50 consumers without pooling, ~500 with PgBouncer in
session-pooling mode") as the planning input.

## Real-world flow

```text
loop:
  new_snap = cdc_wait(lake, consumer, timeout_ms = 30000)
  if new_snap is None: continue           # no work; loop and re-poll
  window = cdc_window(lake, consumer)     # acquires lease
  for batch in cdc_changes(lake, consumer, table):
      sink(batch)
  cdc_commit(lake, consumer, window.end_snapshot)
```

The wait is per-iteration, not per-process. A consumer that processed
work on iteration N+1 starts iteration N+2 with a fresh
`cdc_wait(...)` against the new cursor. There is no "subscribe and
let me know" handle to leak.

## When NOT to use cdc_wait

- **Batch consumers.** A nightly job that wakes up, reads everything
  since yesterday, and exits should call `cdc_window` directly. No
  wait, no holding a connection at the wrong time of day.
- **Multi-tenant proxies.** A daemon that polls many catalogs on
  behalf of many consumers should poll each catalog once on a
  shared cadence and dispatch the resulting snapshot fan-out
  in-process. The bindings ship a `LakeWatcher` helper that does
  exactly this.

## Tests

Coverage in `test/sql/consumer_state.test` (cdc_wait happy path,
NULL on timeout, clamping above the cap), `test/sql/notices_validation.test`
(CDC_WAIT_TIMEOUT_CLAMPED notice fires above 300 000 ms). The
interrupt path is covered by language clients in Phases 3 / 4.
