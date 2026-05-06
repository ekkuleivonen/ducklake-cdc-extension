# 02 &mdash; Low-latency ticks

A change-tap that wakes on every commit to a watched table, records
the commit-to-tick latency, and advances the cursor. The consumer
holds one DuckDB connection in `cdc_dml_ticks_listen` and **never
reads the row payload** &mdash; the tick carries `snapshot_id`,
`snapshot_time`, and `table_ids` only.

```
  raw_ticks  →  cdc_dml_ticks_listen  →  latency histogram
                  (metadata only;
                   no row materialisation)
```

## Real-life use case

The "cache invalidation" / "wake the workers" / "notify the
microservice" pattern. A SaaS app commits to DuckLake; a downstream
recipient &mdash; a CDN, a Redis cache, a search index, a service
that re-reads its slice on demand &mdash; just needs to know
"something new happened in table T at snapshot N" so it can decide
whether to act.

The classic alternative is to publish every changed row to a queue.
That doubles the bandwidth (rows go out twice: once into DuckLake,
once into the queue), couples the publisher to every recipient's
schema, and forces materialisation on the publisher even if the
recipient only wants to invalidate a key. This example is the
opposite trade-off: the publisher's hot path is one metadata-only
listen + one cursor advance per commit.

This example is the *measurement harness* for that pattern. The
forwarding layer (Redis Pub/Sub, WebSocket, HTTP webhook,
in-process callback) is a downstream concern; what matters here is
that the listen primitive itself is cheap and the wake-up is fast.

## Limitations

**At-least-once, not exactly-once.** The tick is recorded before
`cdc_commit` advances the cursor, so a crash between publish and
commit re-emits the tick on restart. Real recipients should be
idempotent (cache invalidation by snapshot id is naturally
idempotent; so is "re-read this table"). If your downstream needs
exactly-once, look at example 01's stage transactions instead.

**Latency floor is bounded by commit cost and metadata wakeup work.**
Postgres catalogs use the snapshot `LISTEN`/`NOTIFY` fast path when
available; other backends use the `poll_min_ms` fallback. The visible
commit-to-tick latency still includes producer commit time and the
metadata work inside `cdc_dml_ticks_listen`; the tick itself does not
read changed rows or compute per-operation counts. SQLite serialises
producer + consumer through a single file lock, which adds hundreds of ms.
This demo disables listen coalescing because it measures first-tick latency,
not throughput-oriented batching.

## Run

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release

# live TUI: latency histogram + p50/p95/p99 + tick rate
uv run --project e2e python e2e/02_ticks/app.py

# unattended: no TUI, periodic stderr summary
uv run --project e2e python e2e/02_ticks/app.py --headless --duration 30

# any catalog (single-process; all three apply)
uv run --project e2e python e2e/02_ticks/app.py --headless --duration 30 --catalog duckdb
uv run --project e2e python e2e/02_ticks/app.py --headless --duration 30 --catalog sqlite
```

The lake is reset before and after each run.

## Reading the numbers

The final stderr line (and the live TUI) report:

```
producer=N        → commits the producer made into raw_ticks
ticks=N           → ticks the consumer delivered (one per snapshot the producer wrote)
rate=N/s          → ticks delivered per second
p50, p95, p99     → commit → tick latency percentiles
                    (snapshot_time from the catalog vs wall-clock at tick arrival)
hist=[a,b,c,d,e]  → latency bin counts: <5ms, 5-10ms, 10-25ms, 25-100ms, >100ms
errors=N          → unhandled exceptions in the consumer or commit path
```

A healthy run:

- `producer == ticks` (or off by 1 if a tick is in flight at shutdown).
  The 1:1 mapping is the proof that the listen primitive doesn't drop
  snapshots.
- `errors=0`.
- Latency depends almost entirely on the catalog choice. On a
  laptop-class machine, expect roughly:

  | catalog  | p50    | p99    | notes |
  |----------|--------|--------|-------|
  | duckdb   | ~55 ms | ~130 ms | embedded; the floor for this stack |
  | postgres | ~90 ms | ~190 ms | network + bookkeeping per commit |
  | sqlite   | ~500 ms | ~3 s   | single-file lock contention |

If `ticks < producer - 1` after the run finishes, the consumer
didn't drain in time. Bump `--duration` (the consumer has one
`LISTEN_TIMEOUT_MS` cycle of grace at shutdown) or reduce
`PRODUCER_INTERVAL_S` in `app.py` so the tail is shorter.
