# 03 &mdash; Publish to a log/queue (Redis Streams)

> *"DuckLake is now an event source for the rest of your stack."*

A consumer that reads CDC from one or more DuckLake tables and publishes the
changes to a Redis Stream (`XADD`). Generalizes to Kafka, Pulsar, file
outputs, webhooks &mdash; the *sink contract* is the same; we pick Redis
Streams as the canonical, lowest-friction example.

This is the bread-and-butter integration story. Every team that adopts
DuckLake will eventually want to fan changes out to systems that don't speak
DuckDB.

## API surface used

- `cdc_dml_consumer_create` &mdash; one consumer per source table (or one
  consumer covering several tables, depending on stream-key strategy)
- `cdc_window` &mdash; standard polling cycle
- `cdc_dml_changes_read` &mdash; full payload read; rows go into the `XADD` body
- `cdc_commit` &mdash; cursor advance, **only after** every row in the window
  has been acknowledged by the sink

The sink contract is the design surface that the later examples (04, 05)
will reuse, so getting it right here matters more than the Redis specifics.
Two things define it:

1. **Idempotency key.** Every published payload carries `(snapshot_id, row_seq)`
   so a redelivery (after a crash before `cdc_commit`) is detectable
   downstream. For Redis Streams we set the entry ID to
   `<snapshot_id>-<row_seq>`, which gives Redis its own dedupe handle for
   free.
2. **Commit-after-publish.** `cdc_commit` runs only after the sink confirms
   durability of the whole window. Crash before commit &rArr; same window
   re-published &rArr; same idempotency keys &rArr; downstream dedupes.
   At-least-once over the wire, exactly-once at the consumer's view of the
   stream.

## Demo visualization

Bespoke. Split-pane terminal:

```
   ── producer (lake)                   ── stream (Redis: orders.cdc)
   commit 421  +120 rows  3ms ack       1700000000000-0  insert  id=42100  ...
   commit 422  + 87 rows  4ms ack       1700000000000-1  insert  id=42101  ...
   commit 423  + 95 rows  3ms ack       1700000000001-0  update  id=42050  ...
                                        1700000000002-0  delete  id=41984
   ── publisher                         ── consumer (XREAD)
   window 12  302 rows  ack 7 ms        lag from xadd:  3 ms p50 / 11 ms p99
   window 13  281 rows  ack 8 ms
   window 14  319 rows  ack 6 ms
   window 15  ack pending ...
```

Producer commits on the left, the publisher's per-window ack timing in the
middle-left, the actual Redis Stream tail on the right, and a tiny
`XREAD`-side reader at the bottom showing end-to-end lag.

## Acceptance criteria

In addition to the suite-wide criteria:

- [ ] Sustains 10,000 published rows/sec to Redis Streams over a 5-minute run
      with zero data loss across a forced producer restart and a forced
      publisher restart (separate runs).
- [ ] Median lake-commit&nbsp;&rarr;&nbsp;`XREAD`-visible latency &le; 25&nbsp;ms;
      p99 &le; 100&nbsp;ms on a known machine class.
- [ ] Killing the publisher mid-window and restarting it produces zero
      duplicate Redis Stream entries (verified by entry-ID inspection
      post-run). At-least-once over the wire, exactly-once at the stream.
- [ ] Schema-change handling: when the source table is `ALTER`ed, the
      published payload's schema version field bumps, downstream readers
      can detect the new shape without coordination, the README explains
      the migration story.
- [ ] The **sink contract module** introduced here (`sinks/contract.py` or
      similar) is reused unchanged by examples 04 and 05.

## Talk story

> "Here's a write to a DuckLake table. Here's a Kafka-shaped Redis Stream
> picking it up two milliseconds later. Now I'll kill the publisher
> mid-flight. Watch &mdash; no duplicates downstream when it comes back."

Demo strength is in the failure recovery. Steady-state publishing is
unsurprising; the crash-and-recover with no dedupe needed by the recipient
is the point.

## Open questions

1. **One stream per table, or one stream multiplexed?** Per-table is simpler
   and more idiomatic for Redis (consumers can `XREAD` only what they care
   about); multiplexed is closer to how teams use Kafka topics. Probably
   per-table by default with a multiplexed mode behind a flag.
2. **DDL events on the same stream?** Argument for: downstream readers see
   schema changes inline with data and can react. Argument against:
   different consumers on the same stream may not all care. Likely:
   separate `<table>.ddl` stream alongside `<table>.cdc`, mirroring the
   DDL/DML separation in the extension.
3. **Payload format.** JSON for the demo (readable in `XREAD`). A
   production version should support Avro / Protobuf with a schema registry
   pointer, but that is out of scope for this example &mdash; we note the
   gap in the README and stop.

## Running this example

```bash
docker compose -f e2e/docker-compose.yml up -d --wait   # adds redis service
make release

# default: split-pane TUI (producer commits + Redis XREAD tail)
uv run --project e2e python e2e/03_publish_redis/app.py

# CI: same workload, no TUI, asserts + writes metrics JSON
uv run --project e2e python e2e/03_publish_redis/app.py --headless --catalog postgres
```

Catalog support: `postgres` (multi-process publisher), `sqlite` (WAL,
multi-process), `duckdb` only when `--in-process` is set (publisher and
producer share one DuckDB).

Storage: `--storage disk` (default) or `--storage s3`.

## Status

TODO. Build third (after 01 and 02). Defines the sink contract that 04 and
05 inherit. Adding the `redis` service to `e2e/docker-compose.yml` happens
alongside this example.
