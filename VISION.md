# Vision

`ducklake-cdc` should make DuckLake changes easy to inspect and consume without
adding a JVM, a coordinator, or another SaaS service.

The project is small on purpose. The extension should stay useful from plain
SQL first. A Python client can come after the SQL surface feels right.

## What It Is For

- **Reverse ETL and fan-out:** read changes from DuckLake and push them to
  search indexes, caches, APIs, queues, or your own scripts.
- **Outbox-style workflows:** route on DuckLake commit metadata instead of
  maintaining a separate event table.
- **Audit logs:** keep a durable, snapshot-precise record of what changed.
- **Schema change feeds:** treat DDL as first-class data, not as an afterthought.

## What It Adds

DuckLake already has snapshots, time travel, commit metadata, and
`table_changes`. `ducklake-cdc` adds the consumer-side pieces:

- named cursors stored in the DuckLake catalog;
- read windows and explicit commits;
- gap detection when maintenance expires snapshots;
- long-polling with `cdc_wait`;
- typed DDL events with `cdc_ddl`;
- schema-boundary handling;
- single-reader enforcement with an owner-token lease;
- basic consumer stats and audit history.

## What It Does Not Try To Be

- It is not OLTP CDC. Debezium owns that world.
- It is not a streaming platform.
- It is not a guarantee of sub-10ms latency.
- It is not trying to support every sink before real users ask for them.
- It is not trying to maintain several client libraries at once.

## Near-Term Shape

The useful path is:

1. Keep the SQL extension installable and honest.
2. Verify the extension API against the maintainer's real use cases.
3. Add focused performance improvements where profiling points.
4. Build a small Python client.

Other language clients can wait until there is clear outside demand or another
maintainer wants to own them.

See also:

- [`README.md`](./README.md) for what works today.
- [`docs/design.md`](./docs/design.md) for durable design decisions.
- [`docs/roadmap.md`](./docs/roadmap.md) for current direction.
- [`docs/hazard-log.md`](./docs/hazard-log.md) for known risks.
