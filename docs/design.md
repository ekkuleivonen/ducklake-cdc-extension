# Design Notes

These are the project decisions worth keeping close at hand. The old decision
log was useful while the extension shape was still unsettled, but it became too
heavy for a small weekend-maintained project.

## What This Extension Is

`ducklake-cdc` is a small DuckDB extension that adds durable CDC consumers on
top of DuckLake. It does not replace DuckLake's own snapshot, time-travel, or
table-change APIs.

The extension exists to make the same SQL surface work from the DuckDB CLI and
from future clients without each client reimplementing cursor state, leasing,
subscription routing, schema-boundary logic, and typed DDL extraction.

## Use DuckLake Directly

Anything DuckLake already exposes should stay DuckLake's job:

- `snapshots()` tells us what changed.
- `table_changes()` reads row-level changes.
- Time travel reads historical state.
- Commit metadata carries producer-side context.
- DuckLake maintenance handles compaction, file rewrites, and cleanup.

The extension adds only the parts needed for consumer workflows: durable cursor
state, identity-based subscriptions, gap detection, long-polling, typed DDL
events, schema-boundary handling, lease enforcement, and basic observability.

## Metadata State

Consumer state lives beside DuckLake's own metadata tables in the metadata
catalog backend:

- `__ducklake_cdc_consumers`
- `__ducklake_cdc_consumer_subscriptions`
- `__ducklake_cdc_audit`

DuckDB and PostgreSQL metadata catalogs use a sibling `__ducklake_cdc` schema
inside `__ducklake_metadata_<catalog>`. SQLite metadata catalogs do not support
schemas, so the same table names live in the metadata database `main` schema.

This keeps state close to the catalog snapshots it tracks without making every
cursor heartbeat or commit create a DuckLake snapshot. There is no external
state store and no side database to keep in sync.

`__ducklake_cdc_consumers` stores one row per named consumer: stream kind
(`ddl` or `dml`), cursor position, lease fields, timestamps, and metadata.
Routing intent is stored only in `__ducklake_cdc_consumer_subscriptions`.

`__ducklake_cdc_consumer_subscriptions` stores one normalized row per resolved
routing rule. Subscriptions are identity-first: names supplied at create time
are resolved to DuckLake `schema_id` and `table_id`, then matching uses those
ids. DDL subscriptions may be catalog-, schema-, or table-scoped. DML
subscriptions are concrete table identities. A table subscription follows table
renames, a schema DDL subscription follows schema renames, and drop + recreate
with the same name is a new object that does not match the old subscription.

`__ducklake_cdc_audit` records lifecycle actions for operational visibility.
Sink retries, idempotency, validation, quarantine handling, and external
side-effect semantics belong in clients and sinks, not in the SQL extension
state schema.

## Read and Commit Stay Separate

`cdc_window` opens a bounded read window. `cdc_commit` advances the cursor.

Those are intentionally separate calls. A consumer should only commit after its
downstream write has succeeded. This is the core at-least-once contract.

The `listen` and `read` functions may offer `auto_commit`, but it is always
opt-in and defaults to `false`. The safe production shape remains:
read/listen, write the sink durably, then call `cdc_commit`.

## One Consumer, One Logical Reader

One consumer name means one logical reader. The extension enforces this with an
owner-token lease stored on the consumer row.

The holder can call `cdc_window` repeatedly and get the same window until it
commits. Another connection trying to read the same consumer gets `CDC_BUSY`.

Parallel reads are still possible inside one window: an orchestrator can hold
the consumer lease, fan out ordinary `table_changes` reads, then commit once.

## DDL and DML Are Separate Streams

DDL changes are part of the stream, but DDL and DML have different subscription
contracts. DDL is the discovery/control-plane stream and can be filtered by
catalog, schema, or table. DML is the data-plane stream and is filtered by
concrete table identities.

Applications that want "all future tables in schema X" should create a DDL
consumer, watch for table creation, apply their own policy, and then create or
configure DML consumers. The extension should not implicitly subscribe a DML
consumer to future tables.

## Schema Changes Are Boundaries

`cdc_window` reports whether schema changes are pending. Consumers that process
both DDL and DML should apply DDL before DML for the same snapshot.

This keeps consumers from applying rows under a downstream schema that has not
been updated yet, while still letting DDL-only consumers act as lightweight
orchestrators.

## Values Stay Native in SQL

The typed table DML path exposes DuckLake values as DuckDB values. The
consumer-level multi-table DML path uses a generic payload because heterogeneous
table schemas cannot share one typed result set. Serialization choices such as
GeoJSON, base64, Avro, or sink-specific envelopes belong in clients or sinks,
not in the extension.

## Release Flow

Releases are manual and pragmatic:

- day-to-day CI stays relatively small;
- release CI builds the full DuckDB extension matrix;
- community extension publishing is the main distribution path.

The project should prefer clear docs, focused tests, and honest limitations
over process-heavy release gates.

## What Is Deferred

The next serious client target is Python. Go and other bindings are deferred
until there is clear outside demand or another maintainer wants to own them.
