# Python Client Draft

This draft captures the agreed shape of the high-level Python CDC client.
Stateful consumers and composable sinks are the primary DX. A low-level client
remains as an escape hatch for power users who need direct access to the SQL
extension surface.

## Core Primitives

The library exposes six primitives. Everything else (built-in sinks,
combinators, helpers) is built from these.

- `CDCClient` — Low-level 1:1 mirror of the SQL extension API. Used internally
  by the higher-level primitives. Available as an escape hatch but not the
  headline API.
- `DMLConsumer` — Durable consumer for row-level changes (insert/update/delete).
- `DDLConsumer` — Durable consumer for schema changes.
- `DMLSink` — Protocol for sinks consuming DML batches.
- `DDLSink` — Protocol for sinks consuming DDL batches.
- `CDCApp` — Multi-consumer runtime host. Runs N consumers concurrently in one
  process with shared lifecycle and signal handling.

The DML/DDL split is kept visible at both consumer and sink levels. They have
different payload shapes and the type checker should catch a `RedisDMLSink`
attached to a `DDLConsumer` at construction time, not at first event.

`CDCApp` is named with the `CDC` prefix (not bare `App`) to avoid shadowing
common variables in user code. FastAPI, Flask, and Typer all assume `app =
App()` is available.

### `CDCClient` Placement

`CDCClient` is imported from `ducklake_cdc.lowlevel`, not the top-level
`ducklake_cdc` namespace. The import path signals "you're on your own for
semantics."

```python
from ducklake_cdc.lowlevel import CDCClient
```

Method names match the SQL extension function names exactly
(`cdc_dml_consumer_create`, not `create_dml_consumer`). The low-level client is
a thin transport layer; it does not reorder arguments or rename functions.

## Sinks Are the Only Output Path

Sinks are the one and only way to get data out of a consumer. There is no
iterator API on consumers, no `for batch in consumer.changes_listen()`, and no
decorator-registered handlers as a parallel surface.

The consumer's job is to deliver batches to sinks. The sink's job is to decide
what to do with them.

This kills several pieces of complexity from earlier iterations:

- No `@cdc.sink` / `@cdc.batch_sink` split.
- No `App` + `@app.on_change` decorator layer separate from `Consumer`.
- No iterator/runner duality.
- No `auto_commit` exposure on the headline API.

### Iteration Lives Inside Sinks

The iterator pattern is not removed — it is relocated. Batches are iterable;
sinks decide whether to iterate them.

```python
class ConsumerBatch(Protocol):
    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int | None
    snapshot_ids: list[int]       # all snapshots in the batch
    received_at: datetime          # when consumer pulled from extension

    def __iter__(self) -> Iterator[Change]: ...
    def __len__(self) -> int: ...

    def ack(self, sink: str, detail: str | None = None) -> SinkAck: ...
    def nack(self, sink: str, detail: str | None = None) -> SinkAck: ...
```

A per-change sink iterates the batch in its `write` method. A per-batch sink
(e.g. `PostgresCopySink`) writes the whole batch in one operation. Same
protocol, same `write` signature; the sink body decides granularity.

## Sink Protocol

```python
class DMLSink(Protocol):
    name: str
    require_ack: bool

    def open(self) -> None: ...                        # optional, default no-op
    def write(self, batch: DMLBatch, ctx: SinkContext) -> None: ...
    def close(self) -> None: ...                       # optional, default no-op
```

`DDLSink` is structurally identical but typed against `DDLBatch`. `DMLBatch`
and `DDLBatch` are `ConsumerBatch` parameterized over `Change` and
`SchemaChange` respectively (see [Data Shapes](#data-shapes)).

`open` and `close` are optional in the Protocol. `CallableSink` provides no-op
defaults so a callable sink can be a single function.

`require_ack` defaults to `True`. Optional sinks (`require_ack=False`) do not
gate commit; their failures are logged but do not nack the batch. This is the
escape hatch for fire-and-forget sinks like a metrics emitter that should never
hold up delivery.

### Callable Sinks

A function `(batch, ctx) -> None` is a valid sink. The two-argument form is the
canonical shape; the single-argument form `(batch) -> None` is also accepted by
dropping `ctx` if the user does not ask for it. Cheap signature inspection at
registration time picks the shape and fails loudly on bad signatures.

### `SinkContext`

`SinkContext` exposes heartbeat internally so slow sinks can keep the consumer
lease alive without exposing heartbeat as ordinary public API.

```python
class SinkContext(Protocol):
    consumer_name: str
    batch_id: str

    def heartbeat(self) -> None: ...
```

The acknowledgement shape returned by `batch.ack(...)` and `batch.nack(...)` is
`SinkAck`, defined alongside the other data shapes (see
[Data Shapes](#data-shapes)).

## Commit Semantics

Sink-gated, at-least-once delivery is the default and the marketing line.

Flow:

1. Consumer reads a batch.
2. Consumer passes it to all attached sinks.
3. Required sinks must `batch.ack(...)` (or return without raising — see below).
4. Consumer commits to the extension only after all required sinks ack.

### Implicit Ack/Nack

Implicit ack/nack is the contract:

- Return without raising = ack.
- Raise = nack = retry.

Explicit `batch.ack(...)` and `batch.nack(detail=...)` remain available for the
rare "fail this without raising" case, but they do not appear in the quickstart
docs.

If a sink writes successfully and the process crashes before `cdc_commit`, the
same batch can be delivered again on restart. Sinks must use stable batch and
event identities for idempotency.

Useful identity fields:

- `consumer_name`, `batch_id`
- `start_snapshot`, `end_snapshot`, `snapshot_id`
- `table`, `rowid`, `kind`

### `auto_commit`

`auto_commit=True` is kept as a hazard, not redesigned today. It is logged in
`docs/hazard-log.md` as `H-021` and accepted for now. It is hidden from the
high-level consumer headline; it is only meaningful when dropping to
`CDCClient`.

### Partial Success

All-or-nothing for v1. Any sink failure on a batch nacks the whole batch and
the whole batch is retried. Sinks must be idempotent. Per-row ack and
dead-letter routing are v2 problems.

## Lifecycle — Context Managers

Consumers and `CDCApp` are context managers. Sinks `open` on enter and `close`
on exit. Lease state is owned by the consumer or app for the duration of the
`with` block.

There is no standalone `close()` method on `CDCApp`. The `with` block handles
it.

```python
with consumer:
    consumer.run(infinite=True)
```

```python
with CDCApp(consumers=[c1, c2]) as app:
    app.run(infinite=True)
```

## `run()` Method Shape

One `run()` method on `DMLConsumer`, `DDLConsumer`, and `CDCApp`. Same
signature, same semantics:

- `run(infinite=False)` — cron / one-shot. Polls once, delivers, commits, exits.
- `run(infinite=True)` — long-running. Polls continuously past listen deadlines
  until interrupted (e.g. `SIGINT`).

There is no separate `run_once()` method. The kwarg flips behavior.

Listen-vocabulary methods (`changes_listen`, `ticks_listen`, `changes_read`)
are not on the headline API. They live on `CDCClient` for power users who need
them.

## Heartbeat

Hidden from the public consumer API. Long-running listen loops handle
heartbeat internally and pass heartbeat capability to sinks via `SinkContext`.
Slow sinks call `ctx.heartbeat()` to keep the consumer lease alive without
exposing heartbeat as ordinary public API.

## Consumer Construction

```python
DMLConsumer(
    lake=lake,
    name="consumer_1",
    tables=["public.users"],
    change_types=["insert", "update", "delete"],
    start_at="now",          # or "beginning" or a snapshot ID
    on_exists="use",          # "error" | "use" | "replace"
    sinks=[...],
)
```

`on_exists` describes only creation/attachment behavior:

- `"error"`: fail if the consumer already exists.
- `"use"`: attach to the existing consumer without recreating filters.
- `"replace"`: drop the existing consumer and create a fresh one.

Lease takeover is a separate concern via `lease_policy`:

- `"wait"` (default): wait for the current holder, with a configurable timeout.
- `"takeover"`: force-release the existing lease and acquire it.
- `"error"`: fail if a lease is already held.

The DB-DAG / multi-consumer reconciler use case will reach for `"takeover"`;
the default stays safe.

`DDLConsumer` mirrors this with `schemas=[...]` instead of `tables=[...]`.

`lake` is a keyword argument in all examples for consistency.

## Multi-Consumer (`CDCApp`)

`CDCApp` is what you reach for when you have a list of consumers.
Single-consumer code does not need `CDCApp` — it uses the consumer's own
`run()`.

Both static and dynamic registration are supported:

```python
# Headline shape — list at construction
app = CDCApp(consumers=[c1, c2])

# Dynamic shape — for the DB-driven DAG use case
app = CDCApp()
app.add_consumer(c1)
app.add_consumer(c2)
app.add_consumers([c3, c4])
```

`add_consumer` and `remove_consumer` work on a running app for reconciliation
use cases (DB-defined nodes, hot-add and hot-remove). Implementation details
(thread per consumer vs asyncio task) are internal and not exposed in the
public API.

`CDCApp.stats()` exposes per-consumer health: last heartbeat, last commit, lag
in snapshots, last error. This is required for "this scales" — without it, 50
consumers in one process is unobservable. The library does not ship a
dashboard; it just exposes the data.

## Headline Examples

Single consumer:

```python
import os
from ducklake import DuckLake, CatalogConfig, PostgresCatalog, LocalStorage
from ducklake_cdc import DMLConsumer, StdoutDMLSink

lake = DuckLake(
    catalog=CatalogConfig(path=os.getenv("DUCKLAKE_CATALOG_PATH")),
    metadata=PostgresCatalog(dsn=os.getenv("DUCKLAKE_METADATA_DSN")),
    storage=LocalStorage(path=os.getenv("DUCKLAKE_STORAGE_PATH")),
)

consumer = DMLConsumer(
    lake=lake,
    name="orders",
    tables=["main.orders"],
    sinks=[StdoutDMLSink()],
)

with consumer:
    consumer.run(infinite=True)
```

Multiple consumers:

```python
ddl = DDLConsumer(
    lake=lake, name="ddl",
    schemas=["public"], start_at="beginning",
    sinks=[StdoutDDLSink()],
)

dml = DMLConsumer(
    lake=lake, name="dml",
    tables=["public.users"], start_at="beginning",
    sinks=[StdoutDMLSink()],
)

with CDCApp(consumers=[ddl, dml]) as app:
    app.run(infinite=True)
```

## Data Shapes

```python
@dataclass(frozen=True)
class Change:
    table: str
    kind: Literal["insert", "update", "delete"]
    snapshot_id: int
    rowid: int
    values: dict[str, Any]
    before: dict[str, Any] | None     # for updates/deletes
```

`rowid` is per-table and may be reused after compaction. Sinks that need a
strict global identity for idempotency should use the composite key
`(snapshot_id, table, rowid, kind)`. This is the same identity surface
referenced in [Implicit Ack/Nack](#implicit-acknack); it lives next to the
`Change` definition so the connection is easy to find.

`SchemaChange` mirrors `Change` for DDL with appropriate fields.

`DMLBatch` and `DDLBatch` are the concrete batch types delivered to sinks. They
are `ConsumerBatch` parameterized over `Change` and `SchemaChange`
respectively:

```python
DMLBatch: TypeAlias = ConsumerBatch[Change]
DDLBatch: TypeAlias = ConsumerBatch[SchemaChange]
```

```python
@dataclass(frozen=True)
class SinkAck:
    sink: str
    batch_id: str
    ok: bool = True
    detail: str | None = None
```

## Sink Library — v1 Scope

The core library ships dependency-light. Network and IO integrations are
separate distributions in the same repo (`ducklake-cdc-redis`,
`ducklake-cdc-postgres`) so that `pip install ducklake-cdc` is fast.

### v1 Core Sinks (per consumer type)

- `StdoutDMLSink` / `StdoutDDLSink` — first impression.
- `FileDMLSink` / `FileDDLSink` — local persistence (jsonl by default, infer
  from extension).
- `MemoryDMLSink` / `MemoryDDLSink` — notebooks and tests; iterable for the
  user. **TODO**: pin down the iteration shape before implementation. Options:
  `for change in sink:` (flat iterable of changes), `sink.changes` (list
  property), `sink.batches` (list of batches). The notebook DX promise rests on
  this being good; revisit when starting step 2 of the build order.
- `CallableDMLSink` / `CallableDDLSink` — wraps a function; powers
  callable-as-sink.

### v1 Core Combinators (per consumer type)

- `MapDMLSink(fn, sink)` / `MapDDLSink(fn, sink)`
- `FilterDMLSink(predicate, sink)` / `FilterDDLSink(predicate, sink)`
- `FanoutDMLSink(*sinks)` / `FanoutDDLSink(*sinks)`

### Explicitly NOT Shipping in v1 Core

- `BatchingSink` — the extension already produces batches; re-batching is the
  user's downstream concern.
- `RetrySink` — `tenacity` and similar libs already do this; exception-as-nack
  composes for free.
- `DeadLetterSink` — pattern, not a class. Document `try/except` plus a
  fallback sink.
- `LoggingSink` — nice-to-have, not foundational.

The "is this our concern?" test: re-batching, debouncing, windowing,
retry-with-backoff, and DLQ routing all fail this test. They are user-side
concerns that depend on the user's destination. We do not ship them.

## Build Order

Discipline that prevents protocol revisions late in the cycle:

1. `CDCClient` (mostly already exists).
2. `DMLSink` protocol, `DMLConsumer`, `StdoutDMLSink`, `MemoryDMLSink`.
3. `CDCApp` running one `DMLConsumer` end-to-end.
4. `DDLSink` protocol, `DDLConsumer`, `StdoutDDLSink` (mechanical at this
   point).
5. `CDCApp` running both consumer types.
6. Expand sink library: `File*`, `Map*`, `Filter*`, `Fanout*`, `Callable*`.

By step 3, every awkward corner of the protocol has been hit — context manager
semantics, sink open/close ordering, lease handling on shutdown, signal
handling in `CDCApp.run()`. Discovering them later forces revisions to a wider
surface.

## Hazards Logged (Not Redesigned)

- `H-021`: `auto_commit=True` bypasses sink-gated delivery. Accepted for now;
  not exposed on the high-level API.
- Lease takeover semantics: `lease_policy="wait" | "takeover" | "error"` shape
  agreed; exact timeout and retry behavior to firm up during implementation.

## Deferred Decisions

These are decisions the design intentionally does not pin down. They will be
made during implementation, not during design review. Listed here so they do
not get lost.

### Signal Handling in `CDCApp.run()`

- Agreed: `CDCApp.run()` handles process signals on behalf of all consumers.
- Deferred: `SIGTERM` vs `SIGINT` semantics, in-flight batch draining behavior
  on shutdown, and the timeout for graceful shutdown before forced stop.
- Likely shape: both signals trigger a graceful shutdown that drains in-flight
  batches up to a configurable timeout, then aborts.
- Surfaces in: build-order step 3 (first end-to-end `CDCApp` with one
  consumer).

### `CDCApp.stats()` Return Shape

- Agreed: per-consumer health surface — last heartbeat, last commit, lag in
  snapshots, last error.
- Deferred: concrete dataclass shape, field names, and whether `stats()`
  returns a list, a dict keyed by consumer name, or a richer aggregate type.
- Surfaces in: build-order step 5 (`CDCApp` running both consumer types,
  observability becomes load-bearing).

### `lease_policy` Timeout Default

- Agreed: `lease_policy="wait"` accepts a configurable timeout.
- Deferred: the default value, and the retry / backoff shape inside the wait
  loop.
- Cross-reference: this is one slice of the broader lease-takeover hazard
  noted above.

### Concurrency Model in `CDCApp`

- Agreed: the choice between thread per consumer and asyncio task per consumer
  is an internal implementation detail and not part of the public API.
- Deferred: which one to actually build.
- Likely v1: threads. The existing low-level client blocks on extension calls,
  and threads are the path of least resistance until that changes.

## What This Design Does Not Include (By Choice)

- No DAG / topology / graph engine. Users build their own DAGs from the
  consumer primitives plus `CDCApp`. The DB-driven dynamic-pipeline case is
  served by `add_consumer` plus a user-written reconciler, not by a built-in
  DAG type.
- No `tail()` shortcut as a separate API layer. If wanted, it is a ten-line
  helper, not a layer.
- No decorator-based handler registration (`@app.on_change`). Sinks-as-list is
  the one way.
- No iterator API on consumers. Iteration lives inside sinks.
- No per-row partial success. All-or-nothing batches in v1.
