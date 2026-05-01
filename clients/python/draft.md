# Python Client Draft

This draft captures the intended next shape of the Python CDC client. The goal is
to make stateful consumers and composable sinks the primary DX, while keeping a
lower-level client for admin and stateless SQL API calls.

## Core Idea

Users should work mostly with two stateful consumer classes:

- `DmlConsumer`
- `DdlConsumer`

Each consumer owns:

- `name`
- filters/subscriptions
- `DuckLake` connection
- attached sinks
- `start_at`
- `on_exists`

The constructor should create or attach to the durable CDC consumer under the
hood by calling either `cdc_dml_consumer_create` or `cdc_ddl_consumer_create`.

## Sinks

Sinks are pluggable delivery targets. They digest whatever a consumer produces.
A consumer can have multiple sinks, and all required sinks must acknowledge a
batch before the consumer commits back to the extension.

First sink examples:

- `StdoutSink`
- `FileSink`
- `RedisStreamSink`
- `RedisQueueSink`
- `PostgresCopySink`
- `PostgresReplicationSink`
- `ParquetSink`
- `WebhookSink`

The important part is the protocol, so users can define their own sinks.

```python
class Sink(Protocol):
    name: str
    require_ack: bool

    def open(self) -> None: ...

    def write(self, batch: ConsumerBatch, context: SinkContext) -> SinkAck: ...

    def close(self) -> None: ...
```

`SinkContext` should expose heartbeat internally, so slow sinks can keep the
consumer lease alive without exposing heartbeat as normal public API.

```python
class SinkContext(Protocol):
    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int | None

    def heartbeat(self) -> None: ...
```

```python
@dataclass(frozen=True)
class SinkAck:
    sink: str
    batch_id: str
    ok: bool = True
    detail: str | None = None
```

## Commit Semantics

Default delivery should be sink-gated:

1. Consumer listens or reads a batch.
2. Consumer passes the batch to all attached sinks.
3. Required sinks must ack.
4. Consumer commits to the extension.

This gives at-least-once delivery. If a sink writes successfully and the process
crashes before `cdc_commit`, the same batch can be delivered again on restart.
Sinks should use stable batch/event identities for idempotency.

Useful batch identity fields:

- `consumer_name`
- `start_snapshot`
- `end_snapshot`
- `snapshot_id`
- `table_name`
- `rowid`
- `change_type`

If `auto_commit=True`, the SQL function commits before sink acknowledgement can
gate delivery. Sinks still receive the results, but the Python sink layer no
longer controls extension commit safety.

## Consumer Constructors

Example DML consumer:

```python
consumer = cdc.dml_consumer(
    "orders",
    tables=["main.orders"],
    change_types=["insert", "update", "delete"],
    start_at="now",
    on_exists="use",
    sinks=[StdoutSink(format="jsonl")],
)
```

Example DDL consumer:

```python
consumer = cdc.ddl_consumer(
    "schema_audit",
    schemas=["main"],
    start_at="beginning",
    on_exists="replace",
    sinks=[FileSink("ddl.jsonl")],
)
```

`on_exists` should support:

- `"error"`: fail if the consumer already exists.
- `"use"`: attach to the existing consumer without recreating filters.
- `"replace"`: drop the existing consumer and create a fresh one.
- `"force_release"`: keep the existing consumer but force-release its lease.

## Stateful Consumer Methods

Both `DmlConsumer` and `DdlConsumer` should expose:

- `reset(...)`
- `drop()`
- `force_release()`
- `window(...)`
- `changes_listen(...)`
- `changes_read(...)`
- `ticks_listen(...)`
- `ticks_read(...)`

`DmlConsumer` also exposes:

- `table_changes_listen(...)`
- `table_changes_read(...)`

Heartbeat should not be a normal public method. Long-running listen loops should
handle heartbeat internally and pass heartbeat capability to sinks through
`SinkContext`.

## Listen Methods

`changes_listen` and `ticks_listen` should accept:

```python
consumer.changes_listen(
    timeout_ms=1000,
    max_snapshots=100,
    auto_commit=False,
    infinite=True,
)
```

```python
consumer.ticks_listen(
    timeout_ms=1000,
    max_snapshots=100,
    auto_commit=False,
    infinite=True,
)
```

Semantics:

- `infinite=False`: call the underlying SQL listen once, deliver one result or
  batch to sinks, optionally commit, then return.
- `infinite=True`: keep calling listen until stopped or interrupted.
- `auto_commit=False`: required sink acknowledgements gate `cdc_commit`.
- `auto_commit=True`: pass through to SQL `auto_commit`; sink acknowledgements no
  longer gate extension commit.

## Low-Level Client

Keep a lower-level `DuckLakeCdcClient` for admin and stateless APIs:

- `version()`
- `doctor(...)`
- `consumers()`
- `subscriptions(...)`
- `consumer_stats(...)`
- `audit_events(...)`
- `ddl_changes_query(...)`
- `dml_changes_query(...)`
- `ddl_ticks_query(...)`
- `dml_ticks_query(...)`
- `schema_diff(...)`
- factories for `dml_consumer(...)` and `ddl_consumer(...)`

The current large SQL-mirror `CDCClient` can become this lower-level client, but
stateful consumers should be the main user-facing surface.

## Important Implementation Note

The current `iter_consumer_batches()` commits before yielding because
`ConsumerBatch` contains `commit`. That needs to change for sink-gated delivery.

The new loop should produce a pre-commit batch, deliver it to sinks, then commit
only after required sinks acknowledge.
