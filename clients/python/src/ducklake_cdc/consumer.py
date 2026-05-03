"""High-level DML consumer for the ducklake-cdc extension.

A :class:`DMLConsumer` is a context manager that owns the consumer's
lifecycle: it creates (or attaches to) a durable consumer in the extension,
opens its sinks, runs a sink-gated listen+deliver+commit loop, and closes
the sinks on exit.

The headline shape::

    with DMLConsumer(lake, "orders", tables=["public.orders"], sinks=[StdoutDMLSink()]) as c:
        c.run()

Sinks are the only output path. Returning from ``sink.write`` acks; raising
nacks and the batch is left uncommitted (so it will be redelivered on the
next listen call).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from types import TracebackType
from typing import Literal, TypeVar

from ducklake import DuckLake

from ducklake_cdc.lowlevel import CDCClient, ChangeRow
from ducklake_cdc.types import Change, DMLBatch, DMLSink, SinkContext

T = TypeVar("T")

OnExists = Literal["error", "use", "replace"]
StartAt = str | int

RetryPolicy = Callable[[Callable[[], object]], object]

_LOG = logging.getLogger(__name__)


class DMLConsumer:
    """Durable consumer for row-level DML changes.

    Construction is cheap — no SQL is issued. The ``with`` block does the
    work: it creates or attaches to the consumer, opens sinks, and (on exit)
    closes sinks.

    ``on_exists`` controls creation/attachment behavior:

    - ``"use"`` (default): attach to an existing consumer if present, else
      create. Filters are not changed.
    - ``"error"``: fail if the consumer already exists.
    - ``"replace"``: drop and recreate. Force-releases the lease first.

    ``start_at`` accepts ``"now"``, ``"beginning"``, ``"oldest"``, or a
    snapshot id. The consumer is created with ``start_at="now"`` and then
    reset to the requested point — the SQL extension's reset accepts the
    full vocabulary. ``start_at`` is ignored for ``on_exists="use"`` when
    the consumer already exists, to avoid silently rewinding an existing
    durable cursor.
    """

    def __init__(
        self,
        lake: DuckLake,
        name: str,
        *,
        tables: Sequence[str] | None = None,
        change_types: Sequence[str] | None = None,
        start_at: StartAt = "now",
        on_exists: OnExists = "use",
        sinks: Sequence[DMLSink] = (),
        client: CDCClient | None = None,
        retry: RetryPolicy | None = None,
    ) -> None:
        if not name:
            raise ValueError("DMLConsumer requires a non-empty name")
        if not sinks:
            raise ValueError(
                "DMLConsumer requires at least one sink — sinks are the only "
                "output path. Pass StdoutDMLSink() if you just want to see "
                "events."
            )

        self._lake = lake
        self._name = name
        self._tables = list(tables) if tables else None
        self._change_types = list(change_types) if change_types else None
        self._start_at = start_at
        self._on_exists = on_exists
        self._sinks: list[DMLSink] = list(sinks)
        self._client = client
        self._retry_policy = retry
        self._opened = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def client(self) -> CDCClient:
        if self._client is None:
            raise RuntimeError(
                "DMLConsumer.client is only available inside a `with` block"
            )
        return self._client

    def __enter__(self) -> DMLConsumer:
        if self._client is None:
            self._client = CDCClient(self._lake)
        try:
            self._setup_consumer()
            self._open_sinks()
            self._opened = True
        except BaseException:
            self._close_sinks_quietly()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._opened = False
        self._close_sinks_quietly()

    def run(
        self,
        *,
        infinite: bool = True,
        max_batches: int = 0,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
        idle_timeout: float = 0.0,
    ) -> int:
        """Run the listen+deliver+commit loop.

        Stops when any of these fires:

        - ``infinite=False`` and one batch has been delivered (one-shot).
        - ``max_batches > 0`` and that many batches have been delivered.
        - ``idle_timeout > 0`` and that many seconds have passed without a
          non-empty batch.
        - ``KeyboardInterrupt`` (caller's responsibility to handle).

        Returns the number of batches delivered.
        """

        self._require_open()
        delivered = 0
        last_activity = time.monotonic()

        while True:
            rows = self._retry(self._listen_op(timeout_ms, max_snapshots))
            if not rows:
                if not infinite:
                    return delivered
                if idle_timeout > 0 and (time.monotonic() - last_activity) >= idle_timeout:
                    return delivered
                continue

            last_activity = time.monotonic()
            batch = self._build_batch(rows)
            self._deliver(batch)
            self._retry(self._commit_op(batch.end_snapshot))
            delivered += 1

            if not infinite:
                return delivered
            if max_batches > 0 and delivered >= max_batches:
                return delivered

    def _setup_consumer(self) -> None:
        client = self._require_client()
        name = self._name
        exists = any(entry.consumer_name == name for entry in client.cdc_list_consumers())

        if exists and self._on_exists == "error":
            raise RuntimeError(f"consumer {name!r} already exists")

        if exists and self._on_exists == "replace":
            client.cdc_consumer_force_release(name)
            client.cdc_consumer_drop(name)
            self._create_and_position(client)
            return

        if exists:
            return

        self._create_and_position(client)

    def _create_and_position(self, client: CDCClient) -> None:
        client.cdc_dml_consumer_create(
            self._name,
            table_names=self._tables,
            change_types=self._change_types,
            start_at="now",
        )
        if self._start_at != "now":
            client.cdc_consumer_reset(self._name, to_snapshot=self._start_at)

    def _open_sinks(self) -> None:
        opened: list[DMLSink] = []
        try:
            for sink in self._sinks:
                sink.open()
                opened.append(sink)
        except BaseException:
            for sink in reversed(opened):
                try:
                    sink.close()
                except Exception:
                    _LOG.exception("error closing sink %r during rollback", _sink_name(sink))
            raise

    def _close_sinks_quietly(self) -> None:
        for sink in reversed(self._sinks):
            try:
                sink.close()
            except Exception:
                _LOG.exception("error closing sink %r", _sink_name(sink))

    def _build_batch(self, rows: list[ChangeRow]) -> DMLBatch:
        start = min(
            row.start_snapshot if row.start_snapshot is not None else row.snapshot_id
            for row in rows
        )
        end = max(
            row.end_snapshot if row.end_snapshot is not None else row.snapshot_id
            for row in rows
        )
        snapshot_ids = tuple(sorted({row.snapshot_id for row in rows}))
        changes = tuple(
            Change(
                kind=row.change_type,
                snapshot_id=row.snapshot_id,
                table=row.table_name,
                schema=row.schema_name,
                rowid=row.rowid,
                snapshot_time=row.snapshot_time,
                values=row.values,
            )
            for row in rows
        )
        return DMLBatch(
            consumer_name=self._name,
            batch_id=DMLBatch.derive_batch_id(self._name, start, end),
            start_snapshot=start,
            end_snapshot=end,
            snapshot_ids=snapshot_ids,
            received_at=datetime.now(timezone.utc),
            changes=changes,
        )

    def _deliver(self, batch: DMLBatch) -> None:
        ctx = SinkContext(
            consumer_name=self._name,
            batch_id=batch.batch_id,
            _heartbeat=self._heartbeat,
        )
        for sink in self._sinks:
            try:
                sink.write(batch, ctx)
            except Exception as exc:
                if getattr(sink, "require_ack", True):
                    raise
                _LOG.warning(
                    "optional sink %r raised on batch %s: %s",
                    _sink_name(sink),
                    batch.batch_id,
                    exc,
                )

    def _heartbeat(self) -> None:
        client = self._client
        if client is None:
            return
        self._retry(lambda: client.cdc_consumer_heartbeat(self._name))

    def _listen_op(self, timeout_ms: int, max_snapshots: int) -> Callable[[], list[ChangeRow]]:
        client = self._require_client()
        name = self._name

        def operation() -> list[ChangeRow]:
            return client.cdc_dml_changes_listen(
                name,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
            )

        return operation

    def _commit_op(self, snapshot: int) -> Callable[[], object]:
        client = self._require_client()
        name = self._name

        def operation() -> object:
            return client.cdc_commit(name, snapshot)

        return operation

    def _retry(self, operation: Callable[[], T]) -> T:
        if self._retry_policy is None:
            return operation()
        return self._retry_policy(operation)  # type: ignore[return-value]

    def _require_open(self) -> None:
        if not self._opened:
            raise RuntimeError(
                "DMLConsumer.run() must be called inside a `with consumer:` block"
            )

    def _require_client(self) -> CDCClient:
        if self._client is None:
            raise RuntimeError("DMLConsumer client is not initialized; use `with consumer:`")
        return self._client


def _sink_name(sink: object) -> str:
    return str(getattr(sink, "name", type(sink).__name__))
