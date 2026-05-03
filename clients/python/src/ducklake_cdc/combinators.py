"""Sink combinators for the high-level CDC client.

A combinator wraps one or more sinks and looks like a sink itself. The three
v1 combinators are:

- :class:`MapDMLSink` / :class:`MapDDLSink` — transform each event before
  forwarding the (re-built) batch to the inner sink.
- :class:`FilterDMLSink` / :class:`FilterDDLSink` — drop events that fail
  a predicate, then forward the (possibly empty) batch.
- :class:`FanoutDMLSink` / :class:`FanoutDDLSink` — broadcast the same
  batch to several inner sinks. ``require_ack=False`` inner sinks do not
  gate ack; their failures are logged.

``open`` and ``close`` propagate to inner sinks and are tolerant of partial
open failures: if one inner sink raises during ``open``, the combinator
closes any sinks that opened successfully and re-raises.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from typing import Any

from ducklake_cdc.types import (
    BaseDDLSink,
    BaseDMLSink,
    Change,
    DDLBatch,
    DDLSink,
    DMLBatch,
    DMLSink,
    SchemaChange,
    SinkContext,
)

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------


class MapDMLSink(BaseDMLSink):
    """Transform each :class:`Change` then forward to ``sink``.

    The new batch reuses the original ``batch_id`` and snapshot range so
    downstream idempotency keys still match across retries. ``fn`` is
    expected to be cheap and side-effect-free; raise to nack the whole
    batch.
    """

    def __init__(
        self,
        fn: Callable[[Change], Change],
        sink: DMLSink,
        *,
        name: str | None = None,
    ) -> None:
        self._fn = fn
        self._inner = sink
        self.name = name or f"map({_inner_name(sink)})"
        self.require_ack = getattr(sink, "require_ack", True)

    def open(self) -> None:
        self._inner.open()

    def close(self) -> None:
        self._inner.close()

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        mapped = tuple(self._fn(change) for change in batch.changes)
        new_batch = DMLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=mapped,
        )
        self._inner.write(new_batch, ctx)


class MapDDLSink(BaseDDLSink):
    """Transform each :class:`SchemaChange` then forward to ``sink``."""

    def __init__(
        self,
        fn: Callable[[SchemaChange], SchemaChange],
        sink: DDLSink,
        *,
        name: str | None = None,
    ) -> None:
        self._fn = fn
        self._inner = sink
        self.name = name or f"map({_inner_name(sink)})"
        self.require_ack = getattr(sink, "require_ack", True)

    def open(self) -> None:
        self._inner.open()

    def close(self) -> None:
        self._inner.close()

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        mapped = tuple(self._fn(event) for event in batch.changes)
        new_batch = DDLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=mapped,
        )
        self._inner.write(new_batch, ctx)


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------


class FilterDMLSink(BaseDMLSink):
    """Forward only :class:`Change`s where ``predicate`` returns truthy.

    A batch with every change filtered out is still forwarded so the inner
    sink can observe the snapshot window — and so that the consumer
    advances its cursor.
    """

    def __init__(
        self,
        predicate: Callable[[Change], bool],
        sink: DMLSink,
        *,
        name: str | None = None,
    ) -> None:
        self._predicate = predicate
        self._inner = sink
        self.name = name or f"filter({_inner_name(sink)})"
        self.require_ack = getattr(sink, "require_ack", True)

    def open(self) -> None:
        self._inner.open()

    def close(self) -> None:
        self._inner.close()

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        kept = tuple(change for change in batch.changes if self._predicate(change))
        new_batch = DMLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=kept,
        )
        self._inner.write(new_batch, ctx)


class FilterDDLSink(BaseDDLSink):
    """Forward only :class:`SchemaChange`s where ``predicate`` returns truthy."""

    def __init__(
        self,
        predicate: Callable[[SchemaChange], bool],
        sink: DDLSink,
        *,
        name: str | None = None,
    ) -> None:
        self._predicate = predicate
        self._inner = sink
        self.name = name or f"filter({_inner_name(sink)})"
        self.require_ack = getattr(sink, "require_ack", True)

    def open(self) -> None:
        self._inner.open()

    def close(self) -> None:
        self._inner.close()

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        kept = tuple(event for event in batch.changes if self._predicate(event))
        new_batch = DDLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=kept,
        )
        self._inner.write(new_batch, ctx)


# ---------------------------------------------------------------------------
# Fanout
# ---------------------------------------------------------------------------


class FanoutDMLSink(BaseDMLSink):
    """Broadcast each batch to every inner sink.

    ``require_ack`` of the combinator is the OR of its inner sinks: if any
    inner sink requires ack, the combinator does too. Failures from
    optional inner sinks (``require_ack=False``) are logged and swallowed
    so they cannot gate the consumer's commit.
    """

    def __init__(
        self,
        *sinks: DMLSink,
        name: str | None = None,
    ) -> None:
        if not sinks:
            raise ValueError("FanoutDMLSink requires at least one inner sink")
        self._inner: tuple[DMLSink, ...] = sinks
        self.name = name or f"fanout({','.join(_inner_name(s) for s in sinks)})"
        self.require_ack = any(getattr(s, "require_ack", True) for s in sinks)

    def open(self) -> None:
        _open_all(self._inner)

    def close(self) -> None:
        _close_all(self._inner)

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        for sink in self._inner:
            try:
                sink.write(batch, ctx)
            except Exception as exc:
                if getattr(sink, "require_ack", True):
                    raise
                _LOG.warning(
                    "optional fanout sink %r raised on batch %s: %s",
                    _inner_name(sink),
                    batch.batch_id,
                    exc,
                )


class FanoutDDLSink(BaseDDLSink):
    """Broadcast each DDL batch to every inner sink."""

    def __init__(
        self,
        *sinks: DDLSink,
        name: str | None = None,
    ) -> None:
        if not sinks:
            raise ValueError("FanoutDDLSink requires at least one inner sink")
        self._inner: tuple[DDLSink, ...] = sinks
        self.name = name or f"fanout({','.join(_inner_name(s) for s in sinks)})"
        self.require_ack = any(getattr(s, "require_ack", True) for s in sinks)

    def open(self) -> None:
        _open_all(self._inner)

    def close(self) -> None:
        _close_all(self._inner)

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        for sink in self._inner:
            try:
                sink.write(batch, ctx)
            except Exception as exc:
                if getattr(sink, "require_ack", True):
                    raise
                _LOG.warning(
                    "optional fanout sink %r raised on batch %s: %s",
                    _inner_name(sink),
                    batch.batch_id,
                    exc,
                )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _inner_name(sink: Any) -> str:
    return str(getattr(sink, "name", type(sink).__name__))


def _open_all(sinks: Iterable[Any]) -> None:
    opened: list[Any] = []
    try:
        for sink in sinks:
            sink.open()
            opened.append(sink)
    except BaseException:
        for sink in reversed(opened):
            try:
                sink.close()
            except Exception:
                _LOG.exception("error closing sink %r during rollback", _inner_name(sink))
        raise


def _close_all(sinks: Iterable[Any]) -> None:
    for sink in reversed(list(sinks)):
        try:
            sink.close()
        except Exception:
            _LOG.exception("error closing sink %r", _inner_name(sink))
