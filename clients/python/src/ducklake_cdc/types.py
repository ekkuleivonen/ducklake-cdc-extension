"""Data shapes and sink protocols for the high-level CDC client.

Sinks are the only output path from a consumer. A consumer reads a window of
DML changes, packages them into a :class:`DMLBatch`, and hands the batch to
each attached sink. The sink decides what to do with it.

Implicit ack/nack is the contract: a sink that returns from ``write`` without
raising acknowledges the batch. A sink that raises nacks it, and the consumer
will retry the same batch (no commit happens). Required sinks gate the commit;
optional sinks (``require_ack=False``) do not.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from ducklake_cdc.enums import ChangeType


@dataclass(frozen=True)
class Change:
    """A single DML change row delivered to a sink.

    The ``kind`` mirrors the SQL extension's emission: ``insert``, ``delete``,
    plus the two halves of an update (``update_preimage`` and
    ``update_postimage``). Pre/post images are kept distinct because they
    carry different ``produced_ns`` semantics — collapsing them loses
    end-to-end latency fidelity.

    For idempotency, sinks should treat ``(snapshot_id, table, rowid, kind)``
    as the stable identity of a change. ``rowid`` is per-table and may be
    reused after compaction, which is why ``snapshot_id`` and ``kind`` are
    part of the key.
    """

    kind: ChangeType
    snapshot_id: int
    table: str | None
    schema: str | None
    rowid: int | None
    snapshot_time: datetime | None
    values: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "snapshot_id": self.snapshot_id,
            "table": self.table,
            "schema": self.schema,
            "rowid": self.rowid,
            "snapshot_time": (
                self.snapshot_time.isoformat() if self.snapshot_time is not None else None
            ),
            "values": dict(self.values),
        }


@dataclass(frozen=True)
class DMLBatch:
    """A pre-commit window of DML changes delivered to sinks.

    The consumer commits to the extension only after every required sink
    returns from ``write`` without raising. ``batch_id`` is stable across
    retries: a batch with the same ``(consumer_name, start_snapshot,
    end_snapshot)`` tuple has the same ``batch_id``. Sinks may use
    ``batch_id`` for crash-recovery dedup.
    """

    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int
    snapshot_ids: tuple[int, ...]
    received_at: datetime
    changes: tuple[Change, ...]

    def __iter__(self) -> Iterator[Change]:
        return iter(self.changes)

    def __len__(self) -> int:
        return len(self.changes)

    @staticmethod
    def derive_batch_id(consumer_name: str, start_snapshot: int, end_snapshot: int) -> str:
        return f"{consumer_name}/{start_snapshot}-{end_snapshot}"


@dataclass(frozen=True)
class SinkAck:
    """Result of an explicit ``batch.ack()`` / ``batch.nack()`` call.

    Implicit ack/nack (return / raise from ``write``) is the headline
    contract; this dataclass exists for sinks that want to record an
    acknowledgement without raising.
    """

    sink: str
    batch_id: str
    ok: bool = True
    detail: str | None = None


HeartbeatFn = Callable[[], None]


@dataclass(frozen=True)
class SinkContext:
    """Per-batch context handed to a sink's ``write`` call.

    ``heartbeat()`` lets a slow sink keep the consumer's lease alive without
    exposing heartbeat as ordinary public API. Sinks that finish a batch
    quickly do not need to call it.
    """

    consumer_name: str
    batch_id: str
    _heartbeat: HeartbeatFn

    def heartbeat(self) -> None:
        self._heartbeat()


@runtime_checkable
class DMLSink(Protocol):
    """Protocol for a sink that consumes DML batches.

    ``open`` and ``close`` are called once per consumer ``with`` block.
    ``write`` is called once per batch, before commit. Returning normally
    acks the batch; raising nacks it and triggers retry.

    Sinks with ``require_ack=False`` do not gate commit — their failures are
    logged and swallowed. Use this for fire-and-forget observability sinks.
    """

    name: str
    require_ack: bool

    def open(self) -> None: ...

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None: ...

    def close(self) -> None: ...


class BaseDMLSink:
    """Convenience base class for DML sinks.

    Subclass and override ``write``. ``open`` and ``close`` are no-ops by
    default; override them if the sink owns external resources.
    """

    name: str = "sink"
    require_ack: bool = True

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        raise NotImplementedError(f"sink {self.name!r} must implement write(batch, ctx)")
