"""Built-in sinks for the high-level CDC client.

Network and IO integrations live in separate distributions
(``ducklake-cdc-redis``, ``ducklake-cdc-postgres``). The core library ships
only sinks that depend on nothing outside the Python standard library.

The DML/DDL split is preserved at the sink level so that a type checker
catches a DDL sink attached to a :class:`~ducklake_cdc.DMLConsumer` (and
vice versa) at construction time, not at first event.
"""

from __future__ import annotations

import inspect
import json
import sys
from collections.abc import Callable, Iterator
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from typing import IO, Any

from ducklake_cdc.types import (
    BaseDDLSink,
    BaseDMLSink,
    Change,
    DDLBatch,
    DMLBatch,
    SchemaChange,
    SinkContext,
)

DMLCallable = Callable[..., None]
DDLCallable = Callable[..., None]


# ---------------------------------------------------------------------------
# Stdout sinks
# ---------------------------------------------------------------------------


class StdoutDMLSink(BaseDMLSink):
    """Emit each DML batch as JSON lines to stdout.

    The sink emits three line types per batch:

    - ``{"type": "window", ...}`` once at the start of the batch, with the
      batch identity.
    - ``{"type": "change", ...}`` once per :class:`ducklake_cdc.Change`.
    - ``{"type": "commit", ...}`` once at the end. Note: this line is
      emitted by the sink before the consumer commits to the extension. It
      means "this batch is acked; the consumer is about to commit at this
      snapshot".

    Pass ``stream`` to redirect output (e.g. for tests). The default is
    ``sys.stdout``.
    """

    name = "stdout"
    require_ack = True

    def __init__(self, *, stream: IO[str] | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        _emit_window(self._stream, batch, len(batch))
        for change in batch:
            payload = change.to_dict()
            payload["type"] = "change"
            _emit(self._stream, payload)
        _emit_commit(self._stream, batch)


class StdoutDDLSink(BaseDDLSink):
    """Emit each DDL batch as JSON lines to stdout.

    Mirrors :class:`StdoutDMLSink` but for :class:`SchemaChange` payloads.
    Each batch produces a ``window`` line, one ``schema_change`` line per
    event, and a closing ``commit`` line.
    """

    name = "stdout"
    require_ack = True

    def __init__(self, *, stream: IO[str] | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        _emit_window(self._stream, batch, len(batch))
        for event in batch:
            payload = event.to_dict()
            payload["type"] = "schema_change"
            _emit(self._stream, payload)
        _emit_commit(self._stream, batch)


# ---------------------------------------------------------------------------
# File sinks
# ---------------------------------------------------------------------------


class _FileSinkBase:
    """Shared file-handle plumbing for :class:`FileDMLSink` and
    :class:`FileDDLSink`.

    The default format is JSON Lines. The format is currently inferred from
    the file extension (``.jsonl`` / ``.ndjson`` / ``.json``); other
    extensions also default to JSONL with a debug log. Future formats
    (``.csv``, ``.parquet``) live in separate distributions.
    """

    name = "file"
    require_ack = True

    def __init__(self, path: str | Path, *, mode: str = "a", encoding: str = "utf-8") -> None:
        if "b" in mode:
            raise ValueError(
                "FileDMLSink/FileDDLSink write text JSON Lines; pass a text mode"
            )
        self._path = Path(path)
        self._mode = mode
        self._encoding = encoding
        self._fh: IO[str] | None = None

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._path.open(self._mode, encoding=self._encoding)

    def close(self) -> None:
        if self._fh is not None:
            with suppress(Exception):
                self._fh.flush()
            self._fh.close()
            self._fh = None

    def _require_fh(self) -> IO[str]:
        if self._fh is None:
            raise RuntimeError(
                f"{type(self).__name__} is not open; use it inside a `with "
                "consumer:` block"
            )
        return self._fh


class FileDMLSink(_FileSinkBase, BaseDMLSink):
    """Append each DML batch as JSON lines to ``path``.

    Lines mirror :class:`StdoutDMLSink`: one ``window`` line, one
    ``change`` line per row, one ``commit`` line. The parent directory is
    created on open. Default mode is ``"a"`` (append); pass ``mode="w"``
    to truncate on open.
    """

    name = "file"

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        fh = self._require_fh()
        _emit_window(fh, batch, len(batch))
        for change in batch:
            payload = change.to_dict()
            payload["type"] = "change"
            _emit(fh, payload)
        _emit_commit(fh, batch)


class FileDDLSink(_FileSinkBase, BaseDDLSink):
    """Append each DDL batch as JSON lines to ``path``.

    Mirrors :class:`FileDMLSink` for :class:`SchemaChange` events.
    """

    name = "file"

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        fh = self._require_fh()
        _emit_window(fh, batch, len(batch))
        for event in batch:
            payload = event.to_dict()
            payload["type"] = "schema_change"
            _emit(fh, payload)
        _emit_commit(fh, batch)


# ---------------------------------------------------------------------------
# Memory sinks
# ---------------------------------------------------------------------------


class MemoryDMLSink(BaseDMLSink):
    """Capture DML batches in memory for tests and notebooks.

    Three views on the data:

    - iterating the sink yields :class:`Change` instances in arrival order;
    - ``sink.changes`` is a flat list of every captured change;
    - ``sink.batches`` is the list of :class:`DMLBatch` instances.

    The sink stays bounded with ``max_changes`` (default ``None`` = unbounded).
    When the limit is reached, oldest entries are dropped from both
    ``changes`` and the corresponding ``batches`` are pruned to keep the
    two views consistent.
    """

    name = "memory"
    require_ack = True

    def __init__(self, *, max_changes: int | None = None) -> None:
        if max_changes is not None and max_changes <= 0:
            raise ValueError("max_changes must be positive when provided")
        self._max_changes = max_changes
        self._batches: list[DMLBatch] = []
        self._changes: list[Change] = []

    @property
    def batches(self) -> list[DMLBatch]:
        return list(self._batches)

    @property
    def changes(self) -> list[Change]:
        return list(self._changes)

    def __iter__(self) -> Iterator[Change]:
        return iter(self._changes)

    def __len__(self) -> int:
        return len(self._changes)

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        self._batches.append(batch)
        self._changes.extend(batch.changes)
        self._enforce_cap()

    def reset(self) -> None:
        self._batches.clear()
        self._changes.clear()

    def _enforce_cap(self) -> None:
        cap = self._max_changes
        if cap is None or len(self._changes) <= cap:
            return
        excess = len(self._changes) - cap
        del self._changes[:excess]
        while self._batches and excess > 0:
            head = self._batches[0]
            count = len(head)
            if count <= excess:
                self._batches.pop(0)
                excess -= count
            else:
                self._batches[0] = _replace_changes(head, head.changes[excess:])
                excess = 0


class MemoryDDLSink(BaseDDLSink):
    """Capture DDL batches in memory for tests and notebooks.

    Mirrors :class:`MemoryDMLSink` for :class:`SchemaChange` events.
    """

    name = "memory"
    require_ack = True

    def __init__(self, *, max_changes: int | None = None) -> None:
        if max_changes is not None and max_changes <= 0:
            raise ValueError("max_changes must be positive when provided")
        self._max_changes = max_changes
        self._batches: list[DDLBatch] = []
        self._changes: list[SchemaChange] = []

    @property
    def batches(self) -> list[DDLBatch]:
        return list(self._batches)

    @property
    def changes(self) -> list[SchemaChange]:
        return list(self._changes)

    def __iter__(self) -> Iterator[SchemaChange]:
        return iter(self._changes)

    def __len__(self) -> int:
        return len(self._changes)

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        self._batches.append(batch)
        self._changes.extend(batch.changes)
        self._enforce_cap()

    def reset(self) -> None:
        self._batches.clear()
        self._changes.clear()

    def _enforce_cap(self) -> None:
        cap = self._max_changes
        if cap is None or len(self._changes) <= cap:
            return
        excess = len(self._changes) - cap
        del self._changes[:excess]
        while self._batches and excess > 0:
            head = self._batches[0]
            count = len(head)
            if count <= excess:
                self._batches.pop(0)
                excess -= count
            else:
                self._batches[0] = _replace_changes(head, head.changes[excess:])
                excess = 0


# ---------------------------------------------------------------------------
# Callable sinks
# ---------------------------------------------------------------------------


class CallableDMLSink(BaseDMLSink):
    """Wrap a function ``(batch, ctx) -> None`` (or ``(batch) -> None``) as a sink.

    The accepted shape is decided once at construction by inspecting the
    callable's positional parameters. A function that takes no arguments,
    or more than two, is rejected eagerly so the failure happens at sink
    creation rather than at first batch.
    """

    require_ack = True

    def __init__(
        self,
        fn: DMLCallable,
        *,
        name: str | None = None,
        require_ack: bool = True,
    ) -> None:
        if not callable(fn):
            raise TypeError("CallableDMLSink expects a callable")
        self._fn = fn
        self._wants_ctx = _callable_wants_context(fn)
        self.name = name or _callable_name(fn)
        self.require_ack = require_ack

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        if self._wants_ctx:
            self._fn(batch, ctx)
        else:
            self._fn(batch)


class CallableDDLSink(BaseDDLSink):
    """Wrap a function ``(batch, ctx) -> None`` (or ``(batch) -> None``) as a DDL sink.

    Mirrors :class:`CallableDMLSink` for :class:`DDLBatch` payloads.
    """

    require_ack = True

    def __init__(
        self,
        fn: DDLCallable,
        *,
        name: str | None = None,
        require_ack: bool = True,
    ) -> None:
        if not callable(fn):
            raise TypeError("CallableDDLSink expects a callable")
        self._fn = fn
        self._wants_ctx = _callable_wants_context(fn)
        self.name = name or _callable_name(fn)
        self.require_ack = require_ack

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        if self._wants_ctx:
            self._fn(batch, ctx)
        else:
            self._fn(batch)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _emit_window(stream: IO[str], batch: DMLBatch | DDLBatch, change_count: int) -> None:
    _emit(
        stream,
        {
            "type": "window",
            "consumer": batch.consumer_name,
            "batch_id": batch.batch_id,
            "start_snapshot": batch.start_snapshot,
            "end_snapshot": batch.end_snapshot,
            "snapshot_ids": list(batch.snapshot_ids),
            "received_at": batch.received_at.isoformat(),
            "change_count": change_count,
        },
    )


def _emit_commit(stream: IO[str], batch: DMLBatch | DDLBatch) -> None:
    _emit(
        stream,
        {
            "type": "commit",
            "consumer": batch.consumer_name,
            "batch_id": batch.batch_id,
            "snapshot": batch.end_snapshot,
        },
    )


def _emit(stream: IO[str], payload: dict[str, Any]) -> None:
    line = json.dumps(payload, default=_json_default, sort_keys=True)
    stream.write(line + "\n")
    stream.flush()


def _json_default(value: object) -> str:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def _callable_name(fn: Callable[..., Any]) -> str:
    name = getattr(fn, "__name__", None)
    if isinstance(name, str) and name and name != "<lambda>":
        return name
    return type(fn).__name__


def _callable_wants_context(fn: Callable[..., Any]) -> bool:
    """Pick the calling shape for a sink callable.

    A two-positional callable ``(batch, ctx)`` is the canonical form.
    A single-positional callable ``(batch)`` is also accepted and the
    context is dropped on each call. ``*args`` callables are treated as
    wanting the context (canonical form). Anything else raises eagerly.
    """

    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return True

    positional: list[inspect.Parameter] = []
    saw_var_positional = False
    for param in sig.parameters.values():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional.append(param)
        elif param.kind is inspect.Parameter.VAR_POSITIONAL:
            saw_var_positional = True

    required = [p for p in positional if p.default is inspect.Parameter.empty]

    if saw_var_positional:
        return True
    if len(required) > 2:
        raise TypeError(
            "callable sink takes at most 2 positional arguments (batch, ctx); "
            f"{_callable_name(fn)!r} requires {len(required)}"
        )
    if len(positional) >= 2:
        return True
    if len(positional) == 1:
        return False
    raise TypeError(
        "callable sink must accept (batch) or (batch, ctx); "
        f"{_callable_name(fn)!r} accepts no positional arguments"
    )


def _replace_changes(batch: Any, changes: tuple[Any, ...]) -> Any:
    """Rebuild a batch with a smaller ``changes`` tuple.

    Used by the in-memory sinks when ``max_changes`` evicts the head of an
    older batch. The batch identity (``batch_id``,
    ``start_snapshot``/``end_snapshot``) is preserved.
    """

    if isinstance(batch, DMLBatch):
        return DMLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=changes,
        )
    if isinstance(batch, DDLBatch):
        return DDLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=changes,
        )
    raise TypeError(f"unsupported batch type: {type(batch).__name__}")
