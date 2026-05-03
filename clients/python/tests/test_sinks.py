"""Pure-Python coverage for built-in sinks and combinators.

These tests exercise the sink surface without touching the SQL extension.
Anything that requires a running DuckLake catalog lives in the demo /
integration tests instead.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ducklake_cdc import (
    CallableDDLSink,
    CallableDMLSink,
    Change,
    DDLBatch,
    DMLBatch,
    FanoutDDLSink,
    FanoutDMLSink,
    FileDDLSink,
    FileDMLSink,
    FilterDDLSink,
    FilterDMLSink,
    MapDMLSink,
    MemoryDDLSink,
    MemoryDMLSink,
    SchemaChange,
    SinkAck,
    SinkContext,
    StdoutDDLSink,
    StdoutDMLSink,
)
from ducklake_cdc.enums import ChangeType, DdlEventKind, DdlObjectKind


def _make_change(
    *,
    snapshot_id: int = 1,
    rowid: int = 1,
    table: str = "main.orders",
    table_id: int | None = 42,
    kind: ChangeType = ChangeType.INSERT,
) -> Change:
    return Change(
        kind=kind,
        snapshot_id=snapshot_id,
        table=table,
        table_id=table_id,
        rowid=rowid,
        snapshot_time=datetime(2025, 1, 1, tzinfo=UTC),
        values={"id": rowid},
    )


def _make_dml_batch(
    *,
    name: str = "consumer_a",
    start: int = 1,
    end: int = 2,
    changes: tuple[Change, ...] | None = None,
) -> DMLBatch:
    if changes is None:
        changes = (_make_change(snapshot_id=start), _make_change(snapshot_id=end, rowid=2))
    return DMLBatch(
        consumer_name=name,
        batch_id=DMLBatch.derive_batch_id(name, start, end),
        start_snapshot=start,
        end_snapshot=end,
        snapshot_ids=tuple(sorted({c.snapshot_id for c in changes})),
        received_at=datetime(2025, 1, 1, tzinfo=UTC),
        changes=changes,
    )


def _make_schema_change(
    *,
    snapshot_id: int = 1,
    object_id: int = 10,
    event_kind: DdlEventKind = DdlEventKind.CREATED,
) -> SchemaChange:
    return SchemaChange(
        event_kind=event_kind,
        object_kind=DdlObjectKind.TABLE,
        snapshot_id=snapshot_id,
        snapshot_time=datetime(2025, 1, 1, tzinfo=UTC),
        schema_id=1,
        schema_name="main",
        object_id=object_id,
        object_name="orders",
        details=None,
    )


def _make_ddl_batch(
    *,
    name: str = "ddl_a",
    start: int = 1,
    end: int = 2,
    changes: tuple[SchemaChange, ...] | None = None,
) -> DDLBatch:
    if changes is None:
        changes = (
            _make_schema_change(snapshot_id=start, object_id=1),
            _make_schema_change(snapshot_id=end, object_id=2, event_kind=DdlEventKind.ALTERED),
        )
    return DDLBatch(
        consumer_name=name,
        batch_id=DDLBatch.derive_batch_id(name, start, end),
        start_snapshot=start,
        end_snapshot=end,
        snapshot_ids=tuple(sorted({c.snapshot_id for c in changes})),
        received_at=datetime(2025, 1, 1, tzinfo=UTC),
        changes=changes,
    )


def _ctx(batch: DMLBatch | DDLBatch) -> SinkContext:
    return SinkContext(
        consumer_name=batch.consumer_name,
        batch_id=batch.batch_id,
        _heartbeat=lambda: None,
    )


# ---------------------------------------------------------------------------
# Batch ack/nack
# ---------------------------------------------------------------------------


def test_dml_batch_ack_nack_returns_sink_ack() -> None:
    batch = _make_dml_batch()

    ack = batch.ack("memory")
    nack = batch.nack("memory", detail="boom")

    assert ack == SinkAck(sink="memory", batch_id=batch.batch_id, ok=True, detail=None)
    assert nack == SinkAck(sink="memory", batch_id=batch.batch_id, ok=False, detail="boom")


def test_ddl_batch_ack_nack_returns_sink_ack() -> None:
    batch = _make_ddl_batch()

    ack = batch.ack("memory")
    nack = batch.nack("memory", detail="boom")

    assert ack == SinkAck(sink="memory", batch_id=batch.batch_id, ok=True, detail=None)
    assert nack == SinkAck(sink="memory", batch_id=batch.batch_id, ok=False, detail="boom")


# ---------------------------------------------------------------------------
# Stdout / file sinks
# ---------------------------------------------------------------------------


def test_stdout_dml_sink_emits_window_change_commit_lines() -> None:
    stream = io.StringIO()
    sink = StdoutDMLSink(stream=stream)
    batch = _make_dml_batch()

    sink.write(batch, _ctx(batch))

    lines = [json.loads(line) for line in stream.getvalue().splitlines()]
    types = [line["type"] for line in lines]
    assert types == ["window", "change", "change", "commit"]
    assert lines[0]["batch_id"] == batch.batch_id
    assert lines[-1]["snapshot"] == batch.end_snapshot


def test_stdout_ddl_sink_emits_window_schema_commit_lines() -> None:
    stream = io.StringIO()
    sink = StdoutDDLSink(stream=stream)
    batch = _make_ddl_batch()

    sink.write(batch, _ctx(batch))

    lines = [json.loads(line) for line in stream.getvalue().splitlines()]
    types = [line["type"] for line in lines]
    assert types == ["window", "schema_change", "schema_change", "commit"]


def test_file_dml_sink_appends_lines_under_with_block(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "events.jsonl"
    sink = FileDMLSink(target)

    batch = _make_dml_batch()
    sink.open()
    try:
        sink.write(batch, _ctx(batch))
    finally:
        sink.close()

    text = target.read_text("utf-8")
    types = [json.loads(line)["type"] for line in text.splitlines()]
    assert types == ["window", "change", "change", "commit"]


def test_file_ddl_sink_writes_schema_change_lines(tmp_path: Path) -> None:
    target = tmp_path / "ddl.jsonl"
    sink = FileDDLSink(target)
    batch = _make_ddl_batch()

    sink.open()
    try:
        sink.write(batch, _ctx(batch))
    finally:
        sink.close()

    types = [json.loads(line)["type"] for line in target.read_text("utf-8").splitlines()]
    assert types == ["window", "schema_change", "schema_change", "commit"]


def test_file_sink_rejects_binary_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        FileDMLSink(tmp_path / "x.jsonl", mode="ab")


def test_file_sink_write_before_open_is_a_clear_error(tmp_path: Path) -> None:
    sink = FileDMLSink(tmp_path / "x.jsonl")
    batch = _make_dml_batch()

    with pytest.raises(RuntimeError):
        sink.write(batch, _ctx(batch))


# ---------------------------------------------------------------------------
# Memory sinks
# ---------------------------------------------------------------------------


def test_memory_dml_sink_exposes_batches_and_flat_changes() -> None:
    sink = MemoryDMLSink()
    batch_a = _make_dml_batch(name="c", start=1, end=2)
    batch_b = _make_dml_batch(name="c", start=3, end=4)

    sink.write(batch_a, _ctx(batch_a))
    sink.write(batch_b, _ctx(batch_b))

    assert sink.batches == [batch_a, batch_b]
    assert len(sink) == 4
    assert list(sink) == list(batch_a) + list(batch_b)


def test_memory_dml_sink_caps_changes_and_prunes_batches() -> None:
    sink = MemoryDMLSink(max_changes=3)

    batch_a = _make_dml_batch(name="c", start=1, end=2)
    batch_b = _make_dml_batch(name="c", start=3, end=4)
    sink.write(batch_a, _ctx(batch_a))
    sink.write(batch_b, _ctx(batch_b))

    assert len(sink) == 3
    assert len(sink.batches) == 2
    assert sink.batches[0].batch_id == batch_a.batch_id
    assert len(sink.batches[0]) == 1
    assert list(sink) == [*list(sink.batches[0]), *list(sink.batches[1])]


def test_memory_dml_sink_reset_clears_state() -> None:
    sink = MemoryDMLSink()
    batch = _make_dml_batch()

    sink.write(batch, _ctx(batch))
    sink.reset()

    assert sink.batches == []
    assert sink.changes == []


def test_memory_ddl_sink_collects_schema_changes() -> None:
    sink = MemoryDDLSink()
    batch = _make_ddl_batch()

    sink.write(batch, _ctx(batch))

    assert sink.batches == [batch]
    assert sink.changes == list(batch.changes)


def test_memory_sink_rejects_non_positive_cap() -> None:
    with pytest.raises(ValueError):
        MemoryDMLSink(max_changes=0)


# ---------------------------------------------------------------------------
# Callable sinks
# ---------------------------------------------------------------------------


def test_callable_dml_sink_two_arg_form_receives_ctx() -> None:
    seen: list[SinkContext] = []

    def handler(batch: DMLBatch, ctx: SinkContext) -> None:
        seen.append(ctx)

    sink = CallableDMLSink(handler)
    batch = _make_dml_batch()
    sink.write(batch, _ctx(batch))

    assert len(seen) == 1
    assert seen[0].batch_id == batch.batch_id


def test_callable_dml_sink_single_arg_form_drops_ctx() -> None:
    seen: list[DMLBatch] = []

    def handler(batch: DMLBatch) -> None:
        seen.append(batch)

    sink = CallableDMLSink(handler)
    batch = _make_dml_batch()
    sink.write(batch, _ctx(batch))

    assert seen == [batch]


def test_callable_ddl_sink_accepts_var_positional() -> None:
    seen: list[tuple[object, ...]] = []

    def handler(*args: object) -> None:
        seen.append(args)

    sink = CallableDDLSink(handler)
    batch = _make_ddl_batch()
    sink.write(batch, _ctx(batch))

    assert len(seen) == 1
    assert seen[0][0] is batch


def test_callable_sink_rejects_zero_arg_callables() -> None:
    def bad() -> None:
        return None

    with pytest.raises(TypeError):
        CallableDMLSink(bad)


def test_callable_sink_rejects_too_many_required_args() -> None:
    def bad(a: object, b: object, c: object) -> None:
        return None

    with pytest.raises(TypeError):
        CallableDMLSink(bad)


# ---------------------------------------------------------------------------
# Combinators
# ---------------------------------------------------------------------------


def test_map_dml_sink_rebuilds_batch_with_new_changes() -> None:
    captured: list[DMLBatch] = []

    def capture(batch: DMLBatch, _ctx: SinkContext) -> None:
        captured.append(batch)

    inner = CallableDMLSink(capture, name="inner")
    sink = MapDMLSink(
        lambda c: Change(
            kind=c.kind,
            snapshot_id=c.snapshot_id,
            table=c.table,
            table_id=c.table_id,
            rowid=c.rowid,
            snapshot_time=c.snapshot_time,
            values={**c.values, "tagged": True},
        ),
        inner,
    )

    batch = _make_dml_batch()
    sink.write(batch, _ctx(batch))

    assert len(captured) == 1
    assert captured[0].batch_id == batch.batch_id
    assert all(change.values.get("tagged") for change in captured[0])


def test_filter_dml_sink_keeps_only_matching_changes() -> None:
    captured: list[DMLBatch] = []

    def capture(batch: DMLBatch, _ctx: SinkContext) -> None:
        captured.append(batch)

    inner = CallableDMLSink(capture, name="inner")
    sink = FilterDMLSink(lambda change: change.rowid == 2, inner)

    batch = _make_dml_batch()
    sink.write(batch, _ctx(batch))

    assert len(captured) == 1
    assert [c.rowid for c in captured[0]] == [2]
    assert captured[0].batch_id == batch.batch_id


def test_filter_ddl_sink_keeps_only_altered_events() -> None:
    captured: list[DDLBatch] = []
    inner = CallableDDLSink(lambda b: captured.append(b), name="inner")
    sink = FilterDDLSink(lambda e: e.event_kind == DdlEventKind.ALTERED, inner)

    batch = _make_ddl_batch()
    sink.write(batch, _ctx(batch))

    assert len(captured) == 1
    assert [e.event_kind for e in captured[0]] == [DdlEventKind.ALTERED]


def test_fanout_dml_sink_broadcasts_to_all_inner_sinks() -> None:
    a = MemoryDMLSink()
    b = MemoryDMLSink()
    sink = FanoutDMLSink(a, b)

    batch = _make_dml_batch()
    sink.write(batch, _ctx(batch))

    assert a.batches == [batch]
    assert b.batches == [batch]


def test_fanout_dml_sink_swallows_failures_from_optional_inner_sink() -> None:
    a = MemoryDMLSink()

    def boom(batch: DMLBatch) -> None:
        raise RuntimeError("kaboom")

    optional = CallableDMLSink(boom, name="optional", require_ack=False)
    sink = FanoutDMLSink(a, optional)

    batch = _make_dml_batch()
    sink.write(batch, _ctx(batch))

    assert a.batches == [batch]


def test_fanout_dml_sink_propagates_required_inner_failure() -> None:
    a = MemoryDMLSink()

    def boom(batch: DMLBatch) -> None:
        raise RuntimeError("kaboom")

    required = CallableDMLSink(boom, name="required")
    sink = FanoutDMLSink(a, required)

    batch = _make_dml_batch()
    with pytest.raises(RuntimeError, match="kaboom"):
        sink.write(batch, _ctx(batch))


def test_fanout_requires_at_least_one_inner_sink() -> None:
    with pytest.raises(ValueError):
        FanoutDMLSink()
    with pytest.raises(ValueError):
        FanoutDDLSink()


def test_fanout_open_close_propagate_to_inner_sinks(tmp_path: Path) -> None:
    target = tmp_path / "out.jsonl"
    file_sink = FileDMLSink(target)
    sink = FanoutDMLSink(MemoryDMLSink(), file_sink)

    sink.open()
    try:
        batch = _make_dml_batch()
        sink.write(batch, _ctx(batch))
    finally:
        sink.close()

    assert target.exists()
    assert target.read_text("utf-8")
