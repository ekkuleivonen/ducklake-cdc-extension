from __future__ import annotations

import sys
import time
from argparse import Namespace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ducklake import DuckLakeQueryError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demo"))

import consumer as demo_consumer  # noqa: E402
from analytics import DemoStats  # noqa: E402

from ducklake_cdc import (  # noqa: E402
    CDCApp,
    DDLBatch,
    DdlEventKind,
    DdlObjectKind,
    SchemaChange,
    SinkContext,
)


class _FakeLake:
    def __init__(self) -> None:
        self.loaded_paths: list[Path] = []
        self.closed = False

    def load_extension(self, *, path: Path) -> None:
        self.loaded_paths.append(path)

    def close(self) -> None:
        self.closed = True


def test_table_spawn_sink_hot_adds_dml_consumers_for_new_tables(monkeypatch: Any) -> None:
    opened_lakes: list[_FakeLake] = []
    extension_path = Path("/tmp/ducklake_cdc.duckdb_extension")

    def open_lake(_args: Namespace) -> _FakeLake:
        lake = _FakeLake()
        opened_lakes.append(lake)
        return lake

    monkeypatch.setattr(demo_consumer, "_open_lake", open_lake)
    monkeypatch.setattr(demo_consumer, "_local_extension_path", lambda: extension_path)

    app = CDCApp(install_signals=False)
    consumer_lakes: list[Any] = []
    sink = demo_consumer._TableSpawnSink(
        app=app,
        args=Namespace(catalog=None, catalog_backend=None, storage=None),
        stats=DemoStats(started_ns=0),
        consumer_lakes=consumer_lakes,
    )
    batch = _ddl_batch(
        SchemaChange(
            event_kind=DdlEventKind.CREATED,
            object_kind=DdlObjectKind.TABLE,
            snapshot_id=7,
            snapshot_time=datetime(2026, 1, 1, tzinfo=UTC),
            schema_id=1,
            schema_name="demo_schema_01",
            object_id=42,
            object_name="events_01",
            details=None,
        ),
        SchemaChange(
            event_kind=DdlEventKind.CREATED,
            object_kind=DdlObjectKind.VIEW,
            snapshot_id=8,
            snapshot_time=datetime(2026, 1, 1, tzinfo=UTC),
            schema_id=1,
            schema_name="demo_schema_01",
            object_id=43,
            object_name="ignored_view",
            details=None,
        ),
    )

    sink.open()
    try:
        sink.write(batch, _ctx(batch))
        sink.write(batch, _ctx(batch))
        _wait_for(lambda: len(app.consumers) == 1)
    finally:
        sink.close()

    assert len(opened_lakes) == 1
    assert consumer_lakes == opened_lakes
    assert opened_lakes[0].loaded_paths == [extension_path]
    consumer: Any = app.consumers[0]
    assert consumer.name == "demo__table_id_42"
    assert consumer._table_id == 42
    assert consumer._start_at == 7


def test_table_spawn_sink_can_attach_existing_tables_by_name(monkeypatch: Any) -> None:
    opened_lakes: list[_FakeLake] = []
    extension_path = Path("/tmp/ducklake_cdc.duckdb_extension")

    def open_lake(_args: Namespace) -> _FakeLake:
        lake = _FakeLake()
        opened_lakes.append(lake)
        return lake

    monkeypatch.setattr(demo_consumer, "_open_lake", open_lake)
    monkeypatch.setattr(demo_consumer, "_local_extension_path", lambda: extension_path)

    app = CDCApp(install_signals=False)
    sink = demo_consumer._TableSpawnSink(
        app=app,
        args=Namespace(catalog=None, catalog_backend=None, storage=None),
        stats=DemoStats(started_ns=0),
        consumer_lakes=[],
    )

    assert sink.add_table(table_id=None, table_name="demo_schema_01.events_01", start_at="now")
    assert not sink.add_table(
        table_id=None,
        table_name="demo_schema_01.events_01",
        start_at="now",
    )

    assert len(opened_lakes) == 1
    consumer: Any = app.consumers[0]
    assert consumer.name == "demo__demo_schema_01_events_01"
    assert consumer._table == "demo_schema_01.events_01"
    assert consumer._start_at == "now"


def test_table_spawn_sink_retries_transient_add_failures(monkeypatch: Any) -> None:
    opened_lakes: list[_FakeLake] = []
    extension_path = Path("/tmp/ducklake_cdc.duckdb_extension")

    def open_lake(_args: Namespace) -> _FakeLake:
        lake = _FakeLake()
        opened_lakes.append(lake)
        return lake

    monkeypatch.setattr(demo_consumer, "_open_lake", open_lake)
    monkeypatch.setattr(demo_consumer, "_local_extension_path", lambda: extension_path)
    monkeypatch.setattr(demo_consumer, "TABLE_SPAWN_RETRY_INTERVAL_S", 0.0)

    app = CDCApp(install_signals=False)
    original_add = app.add_consumer
    attempts = {"count": 0}

    def flaky_add(consumer: Any) -> None:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise DuckLakeQueryError("temporary catalog conflict")
        original_add(consumer)

    app.add_consumer = flaky_add  # type: ignore[method-assign]
    sink = demo_consumer._TableSpawnSink(
        app=app,
        args=Namespace(catalog=None, catalog_backend=None, storage=None),
        stats=DemoStats(started_ns=0),
        consumer_lakes=[],
    )
    request = demo_consumer._SpawnRequest(
        table_id=42,
        table_name="demo_schema_01.events_01",
        start_at=7,
    )

    sink.enqueue_table(request)
    sink.open()
    try:
        _wait_for(lambda: len(app.consumers) == 1)
    finally:
        sink.close()

    assert attempts["count"] == 2
    assert len(opened_lakes) == 2
    assert opened_lakes[0].closed is True
    assert app.consumers[0].name == "demo__table_id_42"


def _wait_for(predicate: Any, *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting up to {timeout:.1f}s for predicate")


def _ddl_batch(*changes: SchemaChange) -> DDLBatch:
    return DDLBatch(
        consumer_name="demo__ddl",
        batch_id="demo__ddl/7-8",
        start_snapshot=7,
        end_snapshot=8,
        snapshot_ids=tuple(change.snapshot_id for change in changes),
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        changes=changes,
    )


def _ctx(batch: DDLBatch) -> SinkContext:
    return SinkContext(
        consumer_name=batch.consumer_name,
        batch_id=batch.batch_id,
        _heartbeat=lambda: None,
    )
