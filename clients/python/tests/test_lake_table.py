from __future__ import annotations

from typing import Any

import pytest

from ducklake import Column, DuckLake, DuckLakeQueryError, Snapshot
from ducklake.result import QueryParameters


class RecordingLake:
    alias = "lake"

    def __init__(self) -> None:
        self.calls: list[tuple[str, QueryParameters]] = []

    def sql(self, query: str, **parameters: object) -> Any:
        self.calls.append((query, parameters or None))
        return FakeResult()


class FakeResult:
    def scalar(self) -> int:
        return 42

    def list(self) -> list[dict[str, object]]:
        return []


class FakeConnection:
    description = [("ok",)]

    def __init__(self) -> None:
        self.calls: list[tuple[str, object | None]] = []

    def execute(self, query: str, parameters: object | None = None) -> FakeConnection:
        self.calls.append((query, parameters))
        return self

    def fetchall(self) -> list[tuple[bool]]:
        return [(True,)]


class FailingCommitConnection(FakeConnection):
    def execute(self, query: str, parameters: object | None = None) -> FakeConnection:
        if query == "COMMIT":
            self.calls.append((query, parameters))
            raise RuntimeError("database is locked")
        return super().execute(query, parameters)


def test_open_is_lazy() -> None:
    lake = DuckLake.open(catalog="catalog.ducklake", storage="data")

    assert lake.alias == "lake"


def test_table_value_objects_are_pydantic_models() -> None:
    column = Column(name="id", data_type="BIGINT", nullable=False, ordinal_position=1)
    snapshot_id: Any = "42"
    snapshot = Snapshot(snapshot_id=snapshot_id)

    assert column.model_dump() == {
        "name": "id",
        "data_type": "BIGINT",
        "nullable": False,
        "ordinal_position": 1,
    }
    assert snapshot.snapshot_id == 42


def test_lake_sql_accepts_named_parameters() -> None:
    lake = DuckLake.open(catalog="catalog.ducklake", storage="data")
    lake._manager.get = lambda: object()  # type: ignore[method-assign]

    result = lake.sql("SELECT $value", value=1)

    assert result._parameters == {"value": 1}


def test_transaction_commits_successful_block() -> None:
    connection = FakeConnection()
    lake = DuckLake.open(catalog="catalog.ducklake", storage="data")
    lake._manager.get = lambda: connection  # type: ignore[method-assign]

    with lake.transaction() as tx:
        assert tx.sql("SELECT $value", value=1).list() == [{"ok": True}]

    assert connection.calls == [
        ("BEGIN TRANSACTION", None),
        ("SELECT $value", {"value": 1}),
        ("COMMIT", None),
    ]


def test_transaction_rolls_back_failed_block() -> None:
    connection = FakeConnection()
    lake = DuckLake.open(catalog="catalog.ducklake", storage="data")
    lake._manager.get = lambda: connection  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="boom"):
        with lake.transaction():
            raise RuntimeError("boom")

    assert connection.calls == [
        ("BEGIN TRANSACTION", None),
        ("ROLLBACK", None),
    ]


def test_transaction_wraps_commit_failures() -> None:
    connection = FailingCommitConnection()
    lake = DuckLake.open(catalog="catalog.ducklake", storage="data")
    lake._manager.get = lambda: connection  # type: ignore[method-assign]

    with pytest.raises(DuckLakeQueryError, match="transaction commit failed"):
        with lake.transaction():
            pass

    assert connection.calls == [
        ("BEGIN TRANSACTION", None),
        ("COMMIT", None),
        ("ROLLBACK", None),
    ]


def test_transaction_handle_cannot_run_after_exit() -> None:
    connection = FakeConnection()
    lake = DuckLake.open(catalog="catalog.ducklake", storage="data")
    lake._manager.get = lambda: connection  # type: ignore[method-assign]

    with lake.transaction() as tx:
        pass

    with pytest.raises(RuntimeError, match="transaction is not active"):
        tx.sql("SELECT 1").list()


def test_table_builds_common_queries() -> None:
    lake = RecordingLake()
    table = DuckLake.table(lake, "events")  # type: ignore[arg-type]

    assert table.qualified_name == '"lake"."main"."events"'
    assert table.row_count() == 42
    assert "SELECT count(*) AS count FROM \"lake\".\"main\".\"events\"" in lake.calls[-1][0]

    table.at(snapshot=7)
    assert 'AT (VERSION => $snapshot)' in lake.calls[-1][0]
    assert lake.calls[-1][1] == {"snapshot": 7}

    table.between(1, 2)
    assert '"lake".table_changes($table, $start, $end)' in lake.calls[-1][0]
    assert lake.calls[-1][1] == {"table": "events", "start": 1, "end": 2}
