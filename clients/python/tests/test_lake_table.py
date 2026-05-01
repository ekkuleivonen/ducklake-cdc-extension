from __future__ import annotations

from typing import Any

from ducklake import Column, DuckLake, Snapshot
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
