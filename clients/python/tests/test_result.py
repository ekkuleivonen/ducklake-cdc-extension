from __future__ import annotations

from typing import Any

import pytest

from ducklake import ResultCardinalityError
from ducklake.result import Result


class FakeCursor:
    description = [("id",), ("name",)]

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        rows = self._rows[:size]
        self._rows = self._rows[size:]
        return rows


class FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, object | None]] = []

    def execute(self, query: str, parameters: object | None = None) -> FakeCursor:
        self.calls.append((query, parameters))
        return FakeCursor(list(self.rows))


def test_result_list_uses_named_parameters() -> None:
    conn = FakeConnection([(1, "Ada")])

    rows = Result(conn, "SELECT $id, $name", {"id": 1, "name": "Ada"}).list()

    assert rows == [{"id": 1, "name": "Ada"}]
    assert conn.calls == [("SELECT $id, $name", {"id": 1, "name": "Ada"})]


def test_result_one_rejects_wrong_cardinality() -> None:
    conn = FakeConnection([])

    with pytest.raises(ResultCardinalityError):
        Result(conn, "SELECT 1").one()


def test_result_is_iterable() -> None:
    conn = FakeConnection([(1, "Ada"), (2, "Grace")])

    assert list(Result(conn, "SELECT * FROM users")) == [
        {"id": 1, "name": "Ada"},
        {"id": 2, "name": "Grace"},
    ]
