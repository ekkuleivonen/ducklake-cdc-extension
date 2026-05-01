"""Explicit DuckLake session context manager."""

from __future__ import annotations

from typing import Any

from ducklake.result import QueryParameters, Result


class Session:
    """A context-managed view over a DuckLake connection."""

    def __init__(self, lake: Any) -> None:
        self._lake = lake
        self._connection: Any | None = None

    def __enter__(self) -> Session:
        self._connection = self._lake.raw_connection()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._connection = None

    def sql(self, query: str, parameters: QueryParameters = None) -> Result:
        connection = (
            self._connection if self._connection is not None else self._lake.raw_connection()
        )
        return Result(connection, query, parameters)

    def raw_connection(self) -> Any:
        return self._connection if self._connection is not None else self._lake.raw_connection()
