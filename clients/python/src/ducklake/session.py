"""Explicit DuckLake session context manager."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ducklake.exceptions import DuckLakeQueryError
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

    def sql(self, query: str, *parameters: object, **named_parameters: object) -> Result:
        connection = (
            self._connection if self._connection is not None else self._lake.raw_connection()
        )
        return Result(connection, query, _normalize_parameters(parameters, named_parameters))

    def raw_connection(self) -> Any:
        return self._connection if self._connection is not None else self._lake.raw_connection()


class Transaction:
    """A context-managed DuckDB transaction on a DuckLake connection."""

    def __init__(self, lake: Any) -> None:
        self._lake = lake
        self._connection: Any | None = None

    def __enter__(self) -> Transaction:
        if self._connection is not None:
            raise RuntimeError("transaction is already active")
        self._connection = self._lake.raw_connection()
        self._connection.execute("BEGIN TRANSACTION")
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        if exc_type is None:
            try:
                connection.execute("COMMIT")
            except Exception as commit_exc:
                try:
                    connection.execute("ROLLBACK")
                except Exception:
                    pass
                raise DuckLakeQueryError("DuckLake transaction commit failed") from commit_exc
        else:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass

    def sql(self, query: str, *parameters: object, **named_parameters: object) -> Result:
        return Result(
            self.raw_connection(),
            query,
            _normalize_parameters(parameters, named_parameters),
        )

    def raw_connection(self) -> Any:
        if self._connection is None:
            raise RuntimeError("transaction is not active")
        return self._connection


def _normalize_parameters(
    positional: tuple[object, ...],
    named: Mapping[str, object],
) -> QueryParameters:
    if positional and named:
        raise TypeError("pass either positional parameters or named parameters, not both")
    if named:
        return dict(named)
    if not positional:
        return None
    if len(positional) == 1 and isinstance(positional[0], Mapping):
        return dict(positional[0])
    return list(positional)
