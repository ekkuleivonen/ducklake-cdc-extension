"""Result wrapper around DuckDB queries."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from html import escape
from importlib import import_module
from typing import Any

from ducklake.exceptions import DuckLakeQueryError, ResultCardinalityError

QueryParameters = Mapping[str, object] | Sequence[object] | None


class Result:
    """A lazy DuckDB query result with small, discoverable materializers."""

    def __init__(self, connection: Any, query: str, parameters: QueryParameters = None) -> None:
        self._connection = connection
        self._query = query
        self._parameters = parameters

    def df(self) -> Any:
        """Materialize the result as a pandas DataFrame."""

        return self._execute().fetchdf()

    def pl(self) -> Any:
        """Materialize the result as a Polars DataFrame."""

        pl = import_module("polars")

        return pl.from_arrow(self.arrow())

    def arrow(self) -> Any:
        """Materialize the result as a pyarrow Table."""

        return self._execute().fetch_arrow_table()

    def list(self) -> list[dict[str, Any]]:
        """Materialize the result as a list of row dictionaries."""

        cursor = self._execute()
        columns = _column_names(cursor)
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]

    def one(self) -> dict[str, Any]:
        """Return exactly one row as a dictionary."""

        rows = self.list()
        if len(rows) != 1:
            raise ResultCardinalityError(f"expected exactly one row, got {len(rows)}")
        return rows[0]

    def scalar(self) -> Any:
        """Return exactly one scalar value."""

        row = self.one()
        if len(row) != 1:
            raise ResultCardinalityError(f"expected exactly one column, got {len(row)}")
        return next(iter(row.values()))

    def iter(self, *, chunk_size: int = 2048) -> Iterator[dict[str, Any]]:
        """Iterate over row dictionaries without materializing the full result."""

        cursor = self._execute()
        columns = _column_names(cursor)
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            for row in rows:
                yield dict(zip(columns, row, strict=False))

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self.iter()

    def __repr__(self) -> str:
        try:
            cursor = self._connection.execute(
                f"SELECT * FROM ({self._query}) AS _ducklake_preview LIMIT 0"
            )
            columns = ", ".join(_column_names(cursor))
            return f"Result(columns=[{columns}])"
        except Exception:
            return "Result(<lazy>)"

    def _repr_html_(self) -> str:
        try:
            rows = Result(
                self._connection,
                f"SELECT * FROM ({self._query}) AS _ducklake_preview LIMIT 10",
                self._parameters,
            ).list()
        except Exception as exc:
            return f"<pre>{escape(type(exc).__name__)}: {escape(str(exc))}</pre>"

        if not rows:
            return "<p><strong>DuckLake Result</strong>: 0 preview rows</p>"

        columns = list(rows[0])
        header = "".join(f"<th>{escape(column)}</th>" for column in columns)
        body = "".join(
            "<tr>"
            + "".join(f"<td>{escape(str(row.get(column, '')))}</td>" for column in columns)
            + "</tr>"
            for row in rows
        )
        return (
            "<div><strong>DuckLake Result</strong>"
            "<table>"
            f"<thead><tr>{header}</tr></thead>"
            f"<tbody>{body}</tbody>"
            "</table></div>"
        )

    def _execute(self) -> Any:
        try:
            if self._parameters is None:
                return self._connection.execute(self._query)
            return self._connection.execute(self._query, self._parameters)
        except Exception as exc:
            raise DuckLakeQueryError("DuckLake query failed") from exc


def _column_names(cursor: Any) -> list[str]:
    description = cursor.description or []
    return [str(column[0]) for column in description]
