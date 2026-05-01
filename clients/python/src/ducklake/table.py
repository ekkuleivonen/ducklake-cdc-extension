"""DuckLake table abstractions."""

from __future__ import annotations

from typing import Any, cast

from ducklake.config import DuckLakeModel, quote_identifier
from ducklake.exceptions import DuckLakeConfigError
from ducklake.result import Result


class Column(DuckLakeModel):
    name: str
    data_type: str
    nullable: bool
    ordinal_position: int


class Snapshot(DuckLakeModel):
    snapshot_id: int
    snapshot_time: Any | None = None
    changes_made: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Snapshot:
        return cls(
            snapshot_id=int(row["snapshot_id"]),
            snapshot_time=row.get("snapshot_time"),
            changes_made=row.get("changes_made"),
        )


class Table:
    """A DuckLake table handle with discovery and query helpers."""

    def __init__(self, lake: Any, name: str, *, schema_name: str | None = None) -> None:
        schema, table = _split_table_name(name, schema_name=schema_name)
        self._lake = lake
        self.schema_name = schema
        self.name = table

    @property
    def qualified_name(self) -> str:
        return ".".join(
            quote_identifier(part) for part in (self._lake.alias, self.schema_name, self.name)
        )

    def schema(self) -> list[Column]:
        rows = self._lake.sql(
            """
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_catalog = $catalog
              AND table_schema = $schema
              AND table_name = $table
            ORDER BY ordinal_position
            """,
            catalog=self._lake.alias,
            schema=self.schema_name,
            table=self.name,
        ).list()
        return [
            Column(
                name=str(row["column_name"]),
                data_type=str(row["data_type"]),
                nullable=str(row["is_nullable"]).upper() == "YES",
                ordinal_position=int(row["ordinal_position"]),
            )
            for row in rows
        ]

    def snapshots(self) -> list[Snapshot]:
        rows = self._lake.sql(
            f"SELECT * FROM {quote_identifier(self._lake.alias)}.snapshots() ORDER BY snapshot_id"
        ).list()
        return [Snapshot.from_row(row) for row in rows]

    def row_count(self) -> int:
        return int(self._lake.sql(f"SELECT count(*) AS count FROM {self.qualified_name}").scalar())

    def head(self, n: int = 10) -> Result:
        if n < 0:
            raise DuckLakeConfigError("head row count must be non-negative")
        return cast(Result, self._lake.sql(f"SELECT * FROM {self.qualified_name} LIMIT {n}"))

    def at(self, *, snapshot: int | None = None, timestamp: str | None = None) -> Result:
        if (snapshot is None) == (timestamp is None):
            raise DuckLakeConfigError("pass exactly one of snapshot= or timestamp=")
        if snapshot is not None:
            return cast(
                Result,
                self._lake.sql(
                    f"SELECT * FROM {self.qualified_name} AT (VERSION => $snapshot)",
                    snapshot=snapshot,
                ),
            )
        return cast(
            Result,
            self._lake.sql(
                f"SELECT * FROM {self.qualified_name} AT (TIMESTAMP => $timestamp)",
                timestamp=timestamp,
            ),
        )

    def between(self, start_snapshot: int, end_snapshot: int) -> Result:
        changes_function = f"{quote_identifier(self._lake.alias)}.table_changes"
        return cast(
            Result,
            self._lake.sql(
                f"SELECT * FROM {changes_function}($table, $start, $end)",
                table=self.name,
                start=start_snapshot,
                end=end_snapshot,
            ),
        )

    def _repr_html_(self) -> str:
        return (
            "<p><strong>DuckLake Table</strong>: "
            f"{self.schema_name}.{self.name} in {self._lake.alias}</p>"
        )

    def __repr__(self) -> str:
        return f"Table({self.schema_name}.{self.name})"


def _split_table_name(name: str, *, schema_name: str | None) -> tuple[str, str]:
    if not name:
        raise DuckLakeConfigError("table name must not be empty")
    parts = name.split(".")
    if schema_name is not None:
        if len(parts) != 1:
            raise DuckLakeConfigError("pass either 'schema.table' or schema_name=, not both")
        return schema_name, name
    if len(parts) == 1:
        return "main", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise DuckLakeConfigError(f"invalid table name: {name!r}")
