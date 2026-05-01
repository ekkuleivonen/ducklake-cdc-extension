"""Public DuckLake client entry point."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ducklake._connection import ConnectionManager, DuckDBSettings
from ducklake.config import (
    CatalogInput,
    StorageInput,
    parse_catalog,
    parse_storage,
    quote_identifier,
)
from ducklake.result import QueryParameters, Result
from ducklake.session import Session, Transaction
from ducklake.table import Table


@dataclass
class DuckLake:
    """A lazy DuckLake connection wrapper."""

    _manager: ConnectionManager
    alias: str = "lake"

    @classmethod
    def open(
        cls,
        *,
        catalog: CatalogInput,
        storage: StorageInput,
        alias: str = "lake",
        database: str = ":memory:",
        duckdb_config: Mapping[str, str | bool | int | float | list[str]] | None = None,
        duckdb_settings: DuckDBSettings | None = None,
        attach_options: Mapping[str, object] | None = None,
        install_extensions: bool = True,
    ) -> DuckLake:
        parsed_catalog = parse_catalog(catalog)
        parsed_storage = parse_storage(storage)
        return cls(
            ConnectionManager(
                catalog=parsed_catalog,
                storage=parsed_storage,
                alias=alias,
                database=database,
                duckdb_config=duckdb_config,
                duckdb_settings=duckdb_settings,
                attach_options=attach_options,
                install_extensions=install_extensions,
            ),
            alias=alias,
        )

    def sql(self, query: str, *parameters: object, **named_parameters: object) -> Result:
        return Result(
            self.raw_connection(),
            query,
            _normalize_parameters(parameters, named_parameters),
        )

    def session(self) -> Session:
        return Session(self)

    def transaction(self) -> Transaction:
        return Transaction(self)

    def raw_connection(self) -> Any:
        return self._manager.get()

    def close(self) -> None:
        self._manager.close()

    def tables(self, *, schema_name: str | None = None) -> list[Table]:
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_catalog = $catalog
              AND table_type = 'BASE TABLE'
        """
        parameters: dict[str, object] = {"catalog": self.alias}
        if schema_name is not None:
            query += " AND table_schema = $schema"
            parameters["schema"] = schema_name
        query += " ORDER BY table_schema, table_name"

        return [
            Table(self, str(row["table_name"]), schema_name=str(row["table_schema"]))
            for row in self.sql(query, **parameters).list()
        ]

    def schemas(self) -> list[str]:
        rows = self.sql(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE catalog_name = $catalog
            ORDER BY schema_name
            """,
            catalog=self.alias,
        ).list()
        return [str(row["schema_name"]) for row in rows]

    def table(self, name: str, *, schema_name: str | None = None) -> Table:
        return Table(self, name, schema_name=schema_name)

    def snapshots(self) -> Result:
        return self.sql(
            f"SELECT * FROM {quote_identifier(self.alias)}.snapshots() ORDER BY snapshot_id"
        )

    def __enter__(self) -> DuckLake:
        self.raw_connection()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


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
