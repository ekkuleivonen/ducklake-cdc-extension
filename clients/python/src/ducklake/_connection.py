"""Lazy DuckDB connection management for DuckLake."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias, cast

from ducklake._attach import build_attach_sql
from ducklake.config import CatalogConfig, StorageConfig, quote_literal
from ducklake.exceptions import DuckLakeConnectionError

DuckDBConfig: TypeAlias = dict[str, str | bool | int | float | list[str]]
DuckDBSettingValue: TypeAlias = str | bool | int | float
DuckDBSettings: TypeAlias = Mapping[str, DuckDBSettingValue]
_SETTING_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class ConnectionManager:
    catalog: CatalogConfig
    storage: StorageConfig
    alias: str
    database: str = ":memory:"
    duckdb_config: Mapping[str, str | bool | int | float | list[str]] | None = None
    duckdb_settings: DuckDBSettings | None = None
    attach_options: Mapping[str, object] | None = None
    install_extensions: bool = True
    _connection: Any | None = field(default=None, init=False, repr=False)

    def get(self) -> Any:
        if self._connection is None:
            self._connection = self._connect()
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _connect(self) -> Any:
        try:
            import duckdb

            config = cast(DuckDBConfig | None, self.duckdb_config)
            conn = (
                duckdb.connect(self.database, config=config)
                if config is not None
                else duckdb.connect(self.database)
            )
            for name, value in (self.duckdb_settings or {}).items():
                conn.execute(_setting_sql(name, value))
            for extension in self._required_extensions():
                if self.install_extensions:
                    conn.execute(f"INSTALL {extension}")
                conn.execute(f"LOAD {extension}")
            for statement in self.storage.setup_statements(secret_name=f"{self.alias}_storage"):
                conn.execute(statement)
            conn.execute(
                build_attach_sql(
                    catalog=self.catalog,
                    storage=self.storage,
                    alias=self.alias,
                    attach_options=self.attach_options,
                )
            )
            return conn
        except Exception as exc:
            raise DuckLakeConnectionError("failed to initialize DuckLake connection") from exc

    def _required_extensions(self) -> tuple[str, ...]:
        names = [
            "ducklake",
            "parquet",
            *self.catalog.required_extensions(),
            *self.storage.required_extensions(),
        ]
        return tuple(dict.fromkeys(names))


def _setting_sql(name: str, value: DuckDBSettingValue) -> str:
    if not _SETTING_NAME.fullmatch(name):
        raise DuckLakeConnectionError(f"invalid DuckDB setting name: {name!r}")
    if isinstance(value, bool):
        rendered_value = "true" if value else "false"
    elif isinstance(value, int | float):
        rendered_value = str(value)
    else:
        rendered_value = quote_literal(value)
    return f"SET {name} = {rendered_value}"
