"""Lazy DuckDB connection management for DuckLake."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, cast

from ducklake._attach import build_attach_sql
from ducklake.config import (
    CatalogConfig,
    DuckDBConfig,
    DuckDBSettingValue,
    StorageConfig,
    quote_literal,
)
from ducklake.exceptions import DuckLakeConnectionError

_SETTING_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class ConnectionManager:
    catalog: CatalogConfig
    storage: StorageConfig
    alias: str
    duckdb: DuckDBConfig = field(default_factory=DuckDBConfig)
    attach_options: Mapping[str, object] | None = None
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

            config = cast(dict[str, str | bool | int | float | list[str]], dict(self.duckdb.config))
            conn = (
                duckdb.connect(str(self.duckdb.database), config=config)
                if config
                else duckdb.connect(str(self.duckdb.database))
            )
            for name, value in self.duckdb.runtime_settings().items():
                conn.execute(_setting_sql(name, value))
            for extension in self._required_extensions():
                if self.duckdb.install_extensions:
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
            *self.duckdb.extensions,
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
