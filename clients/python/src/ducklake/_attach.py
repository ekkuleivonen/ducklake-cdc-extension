"""Internal helpers for DuckLake ATTACH SQL generation."""

from __future__ import annotations

from collections.abc import Mapping

from ducklake.config import CatalogConfig, StorageConfig, quote_identifier, quote_literal


def build_attach_sql(
    *,
    catalog: CatalogConfig,
    storage: StorageConfig,
    alias: str,
    attach_options: Mapping[str, object] | None = None,
) -> str:
    options: dict[str, object] = {"DATA_PATH": storage.data_path()}
    if attach_options:
        options.update(attach_options)
    rendered_options = ", ".join(
        f"{key.upper()} {_format_attach_value(value)}" for key, value in options.items()
    )
    return (
        f"ATTACH {quote_literal(catalog.attach_uri())} "
        f"AS {quote_identifier(alias)} ({rendered_options})"
    )


def _format_attach_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    return quote_literal(value)
