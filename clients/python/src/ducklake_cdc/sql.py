"""SQL rendering helpers for the ducklake-cdc table-function surface."""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import TypeAlias

from ducklake.config import quote_literal
SqlValue: TypeAlias = str | int | float | bool | None


def table_function_sql(
    function_name: str,
    *args: SqlValue,
    named: Mapping[str, SqlValue | list[str] | list[int]] | None = None,
) -> str:
    rendered_args = [_render_value(arg) for arg in args]
    for name, value in (named or {}).items():
        if value is None:
            continue
        rendered_args.append(f"{name} := {_render_value(value)}")
    return f"SELECT * FROM {function_name}({', '.join(rendered_args)})"


def scalar_function_sql(function_name: str, *args: SqlValue) -> str:
    rendered_args = ", ".join(_render_value(arg) for arg in args)
    return f"SELECT {function_name}({rendered_args})"


def _struct_field(name: str, value: object, *, null_type: str | None = None) -> str:
    return f"{name} := {_render_value(value, null_type=null_type)}"


def _render_value(value: object, *, null_type: str | None = None) -> str:
    if value is None:
        return f"NULL::{null_type}" if null_type else "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, Enum):
        return quote_literal(value.value)
    if isinstance(value, list):
        return "[" + ", ".join(_render_value(item) for item in value) + "]"
    return quote_literal(value)
