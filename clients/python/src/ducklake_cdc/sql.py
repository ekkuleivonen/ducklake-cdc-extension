"""SQL rendering helpers for the ducklake-cdc table-function surface."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import Enum
from typing import TypeAlias

from ducklake.config import quote_literal
from ducklake_cdc.models import Subscription

SqlValue: TypeAlias = str | int | float | bool | None | Subscription


def table_function_sql(
    function_name: str,
    *args: SqlValue,
    named: Mapping[str, SqlValue | list[Subscription]] | None = None,
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


def subscription_list_sql(subscriptions: Iterable[Subscription]) -> str:
    rendered = ", ".join(subscription_struct_sql(subscription) for subscription in subscriptions)
    return f"[{rendered}]"


def subscription_struct_sql(subscription: Subscription) -> str:
    data = subscription.model_dump()
    fields = [
        _struct_field("scope_kind", data["scope_kind"]),
        _struct_field("schema_name", data["schema_name"], null_type="VARCHAR"),
        _struct_field("table_name", data["table_name"], null_type="VARCHAR"),
        _struct_field("schema_id", data["schema_id"], null_type="BIGINT"),
        _struct_field("table_id", data["table_id"], null_type="BIGINT"),
        _struct_field("event_category", data["event_category"]),
        _struct_field("change_type", data["change_type"]),
    ]
    return f"struct_pack({', '.join(fields)})"


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
    if isinstance(value, Subscription):
        return subscription_struct_sql(value)
    if isinstance(value, list) and all(isinstance(item, Subscription) for item in value):
        return subscription_list_sql(value)
    return quote_literal(value)
