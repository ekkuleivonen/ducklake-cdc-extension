"""Low-level Python wrapper around the ducklake-cdc SQL extension.

This module is the escape hatch. It is a thin transport layer over the SQL
extension's table functions. Method names match the SQL function names exactly
(``cdc_dml_consumer_create``, not ``create_dml_consumer``); it does not reorder
or rename. If you reach for this surface, you are signing up for the SQL
extension's semantics directly.

The headline Python API lives in :mod:`ducklake_cdc` (``DMLConsumer``,
``StdoutDMLSink``, etc.) and uses this module internally.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ducklake import DuckLake

from ducklake_cdc.enums import ChangeType
from ducklake_cdc.sql import scalar_function_sql, table_function_sql

_FIXED_CHANGE_FIELDS = frozenset(
    {
        "consumer_name",
        "snapshot_id",
        "schema_id",
        "schema_name",
        "table_id",
        "table_name",
        "rowid",
        "change_type",
        "snapshot_time",
        "start_snapshot",
        "end_snapshot",
        "next_snapshot_id",
        "values",
        "author",
        "commit_message",
        "commit_extra_info",
    }
)


@dataclass(frozen=True)
class ChangeRow:
    """One DML change row as the SQL extension returns it.

    The user-table columns are collected into ``values``. The other fields are
    extension metadata.
    """

    snapshot_id: int
    change_type: ChangeType
    schema_name: str | None
    table_name: str | None
    rowid: int | None
    snapshot_time: datetime | None
    start_snapshot: int | None
    end_snapshot: int | None
    next_snapshot_id: int | None
    values: dict[str, Any]

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> ChangeRow:
        values_payload = row.get("values")
        if values_payload is None:
            values = {key: value for key, value in row.items() if key not in _FIXED_CHANGE_FIELDS}
        elif isinstance(values_payload, Mapping):
            values = dict(values_payload)
        elif isinstance(values_payload, str):
            parsed = json.loads(values_payload)
            if not isinstance(parsed, dict):
                raise ValueError("expected DML values payload to be a JSON object")
            values = parsed
        else:
            raise TypeError(
                f"expected DML values payload to be a mapping or JSON object, "
                f"got {type(values_payload).__name__}"
            )
        return cls(
            snapshot_id=int(row["snapshot_id"]),
            change_type=ChangeType(row["change_type"]),
            schema_name=_optional_str(row.get("schema_name")),
            table_name=_optional_str(row.get("table_name")),
            rowid=_optional_int(row.get("rowid")),
            snapshot_time=_optional_datetime(row.get("snapshot_time")),
            start_snapshot=_optional_int(row.get("start_snapshot")),
            end_snapshot=_optional_int(row.get("end_snapshot")),
            next_snapshot_id=_optional_int(row.get("next_snapshot_id")),
            values=values,
        )


@dataclass(frozen=True)
class ConsumerCommit:
    """The result row of ``cdc_commit``."""

    consumer_name: str
    committed_snapshot: int
    schema_version: int


@dataclass(frozen=True)
class ConsumerWindow:
    """The next durable consumer window."""

    start_snapshot: int
    end_snapshot: int
    has_changes: bool
    schema_version: int
    schema_changes_pending: bool


@dataclass(frozen=True)
class ConsumerListEntry:
    """A row from ``cdc_list_consumers``; just enough to find one by name."""

    consumer_name: str
    consumer_kind: str
    consumer_id: int


class CDCClient:
    """Direct, untyped-feeling mirror of the ducklake-cdc SQL surface.

    Used internally by :class:`ducklake_cdc.DMLConsumer`. Available as an
    escape hatch for users who need the raw extension semantics.
    """

    def __init__(self, lake: DuckLake, *, catalog: str | None = None) -> None:
        self.lake = lake
        self.catalog = catalog or lake.alias

    def version(self) -> str:
        return str(self.lake.sql(scalar_function_sql("cdc_version")).scalar())

    def cdc_dml_consumer_create(
        self,
        name: str,
        *,
        table_names: list[str] | None = None,
        change_types: list[str] | None = None,
        start_at: str | int = "now",
    ) -> None:
        self._call(
            "cdc_dml_consumer_create",
            name,
            named={
                "start_at": start_at,
                "table_names": table_names,
                "change_types": change_types,
            },
        ).list()

    def cdc_consumer_reset(
        self, name: str, *, to_snapshot: str | int | None = None
    ) -> None:
        self._call(
            "cdc_consumer_reset",
            name,
            named={"to_snapshot": to_snapshot},
        ).list()

    def cdc_consumer_drop(self, name: str) -> None:
        self._call("cdc_consumer_drop", name).list()

    def cdc_consumer_force_release(self, name: str) -> None:
        self._call("cdc_consumer_force_release", name).list()

    def cdc_consumer_heartbeat(self, name: str) -> None:
        self._call("cdc_consumer_heartbeat", name).list()

    def cdc_list_consumers(self) -> list[ConsumerListEntry]:
        rows = self._call("cdc_list_consumers").list()
        return [
            ConsumerListEntry(
                consumer_name=str(row["consumer_name"]),
                consumer_kind=str(row["consumer_kind"]),
                consumer_id=int(row["consumer_id"]),
            )
            for row in rows
        ]

    def cdc_dml_changes_listen(
        self,
        name: str,
        *,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
    ) -> list[ChangeRow]:
        rows = self._call(
            "cdc_dml_changes_listen",
            name,
            named={
                "timeout_ms": timeout_ms,
                "max_snapshots": max_snapshots,
            },
        ).list()
        return [ChangeRow.from_row(row) for row in rows]

    def cdc_window(self, name: str, *, max_snapshots: int = 100) -> ConsumerWindow:
        row = self._call(
            "cdc_window",
            name,
            named={"max_snapshots": max_snapshots},
        ).one()
        return ConsumerWindow(
            start_snapshot=int(row["start_snapshot"]),
            end_snapshot=int(row["end_snapshot"]),
            has_changes=bool(row["has_changes"]),
            schema_version=int(row["schema_version"]),
            schema_changes_pending=bool(row["schema_changes_pending"]),
        )

    def cdc_commit(self, name: str, snapshot: int) -> ConsumerCommit:
        row = self._call("cdc_commit", name, snapshot).one()
        return ConsumerCommit(
            consumer_name=str(row["consumer_name"]),
            committed_snapshot=int(row["committed_snapshot"]),
            schema_version=int(row["schema_version"]),
        )

    def _call(
        self,
        function_name: str,
        *args: Any,
        named: Mapping[str, Any] | None = None,
    ) -> Any:
        sql = table_function_sql(function_name, self.catalog, *args, named=named)
        result = self.lake.sql(sql)
        return result


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    raise TypeError(f"expected datetime, got {type(value).__name__}")
