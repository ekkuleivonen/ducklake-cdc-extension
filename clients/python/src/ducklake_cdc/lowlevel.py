"""Low-level Python wrapper around the ducklake-cdc SQL extension.

This module is the escape hatch. It is a thin transport layer over the SQL
extension's table functions. Method names match the SQL function names exactly
(``cdc_dml_consumer_create``, not ``create_dml_consumer``); it does not reorder
or rename. If you reach for this surface, you are signing up for the SQL
extension's semantics directly.

The headline Python API lives in :mod:`ducklake_cdc` (``DMLConsumer``,
``DDLConsumer``, ``StdoutDMLSink``, ``StdoutDDLSink``, etc.) and uses this
module internally.

DML consumers are pinned to a single table by contract — see
``cdc_dml_consumer_create`` below and ``docs/api.md``. Pass exactly one of
``table_name`` or ``table_id``; the bind rejects "both set" or "neither
set". The corresponding read/listen functions take no per-call table
argument: the table identity is fixed at create time and follows renames
through the extension's subscription rows.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from ducklake import DuckLake
from ducklake_cdc.enums import ChangeType, DdlEventKind, DdlObjectKind
from ducklake_cdc.sql import scalar_function_sql, table_function_sql

#: Top-level columns the typed DML read emits in addition to the table's
#: own columns. Anything *not* in this set is treated as a user-table column
#: and folded into :attr:`ChangeRow.values` so sinks can iterate the row's
#: data without having to know the SQL extension's column order.
_FIXED_CHANGE_FIELDS = frozenset(
    {
        "consumer_name",
        "start_snapshot",
        "end_snapshot",
        "snapshot_id",
        "rowid",
        "change_type",
        "table_id",
        "table_name",
        "snapshot_time",
        "author",
        "commit_message",
        "commit_extra_info",
    }
)


@dataclass(frozen=True)
class ChangeRow:
    """One row from ``cdc_dml_changes_listen`` / ``cdc_dml_changes_read``.

    The typed DML APIs project the consumer's pinned table's columns at
    the *top level* of the result — there is no JSON ``values`` payload
    anymore. This dataclass keeps the extension's metadata fields as
    typed attributes and folds the table's own columns into
    :attr:`values` for ergonomic iteration.

    ``table_id`` and ``table_name`` are the consumer's pinned table
    identity, redundantly carried on every row so a sink that fans out
    to multiple consumers can route without external bookkeeping.
    """

    consumer_name: str | None
    start_snapshot: int | None
    end_snapshot: int | None
    snapshot_id: int
    change_type: ChangeType
    rowid: int | None
    table_id: int | None
    table_name: str | None
    snapshot_time: datetime | None
    author: str | None
    commit_message: str | None
    commit_extra_info: str | None
    values: dict[str, Any]

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> ChangeRow:
        values = {key: value for key, value in row.items() if key not in _FIXED_CHANGE_FIELDS}
        return cls(
            consumer_name=_optional_str(row.get("consumer_name")),
            start_snapshot=_optional_int(row.get("start_snapshot")),
            end_snapshot=_optional_int(row.get("end_snapshot")),
            snapshot_id=int(row["snapshot_id"]),
            change_type=ChangeType(row["change_type"]),
            rowid=_optional_int(row.get("rowid")),
            table_id=_optional_int(row.get("table_id")),
            table_name=_optional_str(row.get("table_name")),
            snapshot_time=_optional_datetime(row.get("snapshot_time")),
            author=_optional_str(row.get("author")),
            commit_message=_optional_str(row.get("commit_message")),
            commit_extra_info=_optional_str(row.get("commit_extra_info")),
            values=values,
        )


@dataclass(frozen=True)
class SchemaChangeRow:
    """One DDL change row as the SQL extension returns it.

    Mirrors the column shape of ``cdc_ddl_changes_listen`` /
    ``cdc_ddl_changes_read``. ``details`` is the JSON text payload emitted
    by the extension and is left unparsed.
    """

    snapshot_id: int
    snapshot_time: datetime | None
    event_kind: DdlEventKind
    object_kind: DdlObjectKind
    schema_id: int | None
    schema_name: str | None
    object_id: int | None
    object_name: str | None
    details: str | None
    start_snapshot: int | None
    end_snapshot: int | None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> SchemaChangeRow:
        return cls(
            snapshot_id=int(row["snapshot_id"]),
            snapshot_time=_optional_datetime(row.get("snapshot_time")),
            event_kind=DdlEventKind(row["event_kind"]),
            object_kind=DdlObjectKind(row["object_kind"]),
            schema_id=_optional_int(row.get("schema_id")),
            schema_name=_optional_str(row.get("schema_name")),
            object_id=_optional_int(row.get("object_id")),
            object_name=_optional_str(row.get("object_name")),
            details=_optional_str(row.get("details")),
            start_snapshot=_optional_int(row.get("start_snapshot")),
            end_snapshot=_optional_int(row.get("end_snapshot")),
        )


@dataclass(frozen=True)
class ConsumerCommit:
    """The result row of ``cdc_commit``."""

    consumer_name: str
    committed_snapshot: int
    schema_version: int


@dataclass(frozen=True)
class ConsumerWindow:
    """The next durable consumer window.

    ``schema_changes_pending`` is strictly scoped to the DML consumer's
    pinned table — DDL on unrelated tables does not flip it. ``terminal``
    indicates the consumer has reached a hard schema-shape boundary on
    the pinned table; once ``terminal=True`` the consumer can no longer
    advance and the orchestrator must spawn a successor.
    ``terminal_at_snapshot`` points at the boundary snapshot (the first
    snapshot of the new shape), or ``None`` when no boundary is pending.
    """

    start_snapshot: int
    end_snapshot: int
    has_changes: bool
    schema_version: int
    schema_changes_pending: bool
    terminal: bool
    terminal_at_snapshot: int | None


@dataclass(frozen=True)
class ConsumerListEntry:
    """A row from ``cdc_list_consumers``.

    Lease state (``owner_token``, ``owner_acquired_at``,
    ``owner_heartbeat_at``, ``lease_interval_seconds``) is included so the
    high-level consumers can implement ``lease_policy`` without an extra
    round-trip. Older catalog rows that have never been leased return
    ``None`` for the owner columns.

    For DML consumers, ``table_id`` is the pinned table's id and
    ``table_name`` is the *current* qualified name (the extension chases
    renames so orchestrators don't have to). Both are ``None`` for DDL
    consumers. ``terminal_at_snapshot`` is the upcoming schema-shape
    boundary for the pinned table or ``None`` if no boundary is pending.
    """

    consumer_name: str
    consumer_kind: str
    consumer_id: int
    table_id: int | None = None
    table_name: str | None = None
    terminal_at_snapshot: int | None = None
    owner_token: UUID | None = None
    owner_acquired_at: datetime | None = None
    owner_heartbeat_at: datetime | None = None
    lease_interval_seconds: int | None = None


class CDCClient:
    """Direct, untyped-feeling mirror of the ducklake-cdc SQL surface.

    Used internally by :class:`ducklake_cdc.DMLConsumer` and
    :class:`ducklake_cdc.DDLConsumer`. Available as an escape hatch for users
    who need the raw extension semantics.
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
        table_name: str | None = None,
        table_id: int | None = None,
        change_types: list[str] | None = None,
        start_at: str | int = "now",
    ) -> None:
        """Create a DML consumer pinned to a single table.

        Pass exactly one of ``table_name`` (qualified, e.g.
        ``"main.orders"``; bare table names default to the ``main``
        schema) or ``table_id``. The SQL extension rejects "both set" and
        "neither set" at bind time. ``change_types`` defaults to
        ``["insert", "update_preimage", "update_postimage", "delete"]``.
        """

        if (table_name is None) == (table_id is None):
            raise ValueError(
                "cdc_dml_consumer_create requires exactly one of table_name "
                "or table_id (DML consumers are pinned to a single table)."
            )
        self._call(
            "cdc_dml_consumer_create",
            name,
            named={
                "start_at": start_at,
                "table_name": table_name,
                "table_id": table_id,
                "change_types": change_types,
            },
        ).list()

    def cdc_ddl_consumer_create(
        self,
        name: str,
        *,
        schemas: list[str] | None = None,
        table_names: list[str] | None = None,
        start_at: str | int = "now",
    ) -> None:
        self._call(
            "cdc_ddl_consumer_create",
            name,
            named={
                "start_at": start_at,
                "schemas": schemas,
                "table_names": table_names,
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
                table_id=_optional_int(row.get("table_id")),
                table_name=_optional_str(row.get("table_name")),
                terminal_at_snapshot=_optional_int(row.get("terminal_at_snapshot")),
                owner_token=_optional_uuid(row.get("owner_token")),
                owner_acquired_at=_optional_datetime(row.get("owner_acquired_at")),
                owner_heartbeat_at=_optional_datetime(row.get("owner_heartbeat_at")),
                lease_interval_seconds=_optional_int(row.get("lease_interval_seconds")),
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
        """Block-listen for the next window of DML changes for ``name``.

        The pinned-table identity is implicit (set at
        ``cdc_dml_consumer_create`` time); there is no per-call table
        parameter. The returned rows project the pinned table's columns
        at the top level (folded into :attr:`ChangeRow.values`).
        """

        rows = self._call(
            "cdc_dml_changes_listen",
            name,
            named={
                "timeout_ms": timeout_ms,
                "max_snapshots": max_snapshots,
            },
        ).list()
        return [ChangeRow.from_row(row) for row in rows]

    def cdc_dml_changes_read(
        self,
        name: str,
        *,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        auto_commit: bool | None = None,
    ) -> list[ChangeRow]:
        """Non-blocking read of the next DML window for ``name``.

        Mirrors :meth:`cdc_dml_changes_listen` semantics but never blocks
        waiting for a snapshot. Pass an explicit ``[start_snapshot,
        end_snapshot]`` window for replays; otherwise the consumer's
        durable cursor is used.
        """

        named: dict[str, Any] = {"max_snapshots": max_snapshots}
        if start_snapshot is not None or end_snapshot is not None:
            if start_snapshot is None or end_snapshot is None:
                raise ValueError(
                    "cdc_dml_changes_read: start_snapshot and end_snapshot must "
                    "both be provided when either is set"
                )
            named["start_snapshot"] = start_snapshot
            named["end_snapshot"] = end_snapshot
        if auto_commit is not None:
            named["auto_commit"] = auto_commit
        rows = self._call("cdc_dml_changes_read", name, named=named).list()
        return [ChangeRow.from_row(row) for row in rows]

    def cdc_dml_changes_query(
        self,
        from_snapshot: int,
        to_snapshot: int | None = None,
        *,
        table_name: str | None = None,
        table_id: int | None = None,
    ) -> list[ChangeRow]:
        """Stateless single-table DML lookback.

        Replays the change rows for one table over an arbitrary
        ``[from_snapshot, to_snapshot]`` range without touching consumer
        state. Pass exactly one of ``table_name`` or ``table_id``.
        """

        if (table_name is None) == (table_id is None):
            raise ValueError(
                "cdc_dml_changes_query requires exactly one of table_name or table_id"
            )
        args: list[Any] = [from_snapshot]
        if to_snapshot is not None:
            args.append(to_snapshot)
        rows = self._call(
            "cdc_dml_changes_query",
            *args,
            named={"table_name": table_name, "table_id": table_id},
        ).list()
        return [ChangeRow.from_row(row) for row in rows]

    def cdc_ddl_changes_listen(
        self,
        name: str,
        *,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
    ) -> list[SchemaChangeRow]:
        rows = self._call(
            "cdc_ddl_changes_listen",
            name,
            named={
                "timeout_ms": timeout_ms,
                "max_snapshots": max_snapshots,
            },
        ).list()
        return [SchemaChangeRow.from_row(row) for row in rows]

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
            terminal=bool(row["terminal"]),
            terminal_at_snapshot=_optional_int(row.get("terminal_at_snapshot")),
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


def _optional_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    return UUID(str(value))
