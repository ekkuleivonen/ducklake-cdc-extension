"""Thin Python wrappers for the ducklake-cdc SQL extension surface."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, TypeVar

from ducklake import DuckLake, Result
from ducklake_cdc.models import (
    AuditEntry,
    CDCModel,
    ChangeRow,
    ConsumerCommit,
    ConsumerDrop,
    ConsumerForceRelease,
    ConsumerHeartbeat,
    ConsumerListEntry,
    ConsumerReset,
    ConsumerStats,
    ConsumerSubscription,
    ConsumerWait,
    ConsumerWindow,
    DdlEvent,
    DoctorDiagnostic,
    SchemaDiff,
    SnapshotEvent,
    Subscription,
)
from ducklake_cdc.sql import SqlValue, scalar_function_sql, table_function_sql

ModelT = TypeVar("ModelT", bound=CDCModel)


class CDCClient:
    """Direct mirror of the ducklake-cdc SQL API for a DuckLake connection."""

    def __init__(self, lake: DuckLake, *, catalog: str | None = None) -> None:
        self.lake = lake
        self.catalog = catalog or lake.alias
        self._notify_connection: Any | None = None
        self._notify_dsn: str | None = None
        self._notify_channel: str | None = None
        self._notify_trigger_available: bool | None = None

    def load_extension(self, path: str | Path | None = None, *, install: bool = True) -> None:
        connection = self.lake.raw_connection()
        if path is not None:
            connection.execute(f"LOAD {str(Path(path))!r}")
            return
        if install:
            connection.execute("INSTALL ducklake_cdc")
        connection.execute("LOAD ducklake_cdc")

    def version(self) -> str:
        return str(self.lake.sql(scalar_function_sql("cdc_version")).scalar())

    def consumer_create(
        self,
        name: str,
        *,
        subscriptions: list[Subscription],
        start_at: str | int = "now",
        stop_at_schema_change: bool = True,
    ) -> list[ConsumerSubscription]:
        return _model_list(
            self._table(
                "cdc_consumer_create",
                name,
                named={
                    "start_at": start_at,
                    "subscriptions": subscriptions,
                    "stop_at_schema_change": stop_at_schema_change,
                },
            ),
            ConsumerSubscription,
        )

    def consumer_reset(self, name: str, *, to_snapshot: int | None = None) -> ConsumerReset:
        return _model_one(
            self._table("cdc_consumer_reset", name, named={"to_snapshot": to_snapshot}),
            ConsumerReset,
        )

    def consumer_drop(self, name: str) -> ConsumerDrop:
        return _model_one(self._table("cdc_consumer_drop", name), ConsumerDrop)

    def consumer_heartbeat(self, name: str) -> ConsumerHeartbeat:
        return _model_one(self._table("cdc_consumer_heartbeat", name), ConsumerHeartbeat)

    def consumer_force_release(self, name: str) -> ConsumerForceRelease:
        return _model_one(self._table("cdc_consumer_force_release", name), ConsumerForceRelease)

    def consumer_list(self) -> list[ConsumerListEntry]:
        return _model_list(self._table("cdc_consumer_list"), ConsumerListEntry)

    def consumer_subscriptions(self, name: str | None = None) -> list[ConsumerSubscription]:
        return _model_list(
            self._table("cdc_consumer_subscriptions", named={"name": name}),
            ConsumerSubscription,
        )

    def window(self, name: str, *, max_snapshots: int = 100) -> ConsumerWindow:
        return _model_one(
            self._table("cdc_window", name, named={"max_snapshots": max_snapshots}),
            ConsumerWindow,
        )

    def commit(self, name: str, snapshot: int) -> ConsumerCommit:
        return _model_one(self._table("cdc_commit", name, snapshot), ConsumerCommit)

    def wait(self, name: str, *, timeout_ms: int = 30_000) -> ConsumerWait:
        notified = self._wait_postgres_notify(name, timeout_ms=timeout_ms)
        if notified is not None:
            return notified
        return _model_one(
            self._table("cdc_wait", name, named={"timeout_ms": timeout_ms}),
            ConsumerWait,
        )

    def changes(
        self,
        name: str,
        *,
        table_id: int | None = None,
        table_name: str | None = None,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
    ) -> Result:
        return self._table(
            "cdc_changes",
            name,
            named={
                "table_id": table_id,
                "table_name": table_name,
                "max_snapshots": max_snapshots,
                "start_snapshot": start_snapshot,
                "end_snapshot": end_snapshot,
            },
        )

    def changes_rows(
        self,
        name: str,
        *,
        table_id: int | None = None,
        table_name: str | None = None,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
    ) -> list[ChangeRow]:
        return [
            ChangeRow.from_row(row)
            for row in self.changes(
                name,
                table_id=table_id,
                table_name=table_name,
                max_snapshots=max_snapshots,
                start_snapshot=start_snapshot,
                end_snapshot=end_snapshot,
            ).list()
        ]

    def ddl(self, name: str, *, max_snapshots: int = 100) -> list[DdlEvent]:
        return _model_list(
            self._table("cdc_ddl", name, named={"max_snapshots": max_snapshots}),
            DdlEvent,
        )

    def events(self, name: str, *, max_snapshots: int = 100) -> list[SnapshotEvent]:
        return _model_list(
            self._table("cdc_events", name, named={"max_snapshots": max_snapshots}),
            SnapshotEvent,
        )

    def recent_changes(self, table_name: str, *, since_seconds: int = 300) -> Result:
        return self._table(
            "cdc_recent_changes",
            table_name,
            named={"since_seconds": since_seconds},
        )

    def recent_changes_rows(
        self,
        table_name: str,
        *,
        since_seconds: int = 300,
    ) -> list[ChangeRow]:
        return [
            ChangeRow.from_row(row)
            for row in self.recent_changes(table_name, since_seconds=since_seconds).list()
        ]

    def recent_ddl(
        self,
        *,
        since_seconds: int = 86_400,
        for_table: str | None = None,
    ) -> list[DdlEvent]:
        return _model_list(
            self._table(
                "cdc_recent_ddl",
                named={"since_seconds": since_seconds, "for_table": for_table},
            ),
            DdlEvent,
        )

    def schema_diff(
        self,
        table_name: str,
        from_snapshot: int,
        to_snapshot: int,
    ) -> list[SchemaDiff]:
        return _model_list(
            self._table("cdc_schema_diff", table_name, from_snapshot, to_snapshot),
            SchemaDiff,
        )

    def range_events(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
    ) -> list[SnapshotEvent]:
        return _model_list(
            self._table("cdc_range_events", from_snapshot, named={"to_snapshot": to_snapshot}),
            SnapshotEvent,
        )

    def range_ddl(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
    ) -> list[DdlEvent]:
        return _model_list(
            self._table("cdc_range_ddl", from_snapshot, named={"to_snapshot": to_snapshot}),
            DdlEvent,
        )

    def range_changes(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
        table_id: int | None = None,
        table_name: str | None = None,
    ) -> Result:
        return self._table(
            "cdc_range_changes",
            from_snapshot,
            named={
                "to_snapshot": to_snapshot,
                "table_id": table_id,
                "table_name": table_name,
            },
        )

    def range_changes_rows(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
        table_id: int | None = None,
        table_name: str | None = None,
    ) -> list[ChangeRow]:
        return [
            ChangeRow.from_row(row)
            for row in self.range_changes(
                from_snapshot,
                to_snapshot=to_snapshot,
                table_id=table_id,
                table_name=table_name,
            ).list()
        ]

    def consumer_stats(self, *, consumer: str | None = None) -> list[ConsumerStats]:
        return _model_list(
            self._table("cdc_consumer_stats", named={"consumer": consumer}),
            ConsumerStats,
        )

    def audit_recent(
        self,
        *,
        since_seconds: int = 86_400,
        consumer: str | None = None,
    ) -> list[AuditEntry]:
        return _model_list(
            self._table(
                "cdc_audit_recent",
                named={"since_seconds": since_seconds, "consumer": consumer},
            ),
            AuditEntry,
        )

    def doctor(self, *, consumer: str | None = None) -> list[DoctorDiagnostic]:
        return _model_list(
            self._table("cdc_doctor", named={"consumer": consumer}),
            DoctorDiagnostic,
        )

    def _table(
        self,
        function_name: str,
        *args: SqlValue,
        named: dict[str, SqlValue | list[Subscription]] | None = None,
    ) -> Result:
        return self.lake.sql(table_function_sql(function_name, self.catalog, *args, named=named))

    def _wait_once(self, name: str) -> ConsumerWait:
        return _model_one(
            self._table("cdc_wait", name, named={"timeout_ms": 0}),
            ConsumerWait,
        )

    def _wait_postgres_notify(self, name: str, *, timeout_ms: int) -> ConsumerWait | None:
        if timeout_ms < 0:
            return None
        metadata = self._postgres_metadata_attachment()
        if metadata is None:
            return None
        dsn, channel = metadata
        try:
            listener = self._postgres_listener(dsn, channel)
            if listener is None or not self._postgres_notify_trigger_available(listener):
                return None

            ready = self._wait_once(name)
            if ready.snapshot_id is not None or timeout_ms == 0:
                return ready

            # Close the race between the zero-timeout probe and LISTEN taking effect.
            ready = self._wait_once(name)
            if ready.snapshot_id is not None:
                return ready

            deadline = time.monotonic() + timeout_ms / 1000.0
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return self._wait_once(name)
                for _notification in listener.notifies(timeout=remaining, stop_after=1):
                    ready = self._wait_once(name)
                    if ready.snapshot_id is not None:
                        return ready
                    break
                else:
                    return self._wait_once(name)
        except Exception:
            self._close_postgres_listener()
            return None

    def _postgres_metadata_attachment(self) -> tuple[str, str] | None:
        metadata_database = f"__ducklake_metadata_{self.catalog}"
        rows = self.lake.raw_connection().execute(
            """
            SELECT type, path
            FROM duckdb_databases()
            WHERE database_name = $database
            LIMIT 1
            """,
            {"database": metadata_database},
        ).fetchall()
        if not rows:
            return None
        backend_type, dsn = rows[0]
        if str(backend_type).lower() not in {"postgres", "postgres_scanner"} or not dsn:
            return None
        return str(dsn), _snapshot_notify_channel(self.catalog)

    def _postgres_listener(self, dsn: str, channel: str) -> Any | None:
        try:
            import psycopg
            from psycopg import sql
        except ImportError:
            return None
        if (
            self._notify_connection is not None
            and not self._notify_connection.closed
            and self._notify_dsn == dsn
            and self._notify_channel == channel
        ):
            return self._notify_connection

        self._close_postgres_listener()
        connection = psycopg.connect(dsn, autocommit=True)
        connection.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
        self._notify_connection = connection
        self._notify_dsn = dsn
        self._notify_channel = channel
        self._notify_trigger_available = None
        return connection

    def _postgres_notify_trigger_available(self, connection: Any) -> bool:
        if self._notify_trigger_available is not None:
            return self._notify_trigger_available
        rows = connection.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgname = 'ducklake_cdc_snapshot_notify'
                  AND NOT tgisinternal
            )
            """
        ).fetchone()
        self._notify_trigger_available = bool(rows and rows[0])
        return self._notify_trigger_available

    def _close_postgres_listener(self) -> None:
        if self._notify_connection is not None:
            try:
                self._notify_connection.close()
            except Exception:
                pass
        self._notify_connection = None
        self._notify_dsn = None
        self._notify_channel = None
        self._notify_trigger_available = None


def _snapshot_notify_channel(catalog: str) -> str:
    prefix = "ducklake_cdc_snapshot_"
    suffix = "".join(char.lower() if char.isalnum() else "_" for char in catalog)
    return (prefix + suffix)[:63]


def _model_one(result: Result, model: type[ModelT]) -> ModelT:
    return model.model_validate(result.one())


def _model_list(result: Result, model: type[ModelT]) -> list[ModelT]:
    return [model.model_validate(row) for row in result.list()]
