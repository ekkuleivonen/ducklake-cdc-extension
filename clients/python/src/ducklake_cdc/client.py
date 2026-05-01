"""Thin Python wrappers for the ducklake-cdc SQL extension surface."""

from __future__ import annotations

from pathlib import Path
from typing import TypeVar

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
    ConsumerWindow,
    DdlEvent,
    DoctorDiagnostic,
    SchemaDiff,
    SnapshotEvent,
)
from ducklake_cdc.sql import SqlValue, scalar_function_sql, table_function_sql

ModelT = TypeVar("ModelT", bound=CDCModel)


class CDCClient:
    """Direct mirror of the ducklake-cdc SQL API for a DuckLake connection."""

    def __init__(self, lake: DuckLake, *, catalog: str | None = None) -> None:
        self.lake = lake
        self.catalog = catalog or lake.alias

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

    def ddl_consumer_create(
        self,
        name: str,
        *,
        schemas: list[str] | None = None,
        schema_ids: list[int] | None = None,
        table_names: list[str] | None = None,
        table_ids: list[int] | None = None,
        start_at: str | int = "now",
        metadata: str | None = None,
    ) -> list[ConsumerSubscription]:
        return _model_list(
            self._table(
                "cdc_ddl_consumer_create",
                name,
                named={
                    "start_at": start_at,
                    "schemas": schemas,
                    "schema_ids": schema_ids,
                    "table_names": table_names,
                    "table_ids": table_ids,
                    "metadata": metadata,
                },
            ),
            ConsumerSubscription,
        )

    def dml_consumer_create(
        self,
        name: str,
        *,
        table_names: list[str] | None = None,
        table_ids: list[int] | None = None,
        change_types: list[str] | None = None,
        start_at: str | int = "now",
        metadata: str | None = None,
    ) -> list[ConsumerSubscription]:
        return _model_list(
            self._table(
                "cdc_dml_consumer_create",
                name,
                named={
                    "start_at": start_at,
                    "table_names": table_names,
                    "table_ids": table_ids,
                    "change_types": change_types,
                    "metadata": metadata,
                },
            ),
            ConsumerSubscription,
        )

    def consumer_reset(self, name: str, *, to_snapshot: str | int | None = None) -> ConsumerReset:
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
        return _model_list(self._table("cdc_list_consumers"), ConsumerListEntry)

    def consumer_subscriptions(self, name: str | None = None) -> list[ConsumerSubscription]:
        return _model_list(
            self._table("cdc_list_subscriptions", named={"name": name}),
            ConsumerSubscription,
        )

    def window(self, name: str, *, max_snapshots: int = 100) -> ConsumerWindow:
        return _model_one(
            self._table("cdc_window", name, named={"max_snapshots": max_snapshots}),
            ConsumerWindow,
        )

    def commit(self, name: str, snapshot: int) -> ConsumerCommit:
        return _model_one(self._table("cdc_commit", name, snapshot), ConsumerCommit)

    def dml_table_changes_read(
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
            "cdc_dml_table_changes_read",
            name,
            named={
                "table_id": table_id,
                "table_name": table_name,
                "max_snapshots": max_snapshots,
                "start_snapshot": start_snapshot,
                "end_snapshot": end_snapshot,
            },
        )

    def dml_table_changes_read_rows(
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
            for row in self.dml_table_changes_read(
                name,
                table_id=table_id,
                table_name=table_name,
                max_snapshots=max_snapshots,
                start_snapshot=start_snapshot,
                end_snapshot=end_snapshot,
            ).list()
        ]

    def dml_table_changes_listen(
        self,
        name: str,
        *,
        table_id: int | None = None,
        table_name: str | None = None,
        timeout_ms: int = 30_000,
        max_snapshots: int = 100,
        auto_commit: bool = False,
    ) -> Result:
        return self._table(
            "cdc_dml_table_changes_listen",
            name,
            named={
                "table_id": table_id,
                "table_name": table_name,
                "timeout_ms": timeout_ms,
                "max_snapshots": max_snapshots,
                "auto_commit": auto_commit,
            },
        )

    def dml_table_changes_listen_rows(
        self,
        name: str,
        *,
        table_id: int | None = None,
        table_name: str | None = None,
        timeout_ms: int = 30_000,
        max_snapshots: int = 100,
        auto_commit: bool = False,
    ) -> list[ChangeRow]:
        return [
            ChangeRow.from_row(row)
            for row in self.dml_table_changes_listen(
                name,
                table_id=table_id,
                table_name=table_name,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
                auto_commit=auto_commit,
            ).list()
        ]

    def dml_changes_read(
        self,
        name: str,
        *,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        auto_commit: bool = False,
    ) -> Result:
        return self._table(
            "cdc_dml_changes_read",
            name,
            named={
                "max_snapshots": max_snapshots,
                "start_snapshot": start_snapshot,
                "end_snapshot": end_snapshot,
                "auto_commit": auto_commit,
            },
        )

    def dml_changes_listen(
        self,
        name: str,
        *,
        timeout_ms: int = 30_000,
        max_snapshots: int = 100,
        auto_commit: bool = False,
    ) -> Result:
        return self._table(
            "cdc_dml_changes_listen",
            name,
            named={
                "timeout_ms": timeout_ms,
                "max_snapshots": max_snapshots,
                "auto_commit": auto_commit,
            },
        )

    def ddl_changes_read(
        self,
        name: str,
        *,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        auto_commit: bool = False,
    ) -> list[DdlEvent]:
        return _model_list(
            self._table(
                "cdc_ddl_changes_read",
                name,
                named={
                    "max_snapshots": max_snapshots,
                    "start_snapshot": start_snapshot,
                    "end_snapshot": end_snapshot,
                    "auto_commit": auto_commit,
                },
            ),
            DdlEvent,
        )

    def ddl_changes_listen(
        self,
        name: str,
        *,
        timeout_ms: int = 30_000,
        max_snapshots: int = 100,
        auto_commit: bool = False,
    ) -> list[DdlEvent]:
        return _model_list(
            self._table(
                "cdc_ddl_changes_listen",
                name,
                named={
                    "timeout_ms": timeout_ms,
                    "max_snapshots": max_snapshots,
                    "auto_commit": auto_commit,
                },
            ),
            DdlEvent,
        )

    def dml_changes_read_rows(
        self,
        name: str,
        *,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        auto_commit: bool = False,
    ) -> list[ChangeRow]:
        return [
            ChangeRow.from_row(row)
            for row in self.dml_changes_read(
                name,
                max_snapshots=max_snapshots,
                start_snapshot=start_snapshot,
                end_snapshot=end_snapshot,
                auto_commit=auto_commit,
            ).list()
        ]

    def dml_changes_listen_rows(
        self,
        name: str,
        *,
        timeout_ms: int = 30_000,
        max_snapshots: int = 100,
        auto_commit: bool = False,
    ) -> list[ChangeRow]:
        return [
            ChangeRow.from_row(row)
            for row in self.dml_changes_listen(
                name,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
                auto_commit=auto_commit,
            ).list()
        ]

    def dml_ticks_read(
        self,
        name: str,
        *,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        auto_commit: bool = False,
    ) -> list[SnapshotEvent]:
        return _model_list(
            self._table(
                "cdc_dml_ticks_read",
                name,
                named={
                    "max_snapshots": max_snapshots,
                    "start_snapshot": start_snapshot,
                    "end_snapshot": end_snapshot,
                    "auto_commit": auto_commit,
                },
            ),
            SnapshotEvent,
        )

    def dml_ticks_listen(
        self,
        name: str,
        *,
        timeout_ms: int = 30_000,
        max_snapshots: int = 100,
        auto_commit: bool = False,
    ) -> list[SnapshotEvent]:
        return _model_list(
            self._table(
                "cdc_dml_ticks_listen",
                name,
                named={
                    "timeout_ms": timeout_ms,
                    "max_snapshots": max_snapshots,
                    "auto_commit": auto_commit,
                },
            ),
            SnapshotEvent,
        )

    def ddl_ticks_read(
        self,
        name: str,
        *,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        auto_commit: bool = False,
    ) -> list[SnapshotEvent]:
        return _model_list(
            self._table(
                "cdc_ddl_ticks_read",
                name,
                named={
                    "max_snapshots": max_snapshots,
                    "start_snapshot": start_snapshot,
                    "end_snapshot": end_snapshot,
                    "auto_commit": auto_commit,
                },
            ),
            SnapshotEvent,
        )

    def ddl_ticks_listen(
        self,
        name: str,
        *,
        timeout_ms: int = 30_000,
        max_snapshots: int = 100,
        auto_commit: bool = False,
    ) -> list[SnapshotEvent]:
        return _model_list(
            self._table(
                "cdc_ddl_ticks_listen",
                name,
                named={
                    "timeout_ms": timeout_ms,
                    "max_snapshots": max_snapshots,
                    "auto_commit": auto_commit,
                },
            ),
            SnapshotEvent,
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

    def dml_ticks_query(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
        table_ids: list[int] | None = None,
        table_names: list[str] | None = None,
    ) -> list[SnapshotEvent]:
        return _model_list(
            self._table(
                "cdc_dml_ticks_query",
                from_snapshot,
                named={"to_snapshot": to_snapshot, "table_ids": table_ids, "table_names": table_names},
            ),
            SnapshotEvent,
        )

    def ddl_ticks_query(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
        schemas: list[str] | None = None,
        schema_ids: list[int] | None = None,
        table_names: list[str] | None = None,
        table_ids: list[int] | None = None,
    ) -> list[SnapshotEvent]:
        return _model_list(
            self._table(
                "cdc_ddl_ticks_query",
                from_snapshot,
                named={
                    "to_snapshot": to_snapshot,
                    "schemas": schemas,
                    "schema_ids": schema_ids,
                    "table_names": table_names,
                    "table_ids": table_ids,
                },
            ),
            SnapshotEvent,
        )

    def ddl_changes_query(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
        schemas: list[str] | None = None,
        schema_ids: list[int] | None = None,
        table_names: list[str] | None = None,
        table_ids: list[int] | None = None,
    ) -> list[DdlEvent]:
        return _model_list(
            self._table(
                "cdc_ddl_changes_query",
                from_snapshot,
                named={
                    "to_snapshot": to_snapshot,
                    "schemas": schemas,
                    "schema_ids": schema_ids,
                    "table_names": table_names,
                    "table_ids": table_ids,
                },
            ),
            DdlEvent,
        )

    def dml_changes_query(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
        table_ids: list[int] | None = None,
        table_names: list[str] | None = None,
        change_types: list[str] | None = None,
    ) -> Result:
        return self._table(
            "cdc_dml_changes_query",
            from_snapshot,
            named={
                "to_snapshot": to_snapshot,
                "table_ids": table_ids,
                "table_names": table_names,
                "change_types": change_types,
            },
        )

    def dml_changes_query_rows(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
        table_ids: list[int] | None = None,
        table_names: list[str] | None = None,
        change_types: list[str] | None = None,
    ) -> list[ChangeRow]:
        return [
            ChangeRow.from_row(row)
            for row in self.dml_changes_query(
                from_snapshot,
                to_snapshot=to_snapshot,
                table_ids=table_ids,
                table_names=table_names,
                change_types=change_types,
            ).list()
        ]

    def dml_table_changes_query(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
        table_id: int | None = None,
        table_name: str | None = None,
    ) -> Result:
        return self._table(
            "cdc_dml_table_changes_query",
            from_snapshot,
            named={
                "to_snapshot": to_snapshot,
                "table_id": table_id,
                "table_name": table_name,
            },
        )

    def dml_table_changes_query_rows(
        self,
        from_snapshot: int,
        *,
        to_snapshot: int | None = None,
        table_id: int | None = None,
        table_name: str | None = None,
    ) -> list[ChangeRow]:
        return [
            ChangeRow.from_row(row)
            for row in self.dml_table_changes_query(
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

    def audit_events(
        self,
        *,
        since_seconds: int = 86_400,
        consumer: str | None = None,
    ) -> list[AuditEntry]:
        return _model_list(
            self._table(
                "cdc_audit_events",
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
        named: dict[str, SqlValue | list[str] | list[int]] | None = None,
    ) -> Result:
        return self.lake.sql(table_function_sql(function_name, self.catalog, *args, named=named))

def _model_one(result: Result, model: type[ModelT]) -> ModelT:
    return model.model_validate(result.one())


def _model_list(result: Result, model: type[ModelT]) -> list[ModelT]:
    return [model.model_validate(row) for row in result.list()]
