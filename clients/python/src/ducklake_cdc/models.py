"""Pydantic contracts for the ducklake-cdc SQL API."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ducklake_cdc.enums import (
    ChangeType,
    DdlEventKind,
    DdlObjectKind,
    DiagnosticSeverity,
    EventCategory,
    ScopeKind,
    SubscriptionStatus,
)


class CDCModel(BaseModel):
    """Base model for fixed-shape ducklake-cdc rows."""

    model_config = ConfigDict(extra="forbid", frozen=True, use_enum_values=True)


class Subscription(CDCModel):
    """Input model for `cdc_consumer_create(..., subscriptions := ...)`."""

    scope_kind: ScopeKind
    schema_name: str | None = None
    table_name: str | None = None
    schema_id: int | None = None
    table_id: int | None = None
    event_category: EventCategory = EventCategory.ALL
    change_type: ChangeType = ChangeType.ALL

    @model_validator(mode="after")
    def _validate_scope(self) -> Subscription:
        if self.scope_kind == ScopeKind.CATALOG and any(
            value is not None
            for value in (self.schema_name, self.table_name, self.schema_id, self.table_id)
        ):
            raise ValueError("catalog subscriptions must not set schema or table fields")
        if self.scope_kind == ScopeKind.SCHEMA and self.table_id is not None:
            raise ValueError("schema subscriptions must not set table_id")
        if self.scope_kind == ScopeKind.TABLE and not (
            self.table_id is not None or self.table_name is not None
        ):
            raise ValueError("table subscriptions require table_id or table_name")
        if self.event_category == EventCategory.DDL and self.change_type != ChangeType.ALL:
            raise ValueError("DDL subscriptions must use change_type='*'")
        return self


class ConsumerSubscription(CDCModel):
    """Normalized subscription row from create/subscription inspection calls."""

    consumer_name: str
    consumer_id: int
    last_committed_snapshot: int | None = None
    subscription_id: int
    scope_kind: ScopeKind
    schema_id: int | None = None
    table_id: int | None = None
    event_category: EventCategory
    change_type: ChangeType
    original_qualified_name: str | None = None
    current_qualified_name: str | None = None
    status: SubscriptionStatus | None = None


class ConsumerReset(CDCModel):
    consumer_name: str
    consumer_id: int
    previous_snapshot: int
    new_snapshot: int


class ConsumerDrop(CDCModel):
    consumer_name: str
    consumer_id: int
    last_committed_snapshot: int


class ConsumerHeartbeat(CDCModel):
    heartbeat_extended: bool


class ConsumerForceRelease(CDCModel):
    consumer_name: str
    consumer_id: int
    previous_token: UUID | None = None


class ConsumerListEntry(CDCModel):
    consumer_name: str
    consumer_id: int
    subscription_count: int
    subscriptions_active: int
    subscriptions_renamed: int
    subscriptions_dropped: int
    stop_at_schema_change: bool
    last_committed_snapshot: int
    last_committed_schema_version: int
    owner_token: UUID | None = None
    owner_acquired_at: datetime | None = None
    owner_heartbeat_at: datetime | None = None
    lease_interval_seconds: int
    created_at: datetime
    created_by: str | None = None
    updated_at: datetime
    metadata: str | None = None


class ConsumerWindow(CDCModel):
    start_snapshot: int
    end_snapshot: int
    has_changes: bool
    schema_version: int
    schema_changes_pending: bool


class ConsumerCommit(CDCModel):
    consumer_name: str
    committed_snapshot: int
    schema_version: int


class ConsumerWait(CDCModel):
    snapshot_id: int | None = None


class ChangeRow(CDCModel):
    """Dynamic DML row with user table columns collected in `values`."""

    snapshot_id: int
    rowid: int | None = None
    change_type: ChangeType
    snapshot_time: datetime | None = None
    author: str | None = None
    commit_message: str | None = None
    commit_extra_info: str | None = None
    next_snapshot_id: int | None = None
    values: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> ChangeRow:
        fixed_keys = set(cls.model_fields)
        values = {key: value for key, value in row.items() if key not in fixed_keys}
        fixed = {key: value for key, value in row.items() if key in fixed_keys}
        return cls(**fixed, values=values)


class DdlEvent(CDCModel):
    snapshot_id: int
    snapshot_time: datetime
    event_kind: DdlEventKind
    object_kind: DdlObjectKind
    schema_id: int | None = None
    schema_name: str | None = None
    object_id: int | None = None
    object_name: str | None = None
    details: str | None = None


class SnapshotEvent(CDCModel):
    snapshot_id: int
    snapshot_time: datetime
    changes_made: str
    author: str | None = None
    commit_message: str | None = None
    commit_extra_info: str | None = None
    next_snapshot_id: int | None = None
    schema_version: int
    schema_changes_pending: bool


class SchemaDiff(CDCModel):
    snapshot_id: int
    snapshot_time: datetime
    change_kind: str
    column_id: int | None = None
    old_name: str | None = None
    new_name: str | None = None
    old_type: str | None = None
    new_type: str | None = None
    old_default: str | None = None
    new_default: str | None = None
    old_nullable: bool | None = None
    new_nullable: bool | None = None
    parent_column_id: int | None = None


class ConsumerStats(CDCModel):
    consumer_name: str
    consumer_id: int
    last_committed_snapshot: int
    current_snapshot: int
    lag_snapshots: int
    lag_seconds: float
    oldest_available_snapshot: int
    gap_distance: int
    subscription_count: int
    subscriptions_active: int
    subscriptions_renamed: int
    subscriptions_dropped: int
    owner_token: UUID | None = None
    owner_acquired_at: datetime | None = None
    owner_heartbeat_at: datetime | None = None
    lease_interval_seconds: int
    lease_alive: bool


class AuditEntry(CDCModel):
    ts: datetime
    audit_id: int
    actor: str | None = None
    action: str
    consumer_name: str | None = None
    consumer_id: int | None = None
    details: str | None = None


class DoctorDiagnostic(CDCModel):
    severity: DiagnosticSeverity
    code: str
    consumer_name: str | None = None
    message: str
    details: str | None = None
