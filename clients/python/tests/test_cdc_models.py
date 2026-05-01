from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from ducklake_cdc import (
    AuditEntry,
    ChangeRow,
    ConsumerStats,
    ConsumerSubscription,
    ConsumerWait,
    ConsumerWindow,
    DdlEvent,
    DoctorDiagnostic,
    SnapshotEvent,
    Subscription,
)


def test_subscription_validates_scope_and_ddl_change_type() -> None:
    subscription = Subscription.model_validate(
        {
            "scope_kind": "table",
            "schema_name": "main",
            "table_name": "orders",
            "event_category": "dml",
            "change_type": "insert",
        }
    )

    assert subscription.model_dump() == {
        "scope_kind": "table",
        "schema_name": "main",
        "table_name": "orders",
        "schema_id": None,
        "table_id": None,
        "event_category": "dml",
        "change_type": "insert",
    }

    with pytest.raises(ValidationError):
        Subscription.model_validate({"scope_kind": "catalog", "table_name": "orders"})

    with pytest.raises(ValidationError):
        Subscription.model_validate(
            {"scope_kind": "schema", "event_category": "ddl", "change_type": "insert"}
        )


def test_consumer_subscription_matches_create_and_inspection_rows() -> None:
    row = ConsumerSubscription.model_validate(
        {
            "consumer_name": "orders_sink",
            "consumer_id": 1,
            "subscription_id": 2,
            "scope_kind": "table",
            "schema_id": 0,
            "table_id": 5,
            "event_category": "dml",
            "change_type": "*",
            "original_qualified_name": "main.orders",
            "current_qualified_name": "main.orders_v2",
            "status": "renamed",
        }
    )

    assert row.status == "renamed"
    assert row.model_dump()["change_type"] == "*"


def test_cursor_models_coerce_fixed_rows() -> None:
    window = ConsumerWindow.model_validate(
        {
            "start_snapshot": "7",
            "end_snapshot": 42,
            "has_changes": True,
            "schema_version": 3,
            "schema_changes_pending": False,
        }
    )
    wait = ConsumerWait(snapshot_id=None)

    assert window.start_snapshot == 7
    assert wait.snapshot_id is None


def test_change_row_collects_dynamic_table_columns() -> None:
    change = ChangeRow.from_row(
        {
            "snapshot_id": 42,
            "rowid": 0,
            "change_type": "insert",
            "id": 2,
            "status": "paid",
            "snapshot_time": datetime(2026, 4, 30, tzinfo=UTC),
            "author": None,
            "commit_message": None,
            "commit_extra_info": None,
            "next_snapshot_id": 43,
        }
    )

    assert change.values == {"id": 2, "status": "paid"}
    assert change.model_dump()["change_type"] == "insert"


def test_event_and_observability_models_match_docs() -> None:
    now = datetime(2026, 4, 30, 9, 30, tzinfo=UTC)

    ddl = DdlEvent.model_validate(
        {
            "snapshot_id": 11,
            "snapshot_time": now,
            "event_kind": "created",
            "object_kind": "table",
            "schema_id": 0,
            "schema_name": "main",
            "object_id": 5,
            "object_name": "orders",
            "details": '{"columns":["id","status"]}',
        }
    )
    event = SnapshotEvent(
        snapshot_id=42,
        snapshot_time=now,
        changes_made="inserted_into_table:orders",
        next_snapshot_id=43,
        schema_version=3,
        schema_changes_pending=False,
    )
    stats = ConsumerStats(
        consumer_name="orders_sink",
        consumer_id=1,
        last_committed_snapshot=42,
        current_snapshot=43,
        lag_snapshots=1,
        lag_seconds=12.4,
        oldest_available_snapshot=5,
        gap_distance=37,
        subscription_count=2,
        subscriptions_active=2,
        subscriptions_renamed=0,
        subscriptions_dropped=0,
        owner_token=UUID("8f3a3c2e-0000-4000-8000-00000000b2c1"),
        lease_interval_seconds=30,
        lease_alive=True,
    )
    audit = AuditEntry(ts=now, audit_id=1, actor="ekku", action="create")
    doctor = DoctorDiagnostic.model_validate(
        {
            "severity": "warning",
            "code": "CDC_CONSUMER_LAG",
            "consumer_name": "orders_sink",
            "message": "Consumer is behind",
            "details": '{"lag_snapshots":1}',
        }
    )

    assert ddl.event_kind == "created"
    assert event.next_snapshot_id == 43
    assert stats.owner_token == UUID("8f3a3c2e-0000-4000-8000-00000000b2c1")
    assert audit.action == "create"
    assert doctor.severity == "warning"
