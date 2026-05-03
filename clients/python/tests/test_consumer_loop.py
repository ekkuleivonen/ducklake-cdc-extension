from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, cast

from ducklake_cdc import CDCClient, iter_consumer_batches
from ducklake_cdc.enums import ChangeType
from ducklake_cdc.models import ChangeRow, ConsumerCommit


class FakeCDC:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.now = datetime(2026, 5, 1, tzinfo=UTC)

    def dml_changes_listen_rows(
        self,
        name: str,
        *,
        timeout_ms: int,
        max_snapshots: int,
    ) -> list[ChangeRow]:
        self.calls.append(f"dml_changes_listen:{name}:{timeout_ms}:{max_snapshots}")
        return [
            ChangeRow(
                consumer_name=name,
                start_snapshot=1,
                end_snapshot=42,
                snapshot_id=42,
                schema_id=1,
                schema_name="main",
                table_id=10,
                table_name="main.orders",
                rowid=1,
                change_type=ChangeType.INSERT,
                snapshot_time=self.now,
                values={"id": 1, "produced_ns": 1, "produced_epoch_ns": 1},
            )
        ]

    def commit(self, name: str, snapshot: int) -> ConsumerCommit:
        self.calls.append(f"commit:{name}:{snapshot}")
        return ConsumerCommit(consumer_name=name, committed_snapshot=snapshot, schema_version=1)


class LoopStats:
    def __init__(self) -> None:
        self.operations: list[str] = []
        self.waits: list[bool] = []
        self.windows: list[bool] = []
        self.ddl_counts: list[int] = []
        self.event_counts: list[int] = []
        self.table_counts: list[int] = []
        self.change_counts: list[tuple[str | None, int]] = []
        self.commits = 0
        self.consumers: list[str] = []
        self.observations: list[tuple[object, str | None, object | None]] = []
        self.latencies: list[tuple[object, object, object]] = []

    def record_operation(self, name: str, elapsed_ms: float) -> None:
        assert elapsed_ms >= 0
        self.operations.append(name)

    def record_consumer(self, consumer_name: str) -> None:
        self.consumers.append(consumer_name)

    def record_wait(self, *, has_snapshot: bool) -> None:
        self.waits.append(has_snapshot)

    def record_window(self, *, has_changes: bool) -> None:
        self.windows.append(has_changes)

    def record_ddl(self, count: int) -> None:
        self.ddl_counts.append(count)

    def record_events(self, count: int) -> None:
        self.event_counts.append(count)

    def record_tables(self, count: int) -> None:
        self.table_counts.append(count)

    def record_changes(self, count: int, *, table_name: str | None = None) -> None:
        self.change_counts.append((table_name, count))

    def record_commit(self) -> None:
        self.commits += 1

    def record_change_observation(
        self,
        *,
        change_type: object,
        table_name: str | None = None,
        values: object | None = None,
    ) -> None:
        self.observations.append((change_type, table_name, values))

    def record_latency(self, produced_ns: object, *, consumed_ns: int | None = None) -> None:
        assert consumed_ns is not None
        self.latencies.append((produced_ns, None, None))

    def record_change_latency(
        self,
        *,
        change_type: object,
        produced_ns: object,
        produced_epoch_ns: object,
        snapshot_time: object,
        consumed_ns: int | None = None,
        consumed_epoch_ns: int | None = None,
    ) -> None:
        assert consumed_ns is not None
        assert consumed_epoch_ns is not None
        self.latencies.append((produced_ns, produced_epoch_ns, snapshot_time))


def test_iter_consumer_batches_yields_committed_batch() -> None:
    cdc = FakeCDC()
    stats = LoopStats()

    batches = list(
        iter_consumer_batches(
            cast(CDCClient, cdc),
            "orders_sink",
            timeout_ms=25,
            max_snapshots=10,
            max_windows=1,
            stats=stats,
        )
    )

    assert len(batches) == 1
    assert batches[0].window.end_snapshot == 42
    assert batches[0].commit.committed_snapshot == 42
    assert batches[0].ddl_events == []
    assert batches[0].snapshot_events == []
    assert batches[0].change_count == 1
    assert [table.table_name for table in batches[0].table_changes] == ["main.orders"]
    assert cdc.calls == [
        "dml_changes_listen:orders_sink:25:10",
        "commit:orders_sink:42",
    ]
    assert stats.waits == [True]
    assert stats.windows == [True]
    assert stats.ddl_counts == []
    assert stats.event_counts == []
    assert stats.table_counts == [1]
    assert stats.change_counts == [("main.orders", 1)]
    assert stats.commits == 1
    assert stats.consumers == ["orders_sink"]
    assert stats.observations == [
        (ChangeType.INSERT, "main.orders", {"id": 1, "produced_ns": 1, "produced_epoch_ns": 1})
    ]
    assert stats.latencies == [(1, 1, cdc.now)]
    assert "cdc_window_processing" in stats.operations


def test_iter_consumer_batches_applies_retry_policy() -> None:
    cdc = FakeCDC()
    retry_calls = 0

    def retry(operation: Callable[[], Any]) -> Any:
        nonlocal retry_calls
        retry_calls += 1
        return operation()

    list(
        iter_consumer_batches(
            cast(CDCClient, cdc),
            "orders_sink",
            max_windows=1,
            retry=retry,
        )
    )

    assert retry_calls == 2


