"""Reusable consumer loop helpers for ducklake-cdc clients."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Protocol, TypeVar

from ducklake_cdc.client import CDCClient
from ducklake_cdc.models import ChangeRow, ConsumerCommit, ConsumerWindow, DdlEvent, SnapshotEvent

T = TypeVar("T")


class RetryPolicy(Protocol):
    def __call__(self, operation: Callable[[], T]) -> T: ...


class ConsumerLoopStats(Protocol):
    """Metrics sink used by the reusable consumer loop."""

    def record_operation(self, name: str, elapsed_ms: float) -> None: ...

    def record_consumer(self, consumer_name: str) -> None: ...

    def record_wait(self, *, has_snapshot: bool) -> None: ...

    def record_window(self, *, has_changes: bool) -> None: ...

    def record_ddl(self, count: int) -> None: ...

    def record_events(self, count: int) -> None: ...

    def record_tables(self, count: int) -> None: ...

    def record_changes(self, count: int, *, table_name: str | None = None) -> None: ...

    def record_commit(self) -> None: ...

    def record_change_observation(
        self,
        *,
        change_type: object,
        table_name: str | None = None,
        values: Mapping[str, object] | None = None,
    ) -> None: ...

    def record_latency(self, produced_ns: object, *, consumed_ns: int | None = None) -> None: ...

    def record_change_latency(
        self,
        *,
        change_type: object,
        produced_ns: object,
        produced_epoch_ns: object,
        snapshot_time: object,
        consumed_ns: int | None = None,
        consumed_epoch_ns: int | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class TableChangeBatch:
    """Changes read for one DuckLake table within a consumer window."""

    table_name: str
    changes: list[ChangeRow]


@dataclass(frozen=True)
class ConsumerBatch:
    """A committed CDC window with all rows fetched by the loop."""

    window: ConsumerWindow
    ddl_events: list[DdlEvent]
    snapshot_events: list[SnapshotEvent]
    table_changes: list[TableChangeBatch]
    commit: ConsumerCommit

    @property
    def change_count(self) -> int:
        return sum(len(table.changes) for table in self.table_changes)


def iter_consumer_batches(
    cdc: CDCClient,
    consumer_name: str,
    *,
    timeout_ms: int = 1_000,
    max_snapshots: int = 100,
    max_windows: int = 0,
    idle_timeout: float = 5.0,
    retry: RetryPolicy | None = None,
    stats: ConsumerLoopStats | None = None,
) -> Iterator[ConsumerBatch]:
    """Yield committed CDC batches for a consumer.

    The loop keeps the demo/client state machine in one place while callers
    choose whether to print, buffer, or sink the returned batches.
    """

    committed_windows = 0
    last_activity = time.monotonic()
    if stats is not None:
        stats.record_consumer(consumer_name)

    while True:
        changes_operation = partial(
            cdc.dml_changes_listen_rows,
            consumer_name,
            timeout_ms=timeout_ms,
            max_snapshots=max_snapshots,
        )
        changes = _timed_retry(stats, "cdc_dml_changes_listen", retry, changes_operation)
        consumed_ns = time.monotonic_ns()
        consumed_epoch_ns = time.time_ns()
        if stats is not None:
            stats.record_wait(has_snapshot=bool(changes))
            stats.record_window(has_changes=bool(changes))
        if not changes:
            if idle_timeout and time.monotonic() - last_activity >= idle_timeout:
                return
            continue
        last_activity = time.monotonic()

        processing_started = time.perf_counter()
        changes_by_table: dict[str, list[ChangeRow]] = {}
        for change in changes:
            table_name = change.table_name or "<unknown>"
            changes_by_table.setdefault(table_name, []).append(change)
        table_changes = [
            TableChangeBatch(table_name=table_name, changes=table_rows)
            for table_name, table_rows in changes_by_table.items()
        ]
        if stats is not None:
            stats.record_tables(len(table_changes))
        for table in table_changes:
            if stats is not None:
                stats.record_changes(len(table.changes), table_name=table.table_name)
                for change in table.changes:
                    stats.record_change_observation(
                        change_type=change.change_type,
                        table_name=table.table_name,
                        values=change.values,
                    )
                    stats.record_change_latency(
                        change_type=change.change_type,
                        produced_ns=change.values.get("produced_ns"),
                        produced_epoch_ns=change.values.get("produced_epoch_ns"),
                        snapshot_time=change.snapshot_time,
                        consumed_ns=consumed_ns,
                        consumed_epoch_ns=consumed_epoch_ns,
                    )

        processing_ms = (time.perf_counter() - processing_started) * 1000.0
        if stats is not None:
            stats.record_operation(
                "cdc_window_processing",
                processing_ms,
            )
        start_snapshot = min(
            change.start_snapshot if change.start_snapshot is not None else change.snapshot_id
            for change in changes
        )
        end_snapshot = max(
            change.end_snapshot if change.end_snapshot is not None else change.snapshot_id
            for change in changes
        )
        commit_operation = partial(cdc.commit, consumer_name, end_snapshot)
        commit = _timed_retry(stats, "cdc_commit", retry, commit_operation)
        if stats is not None:
            stats.record_commit()
        window = ConsumerWindow(
            start_snapshot=start_snapshot,
            end_snapshot=end_snapshot,
            has_changes=True,
            schema_version=commit.schema_version,
            schema_changes_pending=False,
        )

        yield ConsumerBatch(
            window=window,
            ddl_events=[],
            snapshot_events=[],
            table_changes=table_changes,
            commit=commit,
        )

        committed_windows += 1
        if max_windows and committed_windows >= max_windows:
            return


def _timed(
    stats: ConsumerLoopStats | None,
    operation_name: str,
    operation: Callable[[], T],
) -> T:
    started = time.perf_counter()
    try:
        return operation()
    finally:
        if stats is not None:
            stats.record_operation(operation_name, (time.perf_counter() - started) * 1000.0)


def _timed_retry(
    stats: ConsumerLoopStats | None,
    operation_name: str,
    retry: RetryPolicy | None,
    operation: Callable[[], T],
) -> T:
    return _timed(stats, operation_name, lambda: _run_with_retry(retry, operation))


def _run_with_retry(retry: RetryPolicy | None, operation: Callable[[], T]) -> T:
    if retry is None:
        return operation()
    return retry(operation)
