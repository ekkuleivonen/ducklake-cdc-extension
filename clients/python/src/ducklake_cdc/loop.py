"""Reusable consumer loop helpers for ducklake-cdc clients."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Protocol, TypeVar

from ducklake import DuckLake
from ducklake_cdc.client import CDCClient
from ducklake_cdc.models import ChangeRow, ConsumerCommit, ConsumerWindow, DdlEvent, SnapshotEvent

T = TypeVar("T")
TableNameResolver = Callable[[], Iterable[str]]


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
    wait_cdc: CDCClient,
    consumer_name: str,
    *,
    lake: DuckLake | None = None,
    table_names: Iterable[str] | TableNameResolver | None = None,
    timeout_ms: int = 1_000,
    max_snapshots: int = 100,
    max_windows: int = 0,
    idle_timeout: float = 5.0,
    idle_sleep_seconds: float = 0.05,
    retry: RetryPolicy | None = None,
    stats: ConsumerLoopStats | None = None,
) -> Iterator[ConsumerBatch]:
    """Yield committed CDC batches for a consumer.

    The loop keeps the demo/client state machine in one place while callers
    choose whether to print, buffer, or sink the returned batches.
    """

    table_resolver = _table_resolver(lake=lake, table_names=table_names)
    committed_windows = 0
    last_activity = time.monotonic()
    if stats is not None:
        stats.record_consumer(consumer_name)

    while True:
        wait = _timed(
            stats,
            "cdc_dml_ticks_listen",
            lambda: _run_with_retry(
                retry,
                lambda: wait_cdc.dml_ticks_wait(consumer_name, timeout_ms=timeout_ms),
            ),
        )
        if stats is not None:
            stats.record_wait(has_snapshot=wait.snapshot_id is not None)
        if wait.snapshot_id is None:
            if idle_timeout and time.monotonic() - last_activity >= idle_timeout:
                return
            continue
        last_activity = time.monotonic()

        window_operation = partial(
            cdc.window,
            consumer_name,
            max_snapshots=max_snapshots,
        )
        window = _timed_retry(stats, "cdc_window", retry, window_operation)
        if stats is not None:
            stats.record_window(has_changes=window.has_changes)
        if not window.has_changes:
            time.sleep(idle_sleep_seconds)
            continue

        processing_started = time.perf_counter()
        resolved_table_names = list(
            _timed(
                stats,
                "lake_tables",
                lambda: _run_with_retry(retry, table_resolver),
            )
        )
        if stats is not None:
            stats.record_tables(len(resolved_table_names))

        table_changes: list[TableChangeBatch] = []
        for table_name in resolved_table_names:
            changes_operation = partial(
                cdc.dml_table_changes_read_rows,
                consumer_name,
                table_name=table_name,
                max_snapshots=max_snapshots,
                start_snapshot=window.start_snapshot,
                end_snapshot=window.end_snapshot,
            )
            changes = _timed_retry(stats, "cdc_dml_table_changes_read", retry, changes_operation)
            consumed_ns = time.monotonic_ns()
            consumed_epoch_ns = time.time_ns()
            if stats is not None:
                stats.record_changes(len(changes), table_name=table_name)
                for change in changes:
                    stats.record_change_observation(
                        change_type=change.change_type,
                        table_name=table_name,
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
            table_changes.append(TableChangeBatch(table_name=table_name, changes=changes))

        processing_ms = (time.perf_counter() - processing_started) * 1000.0
        if stats is not None:
            stats.record_operation(
                "cdc_window_processing",
                processing_ms,
            )
        end_snapshot = window.end_snapshot
        commit_operation = partial(cdc.commit, consumer_name, end_snapshot)
        commit = _timed_retry(stats, "cdc_commit", retry, commit_operation)
        if stats is not None:
            stats.record_commit()

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


def _table_resolver(
    *,
    lake: DuckLake | None,
    table_names: Iterable[str] | TableNameResolver | None,
) -> TableNameResolver:
    if callable(table_names):
        return table_names
    if table_names is not None:
        names = tuple(table_names)
        return lambda: names
    if lake is None:
        raise ValueError("pass either lake or table_names")

    def resolve_from_lake() -> Iterable[str]:
        return (f"{table.schema_name}.{table.name}" for table in lake.tables())

    return resolve_from_lake


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
