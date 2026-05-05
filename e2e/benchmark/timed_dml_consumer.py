"""Demo DML consumer wrapper that records pipeline timing by stage."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from ducklake_cdc_client import DMLBatch, DMLConsumer

from analytics import DemoStats


def _elapsed_ms_since(start_ns: int) -> float:
    return (time.monotonic_ns() - start_ns) / 1_000_000.0


class TimedDMLConsumer(DMLConsumer):
    """Demo-only DML consumer that records pipeline timing by stage."""

    def __init__(
        self,
        *args: Any,
        stats: DemoStats,
        fixed_max_snapshots: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._demo_stats = stats
        self._fixed_max_snapshots = fixed_max_snapshots

    def _listen_op(self, timeout_ms: int, max_snapshots: int) -> Callable[[], list[Any]]:
        effective_max_snapshots = self._fixed_max_snapshots or max_snapshots
        operation = super()._listen_op(timeout_ms, effective_max_snapshots)

        def timed_operation() -> list[Any]:
            start_ns = time.monotonic_ns()
            rows = operation()
            self._demo_stats.record_dml_listen(
                elapsed_ms=_elapsed_ms_since(start_ns),
                row_count=len(rows),
                max_snapshots=effective_max_snapshots,
            )
            return rows

        return timed_operation

    def _build_batch(self, rows: list[Any]) -> DMLBatch:
        start_ns = time.monotonic_ns()
        batch = super()._build_batch(rows)
        self._demo_stats.record_dml_build_batch(
            elapsed_ms=_elapsed_ms_since(start_ns),
            snapshot_span=max(1, batch.end_snapshot - batch.start_snapshot + 1),
        )
        return batch

    def _deliver(self, batch: Any) -> None:
        start_ns = time.monotonic_ns()
        try:
            super()._deliver(batch)
        finally:
            self._demo_stats.record_dml_sink(elapsed_ms=_elapsed_ms_since(start_ns))

    def _commit_op(self, snapshot: int) -> Callable[[], object]:
        operation = super()._commit_op(snapshot)

        def timed_operation() -> object:
            start_ns = time.monotonic_ns()
            try:
                return operation()
            finally:
                self._demo_stats.record_dml_commit_duration(
                    elapsed_ms=_elapsed_ms_since(start_ns)
                )

        return timed_operation
