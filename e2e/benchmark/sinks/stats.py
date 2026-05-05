"""Aggregates benchmark metrics from each DML batch (observability-only)."""

from __future__ import annotations

import time

from ducklake_cdc_client import DMLBatch, SinkContext

from analytics import DemoStats


class StatsSink:
    """Sink that drives :class:`DemoStats` off batches + per-change.

    ``required=False`` so stats failures never gate delivery — the benchmark
    is observability, not part of the delivery contract.
    """

    name = "demo_stats"
    required = False

    def __init__(self, stats: DemoStats) -> None:
        self._stats = stats

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        del ctx
        consumed_ns = time.monotonic_ns()
        consumed_epoch_ns = time.time_ns()
        self._stats.record_consumer(batch.consumer_name)
        self._stats.record_window(has_changes=bool(batch), observed_ns=consumed_ns)
        self._stats.record_wait(has_snapshot=bool(batch))

        per_table: dict[str | None, int] = {}
        for change in batch:
            per_table[change.table] = per_table.get(change.table, 0) + 1
        self._stats.record_tables(len(per_table))
        for table_name, count in per_table.items():
            self._stats.record_changes(count, table_name=table_name)

        for change in batch:
            self._stats.record_change_observation(
                change_type=change.kind,
                table_name=change.table,
                values=change.values,
            )
            self._stats.record_change_latency(
                change_type=change.kind,
                produced_ns=change.values.get("produced_ns"),
                produced_epoch_ns=change.values.get("produced_epoch_ns"),
                snapshot_time=change.snapshot_time,
                consumed_ns=consumed_ns,
                consumed_epoch_ns=consumed_epoch_ns,
            )

        self._stats.record_commit()
