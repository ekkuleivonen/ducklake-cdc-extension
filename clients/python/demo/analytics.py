"""Lightweight analytics helpers for the DuckLake CDC demo.

Only the surface the knob-free demo actually drives lives here. Anything
that used to count catalog calls, loop iterations, or DDL events from the
old demo's hand-rolled fan-out is gone — :class:`ducklake_cdc.CDCApp`
owns the loop now and the demo's job is to render observable throughput +
latency on top of it.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

NANOS_PER_MILLISECOND = 1_000_000.0
CLOCK_SKEW_CLAMP_MS = 5.0


@dataclass(frozen=True)
class MetricSummary:
    count: int
    mean: float
    p50: float
    p95: float
    p99: float
    max: float

    def to_json(self) -> dict[str, float | int]:
        return {
            "count": self.count,
            "mean": self.mean,
            "p50": self.p50,
            "p95": self.p95,
            "p99": self.p99,
            "max": self.max,
        }


@dataclass
class DemoStats:
    """Aggregator the demo's optional stats sink writes into.

    Field groups mirror the rendered :func:`summary_table`:

    - throughput: ``consumed_changes`` / ``finished_ns`` - ``started_ns``;
    - latency: ``fresh_action_latencies_ms``,
      ``producer_to_snapshot_ms``, ``snapshot_to_consumer_ms``;
    - shape: per-table counts, per-change-type counts, producer
      benchmark payload echoed back from the row data.
    """

    started_ns: int = field(default_factory=time.monotonic_ns)
    finished_ns: int | None = None
    fresh_action_latencies_ms: list[float] = field(default_factory=list)
    stale_row_latencies_ms: list[float] = field(default_factory=list)
    producer_to_snapshot_ms: list[float] = field(default_factory=list)
    snapshot_to_consumer_ms: list[float] = field(default_factory=list)
    delivered_batches: int = 0
    consumed_changes: int = 0
    rows_per_batch: list[int] = field(default_factory=list)
    changes_by_table: dict[str, int] = field(default_factory=dict)
    consumer_names: set[str] = field(default_factory=set)
    schema_names: set[str] = field(default_factory=set)
    table_names: set[str] = field(default_factory=set)
    change_type_counts: dict[str, int] = field(default_factory=dict)
    benchmark_profiles: set[str] = field(default_factory=set)
    benchmark_duration_seconds: list[float] = field(default_factory=list)
    benchmark_schema_counts: set[int] = field(default_factory=set)
    benchmark_table_counts: set[int] = field(default_factory=set)
    benchmark_worker_counts: set[int] = field(default_factory=set)
    benchmark_update_percents: set[float] = field(default_factory=set)
    benchmark_delete_percents: set[float] = field(default_factory=set)
    error_count: int = 0
    error_type_counts: dict[str, int] = field(default_factory=dict)
    latency_missing_produced_ns: int = 0
    latency_clock_skew_clamped: int = 0

    def finish(self) -> None:
        if self.finished_ns is None:
            self.finished_ns = time.monotonic_ns()

    def record_consumer(self, consumer_name: str) -> None:
        self.consumer_names.add(consumer_name)

    def record_error(self, error: BaseException | str) -> None:
        self.error_count += 1
        error_type = error if isinstance(error, str) else type(error).__name__
        self.error_type_counts[error_type] = self.error_type_counts.get(error_type, 0) + 1

    def record_window(self, *, has_changes: bool) -> None:
        if has_changes:
            self.delivered_batches += 1

    def record_wait(self, *, has_snapshot: bool) -> None:
        # Kept as a no-op hook so the optional stats sink contract stays
        # symmetric with future "saw a window but it was empty" plumbing.
        del has_snapshot

    def record_tables(self, count: int) -> None:
        del count  # tracked implicitly via record_changes(table_name=…)

    def record_changes(self, count: int, *, table_name: str | None = None) -> None:
        self.consumed_changes += count
        self.rows_per_batch.append(count)
        if table_name is not None:
            self.changes_by_table[table_name] = (
                self.changes_by_table.get(table_name, 0) + count
            )
            self.table_names.add(table_name)
            schema_name = schema_from_table_name(table_name)
            if schema_name is not None:
                self.schema_names.add(schema_name)

    def record_commit(self) -> None:
        # Mirrors the consumer's internal commit; the surface is kept so a
        # future stats sink can record commit timing without changing the
        # call sites.
        return None

    def record_change_observation(
        self,
        *,
        change_type: object,
        table_name: str | None = None,
        values: Mapping[str, object] | None = None,
    ) -> None:
        change_type_name = str(change_type)
        self.change_type_counts[change_type_name] = (
            self.change_type_counts.get(change_type_name, 0) + 1
        )
        if table_name is not None:
            self.table_names.add(table_name)
            schema_name = schema_from_table_name(table_name)
            if schema_name is not None:
                self.schema_names.add(schema_name)
        if values is not None:
            self._record_benchmark_payload(values)

    def record_change_latency(
        self,
        *,
        change_type: object,
        produced_ns: object,
        produced_epoch_ns: object,
        snapshot_time: datetime | None,
        consumed_ns: int | None = None,
        consumed_epoch_ns: int | None = None,
    ) -> None:
        parsed_produced_ns = parse_int(produced_ns)
        if parsed_produced_ns is None:
            self.latency_missing_produced_ns += 1
            return

        observed_ns = time.monotonic_ns() if consumed_ns is None else consumed_ns
        latency_ms = (observed_ns - parsed_produced_ns) / NANOS_PER_MILLISECOND

        change_type_name = str(change_type)
        if change_type_name in {"insert", "update_postimage"}:
            self.fresh_action_latencies_ms.append(latency_ms)
            parsed_epoch_ns = parse_int(produced_epoch_ns)
            snapshot_epoch_ns = datetime_to_epoch_ns(snapshot_time)
            if parsed_epoch_ns is not None and snapshot_epoch_ns is not None:
                producer_to_snapshot_ms = (
                    snapshot_epoch_ns - parsed_epoch_ns
                ) / NANOS_PER_MILLISECOND
                if -CLOCK_SKEW_CLAMP_MS < producer_to_snapshot_ms < 0:
                    producer_to_snapshot_ms = 0.0
                    self.latency_clock_skew_clamped += 1
                if producer_to_snapshot_ms >= 0:
                    self.producer_to_snapshot_ms.append(producer_to_snapshot_ms)
                observed_epoch_ns = (
                    time.time_ns() if consumed_epoch_ns is None else consumed_epoch_ns
                )
                self.snapshot_to_consumer_ms.append(
                    (observed_epoch_ns - snapshot_epoch_ns) / NANOS_PER_MILLISECOND
                )
        elif change_type_name in {"update_preimage", "delete"}:
            self.stale_row_latencies_ms.append(latency_ms)

    def summary(self) -> dict[str, Any]:
        end_ns = self.finished_ns if self.finished_ns is not None else time.monotonic_ns()
        actual_duration_seconds = max((end_ns - self.started_ns) / 1_000_000_000.0, 0.0)
        return {
            "actual_duration_seconds": actual_duration_seconds,
            "consumed_changes": self.consumed_changes,
            "consumed_changes_per_second": divide(
                self.consumed_changes, actual_duration_seconds
            ),
            "delivered_batches": self.delivered_batches,
            "fresh_action_latency_ms": metric_summary(
                self.fresh_action_latencies_ms
            ).to_json(),
            "stale_row_latency_ms": metric_summary(self.stale_row_latencies_ms).to_json(),
            "producer_to_snapshot_ms": metric_summary(
                self.producer_to_snapshot_ms
            ).to_json(),
            "snapshot_to_consumer_ms": metric_summary(
                self.snapshot_to_consumer_ms
            ).to_json(),
            "rows_per_batch": metric_summary(
                [float(count) for count in self.rows_per_batch]
            ).to_json(),
            "changes_by_table": dict(sorted(self.changes_by_table.items())),
            "latency_missing_produced_ns": self.latency_missing_produced_ns,
            "latency_clock_skew_clamped": self.latency_clock_skew_clamped,
            "consumer_count_seen": len(self.consumer_names),
            "schema_count_seen": len(self.schema_names),
            "table_count_seen": len(self.table_names),
            "dml_count_seen": self._dml_action_count_seen(),
            "dml_change_type_counts": dict(sorted(self.change_type_counts.items())),
            "dml_action_shares": self._dml_action_shares(),
            "producer_workload": self._producer_workload_summary(),
            "error_count": self.error_count,
            "error_type_counts": dict(sorted(self.error_type_counts.items())),
            "fresh_latency_excluded_row_count": (
                self.latency_missing_produced_ns + len(self.stale_row_latencies_ms)
            ),
            "stale_latency_row_count": len(self.stale_row_latencies_ms),
        }

    def _dml_action_count_seen(self) -> int:
        return (
            self.change_type_counts.get("insert", 0)
            + self.change_type_counts.get("update_postimage", 0)
            + self.change_type_counts.get("delete", 0)
        )

    def _dml_action_shares(self) -> dict[str, float]:
        total = self._dml_action_count_seen()
        return {
            "insert": divide(self.change_type_counts.get("insert", 0), total),
            "update": divide(self.change_type_counts.get("update_postimage", 0), total),
            "delete": divide(self.change_type_counts.get("delete", 0), total),
        }

    def _producer_workload_summary(self) -> dict[str, object]:
        return {
            "profiles": sorted(self.benchmark_profiles),
            "duration_seconds": scalar_or_sorted(self.benchmark_duration_seconds),
            "schema_counts": sorted(self.benchmark_schema_counts),
            "table_counts": sorted(self.benchmark_table_counts),
            "worker_counts": sorted(self.benchmark_worker_counts),
            "update_percents": sorted(self.benchmark_update_percents),
            "delete_percents": sorted(self.benchmark_delete_percents),
        }

    def _record_benchmark_payload(self, values: Mapping[str, object]) -> None:
        profile = values.get("benchmark_profile")
        if profile is not None:
            self.benchmark_profiles.add(str(profile))
        duration = parse_float(values.get("benchmark_duration_s"))
        if duration is not None:
            self.benchmark_duration_seconds.append(duration)
        schema_count = parse_int(values.get("benchmark_schemas"))
        if schema_count is not None:
            self.benchmark_schema_counts.add(schema_count)
        table_count = parse_int(values.get("benchmark_tables"))
        if table_count is not None:
            self.benchmark_table_counts.add(table_count)
        worker_count = parse_int(values.get("benchmark_workers"))
        if worker_count is not None:
            self.benchmark_worker_counts.add(worker_count)
        update_percent = parse_float(values.get("benchmark_update_percent"))
        if update_percent is not None:
            self.benchmark_update_percents.add(update_percent)
        delete_percent = parse_float(values.get("benchmark_delete_percent"))
        if delete_percent is not None:
            self.benchmark_delete_percents.add(delete_percent)


def metric_summary(values: list[float]) -> MetricSummary:
    if not values:
        return MetricSummary(count=0, mean=0.0, p50=0.0, p95=0.0, p99=0.0, max=0.0)
    return MetricSummary(
        count=len(values),
        mean=sum(values) / len(values),
        p50=percentile(values, 50),
        p95=percentile(values, 95),
        p99=percentile(values, 99),
        max=max(values),
    )


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    rank = (percentile_value / 100.0) * (len(sorted_values) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(sorted_values) - 1)
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    return (sorted_values[lower] * (1.0 - weight)) + (sorted_values[upper] * weight)


def summary_table(summary: Mapping[str, Any]) -> str:
    """Render a focused, sectioned summary of the metrics that actually matter.

    The full numeric record stays in :meth:`DemoStats.summary` (and the
    JSON dump). This rendering is the at-a-glance view: throughput, fresh
    CDC latency, where that latency is spent, and a small workload-shape
    footer.
    """

    sections = _summary_sections(summary)
    return _render_sections("DuckLake CDC demo summary", sections)


def _summary_sections(summary: Mapping[str, Any]) -> list[list[tuple[str, str, str]]]:
    sections: list[list[tuple[str, str, str]]] = []

    sections.append(
        [
            (
                "duration_s",
                format_float(summary["actual_duration_seconds"]),
                "wall time of the consumer run",
            ),
            (
                "changes",
                str(summary["consumed_changes"]),
                "rows the consumer delivered to the sink",
            ),
            (
                "changes_per_s",
                format_float(summary["consumed_changes_per_second"]),
                "throughput across the run",
            ),
            (
                "batches",
                str(summary.get("delivered_batches", 0)),
                "non-empty windows committed across all consumers",
            ),
        ]
    )

    workload = summary.get("producer_workload")
    if isinstance(workload, Mapping):
        workload_rows: list[tuple[str, str, str]] = []
        profiles = workload.get("profiles")
        if profiles:
            workload_rows.append(
                (
                    "profile",
                    format_list(profiles),
                    "producer schedule used for commit spacing",
                )
            )
        duration = workload.get("duration_seconds")
        if duration not in (None, [], ""):
            workload_rows.append(
                (
                    "producer_duration_s",
                    format_workload_value(duration),
                    "target producer spread; 0 means as fast as possible",
                )
            )
        worker_counts = workload.get("worker_counts")
        if worker_counts:
            workload_rows.append(
                (
                    "producer_workers",
                    format_workload_value(worker_counts),
                    "concurrent producer connections",
                )
            )
        if workload_rows:
            sections.append(workload_rows)

    fresh = summary.get("fresh_action_latency_ms")
    if _has_observations(fresh):
        sections.append(
            [
                (
                    "latency_fresh_ms_p50",
                    format_float(fresh["p50"]),
                    "end-to-end CDC latency for insert + update_postimage",
                ),
                (
                    "latency_fresh_ms_p95",
                    format_float(fresh["p95"]),
                    "  (only events whose produced_ns reflects emission time)",
                ),
                ("latency_fresh_ms_p99", format_float(fresh["p99"]), "  tail"),
                (
                    "latency_fresh_ms_max",
                    format_float(fresh["max"]),
                    "  worst case observed",
                ),
            ]
        )

    breakdown: list[tuple[str, str, str]] = []
    producer_to_snapshot = summary.get("producer_to_snapshot_ms")
    if _has_observations(producer_to_snapshot):
        breakdown.append(
            (
                "producer_to_snapshot_ms_p95",
                format_float(producer_to_snapshot["p95"]),
                "producer transaction commit time (typically near zero)",
            )
        )
    snapshot_to_consumer = summary.get("snapshot_to_consumer_ms")
    if _has_observations(snapshot_to_consumer):
        breakdown.append(
            (
                "snapshot_to_consumer_ms_p95",
                format_float(snapshot_to_consumer["p95"]),
                "consumer pipeline cost (what this extension owns)",
            )
        )
    if breakdown:
        sections.append(breakdown)

    sections.append(
        [
            (
                "count_errors",
                str(summary.get("error_count", 0)),
                "uncaught consumer/runtime errors observed before exit",
            ),
            (
                "count_missing_produced_ns",
                str(summary.get("latency_missing_produced_ns", 0)),
                "rows delivered but excluded from latency due to missing timestamp",
            ),
            (
                "count_clock_skew_clamped",
                str(summary.get("latency_clock_skew_clamped", 0)),
                "near-zero negative producer->snapshot samples clamped to zero",
            ),
            (
                "count_stale_latency_rows",
                str(summary.get("stale_latency_row_count", 0)),
                "preimage/delete rows excluded from fresh latency",
            ),
            (
                "count_fresh_latency_excluded",
                str(summary.get("fresh_latency_excluded_row_count", 0)),
                "total rows omitted from fresh latency calculations",
            ),
        ]
    )

    shares = summary.get("dml_action_shares")
    workload_shape: list[tuple[str, str, str]] = [
        (
            "count_consumers",
            str(summary.get("consumer_count_seen", 0)),
            "distinct consumers represented in this summary",
        ),
        (
            "count_schemas",
            str(summary.get("schema_count_seen", 0)),
            "distinct schemas observed in consumed rows",
        ),
        (
            "count_tables",
            str(summary.get("table_count_seen", 0)),
            "distinct tables observed in consumed rows",
        ),
        (
            "count_dml",
            str(summary.get("dml_count_seen", 0)),
            "logical DML actions: insert + update_postimage + delete",
        ),
    ]
    if isinstance(shares, Mapping):
        workload_shape.extend(
            [
                (
                    "share_inserts",
                    format_share(shares.get("insert", 0.0)),
                    "insert share of logical DML actions",
                ),
                (
                    "share_updates",
                    format_share(shares.get("update", 0.0)),
                    "update_postimage share of logical DML actions",
                ),
                (
                    "share_deletes",
                    format_share(shares.get("delete", 0.0)),
                    "delete share of logical DML actions",
                ),
            ]
        )
    sections.append(workload_shape)

    rows_per_batch = summary.get("rows_per_batch")
    if isinstance(rows_per_batch, Mapping) and int(rows_per_batch.get("count", 0)) > 0:
        sections.append(
            [
                (
                    "rows_per_batch_p95",
                    format_float(rows_per_batch["p95"]),
                    "typical batch size; large -> consider cdc_dml_changes_read",
                ),
            ]
        )

    return sections


def _has_observations(metrics: object) -> bool:
    return isinstance(metrics, Mapping) and int(metrics.get("count", 0) or 0) > 0


def _render_sections(
    title: str, sections: list[list[tuple[str, str, str]]]
) -> str:
    all_rows = [row for section in sections for row in section]
    if not all_rows:
        return title

    metric_width = max(len("metric"), *(len(metric) for metric, _, _ in all_rows))
    value_width = max(len("value"), *(len(value) for _, value, _ in all_rows))
    desc_width = max(
        len("description"), *(len(description) for _, _, description in all_rows)
    )
    border = (
        f"+-{'-' * metric_width}-+-{'-' * value_width}-+-{'-' * desc_width}-+"
    )
    lines = [
        title,
        border,
        f"| {'metric'.ljust(metric_width)} | {'value'.rjust(value_width)} | "
        f"{'description'.ljust(desc_width)} |",
        border,
    ]
    for index, section in enumerate(sections):
        if index > 0:
            lines.append(border)
        for metric, value, description in section:
            lines.append(
                f"| {metric.ljust(metric_width)} | "
                f"{value.rjust(value_width)} | "
                f"{description.ljust(desc_width)} |"
            )
    lines.append(border)
    return "\n".join(lines)


def format_float(value: object) -> str:
    if isinstance(value, int | float):
        return f"{value:.3f}"
    return str(value)


def format_share(value: object) -> str:
    if isinstance(value, int | float):
        return f"{value * 100.0:.1f}%"
    return str(value)


def format_list(value: object) -> str:
    if isinstance(value, list | tuple | set):
        return ",".join(str(item) for item in value)
    return str(value)


def format_workload_value(value: object) -> str:
    if isinstance(value, int | float):
        return format_float(value)
    if isinstance(value, list):
        if len(value) == 1:
            return format_float(value[0])
        return format_list(value)
    return str(value)


def parse_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def parse_float(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def scalar_or_sorted(values: list[float]) -> float | list[float]:
    unique_values = sorted(set(values))
    if len(unique_values) == 1:
        return unique_values[0]
    return unique_values


def schema_from_table_name(table_name: str) -> str | None:
    parts = table_name.split(".")
    if len(parts) < 2:
        return None
    return parts[-2].strip('"')


def datetime_to_epoch_ns(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.timestamp() * 1_000_000_000)


def divide(numerator: int, denominator: float | int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator
