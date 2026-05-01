"""Lightweight analytics helpers for the DuckLake CDC demo."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeVar

T = TypeVar("T")
NANOS_PER_MILLISECOND = 1_000_000.0


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
    started_ns: int = field(default_factory=time.monotonic_ns)
    finished_ns: int | None = None
    end_to_end_latencies_ms: list[float] = field(default_factory=list)
    operation_ms: dict[str, list[float]] = field(default_factory=dict)
    cdc_wait_calls: int = 0
    cdc_wait_timeouts: int = 0
    cdc_window_calls: int = 0
    cdc_window_non_empty_calls: int = 0
    cdc_window_empty_calls: int = 0
    cdc_changes_calls: int = 0
    cdc_commit_calls: int = 0
    ddl_events: int = 0
    snapshot_events: int = 0
    consumed_changes: int = 0
    latency_observations: int = 0
    latency_missing_produced_ns: int = 0

    def finish(self) -> None:
        if self.finished_ns is None:
            self.finished_ns = time.monotonic_ns()

    def record_operation(self, name: str, elapsed_ms: float) -> None:
        self.operation_ms.setdefault(name, []).append(elapsed_ms)

    def record_wait(self, *, has_snapshot: bool) -> None:
        self.cdc_wait_calls += 1
        if not has_snapshot:
            self.cdc_wait_timeouts += 1

    def record_window(self, *, has_changes: bool) -> None:
        self.cdc_window_calls += 1
        if has_changes:
            self.cdc_window_non_empty_calls += 1
        else:
            self.cdc_window_empty_calls += 1

    def record_changes(self, count: int) -> None:
        self.cdc_changes_calls += 1
        self.consumed_changes += count

    def record_commit(self) -> None:
        self.cdc_commit_calls += 1

    def record_latency(self, produced_ns: object, *, consumed_ns: int | None = None) -> None:
        parsed_produced_ns = parse_int(produced_ns)
        if parsed_produced_ns is None:
            self.latency_missing_produced_ns += 1
            return
        observed_ns = time.monotonic_ns() if consumed_ns is None else consumed_ns
        self.end_to_end_latencies_ms.append(
            (observed_ns - parsed_produced_ns) / NANOS_PER_MILLISECOND
        )
        self.latency_observations += 1

    def summary(self) -> dict[str, Any]:
        end_ns = self.finished_ns if self.finished_ns is not None else time.monotonic_ns()
        actual_duration_seconds = max((end_ns - self.started_ns) / 1_000_000_000.0, 0.0)
        catalog_queries_estimated = (
            self.cdc_window_calls + self.cdc_changes_calls + self.cdc_commit_calls
        )
        return {
            "actual_duration_seconds": actual_duration_seconds,
            "consumed_changes": self.consumed_changes,
            "consumed_changes_per_second": divide(self.consumed_changes, actual_duration_seconds),
            "end_to_end_latency_ms": metric_summary(self.end_to_end_latencies_ms).to_json(),
            "operation_ms": {
                name: metric_summary(values).to_json()
                for name, values in sorted(self.operation_ms.items())
            },
            "catalog_queries_estimated": catalog_queries_estimated,
            "catalog_qps_avg": divide(catalog_queries_estimated, actual_duration_seconds),
            "cdc_wait_calls": self.cdc_wait_calls,
            "cdc_wait_timeouts": self.cdc_wait_timeouts,
            "cdc_window_calls": self.cdc_window_calls,
            "cdc_window_non_empty_calls": self.cdc_window_non_empty_calls,
            "cdc_window_empty_calls": self.cdc_window_empty_calls,
            "empty_window_ratio": divide(self.cdc_window_empty_calls, self.cdc_window_calls),
            "cdc_changes_calls": self.cdc_changes_calls,
            "cdc_commit_calls": self.cdc_commit_calls,
            "ddl_events": self.ddl_events,
            "snapshot_events": self.snapshot_events,
            "latency_observations": self.latency_observations,
            "latency_missing_produced_ns": self.latency_missing_produced_ns,
        }


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


def timed_call(stats: DemoStats | None, operation_name: str, operation: Callable[[], T]) -> T:
    started = time.perf_counter()
    try:
        return operation()
    finally:
        if stats is not None:
            stats.record_operation(operation_name, (time.perf_counter() - started) * 1000.0)


def summary_table(summary: Mapping[str, Any]) -> str:
    rows = [
        ("duration_s", format_float(summary["actual_duration_seconds"])),
        ("changes", str(summary["consumed_changes"])),
        ("changes_per_s", format_float(summary["consumed_changes_per_second"])),
        ("ddl_events", str(summary["ddl_events"])),
        ("snapshot_events", str(summary["snapshot_events"])),
        ("catalog_queries", str(summary["catalog_queries_estimated"])),
        ("catalog_qps", format_float(summary["catalog_qps_avg"])),
        ("cdc_wait_calls", str(summary["cdc_wait_calls"])),
        ("cdc_wait_timeouts", str(summary["cdc_wait_timeouts"])),
        ("empty_window_ratio", format_float(summary["empty_window_ratio"])),
        ("latency_observations", str(summary["latency_observations"])),
        ("latency_missing_produced_ns", str(summary["latency_missing_produced_ns"])),
    ]
    latency = summary["end_to_end_latency_ms"]
    rows.extend(
        [
            ("latency_ms_p50", format_float(latency["p50"])),
            ("latency_ms_p95", format_float(latency["p95"])),
            ("latency_ms_p99", format_float(latency["p99"])),
            ("latency_ms_max", format_float(latency["max"])),
        ]
    )

    operation_ms = summary["operation_ms"]
    if isinstance(operation_ms, Mapping):
        for name, metrics in operation_ms.items():
            if isinstance(metrics, Mapping):
                rows.append((f"{name}_ms_p95", format_float(metrics["p95"])))

    return render_table("DuckLake CDC demo summary", rows)


def render_table(title: str, rows: list[tuple[str, str]]) -> str:
    metric_width = max(len("metric"), *(len(metric) for metric, _ in rows))
    value_width = max(len("value"), *(len(value) for _, value in rows))
    border = f"+-{'-' * metric_width}-+-{'-' * value_width}-+"
    lines = [
        title,
        border,
        f"| {'metric'.ljust(metric_width)} | {'value'.rjust(value_width)} |",
        border,
    ]
    lines.extend(
        f"| {metric.ljust(metric_width)} | {value.rjust(value_width)} |"
        for metric, value in rows
    )
    lines.append(border)
    return "\n".join(lines)


def format_float(value: object) -> str:
    if isinstance(value, int | float):
        return f"{value:.3f}"
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


def divide(numerator: int, denominator: float | int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator
