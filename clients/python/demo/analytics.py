"""Lightweight analytics helpers for the DuckLake CDC demo."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    fresh_action_latencies_ms: list[float] = field(default_factory=list)
    stale_row_latencies_ms: list[float] = field(default_factory=list)
    producer_to_snapshot_ms: list[float] = field(default_factory=list)
    snapshot_to_consumer_ms: list[float] = field(default_factory=list)
    latency_by_change_type_ms: dict[str, list[float]] = field(default_factory=dict)
    operation_ms: dict[str, list[float]] = field(default_factory=dict)
    cdc_dml_ticks_listen_calls: int = 0
    cdc_dml_ticks_listen_timeouts: int = 0
    cdc_window_calls: int = 0
    cdc_window_non_empty_calls: int = 0
    cdc_window_empty_calls: int = 0
    cdc_ddl_changes_read_calls: int = 0
    cdc_dml_ticks_read_calls: int = 0
    lake_tables_calls: int = 0
    cdc_dml_table_changes_read_calls: int = 0
    cdc_commit_calls: int = 0
    ddl_events: int = 0
    snapshot_events: int = 0
    consumed_changes: int = 0
    latency_observations: int = 0
    latency_missing_produced_ns: int = 0
    table_counts: list[int] = field(default_factory=list)
    rows_per_table: list[int] = field(default_factory=list)
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
    dropped_row_count: int = 0

    def finish(self) -> None:
        if self.finished_ns is None:
            self.finished_ns = time.monotonic_ns()

    def record_operation(self, name: str, elapsed_ms: float) -> None:
        self.operation_ms.setdefault(name, []).append(elapsed_ms)

    def record_consumer(self, consumer_name: str) -> None:
        self.consumer_names.add(consumer_name)

    def record_error(self, error: BaseException | str) -> None:
        self.error_count += 1
        error_type = error if isinstance(error, str) else type(error).__name__
        self.error_type_counts[error_type] = self.error_type_counts.get(error_type, 0) + 1

    def record_dropped_rows(self, count: int) -> None:
        self.dropped_row_count += count

    def record_wait(self, *, has_snapshot: bool) -> None:
        self.cdc_dml_ticks_listen_calls += 1
        if not has_snapshot:
            self.cdc_dml_ticks_listen_timeouts += 1

    def record_window(self, *, has_changes: bool) -> None:
        self.cdc_window_calls += 1
        if has_changes:
            self.cdc_window_non_empty_calls += 1
        else:
            self.cdc_window_empty_calls += 1

    def record_ddl(self, count: int) -> None:
        self.cdc_ddl_changes_read_calls += 1
        self.ddl_events += count

    def record_events(self, count: int) -> None:
        self.cdc_dml_ticks_read_calls += 1
        self.snapshot_events += count

    def record_tables(self, count: int) -> None:
        self.lake_tables_calls += 1
        self.table_counts.append(count)

    def record_changes(self, count: int, *, table_name: str | None = None) -> None:
        self.cdc_dml_table_changes_read_calls += 1
        self.consumed_changes += count
        self.rows_per_table.append(count)
        if table_name is not None:
            self.changes_by_table[table_name] = self.changes_by_table.get(table_name, 0) + count
            self.table_names.add(table_name)
            schema_name = schema_from_table_name(table_name)
            if schema_name is not None:
                self.schema_names.add(schema_name)

    def record_commit(self) -> None:
        self.cdc_commit_calls += 1

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
        self.end_to_end_latencies_ms.append(latency_ms)
        self.latency_observations += 1

        change_type_name = str(change_type)
        self.latency_by_change_type_ms.setdefault(change_type_name, []).append(latency_ms)
        if change_type_name in {"insert", "update_postimage"}:
            self.fresh_action_latencies_ms.append(latency_ms)
            parsed_epoch_ns = parse_int(produced_epoch_ns)
            snapshot_epoch_ns = datetime_to_epoch_ns(snapshot_time)
            if parsed_epoch_ns is not None and snapshot_epoch_ns is not None:
                self.producer_to_snapshot_ms.append(
                    (snapshot_epoch_ns - parsed_epoch_ns) / NANOS_PER_MILLISECOND
                )
                observed_epoch_ns = time.time_ns() if consumed_epoch_ns is None else consumed_epoch_ns
                self.snapshot_to_consumer_ms.append(
                    (observed_epoch_ns - snapshot_epoch_ns) / NANOS_PER_MILLISECOND
                )
        elif change_type_name in {"update_preimage", "delete"}:
            # These rows intentionally carry the previous row version's
            # produced timestamp, so they are not fresh action latency.
            self.stale_row_latencies_ms.append(latency_ms)

    def summary(self) -> dict[str, Any]:
        end_ns = self.finished_ns if self.finished_ns is not None else time.monotonic_ns()
        actual_duration_seconds = max((end_ns - self.started_ns) / 1_000_000_000.0, 0.0)
        catalog_queries_estimated = self.catalog_queries_estimated()
        return {
            "actual_duration_seconds": actual_duration_seconds,
            "consumed_changes": self.consumed_changes,
            "consumed_changes_per_second": divide(self.consumed_changes, actual_duration_seconds),
            "end_to_end_latency_ms": metric_summary(self.end_to_end_latencies_ms).to_json(),
            "fresh_action_latency_ms": metric_summary(self.fresh_action_latencies_ms).to_json(),
            "stale_row_latency_ms": metric_summary(self.stale_row_latencies_ms).to_json(),
            "producer_to_snapshot_ms": metric_summary(self.producer_to_snapshot_ms).to_json(),
            "snapshot_to_consumer_ms": metric_summary(self.snapshot_to_consumer_ms).to_json(),
            "latency_by_change_type_ms": {
                name: metric_summary(values).to_json()
                for name, values in sorted(self.latency_by_change_type_ms.items())
            },
            "operation_ms": {
                name: metric_summary(values).to_json()
                for name, values in sorted(self.operation_ms.items())
            },
            "catalog_queries_estimated": catalog_queries_estimated,
            "catalog_qps_avg": divide(catalog_queries_estimated, actual_duration_seconds),
            "cdc_dml_ticks_listen_calls": self.cdc_dml_ticks_listen_calls,
            "cdc_dml_ticks_listen_timeouts": self.cdc_dml_ticks_listen_timeouts,
            "cdc_window_calls": self.cdc_window_calls,
            "cdc_window_non_empty_calls": self.cdc_window_non_empty_calls,
            "cdc_window_empty_calls": self.cdc_window_empty_calls,
            "empty_window_ratio": divide(self.cdc_window_empty_calls, self.cdc_window_calls),
            "cdc_ddl_changes_read_calls": self.cdc_ddl_changes_read_calls,
            "cdc_dml_ticks_read_calls": self.cdc_dml_ticks_read_calls,
            "lake_tables_calls": self.lake_tables_calls,
            "cdc_dml_table_changes_read_calls": self.cdc_dml_table_changes_read_calls,
            "cdc_commit_calls": self.cdc_commit_calls,
            "ddl_events": self.ddl_events,
            "snapshot_events": self.snapshot_events,
            "table_count_per_window": metric_summary(
                [float(count) for count in self.table_counts]
            ).to_json(),
            "rows_per_table_call": metric_summary(
                [float(count) for count in self.rows_per_table]
            ).to_json(),
            "changes_by_table": dict(sorted(self.changes_by_table.items())),
            "latency_observations": self.latency_observations,
            "latency_missing_produced_ns": self.latency_missing_produced_ns,
            "consumer_count_seen": len(self.consumer_names),
            "schema_count_seen": len(self.schema_names),
            "table_count_seen": len(self.table_names),
            "ddl_count_seen": self.ddl_events,
            "dml_count_seen": self.dml_action_count_seen(),
            "dml_row_count_seen": self.consumed_changes,
            "dml_change_type_counts": dict(sorted(self.change_type_counts.items())),
            "dml_action_shares": self.dml_action_shares(),
            "producer_workload": self.producer_workload_summary(),
            "error_count": self.error_count,
            "error_type_counts": dict(sorted(self.error_type_counts.items())),
            "dropped_row_count": self.dropped_row_count,
            "fresh_latency_excluded_row_count": (
                self.latency_missing_produced_ns + len(self.stale_row_latencies_ms)
            ),
            "stale_latency_row_count": len(self.stale_row_latencies_ms),
        }

    def dml_action_count_seen(self) -> int:
        return (
            self.change_type_counts.get("insert", 0)
            + self.change_type_counts.get("update_postimage", 0)
            + self.change_type_counts.get("delete", 0)
        )

    def dml_action_shares(self) -> dict[str, float]:
        total = self.dml_action_count_seen()
        return {
            "insert": divide(self.change_type_counts.get("insert", 0), total),
            "update": divide(self.change_type_counts.get("update_postimage", 0), total),
            "delete": divide(self.change_type_counts.get("delete", 0), total),
        }

    def producer_workload_summary(self) -> dict[str, object]:
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

    def catalog_queries_estimated(self) -> int:
        return (
            self.cdc_dml_ticks_listen_calls
            + self.cdc_window_calls
            + self.cdc_ddl_changes_read_calls
            + self.cdc_dml_ticks_read_calls
            + self.lake_tables_calls
            + self.cdc_dml_table_changes_read_calls
            + self.cdc_commit_calls
        )


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
    """Render a focused, sectioned summary of the metrics that actually matter.

    The full numeric record stays in `stats.summary()` (and the JSON dump);
    this table is the at-a-glance view: throughput, fresh CDC latency,
    where that latency is spent, per-call extension cost, and a small
    workload-shape footer.
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

    operation_ms = summary.get("operation_ms")
    per_call: list[tuple[str, str, str]] = []
    if isinstance(operation_ms, Mapping):
        wait = operation_ms.get("cdc_dml_ticks_listen")
        if isinstance(wait, Mapping):
            per_call.append(
                (
                    "cdc_dml_ticks_listen_ms_p50",
                    format_float(wait["p50"]),
                    "typical inter-iteration wait (p95 inflated by idle timeouts)",
                )
            )
        for name, description in (
            ("cdc_window", "per-call cdc_window cost"),
            ("cdc_dml_table_changes_read", "per-call cdc_dml_table_changes_read cost (per table)"),
            ("cdc_commit", "per-call cdc_commit cost"),
        ):
            metrics = operation_ms.get(name)
            if isinstance(metrics, Mapping):
                per_call.append(
                    (f"{name}_ms_p95", format_float(metrics["p95"]), description)
                )
    if per_call:
        sections.append(per_call)

    sections.append(
        [
            (
                "count_errors",
                str(summary.get("error_count", 0)),
                "uncaught consumer/runtime errors observed before exit",
            ),
            (
                "count_dropped_rows",
                str(summary.get("dropped_row_count", 0)),
                "rows intentionally discarded by the sink or benchmark harness",
            ),
            (
                "count_missing_produced_ns",
                str(summary.get("latency_missing_produced_ns", 0)),
                "rows delivered but excluded from latency due to missing timestamp",
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
            "count_ddl",
            str(summary.get("ddl_count_seen", 0)),
            "DDL events observed",
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

    loop_shape: list[tuple[str, str, str]] = [
        (
            "iterations",
            str(summary["cdc_window_calls"]),
            "total consumer loop iterations",
        ),
        (
            "empty_window_ratio",
            format_float(summary["empty_window_ratio"]),
            "fraction of windows with no changes (>0 = wakeup spam)",
        ),
    ]
    rows_per_table = summary.get("rows_per_table_call")
    if isinstance(rows_per_table, Mapping):
        loop_shape.append(
            (
                "rows_per_changes_call_p95",
                format_float(rows_per_table["p95"]),
                "typical batch size; large -> consider cdc_dml_changes_read",
            )
        )
    sections.append(loop_shape)

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


def render_table(title: str, rows: list[tuple[str, str]]) -> str:
    """Render a simple two-column metric/value table.

    Kept as a small public utility for callers that want to format their
    own ad-hoc metric tables. The richer demo summary uses
    `_render_sections` directly.
    """
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
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1_000_000_000)


def divide(numerator: int, denominator: float | int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator
