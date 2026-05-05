"""Lightweight analytics helpers for the DuckLake CDC benchmark.

The benchmark answers four questions, in order:

1. **Workload** — what was produced (tables, workers, batch sizes,
   update/delete mix) and what reached the consumer (consumers,
   tables_seen).
2. **Throughput** — once data started flowing, how many rows landed
   per second.
3. **End-to-end latency** — from the producer emitting a row to the
   consumer's sink seeing it (`fresh` events: insert + update_postimage).
4. **Where that latency came from** — split by who introduced it:
   the producer (commit + publish), the CDC extension (listen + commit),
   and the Python client (Change/DMLBatch materialisation, sink).

The JSON written via ``--summary-output`` mirrors the rendered table
1-to-1: each section is a top-level object with the same field names as
the rows on screen.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
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

    - throughput: ``consumed_changes`` / ``finished_ns`` - ``first_delivered_ns``;
    - end-to-end + stage: ``e2e_ms``, ``producer_ms``, ``pipeline_ms``;
    - per-batch breakdown: ``extension_listen_ms``, ``python_build_ms``,
      ``sink_ms``, ``extension_commit_ms``, ``snapshots_per_batch``,
      ``snapshots_per_listen``;
    - workload: per-table counts, per-change-type counts, producer
      benchmark payload echoed back from the row data.
    """

    started_ns: int = field(default_factory=time.monotonic_ns)
    first_delivered_ns: int | None = None
    finished_ns: int | None = None
    e2e_ms: list[float] = field(default_factory=list)
    stale_row_latencies_ms: list[float] = field(default_factory=list)
    producer_ms: list[float] = field(default_factory=list)
    pipeline_ms: list[float] = field(default_factory=list)
    extension_listen_ms: list[float] = field(default_factory=list)
    extension_listen_empty_ms: list[float] = field(default_factory=list)
    python_build_ms: list[float] = field(default_factory=list)
    sink_ms: list[float] = field(default_factory=list)
    extension_commit_ms: list[float] = field(default_factory=list)
    snapshots_per_batch: list[int] = field(default_factory=list)
    snapshots_per_listen: list[int] = field(default_factory=list)
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
    benchmark_batch_mins: set[int] = field(default_factory=set)
    benchmark_batch_maxes: set[int] = field(default_factory=set)
    error_count: int = 0
    error_type_counts: dict[str, int] = field(default_factory=dict)
    rows_no_produced_ns: int = 0
    rows_clock_skew_clamped: int = 0

    def finish(self) -> None:
        if self.finished_ns is None:
            self.finished_ns = time.monotonic_ns()

    def record_consumer(self, consumer_name: str) -> None:
        self.consumer_names.add(consumer_name)

    def record_error(self, error: BaseException | str) -> None:
        self.error_count += 1
        error_type = error if isinstance(error, str) else type(error).__name__
        self.error_type_counts[error_type] = self.error_type_counts.get(error_type, 0) + 1

    def record_window(self, *, has_changes: bool, observed_ns: int | None = None) -> None:
        if has_changes:
            if self.first_delivered_ns is None:
                self.first_delivered_ns = (
                    time.monotonic_ns() if observed_ns is None else observed_ns
                )
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

    def record_dml_listen(
        self, *, elapsed_ms: float, row_count: int, max_snapshots: int
    ) -> None:
        self.snapshots_per_listen.append(max_snapshots)
        if row_count > 0:
            self.extension_listen_ms.append(elapsed_ms)
        else:
            self.extension_listen_empty_ms.append(elapsed_ms)

    def record_dml_build_batch(self, *, elapsed_ms: float, snapshot_span: int) -> None:
        self.python_build_ms.append(elapsed_ms)
        self.snapshots_per_batch.append(snapshot_span)

    def record_dml_sink(self, *, elapsed_ms: float) -> None:
        self.sink_ms.append(elapsed_ms)

    def record_dml_commit_duration(self, *, elapsed_ms: float) -> None:
        self.extension_commit_ms.append(elapsed_ms)

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
            self.rows_no_produced_ns += 1
            return

        observed_ns = time.monotonic_ns() if consumed_ns is None else consumed_ns
        latency_ms = (observed_ns - parsed_produced_ns) / NANOS_PER_MILLISECOND

        change_type_name = str(change_type)
        if change_type_name in {"insert", "update_postimage"}:
            self.e2e_ms.append(latency_ms)
            parsed_epoch_ns = parse_int(produced_epoch_ns)
            snapshot_epoch_ns = datetime_to_epoch_ns(snapshot_time)
            if parsed_epoch_ns is not None and snapshot_epoch_ns is not None:
                producer_ms = (snapshot_epoch_ns - parsed_epoch_ns) / NANOS_PER_MILLISECOND
                if -CLOCK_SKEW_CLAMP_MS < producer_ms < 0:
                    producer_ms = 0.0
                    self.rows_clock_skew_clamped += 1
                if producer_ms >= 0:
                    self.producer_ms.append(producer_ms)
                observed_epoch_ns = (
                    time.time_ns() if consumed_epoch_ns is None else consumed_epoch_ns
                )
                self.pipeline_ms.append(
                    (observed_epoch_ns - snapshot_epoch_ns) / NANOS_PER_MILLISECOND
                )
        elif change_type_name in {"update_preimage", "delete"}:
            self.stale_row_latencies_ms.append(latency_ms)

    # -- summary ---------------------------------------------------------

    def summary(self) -> dict[str, Any]:
        end_ns = self.finished_ns if self.finished_ns is not None else time.monotonic_ns()
        run_duration_seconds = max((end_ns - self.started_ns) / 1_000_000_000.0, 0.0)
        active_duration_seconds = (
            max((end_ns - self.first_delivered_ns) / 1_000_000_000.0, 0.0)
            if self.first_delivered_ns is not None
            else 0.0
        )
        rows_per_batch = metric_summary([float(c) for c in self.rows_per_batch])
        e2e = metric_summary(self.e2e_ms)
        producer = metric_summary(self.producer_ms)
        pipeline = metric_summary(self.pipeline_ms)
        listen = metric_summary(self.extension_listen_ms)
        build = metric_summary(self.python_build_ms)
        sink = metric_summary(self.sink_ms)
        commit = metric_summary(self.extension_commit_ms)
        snap_per_batch = metric_summary([float(s) for s in self.snapshots_per_batch])
        snap_per_listen = metric_summary([float(s) for s in self.snapshots_per_listen])

        return {
            "workload": self._workload_section(),
            "throughput": {
                "duration_active_s": active_duration_seconds,
                "duration_run_s": run_duration_seconds,
                "changes_total": self.consumed_changes,
                "changes_per_s": divide(
                    self.consumed_changes, active_duration_seconds
                ),
                "batches_total": self.delivered_batches,
                "rows_per_batch_p95": rows_per_batch.p95,
            },
            "e2e_latency_ms": {
                "p50": e2e.p50,
                "p95": e2e.p95,
                "p99": e2e.p99,
                "max": e2e.max,
            },
            "stage_latency_ms": {
                "producer_p95": producer.p95,
                "pipeline_p95": pipeline.p95,
            },
            "pipeline_breakdown": {
                "extension_listen_ms_p95": listen.p95,
                "python_build_ms_p95": build.p95,
                "snapshots_per_batch_p95": snap_per_batch.p95,
                "snapshots_per_batch_max": snap_per_batch.max,
                "snapshots_per_listen_p50": snap_per_listen.p50,
            },
            "post_delivery_ms": {
                "sink_p95": sink.p95,
                "extension_commit_p95": commit.p95,
            },
            "action_mix": self._action_mix_section(),
            "health": {
                "errors": self.error_count,
                "rows_no_produced_ns": self.rows_no_produced_ns,
                "rows_clock_skew_clamped": self.rows_clock_skew_clamped,
                "rows_excluded_from_e2e": (
                    self.rows_no_produced_ns + len(self.stale_row_latencies_ms)
                ),
            },
        }

    def progress_snapshot(self) -> dict[str, int | float]:
        end_ns = self.finished_ns if self.finished_ns is not None else time.monotonic_ns()
        active_duration_seconds = (
            max((end_ns - self.first_delivered_ns) / 1_000_000_000.0, 0.0)
            if self.first_delivered_ns is not None
            else 0.0
        )
        return {
            "duration_active_s": active_duration_seconds,
            "changes_total": self.consumed_changes,
            "changes_per_s": divide(self.consumed_changes, active_duration_seconds),
            "batches_total": self.delivered_batches,
            "consumers": len(self.consumer_names),
            "tables_seen": len(self.table_names),
            "errors": self.error_count,
        }

    def stage_breakdown_p95(self) -> dict[str, float]:
        """Live attribution view: who introduced each ms.

        Used by the dashboard to render a producer/extension/client
        breakdown without touching the lower-level metric lists.
        ``extension`` = listen + commit (both extension SQL).
        ``client`` = python_build + sink (Python, but ``sink`` is the
        user-supplied callback so we expose it separately too).
        """
        producer_p95 = percentile(self.producer_ms, 95)
        listen_p95 = percentile(self.extension_listen_ms, 95)
        build_p95 = percentile(self.python_build_ms, 95)
        sink_p95 = percentile(self.sink_ms, 95)
        commit_p95 = percentile(self.extension_commit_ms, 95)
        return {
            "producer_p95": producer_p95,
            "extension_listen_p95": listen_p95,
            "extension_commit_p95": commit_p95,
            "python_build_p95": build_p95,
            "sink_p95": sink_p95,
            # Convenience aggregates.
            "extension_p95": listen_p95 + commit_p95,
            "client_p95": build_p95 + sink_p95,
        }

    # -- internals -------------------------------------------------------

    def _workload_section(self) -> dict[str, Any]:
        return {
            "producer_profile": scalar_or_sorted_str(self.benchmark_profiles),
            "producer_workers": scalar_or_sorted_int(self.benchmark_worker_counts),
            "producer_duration_s": scalar_or_sorted_float(self.benchmark_duration_seconds),
            "producer_schemas": scalar_or_sorted_int(self.benchmark_schema_counts),
            "producer_tables": scalar_or_sorted_int(self.benchmark_table_counts),
            "producer_update_pct": scalar_or_sorted_float(self.benchmark_update_percents),
            "producer_delete_pct": scalar_or_sorted_float(self.benchmark_delete_percents),
            "producer_batch_min": scalar_or_sorted_int(self.benchmark_batch_mins),
            "producer_batch_max": scalar_or_sorted_int(self.benchmark_batch_maxes),
            "consumers": len(self.consumer_names),
            "tables_seen": len(self.table_names),
        }

    def _action_mix_section(self) -> dict[str, float]:
        total = self._dml_action_count_seen()
        return {
            "inserts": divide(self.change_type_counts.get("insert", 0), total),
            "updates": divide(self.change_type_counts.get("update_postimage", 0), total),
            "deletes": divide(self.change_type_counts.get("delete", 0), total),
        }

    def _dml_action_count_seen(self) -> int:
        return (
            self.change_type_counts.get("insert", 0)
            + self.change_type_counts.get("update_postimage", 0)
            + self.change_type_counts.get("delete", 0)
        )

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
        batch_min = parse_int(values.get("benchmark_batch_min"))
        if batch_min is not None:
            self.benchmark_batch_mins.add(batch_min)
        batch_max = parse_int(values.get("benchmark_batch_max"))
        if batch_max is not None:
            self.benchmark_batch_maxes.add(batch_max)


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------


def summary_table(summary: Mapping[str, Any]) -> str:
    """Render the benchmark summary as a sectioned ASCII table.

    The input is the dict returned by :meth:`DemoStats.summary` (the
    same one written to ``--summary-output``). Each top-level key
    becomes a titled section; missing sections are silently skipped so
    runs without delivered batches still produce a useful header.
    """
    sections = list(_iter_sections(summary))
    return _render_sections("DuckLake CDC benchmark summary", sections)


_SECTION_TITLE = {
    "workload": "Workload",
    "throughput": "Throughput",
    "e2e_latency_ms": "End-to-end latency (fresh: insert + update_postimage)",
    "stage_latency_ms": "Latency by stage  (p95, sums to ~e2e_p95)",
    "pipeline_breakdown": "Pipeline breakdown  (per delivered batch)",
    "post_delivery_ms": "Post-delivery overhead  (per batch, not in e2e budget)",
    "action_mix": "Action mix",
    "health": "Health",
}


def _iter_sections(
    summary: Mapping[str, Any],
) -> Iterable[tuple[str, list[tuple[str, str, str]]]]:
    workload = summary.get("workload")
    if isinstance(workload, Mapping):
        rows = _workload_rows(workload)
        if rows:
            yield _SECTION_TITLE["workload"], rows

    throughput = summary.get("throughput")
    if isinstance(throughput, Mapping):
        rows = _throughput_rows(throughput)
        if rows:
            yield _SECTION_TITLE["throughput"], rows

    e2e = summary.get("e2e_latency_ms")
    if _has_latency(e2e):
        yield _SECTION_TITLE["e2e_latency_ms"], _e2e_rows(e2e)

    stage = summary.get("stage_latency_ms")
    if _has_latency(stage):
        yield _SECTION_TITLE["stage_latency_ms"], _stage_rows(stage)

    pipeline = summary.get("pipeline_breakdown")
    if isinstance(pipeline, Mapping):
        rows = _pipeline_rows(pipeline)
        if rows:
            yield _SECTION_TITLE["pipeline_breakdown"], rows

    post = summary.get("post_delivery_ms")
    if isinstance(post, Mapping):
        rows = _post_delivery_rows(post)
        if rows:
            yield _SECTION_TITLE["post_delivery_ms"], rows

    mix = summary.get("action_mix")
    if isinstance(mix, Mapping):
        yield _SECTION_TITLE["action_mix"], _action_mix_rows(mix)

    health = summary.get("health")
    if isinstance(health, Mapping):
        yield _SECTION_TITLE["health"], _health_rows(health)


def _workload_rows(section: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    rows.append(_row("producer_profile", _fmt_value(section.get("producer_profile")), "commit-spacing schedule"))
    rows.append(_row("producer_workers", _fmt_value(section.get("producer_workers")), "concurrent producer connections"))
    rows.append(_row("producer_duration_s", _fmt_value(section.get("producer_duration_s")), "target spread; 0 = as fast as possible"))
    rows.append(_row("producer_schemas", _fmt_value(section.get("producer_schemas")), "configured (vs tables_seen)"))
    rows.append(_row("producer_tables", _fmt_value(section.get("producer_tables")), "configured per schema"))
    rows.append(_row("producer_update_pct", _fmt_pct(section.get("producer_update_pct")), "of base actions"))
    rows.append(_row("producer_delete_pct", _fmt_pct(section.get("producer_delete_pct")), "of base actions"))
    rows.append(_row("producer_batch_min", _fmt_value(section.get("producer_batch_min")), "rows per commit, lower bound"))
    rows.append(_row("producer_batch_max", _fmt_value(section.get("producer_batch_max")), "rows per commit, upper bound"))
    rows.append(_row("consumers", _fmt_value(section.get("consumers")), "distinct consumers observed"))
    rows.append(_row("tables_seen", _fmt_value(section.get("tables_seen")), "distinct tables observed"))
    return [r for r in rows if r is not None]


def _throughput_rows(section: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    return [
        _row("duration_active_s", format_float(section.get("duration_active_s", 0.0)), "first delivered batch -> exit"),
        _row("duration_run_s", format_float(section.get("duration_run_s", 0.0)), "wall time of consumer process"),
        _row("changes_total", _fmt_int(section.get("changes_total", 0)), "rows delivered to sinks"),
        _row("changes_per_s", format_float(section.get("changes_per_s", 0.0)), "changes_total / duration_active_s"),
        _row("batches_total", _fmt_int(section.get("batches_total", 0)), "non-empty windows across all consumers"),
        _row("rows_per_batch_p95", format_float(section.get("rows_per_batch_p95", 0.0)), "typical batch size"),
    ]


def _e2e_rows(section: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    return [
        _row("e2e_p50_ms", format_float(section.get("p50", 0.0)), "produced -> delivered to sink"),
        _row("e2e_p95_ms", format_float(section.get("p95", 0.0)), ""),
        _row("e2e_p99_ms", format_float(section.get("p99", 0.0)), ""),
        _row("e2e_max_ms", format_float(section.get("max", 0.0)), ""),
    ]


def _stage_rows(section: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    return [
        _row("producer_ms_p95", format_float(section.get("producer_p95", 0.0)), "produced -> snapshot landed (commit + publish) *"),
        _row("pipeline_ms_p95", format_float(section.get("pipeline_p95", 0.0)), "snapshot landed -> delivered to sink"),
    ]


def _pipeline_rows(section: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    return [
        _row("extension_listen_ms_p95", format_float(section.get("extension_listen_ms_p95", 0.0)), "cdc_dml_changes_listen (incl. catalog wait)"),
        _row("python_build_ms_p95", format_float(section.get("python_build_ms_p95", 0.0)), "Change/DMLBatch materialisation"),
        _row("snapshots_per_batch_p95", format_float(section.get("snapshots_per_batch_p95", 0.0)), "snapshots coalesced per delivered batch"),
        _row("snapshots_per_batch_max", format_float(section.get("snapshots_per_batch_max", 0.0)), "worst-case coalescing window"),
        _row("snapshots_per_listen_p50", format_float(section.get("snapshots_per_listen_p50", 0.0)), "snapshots requested per listen call"),
    ]


def _post_delivery_rows(section: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    return [
        _row("sink_ms_p95", format_float(section.get("sink_p95", 0.0)), "user sink callbacks"),
        _row("extension_commit_ms_p95", format_float(section.get("extension_commit_p95", 0.0)), "cdc_commit after sink success"),
    ]


def _action_mix_rows(section: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    return [
        _row("share_inserts", format_share(section.get("inserts", 0.0)), ""),
        _row("share_updates", format_share(section.get("updates", 0.0)), ""),
        _row("share_deletes", format_share(section.get("deletes", 0.0)), ""),
    ]


def _health_rows(section: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    return [
        _row("errors", _fmt_int(section.get("errors", 0)), "uncaught consumer/runtime errors"),
        _row("rows_no_produced_ns", _fmt_int(section.get("rows_no_produced_ns", 0)), "excluded from e2e: missing producer timestamp"),
        _row("rows_clock_skew_clamped", _fmt_int(section.get("rows_clock_skew_clamped", 0)), "small negative samples clamped to zero"),
        _row("rows_excluded_from_e2e", _fmt_int(section.get("rows_excluded_from_e2e", 0)), "preimage + delete rows"),
    ]


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _row(metric: str, value: str, description: str) -> tuple[str, str, str]:
    return (metric, value, description)


def _has_latency(section: object) -> bool:
    if not isinstance(section, Mapping):
        return False
    # All zeros means no fresh latencies were recorded — skip the
    # section entirely instead of rendering rows of 0.000.
    return any(float(v or 0.0) for v in section.values())


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


def _render_sections(
    title: str, sections: list[tuple[str, list[tuple[str, str, str]]]]
) -> str:
    """Render `(heading, rows)` pairs into a sectioned ASCII table.

    Each section gets a heading line above its block; sections are
    separated by horizontal rules. Column widths span every section so
    the rules align across the whole table.
    """
    all_rows = [row for _, section in sections for row in section]
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
    header = (
        f"| {'metric'.ljust(metric_width)} "
        f"| {'value'.rjust(value_width)} "
        f"| {'description'.ljust(desc_width)} |"
    )
    lines = [title, ""]
    for index, (heading, rows) in enumerate(sections):
        if index == 0:
            lines.append(f"  {heading}")
            lines.append(border)
            lines.append(header)
            lines.append(border)
        else:
            lines.append(border)
            lines.append("")
            lines.append(f"  {heading}")
            lines.append(border)
        for metric, value, description in rows:
            lines.append(
                f"| {metric.ljust(metric_width)} "
                f"| {value.rjust(value_width)} "
                f"| {description.ljust(desc_width)} |"
            )
    lines.append(border)
    lines.append("")
    lines.append(
        "  * producer_ms_p95 is sampled at batch start, before the producer's"
    )
    lines.append(
        "    INSERT/UPDATE executes, so it includes batch-SQL time. With small"
    )
    lines.append(
        "    batches it approaches pure commit + publish cost."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def format_float(value: object) -> str:
    if isinstance(value, int | float):
        return f"{value:.3f}"
    return str(value)


def format_share(value: object) -> str:
    if isinstance(value, int | float):
        return f"{value * 100.0:.1f}%"
    return str(value)


def _fmt_pct(value: object) -> str:
    """Format a [0..100] percent value as ``25.0%``.

    Distinct from :func:`format_share`, which expects a [0..1] share.
    """
    if value is None or value == "" or value == [] or value == ():
        return "-"
    if isinstance(value, list):
        if len(value) == 1:
            return _fmt_pct(value[0])
        return ", ".join(_fmt_pct(v) for v in value)
    if isinstance(value, int | float):
        return f"{value:.1f}%"
    return str(value)


def _fmt_int(value: object) -> str:
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{int(value):,}"
    return str(value)


def _fmt_value(value: object) -> str:
    """Render a workload value: scalar, list of scalars, or string."""
    if value is None or value == "" or value == [] or value == ():
        return "-"
    if isinstance(value, list | tuple | set):
        items = list(value)
        if len(items) == 1:
            return _fmt_value(items[0])
        return ", ".join(_fmt_value(v) for v in items)
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
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


def scalar_or_sorted_int(values: set[int] | list[int]) -> int | list[int] | None:
    items = sorted(set(values))
    if not items:
        return None
    if len(items) == 1:
        return items[0]
    return items


def scalar_or_sorted_float(values: set[float] | list[float]) -> float | list[float] | None:
    items = sorted(set(values))
    if not items:
        return None
    if len(items) == 1:
        return items[0]
    return items


def scalar_or_sorted_str(values: set[str] | list[str]) -> str | list[str] | None:
    items = sorted(set(values))
    if not items:
        return None
    if len(items) == 1:
        return items[0]
    return items


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
