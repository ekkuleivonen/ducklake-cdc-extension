from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demo"))

from analytics import DemoStats, metric_summary, percentile, summary_table  # noqa: E402


def test_percentile_interpolates_values() -> None:
    values = [1.0, 2.0, 3.0, 4.0]

    assert percentile(values, 50) == 2.5
    assert percentile(values, 95) == pytest.approx(3.85)
    assert percentile(values, 99) == pytest.approx(3.97)


def test_metric_summary_handles_empty_values() -> None:
    summary = metric_summary([])

    assert summary.to_json() == {
        "count": 0,
        "mean": 0.0,
        "p50": 0.0,
        "p95": 0.0,
        "p99": 0.0,
        "max": 0.0,
    }


def test_demo_stats_summary_is_json_ready() -> None:
    stats = DemoStats(started_ns=0)
    stats.record_operation("cdc_window", 3.0)
    stats.record_operation("cdc_window", 7.0)
    stats.record_wait(has_snapshot=True)
    stats.record_wait(has_snapshot=False)
    stats.record_window(has_changes=True)
    stats.record_window(has_changes=False)
    stats.record_ddl(1)
    stats.record_events(2)
    stats.record_tables(2)
    stats.record_changes(2, table_name="main.orders")
    stats.record_commit()
    stats.record_latency(1_000_000_000, consumed_ns=1_250_000_000)
    stats.record_latency(None, consumed_ns=1_250_000_000)
    stats.finished_ns = 2_000_000_000

    summary = stats.summary()

    assert summary["actual_duration_seconds"] == 2.0
    assert summary["consumed_changes"] == 2
    assert summary["consumed_changes_per_second"] == 1.0
    assert summary["catalog_queries_estimated"] == 9
    assert summary["catalog_qps_avg"] == 4.5
    assert summary["empty_window_ratio"] == 0.5
    assert summary["cdc_wait_calls"] == 2
    assert summary["cdc_wait_timeouts"] == 1
    assert summary["cdc_ddl_calls"] == 1
    assert summary["cdc_events_calls"] == 1
    assert summary["lake_tables_calls"] == 1
    assert summary["cdc_changes_calls"] == 1
    assert summary["cdc_commit_calls"] == 1
    assert summary["ddl_events"] == 1
    assert summary["snapshot_events"] == 2
    assert summary["table_count_per_window"]["p95"] == 2.0
    assert summary["rows_per_table_call"]["p95"] == 2.0
    assert summary["changes_by_table"] == {"main.orders": 2}
    assert summary["latency_observations"] == 1
    assert summary["latency_missing_produced_ns"] == 1
    assert summary["end_to_end_latency_ms"] == {
        "count": 1,
        "mean": 250.0,
        "p50": 250.0,
        "p95": 250.0,
        "p99": 250.0,
        "max": 250.0,
    }
    assert summary["operation_ms"] == {
        "cdc_window": {
            "count": 2,
            "mean": 5.0,
            "p50": 5.0,
            "p95": pytest.approx(6.8),
            "p99": pytest.approx(6.96),
            "max": 7.0,
        }
    }


def test_summary_table_renders_key_metrics() -> None:
    stats = DemoStats(started_ns=0, finished_ns=2_000_000_000)
    stats.record_changes(4)
    stats.record_operation("cdc_wait", 10.0)
    stats.record_latency(1_000_000_000, consumed_ns=1_100_000_000)

    table = summary_table({"type": "summary", **stats.summary()})

    assert "DuckLake CDC demo summary" in table
    assert "| changes" in table
    assert "| latency_ms_p95" in table
    assert "| cdc_wait_ms_p95" in table
