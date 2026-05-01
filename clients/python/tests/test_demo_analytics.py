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
    stats.record_consumer("consumer_a")
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
    stats.record_error(ValueError("boom"))
    stats.record_dropped_rows(3)
    stats.record_change_observation(
        change_type="insert",
        table_name="main.orders",
        values={
            "benchmark_profile": "flat",
            "benchmark_duration_s": 0.0,
            "benchmark_schemas": 1,
            "benchmark_tables": 2,
            "benchmark_workers": 4,
            "benchmark_update_percent": 25.0,
            "benchmark_delete_percent": 10.0,
        },
    )
    stats.record_change_observation(
        change_type="update_postimage",
        table_name="main.orders",
        values={
            "benchmark_profile": "flat",
            "benchmark_duration_s": 0.0,
            "benchmark_schemas": 1,
            "benchmark_tables": 2,
            "benchmark_workers": 4,
            "benchmark_update_percent": 25.0,
            "benchmark_delete_percent": 10.0,
        },
    )
    stats.record_commit()
    stats.record_latency(1_000_000_000, consumed_ns=1_250_000_000)
    stats.record_latency(None, consumed_ns=1_250_000_000)
    stats.finished_ns = 2_000_000_000

    summary = stats.summary()

    assert summary["actual_duration_seconds"] == 2.0
    assert summary["consumed_changes"] == 2
    assert summary["consumed_changes_per_second"] == 1.0
    assert summary["catalog_queries_estimated"] == 5
    assert summary["catalog_qps_avg"] == 2.5
    assert summary["empty_window_ratio"] == 0.5
    assert summary["cdc_dml_changes_listen_calls"] == 2
    assert summary["cdc_dml_changes_listen_timeouts"] == 1
    assert summary["cdc_ddl_changes_read_calls"] == 1
    assert summary["cdc_dml_ticks_read_calls"] == 1
    assert summary["lake_tables_calls"] == 1
    assert summary["cdc_dml_table_changes_read_calls"] == 0
    assert summary["cdc_commit_calls"] == 1
    assert summary["ddl_events"] == 1
    assert summary["snapshot_events"] == 2
    assert summary["table_count_per_window"]["p95"] == 2.0
    assert summary["rows_per_table_call"]["p95"] == 2.0
    assert summary["changes_by_table"] == {"main.orders": 2}
    assert summary["latency_observations"] == 1
    assert summary["latency_missing_produced_ns"] == 1
    assert summary["consumer_count_seen"] == 1
    assert summary["schema_count_seen"] == 1
    assert summary["table_count_seen"] == 1
    assert summary["ddl_count_seen"] == 1
    assert summary["dml_count_seen"] == 2
    assert summary["dml_row_count_seen"] == 2
    assert summary["dml_change_type_counts"] == {"insert": 1, "update_postimage": 1}
    assert summary["dml_action_shares"] == {
        "insert": 0.5,
        "update": 0.5,
        "delete": 0.0,
    }
    assert summary["producer_workload"] == {
        "profiles": ["flat"],
        "duration_seconds": 0.0,
        "schema_counts": [1],
        "table_counts": [2],
        "worker_counts": [4],
        "update_percents": [25.0],
        "delete_percents": [10.0],
    }
    assert summary["error_count"] == 1
    assert summary["error_type_counts"] == {"ValueError": 1}
    assert summary["dropped_row_count"] == 3
    assert summary["fresh_latency_excluded_row_count"] == 1
    assert summary["stale_latency_row_count"] == 0
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
    stats.record_consumer("consumer_a")
    stats.record_changes(4)
    stats.record_window(has_changes=True)
    stats.record_operation("cdc_dml_changes_listen", 10.0)
    stats.record_operation("cdc_window", 5.0)
    stats.record_operation("cdc_commit", 2.0)
    stats.record_change_latency(
        change_type="insert",
        produced_ns=1_000_000_000,
        produced_epoch_ns=None,
        snapshot_time=None,
        consumed_ns=1_100_000_000,
    )
    stats.record_change_observation(
        change_type="insert",
        table_name="main.orders",
        values={
            "benchmark_profile": "flat",
            "benchmark_duration_s": 0.0,
            "benchmark_schemas": 1,
            "benchmark_tables": 1,
            "benchmark_workers": 4,
            "benchmark_update_percent": 0.0,
            "benchmark_delete_percent": 0.0,
        },
    )

    table = summary_table({"type": "summary", **stats.summary()})

    assert "DuckLake CDC demo summary" in table
    assert "| metric " in table
    assert "| description " in table
    assert "| duration_s " in table
    assert "| changes " in table
    assert "| changes_per_s " in table
    assert "| profile " in table
    assert "| producer_duration_s " in table
    assert "| producer_workers " in table
    assert "| latency_fresh_ms_p50 " in table
    assert "| latency_fresh_ms_p95 " in table
    assert "| cdc_dml_changes_listen_ms_p50 " in table
    assert "| cdc_window_ms_p95 " in table
    assert "| cdc_commit_ms_p95 " in table
    assert "| count_errors " in table
    assert "| count_dropped_rows " in table
    assert "| count_missing_produced_ns " in table
    assert "| count_stale_latency_rows " in table
    assert "| count_fresh_latency_excluded " in table
    assert "| count_consumers " in table
    assert "| count_schemas " in table
    assert "| count_tables " in table
    assert "| count_ddl " in table
    assert "| count_dml " in table
    assert "| share_inserts " in table
    assert "| share_updates " in table
    assert "| share_deletes " in table
    assert "| iterations " in table
    assert "| empty_window_ratio " in table
    assert "| rows_per_changes_call_p95 " in table

    assert "ddl_events" not in table
    assert "snapshot_events" not in table
    assert "catalog_queries" not in table
    assert "cdc_dml_ticks_listen_calls" not in table
    assert "lake_tables_calls" not in table
    assert "latency_observations" not in table
    assert "latency_all_ms_" not in table
    assert "latency_stale_rows_" not in table
    assert "tables_per_window_" not in table
