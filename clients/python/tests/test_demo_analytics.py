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
    stats.record_window(has_changes=True, observed_ns=1_000_000_000)
    stats.record_window(has_changes=True, observed_ns=1_500_000_000)
    stats.record_changes(2, table_name="main.orders")
    stats.record_error(ValueError("boom"))
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
    stats.record_dml_listen(elapsed_ms=10.0, row_count=2, max_snapshots=8)
    stats.record_dml_listen(elapsed_ms=1.0, row_count=0, max_snapshots=4)
    stats.record_dml_build_batch(elapsed_ms=2.0, snapshot_span=8)
    stats.record_dml_sink(elapsed_ms=3.0)
    stats.record_dml_commit_duration(elapsed_ms=4.0)
    stats.finished_ns = 3_000_000_000

    summary = stats.summary()

    assert summary["actual_duration_seconds"] == 3.0
    assert summary["active_duration_seconds"] == 2.0
    assert summary["run_duration_seconds"] == 3.0
    assert summary["consumed_changes"] == 2
    assert summary["consumed_changes_per_second"] == 1.0
    assert summary["delivered_batches"] == 2
    assert summary["rows_per_batch"]["p95"] == 2.0
    assert summary["changes_by_table"] == {"main.orders": 2}
    assert summary["consumer_count_seen"] == 1
    assert summary["schema_count_seen"] == 1
    assert summary["table_count_seen"] == 1
    assert summary["dml_count_seen"] == 2
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
    assert summary["latency_missing_produced_ns"] == 0
    assert summary["dml_listen_nonempty_ms"]["p95"] == 10.0
    assert summary["dml_listen_empty_ms"]["p95"] == 1.0
    assert summary["dml_build_batch_ms"]["p95"] == 2.0
    assert summary["dml_sink_ms"]["p95"] == 3.0
    assert summary["dml_commit_ms"]["p95"] == 4.0
    assert summary["dml_snapshot_span"]["p95"] == 8.0
    assert summary["dml_listen_max_snapshots"]["p50"] == 6.0
    assert summary["fresh_latency_excluded_row_count"] == 0
    assert summary["stale_latency_row_count"] == 0
    assert stats.progress_snapshot()["consumed_changes"] == 2
    assert stats.progress_snapshot()["consumer_count_seen"] == 1


def test_demo_stats_change_latency_records_fresh_and_breakdown() -> None:
    from datetime import UTC, datetime

    stats = DemoStats(started_ns=0)
    snapshot_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    stats.record_change_latency(
        change_type="insert",
        produced_ns=1_000_000_000,
        produced_epoch_ns=int(snapshot_time.timestamp() * 1_000_000_000) - 5_000_000,
        snapshot_time=snapshot_time,
        consumed_ns=1_500_000_000,
        consumed_epoch_ns=int(snapshot_time.timestamp() * 1_000_000_000) + 50_000_000,
    )
    stats.record_change_latency(
        change_type="update_preimage",
        produced_ns=1_000_000_000,
        produced_epoch_ns=None,
        snapshot_time=None,
        consumed_ns=1_500_000_000,
    )
    stats.record_change_latency(
        change_type="insert",
        produced_ns=None,
        produced_epoch_ns=None,
        snapshot_time=None,
    )

    summary = stats.summary()

    fresh = summary["fresh_action_latency_ms"]
    assert fresh["count"] == 1
    assert fresh["max"] == pytest.approx(500.0)
    assert summary["producer_to_snapshot_ms"]["count"] == 1
    assert summary["snapshot_to_consumer_ms"]["count"] == 1
    assert summary["stale_latency_row_count"] == 1
    assert summary["latency_missing_produced_ns"] == 1
    assert summary["fresh_latency_excluded_row_count"] == 2


def test_demo_stats_clamps_near_zero_negative_producer_to_snapshot() -> None:
    from datetime import UTC, datetime

    stats = DemoStats(started_ns=0)
    snapshot_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    snapshot_epoch_ns = int(snapshot_time.timestamp() * 1_000_000_000)
    # Producer epoch ns is 1ms AFTER the snapshot epoch — within the 5ms
    # clock-skew clamp window.
    stats.record_change_latency(
        change_type="insert",
        produced_ns=0,
        produced_epoch_ns=snapshot_epoch_ns + 1_000_000,
        snapshot_time=snapshot_time,
        consumed_ns=1_000_000,
        consumed_epoch_ns=snapshot_epoch_ns + 100_000_000,
    )

    summary = stats.summary()

    assert summary["latency_clock_skew_clamped"] == 1
    assert summary["producer_to_snapshot_ms"]["count"] == 1
    assert summary["producer_to_snapshot_ms"]["max"] == 0.0


def test_summary_table_renders_key_metrics() -> None:
    stats = DemoStats(started_ns=0, finished_ns=2_000_000_000)
    stats.record_consumer("consumer_a")
    stats.record_changes(4)
    stats.record_window(has_changes=True, observed_ns=1_000_000_000)
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
    stats.record_dml_listen(elapsed_ms=10.0, row_count=4, max_snapshots=64)
    stats.record_dml_build_batch(elapsed_ms=2.0, snapshot_span=64)
    stats.record_dml_sink(elapsed_ms=3.0)
    stats.record_dml_commit_duration(elapsed_ms=4.0)

    table = summary_table({"type": "summary", **stats.summary()})

    assert "DuckLake CDC demo summary" in table
    assert "| metric " in table
    assert "| description " in table
    assert "| duration_s " in table
    assert "| run_duration_s " in table
    assert "| changes " in table
    assert "| changes_per_s " in table
    assert "| batches " in table
    assert "| profile " in table
    assert "| producer_duration_s " in table
    assert "| producer_workers " in table
    assert "| latency_fresh_ms_p50 " in table
    assert "| latency_fresh_ms_p95 " in table
    assert "| consumer_listen_ms_p95 " in table
    assert "| consumer_snapshot_span_p95 " in table
    assert "| consumer_snapshot_span_max " in table
    assert "| consumer_listen_max_snapshots_p50 " in table
    assert "| consumer_build_ms_p95 " in table
    assert "| consumer_sink_ms_p95 " in table
    assert "| consumer_commit_ms_p95 " in table
    assert "| count_errors " in table
    assert "| count_missing_produced_ns " in table
    assert "| count_stale_latency_rows " in table
    assert "| count_fresh_latency_excluded " in table
    assert "| count_consumers " in table
    assert "| count_schemas " in table
    assert "| count_tables " in table
    assert "| count_dml " in table
    assert "| share_inserts " in table
    assert "| share_updates " in table
    assert "| share_deletes " in table
    assert "| rows_per_batch_p95 " in table

    # Things removed from the new knob-free demo's surface should not
    # leak back into the rendered table.
    assert "ddl_events" not in table
    assert "snapshot_events" not in table
    assert "catalog_queries" not in table
    assert "lake_tables_calls" not in table
    assert "iterations" not in table
    assert "empty_window_ratio" not in table
    assert "operation_ms" not in table
    assert "cdc_dml_changes_listen_ms_" not in table
    assert "cdc_window_ms_" not in table
    assert "cdc_commit_ms_" not in table
    assert "count_ddl" not in table
    assert "count_dropped_rows" not in table
