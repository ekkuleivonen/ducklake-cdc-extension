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


def test_demo_stats_summary_is_section_keyed() -> None:
    stats = DemoStats(started_ns=0)
    stats.record_consumer("consumer_a")
    stats.record_window(has_changes=True, observed_ns=1_000_000_000)
    stats.record_window(has_changes=True, observed_ns=1_500_000_000)
    stats.record_changes(2, table_name="main.orders")
    stats.record_error(ValueError("boom"))
    workload_values = {
        "benchmark_profile": "flat",
        "benchmark_duration_s": 0.0,
        "benchmark_schemas": 1,
        "benchmark_tables": 2,
        "benchmark_workers": 4,
        "benchmark_update_percent": 25.0,
        "benchmark_delete_percent": 10.0,
        "benchmark_batch_min": 50,
        "benchmark_batch_max": 250,
    }
    stats.record_change_observation(
        change_type="insert", table_name="main.orders", values=workload_values
    )
    stats.record_change_observation(
        change_type="update_postimage",
        table_name="main.orders",
        values=workload_values,
    )
    stats.record_commit()
    stats.record_dml_listen(elapsed_ms=10.0, row_count=2, max_snapshots=8)
    stats.record_dml_listen(elapsed_ms=1.0, row_count=0, max_snapshots=4)
    stats.record_dml_build_batch(elapsed_ms=2.0, snapshot_span=8)
    stats.record_dml_sink(elapsed_ms=3.0)
    stats.record_dml_commit_duration(elapsed_ms=4.0)
    stats.finished_ns = 3_000_000_000

    summary = stats.summary()

    # The top-level keys ARE the rendered sections.
    assert set(summary) == {
        "workload",
        "throughput",
        "e2e_latency_ms",
        "stage_latency_ms",
        "pipeline_breakdown",
        "post_delivery_ms",
        "action_mix",
        "health",
    }

    assert summary["workload"] == {
        "producer_profile": "flat",
        "producer_workers": 4,
        "producer_duration_s": 0.0,
        "producer_schemas": 1,
        "producer_tables": 2,
        "producer_update_pct": 25.0,
        "producer_delete_pct": 10.0,
        "producer_batch_min": 50,
        "producer_batch_max": 250,
        "consumers": 1,
        "tables_seen": 1,
    }

    throughput = summary["throughput"]
    assert throughput["duration_active_s"] == 2.0
    assert throughput["duration_run_s"] == 3.0
    assert throughput["changes_total"] == 2
    assert throughput["changes_per_s"] == 1.0
    assert throughput["batches_total"] == 2
    assert throughput["rows_per_batch_p95"] == 2.0

    pipeline = summary["pipeline_breakdown"]
    assert pipeline["extension_listen_ms_p95"] == 10.0
    assert pipeline["python_build_ms_p95"] == 2.0
    assert pipeline["snapshots_per_batch_p95"] == 8.0
    assert pipeline["snapshots_per_batch_max"] == 8.0
    assert pipeline["snapshots_per_listen_p50"] == 6.0

    post = summary["post_delivery_ms"]
    assert post["sink_p95"] == 3.0
    assert post["extension_commit_p95"] == 4.0

    assert summary["action_mix"] == {
        "inserts": 0.5,
        "updates": 0.5,
        "deletes": 0.0,
    }

    health = summary["health"]
    assert health["errors"] == 1
    assert health["rows_no_produced_ns"] == 0
    assert health["rows_clock_skew_clamped"] == 0
    assert health["rows_excluded_from_e2e"] == 0

    progress = stats.progress_snapshot()
    assert progress["changes_total"] == 2
    assert progress["consumers"] == 1
    assert progress["tables_seen"] == 1
    assert progress["batches_total"] == 2
    assert progress["errors"] == 1


def test_demo_stats_change_latency_records_fresh_and_stage_split() -> None:
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

    assert summary["e2e_latency_ms"]["max"] == pytest.approx(500.0)
    assert summary["stage_latency_ms"]["producer_p95"] == pytest.approx(5.0)
    assert summary["stage_latency_ms"]["pipeline_p95"] == pytest.approx(50.0)
    assert summary["health"]["rows_no_produced_ns"] == 1
    assert summary["health"]["rows_excluded_from_e2e"] == 2  # 1 stale + 1 missing


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

    assert summary["health"]["rows_clock_skew_clamped"] == 1
    assert summary["stage_latency_ms"]["producer_p95"] == 0.0


def test_summary_table_renders_key_sections() -> None:
    from datetime import UTC, datetime

    stats = DemoStats(started_ns=0, finished_ns=2_000_000_000)
    stats.record_consumer("consumer_a")
    stats.record_changes(4)
    stats.record_window(has_changes=True, observed_ns=1_000_000_000)
    snapshot_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    snapshot_epoch_ns = int(snapshot_time.timestamp() * 1_000_000_000)
    # Fully populated row so e2e + stage_latency + pipeline + post-delivery
    # sections all have non-zero data and render in the table.
    stats.record_change_latency(
        change_type="insert",
        produced_ns=1_000_000_000,
        produced_epoch_ns=snapshot_epoch_ns - 5_000_000,
        snapshot_time=snapshot_time,
        consumed_ns=1_100_000_000,
        consumed_epoch_ns=snapshot_epoch_ns + 50_000_000,
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
            "benchmark_batch_min": 1,
            "benchmark_batch_max": 10,
        },
    )
    stats.record_dml_listen(elapsed_ms=10.0, row_count=4, max_snapshots=64)
    stats.record_dml_build_batch(elapsed_ms=2.0, snapshot_span=64)
    stats.record_dml_sink(elapsed_ms=3.0)
    stats.record_dml_commit_duration(elapsed_ms=4.0)

    table = summary_table(stats.summary())

    # Title and headings.
    assert "DuckLake CDC demo summary" in table
    assert "Workload" in table
    assert "Throughput" in table
    assert "End-to-end latency" in table
    assert "Latency by stage" in table
    assert "Pipeline breakdown" in table
    assert "Post-delivery overhead" in table
    assert "Action mix" in table
    assert "Health" in table

    # Workload rows.
    assert "| producer_profile " in table
    assert "| producer_workers " in table
    assert "| producer_batch_min " in table
    assert "| producer_batch_max " in table
    assert "| consumers " in table
    assert "| tables_seen " in table

    # Throughput rows.
    assert "| duration_active_s " in table
    assert "| duration_run_s " in table
    assert "| changes_total " in table
    assert "| changes_per_s " in table
    assert "| batches_total " in table
    assert "| rows_per_batch_p95 " in table

    # Stage breakdown rows.
    assert "| e2e_p50_ms " in table
    assert "| producer_ms_p95 " in table
    assert "| pipeline_ms_p95 " in table
    assert "| extension_listen_ms_p95 " in table
    assert "| python_build_ms_p95 " in table
    assert "| sink_ms_p95 " in table
    assert "| extension_commit_ms_p95 " in table
    assert "| snapshots_per_batch_p95 " in table

    # Action mix and health.
    assert "| share_inserts " in table
    assert "| share_updates " in table
    assert "| share_deletes " in table
    assert "| errors " in table
    assert "| rows_no_produced_ns " in table
    assert "| rows_excluded_from_e2e " in table

    # Old names are gone.
    assert "fresh_action_latency" not in table
    assert "producer_to_snapshot_ms" not in table
    assert "snapshot_to_consumer_ms" not in table
    assert "consumer_listen_ms_p95" not in table
    assert "consumer_build_ms_p95" not in table
    assert "consumer_sink_ms_p95" not in table
    assert "count_consumers" not in table
    assert "count_tables" not in table
    assert "count_dml" not in table
