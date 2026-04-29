"""Run the Phase 1 ducklake-cdc smoke benchmark.

The Python layer owns workload parsing, command-line overrides, temporary
paths, result JSON, and CI warnings. Local development can still run a tiny
C++ harness against a local DuckDB build; CI uses the official DuckDB Python
package plus a previously built `ducklake_cdc` artifact so benchmarks do not
rebuild DuckDB or upstream DuckLake from source.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]
DEFAULT_WORKLOAD = REPO / "bench" / "light.yaml"
DEFAULT_RESULTS_DIR = REPO / "bench" / "results"
RESULT_SCHEMA_VERSION = 1
SUPPORTED_LOAD_PROFILES = {"flat"}


HARNESS = r'''
#include "duckdb.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;

unique_ptr<QueryResult> RequireOk(Connection &conn, const std::string &sql) {
	auto result = conn.Query(sql);
	if (!result || result->HasError()) {
		throw std::runtime_error(sql + "\n" + (result ? result->GetError() : "no result"));
	}
	return result;
}

std::string QuoteString(const std::string &value) {
	std::string result = "'";
	for (auto c : value) {
		if (c == '\'') {
			result += "''";
		} else {
			result += c;
		}
	}
	result += "'";
	return result;
}

int64_t NowNanos() {
	const auto now = std::chrono::steady_clock::now().time_since_epoch();
	return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
}

double Percentile(std::vector<double> values, double percentile) {
	if (values.empty()) {
		return 0.0;
	}
	std::sort(values.begin(), values.end());
	const double rank = (percentile / 100.0) * static_cast<double>(values.size() - 1);
	const auto lower = static_cast<size_t>(std::floor(rank));
	const auto upper = static_cast<size_t>(std::ceil(rank));
	if (lower == upper) {
		return values[lower];
	}
	const double weight = rank - static_cast<double>(lower);
	return values[lower] * (1.0 - weight) + values[upper] * weight;
}

std::string ValuesForSnapshot(int snapshot_index, int rows_per_snapshot, int64_t produced_ns) {
	std::ostringstream sql;
	sql << "INSERT INTO lake.bench_events VALUES ";
	for (int row = 0; row < rows_per_snapshot; row++) {
		if (row > 0) {
			sql << ", ";
		}
		const int64_t event_id = (static_cast<int64_t>(snapshot_index) * rows_per_snapshot) + row;
		sql << "(" << event_id << ", " << produced_ns << ", " << snapshot_index << ", " << row << ")";
	}
	return sql.str();
}

int main(int argc, char **argv) {
	if (argc != 11) {
		std::cerr << "usage: harness <ducklake-extension> <cdc-extension> <lake-path> <data-path>"
		          << " <duration-seconds> <snapshots-per-minute> <rows-per-snapshot>"
		          << " <consumers> <max-snapshots> <workload-name>\n";
		return 2;
	}

	const std::string ducklake_extension = argv[1];
	const std::string cdc_extension = argv[2];
	const std::string lake_path = argv[3];
	const std::string data_path = argv[4];
	const int duration_seconds = std::stoi(argv[5]);
	const double snapshots_per_minute = std::stod(argv[6]);
	const int rows_per_snapshot = std::stoi(argv[7]);
	const int consumers = std::stoi(argv[8]);
	const int max_snapshots = std::stoi(argv[9]);
	const std::string workload_name = argv[10];

	if (duration_seconds <= 0 || snapshots_per_minute <= 0 || rows_per_snapshot <= 0 || consumers <= 0 ||
	    max_snapshots <= 0) {
		throw std::runtime_error("all workload parameters must be positive");
	}

	const int planned_snapshots =
	    std::max(1, static_cast<int>(std::llround((static_cast<double>(duration_seconds) * snapshots_per_minute) / 60.0)));
	const auto interval_ns = static_cast<int64_t>(std::llround(60000000000.0 / snapshots_per_minute));

	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", Value::BOOLEAN(true));
	DuckDB db(nullptr, &config);
	Connection conn(db);

	RequireOk(conn, "LOAD " + QuoteString(ducklake_extension));
	RequireOk(conn, "LOAD parquet");
	RequireOk(conn, "LOAD " + QuoteString(cdc_extension));
	RequireOk(conn, "ATTACH 'ducklake:" + lake_path + "' AS lake (DATA_PATH " + QuoteString(data_path) + ")");
	RequireOk(conn, "CREATE TABLE lake.bench_events(id BIGINT, produced_ns BIGINT, snapshot_no INTEGER, row_no INTEGER)");
	for (int consumer = 0; consumer < consumers; consumer++) {
		RequireOk(conn, "SELECT * FROM cdc_consumer_create('lake', 'bench_" + std::to_string(consumer) + "')");
	}

	std::vector<double> latencies_ms;
	latencies_ms.reserve(static_cast<size_t>(planned_snapshots) * rows_per_snapshot * consumers);

	int64_t total_events = 0;
	int64_t cdc_window_calls = 0;
	int64_t cdc_changes_calls = 0;
	int64_t cdc_commit_calls = 0;
	int64_t producer_inserts = 0;
	const auto started_at = std::chrono::steady_clock::now();

	for (int snapshot = 0; snapshot < planned_snapshots; snapshot++) {
		const auto target = started_at + std::chrono::nanoseconds(interval_ns * static_cast<int64_t>(snapshot));
		std::this_thread::sleep_until(target);

		const auto produced_ns = NowNanos();
		RequireOk(conn, ValuesForSnapshot(snapshot, rows_per_snapshot, produced_ns));
		producer_inserts++;

		for (int consumer = 0; consumer < consumers; consumer++) {
			const std::string consumer_name = "bench_" + std::to_string(consumer);
			auto window = RequireOk(conn, "SELECT * FROM cdc_window('lake', '" + consumer_name + "', max_snapshots => " +
			                              std::to_string(max_snapshots) + ")");
			cdc_window_calls++;
			auto &window_mat = window->Cast<MaterializedQueryResult>();
			if (window_mat.RowCount() != 1 || !window_mat.GetValue(2, 0).GetValue<bool>()) {
				throw std::runtime_error("expected a non-empty cdc_window for " + consumer_name);
			}
			const auto end_snapshot = window_mat.GetValue(1, 0).GetValue<int64_t>();

			auto changes = RequireOk(conn, "SELECT * FROM cdc_changes('lake', '" + consumer_name +
			                               "', 'bench_events', max_snapshots => " +
			                               std::to_string(max_snapshots) + ")");
			cdc_changes_calls++;
			auto &changes_mat = changes->Cast<MaterializedQueryResult>();
			if (changes_mat.RowCount() != static_cast<idx_t>(rows_per_snapshot)) {
				throw std::runtime_error("expected " + std::to_string(rows_per_snapshot) + " rows from cdc_changes, got " +
				                         std::to_string(changes_mat.RowCount()));
			}

			const auto consumed_ns = NowNanos();
			for (idx_t row = 0; row < changes_mat.RowCount(); row++) {
				const auto row_produced_ns = changes_mat.GetValue(4, row).GetValue<int64_t>();
				latencies_ms.push_back(static_cast<double>(consumed_ns - row_produced_ns) / 1000000.0);
			}
			total_events += static_cast<int64_t>(changes_mat.RowCount());

			RequireOk(conn, "SELECT * FROM cdc_commit('lake', '" + consumer_name + "', " + std::to_string(end_snapshot) +
			                    ")");
			cdc_commit_calls++;
		}
	}

	const auto finished_at = std::chrono::steady_clock::now();
	const auto elapsed_seconds =
	    std::chrono::duration_cast<std::chrono::duration<double>>(finished_at - started_at).count();
	const int64_t catalog_queries_estimated = cdc_window_calls + cdc_changes_calls + cdc_commit_calls;
	const double catalog_qps_estimated = static_cast<double>(catalog_queries_estimated) / elapsed_seconds;
	const double events_per_second = static_cast<double>(total_events) / elapsed_seconds;
	const double latency_sum = std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0);
	const double latency_mean = latencies_ms.empty() ? 0.0 : latency_sum / static_cast<double>(latencies_ms.size());
	const auto latency_minmax = std::minmax_element(latencies_ms.begin(), latencies_ms.end());
	const double latency_max = latencies_ms.empty() ? 0.0 : *latency_minmax.second;

	std::cout << std::fixed << std::setprecision(6);
	std::cout << "{\n";
	std::cout << "  \"workload_name\": \"" << workload_name << "\",\n";
	std::cout << "  \"planned_snapshots\": " << planned_snapshots << ",\n";
	std::cout << "  \"producer_inserts\": " << producer_inserts << ",\n";
	std::cout << "  \"total_events\": " << total_events << ",\n";
	std::cout << "  \"elapsed_seconds\": " << elapsed_seconds << ",\n";
	std::cout << "  \"events_per_second\": " << events_per_second << ",\n";
	std::cout << "  \"latency_ms_p50\": " << Percentile(latencies_ms, 50) << ",\n";
	std::cout << "  \"latency_ms_p95\": " << Percentile(latencies_ms, 95) << ",\n";
	std::cout << "  \"latency_ms_p99\": " << Percentile(latencies_ms, 99) << ",\n";
	std::cout << "  \"latency_ms_max\": " << latency_max << ",\n";
	std::cout << "  \"latency_ms_mean\": " << latency_mean << ",\n";
	std::cout << "  \"catalog_queries_estimated\": " << catalog_queries_estimated << ",\n";
	std::cout << "  \"catalog_qps_avg\": " << catalog_qps_estimated << ",\n";
	std::cout << "  \"lag_snapshots_max\": 0,\n";
	std::cout << "  \"cdc_window_calls\": " << cdc_window_calls << ",\n";
	std::cout << "  \"cdc_changes_calls\": " << cdc_changes_calls << ",\n";
	std::cout << "  \"cdc_commit_calls\": " << cdc_commit_calls << "\n";
	std::cout << "}\n";
	return 0;
}
'''


@dataclass
class Workload:
    name: str
    load_profile: str
    duration_seconds: int
    target_snapshots_per_second: float
    target_rows_per_snapshot: int
    consumers: int
    max_snapshots: int


def parse_flat_yaml(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw in enumerate(path.read_text().splitlines(), start=1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if ":" not in line:
            raise ValueError(f"{path}:{line_number}: expected 'key: value'")
        key, value = line.split(":", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def load_workload(path: Path, args: argparse.Namespace) -> Workload:
    raw = parse_flat_yaml(path)
    load_profile = raw.get("load_profile", "flat")
    if load_profile not in SUPPORTED_LOAD_PROFILES:
        supported = ", ".join(sorted(SUPPORTED_LOAD_PROFILES))
        raise ValueError(f"{path}: unsupported load_profile {load_profile!r}; supported: {supported}")

    if "target_snapshots_per_second" in raw:
        target_snapshots_per_second = float(raw["target_snapshots_per_second"])
    else:
        # Backward-compatible parser for pre-schema-v1 descriptors.
        target_snapshots_per_second = float(raw["snapshots_per_minute"]) / 60.0

    target_rows_per_snapshot = int(raw.get("target_rows_per_snapshot", raw.get("rows_per_snapshot", "0")))
    workload = Workload(
        name=raw.get("name", path.stem),
        load_profile=load_profile,
        duration_seconds=int(raw["duration_seconds"]),
        target_snapshots_per_second=target_snapshots_per_second,
        target_rows_per_snapshot=target_rows_per_snapshot,
        consumers=int(raw["consumers"]),
        max_snapshots=int(raw["max_snapshots"]),
    )
    for field_name in (
        "duration_seconds",
        "target_snapshots_per_second",
        "target_rows_per_snapshot",
        "consumers",
        "max_snapshots",
    ):
        override = getattr(args, field_name)
        if override is not None:
            setattr(workload, field_name, override)
    if args.load_profile is not None:
        if args.load_profile not in SUPPORTED_LOAD_PROFILES:
            supported = ", ".join(sorted(SUPPORTED_LOAD_PROFILES))
            raise ValueError(f"unsupported --load-profile {args.load_profile!r}; supported: {supported}")
        workload.load_profile = args.load_profile
    if (
        workload.duration_seconds <= 0
        or workload.target_snapshots_per_second <= 0
        or workload.target_rows_per_snapshot <= 0
        or workload.consumers <= 0
        or workload.max_snapshots <= 0
    ):
        raise ValueError("all numeric workload parameters must be positive")
    return workload


def quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_paths(build: str) -> dict[str, Path]:
    lib_name = "libduckdb.dylib" if sys.platform == "darwin" else "libduckdb.so"
    return {
        "include": REPO / "duckdb" / "src" / "include",
        "lib_dir": REPO / "build" / build / "src",
        "lib": REPO / "build" / build / "src" / lib_name,
        "ducklake_extension": REPO / "build" / build / "extension" / "ducklake" / "ducklake.duckdb_extension",
        "cdc_extension": REPO / "build" / build / "extension" / "ducklake_cdc" / "ducklake_cdc.duckdb_extension",
    }


def require_artifacts(paths: dict[str, Path], build: str) -> None:
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        joined = "\n  ".join(missing)
        raise FileNotFoundError(f"missing {build} build artifacts; run `make {build}` first:\n  {joined}")


def git_commit() -> str:
    result = subprocess.run(["git", "rev-parse", "--short=12", "HEAD"], cwd=REPO, text=True, capture_output=True)
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def compile_harness(tmpdir: Path, paths: dict[str, Path]) -> Path:
    source = tmpdir / "ducklake_cdc_bench.cpp"
    binary = tmpdir / "ducklake_cdc_bench"
    source.write_text(HARNESS)
    compiler = shutil.which("c++") or shutil.which("clang++") or shutil.which("g++")
    if not compiler:
        raise RuntimeError("no C++ compiler found on PATH")
    cmd = [
        compiler,
        "-std=c++17",
        f"-I{paths['include']}",
        str(source),
        f"-L{paths['lib_dir']}",
        "-lduckdb",
        f"-Wl,-rpath,{paths['lib_dir']}",
        "-o",
        str(binary),
    ]
    subprocess.run(cmd, cwd=REPO, check=True)
    return binary


def now_nanos() -> int:
    return time.monotonic_ns()


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    rank = (percentile_value / 100.0) * (len(sorted_values) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    return (sorted_values[lower] * (1.0 - weight)) + (sorted_values[upper] * weight)


def metric_summary(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
            "mean": 0.0,
        }
    return {
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values),
        "mean": sum(values) / len(values),
    }


def timed_execute(conn: Any, sql: str, samples_ms: list[float]) -> Any:
    started_ns = now_nanos()
    try:
        return require_ok_python(conn, sql)
    finally:
        samples_ms.append((now_nanos() - started_ns) / 1000000.0)


def timed_fetchall(conn: Any, sql: str, samples_ms: list[float]) -> list[Any]:
    started_ns = now_nanos()
    try:
        return require_ok_python(conn, sql).fetchall()
    finally:
        samples_ms.append((now_nanos() - started_ns) / 1000000.0)


def values_for_snapshot(snapshot_index: int, rows_per_snapshot: int, produced_ns: int) -> str:
    values = []
    for row in range(rows_per_snapshot):
        event_id = (snapshot_index * rows_per_snapshot) + row
        values.append(f"({event_id}, {produced_ns}, {snapshot_index}, {row})")
    return "INSERT INTO lake.bench_events VALUES " + ", ".join(values)


def require_ok_python(conn: Any, sql: str) -> Any:
    try:
        return conn.execute(sql)
    except Exception as exc:
        raise RuntimeError(f"{sql}\n{exc}") from exc


def run_python_benchmark(cdc_extension: Path, workload: Workload, tmpdir: Path) -> dict[str, Any]:
    import duckdb

    lake = tmpdir / "bench.ducklake"
    data = tmpdir / "bench_data"

    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    require_ok_python(conn, "INSTALL ducklake")
    require_ok_python(conn, "LOAD ducklake")
    require_ok_python(conn, "LOAD parquet")
    require_ok_python(conn, "LOAD " + quote_string(str(cdc_extension)))
    require_ok_python(conn, "ATTACH 'ducklake:" + str(lake) + "' AS lake (DATA_PATH " + quote_string(str(data)) + ")")
    require_ok_python(conn, "CREATE TABLE lake.bench_events(id BIGINT, produced_ns BIGINT, snapshot_no INTEGER, row_no INTEGER)")

    for consumer in range(workload.consumers):
        require_ok_python(conn, f"SELECT * FROM cdc_consumer_create('lake', 'bench_{consumer}')")

    planned_snapshots = max(1, math.floor(workload.duration_seconds * workload.target_snapshots_per_second))
    interval_seconds = 1.0 / workload.target_snapshots_per_second
    end_to_end_latencies_ms: list[float] = []
    producer_insert_ms: list[float] = []
    cdc_window_ms: list[float] = []
    cdc_changes_ms: list[float] = []
    cdc_commit_ms: list[float] = []
    consumed_events = 0
    cdc_window_calls = 0
    cdc_changes_calls = 0
    cdc_commit_calls = 0
    producer_inserts = 0
    produced_rows = 0
    started_at = time.monotonic()

    for snapshot in range(planned_snapshots):
        target = started_at + (interval_seconds * snapshot)
        sleep_seconds = target - time.monotonic()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        produced_ns = now_nanos()
        timed_execute(
            conn,
            values_for_snapshot(snapshot, workload.target_rows_per_snapshot, produced_ns),
            producer_insert_ms,
        )
        producer_inserts += 1
        produced_rows += workload.target_rows_per_snapshot

        for consumer in range(workload.consumers):
            consumer_name = f"bench_{consumer}"
            window_rows = timed_fetchall(
                conn,
                "SELECT * FROM cdc_window('lake', "
                + quote_string(consumer_name)
                + f", max_snapshots => {workload.max_snapshots})",
                cdc_window_ms,
            )
            cdc_window_calls += 1
            if len(window_rows) != 1 or not window_rows[0][2]:
                raise RuntimeError(f"expected a non-empty cdc_window for {consumer_name}")
            end_snapshot = window_rows[0][1]

            change_rows = timed_fetchall(
                conn,
                "SELECT * FROM cdc_changes('lake', "
                + quote_string(consumer_name)
                + ", 'bench_events', max_snapshots => "
                + str(workload.max_snapshots)
                + ")",
                cdc_changes_ms,
            )
            cdc_changes_calls += 1
            if len(change_rows) != workload.target_rows_per_snapshot:
                raise RuntimeError(
                    f"expected {workload.target_rows_per_snapshot} rows from cdc_changes, got {len(change_rows)}"
                )

            consumed_ns = now_nanos()
            for row in change_rows:
                end_to_end_latencies_ms.append((consumed_ns - int(row[4])) / 1000000.0)
            consumed_events += len(change_rows)

            timed_execute(
                conn,
                f"SELECT * FROM cdc_commit('lake', {quote_string(consumer_name)}, {end_snapshot})",
                cdc_commit_ms,
            )
            cdc_commit_calls += 1

    actual_duration_seconds = time.monotonic() - started_at
    catalog_queries_estimated = cdc_window_calls + cdc_changes_calls + cdc_commit_calls

    return {
        "workload_name": workload.name,
        "load_profile": workload.load_profile,
        "scheduled_duration_seconds": workload.duration_seconds,
        "actual_duration_seconds": actual_duration_seconds,
        "target_snapshots_per_second": workload.target_snapshots_per_second,
        "actual_snapshots_per_second": producer_inserts / actual_duration_seconds,
        "target_rows_per_snapshot": workload.target_rows_per_snapshot,
        "actual_rows_per_second": produced_rows / actual_duration_seconds,
        "planned_snapshots": planned_snapshots,
        "producer_inserts": producer_inserts,
        "produced_rows": produced_rows,
        "consumed_events": consumed_events,
        "consumed_events_per_second": consumed_events / actual_duration_seconds,
        "end_to_end_latency_ms": metric_summary(end_to_end_latencies_ms),
        "operation_ms": {
            "producer_insert": metric_summary(producer_insert_ms),
            "cdc_window": metric_summary(cdc_window_ms),
            "cdc_changes": metric_summary(cdc_changes_ms),
            "cdc_commit": metric_summary(cdc_commit_ms),
        },
        "catalog_queries_estimated": catalog_queries_estimated,
        "catalog_qps_avg": catalog_queries_estimated / actual_duration_seconds,
        "lag_snapshots_max": 0,
        "cdc_window_calls": cdc_window_calls,
        "cdc_changes_calls": cdc_changes_calls,
        "cdc_commit_calls": cdc_commit_calls,
    }


def run_harness(binary: Path, paths: dict[str, Path], workload: Workload, tmpdir: Path) -> dict[str, Any]:
    lake = tmpdir / "bench.ducklake"
    data = tmpdir / "bench_data"
    cmd = [
        str(binary),
        str(paths["ducklake_extension"]),
        str(paths["cdc_extension"]),
        str(lake),
        str(data),
        str(workload.duration_seconds),
        str(workload.target_snapshots_per_second * 60.0),
        str(workload.target_rows_per_snapshot),
        str(workload.consumers),
        str(workload.max_snapshots),
        workload.name,
    ]
    completed = subprocess.run(cmd, cwd=REPO, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if completed.returncode != 0:
        sys.stderr.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        raise RuntimeError(f"benchmark harness exited with {completed.returncode}")
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    return json.loads(completed.stdout)


def normalize_legacy_measurements(measurements: dict[str, Any], workload: Workload) -> dict[str, Any]:
    """Normalize local C++ harness output into the schema-v1 envelope."""
    actual_duration_seconds = float(measurements["elapsed_seconds"])
    consumed_events = int(measurements["total_events"])
    producer_inserts = int(measurements["producer_inserts"])
    produced_rows = producer_inserts * workload.target_rows_per_snapshot
    return {
        "workload_name": workload.name,
        "load_profile": workload.load_profile,
        "scheduled_duration_seconds": workload.duration_seconds,
        "actual_duration_seconds": actual_duration_seconds,
        "target_snapshots_per_second": workload.target_snapshots_per_second,
        "actual_snapshots_per_second": producer_inserts / actual_duration_seconds,
        "target_rows_per_snapshot": workload.target_rows_per_snapshot,
        "actual_rows_per_second": produced_rows / actual_duration_seconds,
        "planned_snapshots": int(measurements["planned_snapshots"]),
        "producer_inserts": producer_inserts,
        "produced_rows": produced_rows,
        "consumed_events": consumed_events,
        "consumed_events_per_second": consumed_events / actual_duration_seconds,
        "end_to_end_latency_ms": {
            "p50": float(measurements["latency_ms_p50"]),
            "p95": float(measurements["latency_ms_p95"]),
            "p99": float(measurements["latency_ms_p99"]),
            "max": float(measurements["latency_ms_max"]),
            "mean": float(measurements["latency_ms_mean"]),
        },
        "operation_ms": {},
        "catalog_queries_estimated": int(measurements["catalog_queries_estimated"]),
        "catalog_qps_avg": float(measurements["catalog_qps_avg"]),
        "lag_snapshots_max": int(measurements["lag_snapshots_max"]),
        "cdc_window_calls": int(measurements["cdc_window_calls"]),
        "cdc_changes_calls": int(measurements["cdc_changes_calls"]),
        "cdc_commit_calls": int(measurements["cdc_commit_calls"]),
    }


def result_path(workload: Workload, output: Path | None) -> Path:
    if output:
        return output
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_RESULTS_DIR / f"{workload.name}-{timestamp}-{git_commit()}.json"


def emit_soft_warnings(measurements: dict[str, Any]) -> None:
    lag = float(measurements.get("lag_snapshots_max", 0))
    catalog_qps = float(measurements.get("catalog_qps_avg", 0))
    if lag > 0:
        print(f"::warning::benchmark lag_snapshots_max={lag} (expected 0 for Phase 1 light smoke)")
    if catalog_qps > 5:
        print(f"::warning::benchmark catalog_qps_avg={catalog_qps:.3f} exceeds Phase 1 soft gate of 5")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", type=Path, default=DEFAULT_WORKLOAD)
    parser.add_argument("--runtime", choices=("local-build", "official"), default="local-build")
    parser.add_argument("--build", choices=("debug", "release"), default="debug")
    parser.add_argument("--cdc-extension", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--hardware-label", default=f"{platform.system()} {platform.machine()} {socket.gethostname()}")
    parser.add_argument("--load-profile", choices=sorted(SUPPORTED_LOAD_PROFILES))
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--target-snapshots-per-second", type=float)
    parser.add_argument("--target-rows-per-snapshot", type=int)
    parser.add_argument("--consumers", type=int)
    parser.add_argument("--max-snapshots", type=int)
    args = parser.parse_args()

    workload = load_workload(args.workload, args)

    with tempfile.TemporaryDirectory(prefix="ducklake_cdc_bench_") as tmp:
        tmpdir = Path(tmp)
        if args.runtime == "local-build":
            paths = build_paths(args.build)
            require_artifacts(paths, args.build)
            binary = compile_harness(tmpdir, paths)
            measurements = normalize_legacy_measurements(run_harness(binary, paths, workload, tmpdir), workload)
            build_label = args.build
        else:
            if args.cdc_extension is None:
                raise ValueError("--cdc-extension is required when --runtime official")
            cdc_extension = args.cdc_extension.resolve()
            if not cdc_extension.exists():
                raise FileNotFoundError(f"missing ducklake_cdc extension artifact: {cdc_extension}")
            measurements = run_python_benchmark(cdc_extension, workload, tmpdir)
            build_label = "official"

    result = {
        "result_schema_version": RESULT_SCHEMA_VERSION,
        "run_id": str(uuid.uuid4()),
        "timestamp_utc": dt.datetime.now(dt.UTC).isoformat(),
        "commit": git_commit(),
        "hardware_label": args.hardware_label,
        "runtime": args.runtime,
        "build": build_label,
        "workload": asdict(workload),
        "measurements": measurements,
        "soft_gates": {
            "lag_snapshots_max_lte": 0,
            "catalog_qps_avg_lte": 5,
        },
    }

    out = result_path(workload, args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(f"benchmark result written to {out}")
    print(
        "summary: "
        f"events={measurements['consumed_events']} "
        f"elapsed={measurements['actual_duration_seconds']:.2f}s "
        f"p99={measurements['end_to_end_latency_ms']['p99']:.2f}ms "
        f"catalog_qps={measurements['catalog_qps_avg']:.3f}"
    )
    emit_soft_warnings(measurements)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
