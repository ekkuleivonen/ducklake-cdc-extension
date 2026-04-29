"""Run the Phase 1 ducklake-cdc smoke benchmark.

The Python layer owns workload parsing, command-line overrides, temporary
paths, result JSON, and CI warnings. The benchmark itself is a tiny C++
harness compiled against the locally built DuckDB library so it exercises
the same local extension artifacts as the existing smoke probes.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]
DEFAULT_WORKLOAD = REPO / "bench" / "light.yaml"
DEFAULT_RESULTS_DIR = REPO / "bench" / "results"


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
    duration_seconds: int
    snapshots_per_minute: float
    rows_per_snapshot: int
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
    workload = Workload(
        name=raw.get("name", path.stem),
        duration_seconds=int(raw["duration_seconds"]),
        snapshots_per_minute=float(raw["snapshots_per_minute"]),
        rows_per_snapshot=int(raw["rows_per_snapshot"]),
        consumers=int(raw["consumers"]),
        max_snapshots=int(raw["max_snapshots"]),
    )
    for field_name in ("duration_seconds", "snapshots_per_minute", "rows_per_snapshot", "consumers", "max_snapshots"):
        override = getattr(args, field_name)
        if override is not None:
            setattr(workload, field_name, override)
    return workload


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
        str(workload.snapshots_per_minute),
        str(workload.rows_per_snapshot),
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
    parser.add_argument("--build", choices=("debug", "release"), default="debug")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--hardware-label", default=f"{platform.system()} {platform.machine()} {socket.gethostname()}")
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--snapshots-per-minute", type=float)
    parser.add_argument("--rows-per-snapshot", type=int)
    parser.add_argument("--consumers", type=int)
    parser.add_argument("--max-snapshots", type=int)
    args = parser.parse_args()

    workload = load_workload(args.workload, args)
    paths = build_paths(args.build)
    require_artifacts(paths, args.build)

    with tempfile.TemporaryDirectory(prefix="ducklake_cdc_bench_") as tmp:
        tmpdir = Path(tmp)
        binary = compile_harness(tmpdir, paths)
        measurements = run_harness(binary, paths, workload, tmpdir)

    result = {
        "run_id": str(uuid.uuid4()),
        "timestamp_utc": dt.datetime.now(dt.UTC).isoformat(),
        "commit": git_commit(),
        "hardware_label": args.hardware_label,
        "build": args.build,
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
        f"events={measurements['total_events']} "
        f"elapsed={measurements['elapsed_seconds']:.2f}s "
        f"p99={measurements['latency_ms_p99']:.2f}ms "
        f"catalog_qps={measurements['catalog_qps_avg']:.3f}"
    )
    emit_soft_warnings(measurements)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
