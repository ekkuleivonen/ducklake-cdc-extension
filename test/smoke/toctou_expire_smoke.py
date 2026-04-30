"""Smoke test for the cdc_window TOCTOU race against ducklake_expire_snapshots.

The race we're defending (per Phase 1 roadmap):

1. Connection A sits inside cdc_window (lease + cursor existence + range
   compute + commit) — all in one transaction.
2. Connection B runs `ducklake_expire_snapshots` whose cutoff includes A's
   cursor.
3. A resumes; the existence check + range scan must produce either a
   consistent window OR a clean structured `CDC_GAP` error.

The embedded DuckDB catalog runs writes serially under a single catalog
write lock. The TOCTOU defence collapses to: "B's expire commits before
or after A's transaction; A never sees a torn read." We exercise the
"after A's window, before A's next call" case here — A reads a window,
B expires the cursor out from under A, A's next cdc_window must raise
CDC_GAP cleanly with the recovery message documented in
`docs/errors.md`.

Usage:

    uv run python test/smoke/toctou_expire_smoke.py

Defaults to the release build because the harness loads the official prebuilt
DuckLake extension binary (no ASAN runtime), which conflicts with our debug
build's ASAN. Build it via `make release` first, or override with
`DUCKLAKE_CDC_BUILD=debug` if you have built debug without sanitizers
(`DISABLE_SANITIZER=1 make debug`).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
BUILD = os.environ.get("DUCKLAKE_CDC_BUILD", "release")
DUCKDB_INCLUDE = REPO / "duckdb" / "src" / "include"
LIBDUCKDB_DIR = REPO / "build" / BUILD / "src"
LIBDUCKDB = LIBDUCKDB_DIR / ("libduckdb.dylib" if sys.platform == "darwin" else "libduckdb.so")
CDC_EXTENSION = REPO / "build" / BUILD / "extension" / "ducklake_cdc" / "ducklake_cdc.duckdb_extension"


HARNESS = r'''
#include "duckdb.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace duckdb;

unique_ptr<QueryResult> RequireOk(Connection &conn, const std::string &sql) {
	auto result = conn.Query(sql);
	if (!result || result->HasError()) {
		throw std::runtime_error(sql + "\n" + (result ? result->GetError() : "no result"));
	}
	return result;
}

void RequireError(Connection &conn, const std::string &sql, const std::string &needle) {
	auto result = conn.Query(sql);
	if (!result || !result->HasError()) {
		throw std::runtime_error("expected error containing " + needle + " from: " + sql);
	}
	const auto error = result->GetError();
	if (error.find(needle) == std::string::npos) {
		throw std::runtime_error("expected error containing " + needle + " but got: " + error);
	}
}

std::string QuotePath(const std::string &path) {
	std::string result = "'";
	for (auto c : path) {
		if (c == '\'') {
			result += "''";
		} else {
			result += c;
		}
	}
	result += "'";
	return result;
}

int main(int argc, char **argv) {
	if (argc != 4) {
		std::cerr << "usage: harness <cdc-extension> <lake-path> <data-path>\n";
		return 2;
	}
	const std::string cdc_extension = argv[1];
	const std::string lake_path = argv[2];
	const std::string data_path = argv[3];

	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", Value::BOOLEAN(true));
	config.SetOptionByName("autoinstall_known_extensions", Value::BOOLEAN(true));
	config.SetOptionByName("autoload_known_extensions", Value::BOOLEAN(true));
	DuckDB db(nullptr, &config);
	Connection a(db);
	Connection b(db);

	RequireOk(a, "INSTALL ducklake");
	for (auto *conn : {&a, &b}) {
		RequireOk(*conn, "LOAD ducklake");
		RequireOk(*conn, "LOAD parquet");
		RequireOk(*conn, "LOAD " + QuotePath(cdc_extension));
	}

	const auto attach = "ATTACH 'ducklake:" + lake_path + "' AS lake (DATA_PATH '" + data_path + "')";
	RequireOk(a, attach);
	auto b_has_lake = RequireOk(b, "SELECT count(*) FROM duckdb_databases() WHERE database_name = 'lake'");
	if (b_has_lake->Cast<MaterializedQueryResult>().GetValue(0, 0).GetValue<int64_t>() == 0) {
		RequireOk(b, attach);
	}

	// Build a small history of external snapshots so there is something
	// to expire out from under the consumer.
	RequireOk(a, "CREATE TABLE lake.toctou(id INTEGER)");
	RequireOk(a, "INSERT INTO lake.toctou VALUES (1)");
	RequireOk(a, "INSERT INTO lake.toctou VALUES (2)");
	RequireOk(a, "INSERT INTO lake.toctou VALUES (3)");

	auto snapshots = RequireOk(a, "SELECT snapshot_id FROM lake.snapshots() ORDER BY snapshot_id");
	auto &snap_mat = snapshots->Cast<MaterializedQueryResult>();
	if (snap_mat.RowCount() < 4) {
		throw std::runtime_error("expected at least four lake snapshots before TOCTOU run");
	}
	std::vector<int64_t> snapshot_ids;
	for (idx_t i = 0; i < snap_mat.RowCount(); ++i) {
		snapshot_ids.push_back(snap_mat.GetValue(0, i).GetValue<int64_t>());
	}

	// Pin the consumer to the second-oldest external snapshot so we can
	// expire snapshots strictly behind it without touching the cursor
	// itself; once that succeeds, A's first cdc_window must still
	// produce a clean window.
	const auto pin_snapshot = std::to_string(snapshot_ids[1]);
	RequireOk(a, "SELECT * FROM cdc_consumer_create('lake', 'toctou', start_at = '" + pin_snapshot + "')");

	auto window_a = RequireOk(a, "SELECT * FROM cdc_window('lake', 'toctou')");
	if (!window_a->Cast<MaterializedQueryResult>().GetValue(2, 0).GetValue<bool>()) {
		throw std::runtime_error("expected non-empty cdc_window from connection A before expire");
	}

	// Expire snapshots that include A's cursor. From B's vantage point
	// the catalog write is serial — A must see the expire on its next
	// catalog scan. The defence is "no torn read"; the visible outcome
	// is `CDC_GAP` rather than a silently wrong window.
	std::string versions = std::to_string(snapshot_ids[0]);
	for (size_t i = 1; i <= 1; ++i) {
		versions += ", " + std::to_string(snapshot_ids[i]);
	}
	RequireOk(b, "CALL ducklake_expire_snapshots('lake', versions => [" + versions + "])");

	// A's next cdc_window must raise CDC_GAP — the recovery command in
	// the message is the contract documented in `docs/errors.md`.
	RequireError(a, "SELECT * FROM cdc_window('lake', 'toctou')", "CDC_GAP");

	// `cdc_consumer_reset` recovers — A reattaches to the oldest
	// available snapshot and the next cdc_window succeeds.
	RequireOk(a, "SELECT * FROM cdc_consumer_reset('lake', 'toctou')");
	auto window_after_reset = RequireOk(a, "SELECT * FROM cdc_window('lake', 'toctou')");
	if (window_after_reset->Cast<MaterializedQueryResult>().RowCount() != 1) {
		throw std::runtime_error("expected exactly one row from cdc_window after reset");
	}

	std::cout << "toctou_expire_smoke PASSED\n";
	return 0;
}
'''


def main() -> int:
    if not LIBDUCKDB.exists():
        print(f"missing {LIBDUCKDB}; run `make {BUILD}` first", file=sys.stderr)
        return 1
    if not CDC_EXTENSION.exists():
        print(f"missing ducklake_cdc {BUILD} artifact; run `make {BUILD}` first", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="ducklake_cdc_toctou_") as tmp:
        tmpdir = Path(tmp)
        source = tmpdir / "toctou_expire_smoke.cpp"
        binary = tmpdir / "toctou_expire_smoke"
        lake = tmpdir / "toctou.ducklake"
        data = tmpdir / "toctou_data"
        source.write_text(HARNESS)

        compiler = shutil.which("c++") or shutil.which("clang++") or shutil.which("g++")
        if not compiler:
            print("no C++ compiler found on PATH", file=sys.stderr)
            return 1

        compile_cmd = [
            compiler,
            "-std=c++17",
            f"-I{DUCKDB_INCLUDE}",
            str(source),
            f"-L{LIBDUCKDB_DIR}",
            "-lduckdb",
            f"-Wl,-rpath,{LIBDUCKDB_DIR}",
            "-o",
            str(binary),
        ]
        subprocess.run(compile_cmd, cwd=REPO, check=True)
        completed = subprocess.run(
            [str(binary), str(CDC_EXTENSION), str(lake), str(data)],
            cwd=REPO,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        print(completed.stdout, end="")
        return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
