"""Smoke test for owner-token lease behavior across two DuckDB connections.

SQLLogicTest only gives this extension one connection, but the CDC lease
contract is explicitly about rejecting a second connection while the first
connection owns a consumer. This script compiles a tiny C++ harness against the
locally-built libduckdb, opens two `duckdb::Connection` handles in one
process, and asserts:

1. connection A acquires the lease via `cdc_window`
2. connection B gets `CDC_BUSY` for the same consumer
3. connection B force-releases the lease
4. connection A's later `cdc_commit` gets `CDC_BUSY`

Usage:

    uv run python test/smoke/lease_multiconn_smoke.py

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

#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

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
	RequireOk(a, "LOAD ducklake");
	RequireOk(a, "LOAD parquet");
	RequireOk(a, "LOAD " + QuotePath(cdc_extension));

	const auto attach = "ATTACH 'ducklake:" + lake_path + "' AS lake (DATA_PATH '" + data_path + "')";
	RequireOk(a, attach);
	auto b_has_lake = RequireOk(b, "SELECT count(*) FROM duckdb_databases() WHERE database_name = 'lake'");
	if (b_has_lake->Cast<MaterializedQueryResult>().GetValue(0, 0).GetValue<int64_t>() == 0) {
		RequireOk(b, attach);
	}

	RequireOk(a, "CREATE TABLE lake.multi_conn_probe(id INTEGER)");
	RequireOk(a, "SELECT * FROM cdc_consumer_create('lake', 'multi_conn')");
	RequireOk(a, "INSERT INTO lake.multi_conn_probe VALUES (1)");
	auto window = RequireOk(a, "SELECT * FROM cdc_window('lake', 'multi_conn')");
	auto &window_result = window->Cast<MaterializedQueryResult>();
	const auto end_snapshot = window_result.GetValue(1, 0).GetValue<int64_t>();
	if (!window_result.GetValue(2, 0).GetValue<bool>()) {
		throw std::runtime_error("expected non-empty cdc_window in connection A");
	}

	RequireError(b, "SELECT * FROM cdc_window('lake', 'multi_conn')", "CDC_BUSY");
	RequireOk(b, "SELECT * FROM cdc_consumer_force_release('lake', 'multi_conn')");
	RequireError(a, "SELECT * FROM cdc_commit('lake', 'multi_conn', " + std::to_string(end_snapshot) + ")", "CDC_BUSY");

	std::cout << "lease_multiconn_smoke PASSED\n";
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

    with tempfile.TemporaryDirectory(prefix="ducklake_cdc_multiconn_") as tmp:
        tmpdir = Path(tmp)
        source = tmpdir / "lease_multiconn_smoke.cpp"
        binary = tmpdir / "lease_multiconn_smoke"
        lake = tmpdir / "multi.ducklake"
        data = tmpdir / "multi_data"
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
