"""Smoke test for cdc_wait interrupt handling.

`cdc_wait` blocks for up to `timeout_ms` (capped at 5 minutes). The
honest exit gate for Phase 1 is that DuckDB's interrupt mechanism
returns control to the caller in well under a second — the polling
backoff is bounded at 10s but the interrupt check sits at the top of
each loop iteration, so a `Connection::Interrupt()` from another thread
must wake the wait quickly enough that operators can stop a runaway
long-poll without restarting the process.

This script compiles a tiny C++ harness that:

1. starts `cdc_wait('lake', 'iw_consumer', timeout_ms => 60000)` on
   connection A in a worker thread;
2. lets the wait spin for ~200ms so we are demonstrably mid-poll;
3. calls `a.Interrupt()` from the main thread;
4. asserts that the wait returns within `WAIT_DEADLINE_MS` and that
   the runtime carries DuckDB's interrupt error string.

Usage:

    cd test
    uv run python smoke/cdc_wait_interrupt_smoke.py

Run `make debug` first.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
DUCKDB_INCLUDE = REPO / "duckdb" / "src" / "include"
LIBDUCKDB_DIR = REPO / "build" / "debug" / "src"
LIBDUCKDB = LIBDUCKDB_DIR / ("libduckdb.dylib" if sys.platform == "darwin" else "libduckdb.so")
DUCKLAKE_EXTENSION = REPO / "build" / "debug" / "extension" / "ducklake" / "ducklake.duckdb_extension"
CDC_EXTENSION = REPO / "build" / "debug" / "extension" / "ducklake_cdc" / "ducklake_cdc.duckdb_extension"


HARNESS = r'''
#include "duckdb.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

using namespace duckdb;

// Conservative ceiling: the interrupt check sits at the top of each
// poll iteration and the backoff doubles from 100ms to a 10s cap. A
// 3000ms deadline catches even a worst-case "interrupt landed mid
// 1.6s sleep" run while still failing fast on a real regression.
static constexpr int64_t WAIT_DEADLINE_MS = 3000;

unique_ptr<QueryResult> RequireOk(Connection &conn, const std::string &sql) {
	auto result = conn.Query(sql);
	if (!result || result->HasError()) {
		throw std::runtime_error(sql + "\n" + (result ? result->GetError() : "no result"));
	}
	return result;
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
	if (argc != 5) {
		std::cerr << "usage: harness <ducklake-extension> <cdc-extension> <lake-path> <data-path>\n";
		return 2;
	}
	const std::string ducklake_extension = argv[1];
	const std::string cdc_extension = argv[2];
	const std::string lake_path = argv[3];
	const std::string data_path = argv[4];

	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", Value::BOOLEAN(true));
	DuckDB db(nullptr, &config);
	Connection a(db);

	RequireOk(a, "LOAD " + QuotePath(ducklake_extension));
	RequireOk(a, "LOAD parquet");
	RequireOk(a, "LOAD " + QuotePath(cdc_extension));

	const auto attach = "ATTACH 'ducklake:" + lake_path + "' AS lake (DATA_PATH '" + data_path + "')";
	RequireOk(a, attach);

	RequireOk(a, "CREATE TABLE lake.iw(id INTEGER)");
	RequireOk(a, "INSERT INTO lake.iw VALUES (1)");
	RequireOk(a, "SELECT * FROM cdc_consumer_create('lake', 'iw_consumer')");

	std::atomic<bool> wait_started{false};
	std::atomic<bool> wait_completed{false};
	std::string wait_error;
	std::chrono::steady_clock::time_point wait_start;
	std::chrono::steady_clock::time_point wait_end;

	std::thread waiter([&]() {
		wait_start = std::chrono::steady_clock::now();
		wait_started.store(true, std::memory_order_release);
		auto result = a.Query("SELECT * FROM cdc_wait('lake', 'iw_consumer', timeout_ms => 60000)");
		wait_end = std::chrono::steady_clock::now();
		if (result && result->HasError()) {
			wait_error = result->GetError();
		}
		wait_completed.store(true, std::memory_order_release);
	});

	while (!wait_started.load(std::memory_order_acquire)) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(200));

	const auto interrupt_at = std::chrono::steady_clock::now();
	a.Interrupt();

	while (!wait_completed.load(std::memory_order_acquire)) {
		const auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::milliseconds>(now - interrupt_at).count() > WAIT_DEADLINE_MS) {
			std::cerr << "cdc_wait did not return within " << WAIT_DEADLINE_MS << "ms after Interrupt()" << "\n";
			a.Interrupt();
			waiter.detach();
			return 1;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	waiter.join();

	const auto elapsed_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(wait_end - interrupt_at).count();
	if (wait_error.empty()) {
		std::cerr << "expected cdc_wait to surface an interrupt error; got success after " << elapsed_ms
		          << "ms\n";
		return 1;
	}
	if (wait_error.find("INTERRUPT") == std::string::npos &&
	    wait_error.find("Interrupted") == std::string::npos &&
	    wait_error.find("interrupted") == std::string::npos) {
		std::cerr << "expected interrupt-shaped error from cdc_wait; got: " << wait_error << "\n";
		return 1;
	}

	std::cout << "cdc_wait_interrupt_smoke PASSED (interrupt -> return in " << elapsed_ms << "ms)\n";
	return 0;
}
'''


def main() -> int:
    if not LIBDUCKDB.exists():
        print(f"missing {LIBDUCKDB}; run `make debug` first", file=sys.stderr)
        return 1
    if not DUCKLAKE_EXTENSION.exists() or not CDC_EXTENSION.exists():
        print("missing debug extension artifacts; run `make debug` first", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="ducklake_cdc_iw_") as tmp:
        tmpdir = Path(tmp)
        source = tmpdir / "cdc_wait_interrupt_smoke.cpp"
        binary = tmpdir / "cdc_wait_interrupt_smoke"
        lake = tmpdir / "iw.ducklake"
        data = tmpdir / "iw_data"
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
            [str(binary), str(DUCKLAKE_EXTENSION), str(CDC_EXTENSION), str(lake), str(data)],
            cwd=REPO,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        print(completed.stdout, end="")
        return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
