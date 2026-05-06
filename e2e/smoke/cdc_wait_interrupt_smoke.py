"""Smoke test for cdc_dml_ticks_listen interrupt handling.

`cdc_dml_ticks_listen` blocks for up to `timeout_ms` (capped at 5 minutes). The
honest exit gate for Phase 1 is that DuckDB's interrupt mechanism
returns control to the caller in well under a second — the polling
backoff is bounded at 10s but the interrupt check sits at the top of
each loop iteration, so a `Connection::Interrupt()` from another thread
must wake the wait quickly enough that operators can stop a runaway
long-poll without restarting the process.

This script compiles a tiny C++ harness that:

1. starts `cdc_dml_ticks_listen('lake', 'iw_consumer', timeout_ms => 60000)` on
   connection A in a worker thread;
2. lets the wait spin for ~200ms so we are demonstrably mid-poll;
3. calls `a.Interrupt()` from the main thread;
4. asserts that the wait returns within `WAIT_DEADLINE_MS` and that
   the runtime carries DuckDB's interrupt error string.

Usage:

    uv run python e2e/smoke/cdc_wait_interrupt_smoke.py

Defaults to the release build because the harness loads the official prebuilt
DuckLake extension binary (no ASAN runtime), which conflicts with our debug
build's ASAN. Build it via `make release` first, or override with
`DUCKLAKE_CDC_BUILD=debug` if you have built debug without sanitizers
(`DISABLE_SANITIZER=1 make debug`).
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

_SMOKE_DIR = Path(__file__).resolve().parent
if str(_SMOKE_DIR) not in sys.path:
    sys.path.insert(0, str(_SMOKE_DIR))

from _harness_env import (  # noqa: E402
    BUILD,
    CDC_EXTENSION,
    DUCKDB_INCLUDE,
    DUCKLAKE_EXTENSION,
    LIBDUCKDB,
    LIBDUCKDB_DIR,
    REPO,
)

HARNESS = r"""
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
// poll iteration and the backoff doubles from 50ms to a 10s cap. A
// 3000ms deadline catches even a worst-case "interrupt landed mid
// 1.6s sleep" run while still failing fast on a real regression.
static constexpr int64_t WAIT_DEADLINE_MS = 3000;

// Return the concrete `MaterializedQueryResult` rather than the polymorphic
// `QueryResult` base. `Connection::Query` already returns a
// `unique_ptr<MaterializedQueryResult>`; macOS clang implicit-converts that
// to a `unique_ptr<QueryResult>` on return, but Linux GCC refuses, and
// hand-rolling `std::move` plus `unique_ptr_cast` here just adds noise.
unique_ptr<MaterializedQueryResult> RequireOk(Connection &conn, const std::string &sql) {
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
	RequireOk(a, "SELECT * FROM cdc_dml_consumer_create('lake', 'iw_consumer', table_name := 'iw')");

	std::atomic<bool> wait_started{false};
	std::atomic<bool> wait_completed{false};
	std::string wait_error;
	std::chrono::steady_clock::time_point wait_start;
	std::chrono::steady_clock::time_point wait_end;

	std::thread waiter([&]() {
		wait_start = std::chrono::steady_clock::now();
		wait_started.store(true, std::memory_order_release);
		auto result = a.Query("SELECT * FROM cdc_dml_ticks_listen('lake', 'iw_consumer', timeout_ms => 60000)");
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
			std::cerr << "cdc_dml_ticks_listen did not return within " << WAIT_DEADLINE_MS << "ms after Interrupt()" << "\n";
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
		std::cerr << "expected cdc_dml_ticks_listen to surface an interrupt error; got success after " << elapsed_ms
		          << "ms\n";
		return 1;
	}
	if (wait_error.find("INTERRUPT") == std::string::npos &&
	    wait_error.find("Interrupted") == std::string::npos &&
	    wait_error.find("interrupted") == std::string::npos) {
		std::cerr << "expected interrupt-shaped error from cdc_dml_ticks_listen; got: " << wait_error << "\n";
		return 1;
	}

	std::cout << "cdc_wait_interrupt_smoke PASSED (interrupt -> return in " << elapsed_ms << "ms)\n";
	return 0;
}
"""


def main() -> int:
    if not LIBDUCKDB.exists():
        print(f"missing {LIBDUCKDB}; run `make {BUILD}` first", file=sys.stderr)
        return 1
    if not CDC_EXTENSION.exists():
        print(
            f"missing ducklake_cdc {BUILD} artifact; run `make {BUILD}` first",
            file=sys.stderr,
        )
        return 1
    if not DUCKLAKE_EXTENSION.exists():
        print(
            f"missing {DUCKLAKE_EXTENSION}; run `make prepare_tests` first",
            file=sys.stderr,
        )
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
            [
                str(binary),
                str(DUCKLAKE_EXTENSION),
                str(CDC_EXTENSION),
                str(lake),
                str(data),
            ],
            cwd=REPO,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        print(completed.stdout, end="")
        return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
