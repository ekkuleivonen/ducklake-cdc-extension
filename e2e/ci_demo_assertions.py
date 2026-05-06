"""Run polished e2e demos as CI gates.

The demos are user-facing examples, but they also prove important operating
properties. This runner keeps CI assertions in one place so TUI/readme wording
can evolve without breaking the gate.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent

DUCKDB_DEMOS = ("01", "02", "03", "04")
POSTGRES_DEMOS = ("05",)


@dataclass(frozen=True)
class DemoGate:
    id: str
    name: str
    app: str
    args: tuple[str, ...]
    timeout_s: int
    assert_summary: Callable[[dict[str, Any]], None]


class DemoAssertionError(AssertionError):
    pass


def _as_int(summary: dict[str, Any], key: str) -> int:
    return int(summary.get(key, 0) or 0)


def _as_float(summary: dict[str, Any], key: str) -> float:
    return float(summary.get(key, 0.0) or 0.0)


def _latency(summary: dict[str, Any], key: str) -> float:
    value = dict(summary.get("latency_ms") or {}).get(key)
    if value is None:
        return 0.0
    return float(value)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DemoAssertionError(message)


def assert_01(summary: dict[str, Any]) -> None:
    _require(_as_int(summary, "errors") == 0, "01 recorded errors")
    _require(summary.get("invariant") == "ok", "01 invariant did not pass")
    _require(_as_int(summary, "rows_processed") >= 8, "01 applied too few rows")
    _require(_as_int(summary, "migrations_applied") >= 1, "01 did not apply a schema migration")
    _require(_as_int(summary, "diff_rows") >= 1, "01 did not observe schema diff rows")


def assert_02(summary: dict[str, Any]) -> None:
    _require(_as_int(summary, "errors") == 0, "02 recorded errors")
    _require(summary.get("invariant") == "ok", "02 invariant did not pass")
    _require(_as_int(summary, "events_seen") == _as_int(summary, "events_total"), "02 did not process all source events")
    _require(_as_int(summary, "incremental_changes") > 0, "02 did not apply incremental changes")
    _require(float(summary.get("savings_pct") or 0.0) > 0.0, "02 did not report scan savings")


def assert_03(summary: dict[str, Any]) -> None:
    _require(_as_int(summary, "errors") == 0, "03 recorded errors")
    _require(summary.get("invariant") == "ok", "03 invariant did not pass")
    _require(_as_int(summary, "source_rows") == _as_int(summary, "sink_rows"), "03 source and mirror row counts differ")
    _require(_as_int(summary, "lag_peak") > 0, "03 never accumulated restart backlog")
    _require(_as_int(summary, "batches_applied") > 0, "03 did not apply any consumer batches")


def assert_04(summary: dict[str, Any]) -> None:
    _require(_as_int(summary, "errors") == 0, "04 recorded errors")
    producer_commits = _as_int(summary, "producer_commits")
    ticks = _as_int(summary, "ticks")
    _require(producer_commits >= 10, "04 produced too few commits")
    _require(producer_commits - ticks <= 1, "04 tick consumer fell behind producer commits")
    _require(_as_float(summary, "tick_rate") >= 2.0, "04 tick rate is below the CI floor")
    _require(_latency(summary, "p99") <= 2_000.0, "04 p99 tick latency is above the CI ceiling")


def assert_05(summary: dict[str, Any]) -> None:
    _require(_as_int(summary, "errors") == 0, "05 recorded errors")
    raw_purchases = _as_int(summary, "raw_purchases")
    clean_purchases = _as_int(summary, "clean_purchases")
    joined_orders = _as_int(summary, "joined_orders")
    raw_refunds = _as_int(summary, "raw_refunds")
    clean_refunds = _as_int(summary, "clean_refunds")
    _require(raw_purchases >= 1_000, "05 produced too few purchases")
    _require(raw_purchases == clean_purchases == joined_orders, "05 purchase branch did not drain cleanly")
    _require(raw_refunds == clean_refunds, "05 refund branch did not drain cleanly")
    _require(_as_float(summary, "throughput_rows_per_s") >= 100.0, "05 throughput is below the CI floor")
    _require(_latency(summary, "p99") <= 5_000.0, "05 p99 apply latency is above the CI ceiling")


GATES: dict[str, DemoGate] = {
    "01": DemoGate(
        "01",
        "schema-safe consumer",
        "e2e/01_schema_safe_consumer/app.py",
        ("--headless", "--json-summary", "--catalog", "duckdb"),
        90,
        assert_01,
    ),
    "02": DemoGate(
        "02",
        "incremental materialized view",
        "e2e/02_incremental_materialized_view/app.py",
        ("--headless", "--json-summary", "--catalog", "duckdb"),
        90,
        assert_02,
    ),
    "03": DemoGate(
        "03",
        "restart catchup",
        "e2e/03_backfill_live_catchup/app.py",
        ("--headless", "--json-summary", "--catalog", "duckdb"),
        90,
        assert_03,
    ),
    "04": DemoGate(
        "04",
        "cache/search refresh",
        "e2e/04_cache_refresh/app.py",
        ("--headless", "--json-summary", "--duration", "5", "--catalog", "duckdb"),
        60,
        assert_04,
    ),
    "05": DemoGate(
        "05",
        "pipeline DAG",
        "e2e/05_pipeline_dag/app.py",
        ("--headless", "--json-summary", "--duration", "5", "--catalog", "postgres"),
        90,
        assert_05,
    ),
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--demos",
        nargs="+",
        choices=tuple(GATES),
        default=tuple(GATES),
        help="Demo ids to run. Default: all.",
    )
    return parser.parse_args(argv)


def run_gate(gate: DemoGate) -> dict[str, Any]:
    cmd = [sys.executable, gate.app, *gate.args]
    print(f"::group::demo {gate.id} {gate.name}")
    print("$", " ".join(cmd))
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=gate.timeout_s,
        check=False,
    )
    print(result.stderr)
    if result.stdout.strip():
        print("stdout:")
        print(result.stdout)
    if result.returncode != 0:
        raise DemoAssertionError(f"demo {gate.id} exited with {result.returncode}")

    lines = [line for line in result.stdout.splitlines() if line.strip()]
    _require(bool(lines), f"demo {gate.id} did not emit JSON summary")
    try:
        summary = json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        raise DemoAssertionError(f"demo {gate.id} emitted invalid JSON summary: {lines[-1]!r}") from exc

    gate.assert_summary(summary)
    print("summary:", json.dumps(summary, sort_keys=True))
    print("::endgroup::")
    return summary


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    failures: list[str] = []
    for demo_id in args.demos:
        try:
            run_gate(GATES[demo_id])
        except Exception as exc:  # noqa: BLE001
            print(f"::error::demo {demo_id} failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            failures.append(demo_id)
            print("::endgroup::")

    if failures:
        print(f"failed demos: {', '.join(failures)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
