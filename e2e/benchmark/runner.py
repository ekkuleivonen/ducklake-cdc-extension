"""Run the DuckLake CDC e2e benchmark workload."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

BENCHMARK_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCHMARK_DIR.parents[1]
DEFAULT_WORKLOAD = BENCHMARK_DIR / "light.yaml"
DEFAULT_RESULTS_DIR = BENCHMARK_DIR / "results"
# Mirrors ``common.WORK_DIR``; redefined locally so the runner has no
# transitive import surface from the benchmark workers.
WORK_DIR = BENCHMARK_DIR / ".work"
PRODUCER_LOG_PATH = WORK_DIR / "producer.log"
DEFAULT_EXTENSION = (
    REPO_ROOT
    / "build"
    / "release"
    / "extension"
    / "ducklake_cdc"
    / "ducklake_cdc.duckdb_extension"
)


_PROFILE_CHOICES = ("flat", "ramp", "variate")


@dataclass(frozen=True)
class Workload:
    name: str
    duration_seconds: float
    schemas: int
    tables_per_schema: int
    target_snapshots_per_second: float
    target_rows_per_snapshot: int
    consumers_per_table: int
    producer_workers: int
    update_percent: float
    delete_percent: float
    batch_min: int
    batch_max: int
    max_snapshots: int
    # Inter-commit gap shape: ``flat`` (uniform), ``ramp`` (front-loaded
    # then thinning), ``variate`` (random jitter for bursty traffic).
    # Defaults to ``flat`` so existing workload yamls stay unaffected.
    profile: str = "flat"

    @property
    def table_count(self) -> int:
        return self.schemas * self.tables_per_schema

    @property
    def inserts_per_table(self) -> int:
        total_rows = round(
            self.duration_seconds
            * self.target_snapshots_per_second
            * self.target_rows_per_snapshot
        )
        return max(1, round(total_rows / self.table_count))


def main(argv: list[str] | None = None) -> int:
    # Load shared ``e2e/.env`` (the same file ``e2e/docker-compose.yml``
    # picks up) before we copy ``os.environ`` into the subprocess env,
    # so a single ``.env`` covers the postgres stack, the runner, and
    # both children. Idempotent with the auto-call at ``common`` import
    # time inside the children.
    from common import load_dotenv

    load_dotenv()

    args = parse_args(argv)
    workload = load_workload(args.workload)
    extension = args.cdc_extension or DEFAULT_EXTENSION
    if not extension.exists():
        raise SystemExit(
            "ducklake_cdc extension artifact not found; pass --cdc-extension "
            f"or build release first: {extension}"
        )

    result_path = args.output or DEFAULT_RESULTS_DIR / f"{workload.name}.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["DUCKLAKE_CDC_EXTENSION"] = str(extension)

    # Live mode: when our own stdout is a TTY the consumer activates its
    # alt-screen dashboard. We then need to keep the producer subprocess
    # off the screen and route its chatter into the dashboard's
    # ``producer`` panel via a shared log file. In CI / non-TTY callers
    # we want both children to stream straight to stdout so logs capture
    # everything interleaved.
    live_dashboard = sys.stdout.isatty()

    # ``python -u`` so subprocess prints aren't buffered for ages when
    # stdout is a pipe/file — critical for the producer-log tail in
    # live mode and for prompt CI logs.
    consumer_cmd = [
        sys.executable,
        "-u",
        str(BENCHMARK_DIR / "consumer.py"),
        "--summary-output",
        str(result_path),
        "--consumers-per-table",
        str(workload.consumers_per_table),
        "--max-snapshots",
        str(workload.max_snapshots),
    ]
    if args.catalog_backend:
        consumer_cmd.extend(["--catalog-backend", args.catalog_backend])
    if args.fixed_max_snapshots:
        consumer_cmd.append("--fixed-max-snapshots")
    if live_dashboard:
        consumer_cmd.extend(["--producer-log", str(PRODUCER_LOG_PATH)])

    producer_cmd = [
        sys.executable,
        "-u",
        str(BENCHMARK_DIR / "producer.py"),
        "--schemas",
        str(workload.schemas),
        "--tables",
        str(workload.tables_per_schema),
        "--inserts",
        str(workload.inserts_per_table),
        "--update",
        str(workload.update_percent),
        "--delete",
        str(workload.delete_percent),
        "--duration",
        str(workload.duration_seconds),
        "--batch_min",
        str(workload.batch_min),
        "--batch_max",
        str(workload.batch_max),
        "--workers",
        str(workload.producer_workers),
        "--profile",
        workload.profile,
    ]
    if args.catalog_backend:
        producer_cmd.extend(["--catalog-backend", args.catalog_backend])

    producer_log_fh = None
    if live_dashboard:
        PRODUCER_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        # Truncate so the dashboard panel starts each run with a clean
        # tail rather than reflecting whatever the previous run left
        # behind. ``_ProducerLogTail`` also handles truncation, but
        # truncating here avoids a brief one-frame flash of stale text.
        PRODUCER_LOG_PATH.write_bytes(b"")
        producer_log_fh = PRODUCER_LOG_PATH.open("ab", buffering=0)

    consumer = subprocess.Popen(consumer_cmd, cwd=REPO_ROOT, env=env)
    try:
        time.sleep(args.consumer_startup_seconds)
        subprocess.run(
            producer_cmd,
            cwd=REPO_ROOT,
            env=env,
            check=True,
            stdout=producer_log_fh if producer_log_fh is not None else None,
            stderr=subprocess.STDOUT if producer_log_fh is not None else None,
        )
    finally:
        if producer_log_fh is not None:
            producer_log_fh.close()
        stop_process(consumer, timeout=args.consumer_shutdown_seconds)

    if not result_path.exists():
        raise SystemExit(f"benchmark did not write summary output: {result_path}")
    print(f"benchmark result: {result_path}")
    return 0


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", type=Path, default=DEFAULT_WORKLOAD)
    parser.add_argument("--cdc-extension", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--catalog-backend",
        choices=("postgres", "sqlite"),
        default="postgres",
        help="catalog backend; postgres expects e2e/docker-compose.yml to be running",
    )
    parser.add_argument("--consumer-startup-seconds", type=float, default=2.0)
    parser.add_argument("--consumer-shutdown-seconds", type=float, default=10.0)
    parser.add_argument("--fixed-max-snapshots", action="store_true")
    return parser.parse_args(argv)


def load_workload(path: Path) -> Workload:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"workload must be a mapping: {path}")
    profile = str(data.get("profile", "flat"))
    if profile not in _PROFILE_CHOICES:
        raise ValueError(
            f"workload profile must be one of {_PROFILE_CHOICES}, got {profile!r}"
        )
    return Workload(
        name=str(data.get("name") or path.stem),
        duration_seconds=float(required(data, "duration_seconds")),
        schemas=int(required(data, "schemas")),
        tables_per_schema=int(required(data, "tables_per_schema")),
        target_snapshots_per_second=float(required(data, "target_snapshots_per_second")),
        target_rows_per_snapshot=int(required(data, "target_rows_per_snapshot")),
        consumers_per_table=int(data.get("consumers_per_table", 1)),
        producer_workers=int(data.get("producer_workers", 1)),
        update_percent=float(data.get("update_percent", 0.0)),
        delete_percent=float(data.get("delete_percent", 0.0)),
        batch_min=int(data.get("batch_min", 1)),
        batch_max=int(data.get("batch_max", data.get("batch_min", 1))),
        max_snapshots=int(data.get("max_snapshots", 100)),
        profile=profile,
    )


def required(data: dict[str, Any], key: str) -> Any:
    if key not in data:
        raise ValueError(f"workload is missing required field {key!r}")
    return data[key]


def stop_process(process: subprocess.Popen[Any], *, timeout: float) -> None:
    if process.poll() is not None:
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, process.args)
        return

    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    if process.returncode not in (0, -signal.SIGINT):
        raise subprocess.CalledProcessError(process.returncode, process.args)


if __name__ == "__main__":
    raise SystemExit(main())
