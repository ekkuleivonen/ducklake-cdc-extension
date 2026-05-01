"""Stream demo DuckLake CDC changes to stdout."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterator, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from analytics import DemoStats, summary_table
from common import (
    CATALOG_ENV,
    DEFAULT_POSTGRES_CATALOG,
    STORAGE_ENV,
    open_demo_lake,
    retry_on_lock,
)

from ducklake import DuckLake
from ducklake_cdc import (
    CDCClient,
    ConsumerBatch,
    iter_consumer_batches,
)

CONSUMER_NAME = "stdout_demo"
DEFAULT_CDC_EXTENSION = (
    Path(__file__).resolve().parents[3]
    / "build"
    / "release"
    / "extension"
    / "ducklake_cdc"
    / "ducklake_cdc.duckdb_extension"
)


def main() -> None:
    args = parse_args()
    lake = open_demo_lake(
        allow_unsigned_extensions=True,
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )
    wait_lake: DuckLake | None = None
    stats = DemoStats()

    try:
        try:
            cdc = CDCClient(lake)
            cdc.load_extension(path=_local_extension_path())
            table_names = ensure_consumer(cdc, lake=lake, start_at=args.start_at)

            wait_lake = open_demo_lake(
                allow_unsigned_extensions=True,
                catalog=args.catalog,
                catalog_backend=args.catalog_backend,
                storage=args.storage,
            )
            wait_cdc = CDCClient(wait_lake)
            wait_cdc.load_extension(path=_local_extension_path())

            if args.output_mode == "stdout":
                print_json({"type": "consumer_ready", "consumer": CONSUMER_NAME})
            for batch in stream_changes(
                cdc,
                wait_cdc,
                table_names=table_names,
                timeout_ms=args.timeout_ms,
                max_snapshots=args.max_snapshots,
                max_windows=args.max_windows,
                idle_timeout=args.idle_timeout,
                stats=stats,
            ):
                if args.output_mode == "stdout":
                    print_batch(batch)
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            stats.record_error(exc)
            raise
    finally:
        if wait_lake is not None:
            wait_lake.close()
        lake.close()
        stats.finish()
        emit_summary(stats, output=args.summary_output)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-ms", type=int, default=1_000)
    parser.add_argument("--max-snapshots", type=int, default=100)
    parser.add_argument("--max-windows", type=int, default=0, help="0 means run forever")
    parser.add_argument(
        "--idle-timeout",
        type=non_negative_float,
        default=5.0,
        help="exit after this many seconds without a new snapshot; 0 disables",
    )
    parser.add_argument(
        "--analytics",
        action="store_true",
        help="kept for compatibility; metrics are always shown",
    )
    parser.add_argument(
        "--catalog",
        help=(
            f"DuckLake catalog URL; defaults to ${CATALOG_ENV} or "
            f"{DEFAULT_POSTGRES_CATALOG}"
        ),
    )
    parser.add_argument(
        "--catalog-backend",
        choices=("postgres", "sqlite"),
        help="demo catalog backend when --catalog and $DUCKLAKE_DEMO_CATALOG are unset",
    )
    parser.add_argument(
        "--storage",
        help=f"DuckLake storage path or URL; defaults to ${STORAGE_ENV} or demo/.work/demo_data",
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        help="write aggregate metrics JSON to this path instead of stdout",
    )
    parser.add_argument(
        "--output-mode",
        choices=("stdout", "none"),
        default="stdout",
        help="use 'none' to consume changes without per-row JSON output",
    )
    parser.add_argument(
        "--start-at",
        default="beginning",
        help="'now', 'beginning', 'oldest', or snapshot id",
    )
    return parser.parse_args(argv)


def ensure_consumer(cdc: CDCClient, *, lake: DuckLake, start_at: str) -> list[str]:
    table_names = [f"{table.schema_name}.{table.name}" for table in lake.tables()]
    if not table_names:
        raise RuntimeError(
            "No DuckLake tables found for the DML demo consumer. Run producer.py first, "
            "or start a DDL consumer that creates table-specific DML consumers after tables appear."
        )

    if consumer_exists(cdc, CONSUMER_NAME):
        cdc.consumer_force_release(CONSUMER_NAME)
        cdc.consumer_drop(CONSUMER_NAME)

    def create() -> None:
        cdc.dml_consumer_create(
            CONSUMER_NAME,
            table_names=table_names,
            start_at="now",
        )
        if start_at != "now":
            cdc.consumer_reset(CONSUMER_NAME, to_snapshot=parse_snapshot_arg(start_at))

    create()
    return table_names


def consumer_exists(cdc: CDCClient, name: str) -> bool:
    return any(consumer.consumer_name == name for consumer in cdc.consumer_list())


def parse_snapshot_arg(value: str) -> str | int:
    return int(value) if value.isdigit() else value


def stream_changes(
    cdc: CDCClient,
    wait_cdc: CDCClient,
    *,
    table_names: Sequence[str],
    timeout_ms: int,
    max_snapshots: int,
    max_windows: int,
    idle_timeout: float,
    stats: DemoStats | None,
) -> Iterator[ConsumerBatch]:
    return iter_consumer_batches(
        cdc,
        wait_cdc,
        CONSUMER_NAME,
        table_names=table_names,
        timeout_ms=timeout_ms,
        max_snapshots=max_snapshots,
        max_windows=max_windows,
        idle_timeout=idle_timeout,
        retry=retry_on_lock,
        stats=stats,
    )


def print_batch(batch: ConsumerBatch) -> None:
    print_json({"type": "window", **batch.window.model_dump(mode="json")})
    for ddl in batch.ddl_events:
        print_json({"type": "ddl", **ddl.model_dump(mode="json")})
    for event in batch.snapshot_events:
        print_json({"type": "event", **event.model_dump(mode="json")})
    for table in batch.table_changes:
        for change in table.changes:
            print_json(
                {
                    "type": "change",
                    "table": table.table_name,
                    **change.model_dump(mode="json"),
                }
            )
    print_json(
        {
            "type": "commit",
            "snapshot": batch.commit.committed_snapshot,
        }
    )


def emit_summary(stats: DemoStats, *, output: Path | None) -> None:
    summary = {"type": "summary", **stats.summary()}
    print(summary_table(summary), flush=True)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def print_json(value: dict[str, Any]) -> None:
    print(json.dumps(value, default=json_default, sort_keys=True), flush=True)


def json_default(value: object) -> str:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def _local_extension_path() -> Path:
    configured = os.environ.get("DUCKLAKE_CDC_EXTENSION")
    path = Path(configured).expanduser() if configured else DEFAULT_CDC_EXTENSION
    if not path.exists():
        raise SystemExit(
            "Local ducklake_cdc extension not found. Build it with `make release` "
            "or set DUCKLAKE_CDC_EXTENSION=/path/to/ducklake_cdc.duckdb_extension."
        )
    return path


if __name__ == "__main__":
    main()
