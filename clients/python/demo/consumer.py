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
    reset_demo_state,
    retry_on_lock,
)

from ducklake import DuckLake
from ducklake_cdc import CDCClient, ConsumerBatch, Subscription, iter_consumer_batches

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
    reset_demo_state(
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )
    lake = open_demo_lake(
        allow_unsigned_extensions=True,
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )
    wait_lake: DuckLake | None = None
    stats = DemoStats()

    try:
        cdc = CDCClient(lake)
        cdc.load_extension(path=_local_extension_path())
        ensure_consumer(cdc, start_at=args.start_at)

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
            lake,
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
        default="now",
        help="'now', 'beginning', 'oldest', or snapshot id",
    )
    return parser.parse_args(argv)


def ensure_consumer(cdc: CDCClient, *, start_at: str) -> None:
    subscription = Subscription.model_validate(
        {
            "scope_kind": "catalog",
            "event_category": "*",
            "change_type": "*",
        }
    )
    def create() -> None:
        cdc.consumer_create(
            CONSUMER_NAME,
            subscriptions=[subscription],
            start_at=int(start_at) if start_at.isdigit() else start_at,
            stop_at_schema_change=False,
        )

    try:
        create()
    except Exception:
        try:
            cdc.consumer_force_release(CONSUMER_NAME)
            cdc.consumer_drop(CONSUMER_NAME)
            create()
        except Exception:
            raise


def stream_changes(
    cdc: CDCClient,
    wait_cdc: CDCClient,
    lake: DuckLake,
    *,
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
        lake=lake,
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
