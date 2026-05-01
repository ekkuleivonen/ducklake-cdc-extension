"""Stream demo DuckLake CDC changes to stdout."""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Sequence
from datetime import date, datetime
from functools import partial
from pathlib import Path
from typing import Any

from analytics import DemoStats, summary_table, timed_call
from common import CATALOG_ENV, STORAGE_ENV, WORK_DIR, open_demo_lake, retry_on_lock

from ducklake import DuckLake
from ducklake_cdc import CDCClient, ChangeRow, ConsumerCommit, Subscription

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
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    lake = open_demo_lake(
        allow_unsigned_extensions=True,
        catalog=args.catalog,
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
            storage=args.storage,
        )
        wait_cdc = CDCClient(wait_lake)
        wait_cdc.load_extension(path=_local_extension_path())

        print_json({"type": "consumer_ready", "consumer": CONSUMER_NAME})
        stream_changes(
            cdc,
            wait_cdc,
            lake,
            timeout_ms=args.timeout_ms,
            max_snapshots=args.max_snapshots,
            max_windows=args.max_windows,
            idle_timeout=args.idle_timeout,
            stats=stats,
        )
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
        help=f"DuckLake catalog URL; defaults to ${CATALOG_ENV} or local SQLite",
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
) -> None:
    committed_windows = 0
    last_activity = time.monotonic()
    while True:
        wait = timed_call(
            stats,
            "cdc_wait",
            lambda: retry_on_lock(lambda: wait_cdc.wait(CONSUMER_NAME, timeout_ms=timeout_ms)),
        )
        if stats is not None:
            stats.record_wait(has_snapshot=wait.snapshot_id is not None)
        if wait.snapshot_id is None:
            if idle_timeout and time.monotonic() - last_activity >= idle_timeout:
                return
            continue
        last_activity = time.monotonic()

        window = timed_call(
            stats,
            "cdc_window",
            lambda: retry_on_lock(
                lambda: cdc.window(CONSUMER_NAME, max_snapshots=max_snapshots)
            ),
        )
        if stats is not None:
            stats.record_window(has_changes=window.has_changes)
        if not window.has_changes:
            time.sleep(0.05)
            continue

        print_json({"type": "window", **window.model_dump(mode="json")})

        ddl_events = timed_call(
            stats,
            "cdc_ddl",
            lambda: retry_on_lock(lambda: cdc.ddl(CONSUMER_NAME, max_snapshots=max_snapshots)),
        )
        if stats is not None:
            stats.ddl_events += len(ddl_events)
        for ddl in ddl_events:
            print_json({"type": "ddl", **ddl.model_dump(mode="json")})

        snapshot_events = timed_call(
            stats,
            "cdc_events",
            lambda: retry_on_lock(lambda: cdc.events(CONSUMER_NAME, max_snapshots=max_snapshots)),
        )
        if stats is not None:
            stats.snapshot_events += len(snapshot_events)
        for event in snapshot_events:
            print_json({"type": "event", **event.model_dump(mode="json")})

        for table in retry_on_lock(lake.tables):
            table_name = f"{table.schema_name}.{table.name}"
            read_operation = partial(
                read_changes_retry,
                cdc,
                table_name=table_name,
                max_snapshots=max_snapshots,
            )
            changes = timed_call(
                stats,
                "cdc_changes",
                read_operation,
            )
            consumed_ns = time.monotonic_ns()
            if stats is not None:
                stats.record_changes(len(changes))
            for change in changes:
                if stats is not None:
                    stats.record_latency(change.values.get("produced_ns"), consumed_ns=consumed_ns)
                print_json(
                    {
                        "type": "change",
                        "table": table_name,
                        **change.model_dump(mode="json"),
                    }
                )

        end_snapshot = window.end_snapshot
        commit_operation = partial(commit_window_retry, cdc, end_snapshot)
        timed_call(
            stats,
            "cdc_commit",
            commit_operation,
        )
        if stats is not None:
            stats.record_commit()
        print_json({"type": "commit", "snapshot": end_snapshot})
        committed_windows += 1
        if max_windows and committed_windows >= max_windows:
            return


def read_changes(
    cdc: CDCClient,
    *,
    table_name: str,
    max_snapshots: int,
) -> list[ChangeRow]:
    return cdc.changes_rows(
        CONSUMER_NAME,
        table_name=table_name,
        max_snapshots=max_snapshots,
    )


def commit_window(cdc: CDCClient, snapshot: int) -> ConsumerCommit:
    return cdc.commit(CONSUMER_NAME, snapshot)


def read_changes_retry(
    cdc: CDCClient,
    *,
    table_name: str,
    max_snapshots: int,
) -> list[ChangeRow]:
    return retry_on_lock(
        partial(
            read_changes,
            cdc,
            table_name=table_name,
            max_snapshots=max_snapshots,
        )
    )


def commit_window_retry(cdc: CDCClient, snapshot: int) -> ConsumerCommit:
    return retry_on_lock(partial(commit_window, cdc, snapshot))


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
