"""Stream demo DuckLake CDC changes to stdout via the high-level Python API.

DML consumers are pinned to a single table by contract — see
``cdc_dml_consumer_create`` in the SQL extension. This demo therefore
spawns one :class:`DMLConsumer` per discovered table and processes them
in turn, sharing a single :class:`DemoStats` aggregator so the rendered
summary still reads as one end-to-end run.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections.abc import Sequence
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

from ducklake_cdc import (
    BaseDMLSink,
    DMLBatch,
    DMLConsumer,
    DMLSink,
    SinkContext,
    StdoutDMLSink,
)
from ducklake_cdc.lowlevel import CDCClient

CONSUMER_NAME_PREFIX = "stdout_demo"
DEFAULT_CDC_EXTENSION = (
    Path(__file__).resolve().parents[3]
    / "build"
    / "release"
    / "extension"
    / "ducklake_cdc"
    / "ducklake_cdc.duckdb_extension"
)


class _StatsSink(BaseDMLSink):
    """Optional sink that drives DemoStats off batches + per-change.

    ``require_ack=False`` so stats failures never gate delivery — the demo
    is observability, not part of the delivery contract.
    """

    name = "demo_stats"
    require_ack = False

    def __init__(self, stats: DemoStats) -> None:
        self._stats = stats

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        consumed_ns = time.monotonic_ns()
        consumed_epoch_ns = time.time_ns()
        self._stats.record_consumer(batch.consumer_name)
        self._stats.record_window(has_changes=bool(batch))
        self._stats.record_wait(has_snapshot=bool(batch))

        per_table: dict[str | None, int] = {}
        for change in batch:
            per_table[change.table] = per_table.get(change.table, 0) + 1
        self._stats.record_tables(len(per_table))
        for table_name, count in per_table.items():
            self._stats.record_changes(count, table_name=table_name)

        for change in batch:
            self._stats.record_change_observation(
                change_type=change.kind,
                table_name=change.table,
                values=change.values,
            )
            self._stats.record_change_latency(
                change_type=change.kind,
                produced_ns=change.values.get("produced_ns"),
                produced_epoch_ns=change.values.get("produced_epoch_ns"),
                snapshot_time=change.snapshot_time,
                consumed_ns=consumed_ns,
                consumed_epoch_ns=consumed_epoch_ns,
            )

        self._stats.record_commit()


def main() -> None:
    args = parse_args()
    lake = open_demo_lake(
        allow_unsigned_extensions=True,
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )
    stats = DemoStats()

    try:
        try:
            lake.load_extension(path=_local_extension_path())
            cdc = CDCClient(lake)

            tables = [f"{table.schema_name}.{table.name}" for table in lake.tables()]
            if not tables:
                raise RuntimeError(
                    "No DuckLake tables found for the DML demo consumer. "
                    "Run producer.py first."
                )

            start_at = parse_start_at(args.start_at)
            for table in tables:
                consumer_name = _consumer_name_for(table)
                sinks: list[DMLSink] = [_StatsSink(stats)]
                if args.output_mode == "stdout":
                    sinks.append(StdoutDMLSink())

                consumer = DMLConsumer(
                    lake,
                    consumer_name,
                    table=table,
                    start_at=start_at,
                    on_exists="replace",
                    sinks=sinks,
                    client=cdc,
                    retry=retry_on_lock,
                )

                with consumer:
                    if args.output_mode == "stdout":
                        print_json(
                            {
                                "type": "consumer_ready",
                                "consumer": consumer_name,
                                "table": table,
                            }
                        )
                    consumer.run(
                        infinite=True,
                        timeout_ms=args.timeout_ms,
                        max_snapshots=args.max_snapshots,
                        max_batches=args.max_batches,
                        idle_timeout=args.idle_timeout,
                    )
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            stats.record_error(exc)
            raise
    finally:
        lake.close()
        stats.finish()
        emit_summary(stats, output=args.summary_output)


_CONSUMER_NAME_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _consumer_name_for(qualified_table: str) -> str:
    """Map ``schema.table`` to a deterministic, catalog-safe consumer name.

    The SQL extension stores consumer names as VARCHAR; dots in the
    qualified name are fine, but we sanitise to ``[A-Za-z0-9_]`` so that
    ``cdc_consumer_drop`` / lease-tooling examples in docs can quote the
    name without escaping.
    """

    safe = _CONSUMER_NAME_SAFE.sub("_", qualified_table)
    return f"{CONSUMER_NAME_PREFIX}__{safe}"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=0,
        help="listen timeout in milliseconds; 0 performs a nonblocking catch-up poll",
    )
    parser.add_argument("--max-snapshots", type=int, default=100)
    parser.add_argument(
        "--max-batches",
        type=non_negative_int,
        default=1,
        help="number of non-empty batches to consume; 0 means run forever",
    )
    parser.add_argument(
        "--idle-timeout",
        type=non_negative_float,
        default=5.0,
        help="exit after this many seconds without a non-empty batch; 0 disables",
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


def parse_start_at(value: str) -> str | int:
    return int(value) if value.isdigit() else value


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


def non_negative_int(value: str) -> int:
    parsed = int(value)
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
