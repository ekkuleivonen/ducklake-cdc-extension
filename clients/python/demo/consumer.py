"""Stream demo DuckLake CDC changes to a sink and print a summary on exit.

The demo consumer is intentionally knob-free. The library's job is to
absorb whatever the producer throws at it efficiently with sensible
defaults; if a knob would be needed in production to make this work,
that's a library bug, not a demo flag.

Usage::

    # one terminal — start the consumer first. It resets the demo state,
    # then parks, polling the catalog until the producer creates tables.
    python demo/consumer.py

    # another terminal — run a workload of any shape.
    python demo/producer.py --inserts 2000 --duration 30

    # back in the consumer terminal: Ctrl+C to stop and see the summary.

The consumer attaches to each table at ``start_at="now"`` once tables
appear, so what gets measured is *real-time* CDC latency: the time from
a producer commit to the consumer's sink call. Run the producer with
``--duration`` so commits are spread over time and the consumer is
catching live writes rather than draining cold history.

DML consumers are pinned to a single table by contract — see
``cdc_dml_consumer_create`` in the SQL extension. The demo discovers
the lake's tables, builds one :class:`DMLConsumer` per table, and hands
the lot to a :class:`CDCApp` so all consumers run concurrently in one
process with shared signal handling.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from analytics import DemoStats, summary_table
from common import (
    CATALOG_ENV,
    DEFAULT_POSTGRES_CATALOG,
    STORAGE_ENV,
    open_demo_lake,
    retry_on_lock,
    reset_demo_state,
)

from ducklake_cdc import (
    BaseDMLSink,
    CDCApp,
    DMLBatch,
    DMLConsumer,
    SinkContext,
)

CONSUMER_NAME_PREFIX = "demo"
DEFAULT_CDC_EXTENSION = (
    Path(__file__).resolve().parents[3]
    / "build"
    / "release"
    / "extension"
    / "ducklake_cdc"
    / "ducklake_cdc.duckdb_extension"
)


class _StatsSink(BaseDMLSink):
    """Optional sink that drives :class:`DemoStats` off batches + per-change.

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
    reset_demo_state(
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )
    discovery_lake = _open_lake(args)
    stats = DemoStats()
    consumer_lakes: list[Any] = []

    try:
        try:
            discovery_lake.load_extension(path=_local_extension_path())
            print(
                "demo consumer: waiting for tables, "
                "press Ctrl+C to stop and see the summary",
                flush=True,
            )
            tables = _wait_for_tables(discovery_lake)
            print(
                f"demo consumer: streaming {len(tables)} table(s) at start_at='now'",
                flush=True,
            )

            # Each consumer gets its own DuckLake (and therefore its own
            # DuckDB connection) so concurrent ``cdc_dml_changes_listen``
            # calls do not contend for the same handle. The extension
            # explicitly recommends a dedicated connection per listener
            # (CDC_WAIT_SHARED_CONNECTION).
            consumers = []
            for table in tables:
                lake = _open_lake(args)
                lake.load_extension(path=_local_extension_path())
                consumer_lakes.append(lake)
                consumers.append(
                    DMLConsumer(
                        lake,
                        _consumer_name_for(table),
                        table=table,
                        start_at="now",
                        on_exists="replace",
                        sinks=[_StatsSink(stats)],
                        retry=retry_on_lock,
                    )
                )

            # ``listen_timeout_ms=200`` keeps the per-listen GIL window
            # short enough that the main thread can always service signal
            # handlers within ~200 ms. ``shutdown_timeout=2`` bounds how
            # long ``__exit__`` waits for the in-flight listen call to
            # complete before printing the summary; daemon threads clean
            # up on process exit.
            with CDCApp(
                consumers=consumers,
                listen_timeout_ms=200,
                shutdown_timeout=2.0,
            ) as app:
                try:
                    app.run(infinite=True)
                except KeyboardInterrupt:
                    pass
                # Harvest worker-thread errors that CDCApp swallowed so
                # crashes show up in the summary instead of disappearing
                # behind a "0 changes" line.
                for health in app.stats():
                    if health.last_error is not None:
                        stats.record_error(health.last_error)
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            stats.record_error(exc)
            raise
    finally:
        for lake in consumer_lakes:
            try:
                lake.close()
            except Exception:
                pass
        try:
            discovery_lake.close()
        except Exception:
            pass
        stats.finish()
        emit_summary(stats, output=args.summary_output)


_WAIT_FOR_TABLES_INTERVAL_S = 0.2


def _wait_for_tables(lake: Any) -> list[str]:
    """Poll the lake until at least one table is visible.

    The demo consumer is meant to be started before the producer so the
    sink can capture live writes (sub-second latency). Until the producer
    creates the first table we have nothing to subscribe to, so we just
    sit in a short poll loop. ``KeyboardInterrupt`` exits cleanly via the
    surrounding ``main`` handler.
    """

    while True:
        tables = [
            f"{table.schema_name}.{table.name}" for table in lake.tables()
        ]
        if tables:
            return tables
        time.sleep(_WAIT_FOR_TABLES_INTERVAL_S)


def _open_lake(args: argparse.Namespace) -> Any:
    return open_demo_lake(
        allow_unsigned_extensions=True,
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )


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
        help="write aggregate metrics JSON to this path in addition to stdout",
    )
    return parser.parse_args(argv)


def emit_summary(stats: DemoStats, *, output: Path | None) -> None:
    summary = {"type": "summary", **stats.summary()}
    print(summary_table(summary), flush=True)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


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
