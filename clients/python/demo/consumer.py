"""Stream demo DuckLake CDC changes to a sink and print a summary on exit.

The demo consumer is intentionally knob-free. The library's job is to
absorb whatever the producer throws at it efficiently with sensible
defaults; if a knob would be needed in production to make this work,
that's a library bug, not a demo flag.

Usage::

    # one terminal — start the consumer first. It resets the demo state,
    # then parks a DDL consumer that discovers producer-created tables.
    python demo/consumer.py

    # another terminal — run a workload of any shape.
    python demo/producer.py --inserts 2000 --duration 30

    # back in the consumer terminal: Ctrl+C to stop and see the summary.

    # If attaching to an already-running producer, use:
    python demo/consumer.py --no-reset

The consumer starts one DDL watcher, then hot-adds a DML consumer whenever
the producer creates a table. Each DML consumer starts at the table's DDL
snapshot, so what gets measured is live writes for every producer-created
table rather than a one-time startup table listing.

DML consumers are pinned to a single table by contract — see
``cdc_dml_consumer_create`` in the SQL extension. The demo therefore runs
one catalog-level :class:`DDLConsumer` plus one :class:`DMLConsumer` per
created table in a single :class:`CDCApp`.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
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

from ducklake_cdc import (
    BaseDDLSink,
    BaseDMLSink,
    CDCApp,
    DDLBatch,
    DDLConsumer,
    DdlEventKind,
    DdlObjectKind,
    DMLBatch,
    DMLConsumer,
    SinkContext,
)

CONSUMER_NAME_PREFIX = "demo"
DDL_CONSUMER_NAME = f"{CONSUMER_NAME_PREFIX}__ddl"
DEFAULT_CDC_EXTENSION = (
    Path(__file__).resolve().parents[3]
    / "build"
    / "release"
    / "extension"
    / "ducklake_cdc"
    / "ducklake_cdc.duckdb_extension"
)


@dataclass(frozen=True)
class _SpawnRequest:
    table_id: int | None
    table_name: str | None
    start_at: str | int


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
        self._stats.record_window(has_changes=bool(batch), observed_ns=consumed_ns)
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


class _TableSpawnSink(BaseDDLSink):
    """DDL sink that hot-adds DML consumers for newly-created tables."""

    name = "demo_table_spawner"

    def __init__(
        self,
        *,
        app: CDCApp,
        args: argparse.Namespace,
        stats: DemoStats,
        consumer_lakes: list[Any],
    ) -> None:
        self._app = app
        self._args = args
        self._stats = stats
        self._consumer_lakes = consumer_lakes
        self._seen_table_ids: set[int] = set()
        self._seen_table_names: set[str] = set()
        self._seen_lock = threading.Lock()
        self._queue: queue.Queue[_SpawnRequest | None] = queue.Queue()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def open(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_spawn_loop,
            name="demo-table-spawner",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        self._queue.put(None)
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        del ctx
        for change in batch:
            if (
                change.event_kind != DdlEventKind.CREATED
                or change.object_kind != DdlObjectKind.TABLE
            ):
                continue

            table_id = change.object_id
            table_name = _qualified_table_name(change.schema_name, change.object_name)
            self.enqueue_table(
                _SpawnRequest(
                    table_id=table_id,
                    table_name=table_name,
                    start_at=change.snapshot_id,
                )
            )

    def enqueue_table(self, request: _SpawnRequest) -> bool:
        if not self._try_mark_seen(
            table_id=request.table_id,
            table_name=request.table_name,
        ):
            return False
        self._queue.put(request)
        return True

    def add_table(
        self,
        *,
        table_id: int | None,
        table_name: str | None,
        start_at: str | int,
    ) -> bool:
        if not self._try_mark_seen(table_id=table_id, table_name=table_name):
            return False

        self._add_table_consumer(
            table_id=table_id,
            table_name=table_name,
            start_at=start_at,
        )
        return True

    def _run_spawn_loop(self) -> None:
        while True:
            request = self._queue.get()
            if request is None:
                return
            if self._stop_event.is_set():
                return
            try:
                self._add_table_consumer(
                    table_id=request.table_id,
                    table_name=request.table_name,
                    start_at=request.start_at,
                )
            except Exception as exc:
                self._stats.record_error(exc)
                print(
                    "demo consumer: failed to start DML consumer for "
                    f"{request.table_name or f'table_id={request.table_id}'}: {exc}",
                    flush=True,
                )

    def _add_table_consumer(
        self,
        *,
        table_id: int | None,
        table_name: str | None,
        start_at: str | int,
    ) -> None:
        lake = _open_lake(self._args)
        try:
            lake.load_extension(path=_local_extension_path())
            table_filter = (
                {"table_id": table_id}
                if table_id is not None
                else {"table": _require_table_name(table_name)}
            )
            consumer = DMLConsumer(
                lake,
                _consumer_name_for_table(table_id=table_id, table_name=table_name),
                start_at=start_at,
                on_exists="replace",
                sinks=[_StatsSink(self._stats)],
                retry=retry_on_lock,
                **table_filter,
            )
            self._app.add_consumer(consumer)
        except Exception:
            lake.close()
            raise

        self._consumer_lakes.append(lake)
        print(
            "demo consumer: streaming "
            f"{table_name or f'table_id={table_id}'} from snapshot {start_at}",
            flush=True,
        )

    def _try_mark_seen(self, *, table_id: int | None, table_name: str | None) -> bool:
        with self._seen_lock:
            if table_id is not None and table_id in self._seen_table_ids:
                return False
            if table_name is not None and table_name in self._seen_table_names:
                return False
            if table_id is not None:
                self._seen_table_ids.add(table_id)
            if table_name is not None:
                self._seen_table_names.add(table_name)
            return True

def main() -> None:
    args = parse_args()
    if not args.no_reset:
        reset_demo_state(
            catalog=args.catalog,
            catalog_backend=args.catalog_backend,
            storage=args.storage,
        )
    ddl_lake = _open_lake(args)
    stats = DemoStats()
    consumer_lakes: list[Any] = []
    app: CDCApp | None = None

    try:
        try:
            ddl_lake.load_extension(path=_local_extension_path())
            print(
                "demo consumer: watching for producer-created tables, "
                "press Ctrl+C to stop and see the summary",
                flush=True,
            )

            # ``listen_timeout_ms=200`` keeps the per-listen GIL window
            # short enough that the main thread can always service signal
            # handlers within ~200 ms. ``shutdown_timeout=2`` bounds how
            # long ``__exit__`` waits for the in-flight listen call to
            # complete before printing the summary; daemon threads clean
            # up on process exit.
            app = CDCApp(
                listen_timeout_ms=200,
                shutdown_timeout=2.0,
            )
            spawner = _TableSpawnSink(
                app=app,
                args=args,
                stats=stats,
                consumer_lakes=consumer_lakes,
            )
            if args.no_reset:
                existing = [
                    f"{table.schema_name}.{table.name}" for table in ddl_lake.tables()
                ]
                for table_name in existing:
                    spawner.add_table(
                        table_id=None,
                        table_name=table_name,
                        start_at="now",
                    )
                if existing:
                    print(
                        "demo consumer: attached to "
                        f"{len(existing)} existing table(s) at start_at='now'",
                        flush=True,
                    )
            app.add_consumer(
                DDLConsumer(
                    ddl_lake,
                    DDL_CONSUMER_NAME,
                    start_at="now",
                    on_exists="replace",
                    sinks=[spawner],
                    retry=retry_on_lock,
                )
            )

            with app:
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
        has_running_workers = (
            app is not None and any(health.running for health in app.stats())
        )
        if has_running_workers:
            print(
                "demo consumer: skipping explicit lake close because some "
                "consumer threads are still unwinding",
                flush=True,
            )
        else:
            for lake in consumer_lakes:
                try:
                    lake.close()
                except Exception:
                    pass
            try:
                ddl_lake.close()
            except Exception:
                pass
        stats.finish()
        emit_summary(stats, output=args.summary_output)


def _open_lake(args: argparse.Namespace) -> Any:
    return open_demo_lake(
        allow_unsigned_extensions=True,
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )


_CONSUMER_NAME_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _qualified_table_name(schema_name: str | None, object_name: str | None) -> str | None:
    if schema_name is None or object_name is None:
        return None
    return f"{schema_name}.{object_name}"


def _require_table_name(table_name: str | None) -> str:
    if table_name is None:
        raise ValueError("cannot create a demo DML consumer without table_id or name")
    return table_name


def _consumer_name_for_table(*, table_id: int | None, table_name: str | None) -> str:
    """Map a table identity to a deterministic, catalog-safe consumer name.

    The SQL extension stores consumer names as VARCHAR; dots in the
    qualified name are fine, but we sanitise to ``[A-Za-z0-9_]`` so that
    ``cdc_consumer_drop`` / lease-tooling examples in docs can quote the
    name without escaping.
    """

    identity = f"table_id_{table_id}" if table_id is not None else table_name
    if identity is None:
        raise ValueError("cannot create a demo DML consumer without table_id or name")
    safe = _CONSUMER_NAME_SAFE.sub("_", identity)
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
    parser.add_argument(
        "--no-reset",
        action="store_true",
        help=(
            "do not reset demo catalog/storage on startup; useful when attaching "
            "to a producer that is already running"
        ),
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
