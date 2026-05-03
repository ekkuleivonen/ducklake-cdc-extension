"""Stream demo DuckLake CDC changes to a sink and print a summary on exit.

The demo consumer keeps production-shaped defaults while exposing a few
benchmark controls for attaching to running workloads and stressing fan-out.

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
one catalog-level :class:`DDLConsumer` plus, by default, one
:class:`DMLConsumer` per created table in a single :class:`CDCApp`. Use
``--consumers-per-table`` to spawn multiple independent consumers for each
table when testing duplicate fan-out load.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import threading
import time
from collections.abc import Callable, Sequence
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

from ducklake import DuckLakeError
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
TABLE_SPAWN_RETRY_INTERVAL_S = 0.5
TABLE_SPAWN_MAX_ATTEMPTS = 60
TABLE_SPAWN_MAX_WORKERS = 4
PROGRESS_INTERVAL_S = 5.0
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
    consumer_index: int
    consumers_per_table: int


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


class _TimedDMLConsumer(DMLConsumer):
    """Demo-only DML consumer that records pipeline timing by stage."""

    def __init__(
        self,
        *args: Any,
        stats: DemoStats,
        fixed_max_snapshots: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._demo_stats = stats
        self._fixed_max_snapshots = fixed_max_snapshots

    def _listen_op(self, timeout_ms: int, max_snapshots: int) -> Callable[[], list[Any]]:
        effective_max_snapshots = self._fixed_max_snapshots or max_snapshots
        operation = super()._listen_op(timeout_ms, effective_max_snapshots)

        def timed_operation() -> list[Any]:
            start_ns = time.monotonic_ns()
            rows = operation()
            self._demo_stats.record_dml_listen(
                elapsed_ms=_elapsed_ms_since(start_ns),
                row_count=len(rows),
                max_snapshots=effective_max_snapshots,
            )
            return rows

        return timed_operation

    def _build_batch(self, rows: list[Any]) -> DMLBatch:
        start_ns = time.monotonic_ns()
        batch = super()._build_batch(rows)
        self._demo_stats.record_dml_build_batch(
            elapsed_ms=_elapsed_ms_since(start_ns),
            snapshot_span=max(1, batch.end_snapshot - batch.start_snapshot + 1),
        )
        return batch

    def _deliver(self, batch: Any) -> None:
        start_ns = time.monotonic_ns()
        try:
            super()._deliver(batch)
        finally:
            self._demo_stats.record_dml_sink(elapsed_ms=_elapsed_ms_since(start_ns))

    def _commit_op(self, snapshot: int) -> Callable[[], object]:
        operation = super()._commit_op(snapshot)

        def timed_operation() -> object:
            start_ns = time.monotonic_ns()
            try:
                return operation()
            finally:
                self._demo_stats.record_dml_commit_duration(
                    elapsed_ms=_elapsed_ms_since(start_ns)
                )

        return timed_operation


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
        self._active_consumers: set[str] = set()
        self._pending_consumers: set[str] = set()
        self._seen_lock = threading.Lock()
        self._queue: queue.Queue[_SpawnRequest | None] = queue.Queue()
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []

    def open(self) -> None:
        self._stop_event.clear()
        worker_count = _spawn_worker_count(
            self._args.consumers_per_table,
            self._args.table_spawn_workers,
        )
        self._threads = [
            threading.Thread(
                target=self._run_spawn_loop,
                name=f"demo-table-spawner-{index + 1}",
                daemon=True,
            )
            for index in range(worker_count)
        ]
        for thread in self._threads:
            thread.start()

    def close(self) -> None:
        self._stop_event.set()
        for _ in self._threads:
            self._queue.put(None)
        for thread in self._threads:
            thread.join(timeout=1.0)

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
                table_id=table_id,
                table_name=table_name,
                start_at=change.snapshot_id,
            )

    def enqueue_table(
        self,
        *,
        table_id: int | None,
        table_name: str | None,
        start_at: str | int,
    ) -> int:
        enqueued = 0
        for consumer_index in range(self._args.consumers_per_table):
            request = _SpawnRequest(
                table_id=table_id,
                table_name=table_name,
                start_at=start_at,
                consumer_index=consumer_index,
                consumers_per_table=self._args.consumers_per_table,
            )
            consumer_name = _consumer_name_for_request(request)
            if not self._try_reserve(consumer_name):
                continue
            self._queue.put(request)
            enqueued += 1
        return enqueued

    def add_table(
        self,
        *,
        table_id: int | None,
        table_name: str | None,
        start_at: str | int,
    ) -> bool:
        added = False
        for consumer_index in range(self._args.consumers_per_table):
            request = _SpawnRequest(
                table_id=table_id,
                table_name=table_name,
                start_at=start_at,
                consumer_index=consumer_index,
                consumers_per_table=self._args.consumers_per_table,
            )
            consumer_name = _consumer_name_for_request(request)
            if not self._try_reserve(consumer_name):
                continue
            try:
                self._add_table_consumer(request)
            except Exception:
                self._release_reservation(consumer_name)
                raise
            self._mark_active(consumer_name)
            added = True
        return added

    def _run_spawn_loop(self) -> None:
        while True:
            request = self._queue.get()
            if request is None:
                return
            if self._stop_event.is_set():
                self._release_reservation(_consumer_name_for_request(request))
                return
            self._spawn_with_retries(request)

    def _spawn_with_retries(self, request: _SpawnRequest) -> None:
        consumer_name = _consumer_name_for_request(request)
        label = _spawn_label(request)
        for attempt in range(1, TABLE_SPAWN_MAX_ATTEMPTS + 1):
            if self._stop_event.is_set():
                self._release_reservation(consumer_name)
                return
            try:
                self._add_table_consumer(request)
            except DuckLakeError as exc:
                if attempt == 1 or attempt % 10 == 0:
                    print(
                        "demo consumer: waiting to start DML consumer for "
                        f"{label} (attempt {attempt}): {_exception_summary(exc)}",
                        flush=True,
                    )
                time.sleep(TABLE_SPAWN_RETRY_INTERVAL_S)
                continue
            except Exception as exc:
                self._release_reservation(consumer_name)
                self._stats.record_error(exc)
                print(
                    "demo consumer: failed to start DML consumer for "
                    f"{label}: {_exception_summary(exc)}",
                    flush=True,
                )
                return

            self._mark_active(consumer_name)
            return

        self._release_reservation(consumer_name)
        self._stats.record_error("TableSpawnTimeout")
        print(
            "demo consumer: timed out starting DML consumer for "
            f"{label} after {TABLE_SPAWN_MAX_ATTEMPTS} attempts",
            flush=True,
        )

    def _add_table_consumer(self, request: _SpawnRequest) -> None:
        lake = _open_lake(self._args)
        try:
            lake.load_extension(path=_local_extension_path())
            table_filter = (
                {"table_id": request.table_id}
                if request.table_id is not None
                else {"table": _require_table_name(request.table_name)}
            )
            consumer = _TimedDMLConsumer(
                lake,
                _consumer_name_for_request(request),
                start_at=request.start_at,
                on_exists="replace",
                sinks=[_StatsSink(self._stats)],
                retry=retry_on_lock,
                stats=self._stats,
                fixed_max_snapshots=(
                    getattr(self._args, "max_snapshots", 100)
                    if getattr(self._args, "fixed_max_snapshots", False)
                    else None
                ),
                **table_filter,
            )
            self._app.add_consumer(consumer)
        except Exception:
            lake.close()
            raise

        self._consumer_lakes.append(lake)
        print(
            "demo consumer: streaming "
            f"{_spawn_label(request)} from snapshot {request.start_at}",
            flush=True,
        )

    def _try_reserve(self, consumer_name: str) -> bool:
        with self._seen_lock:
            if (
                consumer_name in self._active_consumers
                or consumer_name in self._pending_consumers
            ):
                return False
            self._pending_consumers.add(consumer_name)
            return True

    def _mark_active(self, consumer_name: str) -> None:
        with self._seen_lock:
            self._pending_consumers.discard(consumer_name)
            self._active_consumers.add(consumer_name)

    def _release_reservation(self, consumer_name: str) -> None:
        with self._seen_lock:
            self._pending_consumers.discard(consumer_name)


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
    progress_stop = threading.Event()
    progress_thread: threading.Thread | None = None

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
                max_snapshots=args.max_snapshots,
                shutdown_timeout=2.0,
            )
            progress_thread = threading.Thread(
                target=_report_progress,
                args=(stats, progress_stop),
                name="demo-consumer-progress",
                daemon=True,
            )
            progress_thread.start()
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
        progress_stop.set()
        if progress_thread is not None:
            progress_thread.join(timeout=1.0)
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


def _report_progress(stats: DemoStats, stop_event: threading.Event) -> None:
    while not stop_event.wait(PROGRESS_INTERVAL_S):
        snapshot = stats.progress_snapshot()
        print(
            "demo consumer: progress "
            f"{snapshot['consumed_changes']} changes, "
            f"{snapshot['delivered_batches']} batches, "
            f"{snapshot['table_count_seen']} table(s), "
            f"{snapshot['consumer_count_seen']} active consumer(s), "
            f"{snapshot['consumed_changes_per_second']:.0f} changes/s, "
            f"{snapshot['error_count']} error(s)",
            flush=True,
        )


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


def _elapsed_ms_since(start_ns: int) -> float:
    return (time.monotonic_ns() - start_ns) / 1_000_000.0


def _spawn_worker_count(consumers_per_table: int, requested_workers: int) -> int:
    return max(1, min(consumers_per_table, requested_workers))


def _exception_summary(exc: BaseException) -> str:
    parts = [f"{type(exc).__name__}: {exc}"]
    current = exc.__cause__
    while current is not None:
        parts.append(f"caused by {type(current).__name__}: {current}")
        current = current.__cause__
    return " | ".join(parts)


def _spawn_label(request: _SpawnRequest) -> str:
    label = request.table_name or f"table_id={request.table_id}"
    if request.consumers_per_table > 1:
        label = f"{label} consumer {request.consumer_index + 1}/{request.consumers_per_table}"
    return label


def _consumer_name_for_request(request: _SpawnRequest) -> str:
    return _consumer_name_for_table(
        table_id=request.table_id,
        table_name=request.table_name,
        consumer_index=request.consumer_index,
        consumers_per_table=request.consumers_per_table,
    )


def _consumer_name_for_table(
    *,
    table_id: int | None,
    table_name: str | None,
    consumer_index: int = 0,
    consumers_per_table: int = 1,
) -> str:
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
    suffix = (
        f"__consumer_{consumer_index + 1:02d}" if consumers_per_table > 1 else ""
    )
    return f"{CONSUMER_NAME_PREFIX}__{safe}{suffix}"


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
    parser.add_argument(
        "--consumers-per-table",
        type=positive_int,
        default=1,
        help=(
            "number of independent DML consumers to spawn for each table. "
            "Values >1 duplicate delivery for fan-out stress testing."
        ),
    )
    parser.add_argument(
        "--table-spawn-workers",
        type=positive_int,
        default=TABLE_SPAWN_MAX_WORKERS,
        help=(
            "maximum concurrent table-consumer startup workers. Lower this to "
            "reduce connection-open bursts when many tables appear at once."
        ),
    )
    parser.add_argument(
        "--max-snapshots",
        type=positive_int,
        default=100,
        help=(
            "maximum snapshots a listen call may coalesce. This is the "
            "adaptive ceiling unless --fixed-max-snapshots is also set."
        ),
    )
    parser.add_argument(
        "--fixed-max-snapshots",
        action="store_true",
        help=(
            "bypass the Python DML adaptive window and request --max-snapshots "
            "on every DML listen call"
        ),
    )
    return parser.parse_args(argv)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


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
