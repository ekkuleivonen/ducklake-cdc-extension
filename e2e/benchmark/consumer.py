"""Stream benchmark DuckLake CDC changes to a sink and print a summary on exit.

The benchmark consumer keeps production-shaped defaults while exposing a few
benchmark controls for attaching to running workloads and stressing fan-out.

Usage::

    # one terminal — start the consumer first. It resets the benchmark state,
    # then parks a DDL consumer that discovers producer-created tables.
    python e2e/benchmark/consumer.py

    # another terminal — run a workload of any shape.
    python e2e/benchmark/producer.py --inserts 2000 --duration 30

    # back in the consumer terminal: Ctrl+C to stop and see the summary.

    # If attaching to an already-running producer, use:
    python e2e/benchmark/consumer.py --no-reset

When stdout is a TTY the consumer renders a live dashboard (fixed-height
per-table panel + scrolling tail, designed for screen-recorded GIFs) and
restores the terminal on exit before printing the analytical summary
table. When stdout is piped or redirected, the dashboard auto-degrades to
no-op rendering so logs and CI runs are unaffected.

The consumer starts one DDL watcher, then hot-adds a DML consumer whenever
the producer creates a table. Each DML consumer starts at the table's DDL
snapshot, so what gets measured is live writes for every producer-created
table rather than a one-time startup table listing.

DML consumers are pinned to a single table by contract — see
``cdc_dml_consumer_create`` in the SQL extension. The benchmark therefore runs
one catalog-level :class:`DDLConsumer` plus, by default, one
:class:`DMLConsumer` per created table in a benchmark-local app. Use
``--consumers-per-table`` to spawn multiple independent consumers for each
table when testing duplicate fan-out load.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ducklake_cdc_client import (
    CDCClient,
    DDLConsumer,
    DdlEventKind,
    DdlObjectKind,
    SchemaChange,
)

from analytics import DemoStats, summary_table
from common import (
    CATALOG_ENV,
    DEFAULT_POSTGRES_CATALOG,
    STORAGE_ENV,
    WORK_DIR,
    load_benchmark_cdc_extension,
    open_demo_lake,
    reset_demo_state,
    retry_on_lock,
)
from constants import CONSUMER_NAME_PREFIX, DDL_CONSUMER_NAME, PROGRESS_INTERVAL_S
from sinks import DemoDashboard, DemoSink, StatsSink
from timed_dml_consumer import TimedDMLConsumer


@dataclass(frozen=True)
class ConsumerHealth:
    name: str
    running: bool
    delivered_batches: int
    last_error: str | None


class BenchmarkApp:
    """Small dynamic consumer host for the benchmark.

    The published client intentionally keeps dynamic topology out of its public
    API. The benchmark still needs to discover producer-created tables, so this
    host owns that orchestration locally while using only public consumers.
    """

    def __init__(
        self,
        *,
        listen_timeout_ms: int,
        max_snapshots: int,
        shutdown_timeout: float,
    ) -> None:
        self._listen_timeout_ms = listen_timeout_ms
        self._max_snapshots = max_snapshots
        self._shutdown_timeout = shutdown_timeout
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._workers: dict[str, _Worker] = {}
        self._opened = False

    def add_consumer(self, consumer: Any) -> None:
        worker: _Worker | None = None
        with self._lock:
            if consumer.name in self._workers:
                return
            worker = _Worker(consumer)
            self._workers[consumer.name] = worker
            should_start = self._opened
        if should_start:
            self._start_worker(worker)

    def stats(self) -> list[ConsumerHealth]:
        with self._lock:
            return [worker.health() for worker in self._workers.values()]

    def __enter__(self) -> BenchmarkApp:
        self._opened = True
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            self._start_worker(worker)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        self._opened = False
        self._stop_event.set()
        deadline = time.monotonic() + self._shutdown_timeout
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            remaining = max(0.0, deadline - time.monotonic())
            worker.join(timeout=remaining)
        for worker in workers:
            if not worker.running:
                worker.close()

    def run(self, *, infinite: bool = True) -> None:
        del infinite
        while not self._stop_event.is_set():
            with self._lock:
                workers = list(self._workers.values())
            if workers and all(not worker.running for worker in workers):
                return
            time.sleep(0.1)

    def _start_worker(self, worker: _Worker) -> None:
        worker.start(
            stop_event=self._stop_event,
            timeout_ms=self._listen_timeout_ms,
            max_snapshots=self._max_snapshots,
        )


class _Worker:
    def __init__(self, consumer: Any) -> None:
        self.consumer = consumer
        self.delivered_batches = 0
        self.last_error: BaseException | None = None
        self._thread: threading.Thread | None = None
        self._entered = False

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(
        self,
        *,
        stop_event: threading.Event,
        timeout_ms: int,
        max_snapshots: int,
    ) -> None:
        if self._thread is not None:
            return
        self.consumer.__enter__()
        self._entered = True
        self._thread = threading.Thread(
            target=self._run,
            args=(stop_event, timeout_ms, max_snapshots),
            name=f"benchmark-consumer:{self.consumer.name}",
            daemon=True,
        )
        self._thread.start()

    def join(self, *, timeout: float) -> None:
        if self._thread is not None:
            self._thread.join(timeout)

    def close(self) -> None:
        if self._entered:
            self.consumer.__exit__(None, None, None)
            self._entered = False

    def health(self) -> ConsumerHealth:
        return ConsumerHealth(
            name=self.consumer.name,
            running=self.running,
            delivered_batches=self.delivered_batches,
            last_error=type(self.last_error).__name__ if self.last_error is not None else None,
        )

    def _run(
        self,
        stop_event: threading.Event,
        timeout_ms: int,
        max_snapshots: int,
    ) -> None:
        try:
            self.delivered_batches = self.consumer.run(
                infinite=True,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
                stop_event=stop_event,
            )
        except BaseException as exc:
            self.last_error = exc


class ConsumerSpawner:
    name = "benchmark_consumer_spawner"
    required = True

    def __init__(self, *, app: BenchmarkApp, on_event: Any) -> None:
        self._app = app
        self._on_event = on_event

    def write(self, batch: Any, ctx: Any) -> None:
        del ctx
        for item in batch:
            consumers = self._on_event(item)
            if consumers is None:
                continue
            for consumer in consumers:
                self._app.add_consumer(consumer)


def main() -> None:
    args = parse_args()
    # The dashboard owns the screen when stdout is a TTY, so any
    # `print()` chatter while it's up would fight the layout. Run quiet
    # in that case; non-TTY callers (CI, pipes) keep the old chatty
    # behaviour.
    use_dashboard = sys.stdout.isatty()
    quiet = use_dashboard
    if not args.no_reset:
        reset_demo_state(
            catalog=args.catalog,
            catalog_backend=args.catalog_backend,
            storage=args.storage,
        )
    ddl_lake = _open_lake(args)
    stats = DemoStats()
    consumer_lakes: list[Any] = []
    app: BenchmarkApp | None = None
    progress_stop = threading.Event()
    progress_thread: threading.Thread | None = None
    dashboard: DemoDashboard | None = None
    if use_dashboard:
        # Construct the dashboard now (cheap) so the spawner hook can
        # attach a DemoSink to every consumer it creates. The actual
        # alt-screen / signal-handler activation only happens when
        # ``start()`` runs inside the ``with app:`` block below. The
        # dashboard reads from the same ``stats`` the analytical sink
        # writes into so the live stage-breakdown matches the final
        # summary.
        dashboard = DemoDashboard(
            log_path=WORK_DIR / "demo-dashboard.log",
            stats=stats,
        )

    try:
        try:
            load_benchmark_cdc_extension(ddl_lake)
            if not quiet:
                print(
                    "benchmark consumer: watching for producer-created tables, "
                    "press Ctrl+C to stop and see the summary",
                    flush=True,
                )

            # ``listen_timeout_ms=200`` keeps the per-listen GIL window
            # short enough that the main thread can always service signal
            # handlers within ~200 ms. ``shutdown_timeout=2`` bounds how
            # long ``__exit__`` waits for the in-flight listen call to
            # complete before printing the summary; daemon threads clean
            # up on process exit.
            app = BenchmarkApp(
                listen_timeout_ms=200,
                max_snapshots=args.max_snapshots,
                shutdown_timeout=2.0,
            )
            if not quiet:
                progress_thread = threading.Thread(
                    target=_report_progress,
                    args=(stats, progress_stop),
                    name="demo-consumer-progress",
                    daemon=True,
                )
                progress_thread.start()
            spawner = ConsumerSpawner(
                app=app,
                on_event=lambda change: _dml_consumers_for_created_table(
                    change,
                    args=args,
                    stats=stats,
                    consumer_lakes=consumer_lakes,
                    dashboard=dashboard,
                    quiet=quiet,
                ),
            )
            if args.no_reset:
                existing = [
                    f"{table.schema_name}.{table.table_name}"
                    for table in ddl_lake.table.list()
                ]
                for table_name in existing:
                    for consumer in _dml_consumers_for_table(
                        args=args,
                        stats=stats,
                        consumer_lakes=consumer_lakes,
                        dashboard=dashboard,
                        quiet=quiet,
                        table_id=None,
                        table_name=table_name,
                        start_at="now",
                    ):
                        app.add_consumer(consumer)
                if existing and not quiet:
                    print(
                        "benchmark consumer: attached to "
                        f"{len(existing)} existing table(s) at start_at='now'",
                        flush=True,
                    )
            app.add_consumer(
                DDLConsumer(
                    ddl_lake,
                    DDL_CONSUMER_NAME,
                    start_at="now",
                    mode="changes",
                    on_exists="replace",
                    sinks=[spawner],
                    client=CDCClient(ddl_lake, install_extension=False),
                    retry=retry_on_lock,
                )
            )

            with app:
                # Start the dashboard after consumers are running so Ctrl+C
                # restores the user's terminal before the benchmark drains.
                if dashboard is not None:
                    dashboard.start()
                try:
                    try:
                        app.run(infinite=True)
                    except KeyboardInterrupt:
                        pass
                    # Harvest worker-thread errors so
                    # crashes show up in the summary instead of disappearing
                    # behind a "0 changes" line.
                    for health in app.stats():
                        if health.last_error is not None:
                            stats.record_error(health.last_error)
                finally:
                    if dashboard is not None:
                        dashboard.stop()
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            stats.record_error(exc)
            raise
    finally:
        progress_stop.set()
        if progress_thread is not None:
            progress_thread.join(timeout=1.0)
        if dashboard is not None:
            dashboard.stop()
        has_running_workers = (
            app is not None and any(health.running for health in app.stats())
        )
        if has_running_workers:
            if not quiet:
                print(
                    "benchmark consumer: skipping explicit lake close because some "
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
            "benchmark consumer: progress "
            f"{snapshot['changes_total']} changes, "
            f"{snapshot['batches_total']} batches, "
            f"{snapshot['tables_seen']} table(s), "
            f"{snapshot['consumers']} active consumer(s), "
            f"{snapshot['changes_per_s']:.0f} changes/s, "
            f"{snapshot['errors']} error(s)",
            flush=True,
        )


def _open_lake(args: argparse.Namespace) -> Any:
    return open_demo_lake(
        allow_unsigned_extensions=True,
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )


@dataclass(frozen=True)
class _TableConsumerSpec:
    table_id: int | None
    table_name: str | None
    start_at: str | int
    consumer_index: int
    consumers_per_table: int


_CONSUMER_NAME_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _dml_consumers_for_created_table(
    change: SchemaChange,
    *,
    args: argparse.Namespace,
    stats: DemoStats,
    consumer_lakes: list[Any],
    dashboard: DemoDashboard | None,
    quiet: bool,
) -> list[TimedDMLConsumer] | None:
    if (
        change.event_kind != DdlEventKind.CREATED
        or change.object_kind != DdlObjectKind.TABLE
    ):
        return None
    return _dml_consumers_for_table(
        args=args,
        stats=stats,
        consumer_lakes=consumer_lakes,
        dashboard=dashboard,
        quiet=quiet,
        table_id=change.object_id,
        table_name=_qualified_table_name(change.schema_name, change.object_name),
        start_at=change.snapshot_id,
    )


def _dml_consumers_for_table(
    *,
    args: argparse.Namespace,
    stats: DemoStats,
    consumer_lakes: list[Any],
    dashboard: DemoDashboard | None,
    quiet: bool,
    table_id: int | None,
    table_name: str | None,
    start_at: str | int,
) -> list[TimedDMLConsumer]:
    consumers: list[TimedDMLConsumer] = []
    for consumer_index in range(args.consumers_per_table):
        spec = _TableConsumerSpec(
            table_id=table_id,
            table_name=table_name,
            start_at=start_at,
            consumer_index=consumer_index,
            consumers_per_table=args.consumers_per_table,
        )
        lake = _open_lake(args)
        try:
            load_benchmark_cdc_extension(lake)
            client = CDCClient(lake, install_extension=False)
            table_filter = (
                {"table_id": spec.table_id}
                if spec.table_id is not None
                else {"table": _require_table_name(spec.table_name)}
            )
            sinks: list[Any] = [StatsSink(stats)]
            if dashboard is not None:
                sinks.append(DemoSink(dashboard))
            consumers.append(
                TimedDMLConsumer(
                    lake,
                    _consumer_name_for_spec(spec),
                    start_at=spec.start_at,
                    mode="changes",
                    on_exists="error",
                    sinks=sinks,
                    client=client,
                    retry=retry_on_lock,
                    stats=stats,
                    fixed_max_snapshots=(
                        getattr(args, "max_snapshots", 100)
                        if getattr(args, "fixed_max_snapshots", False)
                        else None
                    ),
                    **table_filter,
                )
            )
        except Exception:
            lake.close()
            raise
        consumer_lakes.append(lake)
        if not quiet:
            print(
                "benchmark consumer: streaming "
                f"{_spawn_label(spec)} from snapshot {spec.start_at}",
                flush=True,
            )
    return consumers


def _qualified_table_name(schema_name: str | None, object_name: str | None) -> str | None:
    if schema_name is None or object_name is None:
        return None
    return f"{schema_name}.{object_name}"


def _require_table_name(table_name: str | None) -> str:
    if table_name is None:
        raise ValueError("cannot create a benchmark DML consumer without table_id or name")
    return table_name


def _spawn_label(spec: _TableConsumerSpec) -> str:
    label = spec.table_name or f"table_id={spec.table_id}"
    if spec.consumers_per_table > 1:
        label = f"{label} consumer {spec.consumer_index + 1}/{spec.consumers_per_table}"
    return label


def _consumer_name_for_spec(spec: _TableConsumerSpec) -> str:
    return _consumer_name_for_table(
        table_id=spec.table_id,
        table_name=spec.table_name,
        consumer_index=spec.consumer_index,
        consumers_per_table=spec.consumers_per_table,
    )


def _consumer_name_for_table(
    *,
    table_id: int | None,
    table_name: str | None,
    consumer_index: int = 0,
    consumers_per_table: int = 1,
) -> str:
    """Map a table identity to a deterministic, catalog-safe consumer name."""

    identity = f"table_id_{table_id}" if table_id is not None else table_name
    if identity is None:
        raise ValueError("cannot create a benchmark DML consumer without table_id or name")
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
        help=f"benchmark catalog backend when --catalog and ${CATALOG_ENV} are unset",
    )
    parser.add_argument(
        "--storage",
        help=f"DuckLake storage path or URL; defaults to ${STORAGE_ENV} or benchmark .work data",
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
            "do not reset benchmark catalog/storage on startup; useful when attaching "
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


if __name__ == "__main__":
    main()
