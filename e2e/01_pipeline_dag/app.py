"""01 -- in-process pipeline DAG (v1: smallest possible).

This v1 is deliberately the smallest end-to-end pipeline that exercises
the CDC primitives:

    raw_events  ──► normalize  ──►  clean_events

A producer thread INSERTs JSON-payload rows into ``lake.raw_events``.
A consumer thread streams them through three explicit steps:

    (1) ``DMLConsumer.batches()`` blocks server-side until new rows
        arrive, then yields a ``DMLBatch`` of typed change rows;
    (2) Python transforms each row (extract typed columns from JSON);
    (3) ``batch.transaction()`` opens one ACID transaction on the
        consumer's connection, INSERTs the transformed rows into
        ``lake.clean_events``, then runs ``cdc_commit`` and ``COMMIT``
        atomically on exit -- ``ROLLBACK`` on exception.

The transaction context is the exactly-once knob: ``ROLLBACK`` undoes
both the sink write and the cursor advance, so a crash before COMMIT
replays the same batch on restart. That's the entire point of building
this pattern on top of CDC primitives instead of an ad-hoc cursor.

What v1 does NOT do (intentionally; coming in later iterations):
- Multiple stages (next: add a join + an aggregate stage).
- Catalog matrix beyond ``--catalog postgres`` (next: validate sqlite + duckdb).
- ``--storage s3`` (waits until garage is exercised manually first).
- Schema-boundary handling (DDL consumer wired in, ALTER TABLE survival).
- Kill+restart correctness assertions (durable cursor already gives this for free;
  we just need to write the test pass).
- Performance tuning to hit the README's >=5000 rows/s and <=1s p99 lag targets.

Everything in this file is meant to read as documentation. If it stops
reading like docs as later versions add stages, split the DAG declaration
into ``stages.py`` per the original plan.
"""

from __future__ import annotations

import json
import os
import random
import signal
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

# Example directories are named ``NN_xxx/`` for run-order legibility, which
# means they can't be imported as regular Python packages. The shared
# ``_lib/`` lives at the e2e/ root; bootstrap it onto sys.path so plain
# ``python e2e/01_pipeline_dag/app.py`` works the same as ``uv run``.
_E2E_ROOT = Path(__file__).resolve().parent.parent
if str(_E2E_ROOT) not in sys.path:
    sys.path.insert(0, str(_E2E_ROOT))

from ducklake_cdc_client import DMLBatch, DMLConsumer  # noqa: E402
from rich.layout import Layout  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.text import Text  # noqa: E402

from _lib.cli import effective_duration_s, make_parser, parse_common  # noqa: E402
from _lib.config import (  # noqa: E402
    load_cdc_extension,
    open_lake,
    reset_lake,
)
from _lib.metrics import MetricsRecorder  # noqa: E402
from _lib.tui import LiveDisplay, log  # noqa: E402

EXAMPLE = "01_pipeline_dag"

# --- pipeline parameters ----------------------------------------------------

# Producer cadence. Tuned to a comfortable demo speed -- not a perf number.
# v3 will revisit this when we tune for the README's 5000 rows/s target.
# Each batch becomes one DuckLake commit (~376 ms floor on the postgres
# catalog), so smaller batches just thrash the catalog without going
# faster -- the throughput bottleneck is commit cadence, not row count.
PRODUCER_BATCH_ROWS = 200
PRODUCER_BATCH_INTERVAL_S = 0.25

# Long-poll parameters passed into ``DMLConsumer.batches()``. ``timeout_ms``
# bounds shutdown latency (one extra wait window before the consumer
# notices ``stop_event``). ``max_snapshots`` is a ceiling -- the
# consumer's adaptive snapshot window grows up to this number when the
# producer is fast enough to fill it without violating its
# listen-elapsed target.
LISTEN_TIMEOUT_MS = 1000
LISTEN_MAX_SNAPSHOTS = 1000

KIND_CHOICES = ("purchase", "refund", "subscription_renewal", "trial_signup")


# --- CDC consumer name ------------------------------------------------------
# Every consumer in the lake has a unique name. v1 has one; v2 will name
# them per-stage as the DAG grows.
CONSUMER_NAME = "normalize"


# ---------------------------------------------------------------------------
# wiring: schema + consumer setup
# ---------------------------------------------------------------------------


def setup_schema(conn: Any) -> None:
    """Create the source and sink tables. Idempotent."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.raw_events (
            id           BIGINT,
            payload      JSON,
            created_at   TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.clean_events (
            id              BIGINT,
            kind            VARCHAR,
            amount          DOUBLE,
            source_snapshot BIGINT,
            cleaned_at      TIMESTAMP
        )
        """
    )


# ---------------------------------------------------------------------------
# producer
# ---------------------------------------------------------------------------


def producer_loop(
    lake: Any,
    recorder: MetricsRecorder,
    stop: threading.Event,
) -> None:
    """Synthetic source: write batches of JSON-payload rows into lake.raw_events."""
    conn = lake.connection.cursor()
    next_id = 0
    rng = random.Random(42)

    log("producer started")
    while not stop.is_set():
        rows = [_synth_row(next_id + i, rng) for i in range(PRODUCER_BATCH_ROWS)]
        next_id += PRODUCER_BATCH_ROWS

        values_sql = ",".join(["(?, ?::JSON, now())"] * len(rows))
        params: list[object] = []
        for row_id, payload in rows:
            params.extend([row_id, payload])

        try:
            conn.execute(
                f"INSERT INTO lake.raw_events VALUES {values_sql}",
                params,
            )
        except Exception as exc:  # noqa: BLE001 - never let producer death kill the demo
            recorder.record_error()
            log(f"producer error: {type(exc).__name__}: {exc}", level="warn")
            stop.wait(0.5)
            continue

        recorder.set_detail("producer_rows_total", next_id)
        recorder.set_detail("producer_last_batch_size", len(rows))
        stop.wait(PRODUCER_BATCH_INTERVAL_S)
    log("producer stopped")


def _synth_row(row_id: int, rng: random.Random) -> tuple[int, str]:
    payload = {
        "kind": rng.choice(KIND_CHOICES),
        "amount": round(rng.uniform(1.0, 1000.0), 2),
    }
    return row_id, json.dumps(payload)


# ---------------------------------------------------------------------------
# consumer (the actual streaming pipeline)
# ---------------------------------------------------------------------------


def consumer_loop(
    lake: Any,
    recorder: MetricsRecorder,
    stop: threading.Event,
    ready: threading.Event,
) -> None:
    """The streaming pipeline. listen → transform → write+commit, repeat.

    The shape we want every consumer in our docs to start from::

        with DMLConsumer(lake, name, table=src, mode="changes") as consumer:
            for batch in consumer.batches(stop_event=stop):
                with batch.transaction() as tx:
                    tx.executemany(
                        "INSERT INTO sink VALUES (...)",
                        [transform(c) for c in batch],
                    )

    ``DMLConsumer`` owns the things that aren't part of the pipeline's
    business logic: it derives + pins a dedicated DuckDB connection
    (so the CDC lease, the listen long-poll, and the cdc_commit all
    agree on which connection holds the lease), creates the consumer
    pinned to ``raw_events``, handles the H-022 first-bootstrap retry,
    drives ``cdc_dml_changes_listen`` (one server roundtrip per batch,
    no ticks-then-read dance), parses each row into a :class:`Change`
    with the table's columns folded into ``.values``, and adapts the
    snapshot window up to ``LISTEN_MAX_SNAPSHOTS`` based on the
    producer's cadence. ``stop_event`` makes shutdown bounded by
    ``LISTEN_TIMEOUT_MS``.

    ``batch.transaction()`` is the exactly-once knob: ``BEGIN`` on
    enter, ``cdc_commit`` + ``COMMIT`` atomically on success,
    ``ROLLBACK`` on exception. A crash before COMMIT replays the same
    batch on restart, so the consumer's view of the clean stream is
    exactly-once. The transform step is plain Python on purpose --
    real pipelines enrich, filter, look up reference data, etc.

    Why long-poll vs ``cdc_window`` + ``time.sleep``: a 50 ms busy-poll
    cycles roughly 100 postgres-scanner connection acquisitions/second
    on the catalog, which (combined with producer INSERT churn)
    exhausts the postgres-scanner pool under sustained load and
    surfaces as ``Connection pool timeout: all 64 connections in use,
    waited 30000ms``. Long-polling holds one connection idle for the
    wait window instead.
    """
    log(f"consumer '{CONSUMER_NAME}' started")
    batches_seen = 0
    rows_written = 0

    with DMLConsumer(
        lake,
        CONSUMER_NAME,
        table="raw_events",
        mode="changes",
    ) as consumer:
        # Signal "consumer is open" so the producer can start without
        # racing the H-022 first-bootstrap window: cdc_dml_consumer_create
        # provisions the ``__ducklake_cdc.*`` tables in the catalog the
        # first time it runs against a fresh database, and that
        # bootstrap can race with a concurrent INSERT on the lake's
        # other connections, surfacing as ``thread::join failed``.
        # ``DMLConsumer.__enter__`` already wraps the create with
        # ``retry_on_transient`` so its own retry covers the consumer
        # side; gating producer start covers the producer side.
        ready.set()
        for batch in consumer.batches(
            stop_event=stop,
            timeout_ms=LISTEN_TIMEOUT_MS,
            max_snapshots=LISTEN_MAX_SNAPSHOTS,
        ):
            # transform + write+commit, timed end-to-end so the latency
            # metric reflects what the operator actually cares about:
            # time from "batch arrived" to "cursor advanced".
            t_start = time.monotonic()
            try:
                written = _apply_batch(batch)
            except Exception as exc:  # noqa: BLE001
                recorder.record_error()
                log(f"batch apply {type(exc).__name__}: {exc}", level="error")
                stop.wait(0.5)
                continue

            elapsed_ms = (time.monotonic() - t_start) * 1000.0
            batches_seen += 1
            rows_written += written
            recorder.record_rows(written)
            recorder.record_latency_ms(elapsed_ms)
            recorder.set_detail("consumer_batches_seen", batches_seen)
            recorder.set_detail("consumer_rows_total", rows_written)
            recorder.set_detail("consumer_last_snapshot", batch.end_snapshot)
    log(f"consumer '{CONSUMER_NAME}' stopped")


def _transform_change(change: Any) -> tuple[int, str, float, int] | None:
    """Stage's business logic. Returns the cleaned row, or None to drop it.

    The :class:`Change` carries the pinned table's columns at
    ``.values["..."]`` -- ``id``, ``payload``, ``created_at`` here.
    DuckDB returns JSON columns as Python strings, so we json.loads
    the payload before extracting typed fields.

    v1 only emits inserts; updates and deletes are dropped (the v1
    sink is append-only). v2 will route updates and deletes to the
    next stage with explicit semantics.
    """
    if change.kind != "insert":
        return None
    payload = change.values["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return (
        int(change.values["id"]),
        str(payload["kind"]),
        float(payload["amount"]),
        int(change.snapshot_id),
    )


def _apply_batch(batch: DMLBatch) -> int:
    """Transform a batch in Python, then apply + commit in one ACID transaction.

    Returns the number of rows written. The cursor advances to
    ``batch.end_snapshot`` even when every row was filtered out, so
    listen doesn't keep replaying batches that contain only updates
    or deletes.
    """
    cleaned = [t for t in (_transform_change(c) for c in batch) if t is not None]

    # ``batch.transaction()`` runs BEGIN on enter, then on normal exit
    # cdc_commit + COMMIT atomically -- ROLLBACK if we raise. The
    # consumer's connection is the lease holder, so cdc_commit there
    # joins the same BEGIN as our INSERT. Crash recovery replays this
    # batch -> exactly-once.
    with batch.transaction() as tx:
        if cleaned:
            values_sql = ",".join(["(?, ?, ?, ?, now())"] * len(cleaned))
            params: list[object] = []
            for clean in cleaned:
                params.extend(clean)
            tx.execute(
                f"INSERT INTO lake.clean_events VALUES {values_sql}",
                params,
            )
    return len(cleaned)


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------


def build_layout(recorder: MetricsRecorder, stop: threading.Event) -> Layout:
    """Two-pane TUI: producer on the left, consumer on the right, header up top."""
    snap = recorder.snapshot()
    layout = Layout()
    layout.split(
        Layout(_header(snap), name="header", size=3),
        Layout(name="body"),
        Layout(_footer(stop), name="footer", size=2),
    )
    layout["body"].split_row(
        Layout(_producer_panel(snap), name="producer"),
        Layout(_consumer_panel(snap), name="consumer"),
    )
    return layout


def _header(snap: dict[str, Any]) -> Panel:
    elapsed = snap["elapsed_s"]
    text = Text.from_markup(
        f"[bold]ducklake-cdc[/bold] · 01_pipeline_dag · "
        f"[cyan]raw_events[/cyan] → [green]clean_events[/green]   "
        f"[dim]elapsed[/dim] {_fmt_duration(elapsed)}   "
        f"[dim]errors[/dim] {snap['errors']}"
    )
    return Panel(text, border_style="dim")


def _footer(stop: threading.Event) -> Panel:
    text = Text.from_markup(
        "[dim]Ctrl-C to stop · in-process · listen → transform → write+commit · "
        "exactly-once via cdc_commit[/dim]"
    )
    return Panel(text, border_style="dim")


def _producer_panel(snap: dict[str, Any]) -> Panel:
    details = snap["details"]
    table = Table.grid(padding=(0, 2))
    table.add_column(justify="right", style="dim")
    table.add_column()
    table.add_row("rows written", _fmt_int(details.get("producer_rows_total", 0)))
    table.add_row("last batch", _fmt_int(details.get("producer_last_batch_size", 0)))
    table.add_row("interval", f"{PRODUCER_BATCH_INTERVAL_S * 1000:.0f} ms")
    return Panel(table, title="[bold cyan]producer[/bold cyan]", border_style="cyan")


def _consumer_panel(snap: dict[str, Any]) -> Panel:
    details = snap["details"]
    lat = snap["latency_ms"]
    elapsed = max(snap["elapsed_s"], 1e-9)
    rows_per_s = snap["rows_processed"] / elapsed

    table = Table.grid(padding=(0, 2))
    table.add_column(justify="right", style="dim")
    table.add_column()
    table.add_row("batches seen", _fmt_int(details.get("consumer_batches_seen", 0)))
    table.add_row("rows written", _fmt_int(details.get("consumer_rows_total", 0)))
    table.add_row("throughput", f"{rows_per_s:,.0f} rows/s")
    table.add_row("apply p50", _fmt_ms(lat.get("p50")))
    table.add_row("apply p95", _fmt_ms(lat.get("p95")))
    table.add_row("apply p99", _fmt_ms(lat.get("p99")))
    table.add_row("last snapshot", str(details.get("consumer_last_snapshot", "—")))
    return Panel(
        table,
        title="[bold green]consumer · normalize[/bold green]",
        border_style="green",
    )


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _fmt_ms(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:6.1f} ms"


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def headless_summary(recorder: MetricsRecorder) -> Callable[[], str]:
    """Closure used by LiveDisplay for the periodic stderr line in --headless mode."""
    def summary() -> str:
        snap = recorder.snapshot()
        details = snap["details"]
        rows_per_s = snap["rows_processed"] / max(snap["elapsed_s"], 1e-9)
        p99 = snap["latency_ms"].get("p99")
        p99_str = f"{p99:.1f}ms" if p99 is not None else "—"
        return (
            f"raw={_fmt_int(details.get('producer_rows_total', 0))} "
            f"clean={_fmt_int(details.get('consumer_rows_total', 0))} "
            f"throughput={rows_per_s:,.0f} rows/s "
            f"p99={p99_str} "
            f"errors={snap['errors']}"
        )
    return summary


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = make_parser(
        description="In-process pipeline DAG: lake.raw_events -> normalize -> lake.clean_events.",
        # v1 only validates against postgres; v2 expands.
        supported_catalogs=("postgres",),
        supported_storages=("disk",),
    )
    args = parser.parse_args(argv)
    common = parse_common(args)

    log(
        f"starting {EXAMPLE} mode={'headless' if common.headless else 'demo'} "
        f"catalog={common.catalog} storage={common.storage}"
    )

    # Hermetic startup: every run begins from an empty catalog and storage
    # so the demo never inherits stale state from a previous attempt that
    # exited dirty (process killed, pgbouncer pool poisoned, etc).
    reset_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)

    lake = open_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)
    load_cdc_extension(lake)
    setup_schema(lake.connection)
    # The DML consumer itself is owned by ``DMLConsumer`` inside
    # ``consumer_loop`` -- ``with consumer:`` handles the
    # H-022-prone ``cdc_dml_consumer_create`` with the canonical
    # ``retry_on_transient`` wrapper internally.
    log("schema ready")

    recorder = MetricsRecorder(
        example=EXAMPLE,
        catalog=common.catalog,
        storage=common.storage,
    )
    stop = threading.Event()

    # Two-stage signal handling so an impatient operator can always escape:
    #   1st SIGINT (or SIGTERM): request graceful stop. Threads finish their
    #     current batch / window, lake closes, metrics get written.
    #   2nd SIGINT (within ~5s): hard exit via os._exit(130). Skips finalisers
    #     entirely so a hung lake.close() or sticky DuckDB pipeline can't
    #     trap the operator. 130 is the conventional "interrupted" exit code.
    # ``uv run`` 0.7.3 occasionally swallows the first signal when delivery
    # bypasses the process group (see e2e/_lib/README.md notes). The
    # second-SIGINT escape hatch makes that benign.
    def _request_stop(signum: int, _frame: object) -> None:
        if stop.is_set():
            log(f"signal {signum} (again) -> hard exit", level="warn")
            os._exit(130)
        log(f"signal {signum} -> stop")
        stop.set()
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    # Start consumer first and wait for ``DMLConsumer.__enter__`` to
    # finish before letting the producer go: ``cdc_dml_consumer_create``
    # bootstraps the catalog's CDC tables on first call against a fresh
    # database, and that bootstrap can race with concurrent INSERTs on
    # other connections (H-022). Sequencing here keeps both threads on
    # the happy path.
    consumer_ready = threading.Event()
    producer = threading.Thread(
        target=producer_loop, args=(lake, recorder, stop), name="producer", daemon=True
    )
    consumer = threading.Thread(
        target=consumer_loop,
        args=(lake, recorder, stop, consumer_ready),
        name="consumer",
        daemon=True,
    )
    consumer.start()
    if not consumer_ready.wait(timeout=10.0):
        log("consumer failed to ready within 10s; aborting", level="error")
        stop.set()
        consumer.join(timeout=2.0)
        return 1
    producer.start()

    summary_fn = headless_summary(recorder)
    wait_seconds = effective_duration_s(common)
    try:
        with LiveDisplay(
            headless=common.headless,
            renderable_factory=lambda: build_layout(recorder, stop),
            headless_summary=summary_fn,
        ):
            # ``Event.wait(None)`` blocks indefinitely (demo without --duration),
            # ``Event.wait(N)`` blocks up to N seconds. Either path returns early
            # when the signal handler sets stop.
            stop.wait(wait_seconds)
    except KeyboardInterrupt:
        stop.set()
    finally:
        stop.set()
        # Tighter join timeouts than v1 -- a 200-row INSERT batch and a
        # CDC window are both bounded sub-second on the postgres catalog,
        # so 2s per thread is generous. If they overshoot, the daemon
        # threads die on interpreter exit anyway.
        producer.join(timeout=2.0)
        consumer.join(timeout=2.0)
        try:
            lake.close()
        except Exception as exc:  # noqa: BLE001
            log(f"lake.close raised: {exc!r}", level="warn")

        result_path = recorder.finalize_and_write()
        log(f"wrote metrics: {result_path}")
        snap = recorder.snapshot()
        log(summary_fn())

        # Cleanup-on-exit: leave nothing behind for the next run or for
        # ``git status`` to show. Symmetric with the hermetic startup
        # reset above. Failures here are warnings, not errors -- the
        # workload already succeeded; cleanup is best-effort.
        try:
            reset_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)
            log("cleaned up catalog + storage")
        except Exception as exc:  # noqa: BLE001
            log(f"cleanup failed (workload result still valid): {exc!r}", level="warn")

    return 1 if snap["errors"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
