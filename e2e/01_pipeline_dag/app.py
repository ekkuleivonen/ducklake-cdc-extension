"""01_pipeline_dag (v2): the canonical multi-stage in-process streaming DAG.

What this example exists to show
================================

This is the example we point a Python developer at when they ask "how
do I build a streaming pipeline on top of DuckLake?". v1 demonstrated
the unit (one consumer + ``batch.transaction()``); v2 composes that
unit into the smallest interesting topology: two parallel normalize
stages and one join stage that combines them.

Topology::

    raw_purchases ───► normalize_purchases ───► clean_purchases ─┐
                                                                 ├─► join_orders ──► joined_orders
    raw_refunds   ───► normalize_refunds   ───► clean_refunds  ──┘   (driver: clean_purchases,
                                                                      lookup: clean_refunds inside tx)

Five tables, three stages, two producers, all in one Python process.

What a "DAG user" gets to write
-------------------------------

Per ``e2e/_lib/stage.py``, a stage is just a function::

    def normalize_purchases(batch, tx) -> int:
        rows = [...]                                                 # transform Python-side
        tx.executemany("INSERT INTO lake.clean_purchases VALUES ...", rows)
        return len(rows)

The shared :class:`StageRunner` gives each stage one OS thread, one
``DMLConsumer`` (with its own dedicated DuckDB connection holding the
CDC lease), and runs every batch's transform inside
``batch.transaction()`` -- BEGIN + sink work + ``cdc_commit`` + COMMIT
atomically, ROLLBACK on exception. The DAG topology is just a list of
``Stage(name, source, transform)`` tuples; you can read it at one
place to see what's connected to what.

Join semantics
--------------

``join_orders`` is a *driver + snapshot lookup* join. ``clean_purchases``
drives -- one batch at a time. For each purchase row in the batch,
the SELECT inside ``batch.transaction()`` reads ``clean_refunds`` at
the snapshot pinned by BEGIN. Refunds that arrive after a purchase
has already been joined produce a NULL ``refund_amount`` in
``joined_orders`` and *stay NULL forever*. The output is append-only.

That trade-off is deliberate. The alternatives -- re-emitting joined
rows on refund arrival, or a watermark-based stream-stream window
join -- are powerful but expensive in code and concepts. The honest
DuckLake story for this kind of work is "your join target is just
another table; consistency comes for free", and that's what this
stage demonstrates.

Exactly-once
------------

Each stage's cursor advances inside the same transaction as its sink
write, so a crash before COMMIT replays the same batch on restart and
the sink table is exactly the union of every successfully-applied
batch. The cursor is durable, so the pipeline picks up exactly where
it left off across process restarts.

The "exactly-once" guarantee is *per stage*, not across stages. A row
that's been normalized into ``clean_purchases`` but not yet joined
into ``joined_orders`` is durable in the catalog -- the join stage's
own cursor will pick it up next time it runs.

Modes
-----

::

    uv run python 01_pipeline_dag/app.py            # demo, runs until Ctrl-C
    uv run python 01_pipeline_dag/app.py --duration 30
    uv run python 01_pipeline_dag/app.py --headless --duration 30

Headless writes ``./.results/01_pipeline_dag-postgres-disk.json`` and
exits non-zero if any stage error was recorded.
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
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any

# Example directories are named ``NN_xxx/`` for run-order legibility, which
# means they can't be imported as regular Python packages. The shared
# ``_lib/`` lives at the e2e/ root; bootstrap it onto sys.path so plain
# ``python e2e/01_pipeline_dag/app.py`` works the same as ``uv run``.
_E2E_ROOT = Path(__file__).resolve().parent.parent
if str(_E2E_ROOT) not in sys.path:
    sys.path.insert(0, str(_E2E_ROOT))

from ducklake_cdc_client import BatchTransaction, DMLBatch  # noqa: E402
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
from _lib.stage import INSERTS_ONLY, Stage, StageRunner  # noqa: E402
from _lib.tui import LiveDisplay, log  # noqa: E402

EXAMPLE = "01_pipeline_dag"

# --- pipeline parameters ----------------------------------------------------

# Producer cadence. Same comfortable demo speed as v1 -- not a perf
# number. Each batch becomes one DuckLake commit; commit cadence is
# the throughput floor on the postgres catalog.
PURCHASES_BATCH_ROWS = 200
REFUNDS_BATCH_ROWS = 50
PRODUCER_BATCH_INTERVAL_S = 0.25

# Refunds reference random already-emitted purchase IDs. The warmup
# floor avoids the first refund batch racing with an empty
# ``raw_purchases`` (and avoids a noisy "no eligible purchase" branch
# in the producer code).
REFUND_WARMUP_PURCHASES = 100

# Long-poll parameters per stage. ``timeout_ms`` doubles as the
# shutdown latency upper bound (one extra wait window before each
# stage notices ``stop_event``). ``max_snapshots`` is a ceiling that
# the consumer's adaptive window may grow into when the producer is
# fast enough to fill it.
LISTEN_TIMEOUT_MS = 1000
LISTEN_MAX_SNAPSHOTS = 1000

KIND_CHOICES = ("purchase", "refund", "subscription_renewal", "trial_signup")


# ---------------------------------------------------------------------------
# wiring: schema setup
# ---------------------------------------------------------------------------


def setup_schema(conn: Any) -> None:
    """Create the source, clean, and join tables. Idempotent.

    Two source tables (``raw_*``), two intermediate tables (``clean_*``)
    that the normalize stages write to, and one join target
    (``joined_orders``). All tables live in the ``lake`` database
    attached by ``open_lake``.
    """
    # Sources -- producers write JSON payloads (purchases) and typed
    # rows (refunds) so the example shows both shapes pass cleanly
    # through CDC.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.raw_purchases (
            id           BIGINT,
            payload      JSON,
            created_at   TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.raw_refunds (
            id              BIGINT,
            purchase_id     BIGINT,
            refund_amount   DOUBLE,
            created_at      TIMESTAMP
        )
        """
    )
    # Clean -- the two normalize stages' outputs. Identical shapes to
    # the join target's projection so the join's INSERT...SELECT is
    # the obvious thing to write.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.clean_purchases (
            id              BIGINT,
            kind            VARCHAR,
            amount          DOUBLE,
            source_snapshot BIGINT,
            cleaned_at      TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.clean_refunds (
            id              BIGINT,
            purchase_id     BIGINT,
            refund_amount   DOUBLE,
            source_snapshot BIGINT,
            cleaned_at      TIMESTAMP
        )
        """
    )
    # Join -- append-only, one row per driver row. ``refund_amount``
    # is NULL when the matching refund hadn't been cleaned yet at
    # join time (and stays NULL forever -- see module docstring).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.joined_orders (
            purchase_id     BIGINT,
            kind            VARCHAR,
            amount          DOUBLE,
            refund_amount   DOUBLE,
            source_snapshot BIGINT,
            joined_at       TIMESTAMP
        )
        """
    )


# ---------------------------------------------------------------------------
# producers
# ---------------------------------------------------------------------------


@dataclass
class _PurchaseIdSpace:
    """Thread-safe high-water mark of emitted purchase IDs.

    The purchases producer increments after each batch lands; the
    refunds producer reads ``max_emitted`` to pick refund targets.
    A counter is enough -- IDs are dense from 0, and "refund a random
    earlier purchase" is the right semantic for the demo without
    needing a per-ID set.
    """

    _max: int = 0
    _lock: Lock = field(default_factory=Lock)

    def record_emission(self, count: int) -> None:
        with self._lock:
            self._max += count

    def max_emitted(self) -> int:
        with self._lock:
            return self._max


def producer_purchases(
    lake: Any,
    recorder: MetricsRecorder,
    stop: threading.Event,
    id_space: _PurchaseIdSpace,
) -> None:
    """Synthetic source: write batches of JSON-payload rows into raw_purchases."""
    conn = lake.connection.cursor()
    next_id = 0
    rng = random.Random(42)

    log("\\[producer:purchases] started")
    while not stop.is_set():
        rows = [_synth_purchase(next_id + i, rng) for i in range(PURCHASES_BATCH_ROWS)]
        next_id += PURCHASES_BATCH_ROWS

        values_sql = ",".join(["(?, ?::JSON, now())"] * len(rows))
        params: list[object] = []
        for row_id, payload in rows:
            params.extend([row_id, payload])

        try:
            conn.execute(
                f"INSERT INTO lake.raw_purchases VALUES {values_sql}",
                params,
            )
        except Exception as exc:  # noqa: BLE001 - producer death must not kill the demo
            recorder.record_error()
            log(f"\\[producer:purchases] {type(exc).__name__}: {exc}", level="warn")
            stop.wait(0.5)
            continue

        id_space.record_emission(len(rows))
        recorder.set_detail("producer_purchases_total", next_id)
        recorder.set_detail("producer_purchases_last_batch", len(rows))
        stop.wait(PRODUCER_BATCH_INTERVAL_S)
    log("\\[producer:purchases] stopped")


def producer_refunds(
    lake: Any,
    recorder: MetricsRecorder,
    stop: threading.Event,
    id_space: _PurchaseIdSpace,
) -> None:
    """Synthetic source: write refunds that reference random emitted purchase IDs."""
    conn = lake.connection.cursor()
    next_id = 0
    rng = random.Random(7)

    log("\\[producer:refunds] started")
    # Wait for the purchases producer to populate enough IDs so the
    # first refund batch has something legitimate to reference.
    while not stop.is_set() and id_space.max_emitted() < REFUND_WARMUP_PURCHASES:
        stop.wait(0.1)

    while not stop.is_set():
        ceiling = id_space.max_emitted()
        if ceiling == 0:
            stop.wait(0.1)
            continue

        rows = [
            _synth_refund(next_id + i, rng.randint(0, ceiling - 1), rng)
            for i in range(REFUNDS_BATCH_ROWS)
        ]
        next_id += REFUNDS_BATCH_ROWS

        values_sql = ",".join(["(?, ?, ?, now())"] * len(rows))
        params: list[object] = []
        for refund_id, purchase_id, refund_amount in rows:
            params.extend([refund_id, purchase_id, refund_amount])

        try:
            conn.execute(
                f"INSERT INTO lake.raw_refunds VALUES {values_sql}",
                params,
            )
        except Exception as exc:  # noqa: BLE001
            recorder.record_error()
            log(f"\\[producer:refunds] {type(exc).__name__}: {exc}", level="warn")
            stop.wait(0.5)
            continue

        recorder.set_detail("producer_refunds_total", next_id)
        recorder.set_detail("producer_refunds_last_batch", len(rows))
        stop.wait(PRODUCER_BATCH_INTERVAL_S)
    log("\\[producer:refunds] stopped")


def _synth_purchase(row_id: int, rng: random.Random) -> tuple[int, str]:
    payload = {
        "kind": rng.choice(KIND_CHOICES),
        "amount": round(rng.uniform(1.0, 1000.0), 2),
    }
    return row_id, json.dumps(payload)


def _synth_refund(refund_id: int, purchase_id: int, rng: random.Random) -> tuple[int, int, float]:
    return refund_id, purchase_id, round(rng.uniform(1.0, 500.0), 2)


# ---------------------------------------------------------------------------
# stages (the actual streaming pipeline)
# ---------------------------------------------------------------------------


def normalize_purchases(batch: DMLBatch, tx: BatchTransaction) -> int:
    """raw_purchases -> clean_purchases.

    Pulls JSON payload apart and writes the cleaned tuple. The
    consumer is subscribed with ``change_types=INSERTS_ONLY``, so
    every :class:`Change` we see is already an ``insert`` -- no
    sink-side filter needed.
    """
    cleaned: list[tuple[int, str, float, int]] = []
    for change in batch:
        payload = change.values["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        cleaned.append(
            (
                int(change.values["id"]),
                str(payload["kind"]),
                float(payload["amount"]),
                int(change.snapshot_id),
            )
        )
    if not cleaned:
        return 0

    values_sql = ",".join(["(?, ?, ?, ?, now())"] * len(cleaned))
    params: list[object] = []
    for clean in cleaned:
        params.extend(clean)
    tx.execute(
        f"INSERT INTO lake.clean_purchases VALUES {values_sql}",
        params,
    )
    return len(cleaned)


def normalize_refunds(batch: DMLBatch, tx: BatchTransaction) -> int:
    """raw_refunds -> clean_refunds.

    Refunds arrive already typed; the "transform" is just to add
    ``source_snapshot`` and ``cleaned_at``. In a realer pipeline this
    is where currency conversion / fraud filters / late-arrival
    handling would live. Subscribed via ``INSERTS_ONLY`` -- updates
    and deletes never reach this transform.
    """
    cleaned: list[tuple[int, int, float, int]] = []
    for change in batch:
        cleaned.append(
            (
                int(change.values["id"]),
                int(change.values["purchase_id"]),
                float(change.values["refund_amount"]),
                int(change.snapshot_id),
            )
        )
    if not cleaned:
        return 0

    values_sql = ",".join(["(?, ?, ?, ?, now())"] * len(cleaned))
    params: list[object] = []
    for clean in cleaned:
        params.extend(clean)
    tx.execute(
        f"INSERT INTO lake.clean_refunds VALUES {values_sql}",
        params,
    )
    return len(cleaned)


def join_orders(batch: DMLBatch, tx: BatchTransaction) -> int:
    """clean_purchases (driver) -> joined_orders, looking up clean_refunds.

    For each purchase in this batch, LEFT JOIN against the current
    snapshot of ``clean_refunds``. The lookup runs on the consumer's
    connection inside ``batch.transaction()`` -- snapshot-isolated
    reads come for free, and ``cdc_commit`` + ``COMMIT`` are atomic
    with the INSERT.

    The driver's columns come from the batch itself (the consumer is
    pinned to ``clean_purchases``, so ``Change.values`` already has
    ``id`` / ``kind`` / ``amount`` projected). We don't re-read
    ``clean_purchases`` -- the batch *is* our driver.

    Refund-after-join produces NULL ``refund_amount`` here forever.
    See the module docstring for why that's the right semantic.
    Subscribed via ``INSERTS_ONLY``.
    """
    if not batch.changes:
        return 0

    driver_placeholders = ",".join(["(?, ?, ?)"] * len(batch.changes))
    params: list[object] = [batch.end_snapshot]
    for change in batch:
        params.extend(
            [
                int(change.values["id"]),
                str(change.values["kind"]),
                float(change.values["amount"]),
            ]
        )

    tx.execute(
        f"""
        INSERT INTO lake.joined_orders
        SELECT
            d.purchase_id,
            d.kind,
            d.amount,
            r.refund_amount,
            ?               AS source_snapshot,
            now()           AS joined_at
        FROM (VALUES {driver_placeholders}) AS d(purchase_id, kind, amount)
        LEFT JOIN lake.clean_refunds r ON r.purchase_id = d.purchase_id
        """,
        params,
    )
    return len(batch.changes)


# DAG declaration. One place to read the topology. Each tuple becomes
# one OS thread + one DMLConsumer + one durable cursor. ``StageRunner``
# starts them serially so the per-stage ``cdc_dml_consumer_create``
# never races with another stage's first INSERT (H-022).
#
# ``change_types=INSERTS_ONLY`` forwards to ``cdc_dml_consumer_create``
# at the extension level: updates and deletes are filtered before they
# reach the listen call. This v2 only models inserts (producers do
# nothing else, ``clean_*`` and ``joined_orders`` are append-only), so
# the right place to declare that intent is the consumer subscription
# rather than a defensive guard inside every transform.
STAGES: tuple[Stage, ...] = (
    Stage("normalize_purchases", source="raw_purchases", transform=normalize_purchases, change_types=INSERTS_ONLY),
    Stage("normalize_refunds", source="raw_refunds", transform=normalize_refunds, change_types=INSERTS_ONLY),
    Stage("join_orders", source="clean_purchases", transform=join_orders, change_types=INSERTS_ONLY),
)


# ---------------------------------------------------------------------------
# TUI
# ---------------------------------------------------------------------------


def build_layout(recorder: MetricsRecorder, stop: threading.Event) -> Layout:
    """Header / producers panel / stages panel / footer. Re-rendered at 4 Hz."""
    snap = recorder.snapshot()
    layout = Layout()
    layout.split(
        Layout(_header(snap), name="header", size=3),
        Layout(name="body"),
        Layout(_footer(stop), name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(_producers_panel(snap), name="producers", ratio=2),
        Layout(_stages_panel(snap), name="stages", ratio=3),
    )
    return layout


def _header(snap: dict[str, Any]) -> Panel:
    elapsed = snap["elapsed_s"]
    text = Text.from_markup(
        f"[bold]ducklake-cdc[/bold] · 01_pipeline_dag · "
        f"[cyan]raw_purchases[/cyan]/[cyan]raw_refunds[/cyan] → "
        f"[yellow]clean_purchases[/yellow]/[yellow]clean_refunds[/yellow] → "
        f"[green]joined_orders[/green]   "
        f"[dim]elapsed[/dim] {_fmt_duration(elapsed)}   "
        f"[dim]errors[/dim] {snap['errors']}"
    )
    return Panel(text, border_style="dim")


def _footer(stop: threading.Event) -> Panel:
    text = Text.from_markup(
        "[dim]Ctrl-C to stop · in-process · 3 stages × DMLConsumer + "
        "batch.transaction() · join is snapshot-lookup, append-only[/dim]"
    )
    return Panel(text, border_style="dim")


def _producers_panel(snap: dict[str, Any]) -> Panel:
    details = snap["details"]
    table = Table.grid(padding=(0, 2))
    table.add_column(justify="left")
    table.add_column(justify="right", style="dim")
    table.add_column(justify="right")
    table.add_row("[cyan]raw_purchases[/cyan]", "rows", _fmt_int(details.get("producer_purchases_total", 0)))
    table.add_row("", "last batch", _fmt_int(details.get("producer_purchases_last_batch", 0)))
    table.add_row("", "interval", f"{PRODUCER_BATCH_INTERVAL_S * 1000:.0f} ms")
    table.add_row("", "", "")
    table.add_row("[cyan]raw_refunds[/cyan]", "rows", _fmt_int(details.get("producer_refunds_total", 0)))
    table.add_row("", "last batch", _fmt_int(details.get("producer_refunds_last_batch", 0)))
    table.add_row("", "interval", f"{PRODUCER_BATCH_INTERVAL_S * 1000:.0f} ms")
    return Panel(table, title="[bold cyan]producers[/bold cyan]", border_style="cyan")


def _stages_panel(snap: dict[str, Any]) -> Panel:
    details = snap["details"]
    lat = snap["latency_ms"]
    elapsed = max(snap["elapsed_s"], 1e-9)

    table = Table.grid(padding=(0, 2))
    table.add_column(justify="left")
    table.add_column(justify="right", style="dim")
    table.add_column(justify="right")
    for stage in STAGES:
        rows = details.get(f"stage_{stage.name}_rows", 0)
        batches = details.get(f"stage_{stage.name}_batches", 0)
        snap_id = details.get(f"stage_{stage.name}_last_snapshot", "—")
        rps = (rows / elapsed) if elapsed else 0
        table.add_row(
            f"[green]{stage.name}[/green]",
            f"src={stage.source}",
            f"{_fmt_int(rows)} rows · {_fmt_int(batches)} batches · {rps:,.0f}/s · snap {snap_id}",
        )
    table.add_row("", "", "")
    table.add_row(
        "[bold]apply latency[/bold]",
        "p50 / p95 / p99",
        f"{_fmt_ms(lat.get('p50'))} / {_fmt_ms(lat.get('p95'))} / {_fmt_ms(lat.get('p99'))}",
    )
    return Panel(table, title="[bold green]stages[/bold green]", border_style="green")


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _fmt_ms(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:6.1f}ms"


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def headless_summary(recorder: MetricsRecorder) -> Callable[[], str]:
    """Closure for the periodic stderr line in --headless mode."""
    def summary() -> str:
        snap = recorder.snapshot()
        details = snap["details"]
        elapsed = max(snap["elapsed_s"], 1e-9)
        rps = snap["rows_processed"] / elapsed
        p99 = snap["latency_ms"].get("p99")
        p99_str = f"{p99:.1f}ms" if p99 is not None else "—"
        return (
            "rawP={raw_p} rawR={raw_r} "
            "cleanP={clean_p} cleanR={clean_r} joined={joined} "
            "throughput={rps:,.0f} rows/s p99={p99} errors={errors}"
        ).format(
            raw_p=_fmt_int(details.get("producer_purchases_total", 0)),
            raw_r=_fmt_int(details.get("producer_refunds_total", 0)),
            clean_p=_fmt_int(details.get("stage_normalize_purchases_rows", 0)),
            clean_r=_fmt_int(details.get("stage_normalize_refunds_rows", 0)),
            joined=_fmt_int(details.get("stage_join_orders_rows", 0)),
            rps=rps,
            p99=p99_str,
            errors=snap["errors"],
        )
    return summary


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = make_parser(
        description="In-process pipeline DAG: 2 producers, 3 stages, 5 tables.",
        # v2 still validates against postgres only -- the catalog matrix
        # for embedded backends opens up in the multi-process examples.
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
    log("schema ready")

    recorder = MetricsRecorder(
        example=EXAMPLE,
        catalog=common.catalog,
        storage=common.storage,
    )
    stop = threading.Event()

    # Two-stage signal handling so an impatient operator can always escape:
    #   1st SIGINT (or SIGTERM): request graceful stop. Threads finish their
    #     current batch, lake closes, metrics get written.
    #   2nd SIGINT (within ~5s): hard exit via os._exit(130). Skips finalisers
    #     entirely so a hung lake.close() or sticky DuckDB pipeline can't
    #     trap the operator. 130 is the conventional "interrupted" exit code.
    def _request_stop(signum: int, _frame: object) -> None:
        if stop.is_set():
            log(f"signal {signum} (again) -> hard exit", level="warn")
            os._exit(130)
        log(f"signal {signum} -> stop")
        stop.set()
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    id_space = _PurchaseIdSpace()
    summary_fn = headless_summary(recorder)
    wait_seconds = effective_duration_s(common)

    producers = [
        threading.Thread(
            target=producer_purchases,
            args=(lake, recorder, stop, id_space),
            name="producer:purchases",
            daemon=True,
        ),
        threading.Thread(
            target=producer_refunds,
            args=(lake, recorder, stop, id_space),
            name="producer:refunds",
            daemon=True,
        ),
    ]

    snap: dict[str, Any] | None = None
    try:
        # ``StageRunner.__enter__`` boots all three stages serially,
        # waiting for each to register (``cdc_dml_consumer_create``)
        # before starting the next. By the time we drop into the body
        # of this ``with``, all three CDC consumers are live and the
        # ``__ducklake_cdc.*`` metadata is bootstrapped -- safe for
        # producers to start hammering the source tables.
        with StageRunner(lake, STAGES, stop=stop, recorder=recorder):
            for producer in producers:
                producer.start()

            with LiveDisplay(
                headless=common.headless,
                renderable_factory=lambda: build_layout(recorder, stop),
                headless_summary=summary_fn,
            ):
                # ``Event.wait(None)`` blocks indefinitely (demo without --duration),
                # ``Event.wait(N)`` blocks up to N seconds. Either path returns
                # early when the signal handler sets stop.
                stop.wait(wait_seconds)

            stop.set()
            for producer in producers:
                producer.join(timeout=2.0)
        # StageRunner.__exit__ joined the stage threads.
    except KeyboardInterrupt:
        stop.set()
    finally:
        stop.set()
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

    return 1 if (snap is None or snap["errors"] > 0) else 0


if __name__ == "__main__":
    sys.exit(main())
