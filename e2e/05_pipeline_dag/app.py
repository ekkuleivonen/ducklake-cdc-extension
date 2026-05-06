"""05_pipeline_dag: in-process multi-stage CDC pipeline against one DuckLake.

Two parallel normalize stages feed a snapshot-lookup join, all in one
Python process, exactly-once via ``cdc_commit`` / ``COMMIT`` atomicity::

    raw_purchases → normalize_purchases → clean_purchases ┐
                                                          ├→ join_orders → joined_orders
    raw_refunds   → normalize_refunds   → clean_refunds  ─┘

Run once, watch the TUI (or ``--headless`` for stderr summary), Ctrl-C
or ``--duration N`` to stop. The lake is reset before and after each run.
"""

from __future__ import annotations

import json
import os
import random
import signal
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any
from ducklake_client import DuckLake, ColumnDef

_E2E_ROOT = Path(__file__).resolve().parent.parent
if str(_E2E_ROOT) not in sys.path:
    sys.path.insert(0, str(_E2E_ROOT))

from ducklake_cdc_client import BatchTransaction, DMLBatch  # noqa: E402
from rich.layout import Layout  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.text import Text  # noqa: E402

from _lib.cli import effective_duration_s, emit_json_summary, make_parser, parse_common  # noqa: E402
from _lib.config import (  # noqa: E402
    load_cdc_extension,
    load_dir,
    open_lake,
    reset_lake,
)
from _lib.load import IdSpace, LoadCorpus, LoadShape, replay  # noqa: E402
from _lib.metrics import MetricsRecorder  # noqa: E402
from _lib.stage import INSERTS_ONLY, Stage, StageRunner  # noqa: E402
from _lib.tui import LiveDisplay, log  # noqa: E402

EXAMPLE = "05_pipeline_dag"

# Producer cadence balances throughput vs the strict 1:1 drain
# invariant on shutdown. ~1k rows/s offered keeps consumers ahead so
# every emitted row makes it through the DAG by drain time.
PURCHASES_BATCH_ROWS = 500
REFUNDS_BATCH_ROWS = 125
PRODUCER_BATCH_INTERVAL_S = 0.5
CORPUS_FILE_COUNT = 20

# Wait for this many emitted purchases before the first refund batch
# so refund.purchase_id has something legitimate to reference.
REFUND_WARMUP_PURCHASES = 100

LISTEN_TIMEOUT_MS = 1000
LISTEN_MAX_SNAPSHOTS = 1000

KIND_CHOICES = ("purchase", "refund", "subscription_renewal", "trial_signup")


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------


def setup_schema(lake: DuckLake) -> None:
    lake.table.create(
        'raw_purchases',
        id=ColumnDef("BIGINT"),
        payload=ColumnDef("JSON"),
        created_at=ColumnDef("TIMESTAMP"),
    )
    lake.table.create(
        'raw_refunds',
        id=ColumnDef("BIGINT"),
        purchase_id=ColumnDef("BIGINT"),
        refund_amount=ColumnDef("DOUBLE"),
        created_at=ColumnDef("TIMESTAMP"),
    )
    lake.table.create(
        'clean_purchases',
        id=ColumnDef("BIGINT"),
        kind=ColumnDef("VARCHAR"),
        amount=ColumnDef("DOUBLE"),
        source_snapshot=ColumnDef("BIGINT"),
        cleaned_at=ColumnDef("TIMESTAMP"),
    )
    lake.table.create(
        'clean_refunds',
        id=ColumnDef("BIGINT"),
        purchase_id=ColumnDef("BIGINT"),
        refund_amount=ColumnDef("DOUBLE"),
        source_snapshot=ColumnDef("BIGINT"),
        cleaned_at=ColumnDef("TIMESTAMP"),
    )
    lake.table.create(
        'joined_orders',
        purchase_id=ColumnDef("BIGINT"),
        kind=ColumnDef("VARCHAR"),
        amount=ColumnDef("DOUBLE"),
        refund_amount=ColumnDef("DOUBLE"),
        source_snapshot=ColumnDef("BIGINT"),
        joined_at=ColumnDef("TIMESTAMP"),
    )


# ---------------------------------------------------------------------------
# producers (declarative LoadShapes; ``_lib.load.replay`` runs them)
# ---------------------------------------------------------------------------


def _synth_purchase(_corpus_idx: int, rng: random.Random) -> tuple[str]:
    payload = {
        "kind": rng.choice(KIND_CHOICES),
        "amount": round(rng.uniform(1.0, 1000.0), 2),
    }
    return (json.dumps(payload),)


def _synth_refund(_corpus_idx: int, rng: random.Random) -> tuple[int, float]:
    purchase_offset = rng.randint(0, 1_000_000_000)
    refund_amount = round(rng.uniform(1.0, 500.0), 2)
    return purchase_offset, refund_amount


PURCHASES_SHAPE = LoadShape(
    name="purchases",
    table="lake.raw_purchases",
    parquet_columns=(("payload", "JSON"),),
    synth_row=_synth_purchase,
    apply_sql="""
        INSERT INTO {table}
        SELECT
            $id_base + __row_offset AS id,
            payload                 AS payload,
            now()                   AS created_at
        FROM read_parquet('{file}')
    """,
)

# ``purchase_offset % $purchase_max`` keeps refund.purchase_id within
# the live emitted purchase id-space regardless of run length.
REFUNDS_SHAPE = LoadShape(
    name="refunds",
    table="lake.raw_refunds",
    parquet_columns=(
        ("purchase_offset", "BIGINT"),
        ("refund_amount", "DOUBLE"),
    ),
    synth_row=_synth_refund,
    apply_sql="""
        INSERT INTO {table}
        SELECT
            $id_base + __row_offset                                   AS id,
            (purchase_offset % CAST($purchase_max AS BIGINT))::BIGINT AS purchase_id,
            refund_amount                                             AS refund_amount,
            now()                                                     AS created_at
        FROM read_parquet('{file}')
    """,
)


def producer_purchases(
    lake: Any,
    corpus: LoadCorpus,
    recorder: MetricsRecorder,
    stop: threading.Event,
    id_space: IdSpace,
) -> None:
    replay(
        lake,
        corpus,
        stop=stop,
        interval_s=PRODUCER_BATCH_INTERVAL_S,
        recorder=recorder,
        id_space=id_space,
    )


def producer_refunds(
    lake: Any,
    corpus: LoadCorpus,
    recorder: MetricsRecorder,
    stop: threading.Event,
    id_space: IdSpace,
) -> None:
    replay(
        lake,
        corpus,
        stop=stop,
        interval_s=PRODUCER_BATCH_INTERVAL_S,
        recorder=recorder,
        wait_until=lambda: id_space.max_emitted() >= REFUND_WARMUP_PURCHASES,
        extra_params_fn=lambda: {"purchase_max": max(1, id_space.max_emitted())},
    )


# ---------------------------------------------------------------------------
# stages
# ---------------------------------------------------------------------------


def normalize_purchases(batch: DMLBatch, tx: BatchTransaction) -> int:
    """raw_purchases -> clean_purchases (decode JSON payload)."""
    ids: list[int] = []
    kinds: list[str] = []
    amounts: list[float] = []
    snapshots: list[int] = []
    for change in batch:
        payload = change.values["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        ids.append(int(change.values["id"]))
        kinds.append(str(payload["kind"]))
        amounts.append(float(payload["amount"]))
        snapshots.append(int(change.snapshot_id))
    if not ids:
        return 0

    tx.execute(
        """
        INSERT INTO lake.clean_purchases
        SELECT
            UNNEST(?::BIGINT[])  AS id,
            UNNEST(?::VARCHAR[]) AS kind,
            UNNEST(?::DOUBLE[])  AS amount,
            UNNEST(?::BIGINT[])  AS source_snapshot,
            now()                AS cleaned_at
        """,
        [ids, kinds, amounts, snapshots],
    )
    return len(ids)


def normalize_refunds(batch: DMLBatch, tx: BatchTransaction) -> int:
    """raw_refunds -> clean_refunds (typed passthrough + source_snapshot)."""
    ids: list[int] = []
    purchase_ids: list[int] = []
    amounts: list[float] = []
    snapshots: list[int] = []
    for change in batch:
        ids.append(int(change.values["id"]))
        purchase_ids.append(int(change.values["purchase_id"]))
        amounts.append(float(change.values["refund_amount"]))
        snapshots.append(int(change.snapshot_id))
    if not ids:
        return 0

    tx.execute(
        """
        INSERT INTO lake.clean_refunds
        SELECT
            UNNEST(?::BIGINT[]) AS id,
            UNNEST(?::BIGINT[]) AS purchase_id,
            UNNEST(?::DOUBLE[]) AS refund_amount,
            UNNEST(?::BIGINT[]) AS source_snapshot,
            now()               AS cleaned_at
        """,
        [ids, purchase_ids, amounts, snapshots],
    )
    return len(ids)


def join_orders(batch: DMLBatch, tx: BatchTransaction) -> int:
    """clean_purchases (driver) -> joined_orders, snapshot-lookup of clean_refunds.

    Refunds that arrive after the lookup snapshot leave ``refund_amount``
    NULL forever -- the trade-off of a snapshot-lookup join. Multi-match
    refunds are folded with MAX (domain choice; swap for SUM/MIN as needed).
    """
    if not batch.changes:
        return 0

    purchase_ids: list[int] = []
    kinds: list[str] = []
    amounts: list[float] = []
    for change in batch:
        purchase_ids.append(int(change.values["id"]))
        kinds.append(str(change.values["kind"]))
        amounts.append(float(change.values["amount"]))

    tx.execute(
        """
        INSERT INTO lake.joined_orders
        WITH driver AS (
            SELECT
                UNNEST(?::BIGINT[])  AS purchase_id,
                UNNEST(?::VARCHAR[]) AS kind,
                UNNEST(?::DOUBLE[])  AS amount
        ),
        agg AS (
            SELECT purchase_id, MAX(refund_amount) AS refund_amount
            FROM lake.clean_refunds
            WHERE purchase_id IN (SELECT purchase_id FROM driver)
            GROUP BY purchase_id
        )
        SELECT
            d.purchase_id,
            d.kind,
            d.amount,
            a.refund_amount,
            ?               AS source_snapshot,
            now()           AS joined_at
        FROM driver d
        LEFT JOIN agg a USING (purchase_id)
        """,
        [purchase_ids, kinds, amounts, batch.end_snapshot],
    )
    return len(batch.changes)


STAGES: tuple[Stage, ...] = (
    Stage("normalize_purchases", source="raw_purchases", transform=normalize_purchases, change_types=INSERTS_ONLY),
    Stage("normalize_refunds", source="raw_refunds", transform=normalize_refunds, change_types=INSERTS_ONLY),
    Stage("join_orders", source="clean_purchases", transform=join_orders, change_types=INSERTS_ONLY),
)


# ---------------------------------------------------------------------------
# TUI
# ---------------------------------------------------------------------------


def build_layout(recorder: MetricsRecorder, stop: threading.Event) -> Layout:
    snap = recorder.snapshot()
    layout = Layout()
    layout.split(
        Layout(_header(snap), name="header", size=3),
        Layout(name="body"),
        Layout(_footer(snap, stop), name="footer", size=3),
    )
    layout["body"].split(
        Layout(_dag_panel(snap), name="dag", ratio=5),
        Layout(_cursor_panel(snap), name="cursors", ratio=2),
    )
    return layout


def _header(snap: dict[str, Any]) -> Panel:
    text = Text.from_markup(
        f"[bold]ducklake-cdc[/bold] · {EXAMPLE} · "
        f"raw tables → clean tables → joined_orders   "
        f"[dim]elapsed[/dim] {_fmt_duration(snap['elapsed_s'])}   "
        f"[dim]errors[/dim] {snap['errors']}"
    )
    return Panel(text, border_style="dim")


def _footer(snap: dict[str, Any], _stop: threading.Event) -> Panel:
    lat = snap["latency_ms"]
    text = Text()
    text.append("apply latency ", style="dim")
    text.append(f"p50 {_fmt_ms(lat.get('p50'))}  ")
    text.append(f"p95 {_fmt_ms(lat.get('p95'))}  ")
    text.append(f"p99 {_fmt_ms(lat.get('p99'))}")
    text.append("   Ctrl-C to stop", style="dim")
    return Panel(text, border_style="dim")


def _dag_panel(snap: dict[str, Any]) -> Panel:
    details = snap["details"]

    raw_purchases = int(details.get("producer_purchases_total", 0))
    raw_refunds = int(details.get("producer_refunds_total", 0))
    clean_purchases = int(details.get("stage_normalize_purchases_rows", 0))
    clean_refunds = int(details.get("stage_normalize_refunds_rows", 0))
    joined_orders = int(details.get("stage_join_orders_rows", 0))

    graph = Table.grid(expand=True, padding=(0, 1))
    for i in range(9):
        graph.add_column(ratio=4 if i % 2 == 0 else 1)
    graph.add_row(
        _node_panel("raw_purchases", raw_purchases, "source table"),
        _arrow(),
        _node_panel(
            "normalize_purchases",
            clean_purchases,
            "stage",
            snap=details.get("stage_normalize_purchases_last_snapshot"),
        ),
        _arrow(),
        _node_panel("clean_purchases", clean_purchases, "clean table"),
        _join_connector("down"),
        "",
        "",
        "",
    )
    graph.add_row(
        "",
        "",
        "",
        "",
        "",
        _join_connector("into"),
        _node_panel(
            "join_orders",
            joined_orders,
            "stage",
            snap=details.get("stage_join_orders_last_snapshot"),
        ),
        _arrow(),
        _node_panel("joined_orders", joined_orders, "output table"),
    )
    graph.add_row(
        _node_panel("raw_refunds", raw_refunds, "source table"),
        _arrow(),
        _node_panel(
            "normalize_refunds",
            clean_refunds,
            "stage",
            snap=details.get("stage_normalize_refunds_last_snapshot"),
        ),
        _arrow(),
        _node_panel("clean_refunds", clean_refunds, "clean table"),
        _join_connector("up"),
        "",
        "",
        "",
    )

    return Panel(graph, title="PIPELINE DAG", border_style="dim")


def _cursor_panel(snap: dict[str, Any]) -> Panel:
    details = snap["details"]
    elapsed = max(snap["elapsed_s"], 1e-9)
    table = Table.grid(padding=(0, 2))
    table.add_column()
    table.add_column(justify="right")
    table.add_column(justify="right")
    table.add_column(justify="right")
    table.add_column(justify="right")
    table.add_row(
        Text("stage", style="dim"),
        Text("rows", style="dim"),
        Text("batches", style="dim"),
        Text("rate", style="dim"),
        Text("snap", style="dim"),
    )
    for stage in STAGES:
        table.add_row(*_stage_cells(details, stage.name, elapsed))
    return Panel(table, title="STAGE CURSORS", border_style="dim")


def _node_panel(name: str, rows: int, label: str, *, snap: Any = None) -> Panel:
    grid = Table.grid(padding=(0, 1))
    grid.add_column()
    grid.add_row(Text(label, style="dim"))
    grid.add_row(Text(name, style="bold"))

    count = Text()
    count.append(_fmt_int(rows), style="bold")
    count.append(" rows", style="dim")
    grid.add_row(count)
    grid.add_row(_sparkline(rows))
    if snap is not None:
        grid.add_row(Text(f"snap {snap}", style="dim"))
    return Panel(grid, border_style="dim", padding=(0, 1))


def _arrow() -> Text:
    return Text("\n\n──▶", style="dim")


def _join_connector(direction: str) -> Text:
    if direction == "down":
        return Text("\n\n──┐\n  │", style="dim")
    if direction == "up":
        return Text("  │\n\n──┘", style="dim")
    return Text("\n\n──▶", style="dim")


def _sparkline(total: int, width: int = 10) -> Text:
    text = Text()
    filled = min(width, total // 1_000)
    text.append("■" * filled, style="bold")
    text.append("□" * (width - filled), style="dim")
    return text


def _stage_cells(details: dict[str, Any], name: str, elapsed: float) -> tuple[Text, str, str, str, str]:
    rows = int(details.get(f"stage_{name}_rows", 0))
    batches = int(details.get(f"stage_{name}_batches", 0))
    snap_id = details.get(f"stage_{name}_last_snapshot", "—")
    rps = rows / elapsed
    return (
        Text(name, style="bold"),
        _fmt_int(rows),
        _fmt_int(batches),
        f"{rps:,.0f}/s",
        str(snap_id),
    )


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
    def summary() -> str:
        snap = recorder.snapshot()
        details = snap["details"]
        elapsed = max(snap["elapsed_s"], 1e-9)
        rps = snap["rows_processed"] / elapsed
        p99 = snap["latency_ms"].get("p99")
        p99_str = f"{p99:.1f}ms" if p99 is not None else "—"
        return (
            f"rawP={_fmt_int(details.get('producer_purchases_total', 0))} "
            f"rawR={_fmt_int(details.get('producer_refunds_total', 0))} "
            f"cleanP={_fmt_int(details.get('stage_normalize_purchases_rows', 0))} "
            f"cleanR={_fmt_int(details.get('stage_normalize_refunds_rows', 0))} "
            f"joined={_fmt_int(details.get('stage_join_orders_rows', 0))} "
            f"throughput={rps:,.0f} rows/s p99={p99_str} errors={snap['errors']}"
        )
    return summary


def json_summary(recorder: MetricsRecorder) -> dict[str, Any]:
    snap = recorder.snapshot()
    details = snap["details"]
    elapsed = max(snap["elapsed_s"], 1e-9)
    return {
        "example": EXAMPLE,
        "errors": snap["errors"],
        "elapsed_s": snap["elapsed_s"],
        "rows_processed": snap["rows_processed"],
        "throughput_rows_per_s": snap["rows_processed"] / elapsed,
        "raw_purchases": int(details.get("producer_purchases_total", 0)),
        "raw_refunds": int(details.get("producer_refunds_total", 0)),
        "clean_purchases": int(details.get("stage_normalize_purchases_rows", 0)),
        "clean_refunds": int(details.get("stage_normalize_refunds_rows", 0)),
        "joined_orders": int(details.get("stage_join_orders_rows", 0)),
        "latency_ms": snap["latency_ms"],
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = make_parser(
        description="In-process pipeline DAG: 2 producers, 3 stages, 5 tables.",
        supported_catalogs=("postgres",),
        supported_storages=("disk", "s3"),
    )
    args = parser.parse_args(argv)
    common = parse_common(args)

    log(
        f"starting {EXAMPLE} mode={'headless' if common.headless else 'demo'} "
        f"catalog={common.catalog} storage={common.storage}"
    )

    reset_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)

    lake = open_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)
    load_cdc_extension(lake)
    setup_schema(lake)
    log("schema ready")

    recorder = MetricsRecorder(
        example=EXAMPLE,
        catalog=common.catalog,
        storage=common.storage,
    )

    # ``stop`` = panic (Ctrl-C / 2nd signal); ``producers_stop`` =
    # graceful "no new upstream writes" so stages can drain in-flight
    # batches before shutdown.
    stop = threading.Event()
    producers_stop = threading.Event()

    def _request_stop(signum: int, _frame: object) -> None:
        if stop.is_set():
            log(f"signal {signum} (again) -> hard exit", level="warn")
            os._exit(130)
        log(f"signal {signum} -> stop")
        producers_stop.set()
        stop.set()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    id_space = IdSpace()
    summary_fn = headless_summary(recorder)
    wait_seconds = effective_duration_s(common)

    corpus_root = load_dir(EXAMPLE)
    purchases_corpus = LoadCorpus(
        PURCHASES_SHAPE,
        work_dir=corpus_root,
        rows_per_file=PURCHASES_BATCH_ROWS,
        file_count=CORPUS_FILE_COUNT,
    )
    refunds_corpus = LoadCorpus(
        REFUNDS_SHAPE,
        work_dir=corpus_root,
        rows_per_file=REFUNDS_BATCH_ROWS,
        file_count=CORPUS_FILE_COUNT,
    )
    purchases_corpus.prebuild()
    refunds_corpus.prebuild()

    producers = [
        threading.Thread(
            target=producer_purchases,
            args=(lake, purchases_corpus, recorder, producers_stop, id_space),
            name="producer:purchases",
            daemon=True,
        ),
        threading.Thread(
            target=producer_refunds,
            args=(lake, refunds_corpus, recorder, producers_stop, id_space),
            name="producer:refunds",
            daemon=True,
        ),
    ]

    snap: dict[str, Any] | None = None
    try:
        # ``drain_timeout_s`` above the lib default: ``join_orders``
        # commit time grows with ``clean_refunds`` size, and one
        # trailing batch can take a few seconds.
        with StageRunner(lake, STAGES, stop=stop, recorder=recorder, drain_timeout_s=15.0):
            for producer in producers:
                producer.start()

            with LiveDisplay(
                headless=common.headless,
                renderable_factory=lambda: build_layout(recorder, stop),
                headless_summary=summary_fn,
            ):
                stop.wait(wait_seconds)

            producers_stop.set()
            for producer in producers:
                producer.join(timeout=2.0)
    except KeyboardInterrupt:
        stop.set()
        producers_stop.set()
    finally:
        stop.set()
        producers_stop.set()
        try:
            lake.close()
        except Exception as exc:  # noqa: BLE001
            log(f"lake.close raised: {exc!r}", level="warn")

        snap = recorder.snapshot()
        log(summary_fn())
        emit_json_summary(common, json_summary(recorder))

        try:
            reset_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)
        except Exception as exc:  # noqa: BLE001
            log(f"cleanup failed (workload result still valid): {exc!r}", level="warn")

    return 1 if (snap is None or snap["errors"] > 0) else 0


if __name__ == "__main__":
    sys.exit(main())
