"""04_cache_refresh: lightweight table-change ticks for cache/index refresh.

A tick consumer wakes on every new commit to ``lake.raw_ticks`` via
``cdc_dml_ticks_listen`` (metadata only -- no row materialisation),
records commit-to-tick latency against the catalog's ``snapshot_time``,
and advances the cursor. Live latency histogram in the TUI; identical
numbers in ``--headless`` mode via the periodic stderr summary.
"""

from __future__ import annotations

import os
import random
import signal
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_E2E_ROOT = Path(__file__).resolve().parent.parent
if str(_E2E_ROOT) not in sys.path:
    sys.path.insert(0, str(_E2E_ROOT))

from ducklake_cdc_client import DMLConsumer  # noqa: E402
from ducklake_client import ColumnDef, DuckLake  # noqa: E402
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
from _lib.load import LoadCorpus, LoadShape, replay  # noqa: E402
from _lib.metrics import MetricsRecorder  # noqa: E402
from _lib.tui import LiveDisplay, log  # noqa: E402

EXAMPLE = "04_cache_refresh"

# 1 row per commit, 100ms apart -> producer offers ~10 ticks/sec.
# Catalog ceiling caps the actual rate; consumer reports one tick per
# committed snapshot.
TICKS_BATCH_ROWS = 10
PRODUCER_INTERVAL_S = 0.01
CORPUS_FILE_COUNT = 50

# Long-poll budget for the tick listen call. Doubles as the upper
# bound on shutdown latency since the loop only checks ``stop`` after
# each listen returns.
LISTEN_TIMEOUT_MS = 1000
LISTEN_MAX_SNAPSHOTS = 100
LISTEN_POLL_MIN_MS = 1
LISTEN_COALESCE = False

# DMLConsumer's ``table`` parameter is a bare table name (resolved
# inside the consumer's own DuckLake catalog). SQL paths still use
# the catalog-qualified ``lake.raw_ticks`` form for INSERT/SELECT.
CONSUMER_NAME = "ticks_watcher"
CONSUMER_TABLE = "raw_ticks"
SOURCE_TABLE_SQL = "lake.raw_ticks"

# Latency histogram bin edges (ms). Last bin is the open-ended overflow.
HIST_BINS_MS: tuple[float, ...] = (5.0, 10.0, 25.0, 100.0)
HIST_LABELS: tuple[str, ...] = ("<5ms", "5-10ms", "10-25ms", "25-100ms", ">100ms")


# ---------------------------------------------------------------------------
# schema + load
# ---------------------------------------------------------------------------


def setup_schema(lake: DuckLake) -> None:
    lake.table.create(
        "raw_ticks",
        id=ColumnDef("BIGINT"),
        payload=ColumnDef("VARCHAR"),
        created_at=ColumnDef("TIMESTAMP"),
    )


def _synth_tick(corpus_idx: int, _rng: random.Random) -> tuple[str]:
    return (f"tick-{corpus_idx:08d}",)


TICKS_SHAPE = LoadShape(
    name="ticks",
    table=SOURCE_TABLE_SQL,
    parquet_columns=(("payload", "VARCHAR"),),
    synth_row=_synth_tick,
    apply_sql="""
        INSERT INTO {table}
        SELECT
            $id_base + __row_offset AS id,
            payload                 AS payload,
            now()                   AS created_at
        FROM read_parquet('{file}')
    """,
)


def producer_ticks(
    lake: Any,
    corpus: LoadCorpus,
    recorder: MetricsRecorder,
    stop: threading.Event,
) -> None:
    replay(
        lake,
        corpus,
        stop=stop,
        interval_s=PRODUCER_INTERVAL_S,
        recorder=recorder,
    )


# ---------------------------------------------------------------------------
# tick consumer
# ---------------------------------------------------------------------------


def tick_consumer(
    lake: Any,
    recorder: MetricsRecorder,
    histogram: LatencyHistogram,
    refresh_state: RefreshState,
    stop: threading.Event,
    ready: threading.Event,
) -> None:
    log(f"\\[{CONSUMER_NAME}] starting (source={SOURCE_TABLE_SQL})")
    ticks_seen = 0
    try:
        with DMLConsumer(
            lake,
            CONSUMER_NAME,
            table=CONSUMER_TABLE,
            mode="ticks",
            lease_policy="takeover",
        ) as consumer:
            ready.set()
            for batch in consumer.batches(
                stop_event=stop,
                timeout_ms=LISTEN_TIMEOUT_MS,
                max_snapshots=LISTEN_MAX_SNAPSHOTS,
                poll_min_ms=LISTEN_POLL_MIN_MS,
                coalesce=LISTEN_COALESCE,
            ):
                arrival_unix = time.time()
                for tick in batch:
                    commit_unix = (
                        tick.snapshot_time.timestamp()
                        if tick.snapshot_time is not None
                        else arrival_unix
                    )
                    latency_ms = max(0.0, (arrival_unix - commit_unix) * 1000.0)
                    ticks_seen += 1
                    recorder.record_latency_ms(latency_ms)
                    histogram.record(latency_ms)
                    refresh_state.add(tick.snapshot_id, latency_ms)

                try:
                    batch.commit()
                except Exception as exc:  # noqa: BLE001
                    recorder.record_error()
                    log(
                        f"\\[{CONSUMER_NAME}] commit failed: {type(exc).__name__}: {exc}",
                        level="error",
                    )
                    continue

                recorder.set_detail("ticks_total", ticks_seen)
                recorder.set_detail("last_snapshot", batch.end_snapshot)
    except Exception as exc:  # noqa: BLE001
        recorder.record_error()
        log(f"\\[{CONSUMER_NAME}] fatal: {type(exc).__name__}: {exc}", level="error")
    finally:
        log(f"\\[{CONSUMER_NAME}] stopped (ticks_seen={ticks_seen:,})")
        ready.set()


# ---------------------------------------------------------------------------
# latency histogram
# ---------------------------------------------------------------------------


class LatencyHistogram:
    def __init__(self, edges: tuple[float, ...] = HIST_BINS_MS) -> None:
        self._edges = edges
        self._counts = [0] * (len(edges) + 1)
        self._lock = threading.Lock()

    def record(self, value_ms: float) -> None:
        with self._lock:
            for i, edge in enumerate(self._edges):
                if value_ms < edge:
                    self._counts[i] += 1
                    return
            self._counts[-1] += 1

    def snapshot(self) -> list[int]:
        with self._lock:
            return list(self._counts)


@dataclass
class RefreshState:
    tail: deque[str] = field(default_factory=lambda: deque(maxlen=16))
    total: int = 0
    last_snapshot: int | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, snapshot_id: int, latency_ms: float) -> None:
        with self._lock:
            self.total += 1
            self.last_snapshot = snapshot_id
            self.tail.append(f"refresh cache/search @ snapshot {snapshot_id}  {_fmt_ms(latency_ms)}")

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "tail": tuple(self.tail),
                "total": self.total,
                "last_snapshot": self.last_snapshot,
            }


# ---------------------------------------------------------------------------
# TUI
# ---------------------------------------------------------------------------


def build_layout(
    recorder: MetricsRecorder,
    refresh_state: RefreshState,
) -> Layout:
    snap = recorder.snapshot()
    refresh = refresh_state.snapshot()
    layout = Layout()
    layout.split(
        Layout(_header(snap), name="header", size=3),
        Layout(name="body"),
        Layout(_footer(snap), name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(_commit_panel(snap), name="commits", ratio=2),
        Layout(_tick_panel(snap), name="ticks", ratio=2),
        Layout(_refresh_panel(refresh), name="refresh", ratio=3),
    )
    return layout


def _header(snap: dict[str, Any]) -> Panel:
    text = Text.from_markup(
        f"[bold]ducklake-cdc[/bold] · {EXAMPLE} · "
        f"{SOURCE_TABLE_SQL} → refresh signals   "
        f"[dim]elapsed[/dim] {_fmt_duration(snap['elapsed_s'])}   "
        f"[dim]errors[/dim] {snap['errors']}"
    )
    return Panel(text, border_style="dim")


def _footer(snap: dict[str, Any]) -> Panel:
    lat = snap["latency_ms"]
    text = Text()
    text.append("latency ", style="dim")
    text.append(f"p50 {_fmt_ms(lat.get('p50'))}  ")
    text.append(f"p95 {_fmt_ms(lat.get('p95'))}  ")
    text.append(f"p99 {_fmt_ms(lat.get('p99'))}")
    text.append("   Ctrl-C to stop", style="dim")
    return Panel(text, border_style="dim")


def _histogram_panel(histogram: LatencyHistogram, snap: dict[str, Any]) -> Panel:
    counts = histogram.snapshot()
    total = sum(counts) or 1
    width = 30

    table = Table.grid(padding=(0, 1))
    table.add_column(justify="right", style="dim", min_width=9)
    table.add_column(justify="left", min_width=width + 2)
    table.add_column(justify="right", min_width=8)
    for label, count in zip(HIST_LABELS, counts, strict=True):
        bar_len = round(count / total * width)
        bar = "▓" * bar_len + "░" * (width - bar_len)
        table.add_row(label, bar, _fmt_int(count))

    lat = snap["latency_ms"]
    summary = (
        f"\n[bold]p50[/bold] {_fmt_ms(lat.get('p50'))}   "
        f"[bold]p95[/bold] {_fmt_ms(lat.get('p95'))}   "
        f"[bold]p99[/bold] {_fmt_ms(lat.get('p99'))}   "
        f"[dim]n[/dim] {_fmt_int(snap['latency_count'])}"
    )

    inner = Table.grid()
    inner.add_row(table)
    inner.add_row(Text.from_markup(summary))
    return Panel(inner, title="[bold]commit → tick latency[/bold]", border_style="green")


def _commit_panel(snap: dict[str, Any]) -> Panel:
    details = snap["details"]
    elapsed = max(snap["elapsed_s"], 1e-9)
    producer_rows = int(details.get("producer_ticks_total", 0))
    producer_commits = producer_rows // TICKS_BATCH_ROWS
    producer_per_s = producer_commits / elapsed

    grid = Table.grid(padding=(0, 1))
    grid.add_column()
    grid.add_row(_metric_line("commit rate", f"{producer_per_s:,.1f}/s"))
    grid.add_row(_metric_line("snapshots", _fmt_int(producer_commits)))
    grid.add_row(_metric_line("rows", _fmt_int(producer_rows)))
    grid.add_row("")
    grid.add_row(Text("commits", style="dim"))
    grid.add_row(_dots(producer_commits, recent=min(producer_commits, 12)))
    return Panel(grid, title="DUCKLAKE COMMITS", border_style="dim")


def _tick_panel(snap: dict[str, Any]) -> Panel:
    details = snap["details"]
    elapsed = max(snap["elapsed_s"], 1e-9)
    producer_rows = int(details.get("producer_ticks_total", 0))
    producer_commits = producer_rows // TICKS_BATCH_ROWS
    ticks_total = int(details.get("ticks_total", 0))
    pending = max(0, producer_commits - ticks_total)
    ticks_per_s = ticks_total / elapsed

    grid = Table.grid(padding=(0, 1))
    grid.add_column()
    grid.add_row(_metric_line("status", "LISTENING"))
    grid.add_row(_metric_line("tick rate", f"{ticks_per_s:,.1f}/s"))
    grid.add_row(_metric_line("pending", _fmt_int(pending), style="bold yellow" if pending else "bold"))
    grid.add_row("")
    grid.add_row(Text("ticks delivered", style="dim"))
    grid.add_row(_dots(ticks_total, recent=min(ticks_total, 12)))
    grid.add_row("")
    grid.add_row(Text("pending", style="dim"))
    grid.add_row(_pending_dots(pending))
    return Panel(grid, title="CDC TICK CONSUMER", border_style="dim")


def _refresh_panel(refresh: dict[str, Any]) -> Panel:
    grid = Table.grid(padding=(0, 1))
    grid.add_column()
    grid.add_row(_metric_line("refreshes", _fmt_int(refresh["total"])))
    grid.add_row(_metric_line("last snapshot", str(refresh["last_snapshot"] or "—")))
    grid.add_row("")
    grid.add_row(Text("external refresh log", style="dim"))
    tail = list(refresh["tail"])[-14:]
    if not tail:
        grid.add_row(Text("(waiting for first tick)", style="dim"))
    else:
        for line in tail:
            grid.add_row(Text(line, no_wrap=True, overflow="ellipsis"))
    return Panel(grid, title="CACHE / SEARCH REFRESH", border_style="dim")


def _metric_line(label: str, value: str, *, style: str = "bold") -> Text:
    text = Text()
    text.append(f"{label} ", style="dim")
    text.append(value, style=style)
    return text


def _dots(total: int, *, recent: int, width: int = 36) -> Text:
    text = Text()
    dim = max(0, min(width, total - recent))
    bright = max(0, min(width - dim, recent))
    if dim:
        text.append("•" * dim, style="dim")
    if bright:
        text.append("•" * bright, style="bold")
    if not dim and not bright:
        text.append("·", style="dim")
    if total > width:
        text.append(f" +{total - width}", style="dim")
    return text


def _pending_dots(pending: int, width: int = 36) -> Text:
    text = Text()
    if pending <= 0:
        text.append("caught up", style="dim")
        return text
    shown = min(pending, width)
    text.append("•" * shown, style="bold yellow")
    if pending > shown:
        text.append(f" +{pending - shown}", style="yellow")
    return text


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


def headless_summary(
    recorder: MetricsRecorder, histogram: LatencyHistogram
) -> Callable[[], str]:
    def summary() -> str:
        snap = recorder.snapshot()
        details = snap["details"]
        elapsed = max(snap["elapsed_s"], 1e-9)
        ticks = int(details.get("ticks_total", 0))
        producer_rows = int(details.get("producer_ticks_total", 0))
        producer_commits = producer_rows // TICKS_BATCH_ROWS
        lat = snap["latency_ms"]
        return (
            f"producer_rows={_fmt_int(producer_rows)} "
            f"producer_commits={_fmt_int(producer_commits)} ticks={_fmt_int(ticks)} "
            f"rate={ticks / elapsed:,.1f}/s "
            f"p50={_fmt_ms(lat.get('p50'))} p95={_fmt_ms(lat.get('p95'))} "
            f"p99={_fmt_ms(lat.get('p99'))} hist={histogram.snapshot()} "
            f"errors={snap['errors']}"
        )
    return summary


def json_summary(recorder: MetricsRecorder, histogram: LatencyHistogram) -> dict[str, Any]:
    snap = recorder.snapshot()
    details = snap["details"]
    elapsed = max(snap["elapsed_s"], 1e-9)
    producer_rows = int(details.get("producer_ticks_total", 0))
    producer_commits = producer_rows // TICKS_BATCH_ROWS
    ticks = int(details.get("ticks_total", 0))
    return {
        "example": EXAMPLE,
        "errors": snap["errors"],
        "elapsed_s": snap["elapsed_s"],
        "producer_rows": producer_rows,
        "producer_commits": producer_commits,
        "ticks": ticks,
        "tick_rate": ticks / elapsed,
        "last_snapshot": details.get("last_snapshot"),
        "latency_ms": snap["latency_ms"],
        "histogram": histogram.snapshot(),
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = make_parser(
        description="Low-latency tick consumer (metadata-only change-tap).",
        supported_catalogs=("duckdb", "sqlite", "postgres"),
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
    histogram = LatencyHistogram()
    refresh_state = RefreshState()

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

    corpus = LoadCorpus(
        TICKS_SHAPE,
        work_dir=load_dir(EXAMPLE),
        rows_per_file=TICKS_BATCH_ROWS,
        file_count=CORPUS_FILE_COUNT,
    )
    corpus.prebuild()

    summary_fn = headless_summary(recorder, histogram)
    wait_seconds = effective_duration_s(common)

    consumer_ready = threading.Event()
    consumer_thread = threading.Thread(
        target=tick_consumer,
        args=(lake, recorder, histogram, refresh_state, stop, consumer_ready),
        name="tick-consumer",
        daemon=True,
    )
    producer_thread = threading.Thread(
        target=producer_ticks,
        args=(lake, corpus, recorder, producers_stop),
        name="producer:ticks",
        daemon=True,
    )

    snap: dict[str, Any] | None = None
    try:
        consumer_thread.start()
        if not consumer_ready.wait(timeout=10.0):
            raise RuntimeError("tick consumer did not become ready within 10s")

        producer_thread.start()

        with LiveDisplay(
            headless=common.headless,
            renderable_factory=lambda: build_layout(recorder, refresh_state),
            headless_summary=summary_fn,
        ):
            stop.wait(wait_seconds)

        producers_stop.set()
        producer_thread.join(timeout=2.0)
        # Give the consumer one more listen cycle to drain any final ticks.
        time.sleep(LISTEN_TIMEOUT_MS / 1000.0)
        stop.set()
        consumer_thread.join(timeout=LISTEN_TIMEOUT_MS / 1000.0 + 2.0)
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
        emit_json_summary(common, json_summary(recorder, histogram))

        try:
            reset_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)
        except Exception as exc:  # noqa: BLE001
            log(f"cleanup failed (workload result still valid): {exc!r}", level="warn")

    return 1 if (snap is None or snap["errors"] > 0) else 0


if __name__ == "__main__":
    sys.exit(main())
