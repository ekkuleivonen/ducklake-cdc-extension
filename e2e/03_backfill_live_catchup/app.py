"""03_backfill_live_catchup: historical read drain, restart gap, then live listen."""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

_E2E_ROOT = Path(__file__).resolve().parent.parent
if str(_E2E_ROOT) not in sys.path:
    sys.path.insert(0, str(_E2E_ROOT))

from ducklake_cdc_client import DMLConsumer  # noqa: E402
from ducklake_cdc_client.client import CDCClient  # noqa: E402
from ducklake_cdc_client.enums import ChangeType  # noqa: E402
from ducklake_cdc_client.types import Change, DMLBatch  # noqa: E402
from ducklake_client import ColumnDef  # noqa: E402
from rich.layout import Layout  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.text import Text  # noqa: E402

from _lib.cli import emit_json_summary, make_parser, parse_common  # noqa: E402
from _lib.config import catalog_head_snapshot, load_cdc_extension, open_lake, reset_lake  # noqa: E402
from _lib.metrics import MetricsRecorder  # noqa: E402
from _lib.tui import LiveDisplay, log  # noqa: E402

EXAMPLE = "03_backfill_live_catchup"

SOURCE_TABLE = "lake.orders"
SINK_TABLE = "lake.orders_mirror"
CONSUMER_NAME = "orders_mirror_consumer"

READ_MAX_SNAPSHOTS = 30
LISTEN_TIMEOUT_MS = 600
LISTEN_MAX_SNAPSHOTS = 20
POLL_MIN_MS = 1

LAG_OK_SNAPSHOTS = 2
PHASE_TICK_S = 0.35
RESTART_SECONDS = 4.0
CATCHUP_HOLD_S = 1.0
DONE_HOLD_S = 4.0  # leave the green DONE state up long enough to notice

DemoPhase = Literal[
    "boot",
    "live_before_restart",
    "restarting_a",
    "catchup_a",
    "restarting_b",
    "catchup_b",
    "live_after_restart",
    "done",
    "error",
]


@dataclass
class DemoState:
    phase: DemoPhase = "boot"
    phase_detail: str = ""
    consumer_mode: Literal["OFFLINE", "READ", "LISTEN"] = "OFFLINE"
    producer_tail: deque[str] = field(default_factory=lambda: deque(maxlen=16))
    consumer_tail: deque[str] = field(default_factory=lambda: deque(maxlen=16))
    sink_tail: deque[str] = field(default_factory=lambda: deque(maxlen=16))
    # Timestamps of recent producer inserts; used to render an inserts/s
    # rate so the producer panel reads as a *speed*, not just a tail.
    producer_times: deque[float] = field(default_factory=lambda: deque(maxlen=64))
    head_snapshot: int | None = None
    committed_snapshot: int | None = None
    lag_snapshots: int | None = None
    lag_peak: int = 0
    source_rows: int = 0
    sink_rows: int = 0
    batches_applied: int = 0
    producer_inserts: int = 0
    invariant: str = "pending"
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def update(self, **values: Any) -> None:
        with self._lock:
            for key, value in values.items():
                setattr(self, key, value)
            if "lag_snapshots" in values and values["lag_snapshots"] is not None:
                self.lag_peak = max(self.lag_peak, int(values["lag_snapshots"]))

    def add_producer(self, message: str) -> None:
        with self._lock:
            self.producer_tail.append(message)
            self.producer_times.append(time.monotonic())

    def add_consumer(self, message: str) -> None:
        with self._lock:
            self.consumer_tail.append(message)

    def add_sink(self, message: str) -> None:
        with self._lock:
            self.sink_tail.append(message)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            window_s = 3.0
            recent = [t for t in self.producer_times if now - t <= window_s]
            rate = len(recent) / window_s if recent else 0.0
            return {
                "phase": self.phase,
                "phase_detail": self.phase_detail,
                "consumer_mode": self.consumer_mode,
                "producer_tail": tuple(self.producer_tail),
                "consumer_tail": tuple(self.consumer_tail),
                "sink_tail": tuple(self.sink_tail),
                "head_snapshot": self.head_snapshot,
                "committed_snapshot": self.committed_snapshot,
                "lag_snapshots": self.lag_snapshots,
                "lag_peak": self.lag_peak,
                "source_rows": self.source_rows,
                "sink_rows": self.sink_rows,
                "batches_applied": self.batches_applied,
                "producer_inserts": self.producer_inserts,
                "producer_rate": rate,
                "invariant": self.invariant,
            }


def setup_schema(lake: Any) -> None:
    lake.table.create(
        "orders",
        id=ColumnDef("BIGINT"),
        amount=ColumnDef("DOUBLE"),
        updated_at=ColumnDef("TIMESTAMP"),
    )
    lake.table.create(
        "orders_mirror",
        id=ColumnDef("BIGINT"),
        amount=ColumnDef("DOUBLE"),
        updated_at=ColumnDef("TIMESTAMP"),
    )


def head_snapshot(lake: Any) -> int:
    return catalog_head_snapshot(lake)


def refresh_counts(lake: Any, state: DemoState) -> None:
    conn = lake.connection
    src = int(conn.execute(f"SELECT count(*) FROM {SOURCE_TABLE}").fetchone()[0])
    snk = int(conn.execute(f"SELECT count(*) FROM {SINK_TABLE}").fetchone()[0])
    state.update(source_rows=src, sink_rows=snk)


def refresh_lag(lake: Any, state: DemoState) -> None:
    head = head_snapshot(lake)
    committed = state.snapshot()["committed_snapshot"]
    lag = None if committed is None else max(0, head - committed)
    state.update(head_snapshot=head, lag_snapshots=lag)


def insert_order(lake: Any, order_id: int, amount: float, state: DemoState) -> None:
    lake.connection.execute(
        f"""
        INSERT INTO {SOURCE_TABLE} (id, amount, updated_at)
        VALUES (?, ?, now())
        """,
        [order_id, amount],
    )
    state.update(producer_inserts=state.snapshot()["producer_inserts"] + 1)
    state.add_producer(f"INSERT id={order_id} amount={amount:.2f}")
    refresh_counts(lake, state)


def apply_sink_change(tx: Any, change: Change) -> None:
    if change.kind != ChangeType.INSERT:
        return
    values = dict(change.values)
    oid = values.get("id")
    if oid is None:
        return
    amt = values.get("amount")
    ts = values.get("updated_at")
    tx.execute(f"DELETE FROM {SINK_TABLE} WHERE id = ?", [oid])
    tx.execute(
        f"INSERT INTO {SINK_TABLE} (id, amount, updated_at) VALUES (?, ?, ?)",
        [oid, amt, ts],
    )


def apply_batch(
    batch: DMLBatch,
    state: DemoState,
    recorder: MetricsRecorder,
) -> int:
    n = 0
    with batch.transaction() as tx:
        for change in batch.changes:
            if change.kind == ChangeType.INSERT:
                apply_sink_change(tx, change)
                values = dict(change.values)
                state.add_sink(
                    f"id={values.get('id')} amount={float(values.get('amount') or 0):.2f} "
                    f"snap={change.snapshot_id}"
                )
                n += 1
    state.update(
        committed_snapshot=batch.end_snapshot,
        batches_applied=state.snapshot()["batches_applied"] + 1,
    )
    recorder.record_rows(n)
    state.add_consumer(
        f"committed snap {batch.end_snapshot} rows={len(batch.changes)} "
        f"[{batch.start_snapshot}→{batch.end_snapshot}]"
    )
    return n


def drain_read_batch(
    consumer: DMLConsumer,
    lake: Any,
    state: DemoState,
    recorder: MetricsRecorder,
) -> bool:
    state.update(consumer_mode="READ")
    batch = consumer.read(max_snapshots=READ_MAX_SNAPSHOTS)
    if batch is None:
        return False
    if not isinstance(batch, DMLBatch):
        raise TypeError(f"expected DMLBatch, got {type(batch).__name__}")
    apply_batch(batch, state, recorder)
    refresh_counts(lake, state)
    refresh_lag(lake, state)
    return True


def drain_listen_batch(
    consumer: DMLConsumer,
    lake: Any,
    state: DemoState,
    recorder: MetricsRecorder,
) -> bool:
    state.update(consumer_mode="LISTEN")
    batch = consumer.listen(
        timeout_ms=LISTEN_TIMEOUT_MS,
        max_snapshots=LISTEN_MAX_SNAPSHOTS,
        poll_min_ms=POLL_MIN_MS,
        coalesce=False,
    )
    if batch is None:
        refresh_lag(lake, state)
        return False
    if not isinstance(batch, DMLBatch):
        raise TypeError(f"expected DMLBatch, got {type(batch).__name__}")
    apply_batch(batch, state, recorder)
    refresh_counts(lake, state)
    refresh_lag(lake, state)
    return True


def catch_up_reads(
    lake: Any,
    consumer: DMLConsumer,
    state: DemoState,
    recorder: MetricsRecorder,
    stop: threading.Event,
    *,
    label: str,
    max_idle_rounds: int = 60,
) -> None:
    state.update(phase_detail=label)
    idle = 0
    while idle < max_idle_rounds:
        if stop.is_set():
            break
        refresh_lag(lake, state)
        progressed = drain_read_batch(consumer, lake, state, recorder)
        if not progressed:
            idle += 1
        else:
            idle = 0
        lag = state.snapshot()["lag_snapshots"]
        if lag is not None and lag <= LAG_OK_SNAPSHOTS and not progressed:
            break
        stop.wait(PHASE_TICK_S)


def verify_invariant(lake: Any) -> str:
    mismatch = int(
        lake.connection.execute(
            f"""
            SELECT (
                SELECT count(*) FROM (
                    SELECT id, round(amount, 6) FROM {SOURCE_TABLE}
                    EXCEPT
                    SELECT id, round(amount, 6) FROM {SINK_TABLE}
                )
            ) + (
                SELECT count(*) FROM (
                    SELECT id, round(amount, 6) FROM {SINK_TABLE}
                    EXCEPT
                    SELECT id, round(amount, 6) FROM {SOURCE_TABLE}
                )
            )
            """
        ).fetchone()[0]
    )
    return "ok" if mismatch == 0 else f"mismatch={mismatch}"


def open_consumer(*, lake: Any, start_at: str, on_exists: str) -> DMLConsumer:
    consumer = DMLConsumer(
        lake,
        CONSUMER_NAME,
        table="orders",
        start_at=start_at,
        mode="changes",
        on_exists=on_exists,
        connection=lake.connection,
        lease_policy="takeover" if on_exists == "use" else "wait",
    )
    consumer.__enter__()
    consumer.listen(timeout_ms=1, max_snapshots=1, poll_min_ms=POLL_MIN_MS)
    return consumer


def close_consumer(consumer: DMLConsumer | None) -> None:
    if consumer is not None:
        consumer.__exit__(None, None, None)


def drain_catalog_consumer(client: CDCClient, name: str) -> None:
    try:
        client.cdc_consumer_force_release(name)
    except Exception:
        pass


def build_layout(recorder: MetricsRecorder, state: DemoState) -> Layout:
    snap = recorder.snapshot()
    view = state.snapshot()
    layout = Layout()
    layout.split(
        Layout(_header(snap, view), name="header", size=8),
        Layout(name="body"),
        Layout(_footer(view), name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(_producer_panel(view), ratio=2, name="producer"),
        Layout(_consumer_panel(view), ratio=2, name="consumer"),
        Layout(_sink_panel(view), ratio=3, name="sink"),
    )
    return layout


def _header(snap: dict[str, Any], view: dict[str, Any]) -> Panel:
    title_line = Text()
    title_line.append("ducklake-cdc", style="bold")
    title_line.append(" · ", style="dim")
    title_line.append(EXAMPLE)
    title_line.append("    elapsed ", style="dim")
    title_line.append(_fmt_duration(snap["elapsed_s"]))
    title_line.append("    errors ", style="dim")
    title_line.append(str(snap["errors"]))

    state_line = Text()
    state_line.append("producer ", style="dim")
    state_line.append(f"{view['producer_rate']:.1f}/s", style="bold")
    state_line.append("   consumer ", style="dim")
    state_line.append(view["consumer_mode"], style="bold red" if view["consumer_mode"] == "OFFLINE" else "bold")
    state_line.append("   backlog ", style="dim")
    state_line.append(str(view["lag_snapshots"] or 0), style="bold yellow" if (view["lag_snapshots"] or 0) else "bold")
    state_line.append("   sink rows ", style="dim")
    state_line.append(str(view["sink_rows"]), style="bold")

    grid = Table.grid()
    grid.add_column()
    grid.add_row(title_line)
    grid.add_row(state_line)
    return Panel(grid, border_style="dim", padding=(0, 1))


def _footer(view: dict[str, Any]) -> Panel:
    invariant = view["invariant"]
    text = Text(no_wrap=True, overflow="ellipsis")
    text.append(f"orders {view['source_rows']:>4}")
    text.append("  mirror ", style="dim")
    text.append(f"{view['sink_rows']:>4}")
    text.append("  batches ", style="dim")
    text.append(str(view["batches_applied"]), style="bold")
    text.append("  inserts ", style="dim")
    text.append(str(view["producer_inserts"]), style="bold")
    text.append("  peak lag ", style="dim")
    text.append(str(view["lag_peak"]), style="bold")
    text.append("  invariant ", style="dim")
    if invariant == "ok":
        text.append("ok", style="bold")
    elif invariant == "pending":
        text.append(" pending ", style="dim")
    else:
        text.append(invariant, style="bold")
    return Panel(text, border_style="dim", padding=(0, 1))


def _producer_panel(view: dict[str, Any]) -> Panel:
    rate = view["producer_rate"]

    rate_line = Text()
    rate_line.append("rate ", style="dim")
    rate_line.append(f"{rate:>4.1f}/s", style="bold" if rate > 0 else "dim")
    rate_line.append("  total ", style="dim")
    rate_line.append(str(view["producer_inserts"]), style="bold")

    grid = Table.grid(padding=(0, 1))
    grid.add_column()
    grid.add_row(rate_line)
    grid.add_row("")
    grid.add_row(Text("events", style="dim"))
    grid.add_row(_dots(view["producer_inserts"], recent=len(view["producer_tail"])))
    return Panel(grid, title="PRODUCER", border_style="dim")


def _consumer_panel(view: dict[str, Any]) -> Panel:
    mode = view["consumer_mode"]
    backlog = int(view["lag_snapshots"] or 0)

    summary = Text()
    summary.append("status ", style="dim")
    summary.append(mode, style="bold red" if mode == "OFFLINE" else "bold")
    summary.append("  backlog ", style="dim")
    summary.append(str(backlog), style="bold yellow" if backlog else "bold")

    grid = Table.grid(padding=(0, 1))
    grid.add_column()
    grid.add_row(summary)
    grid.add_row("")
    grid.add_row(Text("backlog", style="dim"))
    grid.add_row(_backlog_dots(backlog))
    grid.add_row("")
    grid.add_row(Text("commits", style="dim"))
    grid.add_row(_dots(view["batches_applied"], recent=min(view["batches_applied"], 12)))
    return Panel(grid, title="CONSUMER", border_style="dim")


def _sink_panel(view: dict[str, Any]) -> Panel:
    grid = Table.grid(padding=(0, 1))
    grid.add_column()
    grid.add_row(Text("rows written to mirror", style="bold"))
    grid.add_row("")
    tail = list(view["sink_tail"])[-14:]
    if not tail:
        grid.add_row(Text("(waiting for commits)", style="dim"))
    else:
        for line in tail:
            grid.add_row(Text(line, no_wrap=True, overflow="ellipsis"))
    return Panel(grid, title="SINK WRITES", border_style="dim")


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


def _backlog_dots(backlog: int, width: int = 36) -> Text:
    text = Text()
    if backlog <= 0:
        text.append("caught up", style="dim")
        return text
    shown = min(backlog, width)
    text.append("•" * shown, style="bold yellow")
    if backlog > shown:
        text.append(f" +{backlog - shown}", style="yellow")
    return text


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    minutes, secs = divmod(seconds, 60)
    return f"{minutes:02d}:{secs:02d}"


def headless_summary(recorder: MetricsRecorder, state: DemoState) -> Callable[[], str]:
    def summary() -> str:
        snap = recorder.snapshot()
        view = state.snapshot()
        lag = view["lag_snapshots"]
        lag_s = "—" if lag is None else str(lag)
        return (
            f"phase={view['phase']} detail={view['phase_detail']!r} "
            f"mode={view['consumer_mode']} lag_snap={lag_s} "
            f"head={view['head_snapshot']} committed={view['committed_snapshot']} "
            f"batches={view['batches_applied']} orders={view['source_rows']} "
            f"mirror={view['sink_rows']} invariant={view['invariant']} errors={snap['errors']}"
        )

    return summary


def json_summary(recorder: MetricsRecorder, state: DemoState) -> dict[str, Any]:
    snap = recorder.snapshot()
    view = state.snapshot()
    return {
        "example": EXAMPLE,
        "errors": snap["errors"],
        "rows_processed": snap["rows_processed"],
        "phase": view["phase"],
        "consumer_mode": view["consumer_mode"],
        "lag_snapshots": view["lag_snapshots"],
        "lag_peak": view["lag_peak"],
        "head_snapshot": view["head_snapshot"],
        "committed_snapshot": view["committed_snapshot"],
        "batches_applied": view["batches_applied"],
        "source_rows": view["source_rows"],
        "sink_rows": view["sink_rows"],
        "producer_inserts": view["producer_inserts"],
        "invariant": view["invariant"],
    }


def run_live_inserts(
    lake: Any,
    consumer: DMLConsumer,
    state: DemoState,
    recorder: MetricsRecorder,
    stop: threading.Event,
    *,
    phase: DemoPhase,
    start_id: int,
    count: int,
) -> int:
    state.update(phase=phase, phase_detail="producer + consumer both running", consumer_mode="LISTEN")
    next_id = start_id
    for _ in range(count):
        if stop.is_set():
            break
        insert_order(lake, next_id, float(next_id * 10), state)
        next_id += 1
        drain_listen_batch(consumer, lake, state, recorder)
        refresh_lag(lake, state)
        stop.wait(PHASE_TICK_S)
    return next_id


def run_restart_cycle(
    lake: Any,
    consumer: DMLConsumer,
    state: DemoState,
    recorder: MetricsRecorder,
    stop: threading.Event,
    *,
    restart_phase: DemoPhase,
    catchup_phase: DemoPhase,
    start_id: int,
) -> tuple[DMLConsumer | None, int]:
    state.update(
        phase=restart_phase,
        phase_detail="consumer restarting; producer still writes",
        consumer_mode="OFFLINE",
    )
    close_consumer(consumer)
    consumer = None
    refresh_lag(lake, state)

    next_id = start_id
    deadline = time.monotonic() + RESTART_SECONDS
    while time.monotonic() < deadline and not stop.is_set():
        insert_order(lake, next_id, float(next_id * 10), state)
        next_id += 1
        refresh_lag(lake, state)
        stop.wait(PHASE_TICK_S)

    if stop.is_set():
        return consumer, next_id

    consumer = open_consumer(lake=lake, start_at="now", on_exists="use")
    state.update(
        phase=catchup_phase,
        phase_detail="consumer back online; draining missed inserts",
        consumer_mode="READ",
    )
    catch_up_reads(
        lake,
        consumer,
        state,
        recorder,
        stop,
        label="consumer back online; draining missed inserts",
    )
    stop.wait(CATCHUP_HOLD_S)
    return consumer, next_id


def main(argv: list[str] | None = None) -> int:
    parser = make_parser(
        description="Backfill from beginning with reads, survive restart gap, then live listen.",
        supported_catalogs=("postgres", "duckdb"),
        supported_storages=("disk", "s3"),
        catalog_default="duckdb",
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

    state = DemoState(phase="boot")
    recorder = MetricsRecorder(example=EXAMPLE, catalog=common.catalog, storage=common.storage)
    refresh_counts(lake, state)
    refresh_lag(lake, state)

    stop = threading.Event()
    consumer: DMLConsumer | None = None

    def _request_stop(signum: int, _frame: object) -> None:
        if stop.is_set():
            log(f"signal {signum} (again) -> hard exit", level="warn")
            os._exit(130)
        stop.set()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    client = CDCClient(lake, install_extension=False)

    try:
        with LiveDisplay(
            headless=common.headless,
            renderable_factory=lambda: build_layout(recorder, state),
            headless_summary=headless_summary(recorder, state),
        ):
            next_id = 1
            consumer = open_consumer(lake=lake, start_at="now", on_exists="error")

            if not stop.is_set():
                next_id = run_live_inserts(
                    lake,
                    consumer,
                    state,
                    recorder,
                    stop,
                    phase="live_before_restart",
                    start_id=next_id,
                    count=8,
                )

            if not stop.is_set():
                consumer, next_id = run_restart_cycle(
                    lake,
                    consumer,
                    state,
                    recorder,
                    stop,
                    restart_phase="restarting_a",
                    catchup_phase="catchup_a",
                    start_id=next_id,
                )

            if not stop.is_set():
                consumer, next_id = run_restart_cycle(
                    lake,
                    consumer,
                    state,
                    recorder,
                    stop,
                    restart_phase="restarting_b",
                    catchup_phase="catchup_b",
                    start_id=next_id,
                )

            if not stop.is_set():
                next_id = run_live_inserts(
                    lake,
                    consumer,
                    state,
                    recorder,
                    stop,
                    phase="live_after_restart",
                    start_id=next_id,
                    count=12,
                )

            if not stop.is_set():
                state.update(phase="done", phase_detail="invariant check")
                inv = verify_invariant(lake)
                state.update(invariant=inv)
                refresh_counts(lake, state)
                refresh_lag(lake, state)
                recorder.set_detail("invariant", inv)
                # Hold so the green DONE banner with the final invariant has
                # time to register before the lake reset tears everything down.
                stop.wait(DONE_HOLD_S)

    except KeyboardInterrupt:
        state.update(phase="error", invariant="interrupted")
        log("interrupted", level="warn")
        return 130
    except Exception as exc:  # noqa: BLE001
        recorder.record_error()
        state.update(phase="error", invariant=f"error: {type(exc).__name__}")
        cause = f" cause={exc.__cause__}" if exc.__cause__ is not None else ""
        log(f"fatal: {type(exc).__name__}: {exc}{cause}", level="error")
        return 1
    finally:
        try:
            close_consumer(consumer)
            drain_catalog_consumer(client, CONSUMER_NAME)
        finally:
            lake.close()
            reset_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)

    log(headless_summary(recorder, state)())
    emit_json_summary(common, json_summary(recorder, state))
    if stop.is_set():
        log("stopped early", level="warn")
        return 130
    ok = recorder.snapshot()["errors"] == 0 and state.snapshot()["invariant"] == "ok"
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
