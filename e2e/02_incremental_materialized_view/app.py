"""02_incremental_materialized_view: maintain an aggregate table from DML CDC."""

from __future__ import annotations

import os
import signal
import sys
import threading
from collections import deque
from collections.abc import Callable, Iterable
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
from rich.layout import Layout  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.text import Text  # noqa: E402

from _lib.cli import emit_json_summary, make_parser, parse_common  # noqa: E402
from _lib.config import load_cdc_extension, open_lake, reset_lake  # noqa: E402
from _lib.metrics import MetricsRecorder  # noqa: E402
from _lib.tui import LiveDisplay, log  # noqa: E402

EXAMPLE = "02_incremental_materialized_view"

SOURCE_TABLE = "lake.orders"
MV_TABLE = "lake.daily_revenue"
CONSUMER_NAME = "daily_revenue_mv"

LISTEN_TIMEOUT_MS = 1000
LISTEN_MAX_SNAPSHOTS = 20
POLL_MIN_MS = 1
EVENT_INTERVAL_S = 1.0
INTRO_HOLD_S = 2.5

EXTRAPOLATE_TABLE_SIZES = (1_000, 10_000)

EventKind = Literal["insert", "update", "delete"]


@dataclass(frozen=True)
class OrderRow:
    order_id: int
    customer_id: int
    order_date: str
    status: str
    amount: float


@dataclass(frozen=True)
class SourceEvent:
    kind: EventKind
    order: OrderRow
    label: str


@dataclass(frozen=True)
class RevenueRow:
    order_date: str
    paid_orders: int
    revenue: float


@dataclass(frozen=True)
class OrderView:
    order_id: int
    order_date: str
    status: str
    amount: float


@dataclass
class DemoState:
    phase: str = "boot"
    source_events: deque[str] = field(default_factory=lambda: deque(maxlen=22))
    cdc_events: deque[str] = field(default_factory=lambda: deque(maxlen=22))
    orders: tuple[OrderView, ...] = ()
    incremental_scan_ids: tuple[int, ...] = ()
    naive_scan_ids: tuple[int, ...] = ()
    daily_revenue: tuple[RevenueRow, ...] = ()
    batch_start: int | None = None
    batch_end: int | None = None
    batch_changes: int = 0
    committed_snapshot: int | None = None
    source_rows: int = 0
    source_events_seen: int = 0
    incremental_changes: int = 0
    full_scan_rows: int = 0
    invariant: str = "pending"
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def update(self, **values: Any) -> None:
        with self._lock:
            for key, value in values.items():
                setattr(self, key, value)

    def add_source(self, message: str) -> None:
        with self._lock:
            self.source_events.append(message)

    def add_cdc(self, message: str) -> None:
        with self._lock:
            self.cdc_events.append(message)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "phase": self.phase,
                "source_events": tuple(self.source_events),
                "cdc_events": tuple(self.cdc_events),
                "orders": self.orders,
                "incremental_scan_ids": self.incremental_scan_ids,
                "naive_scan_ids": self.naive_scan_ids,
                "daily_revenue": self.daily_revenue,
                "batch_start": self.batch_start,
                "batch_end": self.batch_end,
                "batch_changes": self.batch_changes,
                "committed_snapshot": self.committed_snapshot,
                "source_rows": self.source_rows,
                "source_events_seen": self.source_events_seen,
                "incremental_changes": self.incremental_changes,
                "full_scan_rows": self.full_scan_rows,
                "invariant": self.invariant,
            }


SEED_ORDERS: tuple[OrderRow, ...] = (
    OrderRow(1, 101, "2026-05-01", "paid", 120.0),
    OrderRow(2, 102, "2026-05-01", "paid", 80.0),
    OrderRow(3, 103, "2026-05-02", "pending", 75.0),
    OrderRow(4, 104, "2026-05-02", "paid", 140.0),
    OrderRow(5, 105, "2026-05-03", "paid", 60.0),
)

EVENTS: tuple[SourceEvent, ...] = (
    SourceEvent("insert", OrderRow(6, 106, "2026-05-01", "paid", 45.0), "new paid order"),
    SourceEvent("update", OrderRow(3, 103, "2026-05-02", "paid", 75.0), "pending becomes paid"),
    SourceEvent("insert", OrderRow(7, 107, "2026-05-03", "paid", 95.0), "new paid order"),
    SourceEvent("update", OrderRow(2, 102, "2026-05-01", "paid", 100.0), "amount correction"),
    SourceEvent("delete", OrderRow(5, 105, "2026-05-03", "paid", 60.0), "refund delete"),
    SourceEvent("insert", OrderRow(8, 108, "2026-05-02", "pending", 110.0), "pending order"),
    SourceEvent("update", OrderRow(8, 108, "2026-05-02", "paid", 110.0), "pending becomes paid"),
    SourceEvent("insert", OrderRow(9, 109, "2026-05-04", "paid", 210.0), "new day"),
    SourceEvent("update", OrderRow(4, 104, "2026-05-02", "cancelled", 140.0), "cancel paid order"),
    SourceEvent("insert", OrderRow(10, 110, "2026-05-04", "paid", 55.0), "new paid order"),
    SourceEvent("update", OrderRow(1, 101, "2026-05-01", "paid", 135.0), "amount correction"),
    SourceEvent("delete", OrderRow(7, 107, "2026-05-03", "paid", 95.0), "refund delete"),
    SourceEvent("insert", OrderRow(11, 111, "2026-05-03", "paid", 130.0), "new paid order"),
    SourceEvent("update", OrderRow(9, 109, "2026-05-04", "paid", 190.0), "discount applied"),
    SourceEvent("insert", OrderRow(12, 112, "2026-05-02", "paid", 65.0), "new paid order"),
    SourceEvent("delete", OrderRow(6, 106, "2026-05-01", "paid", 45.0), "chargeback delete"),
    SourceEvent("insert", OrderRow(13, 113, "2026-05-05", "paid", 40.0), "new day May 5"),
    SourceEvent("insert", OrderRow(14, 114, "2026-05-05", "pending", 90.0), "pending on May 5"),
    SourceEvent("update", OrderRow(14, 114, "2026-05-05", "paid", 90.0), "pending becomes paid"),
    SourceEvent("update", OrderRow(10, 110, "2026-05-04", "paid", 70.0), "price bump"),
    SourceEvent("delete", OrderRow(12, 112, "2026-05-02", "paid", 65.0), "remove order"),
    SourceEvent("insert", OrderRow(15, 115, "2026-05-01", "paid", 25.0), "late insert earlier day"),
    SourceEvent("update", OrderRow(8, 108, "2026-05-02", "paid", 125.0), "amount tweak"),
    SourceEvent("insert", OrderRow(16, 116, "2026-05-06", "paid", 300.0), "new day May 6"),
    SourceEvent("update", OrderRow(11, 111, "2026-05-03", "cancelled", 130.0), "cancel removes paid"),
    SourceEvent("insert", OrderRow(17, 117, "2026-05-03", "paid", 50.0), "replacement revenue"),
    SourceEvent("delete", OrderRow(10, 110, "2026-05-04", "paid", 70.0), "delete paid row"),
    SourceEvent("insert", OrderRow(18, 118, "2026-05-04", "paid", 88.0), "new paid same day"),
    SourceEvent("update", OrderRow(1, 101, "2026-05-01", "paid", 99.0), "discount on classic order"),
    SourceEvent("insert", OrderRow(19, 119, "2026-05-02", "paid", 33.0), "small order"),
    SourceEvent("delete", OrderRow(19, 119, "2026-05-02", "paid", 33.0), "void small order"),
)


def setup_schema(lake: Any) -> None:
    conn = lake.connection
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.orders (
            order_id    BIGINT,
            customer_id BIGINT,
            order_date  DATE,
            status      VARCHAR,
            amount      DOUBLE,
            updated_at  TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.daily_revenue (
            order_date  DATE,
            paid_orders BIGINT,
            revenue     DOUBLE
        )
        """
    )


def seed_orders(lake: Any) -> None:
    for order in SEED_ORDERS:
        insert_order(lake.connection, order)
    refresh_full_materialization(lake.connection)


def insert_order(conn: Any, order: OrderRow) -> None:
    conn.execute(
        f"""
        INSERT INTO {SOURCE_TABLE} (order_id, customer_id, order_date, status, amount, updated_at)
        VALUES (?, ?, ?::DATE, ?, ?, now())
        """,
        [order.order_id, order.customer_id, order.order_date, order.status, order.amount],
    )


def apply_source_event(lake: Any, event: SourceEvent, state: DemoState) -> None:
    conn = lake.connection
    if event.kind == "insert":
        insert_order(conn, event.order)
    elif event.kind == "update":
        conn.execute(
            f"""
            UPDATE {SOURCE_TABLE}
            SET customer_id = ?, order_date = ?::DATE, status = ?, amount = ?, updated_at = now()
            WHERE order_id = ?
            """,
            [
                event.order.customer_id,
                event.order.order_date,
                event.order.status,
                event.order.amount,
                event.order.order_id,
            ],
        )
    elif event.kind == "delete":
        conn.execute(f"DELETE FROM {SOURCE_TABLE} WHERE order_id = ?", [event.order.order_id])
    else:
        raise AssertionError(f"unknown event kind: {event.kind}")
    state.add_source(
        f"{event.kind} order={event.order.order_id} {event.order.order_date} "
        f"{event.order.status} ${event.order.amount:.2f} · {event.label}"
    )


def refresh_full_materialization(conn: Any) -> None:
    conn.execute(f"DELETE FROM {MV_TABLE}")
    conn.execute(
        f"""
        INSERT INTO {MV_TABLE}
        SELECT
            order_date,
            count(*)::BIGINT AS paid_orders,
            coalesce(sum(amount), 0)::DOUBLE AS revenue
        FROM {SOURCE_TABLE}
        WHERE status = 'paid'
        GROUP BY order_date
        """
    )


def listen_and_apply(consumer: DMLConsumer, state: DemoState, recorder: MetricsRecorder) -> int:
    batch = consumer.listen(
        timeout_ms=LISTEN_TIMEOUT_MS,
        max_snapshots=LISTEN_MAX_SNAPSHOTS,
        poll_min_ms=POLL_MIN_MS,
        coalesce=False,
    )
    if batch is None:
        return 0
    if not isinstance(batch, DMLBatch):
        raise TypeError(f"expected DMLBatch, got {type(batch).__name__}")

    applied = 0
    scanned_ids: set[int] = set()
    with batch.transaction() as tx:
        for change in batch.changes:
            oid = dict(change.values).get("order_id")
            if oid is not None:
                scanned_ids.add(int(oid))
            if apply_delta(tx, change):
                applied += 1
                state.add_cdc(format_change(change))
    recorder.record_rows(applied)
    state.update(
        phase="applying",
        batch_start=batch.start_snapshot,
        batch_end=batch.end_snapshot,
        batch_changes=len(batch.changes),
        committed_snapshot=batch.end_snapshot,
        incremental_changes=state.snapshot()["incremental_changes"] + applied,
        incremental_scan_ids=tuple(sorted(scanned_ids)),
    )
    recorder.set_detail("last_batch", batch.batch_id)
    recorder.set_detail("incremental_changes", state.snapshot()["incremental_changes"])
    return applied


def apply_delta(tx: Any, change: Change) -> bool:
    values = dict(change.values)
    if change.kind == ChangeType.INSERT:
        add_contribution(tx, values, 1)
        return True
    if change.kind == ChangeType.DELETE:
        add_contribution(tx, values, -1)
        return True
    if change.kind == ChangeType.UPDATE_PREIMAGE:
        add_contribution(tx, values, -1)
        return True
    if change.kind == ChangeType.UPDATE_POSTIMAGE:
        add_contribution(tx, values, 1)
        return True
    return False


def add_contribution(tx: Any, values: dict[str, Any], sign: int) -> None:
    if values.get("status") != "paid":
        return
    order_date = values["order_date"]
    amount = float(values.get("amount") or 0)
    current = tx.execute(
        f"SELECT paid_orders, revenue FROM {MV_TABLE} WHERE order_date = ?",
        [order_date],
    ).fetchone()
    paid_orders = (int(current[0]) if current else 0) + sign
    revenue = (float(current[1]) if current else 0.0) + sign * amount
    tx.execute(f"DELETE FROM {MV_TABLE} WHERE order_date = ?", [order_date])
    if paid_orders != 0 or abs(revenue) > 0.000001:
        tx.execute(
            f"""
            INSERT INTO {MV_TABLE} (order_date, paid_orders, revenue)
            VALUES (?, ?, ?)
            """,
            [order_date, paid_orders, round(revenue, 6)],
        )


def format_change(change: Change) -> str:
    values = dict(change.values)
    amount = float(values.get("amount") or 0)
    return (
        f"snap={change.snapshot_id} {change.kind.value} "
        f"order={values.get('order_id')} {values.get('order_date')} "
        f"{values.get('status')} ${amount:.2f}"
    )


def update_counts_and_invariant(lake: Any, state: DemoState, *, full_scan: bool) -> str:
    source_rows = int(lake.connection.execute(f"SELECT count(*) FROM {SOURCE_TABLE}").fetchone()[0])
    orders = read_orders(lake.connection)
    if full_scan:
        state.update(full_scan_rows=state.snapshot()["full_scan_rows"] + source_rows)
    state.update(
        source_rows=source_rows,
        orders=orders,
        naive_scan_ids=tuple(order.order_id for order in orders) if full_scan else (),
        daily_revenue=read_daily_revenue(lake.connection),
    )
    result = verify_invariant(lake.connection)
    state.update(invariant=result)
    return result


def read_daily_revenue(conn: Any) -> tuple[RevenueRow, ...]:
    rows = conn.execute(
        f"""
        SELECT order_date::VARCHAR, paid_orders, revenue
        FROM {MV_TABLE}
        ORDER BY order_date
        """
    ).fetchall()
    return tuple(RevenueRow(str(row[0]), int(row[1]), float(row[2])) for row in rows)


def read_orders(conn: Any) -> tuple[OrderView, ...]:
    rows = conn.execute(
        f"""
        SELECT order_id, order_date::VARCHAR, status, amount
        FROM {SOURCE_TABLE}
        ORDER BY order_id
        """
    ).fetchall()
    return tuple(
        OrderView(int(row[0]), str(row[1]), str(row[2]), float(row[3]))
        for row in rows
    )


def verify_invariant(conn: Any) -> str:
    mismatch = int(
        conn.execute(
            f"""
            WITH recomputed AS (
                SELECT
                    order_date,
                    count(*)::BIGINT AS paid_orders,
                    round(coalesce(sum(amount), 0), 6)::DOUBLE AS revenue
                FROM {SOURCE_TABLE}
                WHERE status = 'paid'
                GROUP BY order_date
            ),
            current_mv AS (
                SELECT order_date, paid_orders, round(revenue, 6)::DOUBLE AS revenue
                FROM {MV_TABLE}
            )
            SELECT (
                SELECT count(*) FROM (
                    SELECT * FROM recomputed
                    EXCEPT
                    SELECT * FROM current_mv
                )
            ) + (
                SELECT count(*) FROM (
                    SELECT * FROM current_mv
                    EXCEPT
                    SELECT * FROM recomputed
                )
            )
            """
        ).fetchone()[0]
    )
    return "ok" if mismatch == 0 else f"mismatch={mismatch}"


def _observed_savings_pct(incremental: int, full_scan: int) -> float | None:
    if full_scan <= 0:
        return None
    return 100.0 * (1.0 - incremental / full_scan)


def _scaled_naive_scan_rows(full_scan_rows: int, target_table_rows: int, ref_table_rows: int) -> int:
    if ref_table_rows <= 0:
        return 0
    return int(round(full_scan_rows * (target_table_rows / ref_table_rows)))


def build_layout(recorder: MetricsRecorder, state: DemoState) -> Layout:
    snap = recorder.snapshot()
    view = state.snapshot()
    layout = Layout()
    layout.split(
        Layout(_header(snap, view), name="header", size=5),
        Layout(name="body"),
        Layout(_footer(view), name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(name="left", ratio=3),
        Layout(name="right", ratio=2),
    )
    layout["left"].split(
        Layout(_source_table_panel(view, mode="incremental"), name="incremental"),
        Layout(_source_table_panel(view, mode="naive"), name="naive"),
    )
    layout["right"].split(
        Layout(_result_panel(view), name="result", ratio=2),
        Layout(_savings_panel(view), name="savings", ratio=3),
    )
    return layout


def _header(snap: dict[str, Any], view: dict[str, Any]) -> Panel:
    inc = view["incremental_changes"]
    fs = view["full_scan_rows"]
    pct = _observed_savings_pct(inc, fs)
    pct_s = "—" if pct is None else f"{pct:.1f}%"

    title = Text()
    title.append("ducklake-cdc", style="bold")
    title.append(" · ")
    title.append(EXAMPLE)
    title.append("    elapsed ", style="dim")
    title.append(_fmt_duration(snap["elapsed_s"]))
    title.append("    errors ", style="dim")
    title.append(str(snap["errors"]))

    story = Text()
    story.append("Incremental scan: ", style="dim")
    story.append(_fmt_int(inc), style="bold")
    story.append(" row reads   Full refresh scan: ", style="dim")
    story.append(_fmt_int(fs), style="bold")
    story.append(" row reads   Saved ", style="dim")
    story.append(pct_s, style="bold")

    grid = Table.grid()
    grid.add_column()
    grid.add_row(title)
    grid.add_row(story)
    return Panel(grid, border_style="dim", padding=(0, 1))


def _footer(view: dict[str, Any]) -> Panel:
    inc = view["incremental_changes"]
    fs = view["full_scan_rows"]
    pct = _observed_savings_pct(inc, fs)
    pct_part = f"savings={pct:.1f}%  " if pct is not None else ""
    text = Text(no_wrap=True, overflow="ellipsis")
    if pct_part:
        text.append(pct_part, style="bold")
    text.append(f"events {view['source_events_seen']}/{len(EVENTS)}  ")
    text.append("orders ", style="dim")
    text.append(str(view["source_rows"]), style="bold")
    text.append("  commit ", style="dim")
    text.append(str(view["committed_snapshot"] or "—"), style="bold yellow")
    text.append("  invariant=", style="dim")
    if view["invariant"] == "ok":
        text.append("ok", style="bold")
    elif view["invariant"] == "pending":
        text.append("pending", style="dim")
    else:
        text.append(str(view["invariant"]), style="bold")
    return Panel(text, border_style="dim", padding=(0, 1))


def _source_table_panel(view: dict[str, Any], *, mode: Literal["incremental", "naive"]) -> Panel:
    scan_ids = set(view["incremental_scan_ids"] if mode == "incremental" else view["naive_scan_ids"])
    title = "TABLE A: incremental source" if mode == "incremental" else "TABLE B: full-refresh source"
    caption = "only changed rows flash red" if mode == "incremental" else "every row flashes red on refresh"
    table = Table.grid(padding=(0, 1))
    table.add_column(justify="right")
    table.add_column()
    table.add_column()
    table.add_column(justify="right")
    table.add_row("id", "date", "status", "amount")
    for order in view["orders"]:
        style = "bold red" if order.order_id in scan_ids else None
        table.add_row(
            _cell(str(order.order_id), style),
            _cell(order.order_date, style),
            _cell(order.status, style),
            _cell(f"${order.amount:.2f}", style),
        )
    if not view["orders"]:
        table.add_row("—", "—", "—", "—", style="dim")
    return Panel(table, title=title, subtitle=f"[dim]{caption}[/dim]", border_style="dim")


def _cell(value: str, style: str | None = None) -> Text:
    return Text(value, style=style or "")


def _result_panel(view: dict[str, Any]) -> Panel:
    table = Table()
    table.add_column("date")
    table.add_column("paid", justify="right")
    table.add_column("revenue", justify="right")
    for row in view["daily_revenue"]:
        table.add_row(row.order_date, _fmt_int(row.paid_orders), f"${row.revenue:,.2f}")
    if not view["daily_revenue"]:
        table.add_row("—", "0", "$0.00")
    return Panel(table, title="RESULT TABLE: daily_revenue", border_style="dim")


def _savings_panel(view: dict[str, Any]) -> Panel:
    inc = view["incremental_changes"]
    fs = view["full_scan_rows"]
    ref = max(view["source_rows"], 1)
    pct = _observed_savings_pct(inc, fs)

    grid = Table.grid(padding=(0, 1))
    grid.add_column()

    pct_s = "—" if pct is None else f"{pct:.1f}%"
    headline = Text()
    headline.append("Saved ", style="dim")
    headline.append(pct_s, style="bold")
    headline.append(" scan work", style="bold")
    grid.add_row(headline)
    grid.add_row("")

    cdc_bar = _work_bar(inc, max(fs, inc, 1))
    scan_bar = _work_bar(fs, max(fs, inc, 1))
    grid.add_row(f"incremental  {cdc_bar} {_fmt_int(inc)}")
    grid.add_row(f"full scan    {scan_bar} {_fmt_int(fs)}")
    grid.add_row("")
    grid.add_row("same changes on bigger tables:")
    for target in EXTRAPOLATE_TABLE_SIZES:
        naive_scaled = _scaled_naive_scan_rows(fs, target, ref)
        epct = _observed_savings_pct(inc, naive_scaled)
        label = _fmt_int(target)
        if epct is None:
            grid.add_row(f"orders≈{label}: —")
        else:
            grid.add_row(
                f"{label} orders: [bold]{epct:.2f}% saved[/bold] "
                f"[dim](scan ~{_fmt_int(naive_scaled)})[/dim]"
            )
    grid.add_row("")
    grid.add_row(f"[dim]cursor batch {view['batch_start'] or '—'} -> {view['batch_end'] or '—'}; commit {view['committed_snapshot'] or '—'}[/dim]")

    return Panel(
        grid,
        title="SAVINGS",
        border_style="dim",
        subtitle=f"[dim]invariant {view['invariant']}[/dim]",
    )


def _work_bar(value: int, maximum: int, *, width: int = 20) -> str:
    if maximum <= 0:
        return "[dim]" + ("░" * width) + "[/dim]"
    filled = max(1 if value > 0 else 0, min(width, round(width * value / maximum)))
    return f"[white]{'█' * filled}[/white][dim]{'░' * (width - filled)}[/dim]"


def _fmt_int(value: int) -> str:
    return f"{value:,}"


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    minutes, secs = divmod(seconds, 60)
    return f"{minutes:02d}:{secs:02d}"


def headless_summary(recorder: MetricsRecorder, state: DemoState) -> Callable[[], str]:
    def summary() -> str:
        snap = recorder.snapshot()
        view = state.snapshot()
        inc = view["incremental_changes"]
        fs = view["full_scan_rows"]
        ref = max(view["source_rows"], 1)
        pct = _observed_savings_pct(inc, fs)
        pct_s = f"{pct:.1f}%" if pct is not None else "—"
        e1k = EXTRAPOLATE_TABLE_SIZES[0]
        e10k = EXTRAPOLATE_TABLE_SIZES[1]
        n1k = _scaled_naive_scan_rows(fs, e1k, ref)
        n10k = _scaled_naive_scan_rows(fs, e10k, ref)
        p1k = _observed_savings_pct(inc, n1k)
        p10k = _observed_savings_pct(inc, n10k)
        e1k_s = f"{p1k:.2f}%" if p1k is not None else "—"
        e10k_s = f"{p10k:.2f}%" if p10k is not None else "—"
        return (
            f"phase={view['phase']} rows={snap['rows_processed']} "
            f"events={view['source_events_seen']}/{len(EVENTS)} "
            f"orders_rows={view['source_rows']} "
            f"cdc_rows={inc} naive_scan_rows={fs} savings={pct_s} "
            f"extrap_savings_1k={e1k_s} extrap_savings_10k={e10k_s} "
            f"invariant={view['invariant']} errors={snap['errors']}"
        )

    return summary


def json_summary(recorder: MetricsRecorder, state: DemoState) -> dict[str, Any]:
    snap = recorder.snapshot()
    view = state.snapshot()
    incremental = int(view["incremental_changes"])
    full_scan = int(view["full_scan_rows"])
    savings = _observed_savings_pct(incremental, full_scan)
    return {
        "example": EXAMPLE,
        "errors": snap["errors"],
        "rows_processed": snap["rows_processed"],
        "phase": view["phase"],
        "events_seen": view["source_events_seen"],
        "events_total": len(EVENTS),
        "orders_rows": view["source_rows"],
        "incremental_changes": incremental,
        "full_scan_rows": full_scan,
        "savings_pct": savings,
        "invariant": view["invariant"],
    }


def drain_consumer(client: CDCClient, name: str) -> None:
    try:
        client.cdc_consumer_force_release(name)
    except Exception:
        pass


def main(argv: list[str] | None = None) -> int:
    parser = make_parser(
        description="Maintain daily_revenue incrementally from DML CDC windows.",
        supported_catalogs=("postgres", "duckdb"),
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
    seed_orders(lake)

    state = DemoState(phase="ready")
    recorder = MetricsRecorder(example=EXAMPLE, catalog=common.catalog, storage=common.storage)
    update_counts_and_invariant(lake, state, full_scan=True)
    log("source and materialized view ready")

    stop = threading.Event()
    consumer: DMLConsumer | None = None

    def _request_stop(signum: int, _frame: object) -> None:
        if stop.is_set():
            log(f"signal {signum} (again) -> hard exit", level="warn")
            os._exit(130)
        log(f"signal {signum} -> stop")
        stop.set()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    try:
        consumer = DMLConsumer(
            lake,
            CONSUMER_NAME,
            table="orders",
            start_at="now",
            mode="changes",
        )
        consumer.__enter__()
        consumer.listen(timeout_ms=1, max_snapshots=1, poll_min_ms=POLL_MIN_MS)
        with LiveDisplay(
            headless=common.headless,
            renderable_factory=lambda: build_layout(recorder, state),
            headless_summary=headless_summary(recorder, state),
        ):
            state.update(phase="live")
            if INTRO_HOLD_S > 0:
                state.update(phase="intro")
                stop.wait(INTRO_HOLD_S)
                state.update(phase="live")
            for idx, event in enumerate(EVENTS, start=1):
                if stop.is_set():
                    break
                apply_source_event(lake, event, state)
                state.update(source_events_seen=idx)
                listen_and_apply(consumer, state, recorder)
                update_counts_and_invariant(lake, state, full_scan=True)
                if stop.wait(EVENT_INTERVAL_S):
                    break
            if not stop.is_set():
                state.update(phase="done")
                invariant = update_counts_and_invariant(lake, state, full_scan=True)
                recorder.set_detail("invariant", invariant)
    except Exception as exc:  # noqa: BLE001
        recorder.record_error()
        state.update(phase="error", invariant=f"error: {type(exc).__name__}")
        cause = f" cause={exc.__cause__}" if exc.__cause__ is not None else ""
        log(f"fatal: {type(exc).__name__}: {exc}{cause}", level="error")
        return 1
    finally:
        try:
            if consumer is not None:
                consumer.__exit__(None, None, None)
            drain_consumer(CDCClient(lake, install_extension=False), CONSUMER_NAME)
        finally:
            lake.close()
            reset_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)

    log(headless_summary(recorder, state)())
    emit_json_summary(common, json_summary(recorder, state))
    return 0 if recorder.snapshot()["errors"] == 0 and state.snapshot()["invariant"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
