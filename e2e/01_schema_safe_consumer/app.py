"""01_schema_safe_consumer: DML stops at schema boundaries, then resumes safely."""

from __future__ import annotations

import os
import signal
import sys
import threading
from collections import deque
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_E2E_ROOT = Path(__file__).resolve().parent.parent
if str(_E2E_ROOT) not in sys.path:
    sys.path.insert(0, str(_E2E_ROOT))

from ducklake_cdc_client import DMLConsumer  # noqa: E402
from ducklake_cdc_client.client import CDCClient, ChangeRow, SchemaChangeRow  # noqa: E402
from ducklake_cdc_client.enums import ChangeType  # noqa: E402
from ducklake_client import ColumnDef  # noqa: E402
from rich.layout import Layout  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.text import Text  # noqa: E402

from _lib.cli import emit_json_summary, make_parser, parse_common  # noqa: E402
from _lib.config import catalog_head_snapshot, load_cdc_extension, open_lake, reset_lake  # noqa: E402
from _lib.metrics import MetricsRecorder  # noqa: E402
from _lib.tui import LiveDisplay, log  # noqa: E402

EXAMPLE = "01_schema_safe_consumer"

SOURCE_TABLE = "lake.orders"
SINK_TABLE = "lake.derived_orders"
DDL_CONSUMER = "orders_ddl"

LISTEN_TIMEOUT_MS = 1000
LISTEN_MAX_SNAPSHOTS = 20
POLL_MIN_MS = 1
INSERT_INTERVAL_S = 0.4
DDL_EVERY_ROWS = 10
MAX_SCHEMA_MIGRATIONS = 4
TOTAL_DML_EVENTS = DDL_EVERY_ROWS * (MAX_SCHEMA_MIGRATIONS + 1)
BASE_COLUMNS = ("id", "status", "amount", "updated_at")


@dataclass(frozen=True)
class SchemaMutation:
    op: str
    name: str
    sql_type: str
    default_sql: str
    default_value: Any


SCHEMA_MUTATIONS: tuple[SchemaMutation, ...] = (
    SchemaMutation("add", "tax", "DOUBLE", "0", 0.0),
    SchemaMutation("add", "priority", "VARCHAR", "'standard'", "standard"),
    SchemaMutation("drop", "status", "VARCHAR", "'new'", "new"),
    SchemaMutation("add", "region", "VARCHAR", "'global'", "global"),
)


@dataclass(frozen=True)
class SchemaColumn:
    name: str
    type: str
    nullable: str


@dataclass(frozen=True)
class DiffRow:
    snapshot_id: int
    change_kind: str
    old_name: str | None
    new_name: str | None
    old_type: str | None
    new_type: str | None


@dataclass
class DemoState:
    phase: str = "boot"
    source_schema: tuple[SchemaColumn, ...] = ()
    sink_schema: tuple[SchemaColumn, ...] = ()
    dml_tail: deque[str] = field(default_factory=lambda: deque(maxlen=14))
    ddl_tail: deque[str] = field(default_factory=lambda: deque(maxlen=8))
    diff_rows: tuple[DiffRow, ...] = ()
    boundary_snapshot: int | None = None
    migrations_applied: int = 0
    invariant: str = "pending"
    source_rows: int = 0
    sink_rows: int = 0
    rows_by_epoch: dict[str, int] = field(default_factory=dict)
    active_consumer: str = "orders_dml_v1"
    epoch: int = 1
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def update(self, **values: Any) -> None:
        with self._lock:
            for key, value in values.items():
                setattr(self, key, value)

    def add_dml(self, message: str) -> None:
        with self._lock:
            self.dml_tail.append(message)

    def add_ddl(self, message: str) -> None:
        with self._lock:
            self.ddl_tail.append(message)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "phase": self.phase,
                "source_schema": self.source_schema,
                "sink_schema": self.sink_schema,
                "dml_tail": tuple(self.dml_tail),
                "ddl_tail": tuple(self.ddl_tail),
                "diff_rows": self.diff_rows,
                "boundary_snapshot": self.boundary_snapshot,
                "migrations_applied": self.migrations_applied,
                "invariant": self.invariant,
                "source_rows": self.source_rows,
                "sink_rows": self.sink_rows,
                "rows_by_epoch": dict(self.rows_by_epoch),
                "active_consumer": self.active_consumer,
                "epoch": self.epoch,
            }


def setup_schema(lake: Any) -> None:
    lake.table.create(
        "orders",
        id=ColumnDef("BIGINT"),
        status=ColumnDef("VARCHAR"),
        amount=ColumnDef("DOUBLE"),
        updated_at=ColumnDef("TIMESTAMP"),
    )
    lake.table.create(
        "derived_orders",
        id=ColumnDef("BIGINT"),
        status=ColumnDef("VARCHAR"),
        amount=ColumnDef("DOUBLE"),
        updated_at=ColumnDef("TIMESTAMP"),
    )


def describe_table(lake: Any, logical_name: str) -> tuple[SchemaColumn, ...]:
    info = lake.table.info(logical_name, include_snapshots=False, include_row_count=False)
    return tuple(
        SchemaColumn(col.name, col.data_type, "YES" if col.nullable else "NO")
        for col in info.columns
    )


def refresh_schemas(lake: Any, state: DemoState) -> None:
    state.update(
        source_schema=describe_table(lake, "orders"),
        sink_schema=describe_table(lake, "derived_orders"),
    )


def create_ddl_consumer(client: CDCClient) -> None:
    client.cdc_ddl_consumer_create(
        DDL_CONSUMER,
        table_names=["orders"],
        start_at="now",
    )


@contextmanager
def quiet_extension_output() -> Iterable[None]:
    sys.stdout.flush()
    sys.stderr.flush()
    null_fd = os.open(os.devnull, os.O_WRONLY)
    saved_stdout = os.dup(1)
    saved_stderr = os.dup(2)
    try:
        os.dup2(null_fd, 1)
        os.dup2(null_fd, 2)
        yield
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os.dup2(saved_stdout, 1)
        os.dup2(saved_stderr, 2)
        os.close(saved_stdout)
        os.close(saved_stderr)
        os.close(null_fd)


def consumer_name_for_epoch(epoch: int) -> str:
    return f"orders_dml_v{epoch}"


def active_mutations(epoch: int) -> tuple[SchemaMutation, ...]:
    active: dict[str, SchemaMutation] = {}
    for mutation in SCHEMA_MUTATIONS[: min(MAX_SCHEMA_MIGRATIONS, max(0, epoch - 1))]:
        if mutation.op == "add":
            active[mutation.name] = mutation
        elif mutation.op == "drop":
            active.pop(mutation.name, None)
    return tuple(active.values())


def active_base_columns(epoch: int) -> tuple[str, ...]:
    dropped = {
        mutation.name
        for mutation in SCHEMA_MUTATIONS[: min(MAX_SCHEMA_MIGRATIONS, max(0, epoch - 1))]
        if mutation.op == "drop"
    }
    return tuple(column for column in BASE_COLUMNS if column not in dropped)


def insert_order(lake: Any, order_id: int, *, epoch: int) -> None:
    mutations = active_mutations(epoch)
    base_columns = active_base_columns(epoch)
    columns = [*base_columns, *(mutation.name for mutation in mutations)]
    placeholders = [
        "now()" if column == "updated_at" else "?"
        for column in base_columns
    ] + ["?" for _ in mutations]
    base_values = {
        "id": order_id,
        "status": status_for_epoch(epoch),
        "amount": float(order_id * 10),
    }
    values: list[Any] = [
        base_values[column]
        for column in base_columns
        if column != "updated_at"
    ]
    values.extend(value_for_mutation(mutation, order_id) for mutation in mutations)
    lake.connection.execute(
        f"""
        INSERT INTO lake.orders ({", ".join(columns)})
        VALUES ({", ".join(placeholders)})
        """,
        values,
    )


def status_for_epoch(epoch: int) -> str:
    return "new" if epoch == 1 else "paid"


def value_for_mutation(mutation: SchemaMutation, order_id: int) -> Any:
    if mutation.name == "tax":
        return round((order_id % 7 + 1) * 0.17, 2)
    if mutation.name == "priority":
        return "expedite" if order_id % 3 == 0 else "standard"
    if mutation.name == "region":
        return ("apac", "emea", "amer")[order_id % 3]
    return mutation.default_value


def listen_and_apply(
    lake: Any,
    client: CDCClient,
    state: DemoState,
    recorder: MetricsRecorder,
    *,
    consumer_name: str,
    epoch: int,
) -> int:
    with quiet_extension_output():
        rows = client.cdc_dml_changes_listen(
            consumer_name,
            timeout_ms=LISTEN_TIMEOUT_MS,
            max_snapshots=LISTEN_MAX_SNAPSHOTS,
            poll_min_ms=POLL_MIN_MS,
            coalesce=False,
        )
    if not rows:
        return 0

    end_snapshot = max(row.end_snapshot or row.snapshot_id for row in rows)
    sink_columns = sink_column_names(lake)
    applied = 0
    with lake.transaction():
        for row in rows:
            if apply_change(lake, row, sink_columns):
                applied += 1
                state.add_dml(format_change(row, epoch))
        client.cdc_commit(consumer_name, end_snapshot)

    recorder.record_rows(applied)
    view = state.snapshot()
    rows_by_epoch = dict(view["rows_by_epoch"])
    epoch_key = f"v{epoch}"
    rows_by_epoch[epoch_key] = rows_by_epoch.get(epoch_key, 0) + applied
    state.update(rows_by_epoch=rows_by_epoch)
    update_row_counts(lake, state)
    recorder.set_detail("rows_applied", recorder.snapshot()["rows_processed"])
    recorder.set_detail("last_snapshot", end_snapshot)
    return applied


def sink_column_names(lake: Any) -> tuple[str, ...]:
    return tuple(
        col.name
        for col in lake.table.info(
            "derived_orders",
            include_snapshots=False,
            include_row_count=False,
        ).columns
    )


def apply_change(lake: Any, row: ChangeRow, sink_columns: tuple[str, ...]) -> bool:
    conn = lake.connection
    values = dict(row.values)
    order_id = values.get("id")
    if order_id is None:
        return False
    if row.change_type == ChangeType.DELETE:
        conn.execute(f"DELETE FROM {SINK_TABLE} WHERE id = ?", [order_id])
        return True
    if row.change_type == ChangeType.UPDATE_PREIMAGE:
        return False
    if row.change_type not in (ChangeType.INSERT, ChangeType.UPDATE_POSTIMAGE):
        return False

    conn.execute(f"DELETE FROM {SINK_TABLE} WHERE id = ?", [order_id])
    insert_values = [value_for_sink_column(column, values) for column in sink_columns]
    placeholders = ", ".join("?" for _ in sink_columns)
    conn.execute(
        f"""
        INSERT INTO {SINK_TABLE} ({", ".join(sink_columns)})
        VALUES ({placeholders})
        """,
        insert_values,
    )
    return True


def value_for_sink_column(column: str, values: dict[str, Any]) -> Any:
    if column in values:
        return values[column]
    for mutation in SCHEMA_MUTATIONS:
        if mutation.name == column:
            return mutation.default_value
    return None


def format_change(row: ChangeRow, epoch: int) -> str:
    values = dict(row.values)
    extras = " ".join(
        f"{mutation.name}={values[mutation.name]}"
        for mutation in active_mutations(epoch)
        if mutation.name in values
    )
    extras = "" if not extras else f" {extras}"
    return (
        f"v{epoch} snap={row.snapshot_id} {row.change_type.value} "
        f"id={values.get('id')} amount={values.get('amount')}{extras}"
    )


def trigger_schema_change(lake: Any, state: DemoState, mutation: SchemaMutation) -> int:
    state.update(phase="alter_source")
    if mutation.op == "add":
        state.add_dml(
            f"source ALTER TABLE orders ADD COLUMN {mutation.name} {mutation.sql_type}"
        )
        lake.connection.execute(
            f"ALTER TABLE lake.orders ADD COLUMN {mutation.name} {mutation.sql_type} DEFAULT {mutation.default_sql}"
        )
    elif mutation.op == "drop":
        state.add_dml(f"source ALTER TABLE orders DROP COLUMN {mutation.name}")
        lake.connection.execute(f"ALTER TABLE lake.orders DROP COLUMN {mutation.name}")
    else:
        raise AssertionError(f"unknown schema mutation op: {mutation.op}")
    refresh_schemas(lake, state)
    snapshot_id = catalog_head_snapshot(lake)
    state.update(boundary_snapshot=snapshot_id)
    return snapshot_id


def inspect_boundary(consumer: DMLConsumer, state: DemoState) -> int:
    with quiet_extension_output():
        window = consumer.window(max_snapshots=LISTEN_MAX_SNAPSHOTS)
    boundary = window.terminal_at_snapshot or window.start_snapshot
    state.update(
        phase="schema_boundary",
        boundary_snapshot=boundary,
        active_consumer=consumer.name,
    )
    state.add_dml(f"stopped due to schema diff at snapshot {boundary}")
    return int(boundary)


def read_ddl(client: CDCClient, state: DemoState) -> tuple[SchemaChangeRow, ...]:
    with quiet_extension_output():
        rows = client.cdc_ddl_changes_listen(
            DDL_CONSUMER,
            timeout_ms=LISTEN_TIMEOUT_MS,
            max_snapshots=LISTEN_MAX_SNAPSHOTS,
            poll_min_ms=POLL_MIN_MS,
        )
    if not rows:
        state.add_ddl("no DDL rows returned")
        return ()
    for row in rows:
        state.add_ddl(
            f"snap={row.snapshot_id} {row.event_kind.value}.{row.object_kind.value} "
            f"{row.schema_name or ''}.{row.object_name or ''}".strip()
        )
    client.cdc_commit(DDL_CONSUMER, max(row.end_snapshot or row.snapshot_id for row in rows))
    return tuple(rows)


def read_schema_diff(consumer: DMLConsumer, boundary_snapshot: int) -> tuple[DiffRow, ...]:
    with quiet_extension_output():
        diff = consumer.schema_diff(
            from_snapshot=boundary_snapshot,
            to_snapshot=boundary_snapshot,
        )
    rows = []
    for row in diff:
        rows.append(
            DiffRow(
                snapshot_id=row.snapshot_id,
                change_kind=row.change_kind,
                old_name=row.old_name,
                new_name=row.new_name,
                old_type=row.old_type,
                new_type=row.new_type,
            )
        )
    return tuple(rows)


def apply_sink_migration(lake: Any, state: DemoState, mutation: SchemaMutation) -> None:
    state.update(phase="migrating_sink")
    if mutation.op == "add":
        state.add_dml(
            f"applying migration: derived_orders ADD COLUMN {mutation.name}"
        )
        lake.connection.execute(
            f"ALTER TABLE {SINK_TABLE} ADD COLUMN {mutation.name} {mutation.sql_type} DEFAULT {mutation.default_sql}"
        )
    elif mutation.op == "drop":
        state.add_dml(f"applying migration: derived_orders DROP COLUMN {mutation.name}")
        lake.connection.execute(f"ALTER TABLE {SINK_TABLE} DROP COLUMN {mutation.name}")
    else:
        raise AssertionError(f"unknown schema mutation op: {mutation.op}")
    refresh_schemas(lake, state)
    state.update(migrations_applied=state.snapshot()["migrations_applied"] + 1)
    state.add_dml("migration applied")


def create_next_consumer(
    current_consumer: DMLConsumer, state: DemoState, epoch: int
) -> DMLConsumer:
    consumer_name = consumer_name_for_epoch(epoch)
    with quiet_extension_output():
        next_consumer = current_consumer.successor(
            consumer_name,
            max_snapshots=LISTEN_MAX_SNAPSHOTS,
        )
    current_consumer.__exit__(None, None, None)
    next_consumer.__enter__()
    state.update(phase=f"tail_v{epoch}", active_consumer=consumer_name, epoch=epoch)
    state.add_dml(f"resumed DML with {consumer_name}")
    return next_consumer


def update_row_counts(lake: Any, state: DemoState) -> None:
    source_count = int(lake.connection.execute(f"SELECT count(*) FROM {SOURCE_TABLE}").fetchone()[0])
    sink_count = int(lake.connection.execute(f"SELECT count(*) FROM {SINK_TABLE}").fetchone()[0])
    state.update(source_rows=source_count, sink_rows=sink_count)


def verify_invariant(lake: Any, state: DemoState) -> str:
    current_columns = [column.name for column in describe_table(lake, "orders")]
    comparable_columns = [column for column in current_columns if column != "updated_at"]
    projection = ", ".join(comparable_columns)
    mismatch = int(
        lake.connection.execute(
            f"""
            SELECT (
                SELECT count(*) FROM (
                    SELECT {projection} FROM {SOURCE_TABLE}
                    EXCEPT
                    SELECT {projection} FROM {SINK_TABLE}
                )
            ) + (
                SELECT count(*) FROM (
                    SELECT {projection} FROM {SINK_TABLE}
                    EXCEPT
                    SELECT {projection} FROM {SOURCE_TABLE}
                )
            )
            """
        ).fetchone()[0]
    )
    result = "ok" if mismatch == 0 else f"mismatch={mismatch}"
    state.update(phase="done", invariant=result)
    return result


def run_tail_phase(
    lake: Any,
    client: CDCClient,
    state: DemoState,
    recorder: MetricsRecorder,
    *,
    consumer_name: str,
    epoch: int,
    start_id: int,
    max_rows: int,
    stop: threading.Event,
) -> int:
    next_id = start_id
    applied = 0
    while not stop.is_set() and applied < max_rows:
        insert_order(lake, next_id, epoch=epoch)
        recorder.set_detail("last_inserted_id", next_id)
        applied += listen_and_apply(
            lake,
            client,
            state,
            recorder,
            consumer_name=consumer_name,
            epoch=epoch,
        )
        next_id += 1
        stop.wait(INSERT_INTERVAL_S)
    return next_id


def build_layout(recorder: MetricsRecorder, state: DemoState) -> Layout:
    snap = recorder.snapshot()
    view = state.snapshot()
    layout = Layout()
    layout.split(
        Layout(_header(snap, view), name="header", size=6),
        Layout(name="body"),
        Layout(_footer(view), name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(_schema_timeline_panel(view), name="schema_timeline", ratio=2),
        Layout(_tail_panel(view), name="tail", ratio=3),
        Layout(_schema_diff_panel(view), name="schema_diff", ratio=2),
    )
    return layout


def _header(snap: dict[str, Any], view: dict[str, Any]) -> Panel:
    title = Text()
    title.append("ducklake-cdc", style="bold")
    title.append(" · ")
    title.append(EXAMPLE)
    title.append("    elapsed ", style="dim")
    title.append(_fmt_duration(snap["elapsed_s"]))
    title.append("    errors ", style="dim")
    title.append(str(snap["errors"]))

    current_schema = _current_schema_label(view)
    story_line = Text()
    story_line.append("DATALAKE SCHEMA TIMELINE", style="bold")

    status = Text()
    status.append("current ", style="dim")
    status.append(current_schema, style="bold")
    status.append("   consumer ", style="dim")
    status.append(str(view["active_consumer"]), style="bold")
    status.append("   boundary ", style="dim")
    status.append(str(view["boundary_snapshot"] or "—"), style="bold")

    grid = Table.grid()
    grid.add_column()
    grid.add_row(title)
    grid.add_row(status)
    grid.add_row(story_line)
    return Panel(grid, border_style="dim", padding=(0, 1))


def _phase_story(view: dict[str, Any]) -> tuple[str, str, str]:
    phase = str(view["phase"])
    if phase.startswith("tail_v"):
        return (
            "TAILING DML",
            "Schemas match, so row changes flow safely into the sink.",
            "bright_green",
        )
    if phase == "alter_source":
        return (
            "SOURCE SCHEMA CHANGED",
            "orders changed shape; the old DML consumer must not continue blindly.",
            "yellow",
        )
    if phase == "schema_boundary":
        return (
            "DML STOPPED AT BOUNDARY",
            "CDC shows the diff before incompatible rows are applied.",
            "red",
        )
    if phase == "migrating_sink":
        return (
            "MIGRATING SINK",
            "derived_orders is altered to match the new source schema.",
            "cyan",
        )
    if phase == "done":
        return ("DONE", "Source and sink schemas match; invariant is checked.", "bright_green")
    return ("BOOT", "Preparing schema-safe source and sink consumers.", "dim")


def _current_schema_label(view: dict[str, Any]) -> str:
    # `diff_rows` is cumulative. Every DDL boundary (add or drop) creates
    # the next schema version in the timeline.
    return f"SCHEMA_{len(view['diff_rows']) + 1}"


def _footer(view: dict[str, Any]) -> Panel:
    text = Text(no_wrap=True, overflow="ellipsis")
    text.append(_current_schema_label(view), style="bold")
    text.append("  rows ", style="dim")
    text.append(f"{view['source_rows']}->{view['sink_rows']}", style="bold")
    text.append("  migrations ", style="dim")
    text.append(str(view["migrations_applied"]), style="bold")
    text.append("  invariant=", style="dim")
    if view["invariant"] == "ok":
        text.append("ok", style="bold")
    elif view["invariant"] == "pending":
        text.append("pending", style="dim")
    else:
        text.append(str(view["invariant"]), style="bold")
    return Panel(text, border_style="dim", padding=(0, 1))


def _schema_timeline_panel(view: dict[str, Any]) -> Panel:
    active_schema_number = int(_current_schema_label(view).split("_")[1])
    rows_by_epoch = view["rows_by_epoch"]

    table = Table.grid()
    table.add_column()
    table.add_row(Text("schema timeline", style="bold"))
    table.add_row(Text("each dot = one DML row event", style="dim"))
    table.add_row("")

    for schema_idx in range(1, active_schema_number + 1):
        schema_line = Text()
        marker = "●" if schema_idx == active_schema_number else "○"
        schema_line.append(f"{marker} SCHEMA_{schema_idx}", style="bold" if schema_idx == active_schema_number else "")
        if schema_idx == 1:
            schema_line.append("  original", style="dim")
        else:
            mutation = SCHEMA_MUTATIONS[schema_idx - 2]
            sign = "+" if mutation.op == "add" else "-"
            schema_line.append(f"  {sign} {mutation.name}", style="dim")
        table.add_row(schema_line)

        count = int(rows_by_epoch.get(f"v{schema_idx}", 0))
        if schema_idx < active_schema_number or count:
            for line in _timeline_dot_lines(count):
                table.add_row(line)
        if schema_idx < active_schema_number:
            table.add_row("")

    if active_schema_number == 1 and not view["source_schema"]:
        table.add_row(Text("(waiting for schema)", style="dim"))

    return Panel(table, title="PRODUCER SCHEMA HISTORY", border_style="dim")


def _timeline_dot_lines(count: int, *, width: int = 9) -> tuple[Text, ...]:
    if count <= 0:
        return (Text("   .", style="dim"),)
    shown = min(count, width)
    dots = " ".join("." for _ in range(shown))
    text = Text(f"   {dots}", style="dim")
    if count > shown:
        text.append(f"  +{count - shown}", style="dim")
    return (text,)


def _tail_panel(view: dict[str, Any]) -> Panel:
    table = Table.grid()
    table.add_column()
    table.add_row(Text("live DML tail", style="bold"))
    table.add_row("")
    for item in view["dml_tail"]:
        table.add_row(_tail_event_text(item))
    if not view["dml_tail"]:
        table.add_row("[dim]waiting for DML...[/dim]")
    table.add_row("")
    table.add_row(Text("DDL events", style="bold"))
    for item in view["ddl_tail"]:
        table.add_row(f"[dim]{item}[/dim]")
    return Panel(table, title="LIVE CDC TAIL", border_style="dim")


def _tail_event_text(item: str) -> Text:
    text = Text(item)
    if item.startswith("v") and " snap=" in item:
        text.stylize("white")
    else:
        text.stylize("dim yellow")
    return text


def _schema_diff_panel(view: dict[str, Any]) -> Panel:
    table = Table.grid()
    table.add_column()
    table.add_row(Text("schema.diff", style="bold"))
    table.add_row(Text("current orders schema vs original", style="dim"))
    table.add_row("")
    for line in _schema_diff_lines(view):
        table.add_row(line)

    table.add_row("")
    if view["boundary_snapshot"]:
        boundary = Text()
        boundary.append("boundary snapshot ", style="dim")
        boundary.append(str(view["boundary_snapshot"]), style="bold")
        table.add_row(boundary)
    table.add_row(Text("sink migrates, then the next DML consumer resumes", style="dim"))
    return Panel(table, title="CURRENT SCHEMA DIFF", border_style="dim")


def _schema_diff_lines(view: dict[str, Any]) -> tuple[Text, ...]:
    current = {col.name: col.type for col in view["source_schema"]}
    out: list[Text] = []
    for name in BASE_COLUMNS:
        line = Text()
        if name in current:
            line.append("  ")
            line.append(f"{name} {current[name]}")
        else:
            line.append("- ", style="bold red")
            line.append(name, style="red")
        out.append(line)

    added_columns = [name for name in current if name not in BASE_COLUMNS]
    for mutation in SCHEMA_MUTATIONS:
        if mutation.op == "add" and mutation.name in added_columns:
            line = Text()
            line.append("+ ", style="bold green")
            line.append(f"{mutation.name} {current[mutation.name]}", style="green")
            out.append(line)
        elif mutation.op == "drop" and mutation.name not in current and mutation.name not in BASE_COLUMNS:
            line = Text()
            line.append("- ", style="bold red")
            line.append(f"{mutation.name} {mutation.sql_type}", style="red")
            out.append(line)
    return tuple(out)


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
        diff = ",".join(row.change_kind for row in view["diff_rows"]) or "none"
        return (
            f"phase={view['phase']} rows={snap['rows_processed']} "
            f"source={view['source_rows']} sink={view['sink_rows']} "
            f"boundary={view['boundary_snapshot'] or '—'} diff={diff} "
            f"migrations={view['migrations_applied']} "
            f"invariant={view['invariant']} errors={snap['errors']}"
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
        "source_rows": view["source_rows"],
        "sink_rows": view["sink_rows"],
        "boundary_snapshot": view["boundary_snapshot"],
        "diff_rows": len(view["diff_rows"]),
        "migrations_applied": view["migrations_applied"],
        "invariant": view["invariant"],
    }


def drain_consumers(client: CDCClient, names: Iterable[str]) -> None:
    for name in names:
        try:
            client.cdc_consumer_force_release(name)
        except Exception:
            pass


def main(argv: list[str] | None = None) -> int:
    parser = make_parser(
        description="Schema-safe DML consumer that stops at DDL boundaries and resumes after migration.",
        supported_catalogs=("duckdb", "postgres"),
        supported_storages=("disk", "s3"),
    )
    args = parser.parse_args(argv)
    common = parse_common(args)
    max_rows = TOTAL_DML_EVENTS

    log(
        f"starting {EXAMPLE} mode={'headless' if common.headless else 'demo'} "
        f"catalog={common.catalog} storage={common.storage}"
    )
    reset_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)

    lake = open_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)
    load_cdc_extension(lake)
    setup_schema(lake)
    client = CDCClient(lake, install_extension=False)
    state = DemoState()
    recorder = MetricsRecorder(example=EXAMPLE, catalog=common.catalog, storage=common.storage)
    refresh_schemas(lake, state)
    update_row_counts(lake, state)
    create_ddl_consumer(client)
    log("schema and consumers ready")

    stop = threading.Event()
    current_consumer: DMLConsumer | None = None

    def _request_stop(signum: int, _frame: object) -> None:
        if stop.is_set():
            log(f"signal {signum} (again) -> hard exit", level="warn")
            os._exit(130)
        log(f"signal {signum} -> stop")
        stop.set()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    try:
        with LiveDisplay(
            headless=common.headless,
            renderable_factory=lambda: build_layout(recorder, state),
            headless_summary=headless_summary(recorder, state),
        ):
            epoch = 1
            consumer_name = consumer_name_for_epoch(epoch)
            current_consumer = DMLConsumer(
                lake,
                consumer_name,
                table="orders",
                start_at="now",
                mode="changes",
                connection=lake.connection,
            )
            current_consumer.__enter__()
            next_id = 1
            rows_since_migration = 0
            mutation_idx = 0
            state.update(phase="tail_v1", active_consumer=consumer_name, epoch=epoch)

            while not stop.is_set() and recorder.snapshot()["rows_processed"] < max_rows:
                next_id_before = next_id
                next_id = run_tail_phase(
                    lake,
                    current_consumer.client,
                    state,
                    recorder,
                    consumer_name=consumer_name,
                    epoch=epoch,
                    start_id=next_id,
                    max_rows=1,
                    stop=stop,
                )
                if next_id > next_id_before:
                    rows_since_migration += 1
                if (
                    rows_since_migration >= DDL_EVERY_ROWS
                    and mutation_idx < MAX_SCHEMA_MIGRATIONS
                    and not stop.is_set()
                ):
                    mutation = SCHEMA_MUTATIONS[mutation_idx]
                    boundary = trigger_schema_change(lake, state, mutation)
                    boundary = inspect_boundary(current_consumer, state) or boundary
                    ddl_rows = read_ddl(client, state)
                    recorder.set_detail(
                        "ddl_events",
                        int(recorder.snapshot()["details"].get("ddl_events", 0))
                        + len(ddl_rows),
                    )
                    diff_rows = (
                        *state.snapshot()["diff_rows"],
                        *read_schema_diff(current_consumer, boundary),
                    )
                    state.update(diff_rows=diff_rows)
                    recorder.set_detail("diff_rows", len(diff_rows))
                    apply_sink_migration(lake, state, mutation)
                    epoch += 1
                    current_consumer = create_next_consumer(
                        current_consumer,
                        state,
                        epoch,
                    )
                    consumer_name = current_consumer.name
                    mutation_idx += 1
                    rows_since_migration = 0

            if not stop.is_set():
                invariant = verify_invariant(lake, state)
                recorder.set_detail("invariant", invariant)
    except Exception as exc:  # noqa: BLE001
        recorder.record_error()
        state.update(phase="error", invariant=f"error: {type(exc).__name__}")
        cause = f" cause={exc.__cause__}" if exc.__cause__ is not None else ""
        log(f"fatal: {type(exc).__name__}: {exc}{cause}", level="error")
        return 1
    finally:
        try:
            if current_consumer is not None:
                current_consumer.__exit__(None, None, None)
            drain_consumers(
                client,
                (DDL_CONSUMER, *(consumer_name_for_epoch(i) for i in range(1, MAX_SCHEMA_MIGRATIONS + 2))),
            )
        finally:
            lake.close()
            reset_lake(example=EXAMPLE, catalog=common.catalog, storage=common.storage)

    log(headless_summary(recorder, state)())
    emit_json_summary(common, json_summary(recorder, state))
    return 0 if recorder.snapshot()["errors"] == 0 and state.snapshot()["invariant"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
