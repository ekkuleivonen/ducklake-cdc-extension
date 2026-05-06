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
from rich.layout import Layout  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.text import Text  # noqa: E402

from _lib.cli import make_parser, parse_common  # noqa: E402
from _lib.config import load_cdc_extension, open_lake, reset_lake  # noqa: E402
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
MAX_SCHEMA_MIGRATIONS = 3
TOTAL_DML_EVENTS = DDL_EVERY_ROWS * (MAX_SCHEMA_MIGRATIONS + 1)
BASE_COLUMNS = ("id", "status", "amount", "updated_at")


@dataclass(frozen=True)
class SchemaMutation:
    name: str
    sql_type: str
    default_sql: str
    default_value: Any


SCHEMA_MUTATIONS: tuple[SchemaMutation, ...] = (
    SchemaMutation("tax", "DOUBLE", "0", 0.0),
    SchemaMutation("priority", "VARCHAR", "'standard'", "standard"),
    SchemaMutation("region", "VARCHAR", "'global'", "global"),
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
    conn = lake.connection
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.orders (
            id         BIGINT,
            status     VARCHAR,
            amount     DOUBLE,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.derived_orders (
            id         BIGINT,
            status     VARCHAR,
            amount     DOUBLE,
            updated_at TIMESTAMP
        )
        """
    )


def describe_table(lake: Any, table: str) -> tuple[SchemaColumn, ...]:
    rows = lake.connection.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()
    return tuple(SchemaColumn(str(row[0]), str(row[1]), str(row[2])) for row in rows)


def refresh_schemas(lake: Any, state: DemoState) -> None:
    state.update(
        source_schema=describe_table(lake, SOURCE_TABLE),
        sink_schema=describe_table(lake, SINK_TABLE),
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
    return SCHEMA_MUTATIONS[: min(MAX_SCHEMA_MIGRATIONS, max(0, epoch - 1))]


def insert_order(lake: Any, order_id: int, *, epoch: int) -> None:
    mutations = active_mutations(epoch)
    columns = ["id", "status", "amount", "updated_at", *(mutation.name for mutation in mutations)]
    placeholders = ["?", "?", "?", "now()", *("?" for _ in mutations)]
    values: list[Any] = [order_id, status_for_epoch(epoch), float(order_id * 10)]
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
    applied = 0
    conn = lake.connection
    conn.execute("BEGIN")
    try:
        for row in rows:
            if apply_change(conn, row):
                applied += 1
                state.add_dml(format_change(row, epoch))
        client.cdc_commit(consumer_name, end_snapshot)
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise

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


def apply_change(conn: Any, row: ChangeRow) -> bool:
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

    sink_columns = [str(col[0]) for col in conn.execute(f"DESCRIBE SELECT * FROM {SINK_TABLE}").fetchall()]
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
    state.add_dml(
        f"source ALTER TABLE orders ADD COLUMN {mutation.name} {mutation.sql_type}"
    )
    lake.connection.execute(
        f"ALTER TABLE lake.orders ADD COLUMN {mutation.name} {mutation.sql_type} DEFAULT {mutation.default_sql}"
    )
    refresh_schemas(lake, state)
    snapshot_id = current_snapshot(lake)
    state.update(boundary_snapshot=snapshot_id)
    return snapshot_id


def current_snapshot(lake: Any) -> int:
    value = lake.connection.execute(
        "SELECT max(snapshot_id) FROM __ducklake_metadata_lake.ducklake_snapshot"
    ).fetchone()[0]
    return int(value)


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
    state.add_dml(
        f"applying migration: derived_orders ADD COLUMN {mutation.name}"
    )
    lake.connection.execute(
        f"ALTER TABLE {SINK_TABLE} ADD COLUMN {mutation.name} {mutation.sql_type} DEFAULT {mutation.default_sql}"
    )
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
    current_columns = [column.name for column in describe_table(lake, SOURCE_TABLE)]
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
        Layout(_header(snap, view), name="header", size=3),
        Layout(name="body"),
        Layout(_footer(view), name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(_schema_panel(view), name="schema", ratio=2),
        Layout(_tail_panel(view), name="tail", ratio=3),
        Layout(_diff_panel(view), name="diff", ratio=2),
    )
    return layout


def _header(snap: dict[str, Any], view: dict[str, Any]) -> Panel:
    text = Text.from_markup(
        f"[bold]ducklake-cdc[/bold] · {EXAMPLE} · "
        f"[cyan]orders[/cyan] → [green]derived_orders[/green]   "
        f"[dim]phase[/dim] {view['phase']}   "
        f"[dim]elapsed[/dim] {_fmt_duration(snap['elapsed_s'])}   "
        f"[dim]errors[/dim] {snap['errors']}"
    )
    return Panel(text, border_style="dim")


def _footer(view: dict[str, Any]) -> Panel:
    text = (
        f"consumer={view['active_consumer']}  "
        f"boundary={view['boundary_snapshot'] or '—'}  "
        f"migrations={view['migrations_applied']}  "
        f"invariant={view['invariant']}"
    )
    return Panel(Text.from_markup(f"[dim]{text}[/dim]"), border_style="dim")


def _schema_panel(view: dict[str, Any]) -> Panel:
    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold")
    table.add_column()
    table.add_column(style="dim")
    table.add_row("source", "column", "type")
    for col in view["source_schema"]:
        table.add_row("", col.name, col.type)
    table.add_row("", "", "")
    table.add_row("sink", "column", "type")
    for col in view["sink_schema"]:
        table.add_row("", col.name, col.type)
    table.add_row("", "", "")
    table.add_row("rows", "source", _fmt_int(view["source_rows"]))
    table.add_row("", "sink", _fmt_int(view["sink_rows"]))
    return Panel(table, title="[bold cyan]current schema[/bold cyan]", border_style="cyan")


def _tail_panel(view: dict[str, Any]) -> Panel:
    table = Table.grid()
    table.add_column()
    for item in view["dml_tail"]:
        table.add_row(item)
    if not view["dml_tail"]:
        table.add_row("[dim]waiting for DML...[/dim]")
    table.add_row("")
    table.add_row("[bold]DDL[/bold]")
    for item in view["ddl_tail"]:
        table.add_row(f"[dim]{item}[/dim]")
    return Panel(table, title="[bold green]live CDC tail[/bold green]", border_style="green")


def _diff_panel(view: dict[str, Any]) -> Panel:
    table = Table.grid(padding=(0, 1))
    table.add_column(style="dim")
    table.add_column()
    table.add_column()
    table.add_column()
    table.add_row("snapshot", "kind", "old", "new")
    for row in view["diff_rows"]:
        old = f"{row.old_name or '—'} {row.old_type or ''}".strip()
        new = f"{row.new_name or '—'} {row.new_type or ''}".strip()
        table.add_row(str(row.snapshot_id), row.change_kind, old, new)
    if not view["diff_rows"]:
        table.add_row("—", "waiting", "—", "—")
    return Panel(table, title="[bold yellow]schema diff[/bold yellow]", border_style="yellow")


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
    return 0 if recorder.snapshot()["errors"] == 0 and state.snapshot()["invariant"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
