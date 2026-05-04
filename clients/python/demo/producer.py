"""Generate demo DuckLake changes for the CDC consumer."""

from __future__ import annotations

import argparse
import random
import threading
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Protocol

from common import (
    CATALOG_ENV,
    DEFAULT_POSTGRES_CATALOG,
    STORAGE_ENV,
    is_database_locked,
    open_demo_lake,
    reset_demo_state,
    retry_on_lock,
)

from ducklake import DuckLake, DuckLakeError

RANDOM_SEED = 42
PROGRESS_INTERVAL_S = 5.0


@dataclass(frozen=True)
class TableRef:
    schema: str
    table: str

    @property
    def qualified(self) -> str:
        return f"lake.{quote_identifier(self.schema)}.{quote_identifier(self.table)}"


@dataclass(frozen=True)
class Action:
    kind: str
    table: TableRef
    row_id: int
    payload: str
    action_seq: int


@dataclass
class ProgressCounter:
    label: str
    total_commits: int
    total_actions: int
    interval_s: float = PROGRESS_INTERVAL_S
    completed_commits: int = 0
    completed_actions: int = 0
    started_at: float = field(default_factory=time.monotonic)
    last_report_at: float = field(default_factory=time.monotonic)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record(self, *, commits: int, actions: int, force: bool = False) -> None:
        with self._lock:
            self.completed_commits += commits
            self.completed_actions += actions
            now = time.monotonic()
            if (
                not force
                and self.completed_commits < self.total_commits
                and (now - self.last_report_at) < self.interval_s
            ):
                return
            self.last_report_at = now
            elapsed = max(now - self.started_at, 0.001)
            print(
                "producer demo: "
                f"{self.label} progress "
                f"{self.completed_commits}/{self.total_commits} commits, "
                f"{self.completed_actions}/{self.total_actions} actions, "
                f"{self.completed_actions / elapsed:.0f} actions/s",
                flush=True,
            )


class ResultLike(Protocol):
    def list(self) -> list[dict[str, Any]]: ...


class SqlRunner(Protocol):
    def sql(self, query: str, *parameters: object, **named_parameters: object) -> ResultLike: ...


@dataclass(frozen=True)
class Args:
    schemas: int
    tables: int
    inserts: int
    insert_rate: float | None
    update: float
    delete: float
    duration: float
    profile: str
    batch_min: int
    batch_max: int
    workers: int
    catalog: str | None
    catalog_backend: str | None
    storage: str | None
    reset: bool


def main() -> None:
    args = parse_args()
    rng = random.Random(RANDOM_SEED)

    if args.reset:
        reset_demo_state(
            catalog=args.catalog,
            catalog_backend=args.catalog_backend,
            storage=args.storage,
        )

    lake = open_demo_lake(
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )
    try:
        tables = create_layout(lake, args)
        actions = build_actions(args, tables, rng)
        batches = build_batches(actions, args, rng)
        print(
            "producer demo: "
            f"{len(tables)} tables, {len(actions)} actions, {len(batches)} commits, "
            f"{args.duration:g}s {args.profile}, {args.workers} worker(s)"
        )
        if args.insert_rate is not None:
            print(
                "producer demo: "
                f"{args.insert_rate:g} inserts/s target for {args.duration:g}s "
                f"({args.inserts} inserts/table)",
                flush=True,
            )
        if args.workers > 1:
            lake.close()
        run_batches(lake, batches, args)
    finally:
        lake.close()


def parse_args(argv: Sequence[str] | None = None) -> Args:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schemas", type=positive_int, default=1)
    parser.add_argument("--tables", type=positive_int, default=1)
    parser.add_argument("--inserts", type=non_negative_int, default=10)
    parser.add_argument(
        "--insert-rate",
        type=non_negative_float,
        help=(
            "target inserted rows per second. Requires --duration > 0 and "
            "produces an insert-only workload; total inserts are rate * duration"
        ),
    )
    parser.add_argument("--update", type=percentage, default=25.0)
    parser.add_argument("--delete", type=percentage, default=10.0)
    parser.add_argument("--duration", type=non_negative_float, default=0.0)
    parser.add_argument("--profile", choices=("flat", "ramp", "variate"), default="flat")
    parser.add_argument("--batch_min", type=positive_int, default=1)
    parser.add_argument("--batch_max", type=positive_int, default=10)
    parser.add_argument(
        "--workers",
        type=positive_int,
        default=1,
        help=(
            "number of concurrent producer workers. Values >1 produce inserts, "
            "updates, then deletes in separate dependency-safe phases"
        ),
    )
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
        "--reset",
        action="store_true",
        help=(
            "drop the metadata catalog + storage tree before writing. Defaults "
            "to off so a long-running consumer.py stays attached across "
            "producer runs; use this only when you want a clean baseline."
        ),
    )
    namespace = parser.parse_args(argv)
    if namespace.batch_min > namespace.batch_max:
        parser.error("--batch_min must be <= --batch_max")
    if namespace.insert_rate is not None:
        if namespace.duration <= 0:
            parser.error("--insert-rate requires --duration > 0")
        if namespace.update != 0.0 or namespace.delete != 0.0:
            parser.error("--insert-rate produces insert-only workloads; pass --update 0 --delete 0")
        namespace.inserts = inserts_per_table_for_rate(
            insert_rate=namespace.insert_rate,
            duration=namespace.duration,
            table_count=namespace.schemas * namespace.tables,
        )
    return Args(
        schemas=namespace.schemas,
        tables=namespace.tables,
        inserts=namespace.inserts,
        insert_rate=namespace.insert_rate,
        update=namespace.update,
        delete=namespace.delete,
        duration=namespace.duration,
        profile=namespace.profile,
        batch_min=namespace.batch_min,
        batch_max=namespace.batch_max,
        workers=namespace.workers,
        catalog=namespace.catalog,
        catalog_backend=namespace.catalog_backend,
        storage=namespace.storage,
        reset=namespace.reset,
    )


def create_layout(lake: DuckLake, args: Args) -> list[TableRef]:
    tables: list[TableRef] = []
    for schema_idx in range(args.schemas):
        schema = f"demo_schema_{schema_idx + 1:02d}"
        retry_sql(lake, f"CREATE SCHEMA IF NOT EXISTS {quote_qualified('lake', schema)}")
        for table_idx in range(args.tables):
            table = f"events_{table_idx + 1:02d}"
            ref = TableRef(schema=schema, table=table)
            # Idempotent so subsequent producer runs (without --reset)
            # don't drop tables out from under a long-running consumer.
            retry_sql(
                lake,
                f"""
                CREATE TABLE IF NOT EXISTS {ref.qualified} (
                    id INTEGER,
                    payload VARCHAR,
                    updated_count INTEGER,
                    deleted BOOLEAN,
                    produced_ns BIGINT,
                    produced_epoch_ns BIGINT,
                    action_seq BIGINT,
                    benchmark_profile VARCHAR,
                    benchmark_duration_s DOUBLE,
                    benchmark_schemas INTEGER,
                    benchmark_tables INTEGER,
                    benchmark_workers INTEGER,
                    benchmark_update_percent DOUBLE,
                    benchmark_delete_percent DOUBLE
                )
                """
            )
            tables.append(ref)
    return tables


def retry_sql(lake: DuckLake, query: str) -> None:
    retry_on_lock(lambda: lake.sql(query).list())


def build_actions(args: Args, tables: list[TableRef], rng: random.Random) -> list[Action]:
    actions: list[Action] = []
    inserted_rows: list[tuple[TableRef, int]] = []
    for table in tables:
        for row_id in range(1, args.inserts + 1):
            inserted_rows.append((table, row_id))
            actions.append(
                Action(
                    kind="insert",
                    table=table,
                    row_id=row_id,
                    payload=f"{table.schema}.{table.table}.{row_id}",
                    action_seq=len(actions) + 1,
                )
            )

    update_count = percent_count(len(inserted_rows), args.update)
    delete_count = percent_count(len(inserted_rows), args.delete)
    update_rows = rng.sample(inserted_rows, min(update_count, len(inserted_rows)))
    delete_rows = rng.sample(inserted_rows, min(delete_count, len(inserted_rows)))

    for table, row_id in update_rows:
        actions.append(
            Action(
                kind="update",
                table=table,
                row_id=row_id,
                payload="updated",
                action_seq=len(actions) + 1,
            )
        )
    for table, row_id in delete_rows:
        actions.append(
            Action(
                kind="delete",
                table=table,
                row_id=row_id,
                payload="deleted",
                action_seq=len(actions) + 1,
            )
        )
    return actions


def build_batches(actions: list[Action], args: Args, rng: random.Random) -> list[list[Action]]:
    batches: list[list[Action]] = []
    offset = 0
    while offset < len(actions):
        size = rng.randint(args.batch_min, args.batch_max)
        batches.append(actions[offset : offset + size])
        offset += size
    return batches


def run_batches(lake: DuckLake, batches: list[list[Action]], args: Args) -> None:
    if args.workers > 1:
        run_batches_concurrent(batches, args)
        return

    gaps = schedule_gaps(len(batches), args)
    start = time.monotonic()
    for idx, batch in enumerate(batches):
        apply_batch(lake, batch, args)
        print(f"commit {idx + 1}/{len(batches)}: {len(batch)} actions")
        if idx < len(gaps):
            time.sleep(gaps[idx])
    elapsed = time.monotonic() - start
    print(f"producer demo: completed in {elapsed:.2f}s")


def run_batches_concurrent(batches: list[list[Action]], args: Args) -> None:
    start = time.monotonic()
    phase_batches_by_kind = {
        phase: batches_for_phase(batches, phase, args)
        for phase in ("insert", "update", "delete")
    }
    total_commits = sum(len(phase_batches) for phase_batches in phase_batches_by_kind.values())
    for phase in ("insert", "update", "delete"):
        phase_batches = phase_batches_by_kind[phase]
        if not phase_batches:
            continue
        phase_duration = (
            args.duration * len(phase_batches) / total_commits
            if total_commits > 0
            else 0.0
        )
        run_phase_concurrent(phase, phase_batches, args, duration=phase_duration)
    elapsed = time.monotonic() - start
    print(f"producer demo: completed {total_commits} commits in {elapsed:.2f}s")


def run_phase_concurrent(
    phase: str, batches: list[list[Action]], args: Args, *, duration: float
) -> None:
    worker_batches = split_batches_for_phase(batches, phase, args.workers)
    worker_count = len(worker_batches)
    progress = ProgressCounter(
        label=phase,
        total_commits=len(batches),
        total_actions=sum(len(batch) for batch in batches),
    )
    print(
        f"producer demo: {phase} phase, {len(batches)} commits, "
        f"{worker_count} worker(s), {duration:.2f}s target"
    )
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                run_worker_batches,
                assigned_batches,
                args,
                duration=duration,
                progress=progress,
            )
            for assigned_batches in worker_batches
            if assigned_batches
        ]
        for future in as_completed(futures):
            future.result()
    progress.record(commits=0, actions=0, force=True)


def run_worker_batches(
    batches: list[list[Action]],
    args: Args,
    *,
    duration: float,
    progress: ProgressCounter | None = None,
) -> int:
    lake = open_demo_lake(
        catalog=args.catalog,
        catalog_backend=args.catalog_backend,
        storage=args.storage,
    )
    try:
        gaps = schedule_gaps_for_count(len(batches), duration, args.profile)
        for idx, batch in enumerate(batches):
            apply_batch(lake, batch, args)
            if progress is not None:
                progress.record(commits=1, actions=len(batch))
            if idx < len(gaps):
                time.sleep(gaps[idx])
        return len(batches)
    finally:
        lake.close()


def batches_for_phase(
    batches: list[list[Action]], phase: str, args: Args
) -> list[list[Action]]:
    table_groups: dict[TableRef, list[Action]] = {}
    for batch in batches:
        for action in batch:
            if action.kind == phase:
                table_groups.setdefault(action.table, []).append(action)

    phase_batches: list[list[Action]] = []
    for actions in table_groups.values():
        phase_batches.extend(chunk_actions(actions, args))
    return phase_batches


def chunk_actions(actions: list[Action], args: Args) -> list[list[Action]]:
    batches: list[list[Action]] = []
    offset = 0
    rng = random.Random(RANDOM_SEED + len(actions))
    while offset < len(actions):
        size = rng.randint(args.batch_min, args.batch_max)
        batches.append(actions[offset : offset + size])
        offset += size
    return batches


def split_batches(batches: list[list[Action]], worker_count: int) -> list[list[list[Action]]]:
    worker_batches: list[list[list[Action]]] = [[] for _ in range(worker_count)]
    for idx, batch in enumerate(batches):
        worker_batches[idx % worker_count].append(batch)
    return worker_batches


def split_batches_for_phase(
    batches: list[list[Action]], phase: str, requested_workers: int
) -> list[list[list[Action]]]:
    table_groups: dict[TableRef, list[list[Action]]] = {}
    for batch in batches:
        if not batch:
            continue
        table_groups.setdefault(batch[0].table, []).append(batch)
    if not table_groups:
        return []

    worker_count = min(requested_workers, len(table_groups))
    worker_batches: list[list[list[Action]]] = [[] for _ in range(worker_count)]
    for idx, group in enumerate(table_groups.values()):
        worker_batches[idx % worker_count].extend(group)
    return [assigned for assigned in worker_batches if assigned]


def schedule_gaps(batch_count: int, args: Args) -> list[float]:
    return schedule_gaps_for_count(batch_count, args.duration, args.profile)


def schedule_gaps_for_count(batch_count: int, duration: float, profile: str) -> list[float]:
    if batch_count <= 1 or duration <= 0:
        return [0.0] * max(batch_count - 1, 0)

    gap_count = batch_count - 1
    if profile == "flat":
        weights = [1.0] * gap_count
    elif profile == "ramp":
        weights = [float(gap_count - idx) for idx in range(gap_count)]
    else:
        rng = random.Random(RANDOM_SEED + 1)
        weights = [rng.uniform(0.25, 1.75) for _ in range(gap_count)]

    total_weight = sum(weights)
    return [duration * weight / total_weight for weight in weights]


def apply_batch(lake: DuckLake, batch: list[Action], args: Args) -> None:
    retry_count = 0
    while True:
        try:
            with lake.transaction() as tx:
                apply_action_group(tx, batch, args)
            return
        except DuckLakeError as exc:
            if not is_transient_ducklake_conflict(exc):
                raise
            retry_count += 1
            if retry_count % 10 == 0:
                print(
                    "producer demo: transient transaction conflict, "
                    f"retry {retry_count} for {batch[0].kind} batch on "
                    f"{batch[0].table.schema}.{batch[0].table.table}",
                    flush=True,
                )
            jitter = random.uniform(0.0, 0.2)
            time.sleep(min(0.2 * retry_count, 2.0) + jitter)


def apply_action_group(lake: SqlRunner, actions: list[Action], args: Args) -> None:
    if not actions:
        return
    first = actions[0]
    if any(action.kind != first.kind or action.table != first.table for action in actions):
        for action in actions:
            apply_action(lake, action, args)
        return
    if first.kind == "insert":
        apply_insert_batch(lake, actions, args)
    elif first.kind == "update":
        apply_update_batch(lake, actions, args)
    elif first.kind == "delete":
        apply_delete_batch(lake, actions)
    else:
        raise ValueError(f"unknown action kind: {first.kind}")


def apply_insert_batch(lake: SqlRunner, actions: list[Action], args: Args) -> None:
    if _is_contiguous_insert_batch(actions):
        apply_contiguous_insert_batch(lake, actions, args)
        return

    produced_ns = time.monotonic_ns()
    produced_epoch_ns = time.time_ns()
    params: dict[str, object] = {}
    rows: list[str] = []
    for idx, action in enumerate(actions):
        params.update(_action_params(action, args, idx, produced_ns, produced_epoch_ns))
        rows.append(
            "("
            f"$id_{idx}, $payload_{idx}, 0, false, "
            f"$produced_ns_{idx}, $produced_epoch_ns_{idx}, $action_seq_{idx}, "
            f"$benchmark_profile_{idx}, $benchmark_duration_s_{idx}, "
            f"$benchmark_schemas_{idx}, $benchmark_tables_{idx}, "
            f"$benchmark_workers_{idx}, $benchmark_update_percent_{idx}, "
            f"$benchmark_delete_percent_{idx}"
            ")"
        )
    lake.sql(
        f"""
        INSERT INTO {actions[0].table.qualified}
        VALUES {", ".join(rows)}
        """,
        **params,
    ).list()


def apply_contiguous_insert_batch(
    lake: SqlRunner,
    actions: list[Action],
    args: Args,
) -> None:
    first = actions[0]
    last = actions[-1]
    produced_ns = time.monotonic_ns()
    produced_epoch_ns = time.time_ns()
    payload_prefix = f"{first.table.schema}.{first.table.table}."
    lake.sql(
        f"""
        INSERT INTO {first.table.qualified}
        SELECT
            row_id,
            $payload_prefix || row_id::VARCHAR,
            0,
            false,
            $produced_ns,
            $produced_epoch_ns,
            $action_seq_base + (row_id - $first_row_id),
            $benchmark_profile,
            $benchmark_duration_s,
            $benchmark_schemas,
            $benchmark_tables,
            $benchmark_workers,
            $benchmark_update_percent,
            $benchmark_delete_percent
        FROM range($first_row_id, $end_row_id) AS generated(row_id)
        """,
        payload_prefix=payload_prefix,
        produced_ns=produced_ns,
        produced_epoch_ns=produced_epoch_ns,
        action_seq_base=first.action_seq,
        first_row_id=first.row_id,
        end_row_id=last.row_id + 1,
        benchmark_profile=args.profile,
        benchmark_duration_s=args.duration,
        benchmark_schemas=args.schemas,
        benchmark_tables=args.tables,
        benchmark_workers=args.workers,
        benchmark_update_percent=args.update,
        benchmark_delete_percent=args.delete,
    ).list()


def _is_contiguous_insert_batch(actions: list[Action]) -> bool:
    if not actions or actions[0].kind != "insert":
        return False
    first = actions[0]
    return all(
        action.kind == "insert"
        and action.table == first.table
        and action.row_id == first.row_id + idx
        and action.action_seq == first.action_seq + idx
        for idx, action in enumerate(actions)
    )


def apply_update_batch(lake: SqlRunner, actions: list[Action], args: Args) -> None:
    produced_ns = time.monotonic_ns()
    produced_epoch_ns = time.time_ns()
    params: dict[str, object] = {}
    rows: list[str] = []
    for idx, action in enumerate(actions):
        params.update(_action_params(action, args, idx, produced_ns, produced_epoch_ns))
        rows.append(
            "("
            f"$id_{idx}, $payload_{idx}, $produced_ns_{idx}, "
            f"$produced_epoch_ns_{idx}, $action_seq_{idx}, "
            f"$benchmark_profile_{idx}, $benchmark_duration_s_{idx}, "
            f"$benchmark_schemas_{idx}, $benchmark_tables_{idx}, "
            f"$benchmark_workers_{idx}, $benchmark_update_percent_{idx}, "
            f"$benchmark_delete_percent_{idx}"
            ")"
        )
    lake.sql(
        f"""
        UPDATE {actions[0].table.qualified} AS target
        SET
            payload = source.payload,
            updated_count = target.updated_count + 1,
            produced_ns = source.produced_ns,
            produced_epoch_ns = source.produced_epoch_ns,
            action_seq = source.action_seq,
            benchmark_profile = source.benchmark_profile,
            benchmark_duration_s = source.benchmark_duration_s,
            benchmark_schemas = source.benchmark_schemas,
            benchmark_tables = source.benchmark_tables,
            benchmark_workers = source.benchmark_workers,
            benchmark_update_percent = source.benchmark_update_percent,
            benchmark_delete_percent = source.benchmark_delete_percent
        FROM (
            VALUES {", ".join(rows)}
        ) AS source(
            id,
            payload,
            produced_ns,
            produced_epoch_ns,
            action_seq,
            benchmark_profile,
            benchmark_duration_s,
            benchmark_schemas,
            benchmark_tables,
            benchmark_workers,
            benchmark_update_percent,
            benchmark_delete_percent
        )
        WHERE target.id = source.id
        """,
        **params,
    ).list()


def apply_delete_batch(lake: SqlRunner, actions: list[Action]) -> None:
    params = {f"id_{idx}": action.row_id for idx, action in enumerate(actions)}
    lake.sql(
        f"""
        DELETE FROM {actions[0].table.qualified}
        WHERE id IN ({", ".join(f"$id_{idx}" for idx in range(len(actions)))})
        """,
        **params,
    ).list()


def _action_params(
    action: Action,
    args: Args,
    idx: int,
    produced_ns: int,
    produced_epoch_ns: int,
) -> dict[str, object]:
    return {
        f"id_{idx}": action.row_id,
        f"payload_{idx}": action.payload,
        f"produced_ns_{idx}": produced_ns,
        f"produced_epoch_ns_{idx}": produced_epoch_ns,
        f"action_seq_{idx}": action.action_seq,
        f"benchmark_profile_{idx}": args.profile,
        f"benchmark_duration_s_{idx}": args.duration,
        f"benchmark_schemas_{idx}": args.schemas,
        f"benchmark_tables_{idx}": args.tables,
        f"benchmark_workers_{idx}": args.workers,
        f"benchmark_update_percent_{idx}": args.update,
        f"benchmark_delete_percent_{idx}": args.delete,
    }


def is_transient_ducklake_conflict(exc: BaseException) -> bool:
    if is_database_locked(exc):
        return True
    current: BaseException | None = exc
    while current is not None:
        message = str(current).lower()
        if "transaction conflict" in message or "failed to commit ducklake transaction" in message:
            return True
        current = current.__cause__
    return False


def apply_action(lake: SqlRunner, action: Action, args: Args) -> None:
    if action.kind == "insert":
        produced_ns = time.monotonic_ns()
        produced_epoch_ns = time.time_ns()
        lake.sql(
            f"""
            INSERT INTO {action.table.qualified}
            VALUES (
                $id,
                $payload,
                0,
                false,
                $produced_ns,
                $produced_epoch_ns,
                $action_seq,
                $benchmark_profile,
                $benchmark_duration_s,
                $benchmark_schemas,
                $benchmark_tables,
                $benchmark_workers,
                $benchmark_update_percent,
                $benchmark_delete_percent
            )
            """,
            id=action.row_id,
            payload=action.payload,
            produced_ns=produced_ns,
            produced_epoch_ns=produced_epoch_ns,
            action_seq=action.action_seq,
            benchmark_profile=args.profile,
            benchmark_duration_s=args.duration,
            benchmark_schemas=args.schemas,
            benchmark_tables=args.tables,
            benchmark_workers=args.workers,
            benchmark_update_percent=args.update,
            benchmark_delete_percent=args.delete,
        ).list()
    elif action.kind == "update":
        produced_ns = time.monotonic_ns()
        produced_epoch_ns = time.time_ns()
        lake.sql(
            f"""
            UPDATE {action.table.qualified}
            SET
                payload = $payload,
                updated_count = updated_count + 1,
                produced_ns = $produced_ns,
                produced_epoch_ns = $produced_epoch_ns,
                action_seq = $action_seq,
                benchmark_profile = $benchmark_profile,
                benchmark_duration_s = $benchmark_duration_s,
                benchmark_schemas = $benchmark_schemas,
                benchmark_tables = $benchmark_tables,
                benchmark_workers = $benchmark_workers,
                benchmark_update_percent = $benchmark_update_percent,
                benchmark_delete_percent = $benchmark_delete_percent
            WHERE id = $id
            """,
            id=action.row_id,
            payload=action.payload,
            produced_ns=produced_ns,
            produced_epoch_ns=produced_epoch_ns,
            action_seq=action.action_seq,
            benchmark_profile=args.profile,
            benchmark_duration_s=args.duration,
            benchmark_schemas=args.schemas,
            benchmark_tables=args.tables,
            benchmark_workers=args.workers,
            benchmark_update_percent=args.update,
            benchmark_delete_percent=args.delete,
        ).list()
    elif action.kind == "delete":
        lake.sql(f"DELETE FROM {action.table.qualified} WHERE id = $id", id=action.row_id).list()
    else:
        raise ValueError(f"unknown action kind: {action.kind}")


def percent_count(total: int, percent: float) -> int:
    return round(total * percent / 100.0)


def inserts_per_table_for_rate(
    *,
    insert_rate: float,
    duration: float,
    table_count: int,
) -> int:
    total_inserts = round(insert_rate * duration)
    return max(1, (total_inserts + table_count - 1) // table_count)


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def quote_qualified(*parts: str) -> str:
    return ".".join(quote_identifier(part) for part in parts)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def percentage(value: str) -> float:
    parsed = float(value)
    if parsed < 0 or parsed > 100:
        raise argparse.ArgumentTypeError("must be between 0 and 100")
    return parsed


if __name__ == "__main__":
    main()
