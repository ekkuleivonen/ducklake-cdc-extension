"""Generate demo DuckLake changes for the CDC consumer."""

from __future__ import annotations

import argparse
import random
import time
from collections.abc import Sequence
from dataclasses import dataclass
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


class ResultLike(Protocol):
    def list(self) -> list[dict[str, Any]]: ...


class SqlRunner(Protocol):
    def sql(self, query: str, *parameters: object, **named_parameters: object) -> ResultLike: ...


@dataclass(frozen=True)
class Args:
    schemas: int
    tables: int
    inserts: int
    update: float
    delete: float
    duration: float
    profile: str
    batch_min: int
    batch_max: int
    reset: bool
    catalog: str | None
    catalog_backend: str | None
    storage: str | None


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
            f"{args.duration:g}s {args.profile}"
        )
        run_batches(lake, batches, args)
    finally:
        lake.close()


def parse_args(argv: Sequence[str] | None = None) -> Args:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schemas", type=positive_int, default=1)
    parser.add_argument("--tables", type=positive_int, default=1)
    parser.add_argument("--inserts", type=non_negative_int, default=10)
    parser.add_argument("--update", type=percentage, default=25.0)
    parser.add_argument("--delete", type=percentage, default=10.0)
    parser.add_argument("--duration", type=non_negative_float, default=0.0)
    parser.add_argument("--profile", choices=("flat", "ramp", "variate"), default="flat")
    parser.add_argument("--batch_min", type=positive_int, default=1)
    parser.add_argument("--batch_max", type=positive_int, default=10)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="reset the demo catalog and local storage before producing",
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
    namespace = parser.parse_args(argv)
    if namespace.batch_min > namespace.batch_max:
        parser.error("--batch_min must be <= --batch_max")
    return Args(
        schemas=namespace.schemas,
        tables=namespace.tables,
        inserts=namespace.inserts,
        update=namespace.update,
        delete=namespace.delete,
        duration=namespace.duration,
        profile=namespace.profile,
        batch_min=namespace.batch_min,
        batch_max=namespace.batch_max,
        reset=namespace.reset,
        catalog=namespace.catalog,
        catalog_backend=namespace.catalog_backend,
        storage=namespace.storage,
    )


def create_layout(lake: DuckLake, args: Args) -> list[TableRef]:
    tables: list[TableRef] = []
    for schema_idx in range(args.schemas):
        schema = f"demo_schema_{schema_idx + 1:02d}"
        retry_sql(lake, f"CREATE SCHEMA IF NOT EXISTS {quote_qualified('lake', schema)}")
        for table_idx in range(args.tables):
            table = f"events_{table_idx + 1:02d}"
            ref = TableRef(schema=schema, table=table)
            retry_sql(lake, f"DROP TABLE IF EXISTS {ref.qualified}")
            retry_sql(
                lake,
                f"""
                CREATE TABLE {ref.qualified} (
                    id INTEGER,
                    payload VARCHAR,
                    updated_count INTEGER,
                    deleted BOOLEAN,
                    produced_ns BIGINT,
                    action_seq BIGINT
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
    gaps = schedule_gaps(len(batches), args)
    start = time.monotonic()
    for idx, batch in enumerate(batches):
        apply_batch(lake, batch)
        print(f"commit {idx + 1}/{len(batches)}: {len(batch)} actions")
        if idx < len(gaps):
            time.sleep(gaps[idx])
    elapsed = time.monotonic() - start
    print(f"producer demo: completed in {elapsed:.2f}s")


def schedule_gaps(batch_count: int, args: Args) -> list[float]:
    if batch_count <= 1 or args.duration <= 0:
        return [0.0] * max(batch_count - 1, 0)

    gap_count = batch_count - 1
    if args.profile == "flat":
        weights = [1.0] * gap_count
    elif args.profile == "ramp":
        weights = [float(gap_count - idx) for idx in range(gap_count)]
    else:
        rng = random.Random(RANDOM_SEED + 1)
        weights = [rng.uniform(0.25, 1.75) for _ in range(gap_count)]

    total_weight = sum(weights)
    return [args.duration * weight / total_weight for weight in weights]


def apply_batch(lake: DuckLake, batch: list[Action]) -> None:
    while True:
        try:
            with lake.transaction() as tx:
                for action in batch:
                    apply_action(tx, action)
            return
        except DuckLakeError as exc:
            if not is_database_locked(exc):
                raise
            time.sleep(0.2)


def apply_action(lake: SqlRunner, action: Action) -> None:
    if action.kind == "insert":
        lake.sql(
            f"""
            INSERT INTO {action.table.qualified}
            VALUES ($id, $payload, 0, false, $produced_ns, $action_seq)
            """,
            id=action.row_id,
            payload=action.payload,
            produced_ns=time.monotonic_ns(),
            action_seq=action.action_seq,
        ).list()
    elif action.kind == "update":
        lake.sql(
            f"""
            UPDATE {action.table.qualified}
            SET
                payload = $payload,
                updated_count = updated_count + 1,
                produced_ns = $produced_ns,
                action_seq = $action_seq
            WHERE id = $id
            """,
            id=action.row_id,
            payload=action.payload,
            produced_ns=time.monotonic_ns(),
            action_seq=action.action_seq,
        ).list()
    elif action.kind == "delete":
        lake.sql(f"DELETE FROM {action.table.qualified} WHERE id = $id", id=action.row_id).list()
    else:
        raise ValueError(f"unknown action kind: {action.kind}")


def percent_count(total: int, percent: float) -> int:
    return round(total * percent / 100.0)


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
