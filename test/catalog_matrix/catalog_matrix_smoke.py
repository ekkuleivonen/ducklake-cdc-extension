"""Catalog-backend smoke test for the ducklake_cdc extension.

This is the narrow CI gate for DuckLake catalog portability. It runs the same
DDL + DML consumer flow and owner-token lease rejection check against DuckDB,
SQLite, and Postgres catalog backends.

Usage:

    uv run python test/catalog_matrix/catalog_matrix_smoke.py
    uv run python test/catalog_matrix/catalog_matrix_smoke.py --backends duckdb sqlite postgres

The Postgres backend expects the compose fixture in this directory by default.
Override it with DLCDC_CATALOG_PG_DSN or --postgres-dsn.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD = os.environ.get("DUCKLAKE_CDC_BUILD", "release")
CDC_EXTENSION = REPO_ROOT / "build" / BUILD / "extension" / "ducklake_cdc" / "ducklake_cdc.duckdb_extension"

DEFAULT_POSTGRES_DSN = "host=127.0.0.1 port=5434 user=ducklake password=ducklake dbname=ducklake"


@dataclass(frozen=True)
class Backend:
    name: str
    attach_sql: str
    before_attach: tuple[str, ...] = ()
    optional_before_attach: tuple[str, ...] = ()


def sql_quote(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(config={"allow_unsigned_extensions": "true"})


def load_extensions(con: duckdb.DuckDBPyConnection, backend: Backend) -> None:
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("LOAD parquet")
    for sql in backend.before_attach:
        con.execute(sql)
    for sql in backend.optional_before_attach:
        try:
            con.execute(sql)
        except duckdb.CatalogException as exc:
            if "unrecognized configuration parameter" not in str(exc):
                raise
    con.execute(f"LOAD {sql_quote(CDC_EXTENSION)}")


@contextmanager
def attached_pair(
    backend: Backend,
) -> Iterator[tuple[duckdb.DuckDBPyConnection, duckdb.DuckDBPyConnection]]:
    a = connect()
    b: duckdb.DuckDBPyConnection | None = None
    try:
        load_extensions(a, backend)
        a.execute(backend.attach_sql)

        # `cursor()` gives us a second DuckDB connection against the same
        # database, matching the C++ multiconnection smoke. Attaching the same
        # embedded DuckLake metadata file from two independent Python databases
        # trips DuckDB's unique file-handle guard.
        b = a.cursor()
        load_extensions(b, backend)
        has_lake = b.execute("SELECT count(*) FROM duckdb_databases() WHERE database_name = 'lake'").fetchone()[0]
        if has_lake == 0:
            b.execute(backend.attach_sql)
        yield a, b
    finally:
        if b is not None:
            b.close()
        a.close()


def reset_postgres_catalog(dsn: str) -> None:
    import psycopg

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
            cur.execute("CREATE SCHEMA public")
        conn.commit()


def duckdb_backend(workdir: Path, _: str) -> Backend:
    catalog = workdir / "catalog.ducklake"
    data = workdir / "duckdb_data"
    data.mkdir(parents=True, exist_ok=True)
    return Backend(
        name="duckdb",
        attach_sql=f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH {sql_quote(data)})",
    )


def sqlite_backend(workdir: Path, _: str) -> Backend:
    catalog = workdir / "catalog.sqlite"
    data = workdir / "sqlite_data"
    data.mkdir(parents=True, exist_ok=True)
    return Backend(
        name="sqlite",
        before_attach=("INSTALL sqlite", "LOAD sqlite"),
        attach_sql=f"ATTACH 'ducklake:sqlite:{catalog}' AS lake (DATA_PATH {sql_quote(data)})",
    )


def postgres_backend(workdir: Path, dsn: str) -> Backend:
    reset_postgres_catalog(dsn)
    data = workdir / "postgres_data"
    data.mkdir(parents=True, exist_ok=True)
    return Backend(
        name="postgres",
        before_attach=(
            "INSTALL postgres",
            "LOAD postgres",
        ),
        optional_before_attach=(
            "SET pg_pool_max_connections = 64",
            "SET pg_connection_limit = 64",
        ),
        attach_sql=f"ATTACH 'ducklake:postgres:{dsn}' AS lake (DATA_PATH {sql_quote(data)})",
    )


BACKENDS: dict[str, Callable[[Path, str], Backend]] = {
    "duckdb": duckdb_backend,
    "sqlite": sqlite_backend,
    "postgres": postgres_backend,
}


def one(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return con.execute(sql).fetchone()


def all_rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
    return con.execute(sql).fetchall()


def require_error(con: duckdb.DuckDBPyConnection, sql: str, needle: str) -> None:
    try:
        con.execute(sql).fetchall()
    except Exception as exc:  # noqa: BLE001 - smoke reports the concrete DuckDB text below.
        if needle in str(exc):
            return
        raise AssertionError(f"expected error containing {needle!r}, got: {exc}") from exc
    raise AssertionError(f"expected error containing {needle!r} from: {sql}")


def run_consumer_flow(a: duckdb.DuckDBPyConnection, b: duckdb.DuckDBPyConnection) -> None:
    a.execute("CREATE TABLE lake.orders(id INTEGER, status VARCHAR)")
    a.execute(
        "SELECT * FROM cdc_dml_consumer_create(" "'lake', 'orders_sink', table_name := 'orders', start_at := 'now'" ")"
    )

    a.execute("INSERT INTO lake.orders VALUES (1, 'new'), (2, 'paid')")
    start_snapshot, end_snapshot, has_changes, _, schema_pending = one(
        a,
        "SELECT start_snapshot, end_snapshot, has_changes, schema_version, schema_changes_pending "
        "FROM cdc_window('lake', 'orders_sink')",
    )
    if not has_changes:
        raise AssertionError("expected the initial DML window to have changes")
    if schema_pending:
        raise AssertionError("initial DML window should not report a schema boundary")

    require_error(b, "SELECT * FROM cdc_window('lake', 'orders_sink')", "CDC_BUSY")

    # The single typed DML payload API: rows project the pinned table's
    # native columns (`id`, `status`) at the top level — there's no JSON
    # `values` payload anymore.
    dml_rows = all_rows(
        a,
        "SELECT change_type, id, status "
        "FROM cdc_dml_changes_read("
        "'lake', 'orders_sink', "
        f"start_snapshot := {start_snapshot}, end_snapshot := {end_snapshot}"
        ") "
        "ORDER BY snapshot_id, rowid",
    )
    if len(dml_rows) != 2 or {row[0] for row in dml_rows} != {"insert"}:
        raise AssertionError(f"expected two insert rows from initial DML window, got {dml_rows!r}")
    if {(row[1], row[2]) for row in dml_rows} != {(1, "new"), (2, "paid")}:
        raise AssertionError(f"expected typed DML payloads with id/status, got {dml_rows!r}")

    a.execute(f"SELECT * FROM cdc_commit('lake', 'orders_sink', {end_snapshot})")


def run_schema_boundary_flow(a: duckdb.DuckDBPyConnection) -> None:
    """DDL surfaces the schema change; the original DML consumer terminates at
    its shape boundary; a fresh DML consumer started at the boundary can read
    DML against the new shape.
    """
    a.execute(
        "SELECT * FROM cdc_ddl_consumer_create(" "'lake', 'schema_watch', schemas := ['main'], start_at := 'now'" ")"
    )
    a.execute("ALTER TABLE lake.orders ADD COLUMN note VARCHAR")
    a.execute("INSERT INTO lake.orders VALUES (3, 'shipped', 'schema-boundary')")

    ddl_rows = all_rows(
        a,
        "SELECT event_kind, object_kind, object_name "
        "FROM cdc_ddl_changes_read('lake', 'schema_watch', auto_commit := true)",
    )
    if ("altered", "table", "orders") not in ddl_rows:
        raise AssertionError(f"expected ALTER TABLE event for orders, got {ddl_rows!r}")

    # The original DML consumer 'orders_sink' is pinned to the pre-ALTER
    # shape: it must report a terminal window with no rows visible to it
    # and `terminal_at_snapshot` set to the ALTER's snapshot id.
    has_changes, schema_pending, terminal, terminal_at = one(
        a,
        "SELECT has_changes, schema_changes_pending, terminal, terminal_at_snapshot "
        "FROM cdc_window('lake', 'orders_sink')",
    )
    if has_changes:
        raise AssertionError("orders_sink window should be empty at the shape boundary")
    if not schema_pending:
        raise AssertionError("orders_sink should report schema_changes_pending at its boundary")
    if not terminal:
        raise AssertionError("orders_sink should report terminal=true at its boundary")
    if terminal_at is None:
        raise AssertionError("orders_sink should expose terminal_at_snapshot at its boundary")

    require_error(
        a,
        f"SELECT * FROM cdc_commit('lake', 'orders_sink', {terminal_at})",
        "CDC_SCHEMA_TERMINATED",
    )

    # The orchestration story: the DDL consumer spotted the ALTER, the
    # operator (us) spawns a successor DML consumer at the boundary, and
    # that consumer reads the post-ALTER row under the new shape.
    a.execute(
        "SELECT * FROM cdc_dml_consumer_create("
        "'lake', 'orders_sink_v2', table_name := 'orders', "
        f"start_at := '{terminal_at}'"
        ")"
    )
    start_snapshot_v2, end_snapshot_v2, has_changes_v2, _, _, _, _ = one(
        a,
        "SELECT * FROM cdc_window('lake', 'orders_sink_v2')",
    )
    if not has_changes_v2:
        raise AssertionError("expected the successor DML window to see the post-ALTER INSERT")

    # `cdc_dml_changes_read` is THE typed DML payload API. Its rows now
    # project the pinned table's native columns at the top level (here:
    # the post-ALTER `id`, `status`, and the new `note`).
    typed_rows = all_rows(
        a,
        "SELECT change_type, id, status, note "
        "FROM cdc_dml_changes_read("
        "'lake', 'orders_sink_v2', "
        f"start_snapshot := {start_snapshot_v2}, end_snapshot := {end_snapshot_v2}"
        ") "
        "WHERE change_type = 'insert' "
        "ORDER BY snapshot_id, rowid",
    )
    if ("insert", 3, "shipped", "schema-boundary") not in typed_rows:
        raise AssertionError(f"expected typed post-ALTER insert row with the new note column, got {typed_rows!r}")

    a.execute(f"SELECT * FROM cdc_commit('lake', 'orders_sink_v2', {end_snapshot_v2})")


def run_backend(name: str, postgres_dsn: str) -> None:
    with tempfile.TemporaryDirectory(prefix=f"dlcdc_catalog_{name}_") as tmp:
        backend = BACKENDS[name](Path(tmp), postgres_dsn)
        with attached_pair(backend) as (a, b):
            run_consumer_flow(a, b)
            run_schema_boundary_flow(a)
    print(f"catalog_matrix_smoke[{name}] PASSED")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backends",
        nargs="+",
        choices=sorted(BACKENDS),
        default=["duckdb", "sqlite"],
        help="Catalog backends to exercise. Defaults to local embedded backends.",
    )
    parser.add_argument(
        "--postgres-dsn",
        default=os.environ.get("DLCDC_CATALOG_PG_DSN", DEFAULT_POSTGRES_DSN),
        help="Postgres DSN for the postgres backend.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if not CDC_EXTENSION.exists():
        print(
            f"missing ducklake_cdc artifact at {CDC_EXTENSION}; run `make {BUILD}` first",
            file=sys.stderr,
        )
        return 1

    for backend in args.backends:
        run_backend(backend, args.postgres_dsn)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
