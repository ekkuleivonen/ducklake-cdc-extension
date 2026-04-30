"""Phase 2 catalog-backend smoke matrix for ducklake-cdc.

This is intentionally smaller than the full Phase 1 SQLLogic suite. It
exercises the highest-risk portability contracts against every DuckLake
catalog backend:

* the Phase 1 README-style DDL + DML consumer flow;
* schema-boundary handling via `cdc_ddl` + `cdc_changes`;
* owner-token single-reader enforcement across two connections.

Run after `make debug`:

    uv run python test/catalog_matrix/catalog_matrix_smoke.py

Run the Postgres leg with the local fixture:

    docker compose -f test/catalog_matrix/docker-compose.yml up -d --wait
    uv run python test/catalog_matrix/catalog_matrix_smoke.py --backends duckdb sqlite postgres
    docker compose -f test/catalog_matrix/docker-compose.yml down -v
"""

from __future__ import annotations

import argparse
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

REPO = Path(__file__).resolve().parents[2]
BUILD = os.environ.get("DUCKLAKE_CDC_BUILD", "release")
DEFAULT_CDC_EXTENSION = REPO / "build" / BUILD / "extension" / "ducklake_cdc" / "ducklake_cdc.duckdb_extension"
DEFAULT_BACKENDS = ("duckdb", "sqlite")
ALL_BACKENDS = ("duckdb", "sqlite", "postgres")
DEFAULT_PG_DSN = "host=127.0.0.1 port=5434 user=ducklake password=ducklake dbname=ducklake"


@dataclass(frozen=True)
class BackendConfig:
    name: str
    attach_uri: str
    data_path: Path


def quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def require_rows(conn: duckdb.DuckDBPyConnection, sql: str) -> list[Any]:
    try:
        return conn.execute(sql).fetchall()
    except Exception as exc:
        raise RuntimeError(f"{sql}\n{exc}") from exc


def require_ok(conn: duckdb.DuckDBPyConnection, sql: str) -> None:
    require_rows(conn, sql)


def require_error(conn: duckdb.DuckDBPyConnection, sql: str, needle: str) -> None:
    try:
        conn.execute(sql).fetchall()
    except Exception as exc:
        if needle in str(exc):
            return
        raise RuntimeError(f"expected error containing {needle!r} from:\n{sql}\nactual error:\n{exc}") from exc
    raise RuntimeError(f"expected error containing {needle!r} from:\n{sql}")


def reset_postgres_catalog(pg_dsn: str) -> None:
    import psycopg

    with psycopg.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
            cur.execute("CREATE SCHEMA public")
        conn.commit()


def backend_config(name: str, workdir: Path, pg_dsn: str) -> BackendConfig:
    if name == "duckdb":
        return BackendConfig(
            name=name,
            attach_uri=f"ducklake:{workdir / 'catalog.ducklake'}",
            data_path=workdir / "duckdb_data",
        )
    if name == "sqlite":
        return BackendConfig(
            name=name,
            attach_uri=f"ducklake:sqlite:{workdir / 'catalog.sqlite'}",
            data_path=workdir / "sqlite_data",
        )
    if name == "postgres":
        reset_postgres_catalog(pg_dsn)
        return BackendConfig(
            name=name,
            attach_uri=f"ducklake:postgres:{pg_dsn}",
            data_path=workdir / "postgres_data",
        )
    raise ValueError(f"unknown backend {name!r}")


@contextmanager
def connected(config: BackendConfig, cdc_extension: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    config.data_path.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        require_ok(conn, "INSTALL ducklake")
        require_ok(conn, "LOAD ducklake")
        require_ok(conn, "LOAD parquet")
        if config.name == "sqlite":
            require_ok(conn, "INSTALL sqlite")
            require_ok(conn, "LOAD sqlite")
        elif config.name == "postgres":
            require_ok(conn, "INSTALL postgres")
            require_ok(conn, "LOAD postgres")
            # `pg_connection_limit` keeps the underlying postgres-scanner
            # client pool roomy enough for the lease test, which holds two
            # logical sessions over the same DuckLake catalog. The historic
            # `pg_pool_max_connections` knob was removed upstream.
            require_ok(conn, "SET pg_connection_limit = 64")
        require_ok(conn, "LOAD " + quote_string(str(cdc_extension)))
        require_ok(
            conn,
            "ATTACH "
            + quote_string(config.attach_uri)
            + " AS lake (DATA_PATH "
            + quote_string(str(config.data_path))
            + ")",
        )
        yield conn
    finally:
        conn.close()


def scalar(conn: duckdb.DuckDBPyConnection, sql: str) -> Any:
    rows = require_rows(conn, sql)
    if len(rows) != 1 or len(rows[0]) != 1:
        raise RuntimeError(f"expected one scalar row from:\n{sql}\ngot: {rows!r}")
    return rows[0][0]


def run_demo_flow(conn: duckdb.DuckDBPyConnection, backend: str) -> None:
    consumer = f"matrix_demo_{backend}"
    require_ok(conn, "CREATE TABLE lake.orders(id INTEGER, amount INTEGER)")
    require_ok(conn, f"SELECT * FROM cdc_consumer_create('lake', {quote_string(consumer)})")
    require_ok(conn, "INSERT INTO lake.orders VALUES (1, 100), (2, 200)")

    window = require_rows(conn, f"SELECT end_snapshot, has_changes FROM cdc_window('lake', {quote_string(consumer)})")
    if len(window) != 1 or window[0][1] is not True:
        raise RuntimeError(f"{backend}: expected first window with DML changes, got {window!r}")

    inserted = scalar(
        conn,
        "SELECT count(*) FROM cdc_changes('lake', "
        + quote_string(consumer)
        + ", 'orders') WHERE change_type = 'insert'",
    )
    if inserted != 2:
        raise RuntimeError(f"{backend}: expected two inserted rows before schema change, got {inserted}")
    require_ok(conn, f"SELECT * FROM cdc_commit('lake', {quote_string(consumer)}, {window[0][0]})")

    require_ok(conn, "ALTER TABLE lake.orders ADD COLUMN region VARCHAR DEFAULT 'UNKNOWN'")
    require_ok(conn, "INSERT INTO lake.orders VALUES (3, 300, 'EU')")

    ddl_events = require_rows(
        conn,
        "SELECT event_kind, object_kind FROM cdc_ddl('lake', "
        + quote_string(consumer)
        + ") WHERE event_kind = 'altered' AND object_kind = 'table'",
    )
    if ddl_events != [("altered", "table")]:
        raise RuntimeError(f"{backend}: expected one altered.table event, got {ddl_events!r}")

    new_rows = scalar(
        conn,
        "SELECT count(*) FROM cdc_changes('lake', "
        + quote_string(consumer)
        + ", 'orders') WHERE change_type = 'insert' AND id = 3 AND region = 'EU'",
    )
    if new_rows != 1:
        raise RuntimeError(f"{backend}: expected the post-DDL insert under the new schema, got {new_rows}")

    end_snapshot = scalar(conn, f"SELECT end_snapshot FROM cdc_window('lake', {quote_string(consumer)})")
    require_ok(conn, f"SELECT * FROM cdc_commit('lake', {quote_string(consumer)}, {end_snapshot})")

    # CDC state writes live in the metadata catalog now, so the final commit
    # should leave the consumer caught up to the DuckLake snapshot head.
    stats = require_rows(
        conn,
        "SELECT consumer_name, lag_snapshots, lease_alive FROM cdc_consumer_stats('lake', consumer := "
        + quote_string(consumer)
        + ")",
    )
    if len(stats) != 1 or stats[0][0] != consumer or stats[0][1] != 0 or stats[0][2] is not True:
        raise RuntimeError(f"{backend}: unexpected cdc_consumer_stats row: {stats!r}")


def run_lease_flow(config: BackendConfig, cdc_extension: Path) -> None:
    """Single-reader lease enforcement: a second concurrent reader against the
    same consumer must be rejected with `CDC_BUSY`, and a `force_release`
    invalidates the holder's owner-token so the holder's subsequent commit
    fails (also `CDC_BUSY`).

    The two readers run as two cursors on the same DuckDB instance because
    DuckDB enforces single-attach for the underlying catalog file (DuckDB and
    SQLite backends are local files; Postgres is multi-client at the catalog
    layer but we still share one DuckDB host process). Cursors share the
    ATTACH and the loaded extensions but each call generates its own
    owner-token, so the lease state in `__ducklake_cdc_consumers` is what
    actually arbitrates.
    """
    consumer = f"matrix_lease_{config.name}"
    with connected(config, cdc_extension) as conn:
        a = conn.cursor()
        b = conn.cursor()
        require_ok(a, "CREATE TABLE lake.lease_probe(id INTEGER)")
        require_ok(a, f"SELECT * FROM cdc_consumer_create('lake', {quote_string(consumer)})")
        require_ok(a, "INSERT INTO lake.lease_probe VALUES (1)")
        end_snapshot = scalar(a, f"SELECT end_snapshot FROM cdc_window('lake', {quote_string(consumer)})")
        require_error(b, f"SELECT * FROM cdc_window('lake', {quote_string(consumer)})", "CDC_BUSY")
        require_ok(b, f"SELECT * FROM cdc_consumer_force_release('lake', {quote_string(consumer)})")
        require_error(a, f"SELECT * FROM cdc_commit('lake', {quote_string(consumer)}, {end_snapshot})", "CDC_BUSY")


def run_backend(name: str, cdc_extension: Path, pg_dsn: str) -> None:
    # Each phase gets its own catalog dir + (for Postgres) its own clean
    # `public` schema so the demo and lease flows don't share consumer state.
    with tempfile.TemporaryDirectory(prefix=f"ducklake_cdc_{name}_demo_") as tmp:
        config = backend_config(name, Path(tmp), pg_dsn)
        print(f"[{name}] demo flow", flush=True)
        with connected(config, cdc_extension) as conn:
            run_demo_flow(conn, name)
    with tempfile.TemporaryDirectory(prefix=f"ducklake_cdc_{name}_lease_") as tmp:
        config = backend_config(name, Path(tmp), pg_dsn)
        print(f"[{name}] lease flow", flush=True)
        run_lease_flow(config, cdc_extension)
    print(f"[{name}] ok", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdc-extension", type=Path, default=DEFAULT_CDC_EXTENSION)
    parser.add_argument("--backends", nargs="+", choices=ALL_BACKENDS, default=list(DEFAULT_BACKENDS))
    parser.add_argument("--postgres-dsn", default=os.environ.get("DLCDC_CATALOG_PG_DSN", DEFAULT_PG_DSN))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cdc_extension = args.cdc_extension.resolve()
    if not cdc_extension.exists():
        raise FileNotFoundError(f"missing ducklake_cdc extension artifact: {cdc_extension}; run `make debug` first")
    for backend in args.backends:
        run_backend(backend, cdc_extension, args.postgres_dsn)
    print("catalog_matrix_smoke PASSED", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
