"""Postgres regression for DDL listener progress and bounded metadata reads.

Requires the e2e Postgres service:

    docker compose -f e2e/docker-compose.yml up -d --wait postgres
    uv run --project e2e python e2e/smoke/ddl_postgres_listener_progress_smoke.py
"""

from __future__ import annotations

import ctypes
import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import duckdb
import psycopg
from _harness_env import CDC_EXTENSION

PG_DSN = "postgresql://ducklake:ducklake@localhost:5436/ducklake"
HISTORY_SNAPSHOTS = 300
T = TypeVar("T")


def reset_catalog() -> None:
    with psycopg.connect(PG_DSN) as pg:
        pg.execute('DROP SCHEMA IF EXISTS "__ducklake_cdc" CASCADE')
        pg.execute("DROP SCHEMA IF EXISTS public CASCADE")
        pg.execute("CREATE SCHEMA public")
        pg.commit()


def capture_postgres_queries(con: duckdb.DuckDBPyConnection, action: Callable[[], T]) -> tuple[T, str]:
    """Capture SQL emitted by DuckDB's Postgres scanner for one action."""

    con.execute("SET pg_debug_show_queries = true")
    with tempfile.TemporaryFile(mode="w+b") as captured:
        stdout = os.dup(1)
        stderr = os.dup(2)
        os.dup2(captured.fileno(), 1)
        os.dup2(captured.fileno(), 2)
        try:
            result = action()
            ctypes.CDLL(None).fflush(None)
        finally:
            os.dup2(stdout, 1)
            os.dup2(stderr, 2)
            os.close(stdout)
            os.close(stderr)
            con.execute("SET pg_debug_show_queries = false")
        captured.seek(0)
        queries = captured.read().decode(errors="replace")
    return result, queries


def full_snapshot_copies(queries: str) -> list[str]:
    copies = []
    for raw_statement in queries.split("\n\n"):
        statement = raw_statement.strip()
        if not statement.startswith("COPY (SELECT") or not (
            '"ducklake_snapshot"' in statement
            or '"ducklake_snapshot_changes"' in statement
        ):
            continue
        _, separator, predicate = statement.upper().partition(" WHERE ")
        if not separator or "SNAPSHOT_ID" not in predicate:
            copies.append(statement)
    return copies


def durable_cursor(consumer_name: str) -> tuple[int, int]:
    with psycopg.connect(PG_DSN) as pg:
        row = pg.execute(
            """
            SELECT c.last_committed_snapshot,
                   (SELECT max(snapshot_id) FROM public.ducklake_snapshot)
            FROM __ducklake_cdc.__ducklake_cdc_consumers c
            WHERE c.consumer_name = %s
            """,
            (consumer_name,),
        ).fetchone()
        assert row is not None
        return int(row[0]), int(row[1])


def main() -> None:
    reset_catalog()
    with tempfile.TemporaryDirectory(prefix="ducklake_cdc_ddl_pg_") as tmp:
        data_path = Path(tmp) / "data"
        shutil.rmtree(data_path, ignore_errors=True)
        data_path.mkdir()

        con = duckdb.connect(config={"allow_unsigned_extensions": True, "threads": 1})
        try:
            con.execute("INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake;")
            con.load_extension(str(CDC_EXTENSION))
            con.execute(f"ATTACH 'ducklake:postgres:{PG_DSN}' AS lake (DATA_PATH '{data_path.as_posix()}')")
            con.execute("SELECT cdc_version()")
            con.execute("SELECT count(*) FROM cdc_list_consumers('lake')")
            con.execute("CREATE TABLE lake.items(id BIGINT)")
            con.execute("SELECT * FROM cdc_ddl_consumer_create('lake', 'ddl_scan', start_at := 'now')")

            for value in range(HISTORY_SNAPSHOTS):
                con.execute("INSERT INTO lake.items VALUES (?)", [value])

            # Drain the initial history first. The transport assertion below
            # measures the steady-state case that regressed in Atlas: one new
            # DML snapshot waking a caught-up DDL listener.
            rows = con.execute("SELECT * FROM cdc_ddl_changes_listen('lake', 'ddl_scan', timeout_ms := 0)").fetchall()
            assert rows == []
            assert durable_cursor("ddl_scan")[0] == durable_cursor("ddl_scan")[1]

            con.execute("INSERT INTO lake.items VALUES (?)", [HISTORY_SNAPSHOTS])
            rows = con.execute(
                "SELECT * FROM cdc_ddl_changes_listen('lake', 'ddl_scan', timeout_ms := 0)"
            ).fetchall()
            assert rows == []

            cursor, head = durable_cursor("ddl_scan")
            assert cursor == head, (cursor, head)

            # Advancing empty DML windows must not consume the next real DDL.
            con.execute("ALTER TABLE lake.items ADD COLUMN tag VARCHAR")
            ddl_rows = con.execute(
                "SELECT event_kind, object_kind, object_name, end_snapshot FROM cdc_ddl_changes_listen('lake', 'ddl_scan', timeout_ms := 0)"
            ).fetchall()
            assert len(ddl_rows) == 1, ddl_rows
            assert ddl_rows[0][:3] == ("altered", "table", "items"), ddl_rows
            assert durable_cursor("ddl_scan")[0] < durable_cursor("ddl_scan")[1]
            con.execute(
                "SELECT * FROM cdc_commit('lake', 'ddl_scan', ?)",
                [ddl_rows[0][3]],
            )

            # Atlas uses the non-blocking read entry point. It must have the
            # same bounded metadata access and empty-window progress semantics.
            con.execute("SELECT * FROM cdc_ddl_consumer_create('lake', 'ddl_read_scan', start_at := 'now')")
            con.execute("INSERT INTO lake.items VALUES (?, NULL)", [HISTORY_SNAPSHOTS + 1])
            rows = con.execute("SELECT * FROM cdc_ddl_changes_read('lake', 'ddl_read_scan')").fetchall()
            assert rows == []
            read_cursor, read_head = durable_cursor("ddl_read_scan")
            assert read_cursor == read_head, (read_cursor, read_head)

            con.execute("ALTER TABLE lake.items ADD COLUMN score INTEGER")
            read_ddl_rows = con.execute(
                "SELECT event_kind, object_kind, object_name, end_snapshot "
                "FROM cdc_ddl_changes_read('lake', 'ddl_read_scan')"
            ).fetchall()
            assert len(read_ddl_rows) == 1, read_ddl_rows
            assert read_ddl_rows[0][:3] == ("altered", "table", "items"), read_ddl_rows
            assert durable_cursor("ddl_read_scan")[0] < durable_cursor("ddl_read_scan")[1]
            con.execute(
                "SELECT * FROM cdc_commit('lake', 'ddl_read_scan', ?)",
                [read_ddl_rows[0][3]],
            )

            # Atlas's global DML relay also polls with non-blocking read. At
            # the catalogue head that probe must not fall through to a full
            # DuckLake metadata window scan.
            con.execute("SELECT * FROM cdc_dml_consumer_create('lake', 'dml_read_scan', start_at := 'now')")
            dml_rows: list[tuple] = []

            def poll_dml_head() -> None:
                nonlocal dml_rows
                for _ in range(10):
                    dml_rows = con.execute(
                        "SELECT * FROM cdc_dml_ticks_read('lake', 'dml_read_scan')"
                    ).fetchall()

            _, dml_read_queries = capture_postgres_queries(con, poll_dml_head)
            assert dml_rows == []
            dml_cursor, dml_head = durable_cursor("dml_read_scan")
            assert dml_cursor == dml_head, (dml_cursor, dml_head)
            dml_full_copies = full_snapshot_copies(dml_read_queries)
            assert dml_full_copies == [], dml_full_copies

            # The probe is only an idle fast path. The next matching DML tick
            # remains caller-committed.
            con.execute("INSERT INTO lake.items VALUES (?, NULL, NULL)", [HISTORY_SNAPSHOTS + 2])
            dml_rows = con.execute(
                "SELECT table_ids, end_snapshot FROM cdc_dml_ticks_read('lake', 'dml_read_scan')"
            ).fetchall()
            assert len(dml_rows) == 1, dml_rows
            assert durable_cursor("dml_read_scan")[0] < durable_cursor("dml_read_scan")[1]
            con.execute(
                "SELECT * FROM cdc_commit('lake', 'dml_read_scan', ?)",
                [dml_rows[0][1]],
            )
        finally:
            con.close()

    print(
        "ddl postgres progress smoke: ok "
        "(DDL cursor progress verified, DML idle full copies=0)"
    )


if __name__ == "__main__":
    main()
