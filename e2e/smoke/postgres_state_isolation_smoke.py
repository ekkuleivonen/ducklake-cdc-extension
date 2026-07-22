"""Per-DuckLake CDC state isolation regression for a shared PostgreSQL database.

Requires the e2e PostgreSQL service and a release build:

    docker compose -f e2e/docker-compose.yml up -d --wait postgres
    uv run --project e2e python e2e/smoke/postgres_state_isolation_smoke.py
"""

from __future__ import annotations

import tempfile
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path

import duckdb
import psycopg
from _harness_env import CDC_EXTENSION

PG_DSN = "postgresql://ducklake:ducklake@localhost:5436/ducklake"
LAKES = {
    "alpha": ("ducklake_alpha", "ducklake_cdc_alpha"),
    "beta": ("ducklake_beta", "ducklake_cdc_beta"),
}


def reset_schemas() -> None:
    with psycopg.connect(PG_DSN) as pg:
        for metadata_schema, state_schema in LAKES.values():
            pg.execute(f'DROP SCHEMA IF EXISTS "{state_schema}" CASCADE')
            pg.execute(f'DROP SCHEMA IF EXISTS "{metadata_schema}" CASCADE')
            pg.execute(f'CREATE SCHEMA "{metadata_schema}"')
        pg.commit()


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list[object] | None = None) -> object:
    row = con.execute(sql, params or []).fetchone()
    assert row is not None
    return row[0]


def bootstrap_worker(metadata_schema: str, state_schema: str, data_path: str) -> None:
    """Bootstrap one lake in an independent runner process using the common alias `lake`."""

    con = duckdb.connect(config={"allow_unsigned_extensions": True, "threads": 1})
    try:
        con.execute("INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake;")
        con.load_extension(str(CDC_EXTENSION))
        con.execute(f"ATTACH 'ducklake:postgres:{PG_DSN}' AS lake (METADATA_SCHEMA '{metadata_schema}', DATA_PATH '{data_path}')")
        con.execute(f"CALL cdc_configure('lake', state_schema := '{state_schema}', metadata_schema := '{metadata_schema}')")
        assert con.execute("SELECT count(*) FROM cdc_list_consumers('lake')").fetchone() == (0,)
    finally:
        con.close()


def main() -> None:
    reset_schemas()
    with tempfile.TemporaryDirectory(prefix="ducklake_cdc_isolation_") as tmp:
        con = duckdb.connect(config={"allow_unsigned_extensions": True, "threads": 1})
        try:
            con.execute("INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake;")
            con.load_extension(str(CDC_EXTENSION))
            for alias, (metadata_schema, state_schema) in LAKES.items():
                data_path = (Path(tmp) / alias).as_posix()
                con.execute(f"ATTACH 'ducklake:postgres:{PG_DSN}' AS {alias} (METADATA_SCHEMA '{metadata_schema}', DATA_PATH '{data_path}')")
                configured = con.execute(f"CALL cdc_configure('{alias}', state_schema := '{state_schema}', metadata_schema := '{metadata_schema}')").fetchone()
                assert configured == (alias, state_schema, metadata_schema), configured
                con.execute(f"CREATE TABLE {alias}.items(id BIGINT)")

            # First bootstrap happens concurrently in separate runner
            # processes. Both intentionally attach their lake as `lake`.
            with ProcessPoolExecutor(max_workers=3, mp_context=get_context("spawn")) as executor:
                futures = [
                    executor.submit(
                        bootstrap_worker,
                        metadata_schema,
                        state_schema,
                        (Path(tmp) / alias).as_posix(),
                    )
                    for alias, (metadata_schema, state_schema) in LAKES.items()
                ]
                # A second Alpha runner races the exact same first bootstrap;
                # backend DDL and trigger installation must remain idempotent.
                futures.append(
                    executor.submit(
                        bootstrap_worker,
                        LAKES["alpha"][0],
                        LAKES["alpha"][1],
                        (Path(tmp) / "alpha").as_posix(),
                    )
                )
                for future in futures:
                    future.result(timeout=60)

            for alias in LAKES:
                con.execute(f"SELECT * FROM cdc_dml_consumer_create('{alias}', 'catalog_dml_ticks', start_at := 'now')")
                con.execute(f"SELECT * FROM cdc_ddl_consumer_create('{alias}', 'catalog_ddl', start_at := 'now')")

            assert scalar(con, "SELECT count(*) FROM cdc_list_consumers('alpha')") == 2
            assert scalar(con, "SELECT count(*) FROM cdc_list_consumers('beta')") == 2
            assert scalar(con, "SELECT count(*) FROM cdc_list_subscriptions('alpha')") == 2
            assert scalar(con, "SELECT count(*) FROM cdc_list_subscriptions('beta')") == 2
            assert scalar(con, "SELECT count(*) FROM cdc_consumer_stats('alpha')") == 2
            assert scalar(con, "SELECT count(*) FROM cdc_consumer_stats('beta')") == 2
            con.execute("SELECT * FROM cdc_doctor('alpha')").fetchall()
            con.execute("SELECT * FROM cdc_doctor('beta')").fetchall()

            con.execute("INSERT INTO alpha.items VALUES (1)")
            alpha_window = con.execute("SELECT start_snapshot, end_snapshot FROM cdc_window('alpha', 'catalog_dml_ticks')").fetchone()
            assert alpha_window is not None
            beta_window = con.execute("SELECT start_snapshot, end_snapshot FROM cdc_window('beta', 'catalog_dml_ticks')").fetchone()
            assert beta_window is not None and beta_window[0] > beta_window[1], beta_window

            con.execute(
                "SELECT * FROM cdc_commit('alpha', 'catalog_dml_ticks', ?)",
                [alpha_window[1]],
            )

            with psycopg.connect(PG_DSN) as pg:
                alpha_cursor = pg.execute(
                    "SELECT last_committed_snapshot FROM ducklake_cdc_alpha.__ducklake_cdc_consumers WHERE consumer_name = 'catalog_dml_ticks'"
                ).fetchone()
                beta_cursor = pg.execute(
                    "SELECT last_committed_snapshot FROM ducklake_cdc_beta.__ducklake_cdc_consumers WHERE consumer_name = 'catalog_dml_ticks'"
                ).fetchone()
                assert alpha_cursor == (alpha_window[1],), alpha_cursor
                assert beta_cursor is not None and beta_cursor[0] != alpha_window[1], beta_cursor

                alpha_trigger = pg.execute(
                    "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE NOT t.tgisinternal AND t.tgname = 'ducklake_cdc_snapshot_notify' "
                    "AND n.nspname = 'ducklake_alpha' AND c.relname = 'ducklake_snapshot'"
                ).fetchone()
                beta_trigger = pg.execute(
                    "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE NOT t.tgisinternal AND t.tgname = 'ducklake_cdc_snapshot_notify' "
                    "AND n.nspname = 'ducklake_beta' AND c.relname = 'ducklake_snapshot'"
                ).fetchone()
                assert alpha_trigger == (1,), alpha_trigger
                assert beta_trigger == (1,), beta_trigger

                beta_owner_before = pg.execute(
                    "SELECT owner_token::text FROM ducklake_cdc_beta.__ducklake_cdc_consumers WHERE consumer_name = 'catalog_dml_ticks'"
                ).fetchone()
                assert beta_owner_before is not None and beta_owner_before[0] is not None

            con.execute("SELECT * FROM cdc_consumer_force_release('alpha', 'catalog_dml_ticks')")
            assert scalar(con, "SELECT count(*) FROM cdc_audit_events('alpha')") >= 2
            assert scalar(con, "SELECT count(*) FROM cdc_audit_events('beta')") >= 1

            with psycopg.connect(PG_DSN) as pg:
                beta_owner_after = pg.execute(
                    "SELECT owner_token::text FROM ducklake_cdc_beta.__ducklake_cdc_consumers WHERE consumer_name = 'catalog_dml_ticks'"
                ).fetchone()
                assert beta_owner_after == beta_owner_before, (beta_owner_before, beta_owner_after)

            # A fresh runner may reuse the common alias `lake`; explicit schema
            # identity must still resume Beta's durable cursor only.
            restarted = duckdb.connect(config={"allow_unsigned_extensions": True, "threads": 1})
            try:
                restarted.execute("INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake;")
                restarted.load_extension(str(CDC_EXTENSION))
                restarted.execute(
                    f"ATTACH 'ducklake:postgres:{PG_DSN}' AS lake (METADATA_SCHEMA 'ducklake_beta', DATA_PATH '{(Path(tmp) / 'beta').as_posix()}')"
                )
                restarted.execute("CALL cdc_configure('lake', state_schema := 'ducklake_cdc_beta', metadata_schema := 'ducklake_beta')")
                resumed = restarted.execute("SELECT consumer_name, last_committed_snapshot FROM cdc_list_consumers('lake')").fetchall()
                assert len(resumed) == 2, resumed
                dml_resumed = [row for row in resumed if row[0] == "catalog_dml_ticks"]
                assert dml_resumed == [("catalog_dml_ticks", beta_cursor[0])], (resumed, beta_cursor)
            finally:
                restarted.close()

            with psycopg.connect(PG_DSN) as pg:
                pg.execute("DROP SCHEMA ducklake_cdc_alpha CASCADE")
                pg.commit()
            assert scalar(con, "SELECT count(*) FROM cdc_list_consumers('beta')") == 2
        finally:
            con.close()


if __name__ == "__main__":
    main()
