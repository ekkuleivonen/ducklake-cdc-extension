"""Shared demo DuckLake configuration."""

from __future__ import annotations

import shutil
import time
from collections.abc import Callable
from os import environ
from pathlib import Path
from typing import TypeVar
from urllib.parse import urlsplit

from ducklake import DuckLake, DuckLakeError, SqliteCatalog

WORK_DIR = Path(__file__).resolve().parent / ".work"
CATALOG_PATH = WORK_DIR / "demo.sqlite"
DATA_PATH = WORK_DIR / "demo_data"
LOCK_RETRY_SECONDS = 0.2
CATALOG_ENV = "DUCKLAKE_DEMO_CATALOG"
STORAGE_ENV = "DUCKLAKE_DEMO_STORAGE"
DEFAULT_POSTGRES_CATALOG = "postgresql://ducklake:ducklake@localhost:5435/ducklake"
T = TypeVar("T")


def open_demo_lake(
    *,
    allow_unsigned_extensions: bool = False,
    catalog: str | None = None,
    catalog_backend: str | None = None,
    storage: str | None = None,
) -> DuckLake:
    config = {"allow_unsigned_extensions": "true"} if allow_unsigned_extensions else None
    catalog_input = resolve_catalog(catalog=catalog, catalog_backend=catalog_backend)
    storage_input = resolve_storage(storage=storage)
    return DuckLake.open(
        catalog=catalog_input,
        storage=storage_input,
        duckdb_config=config,
    )


def resolve_catalog(
    *,
    catalog: str | None = None,
    catalog_backend: str | None = None,
) -> str | SqliteCatalog:
    configured = catalog or environ.get(CATALOG_ENV)
    if configured:
        return configured
    if catalog_backend == "sqlite":
        return SqliteCatalog(path=CATALOG_PATH)
    return DEFAULT_POSTGRES_CATALOG


def resolve_storage(*, storage: str | None = None) -> str:
    return storage or environ.get(STORAGE_ENV) or str(DATA_PATH)


def reset_demo_state(
    *,
    catalog: str | None = None,
    catalog_backend: str | None = None,
    storage: str | None = None,
) -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    reset_demo_catalog(catalog=resolve_catalog(catalog=catalog, catalog_backend=catalog_backend))
    reset_demo_storage(storage=resolve_storage(storage=storage))


def reset_demo_catalog(*, catalog: str | SqliteCatalog) -> None:
    if isinstance(catalog, SqliteCatalog):
        Path(catalog.path).unlink(missing_ok=True)
        return
    if _is_postgres_catalog(catalog):
        reset_postgres_database(_strip_ducklake_postgres_prefix(catalog))
        return
    if catalog.startswith("sqlite://"):
        Path(urlsplit(catalog).path).unlink(missing_ok=True)
        return
    if catalog.startswith("ducklake:sqlite:"):
        Path(catalog.removeprefix("ducklake:sqlite:")).unlink(missing_ok=True)
        return
    if catalog.startswith("ducklake:"):
        Path(catalog.removeprefix("ducklake:")).unlink(missing_ok=True)
        return
    parsed = urlsplit(catalog)
    if parsed.scheme in {"", "file"}:
        Path(parsed.path if parsed.scheme == "file" else catalog).unlink(missing_ok=True)


def reset_demo_storage(*, storage: str) -> None:
    parsed = urlsplit(storage)
    if parsed.scheme not in {"", "file"}:
        return
    path = Path(parsed.path if parsed.scheme == "file" else storage)
    shutil.rmtree(path, ignore_errors=True)


def reset_postgres_database(dsn: str) -> None:
    try:
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
    except ImportError as exc:
        raise RuntimeError("Postgres demo reset requires the psycopg package") from exc

    params = conninfo_to_dict(dsn)
    database = params.get("dbname")
    if not database:
        raise ValueError("Postgres demo catalog DSN must include a database name")
    maintenance_params = dict(params)
    maintenance_params["dbname"] = "postgres" if database != "postgres" else "template1"
    maintenance_dsn = make_conninfo(**maintenance_params)

    with psycopg.connect(maintenance_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s
                  AND pid <> pg_backend_pid()
                """,
                (database,),
            )
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database)))
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))


def _is_postgres_catalog(catalog: str) -> bool:
    return catalog.startswith(("postgres://", "postgresql://", "ducklake:postgres:"))


def _strip_ducklake_postgres_prefix(catalog: str) -> str:
    return catalog.removeprefix("ducklake:postgres:")


def retry_on_lock(operation: Callable[[], T]) -> T:
    while True:
        try:
            return operation()
        except DuckLakeError as exc:
            if not is_database_locked(exc):
                raise
            time.sleep(LOCK_RETRY_SECONDS)


def is_database_locked(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if "database is locked" in str(current).lower():
            return True
        current = current.__cause__
    return False
