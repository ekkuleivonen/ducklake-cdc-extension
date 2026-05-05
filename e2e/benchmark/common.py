"""Shared benchmark DuckLake configuration."""

from __future__ import annotations

import shutil
import time
from collections.abc import Callable
from os import environ
from pathlib import Path
from typing import TypeVar
from urllib.parse import urlsplit

from ducklake_client import (
    DiskStorage,
    DuckDBCatalog,
    DuckDBConfig,
    DuckLake,
    DuckLakeError,
    PostgresCatalog,
    S3Storage,
    SqliteCatalog,
)

WORK_DIR = Path(__file__).resolve().parent / ".work"
CATALOG_PATH = WORK_DIR / "benchmark.sqlite"
DATA_PATH = WORK_DIR / "benchmark_data"
LOCK_RETRY_SECONDS = 0.2
CATALOG_ENV = "DUCKLAKE_BENCHMARK_CATALOG"
CATALOG_ADMIN_ENV = "DUCKLAKE_BENCHMARK_CATALOG_ADMIN"
STORAGE_ENV = "DUCKLAKE_BENCHMARK_STORAGE"
DEFAULT_POSTGRES_CATALOG = "postgresql://ducklake:ducklake@localhost:5435/ducklake"
DEFAULT_POSTGRES_ADMIN_CATALOG = "postgresql://ducklake:ducklake@localhost:5436/ducklake"
BENCHMARK_PG_POOL_MAX_CONNECTIONS = 64
CDC_EXTENSION_ENV = "DUCKLAKE_CDC_EXTENSION"
T = TypeVar("T")


def load_benchmark_cdc_extension(lake: DuckLake) -> None:
    """Load the benchmark ``ducklake_cdc`` artifact from disk (see ``resolve_cdc_extension_path``)."""

    lake.connection.load_extension(str(resolve_cdc_extension_path()))


def resolve_cdc_extension_path() -> Path:
    """Path to the locally built ducklake_cdc DuckDB extension (or override via env)."""

    repo_root = Path(__file__).resolve().parents[2]
    default = (
        repo_root
        / "build"
        / "release"
        / "extension"
        / "ducklake_cdc"
        / "ducklake_cdc.duckdb_extension"
    )
    configured = environ.get(CDC_EXTENSION_ENV)
    path = Path(configured).expanduser() if configured else default
    if not path.exists():
        raise SystemExit(
            "Local ducklake_cdc extension not found. Build it with `make release` "
            f"or set {CDC_EXTENSION_ENV}=/path/to/ducklake_cdc.duckdb_extension."
        )
    return path


def open_demo_lake(
    *,
    allow_unsigned_extensions: bool = False,
    catalog: str | None = None,
    catalog_backend: str | None = None,
    storage: str | None = None,
) -> DuckLake:
    duckdb_config: dict[str, int | bool] = {
        # The postgres-scanner pool defaults to 8 connections. The benchmark can
        # legitimately run one DuckLake connection per producer/consumer worker,
        # so lift the pool ceiling instead of making --workers 10 look hung.
        "pg_pool_max_connections": BENCHMARK_PG_POOL_MAX_CONNECTIONS,
    }
    if allow_unsigned_extensions:
        duckdb_config["allow_unsigned_extensions"] = True
    duckdb = DuckDBConfig(config=duckdb_config)
    catalog_input = resolve_catalog(catalog=catalog, catalog_backend=catalog_backend)
    storage_input = resolve_storage(storage=storage)
    return DuckLake(
        catalog=catalog_input,
        storage=storage_input,
        duckdb=duckdb,
    )


def resolve_catalog(
    *,
    catalog: str | None = None,
    catalog_backend: str | None = None,
) -> DuckDBCatalog | PostgresCatalog | SqliteCatalog:
    configured = catalog or environ.get(CATALOG_ENV)
    if configured:
        return _catalog_from_string(configured)
    if catalog_backend == "sqlite":
        return SqliteCatalog(path=CATALOG_PATH)
    return PostgresCatalog(DEFAULT_POSTGRES_CATALOG)


def resolve_storage(*, storage: str | None = None) -> DiskStorage | S3Storage:
    configured = storage or environ.get(STORAGE_ENV) or str(DATA_PATH)
    return _storage_from_string(configured)


def reset_demo_state(
    *,
    catalog: str | None = None,
    catalog_backend: str | None = None,
    storage: str | None = None,
) -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    reset_demo_catalog(catalog=resolve_catalog(catalog=catalog, catalog_backend=catalog_backend))
    reset_demo_storage(storage=resolve_storage(storage=storage))


def reset_demo_catalog(*, catalog: DuckDBCatalog | PostgresCatalog | SqliteCatalog) -> None:
    if isinstance(catalog, SqliteCatalog | DuckDBCatalog):
        Path(catalog.path).unlink(missing_ok=True)
        return
    reset_postgres_database(_postgres_reset_dsn(catalog.dsn))


def reset_demo_storage(*, storage: DiskStorage | S3Storage) -> None:
    if not isinstance(storage, DiskStorage):
        return
    raw_path = str(storage.path)
    parsed = urlsplit(raw_path)
    path = Path(parsed.path if parsed.scheme == "file" else raw_path)
    shutil.rmtree(path, ignore_errors=True)


def reset_postgres_database(dsn: str) -> None:
    try:
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
    except ImportError as exc:
        raise RuntimeError("Postgres benchmark reset requires the psycopg package") from exc

    params = {key: str(value) for key, value in conninfo_to_dict(dsn).items() if value is not None}
    database = params.get("dbname")
    if not database:
        raise ValueError("Postgres benchmark catalog DSN must include a database name")
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


def _postgres_reset_dsn(catalog: str) -> str:
    configured = environ.get(CATALOG_ADMIN_ENV)
    if configured:
        return _strip_ducklake_postgres_prefix(configured)

    dsn = _strip_ducklake_postgres_prefix(catalog)
    if dsn == DEFAULT_POSTGRES_CATALOG:
        return DEFAULT_POSTGRES_ADMIN_CATALOG
    return dsn


def _catalog_from_string(value: str) -> DuckDBCatalog | PostgresCatalog | SqliteCatalog:
    if _is_postgres_catalog(value):
        return PostgresCatalog(_strip_ducklake_postgres_prefix(value))
    if value.startswith("sqlite://"):
        return SqliteCatalog(path=urlsplit(value).path)
    if value.startswith("ducklake:sqlite:"):
        return SqliteCatalog(path=value.removeprefix("ducklake:sqlite:"))
    if value.startswith("ducklake:"):
        return DuckDBCatalog(path=value.removeprefix("ducklake:"))
    parsed = urlsplit(value)
    if parsed.scheme == "file":
        return DuckDBCatalog(path=parsed.path)
    if parsed.scheme:
        raise ValueError(f"unsupported DuckLake benchmark catalog URL: {value}")
    return DuckDBCatalog(path=value)


def _storage_from_string(value: str) -> DiskStorage | S3Storage:
    parsed = urlsplit(value)
    if parsed.scheme == "s3":
        return S3Storage(bucket=parsed.netloc, prefix=parsed.path.lstrip("/"))
    if parsed.scheme == "file":
        return DiskStorage(path=parsed.path)
    if parsed.scheme:
        raise ValueError(f"unsupported DuckLake benchmark storage URL: {value}")
    return DiskStorage(path=value)


def retry_on_lock(operation: Callable[[], T]) -> T:
    while True:
        try:
            return operation()
        except DuckLakeError as exc:
            if not (is_database_locked(exc) or is_thread_join_deadlock(exc)):
                raise
            time.sleep(LOCK_RETRY_SECONDS)


def is_database_locked(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if "database is locked" in str(current).lower():
            return True
        current = current.__cause__
    return False


def is_thread_join_deadlock(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        message = str(current).lower()
        if "thread::join failed" in message and "resource deadlock avoided" in message:
            return True
        current = current.__cause__
    return False
