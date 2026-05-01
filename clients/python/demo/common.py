"""Shared demo DuckLake configuration."""

from __future__ import annotations

import time
from collections.abc import Callable
from os import environ
from pathlib import Path
from typing import TypeVar

from ducklake import DuckLake, DuckLakeError, SqliteCatalog

WORK_DIR = Path(__file__).resolve().parent / ".work"
CATALOG_PATH = WORK_DIR / "demo.sqlite"
DATA_PATH = WORK_DIR / "demo_data"
LOCK_RETRY_SECONDS = 0.2
CATALOG_ENV = "DUCKLAKE_DEMO_CATALOG"
STORAGE_ENV = "DUCKLAKE_DEMO_STORAGE"
T = TypeVar("T")


def open_demo_lake(
    *,
    allow_unsigned_extensions: bool = False,
    catalog: str | None = None,
    storage: str | None = None,
) -> DuckLake:
    config = {"allow_unsigned_extensions": "true"} if allow_unsigned_extensions else None
    catalog_input = catalog or environ.get(CATALOG_ENV) or SqliteCatalog(path=CATALOG_PATH)
    storage_input = storage or environ.get(STORAGE_ENV) or str(DATA_PATH)
    return DuckLake.open(
        catalog=catalog_input,
        storage=storage_input,
        duckdb_config=config,
    )


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
