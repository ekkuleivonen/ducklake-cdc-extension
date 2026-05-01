"""Friendly Python client helpers for DuckLake."""

from ducklake.config import (
    CatalogConfig,
    DuckLakeModel,
    LocalCatalog,
    LocalStorage,
    PostgresCatalog,
    RawCatalog,
    S3Storage,
    SqliteCatalog,
    StorageConfig,
)
from ducklake.exceptions import (
    DuckLakeConfigError,
    DuckLakeConnectionError,
    DuckLakeError,
    DuckLakeQueryError,
    ResultCardinalityError,
)
from ducklake.lake import DuckLake
from ducklake.result import Result
from ducklake.session import Transaction
from ducklake.table import Column, Snapshot, Table

__all__ = [
    "Column",
    "CatalogConfig",
    "DuckLake",
    "DuckLakeConfigError",
    "DuckLakeConnectionError",
    "DuckLakeError",
    "DuckLakeModel",
    "DuckLakeQueryError",
    "LocalCatalog",
    "LocalStorage",
    "PostgresCatalog",
    "RawCatalog",
    "Result",
    "ResultCardinalityError",
    "S3Storage",
    "Snapshot",
    "SqliteCatalog",
    "StorageConfig",
    "Table",
    "Transaction",
]
