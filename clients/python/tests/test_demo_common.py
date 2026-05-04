from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from ducklake import SqliteCatalog

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demo"))

common = importlib.import_module("common")
DEFAULT_POSTGRES_CATALOG = common.DEFAULT_POSTGRES_CATALOG
open_demo_lake = common.open_demo_lake
resolve_catalog = common.resolve_catalog
reset_demo_storage = common.reset_demo_storage


def test_demo_catalog_defaults_to_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DUCKLAKE_DEMO_CATALOG", raising=False)

    assert resolve_catalog() == DEFAULT_POSTGRES_CATALOG


def test_demo_catalog_backend_can_opt_into_sqlite(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DUCKLAKE_DEMO_CATALOG", raising=False)

    assert isinstance(resolve_catalog(catalog_backend="sqlite"), SqliteCatalog)


def test_explicit_catalog_wins_over_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DUCKLAKE_DEMO_CATALOG", raising=False)

    assert resolve_catalog(catalog="sqlite:///tmp/demo.sqlite", catalog_backend="postgres") == (
        "sqlite:///tmp/demo.sqlite"
    )


def test_demo_lake_sets_unsigned_extensions_at_connect_time(tmp_path: Path) -> None:
    lake = open_demo_lake(
        allow_unsigned_extensions=True,
        catalog=f"sqlite://{tmp_path / 'catalog.sqlite'}",
        storage=str(tmp_path / "data"),
    )

    duckdb = lake._manager.duckdb

    assert duckdb.config["allow_unsigned_extensions"] is True
    assert "allow_unsigned_extensions" not in duckdb.runtime_settings()


def test_reset_demo_storage_removes_local_parquet_tree(tmp_path: Path) -> None:
    storage = tmp_path / "data"
    parquet = storage / "schema" / "table" / "part.parquet"
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b"parquet-ish")

    reset_demo_storage(storage=str(storage))

    assert not storage.exists()
