import pytest

from ducklake._attach import build_attach_sql
from ducklake.config import (
    DuckDBCatalog,
    DuckDBConfig,
    FileStorage,
    PostgresCatalog,
    S3Storage,
    parse_catalog,
    parse_storage,
)
from ducklake.exceptions import DuckLakeConfigError


def test_config_models_are_pydantic_models() -> None:
    catalog = PostgresCatalog(dsn="postgresql://user:pw@host/db")
    storage = S3Storage(bucket="bucket", region="us-east-1")

    assert catalog.model_dump() == {"dsn": "postgresql://user:pw@host/db"}
    assert storage.model_dump()["bucket"] == "bucket"


def test_plain_catalog_path_maps_to_duckdb_catalog() -> None:
    catalog = parse_catalog("catalog.ducklake")

    assert isinstance(catalog, DuckDBCatalog)
    assert catalog.attach_uri() == "ducklake:catalog.ducklake"


def test_postgres_catalog_url_maps_to_ducklake_attach_uri() -> None:
    catalog = parse_catalog("postgresql://user:pw@host/db")

    assert isinstance(catalog, PostgresCatalog)
    assert catalog.attach_uri() == "ducklake:postgres:postgresql://user:pw@host/db"
    assert catalog.required_extensions() == ("postgres",)


def test_file_storage_url_maps_to_file_storage() -> None:
    storage = parse_storage("file:///tmp/ducklake-data")

    assert isinstance(storage, FileStorage)
    assert storage.data_path() == "/tmp/ducklake-data"


def test_s3_storage_url_maps_to_data_path_and_secret() -> None:
    storage = parse_storage(
        "s3://bucket/path/to/data?endpoint=https://s3.example.com&region=us-east-1"
        "&key_id=abc&secret_access_key=def"
    )

    assert isinstance(storage, S3Storage)
    assert storage.data_path() == "s3://bucket/path/to/data"
    assert storage.required_extensions() == ("httpfs",)
    assert storage.setup_statements(secret_name="lake_storage") == (
        'CREATE OR REPLACE SECRET "lake_storage" '
        "(TYPE s3, KEY_ID 'abc', SECRET 'def', REGION 'us-east-1', "
        "ENDPOINT 's3.example.com', USE_SSL true)",
    )


def test_attach_sql_quotes_catalog_alias_and_data_path() -> None:
    sql = build_attach_sql(
        catalog=parse_catalog("catalog's.ducklake"),
        storage=parse_storage("data path"),
        alias="my lake",
        attach_options={"DATA_INLINING_ROW_LIMIT": 100},
    )

    assert sql == (
        "ATTACH 'ducklake:catalog''s.ducklake' AS \"my lake\" "
        "(DATA_PATH 'data path', DATA_INLINING_ROW_LIMIT 100)"
    )


def test_duckdb_config_uses_exact_setting_names() -> None:
    config = DuckDBConfig(
        threads=4,
        memory_limit="4GB",
        max_temp_directory_size="20GB",
        temp_directory="/tmp/duckdb",
        s3_uploader_max_filesize="50GB",
        settings={"enable_http_metadata_cache": True},
    )

    assert config.runtime_settings() == {
        "enable_http_metadata_cache": True,
        "threads": 4,
        "memory_limit": "4GB",
        "max_temp_directory_size": "20GB",
        "temp_directory": "/tmp/duckdb",
        "s3_uploader_max_filesize": "50GB",
    }


def test_duckdb_config_rejects_duplicate_setting_names() -> None:
    with pytest.raises(DuckLakeConfigError, match="threads"):
        DuckDBConfig(threads=4, settings={"threads": 8}).runtime_settings()
