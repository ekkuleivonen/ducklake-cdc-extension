from ducklake._attach import build_attach_sql
from ducklake.config import PostgresCatalog, S3Storage, parse_catalog, parse_storage


def test_config_models_are_pydantic_models() -> None:
    catalog = PostgresCatalog(dsn="postgresql://user:pw@host/db")
    storage = S3Storage(bucket="bucket", region="us-east-1")

    assert catalog.model_dump() == {"dsn": "postgresql://user:pw@host/db"}
    assert storage.model_dump()["bucket"] == "bucket"


def test_postgres_catalog_url_maps_to_ducklake_attach_uri() -> None:
    catalog = parse_catalog("postgresql://user:pw@host/db")

    assert isinstance(catalog, PostgresCatalog)
    assert catalog.attach_uri() == "ducklake:postgres:postgresql://user:pw@host/db"
    assert catalog.required_extensions() == ("postgres",)


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
