"""Typed configuration and URL parsing for DuckLake connections."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TypeAlias
from urllib.parse import SplitResult, parse_qs, unquote, urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ducklake.exceptions import DuckLakeConfigError


class DuckLakeModel(BaseModel):
    """Base Pydantic model for public DuckLake value types."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class CatalogConfig(DuckLakeModel):
    """Base class for DuckLake catalog configuration."""

    def attach_uri(self) -> str:
        raise NotImplementedError

    def required_extensions(self) -> tuple[str, ...]:
        return ()


DuckDBConfigValue: TypeAlias = str | bool | int | float | list[str]
DuckDBSettingValue: TypeAlias = str | bool | int | float
DuckDBSettings: TypeAlias = Mapping[str, DuckDBSettingValue]


class DuckDBCatalog(CatalogConfig):
    """A DuckDB-backed DuckLake catalog."""

    path: str | Path

    @field_validator("path")
    @classmethod
    def _path_must_not_be_empty(cls, value: str | Path) -> str | Path:
        if not str(value):
            raise ValueError("path must not be empty")
        return value

    def attach_uri(self) -> str:
        return f"ducklake:{self.path}"


class SqliteCatalog(CatalogConfig):
    """A DuckLake catalog stored in SQLite."""

    path: str | Path

    @field_validator("path")
    @classmethod
    def _path_must_not_be_empty(cls, value: str | Path) -> str | Path:
        if not str(value):
            raise ValueError("path must not be empty")
        return value

    def attach_uri(self) -> str:
        return f"ducklake:sqlite:{self.path}"

    def required_extensions(self) -> tuple[str, ...]:
        return ("sqlite",)


class PostgresCatalog(CatalogConfig):
    """A DuckLake catalog stored in PostgreSQL."""

    dsn: str

    @field_validator("dsn")
    @classmethod
    def _dsn_must_not_be_empty(cls, value: str) -> str:
        if not value:
            raise ValueError("dsn must not be empty")
        return value

    def attach_uri(self) -> str:
        return f"ducklake:postgres:{self.dsn}"

    def required_extensions(self) -> tuple[str, ...]:
        return ("postgres",)


class StorageConfig(DuckLakeModel):
    """Base class for DuckLake data storage configuration."""

    def data_path(self) -> str:
        raise NotImplementedError

    def required_extensions(self) -> tuple[str, ...]:
        return ()

    def setup_statements(self, *, secret_name: str) -> tuple[str, ...]:
        return ()


class FileStorage(StorageConfig):
    """Local filesystem data storage."""

    path: str | Path

    @field_validator("path")
    @classmethod
    def _path_must_not_be_empty(cls, value: str | Path) -> str | Path:
        if not str(value):
            raise ValueError("path must not be empty")
        return value

    def data_path(self) -> str:
        return str(self.path)


class S3Storage(StorageConfig):
    """S3-compatible object storage for DuckLake data files."""

    bucket: str
    prefix: str = ""
    endpoint: str | None = None
    region: str | None = None
    key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    url_style: str | None = None
    use_ssl: bool | None = None
    extra_secret_options: Mapping[str, str] = Field(default_factory=dict)

    @field_validator("bucket")
    @classmethod
    def _bucket_must_not_be_empty(cls, value: str) -> str:
        if not value:
            raise ValueError("bucket must not be empty")
        return value

    def data_path(self) -> str:
        suffix = f"/{self.prefix.lstrip('/')}" if self.prefix else ""
        return f"s3://{self.bucket}{suffix}"

    def required_extensions(self) -> tuple[str, ...]:
        return ("httpfs",)

    def setup_statements(self, *, secret_name: str) -> tuple[str, ...]:
        options: dict[str, str | bool] = {}
        if self.key_id:
            options["KEY_ID"] = self.key_id
        if self.secret_access_key:
            options["SECRET"] = self.secret_access_key
        if self.session_token:
            options["SESSION_TOKEN"] = self.session_token
        if self.region:
            options["REGION"] = self.region
        if self.endpoint:
            options["ENDPOINT"] = _endpoint_host(self.endpoint)
        if self.url_style:
            options["URL_STYLE"] = self.url_style
        if self.use_ssl is not None:
            options["USE_SSL"] = self.use_ssl
        for key, value in self.extra_secret_options.items():
            options[key.upper()] = value

        if not options:
            return ()

        rendered = ", ".join(
            ["TYPE s3", *(f"{key} {_format_secret_value(value)}" for key, value in options.items())]
        )
        return (f"CREATE OR REPLACE SECRET {quote_identifier(secret_name)} ({rendered})",)


class DuckDBConfig(DuckLakeModel):
    """DuckDB connection and runtime settings for a DuckLake client."""

    database: str | Path = ":memory:"
    config: Mapping[str, DuckDBConfigValue] = Field(default_factory=dict)
    extensions: tuple[str, ...] = ()
    settings: DuckDBSettings = Field(default_factory=dict)
    install_extensions: bool = True
    threads: int | None = None
    memory_limit: str | None = None
    max_temp_directory_size: str | None = None
    temp_directory: str | Path | None = None
    s3_uploader_max_filesize: str | None = None

    def runtime_settings(self) -> dict[str, DuckDBSettingValue]:
        settings = dict(self.settings)
        explicit_settings: dict[str, DuckDBSettingValue] = {}
        if self.threads is not None:
            explicit_settings["threads"] = self.threads
        if self.memory_limit is not None:
            explicit_settings["memory_limit"] = self.memory_limit
        if self.max_temp_directory_size is not None:
            explicit_settings["max_temp_directory_size"] = self.max_temp_directory_size
        if self.temp_directory is not None:
            explicit_settings["temp_directory"] = str(self.temp_directory)
        if self.s3_uploader_max_filesize is not None:
            explicit_settings["s3_uploader_max_filesize"] = self.s3_uploader_max_filesize

        duplicates = set(settings).intersection(explicit_settings)
        if duplicates:
            names = ", ".join(sorted(duplicates))
            raise DuckLakeConfigError(f"DuckDB settings specified more than once: {names}")
        settings.update(explicit_settings)
        return settings


CatalogInput: TypeAlias = str | CatalogConfig
StorageInput: TypeAlias = str | StorageConfig


def parse_catalog(catalog: CatalogInput) -> CatalogConfig:
    if isinstance(catalog, CatalogConfig):
        return catalog
    if not catalog:
        raise DuckLakeConfigError("catalog must not be empty")

    parsed = urlsplit(catalog)
    scheme = parsed.scheme.lower()
    if scheme in {"postgres", "postgresql"}:
        return PostgresCatalog(dsn=catalog)
    if scheme == "sqlite":
        return SqliteCatalog(path=_file_url_path(parsed))
    if scheme == "duckdb":
        return DuckDBCatalog(path=_file_url_path(parsed))
    if scheme == "file":
        return DuckDBCatalog(path=_file_url_path(parsed))
    if scheme:
        raise DuckLakeConfigError(f"unsupported DuckLake catalog URL scheme: {parsed.scheme!r}")
    return DuckDBCatalog(path=catalog)


def parse_storage(storage: StorageInput) -> StorageConfig:
    if isinstance(storage, StorageConfig):
        return storage
    if not storage:
        raise DuckLakeConfigError("storage must not be empty")

    parsed = urlsplit(storage)
    scheme = parsed.scheme.lower()
    if scheme == "s3":
        return _parse_s3_storage(parsed)
    if scheme == "file":
        return FileStorage(path=_file_url_path(parsed))
    if scheme:
        raise DuckLakeConfigError(f"unsupported DuckLake storage URL scheme: {parsed.scheme!r}")
    return FileStorage(path=storage)


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def quote_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _parse_s3_storage(parsed: SplitResult) -> S3Storage:
    bucket = unquote(parsed.netloc)
    if not bucket:
        raise DuckLakeConfigError("S3 storage URL must include a bucket")

    query = {
        key: values[-1]
        for key, values in parse_qs(parsed.query, keep_blank_values=True).items()
    }
    endpoint = query.pop("endpoint", None)
    region = query.pop("region", None)
    key_id = query.pop("key_id", query.pop("access_key_id", None))
    secret = query.pop("secret_access_key", query.pop("secret", None))
    token = query.pop("session_token", None)
    url_style = query.pop("url_style", None)
    use_ssl_value = query.pop("use_ssl", None)

    endpoint_use_ssl: bool | None = None
    if endpoint and "://" in endpoint:
        endpoint_use_ssl = urlsplit(endpoint).scheme == "https"
    use_ssl = _parse_bool(use_ssl_value) if use_ssl_value is not None else endpoint_use_ssl

    return S3Storage(
        bucket=bucket,
        prefix=unquote(parsed.path.lstrip("/")),
        endpoint=endpoint,
        region=region,
        key_id=key_id,
        secret_access_key=secret,
        session_token=token,
        url_style=url_style,
        use_ssl=use_ssl,
        extra_secret_options=query,
    )


def _file_url_path(parsed: SplitResult) -> str:
    path = unquote(parsed.path)
    if parsed.netloc and parsed.netloc != "localhost":
        path = f"//{parsed.netloc}{path}"
    return path


def _parse_bool(value: str) -> bool:
    normalized = value.lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise DuckLakeConfigError(f"invalid boolean value: {value!r}")


def _endpoint_host(endpoint: str) -> str:
    parsed = urlsplit(endpoint)
    if parsed.scheme and parsed.netloc:
        return urlunsplit(("", parsed.netloc, parsed.path, "", "")).removeprefix("//")
    return endpoint


def _format_secret_value(value: str | bool) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return quote_literal(value)
