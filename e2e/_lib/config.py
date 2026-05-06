"""Lake / catalog / storage configuration shared across the e2e examples.

This module is the single point that knows how to:

- load ``e2e/.env`` (parent env wins, mirroring docker compose's precedence);
- open a ``DuckLake`` for a given ``--catalog`` and ``--storage`` choice;
- reset the catalog + storage to a known empty state (used by the
  ``--headless`` runs so they're hermetic);
- load the locally built ``ducklake_cdc`` extension into a lake;
- retry around the SQLite "database is locked" / DuckDB ``thread::join``
  H-022 transient that surfaces on first cdc_* call against a fresh catalog.

Most of the heavy lifting was lifted from the previous
``e2e/benchmark/common.py`` so we don't reimplement S3 URL parsing,
postgres reset, etc. The differences:

- The flag surface is the new minimal set (``--catalog {duckdb,sqlite,
  postgres}``, ``--storage {disk,s3}``) rather than the benchmark's
  ``--catalog-backend`` + URL string combo.
- S3 credentials are read from ``S3_*`` env vars (the names
  ``setup-garage.sh`` writes) with a fallback to the legacy
  ``DUCKLAKE_BENCHMARK_S3_*`` names so existing local ``.env`` files keep
  working through the migration.
"""

from __future__ import annotations

import shutil
from os import environ
from pathlib import Path
from typing import Literal

from ducklake_client import (
    DiskStorage,
    DuckDBCatalog,
    DuckDBConfig,
    DuckLake,
    PostgresCatalog,
    S3Storage,
    SqliteCatalog,
)

CatalogChoice = Literal["duckdb", "sqlite", "postgres"]
StorageChoice = Literal["disk", "s3"]

# All lake-related working files live under e2e/.work/<example>/, keyed by
# the example name so two examples don't clobber each other when both run
# locally. Each example passes its name into ``open_lake``.
E2E_DIR = Path(__file__).resolve().parents[1]
WORK_ROOT = E2E_DIR / ".work"
DOTENV_PATH = E2E_DIR / ".env"

DEFAULT_POSTGRES_CATALOG = "postgresql://ducklake:ducklake@localhost:5435/ducklake"
DEFAULT_POSTGRES_ADMIN_CATALOG = "postgresql://ducklake:ducklake@localhost:5436/ducklake"

# CDC extension artifact location. ``make release`` builds it under
# ``build/release/extension/ducklake_cdc/``. Override via env if you
# build elsewhere or want to load a debug variant.
CDC_EXTENSION_ENV = "DUCKLAKE_CDC_EXTENSION"

# S3 credentials and tunables. The first name is what setup-garage.sh
# writes (canonical going forward); the second name is what the legacy
# benchmark used and is kept for back-compat while ``e2e/benchmark/`` is
# still around. New env vars on a fresh ``e2e/.env`` should use the first.
S3_ENV_KEY_PAIRS: dict[str, tuple[str, str]] = {
    "endpoint": ("S3_ENDPOINT", "DUCKLAKE_BENCHMARK_S3_ENDPOINT"),
    "region": ("S3_REGION", "DUCKLAKE_BENCHMARK_S3_REGION"),
    "access_key": ("S3_ACCESS_KEY", "DUCKLAKE_BENCHMARK_S3_KEY_ID"),
    "secret_key": ("S3_SECRET_KEY", "DUCKLAKE_BENCHMARK_S3_SECRET"),
    "bucket": ("S3_BUCKET", "DUCKLAKE_BENCHMARK_S3_BUCKET"),
    "url_style": ("S3_USE_PATH_STYLE", "DUCKLAKE_BENCHMARK_S3_URL_STYLE"),
    "use_ssl": ("S3_USE_SSL", "DUCKLAKE_BENCHMARK_S3_USE_SSL"),
}

# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------


def load_dotenv(path: Path = DOTENV_PATH) -> None:
    """Populate ``os.environ`` from ``path`` without overwriting existing keys.

    Mirrors docker compose's precedence rules: variables already present in
    the parent environment win over file values, so CI-injected secrets
    and ad-hoc shell exports always override the committed template.
    Tiny built-in parser to avoid taking ``python-dotenv`` as a runtime
    dep -- the grammar accepted is the strict subset compose itself
    documents (``KEY=value``, ``#`` line comments, optional surrounding
    quotes).
    """
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key in environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        environ[key] = value


# Auto-load on import. Idempotent: keys already present in env stay.
load_dotenv()


# ---------------------------------------------------------------------------
# CDC extension
# ---------------------------------------------------------------------------


def resolve_cdc_extension_path() -> Path:
    """Path to the locally built ducklake_cdc DuckDB extension."""
    repo_root = E2E_DIR.parent
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


def load_cdc_extension(lake: DuckLake) -> None:
    """Load the locally built ducklake_cdc artifact into ``lake``'s connection."""
    lake.connection.load_extension(str(resolve_cdc_extension_path()))


# ---------------------------------------------------------------------------
# Lake construction
# ---------------------------------------------------------------------------


def open_lake(
    *,
    example: str,
    catalog: CatalogChoice,
    storage: StorageChoice,
    allow_unsigned_extensions: bool = True,
) -> DuckLake:
    """Open a ``DuckLake`` for ``example`` against the chosen catalog and storage.

    ``allow_unsigned_extensions=True`` is the default because we always
    load a locally built ducklake_cdc artifact, which is by definition
    unsigned.

    No ``pg_pool_max_connections`` override: the legacy benchmark needed
    a 64-connection pool to support many parallel workers, but the
    examples each use a small fixed number of cursors and rely on
    long-poll CDC primitives that hold connections idle rather than
    cycling them. duckdb's default pool size is sufficient. If a future
    example legitimately needs more, override here.
    """
    duckdb_config: dict[str, bool] = {}
    if allow_unsigned_extensions:
        duckdb_config["allow_unsigned_extensions"] = True
    return DuckLake(
        catalog=_catalog_for(catalog, example=example),
        storage=_storage_for(storage, example=example),
        duckdb=DuckDBConfig(config=duckdb_config),
    )


def reset_lake(*, example: str, catalog: CatalogChoice, storage: StorageChoice) -> None:
    """Drop and recreate the catalog + clear the storage for ``example``.

    Headless runs call this so each invocation starts from a known-empty
    state. Demo mode skips it by default so you can inspect leftover
    state across local restarts.
    """
    work_dir(example).mkdir(parents=True, exist_ok=True)
    catalog_handle = _catalog_for(catalog, example=example)
    storage_handle = _storage_for(storage, example=example)
    _reset_catalog(catalog_handle)
    _reset_storage(storage_handle)


def work_dir(example: str) -> Path:
    """Per-example scratch dir under e2e/.work/, used for embedded catalogs and disk storage."""
    return WORK_ROOT / example


# Note on retry policy: the suite intentionally does NOT define its own
# retry helper. The canonical mitigation for the documented transients
# (H-022 first-bootstrap thread::join, SQLite "database is locked") lives
# in ``ducklake_cdc_client.retry.retry_on_transient`` -- import and use
# that directly at call sites that touch CDC primitives:
#
#     from ducklake_cdc_client import retry_on_transient
#     retry_on_transient(lambda: client.cdc_dml_consumer_create(...))
#
# It walks ``__cause__`` and matches on message text, so it works the
# same whether the call goes through ``CDCClient`` (DuckLakeQueryError)
# or raw ``lake.connection.execute`` (_duckdb.Error).


# ---------------------------------------------------------------------------
# private helpers
# ---------------------------------------------------------------------------


def _catalog_for(
    choice: CatalogChoice,
    *,
    example: str,
) -> DuckDBCatalog | PostgresCatalog | SqliteCatalog:
    if choice == "postgres":
        return PostgresCatalog(DEFAULT_POSTGRES_CATALOG)
    if choice == "sqlite":
        return SqliteCatalog(path=work_dir(example) / "catalog.sqlite")
    if choice == "duckdb":
        return DuckDBCatalog(path=work_dir(example) / "catalog.duckdb")
    raise ValueError(f"unsupported catalog choice: {choice!r}")


def _storage_for(choice: StorageChoice, *, example: str) -> DiskStorage | S3Storage:
    if choice == "disk":
        return DiskStorage(path=work_dir(example) / "data")
    if choice == "s3":
        return _s3_storage_from_env(example=example)
    raise ValueError(f"unsupported storage choice: {choice!r}")


def _s3_storage_from_env(*, example: str) -> S3Storage:
    """Construct ``S3Storage`` from ``S3_*`` env vars (with legacy fallback)."""
    bucket = _env("bucket")
    if not bucket:
        raise SystemExit(
            "S3 storage requested but no bucket configured. "
            "Run ./e2e/setup-garage.sh and paste the printed env block into e2e/.env, "
            "or set S3_BUCKET (and S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY) yourself."
        )
    return S3Storage(
        bucket=bucket,
        # Per-example prefix so two examples sharing one bucket don't collide.
        prefix=f"e2e/{example}/",
        endpoint=_env("endpoint"),
        region=_env("region"),
        url_style=_url_style(),
        use_ssl=_optional_bool(_env("use_ssl")),
        key_id=_env("access_key"),
        secret_access_key=_env("secret_key"),
    )


def _env(field: str) -> str | None:
    """Read an S3 field with the modern name, falling back to the legacy benchmark name."""
    new_name, legacy_name = S3_ENV_KEY_PAIRS[field]
    return environ.get(new_name) or environ.get(legacy_name)


def _url_style() -> str | None:
    """Translate S3_USE_PATH_STYLE=true (the canonical setup-garage.sh form) into DuckDB's ``path``."""
    raw = _env("url_style")
    if raw is None:
        return None
    if raw.strip().lower() in ("true", "1", "yes", "on", "path"):
        return "path"
    if raw.strip().lower() in ("false", "0", "no", "off", "vhost"):
        return "vhost"
    return raw


def _optional_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.strip().lower() in ("1", "true", "yes", "on")


def _reset_catalog(catalog: DuckDBCatalog | PostgresCatalog | SqliteCatalog) -> None:
    if isinstance(catalog, SqliteCatalog | DuckDBCatalog):
        Path(catalog.path).unlink(missing_ok=True)
        return
    _reset_postgres_database(catalog.dsn)


def _reset_storage(storage: DiskStorage | S3Storage) -> None:
    if isinstance(storage, DiskStorage):
        shutil.rmtree(Path(str(storage.path)), ignore_errors=True)
        return
    # S3 reset for headless runs is intentionally not implemented in v1: the
    # per-example prefix isolates collisions, and full delete-by-prefix
    # against Garage is a separate concern. If we end up needing it, fall
    # back to ``boto3`` here -- not pulling that dep in just for resets yet.


def _reset_postgres_database(dsn: str) -> None:
    try:
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
    except ImportError as exc:
        raise RuntimeError("Postgres reset requires the psycopg package") from exc

    # The catalog DSN routes through pgbouncer (port 5435), but DROP/CREATE
    # DATABASE can't run inside pgbouncer transaction-pooled connections.
    # Whenever we're talking to the default catalog, swap to the direct
    # postgres port for the reset session.
    reset_dsn = DEFAULT_POSTGRES_ADMIN_CATALOG if dsn == DEFAULT_POSTGRES_CATALOG else dsn

    params = {key: str(value) for key, value in conninfo_to_dict(reset_dsn).items() if value is not None}
    database = params.get("dbname")
    if not database:
        raise ValueError("Postgres catalog DSN must include a database name")
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
