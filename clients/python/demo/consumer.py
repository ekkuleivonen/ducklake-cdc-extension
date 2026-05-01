"""Consumer demo placeholder that runs cdc_doctor against a local DuckLake."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from ducklake import DuckLake
from ducklake_cdc import CDCClient

WORK_DIR = Path(__file__).resolve().parent / ".work"
DEFAULT_CDC_EXTENSION = (
    Path(__file__).resolve().parents[3]
    / "build"
    / "release"
    / "extension"
    / "ducklake_cdc"
    / "ducklake_cdc.duckdb_extension"
)


def main() -> None:
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    lake = DuckLake.open(
        catalog=str(WORK_DIR / "demo.ducklake"),
        storage=str(WORK_DIR / "demo_data"),
        duckdb_config={"allow_unsigned_extensions": "true"},
    )

    try:
        cdc = CDCClient(lake)
        cdc.load_extension(path=_local_extension_path())
        diagnostics = cdc.doctor()
        if not diagnostics:
            print("cdc_doctor returned no diagnostics")
            return
        for diagnostic in diagnostics:
            print(f"{diagnostic.severity}: {diagnostic.code} - {diagnostic.message}")
    finally:
        lake.close()


def _local_extension_path() -> Path:
    configured = os.environ.get("DUCKLAKE_CDC_EXTENSION")
    path = Path(configured).expanduser() if configured else DEFAULT_CDC_EXTENSION
    if not path.exists():
        raise SystemExit(
            "Local ducklake_cdc extension not found. Build it with `make release` "
            "or set DUCKLAKE_CDC_EXTENSION=/path/to/ducklake_cdc.duckdb_extension."
        )
    return path


if __name__ == "__main__":
    main()
