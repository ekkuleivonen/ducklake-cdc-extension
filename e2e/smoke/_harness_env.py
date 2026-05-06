"""Paths and helpers shared by C++ multiconn / TOCTOU / interrupt smoke launchers."""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BUILD = os.environ.get("DUCKLAKE_CDC_BUILD", "release")
DUCKDB_INCLUDE = REPO / "duckdb" / "src" / "include"
LIBDUCKDB_DIR = REPO / "build" / BUILD / "src"
LIBDUCKDB = LIBDUCKDB_DIR / ("libduckdb.dylib" if sys.platform == "darwin" else "libduckdb.so")
CDC_EXTENSION = REPO / "build" / BUILD / "extension" / "ducklake_cdc" / "ducklake_cdc.duckdb_extension"
DUCKDB_CLI = REPO / "build" / BUILD / "duckdb"


def platform_dir() -> str:
    """Match ``~/.duckdb/extensions/<version>/<platform>/`` used by ``make prepare_tests``."""

    os_name = "osx" if sys.platform == "darwin" else "linux"
    machine = os.uname().machine
    arch = "amd64" if machine == "x86_64" else ("arm64" if machine == "aarch64" else machine)
    return f"{os_name}_{arch}"


def pinned_duckdb_version() -> str:
    return (REPO / ".github" / "duckdb-version").read_text().strip()


# Prebuilt DuckLake binary from ``make prepare_tests`` (see smoke docstrings).
DUCKLAKE_EXTENSION = (
    Path(os.environ.get("HOME", "~")).expanduser()
    / ".duckdb"
    / "extensions"
    / pinned_duckdb_version()
    / platform_dir()
    / "ducklake.duckdb_extension"
)
