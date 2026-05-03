"""Smoke test for the `CDC_INCOMPATIBLE_CATALOG` compatibility surfaces.

Exists because SQLLogicTest can't easily assert against a notice's *text* —
the structured-error contract in `docs/errors.md` makes the message the
load-bearing surface for clients, so we need a test that pins down the
exact wording. The complementary SQL test `test/sql/compat_check.test`
covers the silent paths (no DuckLake attached, compatible DuckLake
attached); this script covers the warning/error text path.

What this script does (no Docker, no PyPI deps; all stdlib):

1. Builds a fresh DuckDB database file inside a temp directory and seeds
   it with a `__ducklake_metadata_<dbname>` schema whose
   `ducklake_metadata` row says `('version', '99.99')`. This is **not** a
   real DuckLake catalog — DuckLake itself rejects unsupported
   versions on `ATTACH`, so we can't tamper with a real catalog and have
   the smoke survive — but it is byte-identical (from the compat probe's
   perspective) to a DuckLake catalog written by some hypothetical future
   DuckLake release that stamps `'99.99'`. That isomorphism is what
   makes this test meaningful: the probe's discovery query (`schema
   exists, then SELECT value FROM ducklake_metadata WHERE key='version'`)
   does not distinguish a fake from a real catalog.
2. Spawns a fresh subprocess of the **locally-built duckdb CLI**
   (`build/release/duckdb` by default, override with
   `DUCKLAKE_CDC_BUILD=debug`) with the seeded file as the *initial*
   attached database. The CLI's static-link of `ducklake_cdc` triggers
   `LoadInternal` at session start, after the initial database is
   visible. The probe then hits the seeded `__ducklake_metadata_<name>`
   schema, reads `'99.99'`, and emits the structured notice on stderr.
3. Calls `cdc_dml_consumer_create('<catalog>', 'test', table_name := ...)`
   against the same seeded incompatible catalog and asserts the call-time
   gate throws the same structured prefix before any catalog write.
4. Asserts both outputs contain the `CDC_INCOMPATIBLE_CATALOG:` prefix
   (the load-bearing surface bindings parse on per `docs/errors.md`)
   AND every required field per the same doc:
   - the catalog name we attached
   - the observed version string verbatim
   - the supported-set in `{...}` form
   - the `ducklake_cdc ` build-stamp prefix (the version segment is
     either the release tag at HEAD or the git short SHA)
   - a pointer to `docs/development.md`

Why this is a Python smoke probe:

SQLLogicTest can cover the silent compatibility paths, but it cannot easily
assert the LOAD-time notice text that clients parse. CI runs this probe
after the release build so the structured notice/error contract stays pinned.

Usage:

    uv run python test/smoke/compat_warning_smoke.py

Defaults to the release build, in line with the other smoke probes and the
CI integration-smoke job. Override with `DUCKLAKE_CDC_BUILD=debug` if you
have a local debug build.

Exit codes:

    0 = the seeded incompatible catalog produced the expected LOAD-time
        notice and call-time exception, with all required fields present
    1 = the notice was missing or malformed; stderr is dumped to stdout
        for human inspection and the script returns the discrepancy

Preconditions:

The locally-built duckdb CLI must exist at `build/${DUCKLAKE_CDC_BUILD}/duckdb`
(default `build/release/duckdb`). Run `make release` first if not.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD = os.environ.get("DUCKLAKE_CDC_BUILD", "release")
DUCKDB_CLI = REPO_ROOT / "build" / BUILD / "duckdb"

# Catalog format version we'll fake. Chosen to be wildly outside any
# plausible real DuckLake version so it's obvious this is a test value
# if it ever leaks into a real bug report.
FAKE_VERSION = "99.99"

# The expected error code prefix. `docs/errors.md` is the single source
# of truth; this is the parser-stable identifier bindings rely on.
EXPECTED_PREFIX = "CDC_INCOMPATIBLE_CATALOG:"

# Build-stamp prefix. The version segment after the prefix is whatever
# `EXT_VERSION_DUCKLAKE_CDC` resolves to at build time (a git tag if the
# build is tagged, otherwise a short SHA — see
# `extension-ci-tools/scripts/configure_helper.py` and
# `docs/decisions/0013-versioning-and-release-automation.md`). The prefix
# is the only part the structured notice contract guarantees, so this
# smoke pins only the prefix.
EXPECTED_VERSION_STAMP = "ducklake_cdc "

# Pointer back to the matrix the notice tells operators to consult.
# Hard-coded here so a refactor of compat_check.cpp that drops the
# pointer is caught loudly.
EXPECTED_DOC_POINTER = "docs/development.md"

# Supported catalog-format set rendered by compat_check.cpp.
EXPECTED_SUPPORTED_SET = "{0.4, 1.0}"


def seed_fake_catalog(db_path: Path, catalog_name: str) -> None:
    """Write a fresh DuckDB file containing a `__ducklake_metadata_<name>`
    schema with a `ducklake_metadata` row of `('version', FAKE_VERSION)`.

    This is the exact shape `compat_check.cpp`'s discovery query
    (`SELECT ... FROM duckdb_schemas() WHERE schema_name = '__ducklake_metadata_' || database_name`)
    looks for.
    """
    seed_sql = (
        f"CREATE SCHEMA __ducklake_metadata_{catalog_name};"
        f"CREATE TABLE __ducklake_metadata_{catalog_name}.ducklake_metadata (key VARCHAR, value VARCHAR);"
        f"INSERT INTO __ducklake_metadata_{catalog_name}.ducklake_metadata VALUES ('version', '{FAKE_VERSION}');"
    )
    result = subprocess.run(
        [str(DUCKDB_CLI), "-unsigned", str(db_path), "-c", seed_sql],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        sys.stderr.write("seed step failed:\n" f"  stdout: {result.stdout}\n" f"  stderr: {result.stderr}\n")
        sys.exit(1)


def capture_load_notice(db_path: Path) -> str:
    """Open the seeded DuckDB file and capture stderr from session start.

    The locally-built debug duckdb CLI statically links `ducklake_cdc`,
    so opening directly against `db_path` runs `LoadInternal` after
    `db_path` is visible as the initial database. That's the only
    timing window in which the LOAD-time probe can observe a non-system
    attachment in this build configuration; users who LOAD the extension
    after attaching see the same effect at LOAD time when the extension
    is not statically linked (the production case).
    """
    result = subprocess.run(
        [str(DUCKDB_CLI), "-unsigned", str(db_path), "-c", "SELECT 1;"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        sys.stderr.write("smoke run exited non-zero:\n" f"  stdout: {result.stdout}\n" f"  stderr: {result.stderr}\n")
        sys.exit(1)
    return result.stderr


def capture_call_time_error(db_path: Path, catalog_name: str) -> str:
    """Run the first catalog-touching cdc_* function against the seeded
    incompatible catalog and return stderr. The process must fail.
    """
    result = subprocess.run(
        [
            str(DUCKDB_CLI),
            "-unsigned",
            str(db_path),
            "-c",
            f"SELECT * FROM cdc_dml_consumer_create('{catalog_name}', 'test', table_name := 'probe');",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        sys.stderr.write(
            "call-time smoke unexpectedly succeeded:\n" f"  stdout: {result.stdout}\n" f"  stderr: {result.stderr}\n"
        )
        sys.exit(1)
    return result.stderr


def assert_required_fields(catalog_name: str, notice: str) -> list[str]:
    """Return a list of human-readable field-presence problems. Empty list
    means every required field per `docs/errors.md` is in the notice.
    """
    problems: list[str] = []
    if EXPECTED_PREFIX not in notice:
        problems.append(f"missing structured prefix '{EXPECTED_PREFIX}'")
    if f"'{catalog_name}'" not in notice:
        problems.append(f"catalog name {catalog_name!r} missing or unquoted")
    if f"'{FAKE_VERSION}'" not in notice:
        problems.append(f"observed version {FAKE_VERSION!r} missing or unquoted")
    if EXPECTED_SUPPORTED_SET not in notice:
        problems.append(f"supported set '{EXPECTED_SUPPORTED_SET}' missing")
    if EXPECTED_VERSION_STAMP not in notice:
        problems.append(f"build-stamp prefix {EXPECTED_VERSION_STAMP!r} missing")
    if EXPECTED_DOC_POINTER not in notice:
        problems.append(f"doc pointer {EXPECTED_DOC_POINTER!r} missing")
    return problems


def main() -> int:
    if not DUCKDB_CLI.exists():
        sys.stderr.write(f"locally-built duckdb CLI not found at {DUCKDB_CLI}; run `make {BUILD}` first.\n")
        return 1

    catalog_name = "smokelake"
    with tempfile.TemporaryDirectory(prefix="dlcdc_compat_smoke_") as tmp:
        # The DB filename has to match the catalog name because DuckDB
        # auto-derives the attached-database name from the bare filename
        # when you open it directly (`./duckdb path/to/<name>.db` →
        # database_name = '<name>').
        db_path = Path(tmp) / f"{catalog_name}.db"
        seed_fake_catalog(db_path, catalog_name)
        notice = capture_load_notice(db_path)
        call_error = capture_call_time_error(db_path, catalog_name)

    problems = [f"LOAD-time notice: {p}" for p in assert_required_fields(catalog_name, notice)]
    problems.extend(f"call-time exception: {p}" for p in assert_required_fields(catalog_name, call_error))
    if problems:
        sys.stderr.write("compat_warning_smoke FAILED — expected compatibility text not produced.\n")
        sys.stderr.write("--- captured LOAD-time stderr ---\n")
        sys.stderr.write(notice if notice else "(empty)\n")
        sys.stderr.write("--- captured call-time stderr ---\n")
        sys.stderr.write(call_error if call_error else "(empty)\n")
        sys.stderr.write("--- problems ---\n")
        for p in problems:
            sys.stderr.write(f"  - {p}\n")
        return 1

    print("compat_warning_smoke PASSED")
    print()
    print("Captured CDC_INCOMPATIBLE_CATALOG LOAD-time notice:")
    print()
    # Strip any leading whitespace/newlines for a clean copy-paste.
    print(notice.strip())
    print()
    print("Captured CDC_INCOMPATIBLE_CATALOG call-time exception:")
    print()
    print(call_error.strip())
    return 0


if __name__ == "__main__":
    sys.exit(main())
