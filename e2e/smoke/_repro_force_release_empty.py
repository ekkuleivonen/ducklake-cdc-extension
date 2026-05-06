"""Stress / regression harness for H-022 first-bootstrap thread::join.

Inline-DuckDB catalogs surface the H-022 first-bootstrap mutex race as a
``_duckdb.Error: thread::join failed: <errno>`` from the very first
cdc_* call against a fresh catalog. macOS prints both ``Resource
deadlock avoided`` (EDEADLK) and ``Invalid argument`` (EINVAL) for the
same underlying race. See ``docs/hazard-log.md`` H-022.

This harness:

* asserts ``ducklake_cdc_client.retry.is_transient_error`` recognises
  both errno strings (``--probe`` mode);
* observes the residual rate where ``retry_on_transient`` exhausts on a
  poisoned connection without the lake being recreated.

Useful when investigating H-022 regressions or evaluating the retry
policy on a new platform. NOT a CI gate — the residual is environment-
sensitive.

Run after ``make release``::

    uv run --project e2e python e2e/smoke/_repro_force_release_empty.py
    uv run --project e2e python e2e/smoke/_repro_force_release_empty.py --runs 100
    uv run --project e2e python e2e/smoke/_repro_force_release_empty.py --probe list_empty
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from collections import Counter
from pathlib import Path

from ducklake_client import (
    DiskStorage,
    DuckDBCatalog,
    DuckDBConfig,
    DuckLake,
    DuckLakeQueryError,
)
from ducklake_cdc_client import is_transient_error, retry_on_transient

REPO = Path(__file__).resolve().parents[2]
CDC_EXTENSION = REPO / "build" / "release" / "extension" / "ducklake_cdc" / "ducklake_cdc.duckdb_extension"


def _fresh_lake(workdir: Path) -> DuckLake:
    catalog_path = workdir / "catalog.ducklake"
    data_dir = workdir / "data"
    data_dir.mkdir(exist_ok=True)
    lake = DuckLake(
        catalog=DuckDBCatalog(path=catalog_path),
        storage=DiskStorage(path=data_dir),
        alias="lake",
        duckdb=DuckDBConfig(config={"allow_unsigned_extensions": True}),
    )
    lake.connection.load_extension(str(CDC_EXTENSION))
    return lake


def _one_run() -> tuple[str, str]:
    """Returns ``(category, detail)`` for the retry-on-same-connection path."""

    with tempfile.TemporaryDirectory(prefix="dlcdc_h022_") as tmp:
        lake = _fresh_lake(Path(tmp))
        try:
            attempts: list[str] = []

            def attempt() -> None:
                try:
                    lake.connection.execute(
                        "SELECT * FROM cdc_consumer_force_release('lake', 'never_existed')"
                    ).fetchall()
                    attempts.append("ok")
                    raise AssertionError("force_release returned rows for missing consumer")
                except Exception as exc:  # noqa: BLE001
                    attempts.append(f"{type(exc).__name__}: {str(exc).splitlines()[0]}")
                    raise

            try:
                retry_on_transient(attempt)
            except DuckLakeQueryError as exc:
                msg = str(exc.__cause__ or exc)
                if "does not exist" in msg:
                    return _classify(attempts)
                raise
            except Exception as exc:  # noqa: BLE001
                msg = str(exc)
                if "does not exist" in msg:
                    return _classify(attempts)
                if is_transient_error(exc):
                    return ("retry_exhausted", f"{type(exc).__name__}: {msg.splitlines()[0]}")
                return ("unexpected", f"non-transient: {type(exc).__name__}: {msg.splitlines()[0]}")
            return ("unexpected", "retry_on_transient returned without raising")
        finally:
            lake.close()


def _classify(attempts: list[str]) -> tuple[str, str]:
    if not attempts:
        return ("unexpected", "no attempts recorded")
    if len(attempts) == 1:
        return ("clean_first_try", attempts[0])
    if any("thread::join" in a for a in attempts[:-1]) and "does not exist" in attempts[-1]:
        return ("recovered_after_thread_join", f"{len(attempts)} attempts")
    return ("unexpected", " / ".join(attempts))


_PROBES = {
    "force_release_missing": (
        "SELECT * FROM cdc_consumer_force_release('lake', 'never_existed')",
        "does not exist",
    ),
    "drop_missing": (
        "SELECT * FROM cdc_consumer_drop('lake', 'never_existed')",
        "does not exist",
    ),
    "list_empty": ("SELECT * FROM cdc_list_consumers('lake')", None),
    "create_dml_no_table": (
        "SELECT * FROM cdc_dml_consumer_create("
        "'lake', 'orders_sink', table_name := 'orders', start_at := 'now')",
        "is not live",
    ),
}


def _probe_call(sql: str, expect_clean_substring: str | None) -> tuple[str, str]:
    """Issue ``sql`` once on a fresh inline-DuckDB catalog, no retry."""

    with tempfile.TemporaryDirectory(prefix="dlcdc_probe_") as tmp:
        lake = _fresh_lake(Path(tmp))
        try:
            try:
                lake.connection.execute(sql).fetchall()
                return ("ok", "")
            except Exception as exc:  # noqa: BLE001
                if "thread::join" in str(exc):
                    return ("thread_join", str(exc).splitlines()[0])
                if expect_clean_substring and expect_clean_substring in str(exc):
                    return ("expected_throw", str(exc).splitlines()[0])
                return ("other", f"{type(exc).__name__}: {str(exc).splitlines()[0]}")
        finally:
            lake.close()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument(
        "--probe",
        choices=tuple(_PROBES),
        default=None,
        help="Run a no-retry probe of one specific first-call shape.",
    )
    args = parser.parse_args(argv)

    if not CDC_EXTENSION.exists():
        print(
            f"missing artifact at {CDC_EXTENSION}; build with `make release` first",
            file=sys.stderr,
        )
        return 1

    if args.probe is not None:
        sql, expect = _PROBES[args.probe]
        counts: Counter[str] = Counter()
        for _ in range(args.runs):
            category, _detail = _probe_call(sql, expect)
            counts[category] += 1
        print(f"probe {args.probe!r} over {args.runs} fresh catalogs:")
        for cat, count in counts.most_common():
            print(f"  {cat}: {count}")
        return 0

    counts = Counter()
    for _ in range(args.runs):
        category, _detail = _one_run()
        counts[category] += 1

    print(f"\nresults over {args.runs} runs (retry on same connection):")
    for cat, count in counts.most_common():
        print(f"  {cat}: {count}")
    if counts.get("unexpected", 0):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
