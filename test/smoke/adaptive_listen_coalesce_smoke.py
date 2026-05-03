"""Smoke test for adaptive coalescing in integrated DML listen.

This exercises the production API shape: a producer emits many tiny commits
while a consumer repeatedly calls `cdc_dml_changes_listen` and commits the
returned window. After the first couple of quick, small windows, the extension
should reactively wait a few milliseconds after work is visible so it can claim
a larger visible window without taxing the first event after idle.

Usage:

    uv run --project clients/python python test/smoke/adaptive_listen_coalesce_smoke.py
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[2]
BUILD = os.environ.get("DUCKLAKE_CDC_BUILD", "release")
CDC_EXTENSION = REPO / "build" / BUILD / "extension" / "ducklake_cdc" / "ducklake_cdc.duckdb_extension"


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def open_conn(lake: Path, data: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    conn.execute("LOAD parquet")
    conn.execute(f"LOAD {quote(str(CDC_EXTENSION))}")
    conn.execute(f"ATTACH 'ducklake:{str(lake)}' AS lake (DATA_PATH {quote(str(data))})")
    return conn


def listen_window(conn: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    rows = conn.execute(
        """
        SELECT start_snapshot, end_snapshot, count(*)::BIGINT AS rows
        FROM cdc_dml_changes_listen(
          'lake',
          'adaptive',
          timeout_ms := 1000,
          max_snapshots := 100
        )
        GROUP BY start_snapshot, end_snapshot
        """
    ).fetchall()
    if not rows:
        return (-1, -1, 0)
    start, end, row_count = rows[0]
    conn.execute("SELECT * FROM cdc_commit('lake', 'adaptive', ?)", [end])
    return (int(start), int(end), int(row_count))


def main() -> int:
    if not CDC_EXTENSION.exists():
        raise SystemExit(f"missing {CDC_EXTENSION}; run `make {BUILD}` first")

    with tempfile.TemporaryDirectory(prefix="ducklake_cdc_adaptive_") as tmp:
        tmpdir = Path(tmp)
        lake = tmpdir / "adaptive.ducklake"
        data = tmpdir / "adaptive_data"

        setup = open_conn(lake, data)
        setup.execute("CREATE TABLE lake.events(id INTEGER, payload VARCHAR)")
        setup.execute("SELECT * FROM cdc_dml_consumer_create('lake', 'adaptive', table_name := 'events')")
        setup.execute("INSERT INTO lake.events VALUES (0, 'seed')")

        consumer = setup.cursor()
        first = listen_window(consumer)
        if first[2] != 1:
            raise AssertionError(f"expected one seed row in first listen, got {first}")

        producer = setup.cursor()
        done = threading.Event()

        def produce() -> None:
            for idx in range(1, 40):
                producer.execute("INSERT INTO lake.events VALUES (?, ?)", [idx, f"p{idx}"])
                time.sleep(0.01)
            done.set()

        thread = threading.Thread(target=produce, daemon=True)
        thread.start()

        windows: list[tuple[int, int, int]] = []
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            window = listen_window(consumer)
            if window[2] > 0:
                windows.append(window)
            if done.is_set() and sum(row_count for _, _, row_count in windows) >= 39:
                break

        thread.join(timeout=2.0)
        spans = [end - start + 1 for start, end, row_count in windows if row_count > 0]
        late_spans = spans[2:]
        print(f"adaptive listen windows={windows} spans={spans}")
        if not late_spans or max(late_spans) < 5:
            raise AssertionError(
                "expected adaptive listen coalescing to claim a multi-snapshot late window; "
                f"windows={windows}, spans={spans}"
            )

    print("adaptive_listen_coalesce_smoke PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
