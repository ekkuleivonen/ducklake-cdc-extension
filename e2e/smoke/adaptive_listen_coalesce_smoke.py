"""Smoke test for adaptive coalescing in integrated DML listen.

This exercises the production API shape: a producer emits many tiny commits
while a consumer repeatedly calls `cdc_dml_changes_listen` and commits the
returned window. After the first couple of quick, small windows, the extension
should reactively wait a few milliseconds after work is visible so it can claim
a larger visible window without taxing the first event after idle.

Uses :mod:`ducklake_client` for attach + ``lake.table.create``, and
:class:`ducklake_cdc_client.CDCClient` for CDC calls.

Usage:

    uv run python e2e/smoke/adaptive_listen_coalesce_smoke.py
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from pathlib import Path

from ducklake_cdc_client import CDCClient
from ducklake_client import ColumnDef, DiskStorage, DuckDBCatalog, DuckDBConfig, DuckLake

REPO = Path(__file__).resolve().parents[2]
BUILD = os.environ.get("DUCKLAKE_CDC_BUILD", "release")
CDC_EXTENSION = REPO / "build" / BUILD / "extension" / "ducklake_cdc" / "ducklake_cdc.duckdb_extension"


def open_lake(lake_path: Path, data_path: Path) -> DuckLake:
    return DuckLake(
        catalog=DuckDBCatalog(path=lake_path),
        storage=DiskStorage(path=data_path),
        alias="lake",
        duckdb=DuckDBConfig(config={"allow_unsigned_extensions": True}),
    )


def listen_window(client: CDCClient, consumer_name: str = "adaptive") -> tuple[int, int, int]:
    rows = client.cdc_dml_changes_listen(
        consumer_name,
        timeout_ms=1000,
        max_snapshots=100,
    )
    if not rows:
        return (-1, -1, 0)
    r0 = rows[0]
    start = int(r0.start_snapshot if r0.start_snapshot is not None else r0.snapshot_id)
    end = int(r0.end_snapshot if r0.end_snapshot is not None else r0.snapshot_id)
    client.cdc_commit(consumer_name, end)
    return (start, end, len(rows))


def main() -> int:
    if not CDC_EXTENSION.exists():
        raise SystemExit(f"missing {CDC_EXTENSION}; run `make {BUILD}` first")

    with tempfile.TemporaryDirectory(prefix="ducklake_cdc_adaptive_") as tmp:
        tmpdir = Path(tmp)
        lake_path = tmpdir / "adaptive.ducklake"
        data_path = tmpdir / "adaptive_data"

        lake = open_lake(lake_path, data_path)
        try:
            lake.connection.load_extension(str(CDC_EXTENSION))
            lake.table.create(
                "events",
                id=ColumnDef("INTEGER"),
                payload=ColumnDef("VARCHAR"),
            )
            client = CDCClient(lake, install_extension=False)
            client.cdc_dml_consumer_create("adaptive", table_name="events")
            lake.connection.execute("INSERT INTO lake.events VALUES (0, 'seed')")

            first = listen_window(client)
            if first[2] != 1:
                raise AssertionError(f"expected one seed row in first listen, got {first}")

            producer = lake.connection.cursor()
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
                window = listen_window(client)
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
        finally:
            lake.close()

    print("adaptive_listen_coalesce_smoke PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
