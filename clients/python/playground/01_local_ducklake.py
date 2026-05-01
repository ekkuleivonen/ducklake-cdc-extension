"""Zero-infra playground for the DuckLake Python client.

Run from clients/python with:

    uv run python playground/01_local_ducklake.py
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from ducklake import DuckLake

WORK_DIR = Path(__file__).resolve().parent / ".work"


def main() -> None:
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    lake = DuckLake.open(
        catalog=str(WORK_DIR / "basic.ducklake"),
        storage=str(WORK_DIR / "basic_data"),
        duckdb_settings={
            "threads": 4,
        },
    )

    try:
        seed(lake)
        recent_events = lake.sql(
            """
            SELECT id, user_id, event_type, ts
            FROM lake.events
            WHERE ts > $cutoff
            ORDER BY ts, id
            """,
            cutoff="2025-01-01",
        ).list()
        show("events after 2025-01-01", recent_events)

        tables = lake.tables()
        print("\ntables")
        for table in tables:
            print(f"- {table.schema_name}.{table.name}")

        events = lake.table("events")
        show("events schema", [column.model_dump() for column in events.schema()])
        show("events head", events.head(5).list())
        print(f"\nevents row_count\n- {events.row_count()}")
        show("snapshots", lake.snapshots().list())
    finally:
        lake.close()


def seed(lake: DuckLake) -> None:
    lake.sql("DROP TABLE IF EXISTS lake.events").list()
    lake.sql(
        """
        CREATE TABLE lake.events (
            id INTEGER,
            user_id INTEGER,
            event_type VARCHAR,
            ts TIMESTAMP
        )
        """
    ).list()

    rows = [
        (1, 42, "signup", "2024-12-31 23:59:00"),
        (2, 42, "page_view", "2025-01-01 09:00:00"),
        (3, 7, "purchase", "2025-01-02 12:30:00"),
    ]
    for row in rows:
        lake.sql(
            """
            INSERT INTO lake.events VALUES ($id, $user_id, $event_type, $ts)
            """,
            id=row[0],
            user_id=row[1],
            event_type=row[2],
            ts=row[3],
        ).list()


def show(title: str, rows: list[dict[str, Any]]) -> None:
    print(f"\n{title}")
    if not rows:
        print("- <empty>")
        return
    for row in rows:
        print(f"- {row}")


if __name__ == "__main__":
    main()
