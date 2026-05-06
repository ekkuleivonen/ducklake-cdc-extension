"""One-shot row-count inspector for the 01_pipeline_dag lake.

Used by ``test_restart.sh`` to verify exactly-once semantics across a
process restart: opens the lake without resetting it, prints row
counts for every table in the pipeline, and exits. Output is one
``key=value`` per line so the shell test can parse it with
``grep | cut`` and assert against expected values.

Not part of the example's runtime path -- this is a test/inspection
tool. ``app.py`` doesn't import it.
"""

from __future__ import annotations

import sys
from pathlib import Path

_E2E_ROOT = Path(__file__).resolve().parent.parent
if str(_E2E_ROOT) not in sys.path:
    sys.path.insert(0, str(_E2E_ROOT))

from _lib.config import load_cdc_extension, open_lake  # noqa: E402

EXAMPLE = "01_pipeline_dag"

TABLES = (
    "raw_purchases",
    "raw_refunds",
    "clean_purchases",
    "clean_refunds",
    "joined_orders",
)


def main() -> int:
    lake = open_lake(example=EXAMPLE, catalog="postgres", storage="disk")
    # ``load_cdc_extension`` already issues the H-022 pre-warm
    # (``SELECT cdc_version()``) on ``lake.connection`` so subsequent
    # cdc_* table-function calls -- including ``cdc_consumer_stats``
    # below -- don't hit the first-call mutex race.
    load_cdc_extension(lake)
    conn = lake.connection
    try:
        # Distinct keys for joined_orders so the kill+restart test can
        # assert there are no duplicates -- exactly-once means every
        # purchase ID appears at most once in joined_orders.
        for table in TABLES:
            count = conn.execute(f"SELECT count(*) FROM lake.{table}").fetchone()[0]
            print(f"{table}={count}")
        distinct_purchases_in_joined = conn.execute(
            "SELECT count(DISTINCT purchase_id) FROM lake.joined_orders"
        ).fetchone()[0]
        print(f"joined_orders_distinct_purchase_ids={distinct_purchases_in_joined}")

        # Cursor positions per consumer, so the test can confirm they
        # advanced across restarts. ``cdc_consumer_stats`` is the
        # supported observability surface (per docs/api.md); reading
        # the underlying ``__ducklake_cdc.consumers`` table directly
        # would couple this test to internal layout.
        #
        # Use a derived cursor: the parent ``conn`` just ran several
        # metadata-touching COUNTs on lake tables, and the H-022
        # surface is sensitive to "metadata read on lake followed by
        # cdc_* table function on the same connection." A fresh
        # cursor sidesteps that without us having to re-pre-warm
        # mid-script. Same trick the lib's ``DMLConsumer`` uses for
        # its lease connection.
        stats_cursor = lake.connection.cursor()
        try:
            cursor_rows = stats_cursor.execute(
                "SELECT consumer_name, last_committed_snapshot "
                "FROM cdc_consumer_stats('lake') "
                "WHERE consumer_name IN ('normalize_purchases', 'normalize_refunds', 'join_orders') "
                "ORDER BY consumer_name"
            ).fetchall()
        finally:
            stats_cursor.close()
        for name, last_snap in cursor_rows:
            print(f"cursor_{name}={last_snap}")
    finally:
        try:
            lake.close()
        except Exception:  # noqa: BLE001
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
