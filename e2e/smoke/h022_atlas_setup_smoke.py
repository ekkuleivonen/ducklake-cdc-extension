"""Reproduce Atlas-shaped CDC setup on a populated Postgres DuckLake catalog.

Atlas historically created ``CDCClient`` on its general catalogue connection,
read the latest snapshot, then passed that client into ``DMLConsumer``.  That
keeps consumer setup on the same outer connection that just touched DuckLake
metadata and can surface H-022's outer/inner connection lock handoff.

This smoke runs that shared-connection shape and the high-level client's normal
derived-connection shape side by side.  It requires the e2e Postgres services:

    docker compose -f e2e/docker-compose.yml up -d --wait postgres pgbouncer
    uv run --project e2e python e2e/smoke/h022_atlas_setup_smoke.py --attempts 50
"""

from __future__ import annotations

import argparse
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SMOKE_DIR = Path(__file__).resolve().parent
E2E_DIR = SMOKE_DIR.parent
if str(E2E_DIR) not in sys.path:
    sys.path.insert(0, str(E2E_DIR))

from ducklake_cdc_client import CDCClient, DMLConsumer  # noqa: E402

from _lib.config import load_cdc_extension, open_lake, reset_lake  # noqa: E402

EXAMPLE = "h022_atlas_setup"
TABLE = "main.crawls"
CONSUMER = "atlas-crawl-materialization-planner"


def _prepare(*, history: int, bootstrap_cdc: bool) -> None:
    reset_lake(example=EXAMPLE, catalog="postgres", storage="disk")
    with open_lake(example=EXAMPLE, catalog="postgres", storage="disk") as lake:
        load_cdc_extension(lake)
        lake.connection.execute(
            "CREATE TABLE lake.main.crawls(crawl_id UUID, captured_at TIMESTAMPTZ)"
        )
        for index in range(history):
            lake.connection.execute(
                "INSERT INTO lake.main.crawls VALUES (uuid(), now())"
            )
            if index % 5 == 0:
                lake.connection.execute(
                    f"CREATE TABLE lake.main.history_{index}(id INTEGER)"
                )
        if bootstrap_cdc:
            # Commit a single-threaded first bootstrap. The default stress
            # phase is about Atlas's setup against an already-populated CDC
            # catalog, not racing initial schema creation. Pass
            # --first-bootstrap-race to isolate the remaining core handoff.
            client = CDCClient(lake, install_extension=False)
            client.cdc_dml_consumer_create(
                "h022-bootstrap",
                table_name=TABLE,
                start_at="now",
            )
            client.cdc_consumer_drop("h022-bootstrap")


def _attempt(*, shared_connection: bool, attempt: int) -> None:
    with open_lake(example=EXAMPLE, catalog="postgres", storage="disk") as lake:
        load_cdc_extension(lake)
        # Atlas obtains start_at on the catalogue connection before setup.
        start_at = lake.snapshots.latest()
        if start_at is None:
            raise RuntimeError("DuckLake has no snapshot")

        consumer_name = f"{CONSUMER}-{attempt}"
        if shared_connection:
            client = CDCClient(lake, install_extension=False)
            consumer = DMLConsumer(
                lake,
                consumer_name,
                table=TABLE,
                mode="changes",
                start_at=start_at,
                on_exists="use",
                client=client,
            ).open()
        else:
            # No explicit client: DMLConsumer derives and prewarms a dedicated
            # cursor, separating CDC table functions from catalogue reads.
            consumer = DMLConsumer(
                lake,
                consumer_name,
                table=TABLE,
                mode="changes",
                start_at=start_at,
                on_exists="use",
            ).open()

        try:
            consumer.window(max_snapshots=1)
            consumer.connection.execute(
                "SELECT * FROM cdc_consumer_release('lake', ?)", [consumer_name]
            ).fetchall()
        finally:
            consumer.close()


def _run_shape(*, attempts: int, workers: int, shared_connection: bool) -> int:
    failures = 0
    label = "shared" if shared_connection else "derived"
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _attempt,
                shared_connection=shared_connection,
                attempt=attempt,
            ): attempt
            for attempt in range(1, attempts + 1)
        }
        for future in as_completed(futures):
            attempt = futures[future]
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001 - this is a reproducer
                failures += 1
                chain: list[str] = []
                current: BaseException | None = exc
                while current is not None:
                    chain.append(f"{type(current).__name__}: {current}")
                    current = current.__cause__
                print(f"{label} attempt {attempt}: {' <- '.join(chain)}", flush=True)
    print(f"{label}: {attempts - failures}/{attempts} succeeded", flush=True)
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempts", type=int, default=50)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--history", type=int, default=50)
    parser.add_argument("--shape", choices=("shared", "derived", "both"), default="both")
    parser.add_argument("--first-bootstrap-race", action="store_true")
    args = parser.parse_args()
    if args.attempts < 1:
        parser.error("--attempts must be >= 1")

    if args.workers < 1:
        parser.error("--workers must be >= 1")
    if args.history < 0:
        parser.error("--history must be >= 0")

    _prepare(history=args.history, bootstrap_cdc=not args.first_bootstrap_race)
    try:
        shared_failures = 0
        derived_failures = 0
        if args.shape in ("shared", "both"):
            shared_failures = _run_shape(
                attempts=args.attempts,
                workers=args.workers,
                shared_connection=True,
            )
        if args.shape in ("derived", "both"):
            derived_failures = _run_shape(
                attempts=args.attempts,
                workers=args.workers,
                shared_connection=False,
            )
    finally:
        shutil.rmtree(E2E_DIR / ".work" / EXAMPLE, ignore_errors=True)

    if args.shape == "both" and shared_failures and not derived_failures:
        print("H-022 reproduced only on Atlas's shared catalogue connection shape")
    return 0 if derived_failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
