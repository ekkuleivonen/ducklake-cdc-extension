"""Shared CLI surface for every example.

Every ``e2e/NN_xxx/app.py`` accepts the same minimal flag set:

    --headless                no TUI; periodic stderr summary instead
    --catalog {duckdb,sqlite,postgres}
    --storage {disk,s3}       local disk (default) or Garage S3
    --duration <seconds>      hard stop (CI safety net for --headless)
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import dataclass

from _lib.config import CatalogChoice, StorageChoice

# Default per-headless-run cap. The suite README sets a 3-min budget for
# any single example to fit a CI matrix sweep; default the safety-net
# timeout a touch above that so a healthy run finishes naturally rather
# than tripping the timeout.
DEFAULT_HEADLESS_DURATION_S = 180


@dataclass(frozen=True)
class CommonArgs:
    """The args every example shares. Subclassed by examples that need more.

    ``duration_s=None`` means "no caller-imposed cap" -- i.e. demo mode
    runs until Ctrl-C, headless mode runs until the safety-net default
    (``DEFAULT_HEADLESS_DURATION_S``). Examples can resolve this via
    :func:`effective_duration_s` to keep the precedence rules in one
    place.
    """
    headless: bool
    catalog: CatalogChoice
    storage: StorageChoice
    duration_s: int | None


def make_parser(
    *,
    description: str,
    supported_catalogs: Iterable[CatalogChoice] = ("postgres", "sqlite", "duckdb"),
    supported_storages: Iterable[StorageChoice] = ("disk", "s3"),
) -> argparse.ArgumentParser:
    """Build an ``ArgumentParser`` with the suite-standard flags.

    ``supported_catalogs`` / ``supported_storages`` let an example
    advertise (and enforce) which backends it actually handles. Per the
    catalog matrix in ``e2e/README.md``, not every example supports
    every catalog -- e.g. the publisher examples can fall back to
    ``duckdb`` only with ``--in-process``.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--headless",
        action="store_true",
        help="No TUI. Periodic stderr summary instead.",
    )
    parser.add_argument(
        "--catalog",
        choices=tuple(supported_catalogs),
        default="postgres",
        help="Catalog backend. Default: postgres (always supported).",
    )
    parser.add_argument(
        "--storage",
        choices=tuple(supported_storages),
        default="disk",
        help="Object storage backend. Default: disk. s3 requires ./e2e/setup-garage.sh + creds in e2e/.env.",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help=(
            "Run for N seconds, then stop and write metrics. Honored in both "
            "demo and headless modes. If omitted: demo mode runs until Ctrl-C, "
            f"headless mode runs until the {DEFAULT_HEADLESS_DURATION_S}s safety-net cap."
        ),
    )
    return parser


def parse_common(args: argparse.Namespace) -> CommonArgs:
    """Pluck the common fields out of an argparse Namespace into the typed dataclass."""
    return CommonArgs(
        headless=bool(args.headless),
        catalog=args.catalog,
        storage=args.storage,
        duration_s=int(args.duration) if args.duration is not None else None,
    )


def effective_duration_s(common: CommonArgs) -> float | None:
    """Resolve the wait timeout the example should pass to ``stop.wait(...)``.

    Precedence (the same in every example):

    - ``--duration N`` always wins (returned as ``float(N)``).
    - Otherwise, headless mode falls back to the safety-net cap
      (so a misbehaving CI run can't burn budget forever).
    - Otherwise, demo mode returns ``None`` (block until Ctrl-C).
    """
    if common.duration_s is not None:
        return float(common.duration_s)
    if common.headless:
        return float(DEFAULT_HEADLESS_DURATION_S)
    return None
