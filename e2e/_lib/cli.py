"""Shared CLI surface for every example.

Every ``e2e/NN_xxx/app.py`` accepts the same minimal flag set:

    --headless                no TUI; periodic stderr summary instead
    --catalog {duckdb,sqlite,postgres}
    --storage {disk,s3}       local disk (default) or Garage S3
    --duration <seconds>      hard stop (CI safety net for --headless)
    --json-summary            final machine-readable summary for CI
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

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
    json_summary: bool


def make_parser(
    *,
    description: str,
    supported_catalogs: Iterable[CatalogChoice] = ("postgres", "sqlite", "duckdb"),
    supported_storages: Iterable[StorageChoice] = ("disk", "s3"),
    catalog_default: CatalogChoice | None = None,
) -> argparse.ArgumentParser:
    """Build an ``ArgumentParser`` with the suite-standard flags.

    ``supported_catalogs`` / ``supported_storages`` let an example
    advertise (and enforce) which backends it actually handles. Per the
    e2e suite docs, not every example supports every catalog.

    ``catalog_default`` overrides the implicit default for ``--catalog``.
    When omitted, the default remains ``postgres`` if it appears in
    ``supported_catalogs``, otherwise the first supported backend — so
    single-catalog demos still parse without extra flags.
    """
    catalog_choices = tuple(supported_catalogs)
    if catalog_default is None:
        resolved_catalog_default: CatalogChoice = (
            "postgres" if "postgres" in catalog_choices else catalog_choices[0]
        )
    else:
        if catalog_default not in catalog_choices:
            raise ValueError(
                f"catalog_default {catalog_default!r} must be one of {catalog_choices}"
            )
        resolved_catalog_default = catalog_default

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--headless",
        action="store_true",
        help="No TUI. Periodic stderr summary instead.",
    )
    parser.add_argument(
        "--catalog",
        choices=catalog_choices,
        default=resolved_catalog_default,
        help=f"Catalog backend. Default: {resolved_catalog_default}.",
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
    parser.add_argument(
        "--json-summary",
        action="store_true",
        help="Emit one final machine-readable JSON summary to stdout.",
    )
    return parser


def parse_common(args: argparse.Namespace) -> CommonArgs:
    """Pluck the common fields out of an argparse Namespace into the typed dataclass."""
    return CommonArgs(
        headless=bool(args.headless),
        catalog=args.catalog,
        storage=args.storage,
        duration_s=int(args.duration) if args.duration is not None else None,
        json_summary=bool(args.json_summary),
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


def emit_json_summary(common: CommonArgs, payload: dict[str, Any]) -> None:
    """Write a final machine-readable summary when requested."""
    if common.json_summary:
        print(json.dumps(payload, sort_keys=True), file=sys.stdout, flush=True)
