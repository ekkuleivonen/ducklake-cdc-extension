"""Thin TUI helpers shared across examples.

The visualization choices stay in each example's own ``app.py`` (per
``e2e/README.md``: bespoke per example). This module just provides the
plumbing every example needs to behave consistently between
demo-with-TUI and ``--headless`` modes:

- ``LiveDisplay``: a context manager that wraps ``rich.live.Live`` in TUI
  mode and degrades to a tiny periodic stderr logger in headless mode.
  Examples build their own renderable (``Layout`` / ``Panel`` / table /
  whatever) and hand it in; this class hides the headless toggle.
- ``log``: stderr logging that is always safe to call from any thread.
  Use this in headless mode for the periodic progress lines.

The point is that ``app.py`` should not contain any ``if headless:``
TUI gating beyond passing a single flag into ``LiveDisplay``.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.live import Live

# Stderr console so live output never collides with anything an example
# might want to write to stdout (e.g. a final result-path line).
_stderr_console = Console(stderr=True, highlight=False)
_log_lock = threading.Lock()


def log(message: str, *, level: str = "info") -> None:
    """Thread-safe timestamped stderr log line. Same shape in TUI and headless modes."""
    ts = datetime.now().strftime("%H:%M:%S")
    with _log_lock:
        _stderr_console.print(f"[dim]{ts}[/dim] [{level}] {message}")


class LiveDisplay(AbstractContextManager):
    """Render a rich renderable in TUI mode; emit periodic stderr lines in headless mode.

    Usage:

        layout = build_my_layout(...)

        with LiveDisplay(headless=args.headless,
                         renderable_factory=lambda: build_my_layout(...),
                         headless_summary=lambda: short_progress_string(...)):
            run_workload()

    The TUI path uses ``rich.live.Live`` with a 4 Hz refresh; the
    headless path emits ``headless_summary`` to stderr every 5 seconds.
    Either way the body of the ``with`` block runs the workload; nothing
    in the example needs to know which mode it's in.
    """

    def __init__(
        self,
        *,
        headless: bool,
        renderable_factory: Callable[[], Any],
        headless_summary: Callable[[], str] | None = None,
        refresh_per_second: float = 4.0,
        headless_log_period_s: float = 5.0,
    ) -> None:
        self._headless = headless
        self._renderable_factory = renderable_factory
        self._headless_summary = headless_summary or (lambda: "running")
        self._refresh_per_second = refresh_per_second
        self._headless_log_period_s = headless_log_period_s
        self._live: Live | None = None
        self._stop_event = threading.Event()
        self._headless_thread: threading.Thread | None = None
        self._tui_thread: threading.Thread | None = None

    def __enter__(self) -> LiveDisplay:
        if self._headless:
            self._headless_thread = threading.Thread(
                target=self._headless_loop, name="live-headless-log", daemon=True
            )
            self._headless_thread.start()
        else:
            self._live = Live(
                self._renderable_factory(),
                console=_stderr_console,
                refresh_per_second=self._refresh_per_second,
                screen=True,
                transient=False,
            )
            self._live.__enter__()
            self._tui_thread = threading.Thread(
                target=self._tui_refresh_loop, name="live-tui-refresh", daemon=True
            )
            self._tui_thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stop_event.set()
        if self._tui_thread is not None:
            self._tui_thread.join(timeout=2.0)
        if self._live is not None:
            self._live.__exit__(exc_type, exc, tb)
        if self._headless_thread is not None:
            self._headless_thread.join(timeout=2.0)

    def _tui_refresh_loop(self) -> None:
        """Re-render the TUI on a fixed cadence so stage panels reflect the live counters."""
        period = 1.0 / max(self._refresh_per_second, 0.1)
        while not self._stop_event.is_set():
            assert self._live is not None
            try:
                self._live.update(self._renderable_factory(), refresh=True)
            except Exception:  # noqa: BLE001 - never let the renderer kill the workload
                pass
            self._stop_event.wait(period)

    def _headless_loop(self) -> None:
        while not self._stop_event.wait(self._headless_log_period_s):
            try:
                log(self._headless_summary())
            except Exception as exc:  # noqa: BLE001 - same: don't crash the workload
                log(f"headless-log error: {exc!r}", level="warn")


def is_tty() -> bool:
    """True when stdout is attached to a terminal (rough proxy for 'has a TUI')."""
    return sys.stdout.isatty()
