"""Live dashboard sink for the demo, designed for screen-recorded GIFs.

This module ships two pieces:

- :class:`DemoDashboard` — the renderer. It owns the terminal between
  :meth:`start` and :meth:`stop`, switches to the alternate screen
  buffer on start (so scrollback isn't polluted) and prints a single
  one-line summary back into the original buffer on stop. A background
  thread redraws at a fixed FPS so frames never jump while recording
  with ``vhs`` or ``asciinema``.
- :class:`DemoSink` — a thin :class:`BaseDMLSink` that forwards each
  batch to a shared dashboard. Multiple consumers attach their own
  ``DemoSink`` instances to the same dashboard so the demo can render a
  unified view across every per-table consumer.

The dashboard is observability-only (``require_ack=False``). Render
failures never gate delivery, so a misbehaving terminal can't stall the
CDC pipeline.

Layout — fixed height so frames stay still while recording::

    ducklake-cdc · live                                          00:00:23
    ──────────────────────────────────────────────────────────────────────
      2,341 ch/s    ·    p50 1.8 ms / p99 4.2 ms    ·    12,438 changes

      events_01     ████████████████░░░░    +1,210  ~340  −38     1.8 ms
      events_02     ████████░░░░░░░░░░░░      +843  ~210  −22     2.1 ms
      events_03     ███████████░░░░░░░░░      +987  ~190  −41     2.3 ms
      events_04     ██████░░░░░░░░░░░░░░      +621  ~110  −19     1.9 ms

    ─── stream ──────────────────────────────────────────────────────────
      + events_01   id   1247   …events_01.1247        2.1 ms
      + events_02   id   1248   …events_02.1248        1.8 ms
      ~ events_01   id     42   updated                3.4 ms
      ...

Bars in the per-table panel are proportional to each table's *current*
rate (1 s window), not its lifetime share — this makes the bars animate
visibly even at sustained throughput. New tail lines render in inverse
for ~180 ms before settling, so the eye locks onto the motion.
"""

from __future__ import annotations

import atexit
import contextlib
import os
import shutil
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType
from typing import IO, Any

from ducklake_cdc import BaseDMLSink, DMLBatch, SinkContext


@contextlib.contextmanager
def _suppress_io_errors():
    try:
        yield
    except (OSError, ValueError):
        pass

# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------

RESET = "\x1b[0m"
DIM = "\x1b[2m"
BOLD = "\x1b[1m"
INVERSE = "\x1b[7m"
GREEN = "\x1b[32m"
YELLOW = "\x1b[33m"
RED = "\x1b[31m"
GREY = "\x1b[90m"
HIDE_CURSOR = "\x1b[?25l"
SHOW_CURSOR = "\x1b[?25h"
ALT_SCREEN_ON = "\x1b[?1049h"
ALT_SCREEN_OFF = "\x1b[?1049l"
CURSOR_HOME = "\x1b[H"
CLEAR_BELOW = "\x1b[J"
CLEAR_LINE = "\x1b[2K"
CLEAR_SCREEN = "\x1b[2J"

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

DEFAULT_FPS = 24
DEFAULT_TAIL_SIZE = 10
DEFAULT_MAX_TABLES = 4
# The bar in the per-table panel and the payload column in the tail are
# both sized adaptively on each frame — see ``_compute_layout`` — so the
# two panels share the same right edge regardless of terminal width. The
# bounds keep narrow terminals readable and prevent absurdly long bars on
# very wide ones (where extra width adds no information).
MIN_BAR_WIDTH = 12
MAX_BAR_WIDTH = 50
MIN_PAYLOAD_WIDTH = 18
MAX_PAYLOAD_WIDTH = 60
DEFAULT_RATE_WINDOW_S = 1.0
DEFAULT_LATENCY_RESERVOIR = 2048
FLASH_DURATION_S = 0.18
LATENCY_EWMA_ALPHA = 0.2
NANOS_PER_MS = 1_000_000.0
# Fixed-width cells in each panel line (2-space pad + content widths +
# inter-cell 2-space gaps + 2-space right pad). The remaining width is
# split between the bar and payload columns.
_TABLE_LINE_FIXED = 2 + 14 + 2 + 0 + 2 + 22 + 2 + 8 + 2  # = 54 + bar
_TAIL_LINE_FIXED = 2 + 3 + 2 + 10 + 2 + 9 + 2 + 0 + 2 + 8 + 2  # = 42 + payload


# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------


@dataclass
class _TableState:
    name_short: str
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    recent_event_ns: deque[int] = field(default_factory=lambda: deque(maxlen=8192))
    latency_ema_ms: float | None = None

    @property
    def total(self) -> int:
        return self.inserts + self.updates + self.deletes

    def record(self, marker: str, latency_ms: float | None, now_ns: int) -> None:
        if marker == "+":
            self.inserts += 1
        elif marker == "~":
            self.updates += 1
        else:
            self.deletes += 1
        self.recent_event_ns.append(now_ns)
        if latency_ms is not None:
            if self.latency_ema_ms is None:
                self.latency_ema_ms = latency_ms
            else:
                self.latency_ema_ms = (
                    (1.0 - LATENCY_EWMA_ALPHA) * self.latency_ema_ms
                    + LATENCY_EWMA_ALPHA * latency_ms
                )

    def rate_per_s(self, now_ns: int, window_ns: int) -> float:
        if not self.recent_event_ns:
            return 0.0
        threshold = now_ns - window_ns
        count = 0
        for ts in reversed(self.recent_event_ns):
            if ts < threshold:
                break
            count += 1
        return count * 1e9 / window_ns


@dataclass
class _TailEvent:
    marker: str
    table_short: str
    row_id: str
    payload: str
    latency_ms: float | None
    arrived_ns: int


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _kind_marker(kind: object) -> str:
    """Map a :class:`ChangeType` to a single-char display marker."""
    name = getattr(kind, "value", str(kind)).lower()
    if name.startswith("update"):
        return "~"
    if name == "delete":
        return "−"
    return "+"


def _table_short(qualified: str | None) -> str:
    if qualified is None:
        return "?"
    if "." in qualified:
        return qualified.rsplit(".", 1)[-1]
    return qualified


def _truncate(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def _fmt_count(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 10_000:
        return f"{n / 1_000:.1f}k"
    return f"{n:,}"


def _fmt_latency(ms: float | None) -> str:
    if ms is None:
        return "  --   "
    if ms < 10:
        return f"{ms:5.2f} ms"
    if ms < 1000:
        return f"{ms:5.1f} ms"
    return f"{ms / 1000:5.1f} s "


def _fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _plural(n: int, singular: str) -> str:
    return f"{n} {singular}" if n == 1 else f"{n} {singular}s"


def _color_latency(ms: float | None, formatted: str) -> str:
    if ms is None:
        return f"{DIM}{formatted}{RESET}"
    if ms < 5.0:
        return f"{GREEN}{formatted}{RESET}"
    if ms < 25.0:
        return f"{YELLOW}{formatted}{RESET}"
    return f"{RED}{formatted}{RESET}"


def _marker_color(marker: str) -> str:
    if marker == "+":
        return GREEN
    if marker == "~":
        return YELLOW
    return RED


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


class DemoDashboard:
    """Shared renderer driving a fixed-height live dashboard.

    Lifecycle: :meth:`start` switches the terminal to the alt screen and
    spawns the render thread; :meth:`stop` joins the thread, restores
    the terminal, and prints a single-line summary into the original
    buffer. Both methods are idempotent.

    Concurrency: :meth:`record_batch` is safe to call from any thread.
    All mutable state is guarded by a single lock; the render loop takes
    a snapshot under the lock and then writes to the stream outside it.

    Non-TTY streams: :meth:`start` is a no-op and :meth:`record_batch`
    only updates counters. The summary is still printed on :meth:`stop`.
    """

    def __init__(
        self,
        *,
        stream: IO[str] | None = None,
        fps: int = DEFAULT_FPS,
        tail_size: int = DEFAULT_TAIL_SIZE,
        max_tables: int = DEFAULT_MAX_TABLES,
        min_bar_width: int = MIN_BAR_WIDTH,
        max_bar_width: int = MAX_BAR_WIDTH,
        log_path: Path | None = None,
    ) -> None:
        self._stream = stream if stream is not None else sys.stdout
        self._fps = max(1, fps)
        self._tail_size = max(1, tail_size)
        self._max_tables = max(1, max_tables)
        self._min_bar_width = max(4, min_bar_width)
        self._max_bar_width = max(self._min_bar_width, max_bar_width)
        self._tty = bool(getattr(self._stream, "isatty", lambda: False)())
        # Captured stdout/stderr from C++ extension and Python landing in fd
        # 1/2 would corrupt the alt-screen layout. We dup2 both fds onto a
        # log file while the dashboard owns the screen, and write our frames
        # via a saved dup of the original stdout fd that doesn't go through
        # fd 1.
        self._log_path = log_path
        self._log_fh: IO[str] | None = None
        self._saved_stdout_fd: int | None = None
        self._saved_stderr_fd: int | None = None
        self._terminal_fd: int | None = None

        self._lock = threading.Lock()
        self._tables: dict[str, _TableState] = {}
        self._consumers: set[str] = set()
        self._tail: deque[_TailEvent] = deque(maxlen=self._tail_size)
        self._latencies: deque[float] = deque(maxlen=DEFAULT_LATENCY_RESERVOIR)
        self._total_changes = 0
        self._started_ns: int | None = None

        self._stop_event = threading.Event()
        self._render_thread: threading.Thread | None = None
        self._started = False
        self._stopped = False
        # ``_suspended`` is the signal-handler hand-off: setting it makes the
        # render loop drop frames immediately so an eager terminal restore
        # from the signal handler cannot race with one more in-flight frame.
        self._suspended = False
        self._previous_signal_handlers: dict[int, Any] = {}
        self._atexit_registered = False

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._started_ns = time.monotonic_ns()
        if not self._tty:
            return

        # Save a dup of the original stdout fd so we can keep writing frames
        # directly to the terminal even after we redirect fd 1/2 to the log.
        try:
            self._terminal_fd = os.dup(self._stream.fileno())
        except (AttributeError, OSError, ValueError):
            self._terminal_fd = None

        if self._terminal_fd is not None and self._log_path is not None:
            try:
                self._log_path.parent.mkdir(parents=True, exist_ok=True)
                self._log_fh = self._log_path.open("a", encoding="utf-8")
                # Mark the boundary so re-running consumers leave readable history.
                self._log_fh.write(
                    f"\n--- demo dashboard started {time.strftime('%Y-%m-%dT%H:%M:%S')}\n"
                )
                self._log_fh.flush()
                # Flush any pending Python buffers before swapping fds.
                with _suppress_io_errors():
                    sys.stdout.flush()
                with _suppress_io_errors():
                    sys.stderr.flush()
                log_fd = self._log_fh.fileno()
                self._saved_stdout_fd = os.dup(1)
                self._saved_stderr_fd = os.dup(2)
                os.dup2(log_fd, 1)
                os.dup2(log_fd, 2)
            except OSError:
                # Best effort — if redirection fails, fall back to whatever
                # dup state we have. Frames still go to ``terminal_fd``.
                self._close_log()

        os.write(
            self._terminal_fd if self._terminal_fd is not None else 1,
            (ALT_SCREEN_ON + HIDE_CURSOR + CLEAR_SCREEN + CURSOR_HOME).encode("utf-8"),
        )
        self._render_thread = threading.Thread(
            target=self._render_loop,
            name="demo-dashboard-render",
            daemon=True,
        )
        self._render_thread.start()

        # Install signal handlers AFTER the alt screen is up so a Ctrl+C
        # during startup doesn't run the dashboard handler before we have a
        # terminal_fd to restore. ``signal.signal`` returns whatever handler
        # was previously installed — typically :class:`CDCApp`'s flag-only
        # handler, since :meth:`start` is called from inside ``with app:``.
        # Our handler restores the terminal eagerly, then chains to the
        # previous handler so the existing drain logic still runs.
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    previous = signal.signal(sig, self._handle_signal)
                except (OSError, ValueError):
                    continue
                self._previous_signal_handlers[sig] = previous

        # Belt-and-suspenders: even if the process is torn down without
        # going through stop() (unhandled exception, os._exit), atexit
        # still gives us one last shot to leave the user's terminal sane.
        if not self._atexit_registered:
            atexit.register(self._atexit_restore)
            self._atexit_registered = True

    def stop(self) -> None:
        if self._stopped or not self._started:
            self._stopped = True
            return
        self._stopped = True
        self._suspended = True
        self._stop_event.set()
        if self._render_thread is not None:
            self._render_thread.join(timeout=2.0)
        # Restore signal handlers before any further IO so a Ctrl+C
        # received during shutdown falls through to whatever was
        # installed before us (typically default Python behaviour by
        # this point — :class:`CDCApp` has already restored too).
        self._restore_signal_handlers()
        if self._tty and self._terminal_fd is not None:
            os.write(
                self._terminal_fd, (SHOW_CURSOR + ALT_SCREEN_OFF).encode("utf-8")
            )
        # Restore fd 1/2 before printing the summary so the user sees it
        # in their normal terminal scrollback.
        if self._saved_stdout_fd is not None:
            try:
                os.dup2(self._saved_stdout_fd, 1)
                os.close(self._saved_stdout_fd)
            except OSError:
                pass
            self._saved_stdout_fd = None
        if self._saved_stderr_fd is not None:
            try:
                os.dup2(self._saved_stderr_fd, 2)
                os.close(self._saved_stderr_fd)
            except OSError:
                pass
            self._saved_stderr_fd = None
        if self._terminal_fd is not None:
            try:
                os.close(self._terminal_fd)
            except OSError:
                pass
            self._terminal_fd = None
        self._close_log()
        summary = self._final_summary()
        if summary:
            self._stream.write(summary + "\n")
            self._stream.flush()
        if self._log_path is not None:
            self._stream.write(
                f"demo dashboard: stdout/stderr captured to {self._log_path}\n"
            )
            self._stream.flush()

    def _close_log(self) -> None:
        if self._log_fh is not None:
            try:
                self._log_fh.flush()
            except OSError:
                pass
            try:
                self._log_fh.close()
            except OSError:
                pass
            self._log_fh = None

    def _restore_signal_handlers(self) -> None:
        for sig, previous in self._previous_signal_handlers.items():
            with _suppress_io_errors():
                signal.signal(sig, previous)
        self._previous_signal_handlers.clear()

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        """Tear down the alt screen *immediately*, then chain.

        The point of running this in the signal handler — rather than
        waiting for ``stop()`` to be called from the ``finally`` block —
        is responsiveness. Without this, the user sees the dashboard
        frozen in alt-screen mode for up to ``CDCApp.shutdown_timeout``
        seconds while workers drain, which feels exactly like "the
        dashboard ate my Ctrl+C".

        We only do the *minimum* tear-down here: stop emitting frames,
        write the alt-screen-off sequence, restore fd 1/2. The render
        thread is joined and the log file is closed by the regular
        ``stop()`` call later in cleanup.
        """
        # Race-free hand-off: drop any in-flight frame on the floor.
        self._suspended = True
        self._stop_event.set()
        self._eager_terminal_restore()

        previous = self._previous_signal_handlers.get(signum)
        if callable(previous):
            previous(signum, frame)
        elif previous in (signal.SIG_DFL, None):
            # Nothing was chained ahead of us — fall back to default
            # behaviour by re-raising the signal once our terminal is
            # already sane.
            with _suppress_io_errors():
                signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)

    def _eager_terminal_restore(self) -> None:
        """Restore the user's terminal *now*, even if stop() hasn't run.

        Idempotent. Safe to call from a signal handler or from atexit.
        """
        if self._terminal_fd is not None:
            with _suppress_io_errors():
                os.write(
                    self._terminal_fd,
                    (SHOW_CURSOR + ALT_SCREEN_OFF).encode("utf-8"),
                )
        if self._saved_stdout_fd is not None:
            with _suppress_io_errors():
                os.dup2(self._saved_stdout_fd, 1)
        if self._saved_stderr_fd is not None:
            with _suppress_io_errors():
                os.dup2(self._saved_stderr_fd, 2)

    def _atexit_restore(self) -> None:
        """Last-ditch terminal restore. Called by ``atexit``."""
        if not self._stopped and self._started:
            self._eager_terminal_restore()

    # -- ingest -------------------------------------------------------------

    def record_batch(self, batch: DMLBatch) -> None:
        now_ns = time.monotonic_ns()
        with self._lock:
            self._consumers.add(batch.consumer_name)
            for change in batch:
                marker = _kind_marker(change.kind)
                table_full = change.table or "(unknown)"
                table_short = _table_short(table_full)

                values = change.values or {}
                produced_ns = values.get("produced_ns")
                if isinstance(produced_ns, (int, float)):
                    latency_ms = (now_ns - int(produced_ns)) / NANOS_PER_MS
                    if latency_ms < 0:
                        latency_ms = 0.0
                else:
                    latency_ms = None

                state = self._tables.get(table_full)
                if state is None:
                    state = _TableState(name_short=table_short)
                    self._tables[table_full] = state
                state.record(marker, latency_ms, now_ns)

                kind_str = getattr(change.kind, "value", str(change.kind)).lower()
                if kind_str != "update_preimage":
                    payload_raw = values.get("payload")
                    payload = str(payload_raw) if payload_raw is not None else ""
                    rid = values.get("id")
                    self._tail.append(
                        _TailEvent(
                            marker=marker,
                            table_short=table_short,
                            row_id=str(rid) if rid is not None else "",
                            payload=payload,
                            latency_ms=latency_ms,
                            arrived_ns=now_ns,
                        )
                    )

                if latency_ms is not None:
                    self._latencies.append(latency_ms)
                self._total_changes += 1

    # -- render loop --------------------------------------------------------

    def _render_loop(self) -> None:
        period = 1.0 / self._fps
        while not self._stop_event.wait(period):
            if self._suspended:
                continue
            try:
                self._render_frame()
            except Exception:
                # Never let a render glitch crash the consumer process.
                pass

    def _compute_layout(self, cols: int) -> tuple[int, int]:
        """Choose ``(bar_w, payload_w)`` so both panels fill the terminal.

        The per-table line and the tail line have different fixed cells
        but we want their right edges (the latency column) to land at
        the same column. That requires ``bar_w`` and ``payload_w`` to
        differ by the difference of the fixed-cell widths — see
        ``_TABLE_LINE_FIXED`` / ``_TAIL_LINE_FIXED`` above.

        ``min``/``max`` bounds keep narrow terminals readable and stop
        very wide terminals from rendering absurdly long bars (where the
        extra width adds no information — the bar is a relative scale,
        not a magnitude readout).
        """
        bar_w = cols - _TABLE_LINE_FIXED
        bar_w = max(self._min_bar_width, min(self._max_bar_width, bar_w))
        # Right-edge alignment: 54 + bar = 42 + payload  ⇒  payload = bar + 12.
        payload_w = bar_w + (_TABLE_LINE_FIXED - _TAIL_LINE_FIXED)
        payload_w = max(MIN_PAYLOAD_WIDTH, min(MAX_PAYLOAD_WIDTH, payload_w))
        return bar_w, payload_w

    def _render_frame(self) -> None:
        cols, _ = shutil.get_terminal_size((100, 30))
        cols = max(60, cols)
        bar_w, payload_w = self._compute_layout(cols)
        # Rules span the same content width as the data lines so the
        # dashboard feels cohesive when bar/payload hit their caps on
        # very wide terminals (otherwise the rules would extend past
        # the data right edge into a dead zone).
        rule_w = bar_w + (_TABLE_LINE_FIXED - 4)

        with self._lock:
            now_ns = time.monotonic_ns()
            elapsed_s = (now_ns - (self._started_ns or now_ns)) / 1e9
            total = self._total_changes
            window_ns = int(DEFAULT_RATE_WINDOW_S * 1e9)
            scored = [
                (full, state, state.rate_per_s(now_ns, window_ns))
                for full, state in self._tables.items()
            ]
            tail_snapshot = list(self._tail)
            p50, p99 = self._quantiles_locked()
            table_count = len(self._tables)
            consumer_count = len(self._consumers)

        scored.sort(key=lambda t: (-t[2], -t[1].total, t[0]))
        visible = scored[: self._max_tables]
        max_rate = max((rate for _, _, rate in visible), default=0.0)
        overall_rate = sum(rate for _, _, rate in scored)

        lines: list[str] = []
        lines.append(self._title_line(rule_w, elapsed_s, table_count, consumer_count))
        lines.append(self._top_rule(rule_w))
        lines.append(self._stats_line(overall_rate, p50, p99, total))
        lines.append("")
        for _, state, rate in visible:
            lines.append(self._table_line(state, rate, max_rate, bar_w))
        for _ in range(self._max_tables - len(visible)):
            lines.append("")
        lines.append("")
        lines.append(self._stream_separator(rule_w))
        for ev in tail_snapshot:
            lines.append(self._tail_line(ev, now_ns, payload_w))
        for _ in range(self._tail_size - len(tail_snapshot)):
            lines.append("")

        chunks = [CURSOR_HOME]
        for line in lines:
            chunks.append(CLEAR_LINE + line + "\n")
        chunks.append(CLEAR_BELOW)
        frame = "".join(chunks)
        if self._terminal_fd is not None:
            try:
                os.write(self._terminal_fd, frame.encode("utf-8"))
            except OSError:
                pass
        else:
            self._stream.write(frame)
            self._stream.flush()

    # -- line builders ------------------------------------------------------

    def _title_line(
        self,
        content_w: int,
        elapsed_s: float,
        table_count: int,
        consumer_count: int,
    ) -> str:
        clock = _fmt_duration(elapsed_s)
        scope = f" · {_plural(table_count, 'table')} · {_plural(consumer_count, 'consumer')}"
        plain_left = f"ducklake-cdc · live{scope}"
        gap = max(2, content_w - len(plain_left) - len(clock))
        title = f"{BOLD}ducklake-cdc{RESET}{DIM} · live{RESET}"
        scope_disp = f"{DIM}{scope}{RESET}"
        clock_disp = f"{DIM}{clock}{RESET}"
        return "  " + title + scope_disp + " " * gap + clock_disp

    def _top_rule(self, content_w: int) -> str:
        return f"  {DIM}{'─' * max(0, content_w)}{RESET}"

    def _stats_line(
        self, rate: float, p50: float | None, p99: float | None, total: int
    ) -> str:
        rate_disp = (
            f"{BOLD}{_fmt_count(int(rate)):>6}{RESET}{DIM} ch/s{RESET}"
        )
        lat_disp = (
            f"{DIM}p50{RESET} {_color_latency(p50, _fmt_latency(p50))}"
            f" {DIM}/{RESET} "
            f"{DIM}p99{RESET} {_color_latency(p99, _fmt_latency(p99))}"
        )
        total_disp = f"{BOLD}{_fmt_count(total)}{RESET}{DIM} changes total{RESET}"
        sep = f"   {DIM}·{RESET}   "
        return "  " + rate_disp + sep + lat_disp + sep + total_disp

    def _table_line(
        self, state: _TableState, rate: float, max_rate: float, bar_w: int
    ) -> str:
        name_w = 14
        if max_rate > 0:
            filled = int(round(rate / max_rate * bar_w))
        else:
            filled = 0
        filled = max(0, min(bar_w, filled))
        bar = (
            f"{GREEN}{'█' * filled}{RESET}"
            f"{DIM}{'░' * (bar_w - filled)}{RESET}"
        )
        name = _truncate(state.name_short, name_w).ljust(name_w)
        counts = (
            f"{GREEN}+{state.inserts:>6,}{RESET}  "
            f"{YELLOW}~{state.updates:>5,}{RESET}  "
            f"{RED}−{state.deletes:>5,}{RESET}"
        )
        lat_text = _fmt_latency(state.latency_ema_ms)
        lat = _color_latency(state.latency_ema_ms, lat_text)
        return f"  {BOLD}{name}{RESET}  {bar}  {counts}  {lat}"

    def _stream_separator(self, content_w: int) -> str:
        label = " stream "
        rule = "─" * max(0, content_w - len(label) - 3)
        return f"  {DIM}───{label}{rule}{RESET}"

    def _tail_line(self, ev: _TailEvent, now_ns: int, payload_w: int) -> str:
        flashing = (now_ns - ev.arrived_ns) / 1e9 < FLASH_DURATION_S
        color = _marker_color(ev.marker)
        if flashing:
            marker_disp = f"{INVERSE}{BOLD}{color} {ev.marker} {RESET}"
            table_disp = f"{BOLD}{ev.table_short.ljust(10)[:10]}{RESET}"
        else:
            marker_disp = f" {BOLD}{color}{ev.marker}{RESET} "
            table_disp = ev.table_short.ljust(10)[:10]

        rid_str = ev.row_id if ev.row_id else "-"
        id_disp = f"{DIM}id{RESET} {rid_str:>6}"

        payload = _truncate(ev.payload, payload_w).ljust(payload_w)
        payload_disp = f"{DIM}{payload}{RESET}"

        lat_disp = _color_latency(ev.latency_ms, _fmt_latency(ev.latency_ms))
        return f"  {marker_disp}  {table_disp}  {id_disp}  {payload_disp}  {lat_disp}"

    # -- summaries / quantiles ---------------------------------------------

    def _quantiles_locked(self) -> tuple[float | None, float | None]:
        n = len(self._latencies)
        if n == 0:
            return None, None
        data = sorted(self._latencies)
        p50_idx = max(0, min(n - 1, int(n * 0.50)))
        p99_idx = max(0, min(n - 1, int(n * 0.99)))
        return data[p50_idx], data[p99_idx]

    def _final_summary(self) -> str:
        with self._lock:
            total = self._total_changes
            tables = len(self._tables)
            consumers = len(self._consumers)
            now_ns = time.monotonic_ns()
            elapsed_s = (
                (now_ns - self._started_ns) / 1e9
                if self._started_ns is not None
                else 0.0
            )
            p50, p99 = self._quantiles_locked()
        avg_rate = total / elapsed_s if elapsed_s > 0 else 0.0
        return (
            f"demo dashboard: streamed {total:,} changes "
            f"from {_plural(tables, 'table')} via {_plural(consumers, 'consumer')} · "
            f"avg {avg_rate:,.0f} ch/s · "
            f"p50 {_fmt_latency(p50).strip()} · "
            f"p99 {_fmt_latency(p99).strip()}"
        )


# ---------------------------------------------------------------------------
# Sink wrapper
# ---------------------------------------------------------------------------


class DemoSink(BaseDMLSink):
    """Forward each DML batch to a shared :class:`DemoDashboard`.

    Multiple consumers can each attach their own ``DemoSink`` bound to
    the same dashboard so the demo renders a unified view across every
    per-table consumer. ``require_ack=False`` so dashboard hiccups never
    gate delivery.
    """

    name = "demo_dashboard"
    require_ack = False

    def __init__(self, dashboard: DemoDashboard) -> None:
        self._dashboard = dashboard

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        del ctx
        self._dashboard.record_batch(batch)


__all__ = ["DemoDashboard", "DemoSink"]
