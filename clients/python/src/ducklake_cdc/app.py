"""Multi-consumer runtime host for the high-level CDC client.

:class:`CDCApp` runs N consumers concurrently in one process with shared
lifecycle and signal handling. Use it whenever you have more than one
consumer; a single consumer's own :meth:`DMLConsumer.run` is sufficient
otherwise.

The headline shape::

    with CDCApp(consumers=[c1, c2]) as app:
        app.run(infinite=True)

Per the design draft, consumer-level concurrency is an internal detail.
v1 is threaded — one OS thread per consumer — because the underlying
``cdc_*_changes_listen`` calls block in C++ and threads are the path of
least resistance until that changes. The threading model is intentionally
not exposed on the public API.

Signal handling (``SIGINT`` / ``SIGTERM``) is auto-installed on the main
thread so a ``Ctrl+C`` or container shutdown drains the in-flight batch
and returns from :meth:`CDCApp.run` cleanly. Tests opt out via
``install_signals=False``.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Iterable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from types import FrameType, TracebackType
from typing import Any, Self

from ducklake_cdc.consumer import DDLConsumer, DMLConsumer

_LOG = logging.getLogger(__name__)

#: A consumer the app can host. The split between DML and DDL is preserved
#: at the type level; the app does not care which is which.
Consumer = DMLConsumer | DDLConsumer

DEFAULT_SHUTDOWN_TIMEOUT = 30.0
DEFAULT_LISTEN_TIMEOUT_MS = 1_000
DEFAULT_MAX_SNAPSHOTS = 100


@dataclass(frozen=True)
class ConsumerHealth:
    """Per-consumer health snapshot returned by :meth:`CDCApp.stats`.

    Concrete enough for a "this scales" dashboard without committing to
    a wire format. Field meanings:

    - ``name`` / ``kind`` — consumer identity and class (``DMLConsumer`` or
      ``DDLConsumer``).
    - ``running`` — whether the worker thread is currently alive.
    - ``started_at`` — when the worker first started (UTC). ``None`` if it
      has not been launched yet (e.g. registered but the app has not been
      ``run`` yet).
    - ``finished_at`` — when the worker thread returned (UTC), success or
      failure. ``None`` while still running.
    - ``delivered_batches`` — total non-empty batches delivered so far. The
      value updates after each commit returns.
    - ``last_error`` — the type name of the last unhandled exception raised
      from the consumer's run loop, or ``None``.
    """

    name: str
    kind: str
    running: bool
    started_at: datetime | None
    finished_at: datetime | None
    delivered_batches: int
    last_error: str | None


class CDCApp:
    """Multi-consumer runtime host.

    Hosts a list of :class:`DMLConsumer` and/or :class:`DDLConsumer`
    instances and runs them concurrently as long as the app is alive. The
    app:

    - opens each consumer's sinks on ``__enter__`` (delegating to the
      consumer's own context manager);
    - launches one worker thread per consumer when :meth:`run` is called;
    - installs ``SIGINT`` / ``SIGTERM`` handlers (on the main thread only)
      so external shutdown signals trigger a graceful drain;
    - waits for all worker threads to exit, up to ``shutdown_timeout``
      seconds, on ``__exit__``.

    Both static and dynamic registration are supported. Pass consumers at
    construction for the headline shape; use :meth:`add_consumer` /
    :meth:`remove_consumer` for the DB-driven DAG / reconciler use case
    where the consumer set changes at runtime.
    """

    def __init__(
        self,
        consumers: Sequence[Consumer] = (),
        *,
        install_signals: bool | None = None,
        shutdown_timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
        listen_timeout_ms: int = DEFAULT_LISTEN_TIMEOUT_MS,
        max_snapshots: int = DEFAULT_MAX_SNAPSHOTS,
    ) -> None:
        if shutdown_timeout < 0:
            raise ValueError("shutdown_timeout must be >= 0")
        if listen_timeout_ms < 0:
            raise ValueError("listen_timeout_ms must be >= 0")
        if max_snapshots <= 0:
            raise ValueError("max_snapshots must be > 0")

        self._shutdown_timeout = shutdown_timeout
        self._listen_timeout_ms = listen_timeout_ms
        self._max_snapshots = max_snapshots
        self._install_signals = install_signals
        self._lock = threading.RLock()
        self._workers: dict[str, _Worker] = {}
        self._opened = False
        self._stopping = False
        self._installed_handlers: dict[int, Any] = {}
        for consumer in consumers:
            self._register(consumer)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def add_consumer(self, consumer: Consumer) -> None:
        """Register ``consumer`` with the app.

        Safe to call before or after :meth:`run`. When called on a running
        app, the consumer's sinks are opened immediately and a worker
        thread is launched. Names must be unique within an app.
        """

        with self._lock:
            self._register(consumer)
            worker = self._workers[consumer.name]
            should_enter = self._opened
            should_start = self._opened and not self._stopping

        if should_enter:
            worker.enter()
        if should_start:
            worker.start(
                timeout_ms=self._listen_timeout_ms,
                max_snapshots=self._max_snapshots,
                infinite=True,
            )

    def add_consumers(self, consumers: Iterable[Consumer]) -> None:
        for consumer in consumers:
            self.add_consumer(consumer)

    def remove_consumer(self, name: str) -> Consumer:
        """Stop and de-register the consumer with ``name``.

        Returns the de-registered consumer so the caller can keep a
        reference for re-use. Raises :class:`KeyError` if no consumer
        with that name is registered.
        """

        with self._lock:
            worker = self._workers.pop(name, None)
        if worker is None:
            raise KeyError(f"unknown consumer {name!r}")

        worker.request_stop()
        worker.join(self._shutdown_timeout)
        worker.exit()
        return worker.consumer

    @property
    def consumers(self) -> list[Consumer]:
        with self._lock:
            return [worker.consumer for worker in self._workers.values()]

    def stats(self) -> list[ConsumerHealth]:
        """Per-consumer health snapshot.

        Returns one :class:`ConsumerHealth` per registered consumer in
        registration order.
        """

        with self._lock:
            return [worker.health() for worker in self._workers.values()]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def __enter__(self) -> Self:
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            worker.enter()
        self._install_signal_handlers()
        self._opened = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._opened = False
        self._stopping = True
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            worker.request_stop()

        deadline = time.monotonic() + self._shutdown_timeout
        for worker in workers:
            remaining = max(0.0, deadline - time.monotonic())
            worker.join(remaining)
            if worker.is_alive():
                _LOG.warning(
                    "consumer %r did not exit within shutdown_timeout=%.1fs",
                    worker.name,
                    self._shutdown_timeout,
                )

        for worker in workers:
            worker.exit()
        self._restore_signal_handlers()
        self._stopping = False

    def run(self, *, infinite: bool = True) -> None:
        """Run all registered consumers concurrently.

        ``infinite=True`` (the default) keeps consumers in their listen
        loop until either every worker exits on its own (rare; usually
        only happens on schema-shape termination), the process receives
        ``SIGINT`` / ``SIGTERM``, or the surrounding ``with`` block
        exits.

        ``infinite=False`` runs each consumer in one-shot mode: each
        worker delivers at most one batch and returns. The call returns
        when every worker has finished.

        Must be called inside ``with app:``.
        """

        if not self._opened:
            raise RuntimeError("CDCApp.run() must be called inside `with app:`")

        with self._lock:
            workers = list(self._workers.values())
            if not workers:
                raise RuntimeError("CDCApp has no consumers to run")
            for worker in workers:
                worker.start(
                    timeout_ms=self._listen_timeout_ms,
                    max_snapshots=self._max_snapshots,
                    infinite=infinite,
                )

        try:
            self._wait_for_workers()
        except KeyboardInterrupt:
            with self._lock:
                for worker in self._workers.values():
                    worker.request_stop()
            raise

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _register(self, consumer: Consumer) -> None:
        name = consumer.name
        if name in self._workers:
            raise ValueError(f"consumer {name!r} is already registered")
        self._workers[name] = _Worker(consumer)

    def _wait_for_workers(self) -> None:
        """Block the caller until workers are done or shutdown is signalled.

        Returns when any of the following becomes true:

        - the worker map is empty;
        - every worker thread has exited on its own;
        - ``_stopping`` flips to ``True`` (signal handler, ``__exit__``,
          or :meth:`KeyboardInterrupt` cleanup) — the bounded
          ``shutdown_timeout`` join lives in ``__exit__``, so this method
          must hand control back to it as soon as a stop is requested.

        Hot-added consumers are picked up on each 100 ms tick — small
        enough to feel responsive on ``Ctrl+C`` and large enough to be
        free.
        """

        while True:
            if self._stopping:
                return
            with self._lock:
                workers = list(self._workers.values())
            if not workers:
                return
            if all(not worker.is_alive() for worker in workers):
                return
            time.sleep(0.1)

    def _install_signal_handlers(self) -> None:
        if self._install_signals is False:
            return
        if (
            self._install_signals is None
            and threading.current_thread() is not threading.main_thread()
        ):
            return
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                previous = signal.signal(sig, self._handle_signal)
            except (OSError, ValueError):
                continue
            self._installed_handlers[sig] = previous

    def _restore_signal_handlers(self) -> None:
        for sig, previous in self._installed_handlers.items():
            with suppress(Exception):
                signal.signal(sig, previous)
        self._installed_handlers.clear()

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        _LOG.info("CDCApp received signal %d, shutting down", signum)
        self._stopping = True
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            worker.request_stop()


class _Worker:
    """Per-consumer thread + lifecycle state for :class:`CDCApp`.

    Encapsulates the entered/started/joined state machine so :class:`CDCApp`
    only has to care about three operations per consumer (``enter``,
    ``start``, ``request_stop`` / ``join`` / ``exit``).
    """

    def __init__(self, consumer: Consumer) -> None:
        self.consumer = consumer
        self.stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._entered = False
        self._started_at: datetime | None = None
        self._finished_at: datetime | None = None
        self._delivered = 0
        self._last_error: BaseException | None = None

    @property
    def name(self) -> str:
        return self.consumer.name

    @property
    def kind(self) -> str:
        return type(self.consumer).__name__

    def enter(self) -> None:
        if self._entered:
            return
        self.consumer.__enter__()
        self._entered = True

    def exit(self) -> None:
        if not self._entered:
            return
        self.consumer.__exit__(None, None, None)
        self._entered = False

    def start(
        self,
        *,
        timeout_ms: int,
        max_snapshots: int,
        infinite: bool,
    ) -> None:
        if self._thread is not None:
            return
        self.stop_event.clear()
        thread = threading.Thread(
            target=self._run_loop,
            name=f"cdc-consumer:{self.name}",
            args=(timeout_ms, max_snapshots, infinite),
            daemon=True,
        )
        self._thread = thread
        self._started_at = datetime.now(UTC)
        thread.start()

    def request_stop(self) -> None:
        self.stop_event.set()

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def join(self, timeout: float) -> None:
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout)

    def health(self) -> ConsumerHealth:
        last_error = (
            type(self._last_error).__name__ if self._last_error is not None else None
        )
        return ConsumerHealth(
            name=self.name,
            kind=self.kind,
            running=self.is_alive(),
            started_at=self._started_at,
            finished_at=self._finished_at,
            delivered_batches=self._delivered,
            last_error=last_error,
        )

    def _run_loop(
        self, timeout_ms: int, max_snapshots: int, infinite: bool
    ) -> None:
        try:
            self._delivered = self.consumer.run(
                infinite=infinite,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
                stop_event=self.stop_event,
            )
        except BaseException as exc:
            self._last_error = exc
            _LOG.exception("consumer %r failed", self.name)
        finally:
            self._finished_at = datetime.now(UTC)
