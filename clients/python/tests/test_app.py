"""Tests for :class:`ducklake_cdc.CDCApp`.

These tests stay pure-Python: they use a fake consumer that exposes the
same surface :class:`CDCApp` cares about (``name``, ``__enter__`` /
``__exit__``, ``run(stop_event=…)``) without touching the SQL extension.
The real listen+commit loop is exercised by the demo / integration suite.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any, cast

import pytest

from ducklake_cdc import CDCApp, ConsumerHealth, DMLConsumer
from ducklake_cdc.consumer import _ConsumerBase


class _FakeConsumer:
    """Stand-in consumer that records lifecycle calls and runs a loop.

    Quack-compatible with :class:`DMLConsumer` for the surface
    :class:`CDCApp` uses: ``name``, ``__enter__`` / ``__exit__``,
    and ``run(stop_event=…)``. Behavior is parameterised through
    callbacks so individual tests can shape the worker's loop.
    """

    def __init__(
        self,
        name: str,
        *,
        on_enter: Callable[[], None] | None = None,
        on_run: Callable[[threading.Event], int] | None = None,
        raise_on_run: BaseException | None = None,
    ) -> None:
        self._name = name
        self._on_enter = on_enter
        self._on_run = on_run
        self._raise = raise_on_run
        self.entered = 0
        self.exited = 0
        self.run_calls: list[dict[str, Any]] = []

    @property
    def name(self) -> str:
        return self._name

    def __enter__(self) -> _FakeConsumer:
        self.entered += 1
        if self._on_enter is not None:
            self._on_enter()
        return self

    def __exit__(self, *_args: object) -> None:
        self.exited += 1

    def run(
        self,
        *,
        infinite: bool = True,
        max_batches: int = 0,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
        idle_timeout: float = 0.0,
        stop_event: threading.Event | None = None,
    ) -> int:
        self.run_calls.append(
            {
                "infinite": infinite,
                "max_batches": max_batches,
                "timeout_ms": timeout_ms,
                "max_snapshots": max_snapshots,
                "idle_timeout": idle_timeout,
                "stop_event": stop_event,
            }
        )
        if self._raise is not None:
            raise self._raise
        if self._on_run is not None:
            assert stop_event is not None
            return self._on_run(stop_event)
        return 0


def _consumer(name: str, **kwargs: Any) -> DMLConsumer:
    """Cast a fake to :class:`DMLConsumer` to satisfy CDCApp's type hints."""
    return cast(DMLConsumer, _FakeConsumer(name, **kwargs))


def _wait_for(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting up to {timeout:.1f}s for predicate")


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_app_rejects_negative_shutdown_timeout() -> None:
    with pytest.raises(ValueError, match="shutdown_timeout"):
        CDCApp(shutdown_timeout=-1.0, install_signals=False)


def test_app_rejects_negative_listen_timeout_ms() -> None:
    with pytest.raises(ValueError, match="listen_timeout_ms"):
        CDCApp(listen_timeout_ms=-1, install_signals=False)


def test_app_rejects_non_positive_max_snapshots() -> None:
    with pytest.raises(ValueError, match="max_snapshots"):
        CDCApp(max_snapshots=0, install_signals=False)


def test_app_rejects_duplicate_consumer_names() -> None:
    consumers = [_consumer("dup"), _consumer("dup")]
    with pytest.raises(ValueError, match="already registered"):
        CDCApp(consumers=consumers, install_signals=False)


def test_app_consumers_property_returns_registration_order() -> None:
    a, b, c = _consumer("a"), _consumer("b"), _consumer("c")
    app = CDCApp(consumers=[a, b, c], install_signals=False)

    assert [con.name for con in app.consumers] == ["a", "b", "c"]


def test_app_run_without_consumers_raises() -> None:
    with CDCApp(install_signals=False) as app:
        with pytest.raises(RuntimeError, match="no consumers"):
            app.run(infinite=False)


def test_app_run_outside_with_block_raises() -> None:
    app = CDCApp(consumers=[_consumer("a")], install_signals=False)
    with pytest.raises(RuntimeError, match="inside `with app:`"):
        app.run()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_app_enter_opens_each_consumer_once() -> None:
    fakes = [_FakeConsumer("a"), _FakeConsumer("b")]
    app = CDCApp(
        consumers=[cast(DMLConsumer, fake) for fake in fakes],
        install_signals=False,
    )

    with app:
        assert all(fake.entered == 1 for fake in fakes)

    assert all(fake.exited == 1 for fake in fakes)


def test_app_run_one_shot_invokes_each_consumer_with_stop_event() -> None:
    fakes = [_FakeConsumer("a"), _FakeConsumer("b")]
    app = CDCApp(
        consumers=[cast(DMLConsumer, fake) for fake in fakes],
        install_signals=False,
    )

    with app:
        app.run(infinite=False)

    for fake in fakes:
        assert len(fake.run_calls) == 1
        call = fake.run_calls[0]
        assert call["infinite"] is False
        assert isinstance(call["stop_event"], threading.Event)


def test_app_run_infinite_returns_when_stop_event_set() -> None:
    def loop_until_stop(stop_event: threading.Event) -> int:
        while not stop_event.is_set():
            time.sleep(0.01)
        return 7

    fake = _FakeConsumer("a", on_run=loop_until_stop)
    app = CDCApp(
        consumers=[cast(DMLConsumer, fake)],
        install_signals=False,
        shutdown_timeout=2.0,
    )

    with app:
        runner = threading.Thread(target=lambda: app.run(infinite=True))
        runner.start()

        worker = next(iter(_workers(app).values()))
        _wait_for(lambda: worker.is_alive())
        worker.request_stop()
        runner.join(timeout=2.0)

        assert not runner.is_alive()
        health = app.stats()[0]
        assert health.delivered_batches == 7

    assert fake.exited == 1


def test_app_propagates_consumer_exception_to_health() -> None:
    fake = _FakeConsumer("a", raise_on_run=RuntimeError("boom"))
    app = CDCApp(
        consumers=[cast(DMLConsumer, fake)],
        install_signals=False,
    )

    with app:
        app.run(infinite=False)

    health = app.stats()[0]
    assert health.last_error == "RuntimeError"
    assert health.delivered_batches == 0
    assert health.running is False


def test_app_one_failing_consumer_does_not_kill_others() -> None:
    crash = _FakeConsumer("crash", raise_on_run=RuntimeError("boom"))
    counter = {"runs": 0}

    def succeed(_stop: threading.Event) -> int:
        counter["runs"] += 1
        return 3

    survivor = _FakeConsumer("survivor", on_run=succeed)
    app = CDCApp(
        consumers=[cast(DMLConsumer, crash), cast(DMLConsumer, survivor)],
        install_signals=False,
    )

    with app:
        app.run(infinite=False)

    assert counter["runs"] == 1
    crash_health, survivor_health = app.stats()
    assert crash_health.last_error == "RuntimeError"
    assert survivor_health.last_error is None
    assert survivor_health.delivered_batches == 3


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


def test_app_stats_initial_state_for_unstarted_consumers() -> None:
    app = CDCApp(consumers=[_consumer("a")], install_signals=False)

    health = app.stats()[0]
    assert health == ConsumerHealth(
        name="a",
        kind="_FakeConsumer",
        running=False,
        started_at=None,
        finished_at=None,
        delivered_batches=0,
        last_error=None,
    )


def test_app_stats_marks_finished_consumers_with_started_and_finished() -> None:
    fake = _FakeConsumer("a", on_run=lambda _stop: 2)
    app = CDCApp(consumers=[cast(DMLConsumer, fake)], install_signals=False)

    with app:
        app.run(infinite=False)

    health = app.stats()[0]
    assert health.delivered_batches == 2
    assert health.running is False
    assert health.started_at is not None
    assert health.finished_at is not None
    assert health.finished_at >= health.started_at


# ---------------------------------------------------------------------------
# Hot add / remove
# ---------------------------------------------------------------------------


def test_app_add_consumer_after_enter_starts_worker() -> None:
    started = threading.Event()

    def loop(stop_event: threading.Event) -> int:
        started.set()
        while not stop_event.is_set():
            time.sleep(0.01)
        return 1

    fake = _FakeConsumer("late", on_run=loop)
    app = CDCApp(install_signals=False, shutdown_timeout=2.0)

    with app:
        app.add_consumer(cast(DMLConsumer, fake))
        _wait_for(started.is_set)
        assert fake.entered == 1
        worker = next(iter(_workers(app).values()))
        worker.request_stop()
        _wait_for(lambda: not worker.is_alive())

    assert fake.exited == 1


def test_app_remove_consumer_stops_and_returns_it() -> None:
    started = threading.Event()

    def loop(stop_event: threading.Event) -> int:
        started.set()
        while not stop_event.is_set():
            time.sleep(0.01)
        return 4

    fake = _FakeConsumer("removable", on_run=loop)
    app = CDCApp(
        consumers=[cast(DMLConsumer, fake)],
        install_signals=False,
        shutdown_timeout=2.0,
    )

    with app:
        worker = next(iter(_workers(app).values()))
        worker.start(timeout_ms=10, max_snapshots=10, infinite=True)
        _wait_for(started.is_set)
        removed = app.remove_consumer("removable")

        assert removed is fake
        assert app.consumers == []
        assert fake.exited == 1


def test_app_remove_consumer_unknown_raises_keyerror() -> None:
    app = CDCApp(install_signals=False)
    with pytest.raises(KeyError, match="missing"):
        app.remove_consumer("missing")


def test_app_add_consumer_with_existing_name_raises() -> None:
    app = CDCApp(consumers=[_consumer("dup")], install_signals=False)
    with pytest.raises(ValueError, match="already registered"):
        app.add_consumer(_consumer("dup"))


def test_app_hot_add_serializes_consumer_enter() -> None:
    inside_enter = 0
    max_inside_enter = 0
    guard = threading.Lock()

    def enter_slowly() -> None:
        nonlocal inside_enter, max_inside_enter
        with guard:
            inside_enter += 1
            max_inside_enter = max(max_inside_enter, inside_enter)
        time.sleep(0.02)
        with guard:
            inside_enter -= 1

    app = CDCApp(install_signals=False)
    fakes = [
        _FakeConsumer(f"hot-{idx}", on_enter=enter_slowly)
        for idx in range(8)
    ]

    with app:
        threads = [
            threading.Thread(target=app.add_consumer, args=(cast(DMLConsumer, fake),))
            for fake in fakes
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2.0)

    assert all(not thread.is_alive() for thread in threads)
    assert all(fake.entered == 1 for fake in fakes)
    assert max_inside_enter == 1


# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------


def test_app_install_signals_false_skips_handler_registration() -> None:
    import signal as signal_mod

    original_int = signal_mod.getsignal(signal_mod.SIGINT)
    original_term = signal_mod.getsignal(signal_mod.SIGTERM)

    fake = _FakeConsumer("a")
    app = CDCApp(consumers=[cast(DMLConsumer, fake)], install_signals=False)

    with app:
        assert signal_mod.getsignal(signal_mod.SIGINT) is original_int
        assert signal_mod.getsignal(signal_mod.SIGTERM) is original_term

    assert signal_mod.getsignal(signal_mod.SIGINT) is original_int
    assert signal_mod.getsignal(signal_mod.SIGTERM) is original_term


def test_app_install_signals_true_restores_previous_handlers() -> None:
    import signal as signal_mod

    original_int = signal_mod.getsignal(signal_mod.SIGINT)
    original_term = signal_mod.getsignal(signal_mod.SIGTERM)

    fake = _FakeConsumer("a")
    app = CDCApp(consumers=[cast(DMLConsumer, fake)], install_signals=True)

    with app:
        # Inside the with block our handler should be installed when on the
        # main thread; either way, restoration on exit is what we are
        # actually asserting.
        pass

    assert signal_mod.getsignal(signal_mod.SIGINT) is original_int
    assert signal_mod.getsignal(signal_mod.SIGTERM) is original_term


# ---------------------------------------------------------------------------
# Stop-event plumbing in the real consumer
# ---------------------------------------------------------------------------


def test_consumer_run_loop_returns_when_stop_event_pre_set() -> None:
    """The cooperative-stop hook on _ConsumerBase.run() exits without ever
    issuing a listen call when the event is already set."""

    class _Recorder(_ConsumerBase):
        _kind = "test"

        def __init__(self) -> None:
            self._opened = True
            self._listen_calls = 0

        def _listen_op(self, *_args: int) -> Callable[[], list[Any]]:  # type: ignore[override]
            def op() -> list[Any]:
                self._listen_calls += 1
                return []

            return op

        def _build_batch(self, _rows: list[Any]) -> Any:  # type: ignore[override]
            raise AssertionError("unreachable")

        def _commit_op(self, _snap: int) -> Callable[[], object]:  # type: ignore[override]
            raise AssertionError("unreachable")

        def _retry(self, op: Callable[..., Any]) -> Any:  # type: ignore[override]
            return op()

    recorder = _Recorder()
    stop = threading.Event()
    stop.set()

    delivered = recorder.run(
        infinite=True,
        timeout_ms=1_000,
        stop_event=stop,
    )

    assert delivered == 0
    assert recorder._listen_calls == 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _workers(app: CDCApp) -> dict[str, Any]:
    """Reach into the app for direct worker access in tests."""
    return app._workers  # type: ignore[attr-defined]
