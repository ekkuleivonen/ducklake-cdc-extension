"""Tiny multi-stage runner that composes ``DMLConsumer`` + ``batch.transaction()``.

A stage is a function ``transform(batch, tx) -> rows_written``. The
runner gives it one OS thread, one ``DMLConsumer`` (which derives a
dedicated DuckDB connection), and runs every batch's transform inside
``batch.transaction()`` -- BEGIN on enter, ``cdc_commit`` + ``COMMIT``
on success, ROLLBACK on exception.

Usage::

    STAGES = [
        Stage("normalize_purchases", source="raw_purchases", transform=normalize_purchases),
        Stage("normalize_refunds",   source="raw_refunds",   transform=normalize_refunds),
        Stage("join_orders",         source="clean_purchases", transform=join_orders),
    ]

    with StageRunner(lake, STAGES, stop=stop, recorder=recorder):
        ...                  # producers run here, TUI renders, Ctrl-C, etc.
    # __exit__ joins the stage threads.

Why example-local (``e2e/_lib/``) and not in the client lib: this is
the *first* place we've needed it, and the right shape will only
emerge after a second example exercises it. Promote when a second
caller wants the same thing.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Self

from ducklake_cdc_client import BatchTransaction, DMLBatch, DMLConsumer
from ducklake_cdc_client.consumers.base import LeasePolicy

from _lib.metrics import MetricsRecorder
from _lib.tui import log

# Long-poll defaults. ``timeout_ms`` doubles as the upper bound on
# shutdown latency: each stage's listen blocks for at most this long
# before noticing ``stop_event``. ``max_snapshots`` is a ceiling --
# ``DMLConsumer`` adapts the actual size based on producer cadence.
DEFAULT_LISTEN_TIMEOUT_MS = 1000
DEFAULT_LISTEN_MAX_SNAPSHOTS = 1000

# Per-stage ``__enter__`` budget. The first ``cdc_dml_consumer_create``
# against a fresh catalog bootstraps ``__ducklake_cdc.*`` and can take
# a few seconds on a cold pgbouncer; later stages just register a row
# in ``__ducklake_cdc.consumers`` and finish promptly.
STAGE_READY_TIMEOUT_S = 10.0

# Drain budget on ``__exit__``. The runner sets ``_drain_event`` and
# gives every stage up to this long to drain its consumer's backlog
# (producers are stopped by the time we're here). 5s is generous --
# typical drain finishes in 1-2 ``LISTEN_TIMEOUT_MS`` cycles plus the
# drain idle wait. Tune up if a perf-tuned producer commits batches
# right at the shutdown edge faster than the consumer can process
# them.
DRAIN_TIMEOUT_S = 5.0

# How long a stage's listen has to be empty before the drain pass
# considers the consumer "caught up" and exits. Bigger value =
# safer (less chance of exiting before a late commit lands), smaller
# = faster shutdown. 2s matches the lib default and is the right
# tradeoff for the v3 example.
DRAIN_IDLE_TIMEOUT_S = 2.0

# Per-thread force-stop budget after drain has run. A batch
# transaction (INSERT + cdc_commit + COMMIT) is bounded sub-second on
# the postgres catalog; anything longer means the daemon thread will
# be killed at interpreter exit.
STAGE_JOIN_TIMEOUT_S = 2.0


StageTransform = Callable[[DMLBatch, BatchTransaction], int]
"""``transform(batch, tx) -> rows_written``.

Inside the function:

- ``batch`` is the typed CDC batch (iterate ``batch`` for ``Change``
  objects with ``.kind`` / ``.snapshot_id`` / ``.values["..."]``).
- ``tx`` exposes ``execute`` / ``executemany`` / ``connection`` on the
  consumer's lease-holding DuckDB connection. Anything you run on it
  is part of the same BEGIN that wraps the cursor advance.

The return value is the number of rows the stage wrote to its sink (0
if the batch was filtered out entirely). Used for per-stage metrics
only -- the cursor advances regardless, so the consumer's view of the
upstream stream is always exactly-once.
"""


#: Convenience alias for the common "consume only inserts" subscription.
#: Passing this to ``Stage(change_types=...)`` makes
#: ``cdc_dml_consumer_create`` filter the cursor at the extension level
#: -- updates and deletes never even cross the listen boundary, so the
#: transform doesn't need to filter and the listen call doesn't waste
#: bandwidth shipping rows the sink would drop anyway.
INSERTS_ONLY: tuple[str, ...] = ("insert",)


@dataclass(frozen=True)
class Stage:
    """One node in the in-process pipeline DAG.

    ``name`` is the CDC consumer name (must be unique within the lake)
    and is used as the metrics namespace (``stage_<name>_rows`` etc).
    ``source`` is the qualified table name the consumer subscribes to.
    ``change_types`` is forwarded to ``DMLConsumer`` and ultimately to
    ``cdc_dml_consumer_create`` -- ``None`` (the default) means "all
    change types" (insert + update_preimage + update_postimage +
    delete); pass e.g. :data:`INSERTS_ONLY` to make the cursor itself
    skip non-insert change rows.

    ``lease_policy`` controls what happens when the consumer name
    already exists in the catalog with a live lease. The lib's
    ``DMLConsumer`` default is ``"wait"`` (block up to
    ``lease_wait_timeout`` seconds for the lease to expire), which is
    correct in *multi-process* scenarios where two processes might
    race for the same consumer. For an *in-process* pipeline DAG --
    where the only thing that holds a lease for the name is the same
    pipeline's previous incarnation -- ``"takeover"`` is the right
    default: the previous process is gone, this one is the rightful
    owner, no need to wait. ``StageRunner`` defaults to ``"takeover"``
    accordingly. Switch to ``"wait"`` (or ``"error"``) only if you're
    intentionally running multiple instances against the same lake
    and want them to coordinate.
    """

    name: str
    source: str
    transform: StageTransform
    change_types: tuple[str, ...] | None = None
    lease_policy: LeasePolicy = "takeover"


@dataclass
class StageRunner:
    """Owns one OS thread per stage and the H-022-safe sequenced startup.

    Acts as a context manager:

    - ``__enter__`` starts each stage's thread serially (so per-stage
      ``cdc_dml_consumer_create`` doesn't race with a previous stage's
      first INSERTs on a fresh catalog -- H-022), waiting for each to
      signal ready before starting the next.
    - ``__exit__`` is a two-phase shutdown:

      1. *Drain phase*: set the internal ``_drain_event``, which makes
         every stage's ``DMLConsumer.batches()`` switch from "block on
         listen forever" to "exit when listen has been idle for
         ``drain_idle_timeout`` seconds." Wait up to
         ``drain_timeout_s`` for the threads to finish naturally. This
         is what closes the v2 "200-row tail" gap: producers have
         already stopped by the time ``__exit__`` runs (caller's
         responsibility), so the drain pass picks up any commits the
         consumer hadn't processed yet and advances cursors past
         them.
      2. *Force-stop phase*: any thread that didn't finish during
         drain gets ``stop`` set on it, then ``join(timeout=
         STAGE_JOIN_TIMEOUT_S)``. Daemons die on interpreter exit if
         even that times out.

    Caller's contract is unchanged from v2: stop the producers (so no
    new upstream writes arrive), then exit the ``with`` block. The
    runner handles the rest.
    """

    lake: Any
    stages: tuple[Stage, ...]
    stop: threading.Event
    recorder: MetricsRecorder
    timeout_ms: int = DEFAULT_LISTEN_TIMEOUT_MS
    max_snapshots: int = DEFAULT_LISTEN_MAX_SNAPSHOTS
    drain_timeout_s: float = DRAIN_TIMEOUT_S
    drain_idle_timeout_s: float = DRAIN_IDLE_TIMEOUT_S
    _threads: list[threading.Thread] = field(default_factory=list, init=False, repr=False)
    _drain_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)

    def __enter__(self) -> Self:
        for stage in self.stages:
            ready = threading.Event()
            thread = threading.Thread(
                target=self._run_stage,
                args=(stage, ready),
                name=f"stage:{stage.name}",
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()
            if not ready.wait(timeout=STAGE_READY_TIMEOUT_S):
                self.stop.set()
                raise RuntimeError(
                    f"stage {stage.name!r} did not become ready within "
                    f"{STAGE_READY_TIMEOUT_S:.0f}s"
                )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Phase 1: drain. Stages finish their listen loops naturally
        # once the upstream is idle for ``drain_idle_timeout_s``
        # seconds. Skip the drain phase entirely if the caller already
        # set ``stop`` (panic shutdown) -- the drain wait would just
        # eat shutdown latency for no benefit, and the lib's
        # ``stop_event`` check inside ``batches()`` would short-
        # circuit it anyway.
        if not self.stop.is_set():
            self._drain_event.set()
            drain_deadline = time.monotonic() + self.drain_timeout_s
            for thread in self._threads:
                remaining = max(0.0, drain_deadline - time.monotonic())
                thread.join(timeout=remaining)

        # Phase 2: force-stop. Anything still alive gets the panic
        # signal and a small join window; daemons die on interpreter
        # exit if they're still stuck after that.
        self.stop.set()
        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=STAGE_JOIN_TIMEOUT_S)

    def _run_stage(self, stage: Stage, ready: threading.Event) -> None:
        # Note: stage.name is wrapped with double-square-brackets so
        # rich's markup parser in ``_lib.tui.log`` keeps the literal
        # ``[normalize_purchases]`` instead of treating it as a style.
        log(f"\\[{stage.name}] starting (source={stage.source})")
        batches_seen = 0
        rows_written = 0
        try:
            with DMLConsumer(
                self.lake,
                stage.name,
                table=stage.source,
                mode="changes",
                change_types=list(stage.change_types) if stage.change_types else None,
                lease_policy=stage.lease_policy,
            ) as consumer:
                # Signal "consumer is open" so ``StageRunner.__enter__``
                # advances to the next stage. The consumer-create call
                # is what trips H-022; doing it serially across stages
                # is what makes the pipeline boot reliably.
                ready.set()
                for batch in consumer.batches(
                    stop_event=self.stop,
                    drain_event=self._drain_event,
                    drain_idle_timeout=self.drain_idle_timeout_s,
                    timeout_ms=self.timeout_ms,
                    max_snapshots=self.max_snapshots,
                ):
                    t_start = time.monotonic()
                    try:
                        with batch.transaction() as tx:
                            written = stage.transform(batch, tx)
                    except Exception as exc:  # noqa: BLE001
                        self.recorder.record_error()
                        log(
                            f"\\[{stage.name}] {type(exc).__name__}: {exc}",
                            level="error",
                        )
                        # Don't sleep -- the next listen call already
                        # blocks for up to ``timeout_ms``, and the
                        # consumer's cursor stays parked until we
                        # successfully apply the same batch.
                        continue

                    elapsed_ms = (time.monotonic() - t_start) * 1000.0
                    batches_seen += 1
                    rows_written += written
                    self.recorder.record_rows(written)
                    self.recorder.record_latency_ms(elapsed_ms)
                    self.recorder.set_detail(f"stage_{stage.name}_batches", batches_seen)
                    self.recorder.set_detail(f"stage_{stage.name}_rows", rows_written)
                    self.recorder.set_detail(
                        f"stage_{stage.name}_last_snapshot", batch.end_snapshot
                    )
        finally:
            log(f"\\[{stage.name}] stopped")
            # Make sure ``__enter__`` doesn't hang if ``DMLConsumer`` itself
            # raised before we got into the loop.
            ready.set()
