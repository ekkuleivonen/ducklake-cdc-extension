"""High-level consumers for the ducklake-cdc extension.

This module exposes :class:`DMLConsumer` and :class:`DDLConsumer`. Both are
context managers that own a consumer's lifecycle: they create (or attach to)
a durable consumer in the extension, open the attached sinks, run a
sink-gated listen+deliver+commit loop, and close the sinks on exit.

The headline shape::

    with DMLConsumer(lake, "orders", table="public.orders", sinks=[...]) as c:
        c.run()

DML consumers are pinned to exactly one table by contract — see
``cdc_dml_consumer_create`` in the SQL extension. Multi-table fan-out is
the orchestrator's job: spawn one :class:`DMLConsumer` per table and join
downstream of the sinks. This keeps the schema-shape termination
contract (``cdc_window.terminal``) crisp: if the pinned table's shape
changes, that one consumer hard-stops and the orchestrator spawns a
successor at the boundary snapshot.

Sinks are the only output path. Returning from ``sink.write`` acks; raising
nacks and the batch is left uncommitted (so it will be redelivered on the
next listen call).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Literal, Self, TypeVar

from ducklake import DuckLake
from ducklake_cdc.lowlevel import (
    CDCClient,
    ChangeRow,
    ConsumerListEntry,
    SchemaChangeRow,
)
from ducklake_cdc.types import (
    Change,
    DDLBatch,
    DDLSink,
    DMLBatch,
    DMLSink,
    SchemaChange,
    SinkContext,
)

T = TypeVar("T")

OnExists = Literal["error", "use", "replace"]
LeasePolicy = Literal["wait", "takeover", "error"]
StartAt = str | int

RetryPolicy = Callable[[Callable[[], object]], object]

_LOG = logging.getLogger(__name__)
_DEFAULT_LEASE_WAIT_TIMEOUT = 30.0
_LEASE_WAIT_POLL_INTERVAL = 0.5
_LEASE_FRESHNESS_GRACE_SECONDS = 5.0


class _ConsumerBase:
    """Shared lifecycle and run loop for DML and DDL consumers.

    The base is intentionally not parameterized over the batch / sink
    types: mypy cannot pair two independent ``TypeVar``s the way we need
    (DML batch ↔ DML sink, DDL batch ↔ DDL sink), so we keep the precise
    typing on the public :class:`DMLConsumer` / :class:`DDLConsumer`
    surface and use ``Any`` internally. Subclasses provide:

    - ``_kind`` (``"dml"`` or ``"ddl"``) for error messages.
    - ``_create_consumer`` to call ``cdc_*_consumer_create``.
    - ``_listen_op`` to call the appropriate ``cdc_*_changes_listen``.
    - ``_build_batch`` to package raw rows into the batch type.
    """

    _kind: str

    def __init__(
        self,
        lake: DuckLake,
        name: str,
        *,
        start_at: StartAt = "now",
        on_exists: OnExists = "use",
        lease_policy: LeasePolicy = "wait",
        lease_wait_timeout: float = _DEFAULT_LEASE_WAIT_TIMEOUT,
        sinks: Sequence[Any] = (),
        client: CDCClient | None = None,
        retry: RetryPolicy | None = None,
    ) -> None:
        if not name:
            raise ValueError(f"{type(self).__name__} requires a non-empty name")
        if not sinks:
            raise ValueError(
                f"{type(self).__name__} requires at least one sink — sinks are "
                "the only output path. Pass a built-in sink (e.g. "
                "StdoutDMLSink() or StdoutDDLSink()) if you just want to see "
                "events."
            )
        if lease_policy not in ("wait", "takeover", "error"):
            raise ValueError(
                f"lease_policy must be 'wait', 'takeover', or 'error'; "
                f"got {lease_policy!r}"
            )
        if lease_wait_timeout < 0:
            raise ValueError("lease_wait_timeout must be >= 0")

        self._lake = lake
        self._name = name
        self._start_at = start_at
        self._on_exists = on_exists
        self._lease_policy: LeasePolicy = lease_policy
        self._lease_wait_timeout = lease_wait_timeout
        self._sinks: list[Any] = list(sinks)
        self._client = client
        self._retry_policy = retry
        self._opened = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def client(self) -> CDCClient:
        if self._client is None:
            raise RuntimeError(
                f"{type(self).__name__}.client is only available inside a "
                "`with` block"
            )
        return self._client

    def __enter__(self) -> Self:
        if self._client is None:
            self._client = CDCClient(self._lake)
        try:
            self._setup_consumer()
            self._apply_lease_policy()
            self._open_sinks()
            self._opened = True
        except BaseException:
            self._close_sinks_quietly()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._opened = False
        self._close_sinks_quietly()

    def run(
        self,
        *,
        infinite: bool = True,
        max_batches: int = 0,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
        idle_timeout: float = 0.0,
    ) -> int:
        """Run the listen+deliver+commit loop.

        Stops when any of these fires:

        - ``infinite=False`` and one batch has been delivered (one-shot).
        - ``max_batches > 0`` and that many batches have been delivered.
        - ``idle_timeout > 0`` and that many seconds have passed without a
          non-empty batch.
        - ``KeyboardInterrupt`` (caller's responsibility to handle).

        Returns the number of batches delivered.
        """

        self._require_open()
        delivered = 0
        last_activity = time.monotonic()

        while True:
            rows = self._retry(self._listen_op(timeout_ms, max_snapshots))
            if not rows:
                if not infinite:
                    return delivered
                if idle_timeout > 0 and (time.monotonic() - last_activity) >= idle_timeout:
                    return delivered
                continue

            last_activity = time.monotonic()
            batch = self._build_batch(rows)
            self._deliver(batch)
            self._retry(self._commit_op(batch.end_snapshot))
            delivered += 1

            if not infinite:
                return delivered
            if max_batches > 0 and delivered >= max_batches:
                return delivered

    def _setup_consumer(self) -> None:
        client = self._require_client()
        name = self._name
        exists = any(entry.consumer_name == name for entry in client.cdc_list_consumers())

        if exists and self._on_exists == "error":
            raise RuntimeError(f"consumer {name!r} already exists")

        if exists and self._on_exists == "replace":
            client.cdc_consumer_force_release(name)
            client.cdc_consumer_drop(name)
            self._create_and_position(client)
            return

        if exists:
            return

        self._create_and_position(client)

    def _create_and_position(self, client: CDCClient) -> None:
        self._create_consumer(client)
        if self._start_at != "now":
            client.cdc_consumer_reset(self._name, to_snapshot=self._start_at)

    def _apply_lease_policy(self) -> None:
        """Resolve a held lease per ``lease_policy`` before listening.

        ``replace`` already force-releases as part of `_setup_consumer`, so
        the policy is effectively bypassed for that path; we still re-check
        here in case a third party reacquired the lease in the gap.
        """

        client = self._require_client()
        entry = self._lookup_consumer(client)
        if entry is None or not _lease_is_alive(entry):
            return

        if self._lease_policy == "error":
            raise RuntimeError(
                f"consumer {self._name!r} is leased by {entry.owner_token} "
                "and lease_policy='error' was requested"
            )

        if self._lease_policy == "takeover":
            client.cdc_consumer_force_release(self._name)
            return

        deadline = time.monotonic() + self._lease_wait_timeout
        while True:
            entry = self._lookup_consumer(client)
            if entry is None or not _lease_is_alive(entry):
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"timed out after {self._lease_wait_timeout:.1f}s waiting "
                    f"for consumer {self._name!r} lease (held by "
                    f"{entry.owner_token})"
                )
            time.sleep(_LEASE_WAIT_POLL_INTERVAL)

    def _lookup_consumer(self, client: CDCClient) -> ConsumerListEntry | None:
        for entry in client.cdc_list_consumers():
            if entry.consumer_name == self._name:
                return entry
        return None

    def _open_sinks(self) -> None:
        opened: list[Any] = []
        try:
            for sink in self._sinks:
                sink.open()
                opened.append(sink)
        except BaseException:
            for sink in reversed(opened):
                try:
                    sink.close()
                except Exception:
                    _LOG.exception("error closing sink %r during rollback", _sink_name(sink))
            raise

    def _close_sinks_quietly(self) -> None:
        for sink in reversed(self._sinks):
            try:
                sink.close()
            except Exception:
                _LOG.exception("error closing sink %r", _sink_name(sink))

    def _deliver(self, batch: Any) -> None:
        ctx = SinkContext(
            consumer_name=self._name,
            batch_id=batch.batch_id,
            _heartbeat=self._heartbeat,
        )
        for sink in self._sinks:
            try:
                sink.write(batch, ctx)
            except Exception as exc:
                if getattr(sink, "require_ack", True):
                    raise
                _LOG.warning(
                    "optional sink %r raised on batch %s: %s",
                    _sink_name(sink),
                    batch.batch_id,
                    exc,
                )

    def _heartbeat(self) -> None:
        client = self._client
        if client is None:
            return
        self._retry(lambda: client.cdc_consumer_heartbeat(self._name))

    def _commit_op(self, snapshot: int) -> Callable[[], object]:
        client = self._require_client()
        name = self._name

        def operation() -> object:
            return client.cdc_commit(name, snapshot)

        return operation

    def _retry(self, operation: Callable[[], T]) -> T:
        if self._retry_policy is None:
            return operation()
        return self._retry_policy(operation)  # type: ignore[return-value]

    def _require_open(self) -> None:
        if not self._opened:
            raise RuntimeError(
                f"{type(self).__name__}.run() must be called inside a "
                "`with consumer:` block"
            )

    def _require_client(self) -> CDCClient:
        if self._client is None:
            raise RuntimeError(
                f"{type(self).__name__} client is not initialized; use "
                "`with consumer:`"
            )
        return self._client

    def _create_consumer(self, client: CDCClient) -> None:
        raise NotImplementedError

    def _listen_op(self, timeout_ms: int, max_snapshots: int) -> Callable[[], list[Any]]:
        raise NotImplementedError

    def _build_batch(self, rows: list[Any]) -> Any:
        raise NotImplementedError


class DMLConsumer(_ConsumerBase):
    """Durable consumer for row-level DML changes.

    Construction is cheap — no SQL is issued. The ``with`` block does the
    work: it creates or attaches to the consumer, opens sinks, and (on exit)
    closes sinks.

    ``on_exists`` controls creation/attachment behavior:

    - ``"use"`` (default): attach to an existing consumer if present, else
      create. Filters are not changed.
    - ``"error"``: fail if the consumer already exists.
    - ``"replace"``: drop and recreate. Force-releases the lease first.

    ``start_at`` accepts ``"now"``, ``"beginning"``, ``"oldest"``, or a
    snapshot id. The consumer is created with ``start_at="now"`` and then
    reset to the requested point — the SQL extension's reset accepts the
    full vocabulary. ``start_at`` is ignored for ``on_exists="use"`` when
    the consumer already exists, to avoid silently rewinding an existing
    durable cursor.

    ``lease_policy`` controls how the consumer reconciles a lease that is
    already held by someone else:

    - ``"wait"`` (default): poll up to ``lease_wait_timeout`` seconds for
      the holder to release. ``TimeoutError`` is raised on timeout.
    - ``"takeover"``: force-release the existing lease.
    - ``"error"``: raise immediately if a lease is held.
    """

    _kind = "dml"

    def __init__(
        self,
        lake: DuckLake,
        name: str,
        *,
        table: str | None = None,
        table_id: int | None = None,
        change_types: Sequence[str] | None = None,
        start_at: StartAt = "now",
        on_exists: OnExists = "use",
        lease_policy: LeasePolicy = "wait",
        lease_wait_timeout: float = _DEFAULT_LEASE_WAIT_TIMEOUT,
        sinks: Sequence[DMLSink] = (),
        client: CDCClient | None = None,
        retry: RetryPolicy | None = None,
    ) -> None:
        # Run name / sinks / lease validation first via the base class, so
        # callers see those (simpler) errors instead of the more specific
        # "exactly one of table / table_id" message when they pass
        # multiple invalid arguments at once.
        super().__init__(
            lake,
            name,
            start_at=start_at,
            on_exists=on_exists,
            lease_policy=lease_policy,
            lease_wait_timeout=lease_wait_timeout,
            sinks=sinks,
            client=client,
            retry=retry,
        )
        # The "exactly one of table / table_id" check is enforced by the
        # SQL extension at create time; we mirror it here so the error
        # surfaces before we open sinks (cheaper to fail-fast in the
        # constructor than mid-`__enter__`).
        if (table is None) == (table_id is None):
            raise ValueError(
                "DMLConsumer requires exactly one of table=… or table_id=… "
                "(DML consumers are pinned to a single table)."
            )
        self._table = table
        self._table_id = table_id
        self._change_types = list(change_types) if change_types else None

    def _create_consumer(self, client: CDCClient) -> None:
        client.cdc_dml_consumer_create(
            self._name,
            table_name=self._table,
            table_id=self._table_id,
            change_types=self._change_types,
            start_at="now",
        )

    def _listen_op(
        self, timeout_ms: int, max_snapshots: int
    ) -> Callable[[], list[ChangeRow]]:
        client = self._require_client()
        name = self._name

        def operation() -> list[ChangeRow]:
            return client.cdc_dml_changes_listen(
                name,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
            )

        return operation

    def _build_batch(self, rows: list[ChangeRow]) -> DMLBatch:
        start = min(
            row.start_snapshot if row.start_snapshot is not None else row.snapshot_id
            for row in rows
        )
        end = max(
            row.end_snapshot if row.end_snapshot is not None else row.snapshot_id
            for row in rows
        )
        snapshot_ids = tuple(sorted({row.snapshot_id for row in rows}))
        changes = tuple(
            Change(
                kind=row.change_type,
                snapshot_id=row.snapshot_id,
                table=row.table_name,
                table_id=row.table_id,
                rowid=row.rowid,
                snapshot_time=row.snapshot_time,
                values=row.values,
            )
            for row in rows
        )
        return DMLBatch(
            consumer_name=self._name,
            batch_id=DMLBatch.derive_batch_id(self._name, start, end),
            start_snapshot=start,
            end_snapshot=end,
            snapshot_ids=snapshot_ids,
            received_at=datetime.now(UTC),
            changes=changes,
        )


class DDLConsumer(_ConsumerBase):
    """Durable consumer for catalog/schema/table DDL events.

    Mirrors :class:`DMLConsumer` but consumes :class:`SchemaChange` events
    out of ``cdc_ddl_changes_listen``. Filters are scoped at construction
    time:

    - ``schemas=[...]`` — watch DDL for those schemas.
    - ``tables=[...]`` — watch DDL touching those table identities.
    - both omitted — watch all DDL for the catalog.

    The ``on_exists``, ``start_at``, and ``lease_policy`` arguments behave
    the same as on :class:`DMLConsumer`.
    """

    _kind = "ddl"

    def __init__(
        self,
        lake: DuckLake,
        name: str,
        *,
        schemas: Sequence[str] | None = None,
        tables: Sequence[str] | None = None,
        start_at: StartAt = "now",
        on_exists: OnExists = "use",
        lease_policy: LeasePolicy = "wait",
        lease_wait_timeout: float = _DEFAULT_LEASE_WAIT_TIMEOUT,
        sinks: Sequence[DDLSink] = (),
        client: CDCClient | None = None,
        retry: RetryPolicy | None = None,
    ) -> None:
        super().__init__(
            lake,
            name,
            start_at=start_at,
            on_exists=on_exists,
            lease_policy=lease_policy,
            lease_wait_timeout=lease_wait_timeout,
            sinks=sinks,
            client=client,
            retry=retry,
        )
        self._schemas = list(schemas) if schemas else None
        self._tables = list(tables) if tables else None

    def _create_consumer(self, client: CDCClient) -> None:
        client.cdc_ddl_consumer_create(
            self._name,
            schemas=self._schemas,
            table_names=self._tables,
            start_at="now",
        )

    def _listen_op(
        self, timeout_ms: int, max_snapshots: int
    ) -> Callable[[], list[SchemaChangeRow]]:
        client = self._require_client()
        name = self._name

        def operation() -> list[SchemaChangeRow]:
            return client.cdc_ddl_changes_listen(
                name,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
            )

        return operation

    def _build_batch(self, rows: list[SchemaChangeRow]) -> DDLBatch:
        start = min(
            row.start_snapshot if row.start_snapshot is not None else row.snapshot_id
            for row in rows
        )
        end = max(
            row.end_snapshot if row.end_snapshot is not None else row.snapshot_id
            for row in rows
        )
        snapshot_ids = tuple(sorted({row.snapshot_id for row in rows}))
        changes = tuple(
            SchemaChange(
                event_kind=row.event_kind,
                object_kind=row.object_kind,
                snapshot_id=row.snapshot_id,
                snapshot_time=row.snapshot_time,
                schema_id=row.schema_id,
                schema_name=row.schema_name,
                object_id=row.object_id,
                object_name=row.object_name,
                details=row.details,
            )
            for row in rows
        )
        return DDLBatch(
            consumer_name=self._name,
            batch_id=DDLBatch.derive_batch_id(self._name, start, end),
            start_snapshot=start,
            end_snapshot=end,
            snapshot_ids=snapshot_ids,
            received_at=datetime.now(UTC),
            changes=changes,
        )


def _lease_is_alive(entry: ConsumerListEntry) -> bool:
    """Return ``True`` if the consumer entry still has a live lease holder.

    A row with no ``owner_token`` was never leased (or just had the lease
    cleared). When a token is present we look at the heartbeat: if the
    catalog's most recent heartbeat is older than
    ``lease_interval_seconds + grace`` we treat the lease as stale and let
    the next listen call reacquire it without surprising the caller.
    """

    if entry.owner_token is None:
        return False
    if entry.owner_heartbeat_at is None:
        return True

    interval = entry.lease_interval_seconds or 0
    grace = max(_LEASE_FRESHNESS_GRACE_SECONDS, float(interval) * 0.5)
    cutoff_age = float(interval) + grace
    now = datetime.now(UTC)
    heartbeat = entry.owner_heartbeat_at
    if heartbeat.tzinfo is None:
        heartbeat = heartbeat.replace(tzinfo=UTC)
    age = (now - heartbeat).total_seconds()
    return age <= cutoff_age


def _sink_name(sink: object) -> str:
    return str(getattr(sink, "name", type(sink).__name__))
