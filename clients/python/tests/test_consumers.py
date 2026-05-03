"""Construction-time tests for high-level consumers.

These tests cover validation, lease-policy parsing, and lease-freshness
detection. The full listen+commit loop sits behind the SQL extension and
is exercised by the demo / integration suite.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from ducklake import DuckLake
from ducklake_cdc import (
    DDLConsumer,
    DMLConsumer,
    StdoutDDLSink,
    StdoutDMLSink,
)
from ducklake_cdc.consumer import _lease_is_alive
from ducklake_cdc.lowlevel import ConsumerListEntry


class _FakeLake:
    """Stand-in :class:`ducklake.DuckLake` that never issues SQL.

    Construction is the only thing under test in this file, so we never
    need a working ``sql`` method.
    """

    alias = "lake"

    def sql(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("DuckLake.sql should not be called in these tests")


def _fake_lake() -> DuckLake:
    return cast(DuckLake, _FakeLake())


class _FakeClient:
    def __init__(self) -> None:
        self.list_calls = 0
        self.created = 0

    def cdc_list_consumers(self) -> list[ConsumerListEntry]:
        self.list_calls += 1
        if self.list_calls == 1:
            raise RuntimeError("transient setup failure")
        return []

    def cdc_dml_consumer_create(self, *_args: Any, **_kwargs: Any) -> None:
        self.created += 1


def test_dml_consumer_requires_at_least_one_sink() -> None:
    with pytest.raises(ValueError, match="at least one sink"):
        DMLConsumer(_fake_lake(), "name", table="t", sinks=[])


def test_ddl_consumer_requires_at_least_one_sink() -> None:
    with pytest.raises(ValueError, match="at least one sink"):
        DDLConsumer(_fake_lake(), "name", sinks=[])


def test_dml_consumer_requires_non_empty_name() -> None:
    with pytest.raises(ValueError, match="non-empty name"):
        DMLConsumer(_fake_lake(), "", table="t", sinks=[StdoutDMLSink()])


def test_ddl_consumer_requires_non_empty_name() -> None:
    with pytest.raises(ValueError, match="non-empty name"):
        DDLConsumer(_fake_lake(), "", sinks=[StdoutDDLSink()])


def test_dml_consumer_rejects_unknown_lease_policy() -> None:
    with pytest.raises(ValueError, match="lease_policy"):
        DMLConsumer(
            _fake_lake(),
            "n",
            table="t",
            sinks=[StdoutDMLSink()],
            lease_policy="grab",  # type: ignore[arg-type]
        )


def test_dml_consumer_rejects_negative_lease_wait_timeout() -> None:
    with pytest.raises(ValueError, match="lease_wait_timeout"):
        DMLConsumer(
            _fake_lake(),
            "n",
            table="t",
            sinks=[StdoutDMLSink()],
            lease_wait_timeout=-1.0,
        )


def test_dml_consumer_requires_exactly_one_table_input() -> None:
    """The contract is one DML consumer = one table; the bind enforces
    exactly one of `table` / `table_id`. We mirror that in the
    constructor so the error surfaces synchronously before sinks open.
    """

    with pytest.raises(ValueError, match="exactly one of table"):
        DMLConsumer(_fake_lake(), "n", sinks=[StdoutDMLSink()])
    with pytest.raises(ValueError, match="exactly one of table"):
        DMLConsumer(
            _fake_lake(),
            "n",
            table="t",
            table_id=42,
            sinks=[StdoutDMLSink()],
        )


def test_consumer_enter_applies_retry_policy_to_setup() -> None:
    client = _FakeClient()
    attempts = 0

    def retry(operation: Any) -> Any:
        nonlocal attempts
        while True:
            attempts += 1
            try:
                return operation()
            except RuntimeError:
                if attempts >= 2:
                    raise

    consumer = DMLConsumer(
        _fake_lake(),
        "retry-setup",
        table="demo_schema.events",
        sinks=[StdoutDMLSink()],
        client=cast(Any, client),
        retry=retry,
    )

    with consumer:
        pass

    assert attempts == 2
    assert client.list_calls == 3
    assert client.created == 1


def test_lease_is_alive_treats_missing_token_as_free() -> None:
    entry = ConsumerListEntry(
        consumer_name="x",
        consumer_kind="dml",
        consumer_id=1,
        owner_token=None,
    )

    assert _lease_is_alive(entry) is False


def test_lease_is_alive_treats_recent_heartbeat_as_held() -> None:
    from uuid import uuid4

    entry = ConsumerListEntry(
        consumer_name="x",
        consumer_kind="dml",
        consumer_id=1,
        owner_token=uuid4(),
        owner_heartbeat_at=datetime.now(UTC) - timedelta(seconds=1),
        lease_interval_seconds=10,
    )

    assert _lease_is_alive(entry) is True


def test_lease_is_alive_treats_stale_heartbeat_as_free() -> None:
    from uuid import uuid4

    entry = ConsumerListEntry(
        consumer_name="x",
        consumer_kind="dml",
        consumer_id=1,
        owner_token=uuid4(),
        owner_heartbeat_at=datetime.now(UTC) - timedelta(seconds=120),
        lease_interval_seconds=10,
    )

    assert _lease_is_alive(entry) is False
