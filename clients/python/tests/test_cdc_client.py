from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from ducklake import DuckLake
from ducklake_cdc import CDCClient, Subscription


class FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def list(self) -> list[dict[str, Any]]:
        return self._rows

    def one(self) -> dict[str, Any]:
        if len(self._rows) != 1:
            raise AssertionError(f"expected one row, got {len(self._rows)}")
        return self._rows[0]

    def scalar(self) -> Any:
        return next(iter(self.one().values()))


class FakeConnection:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, sql: str) -> None:
        self.calls.append(sql)


class FakeLake:
    alias = "lake"

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.results: dict[str, list[dict[str, Any]]] = {}
        self.connection = FakeConnection()

    def sql(self, query: str) -> FakeResult:
        self.calls.append(query)
        return FakeResult(self.results.get(query, []))

    def raw_connection(self) -> FakeConnection:
        return self.connection


def test_client_load_extension_is_explicit() -> None:
    lake = FakeLake()

    CDCClient(cast(DuckLake, lake)).load_extension()

    assert lake.connection.calls == ["INSTALL ducklake_cdc", "LOAD ducklake_cdc"]


def test_client_load_extension_accepts_local_path() -> None:
    lake = FakeLake()

    CDCClient(cast(DuckLake, lake)).load_extension(path="/tmp/ducklake_cdc.duckdb_extension")

    assert lake.connection.calls == ["LOAD '/tmp/ducklake_cdc.duckdb_extension'"]


def test_consumer_create_returns_subscription_models_and_renders_sql() -> None:
    lake = FakeLake()
    cdc = CDCClient(cast(DuckLake, lake))
    subscription = Subscription.model_validate(
        {
            "scope_kind": "table",
            "schema_name": "main",
            "table_name": "orders",
            "event_category": "dml",
            "change_type": "*",
        }
    )
    expected_sql = (
        "SELECT * FROM cdc_consumer_create('lake', 'orders_sink', start_at := 'now', "
        "subscriptions := [struct_pack(scope_kind := 'table', schema_name := 'main', "
        "table_name := 'orders', schema_id := NULL::BIGINT, table_id := NULL::BIGINT, "
        "event_category := 'dml', change_type := '*')], stop_at_schema_change := true)"
    )
    lake.results[expected_sql] = [
        {
            "consumer_name": "orders_sink",
            "consumer_id": 1,
            "last_committed_snapshot": 7,
            "subscription_id": 1,
            "scope_kind": "table",
            "schema_id": 0,
            "table_id": 5,
            "event_category": "dml",
            "change_type": "*",
            "original_qualified_name": "main.orders",
            "current_qualified_name": "main.orders",
        }
    ]

    rows = cdc.consumer_create("orders_sink", subscriptions=[subscription])

    assert lake.calls == [expected_sql]
    assert rows[0].consumer_name == "orders_sink"
    assert rows[0].current_qualified_name == "main.orders"


def test_cursor_methods_return_fixed_models() -> None:
    lake = FakeLake()
    cdc = CDCClient(cast(DuckLake, lake))
    window_sql = "SELECT * FROM cdc_window('lake', 'orders_sink', max_snapshots := 50)"
    commit_sql = "SELECT * FROM cdc_commit('lake', 'orders_sink', 42)"
    lake.results[window_sql] = [
        {
            "start_snapshot": 7,
            "end_snapshot": 42,
            "has_changes": True,
            "schema_version": 3,
            "schema_changes_pending": False,
        }
    ]
    lake.results[commit_sql] = [
        {
            "consumer_name": "orders_sink",
            "committed_snapshot": 42,
            "schema_version": 3,
        }
    ]

    window = cdc.window("orders_sink", max_snapshots=50)
    commit = cdc.commit("orders_sink", window.end_snapshot)

    assert window.end_snapshot == 42
    assert commit.committed_snapshot == 42


def test_dynamic_change_methods_return_result_or_change_rows() -> None:
    lake = FakeLake()
    cdc = CDCClient(cast(DuckLake, lake))
    expected_sql = (
        "SELECT * FROM cdc_changes('lake', 'orders_sink', table_name := 'orders', "
        "max_snapshots := 100)"
    )
    lake.results[expected_sql] = [
        {
            "snapshot_id": 42,
            "rowid": 0,
            "change_type": "insert",
            "id": 2,
            "status": "paid",
            "snapshot_time": datetime(2026, 4, 30, tzinfo=UTC),
            "next_snapshot_id": 43,
        }
    ]

    result = cdc.changes("orders_sink", table_name="orders")
    changes = cdc.changes_rows("orders_sink", table_name="orders")

    assert result.list()[0]["status"] == "paid"
    assert changes[0].values == {"id": 2, "status": "paid"}


def test_observability_methods_return_models() -> None:
    lake = FakeLake()
    cdc = CDCClient(cast(DuckLake, lake))
    now = datetime(2026, 4, 30, tzinfo=UTC)
    stats_sql = "SELECT * FROM cdc_consumer_stats('lake', consumer := 'orders_sink')"
    doctor_sql = "SELECT * FROM cdc_doctor('lake')"
    lake.results[stats_sql] = [
        {
            "consumer_name": "orders_sink",
            "consumer_id": 1,
            "last_committed_snapshot": 42,
            "current_snapshot": 43,
            "lag_snapshots": 1,
            "lag_seconds": 12.4,
            "oldest_available_snapshot": 5,
            "gap_distance": 37,
            "subscription_count": 1,
            "subscriptions_active": 1,
            "subscriptions_renamed": 0,
            "subscriptions_dropped": 0,
            "owner_token": None,
            "owner_acquired_at": now,
            "owner_heartbeat_at": now,
            "lease_interval_seconds": 30,
            "lease_alive": True,
        }
    ]
    lake.results[doctor_sql] = [
        {
            "severity": "info",
            "code": "CDC_METADATA_OK",
            "consumer_name": None,
            "message": "CDC metadata tables are present",
            "details": None,
        }
    ]

    assert cdc.consumer_stats(consumer="orders_sink")[0].lag_snapshots == 1
    assert cdc.doctor()[0].code == "CDC_METADATA_OK"
