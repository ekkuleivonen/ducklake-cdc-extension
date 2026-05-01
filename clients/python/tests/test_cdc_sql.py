from ducklake_cdc import Subscription
from ducklake_cdc.sql import table_function_sql


def test_subscription_list_renders_duckdb_struct_pack() -> None:
    sql = table_function_sql(
        "cdc_consumer_create",
        "lake",
        "orders_sink",
        named={
            "subscriptions": [
                Subscription.model_validate(
                    {
                        "scope_kind": "table",
                        "schema_name": "main",
                        "table_name": "orders",
                        "event_category": "dml",
                        "change_type": "*",
                    }
                )
            ],
            "stop_at_schema_change": True,
        },
    )

    assert sql == (
        "SELECT * FROM cdc_consumer_create('lake', 'orders_sink', "
        "subscriptions := [struct_pack(scope_kind := 'table', schema_name := 'main', "
        "table_name := 'orders', schema_id := NULL::BIGINT, table_id := NULL::BIGINT, "
        "event_category := 'dml', change_type := '*')], stop_at_schema_change := true)"
    )


def test_table_function_omits_none_named_arguments() -> None:
    assert table_function_sql(
        "cdc_consumer_stats",
        "lake",
        named={"consumer": None},
    ) == "SELECT * FROM cdc_consumer_stats('lake')"
