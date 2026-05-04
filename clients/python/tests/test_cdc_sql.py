from ducklake_cdc.sql import table_function_sql


def test_named_arguments_render_duckdb_literals() -> None:
    """A scalar `table_name` plus a list `change_types` exercises both
    the scalar and the list-named-parameter rendering paths.
    """

    sql = table_function_sql(
        "cdc_dml_consumer_create",
        "lake",
        "orders_sink",
        named={
            "table_name": "orders",
            "change_types": ["insert", "delete"],
        },
    )

    assert sql == (
        "SELECT * FROM cdc_dml_consumer_create('lake', 'orders_sink', "
        "table_name := 'orders', change_types := ['insert', 'delete'])"
    )


def test_table_function_omits_none_named_arguments() -> None:
    assert table_function_sql(
        "cdc_consumer_stats",
        "lake",
        named={"consumer": None},
    ) == "SELECT * FROM cdc_consumer_stats('lake')"
