import pytest

from ducklake._connection import _setting_sql
from ducklake.exceptions import DuckLakeConnectionError


def test_setting_sql_mirrors_duckdb_set_syntax() -> None:
    assert _setting_sql("threads", 4) == "SET threads = 4"
    assert _setting_sql("memory_limit", "4GB") == "SET memory_limit = '4GB'"
    assert _setting_sql("enable_object_cache", True) == "SET enable_object_cache = true"


def test_setting_sql_rejects_non_setting_names() -> None:
    with pytest.raises(DuckLakeConnectionError):
        _setting_sql("threads; DROP TABLE x", 4)
