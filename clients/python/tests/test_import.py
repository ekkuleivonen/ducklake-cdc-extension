from ducklake import DuckLake, Result, Table
from ducklake_cdc import __version__


def test_package_imports() -> None:
    assert __version__
    assert DuckLake.__name__ == "DuckLake"
    assert Result.__name__ == "Result"
    assert Table.__name__ == "Table"
