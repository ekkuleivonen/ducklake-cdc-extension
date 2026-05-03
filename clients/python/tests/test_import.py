from ducklake import DuckLake, Result, Table, Transaction
from ducklake_cdc import (
    BaseDMLSink,
    Change,
    DMLBatch,
    DMLConsumer,
    DMLSink,
    SinkAck,
    SinkContext,
    StdoutDMLSink,
    __version__,
)
from ducklake_cdc.lowlevel import CDCClient


def test_package_imports() -> None:
    assert __version__
    assert DuckLake.__name__ == "DuckLake"
    assert Result.__name__ == "Result"
    assert Table.__name__ == "Table"
    assert Transaction.__name__ == "Transaction"

    assert DMLConsumer.__name__ == "DMLConsumer"
    assert StdoutDMLSink.__name__ == "StdoutDMLSink"
    assert BaseDMLSink.__name__ == "BaseDMLSink"
    assert Change.__name__ == "Change"
    assert DMLBatch.__name__ == "DMLBatch"
    assert SinkAck.__name__ == "SinkAck"
    assert SinkContext.__name__ == "SinkContext"
    assert DMLSink.__name__ == "DMLSink"
    assert CDCClient.__name__ == "CDCClient"
