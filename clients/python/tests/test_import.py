from ducklake import DuckLake, Result, Table, Transaction
from ducklake_cdc import (
    BaseDDLSink,
    BaseDMLSink,
    CallableDDLSink,
    CallableDMLSink,
    Change,
    DDLBatch,
    DDLConsumer,
    DDLSink,
    DMLBatch,
    DMLConsumer,
    DMLSink,
    FanoutDDLSink,
    FanoutDMLSink,
    FileDDLSink,
    FileDMLSink,
    FilterDDLSink,
    FilterDMLSink,
    MapDDLSink,
    MapDMLSink,
    MemoryDDLSink,
    MemoryDMLSink,
    SchemaChange,
    SinkAck,
    SinkContext,
    StdoutDDLSink,
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
    assert DDLConsumer.__name__ == "DDLConsumer"
    assert StdoutDMLSink.__name__ == "StdoutDMLSink"
    assert StdoutDDLSink.__name__ == "StdoutDDLSink"
    assert FileDMLSink.__name__ == "FileDMLSink"
    assert FileDDLSink.__name__ == "FileDDLSink"
    assert MemoryDMLSink.__name__ == "MemoryDMLSink"
    assert MemoryDDLSink.__name__ == "MemoryDDLSink"
    assert CallableDMLSink.__name__ == "CallableDMLSink"
    assert CallableDDLSink.__name__ == "CallableDDLSink"
    assert MapDMLSink.__name__ == "MapDMLSink"
    assert MapDDLSink.__name__ == "MapDDLSink"
    assert FilterDMLSink.__name__ == "FilterDMLSink"
    assert FilterDDLSink.__name__ == "FilterDDLSink"
    assert FanoutDMLSink.__name__ == "FanoutDMLSink"
    assert FanoutDDLSink.__name__ == "FanoutDDLSink"
    assert BaseDMLSink.__name__ == "BaseDMLSink"
    assert BaseDDLSink.__name__ == "BaseDDLSink"
    assert Change.__name__ == "Change"
    assert SchemaChange.__name__ == "SchemaChange"
    assert DMLBatch.__name__ == "DMLBatch"
    assert DDLBatch.__name__ == "DDLBatch"
    assert SinkAck.__name__ == "SinkAck"
    assert SinkContext.__name__ == "SinkContext"
    assert DMLSink.__name__ == "DMLSink"
    assert DDLSink.__name__ == "DDLSink"
    assert CDCClient.__name__ == "CDCClient"
