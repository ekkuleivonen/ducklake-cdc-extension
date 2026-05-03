"""Python client for the ducklake-cdc DuckDB extension.

The headline surface is the high-level consumers plus sink protocols:

    from ducklake_cdc import DMLConsumer, StdoutDMLSink

    with DMLConsumer(lake, "orders", table="public.orders", sinks=[StdoutDMLSink()]) as c:
        c.run()

DDL events flow through the parallel :class:`DDLConsumer` / :class:`DDLSink`
surface. Built-in sinks (``Stdout*``, ``File*``, ``Memory*``, ``Callable*``)
and combinators (``Map*``, ``Filter*``, ``Fanout*``) are dependency-light;
network/IO sinks live in separate distributions.

The low-level 1:1 mirror of the SQL extension surface lives at
``ducklake_cdc.lowlevel.CDCClient``. Reach for it when you need raw access
to the extension's table functions; use the high-level consumers for the
listen + deliver + commit loop.
"""

from ducklake_cdc._version import __version__
from ducklake_cdc.combinators import (
    FanoutDDLSink,
    FanoutDMLSink,
    FilterDDLSink,
    FilterDMLSink,
    MapDDLSink,
    MapDMLSink,
)
from ducklake_cdc.consumer import DDLConsumer, DMLConsumer
from ducklake_cdc.enums import (
    ChangeType,
    DdlEventKind,
    DdlObjectKind,
    DiagnosticSeverity,
    EventCategory,
    ScopeKind,
    SubscriptionStatus,
)
from ducklake_cdc.sinks import (
    CallableDDLSink,
    CallableDMLSink,
    FileDDLSink,
    FileDMLSink,
    MemoryDDLSink,
    MemoryDMLSink,
    StdoutDDLSink,
    StdoutDMLSink,
)
from ducklake_cdc.types import (
    BaseDDLSink,
    BaseDMLSink,
    Change,
    DDLBatch,
    DDLSink,
    DMLBatch,
    DMLSink,
    SchemaChange,
    SinkAck,
    SinkContext,
)

__all__ = [
    "BaseDDLSink",
    "BaseDMLSink",
    "CallableDDLSink",
    "CallableDMLSink",
    "Change",
    "ChangeType",
    "DDLBatch",
    "DDLConsumer",
    "DDLSink",
    "DdlEventKind",
    "DdlObjectKind",
    "DiagnosticSeverity",
    "DMLBatch",
    "DMLConsumer",
    "DMLSink",
    "EventCategory",
    "FanoutDDLSink",
    "FanoutDMLSink",
    "FileDDLSink",
    "FileDMLSink",
    "FilterDDLSink",
    "FilterDMLSink",
    "MapDDLSink",
    "MapDMLSink",
    "MemoryDDLSink",
    "MemoryDMLSink",
    "SchemaChange",
    "ScopeKind",
    "SinkAck",
    "SinkContext",
    "StdoutDDLSink",
    "StdoutDMLSink",
    "SubscriptionStatus",
    "__version__",
]
