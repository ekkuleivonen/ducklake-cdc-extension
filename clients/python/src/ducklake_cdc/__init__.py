"""Python client for the ducklake-cdc DuckDB extension.

The headline surface is the high-level consumer plus a sink protocol:

    from ducklake_cdc import DMLConsumer, StdoutDMLSink

    with DMLConsumer(lake, "orders", tables=["public.orders"], sinks=[StdoutDMLSink()]) as c:
        c.run()

The low-level 1:1 mirror of the SQL extension surface lives at
``ducklake_cdc.lowlevel.CDCClient``. Reach for it when you need raw access
to the extension's table functions; use the high-level consumer for the
listen + deliver + commit loop.
"""

from ducklake_cdc._version import __version__
from ducklake_cdc.consumer import DMLConsumer
from ducklake_cdc.enums import (
    ChangeType,
    DdlEventKind,
    DdlObjectKind,
    DiagnosticSeverity,
    EventCategory,
    ScopeKind,
    SubscriptionStatus,
)
from ducklake_cdc.sinks import StdoutDMLSink
from ducklake_cdc.types import (
    BaseDMLSink,
    Change,
    DMLBatch,
    DMLSink,
    SinkAck,
    SinkContext,
)

__all__ = [
    "BaseDMLSink",
    "Change",
    "ChangeType",
    "DdlEventKind",
    "DdlObjectKind",
    "DiagnosticSeverity",
    "DMLBatch",
    "DMLConsumer",
    "DMLSink",
    "EventCategory",
    "ScopeKind",
    "SinkAck",
    "SinkContext",
    "StdoutDMLSink",
    "SubscriptionStatus",
    "__version__",
]
