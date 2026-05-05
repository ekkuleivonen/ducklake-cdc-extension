"""User-defined CDC sinks for the benchmark consumer."""

from .dashboard import DemoDashboard, DemoSink
from .stats import StatsSink

__all__ = ["DemoDashboard", "DemoSink", "StatsSink"]
