"""Thread-safe in-memory metrics for the live TUI / headless summary.

No persistence -- the live display and the final stderr line are the
output. Examples that want JSON-on-shutdown can build it from
``snapshot()``.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from _lib.config import CatalogChoice, StorageChoice

# 100k samples is enough for stable p99 across a multi-hour run; older
# samples roll off in FIFO order.
DEFAULT_LATENCY_RESERVOIR = 100_000


@dataclass
class MetricsRecorder:
    """Thread-safe accumulator. Every method is safe to call from any thread."""

    example: str
    catalog: CatalogChoice
    storage: StorageChoice
    started_at: float = field(default_factory=time.monotonic)
    rows_processed: int = 0
    errors: int = 0
    details: dict[str, Any] = field(default_factory=dict)
    _latency_samples_ms: deque[float] = field(
        default_factory=lambda: deque(maxlen=DEFAULT_LATENCY_RESERVOIR)
    )
    _lock: Lock = field(default_factory=Lock)

    def record_rows(self, n: int) -> None:
        with self._lock:
            self.rows_processed += n

    def record_latency_ms(self, ms: float) -> None:
        with self._lock:
            self._latency_samples_ms.append(ms)

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1

    def set_detail(self, key: str, value: Any) -> None:
        with self._lock:
            self.details[key] = value

    def snapshot(self) -> dict[str, Any]:
        """Cheap, lock-held snapshot the TUI re-renders from."""
        with self._lock:
            return {
                "elapsed_s": time.monotonic() - self.started_at,
                "rows_processed": self.rows_processed,
                "errors": self.errors,
                "latency_count": len(self._latency_samples_ms),
                "latency_ms": _percentiles(list(self._latency_samples_ms)),
                "details": dict(self.details),
            }


def _percentiles(samples: list[float]) -> dict[str, float | None]:
    if not samples:
        return {"p50": None, "p95": None, "p99": None}
    samples = sorted(samples)
    n = len(samples)
    return {
        "p50": round(samples[max(0, n * 50 // 100 - 1)], 3),
        "p95": round(samples[max(0, n * 95 // 100 - 1)], 3),
        "p99": round(samples[max(0, n * 99 // 100 - 1)], 3),
    }
