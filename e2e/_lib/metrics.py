"""Uniform metrics shape every example writes on clean shutdown.

Same JSON file in TUI demo (Ctrl-C) and ``--headless`` CI runs -- the
data is the same; only the live rendering differs. Release tooling
collects ``./.results/*.json`` across the matrix without per-example
parsing.

Schema (also reproduced in ``e2e/_lib/README.md``):

    {
      "example": "01_pipeline_dag",
      "catalog": "postgres",
      "storage": "disk",
      "duration_s": 180.0,
      "throughput": { "rows_per_s": 12340.5 },
      "latency_ms": { "p50": 14.2, "p95": 41.0, "p99": 102.7 },
      "errors": 0,
      "details": { ... example-specific keys ... }
    }
"""

from __future__ import annotations

import json
import os
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any

from _lib.config import E2E_DIR, CatalogChoice, StorageChoice

# .results/ lives at the e2e/ root so a single CI step can collect across
# every example without globbing into per-example folders. Gitignored.
RESULTS_DIR = E2E_DIR / ".results"

# Bound the latency reservoir so a multi-hour demo session doesn't grow
# unboundedly. 100k samples is enough for stable p99 across any
# realistic example duration; older samples roll off.
DEFAULT_LATENCY_RESERVOIR = 100_000


@dataclass
class MetricsRecorder:
    """Thread-safe accumulator for per-example metrics.

    Every counter / sample method is safe to call from any thread.
    ``finalize_and_write`` should be called once on clean shutdown.
    """

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

    # ----- read-side helpers used by the live TUI ---------------------------

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

    # ----- finalisation -----------------------------------------------------

    def finalize_and_write(self, *, results_dir: Path = RESULTS_DIR) -> Path:
        """Compute final percentiles, write atomically to ``.results/<key>.json``, return the path.

        Safe to call once at the very end. Atomic via temp-file + rename
        so a partial write never overwrites a previous result if the
        process is killed during finalisation.
        """
        with self._lock:
            duration_s = max(time.monotonic() - self.started_at, 1e-9)
            samples = list(self._latency_samples_ms)
            payload = {
                "example": self.example,
                "catalog": self.catalog,
                "storage": self.storage,
                "duration_s": round(duration_s, 3),
                "throughput": {
                    "rows_per_s": round(self.rows_processed / duration_s, 1),
                    "rows_total": self.rows_processed,
                },
                "latency_ms": _percentiles(samples),
                "errors": self.errors,
                "details": dict(self.details),
            }
        return _atomic_write_json(payload, results_dir, self._key())

    def _key(self) -> str:
        return f"{self.example}-{self.catalog}-{self.storage}"


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


def _atomic_write_json(payload: dict[str, Any], results_dir: Path, key: str) -> Path:
    """Write ``payload`` to ``<results_dir>/<key>.json`` atomically.

    Uses ``os.replace`` after a temp-file write -- on POSIX this is an
    atomic rename, so concurrent readers (or a hard kill mid-write)
    never see a partial file.
    """
    results_dir.mkdir(parents=True, exist_ok=True)
    final_path = results_dir / f"{key}.json"
    tmp_path = results_dir / f"{key}.json.tmp.{os.getpid()}"
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp_path, final_path)
    return final_path
