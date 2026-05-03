"""Built-in sinks for the high-level CDC client.

Network and IO integrations live in separate distributions
(``ducklake-cdc-redis``, ``ducklake-cdc-postgres``). The core library ships
only sinks that depend on nothing outside the Python standard library.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from typing import IO, Any

from ducklake_cdc.types import BaseDMLSink, DMLBatch, SinkContext


class StdoutDMLSink(BaseDMLSink):
    """Emit each batch as JSON lines to stdout.

    The sink emits three line types per batch:

    - ``{"type": "window", "start_snapshot": ..., "end_snapshot": ...}``
      once at the start of the batch, with the batch identity.
    - ``{"type": "change", ...}`` once per :class:`ducklake_cdc.Change`.
    - ``{"type": "commit", "snapshot": ...}`` once at the end. Note: this
      line is emitted by the sink before the consumer commits to the
      extension. It means "this batch is acked; the consumer is about to
      commit at this snapshot".

    Pass ``stream`` to redirect output (e.g. for tests). The default is
    ``sys.stdout``.
    """

    name = "stdout"
    require_ack = True

    def __init__(self, *, stream: IO[str] | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        self._emit(
            {
                "type": "window",
                "consumer": batch.consumer_name,
                "batch_id": batch.batch_id,
                "start_snapshot": batch.start_snapshot,
                "end_snapshot": batch.end_snapshot,
                "snapshot_ids": list(batch.snapshot_ids),
                "received_at": batch.received_at.isoformat(),
                "change_count": len(batch),
            }
        )
        for change in batch:
            payload = change.to_dict()
            payload["type"] = "change"
            self._emit(payload)
        self._emit(
            {
                "type": "commit",
                "consumer": batch.consumer_name,
                "batch_id": batch.batch_id,
                "snapshot": batch.end_snapshot,
            }
        )

    def _emit(self, payload: dict[str, Any]) -> None:
        line = json.dumps(payload, default=_json_default, sort_keys=True)
        self._stream.write(line + "\n")
        self._stream.flush()


def _json_default(value: object) -> str:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)
