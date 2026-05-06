# `_lib/` &mdash; shared code across examples

Reusable building blocks that more than one example needs. **Not** a generic
utility dumping ground &mdash; if a piece of code is only ever called by one
example, it lives in that example's folder.

The leading underscore signals "internal to this directory tree, not a
top-level part of the suite."

## What belongs here

These are the planned residents (added as the corresponding examples are
built; not pre-populated):

- **`load.py`** &mdash; synthetic DML load generator. Lifted from the old
  `e2e/benchmark/producer.py`. Used by examples whose `--mode bench` needs
  controllable synthetic write traffic into a DuckLake table (01, 03, 04, 05).
  Exposes a small async API: drive N rows with a workload-shape profile, get
  back per-commit timing telemetry.
- **`stage.py`** &mdash; multi-stage pipeline runner. Composes
  `DMLConsumer` + `batch.transaction()` into a `Stage(name, source,
  transform, change_types)` dataclass and a `StageRunner` context
  manager that gives each stage one OS thread + one dedicated CDC
  connection, runs every batch's transform inside
  `batch.transaction()`, and starts stages serially to avoid the
  H-022 first-bootstrap race. First used by `01_pipeline_dag`;
  expected to be the substrate for any example that wires more than
  one consumer in a single process.
- **`sink_contract.py`** &mdash; the at-least-once + idempotency-key shape
  that examples 03 (Redis Streams) and 05 (Postgres serving) both implement.
  Defines a `Sink` protocol and a `commit_after_publish(window, sink)` driver
  so each example can focus on its sink-specific logic.
- **`tui.py`** &mdash; thin helpers on top of [`rich`](https://rich.readthedocs.io/)
  for the live-dashboard idioms every example needs (a `Live` wrapper that
  honors `--headless` by no-op'ing, a standard top-bar with elapsed time
  and exit hints, helpers for stage/lag panels). The *visualization
  choices* stay in each example's `app.py`; `_lib/tui.py` is the bedrock
  so individual examples don't reinvent the rich plumbing.

  **Decision: use a library, do not hand-roll.** The previous benchmark
  dashboard hand-rolled ANSI escapes, alt-screen mode, and panel sizing.
  As the TUI becomes more prominent (one bespoke dashboard per example,
  demo-quality, sized to talks), maintaining that by hand will cost more
  than the abstraction penalty of `rich`. `rich` is a small dep, has no
  event loop requirement, and renders cleanly to non-TTY when
  `--headless` is set. Escalate to [`textual`](https://textual.textualize.io/)
  only if an example needs interactive widgets (none currently planned).
- **`metrics.py`** &mdash; uniform JSON shape every example writes on clean
  shutdown to `./.results/<example>-<catalog>-<storage>.json`. Same file in
  TUI demo mode (Ctrl-C) and `--headless` CI mode &mdash; the data is the
  same; only the live rendering differs. Schema:
  ```json
  {
    "example": "01_pipeline_dag",
    "catalog": "postgres",
    "storage": "disk",
    "duration_s": 180.0,
    "throughput": { "rows_per_s": 12340.5 },
    "latency_ms": { "p50": 14.2, "p95": 41.0, "p99": 102.7 },
    "errors": 0
  }
  ```
  Per-example fields can be added under a `details` key.

## What does NOT belong here

- **Visualization code** that's specific to one example. Bespoke per example
  is the rule (see `e2e/README.md`).
- **Use-case logic.** If you find yourself moving "how to do reverse-ETL"
  into `_lib/`, stop &mdash; it belongs in `05_reverse_etl/app.py` so it
  reads as documentation.
- **Catalog or extension internals.** Those live in the `ducklake-client` /
  `ducklake-cdc-client` Python packages, not here.

## Status

Empty directory with this README only. Files are added as the first example
that needs them lands. The first concrete add will be `load.py` when the
synthetic load generator is migrated out of `e2e/benchmark/producer.py`
during the example 01 build.
