# `_lib/` &mdash; shared code across examples

Reusable building blocks that more than one example needs. **Not** a generic
utility dumping ground &mdash; if a piece of code is only ever called by one
example, it lives in that example's folder.

The leading underscore signals "internal to this directory tree, not a
top-level part of the suite."

## Modules

- **`config.py`** &mdash; lake / catalog / storage construction and
  `reset_lake` (drops catalog + storage + load corpora). Loads
  `e2e/.env` on import.
- **`cli.py`** &mdash; the suite-standard flag set
  (`--headless`, `--catalog`, `--storage`, `--duration`).
- **`load.py`** &mdash; synthetic load harness with **pre-built parquet
  corpora**. Frame this as test harness, not system under test: an
  example declares one `LoadShape` per source table, builds a
  `LoadCorpus` once at startup (synthesis happens here, idempotently,
  under `e2e/.work/<example>/load/`), and then either calls `prime`
  (one big load) or `replay` (paced). The hot loop is just
  `INSERT INTO ... SELECT * FROM read_parquet('...')` so producers
  use minimum catalog-commit budget and don't contaminate
  consumer-side measurements.
- **`stage.py`** &mdash; multi-stage pipeline runner. `Stage(name,
  source, transform, change_types)` + `StageRunner` context manager;
  one OS thread + one dedicated CDC connection per stage, transforms
  run inside `batch.transaction()`, stages start serially to avoid
  the H-022 first-bootstrap race.
- **`metrics.py`** &mdash; thread-safe in-memory counters + percentiles
  for the live TUI / headless summary. No persistence; the displayed
  / printed numbers are the output.
- **`tui.py`** &mdash; thin `rich` wrapper. `LiveDisplay` honors
  `--headless` by emitting a periodic stderr summary instead of the
  TUI; `log()` is the thread-safe stderr line every example uses.

## What does NOT belong here

- Visualization code specific to one example (bespoke per example).
- Use-case logic (lives in the example, so it reads as documentation).
- Catalog or extension internals (lives in the `ducklake-client` /
  `ducklake-cdc-client` Python packages).
