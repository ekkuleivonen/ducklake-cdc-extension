# Smoke Tests

This directory contains extension smoke tests that need a richer harness than
SQLLogicTest provides, such as stderr notice assertions, multiple concurrent
connections, and explicit DuckDB connection interrupts.

Build the debug extension first:

```bash
make debug
```

Run all smoke probes from the shared test Python project:

```bash
uv run python test/smoke/compat_warning_smoke.py
uv run python test/smoke/lease_multiconn_smoke.py
uv run python test/smoke/cdc_wait_interrupt_smoke.py
uv run python test/smoke/toctou_expire_smoke.py
```
