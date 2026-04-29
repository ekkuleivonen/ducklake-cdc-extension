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
cd test
uv run python smoke/compat_warning_smoke.py
uv run python smoke/lease_multiconn_smoke.py
uv run python smoke/cdc_wait_interrupt_smoke.py
uv run python smoke/toctou_expire_smoke.py
```
