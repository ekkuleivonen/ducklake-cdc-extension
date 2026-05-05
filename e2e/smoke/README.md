# Smoke tests

Extension smoke tests that need a richer harness than SQLLogicTest (stderr
notice assertions, multiple concurrent connections, DuckDB interrupts,
optional C++ multiconn harnesses).

Build the release extension first (and `make prepare_tests` for probes that
load the official DuckLake artifact):

```bash
make release
make prepare_tests
```

Run CI’s Python probes from the repo root (`ducklake-cdc-tests` uv project):

```bash
uv run python e2e/smoke/compat_warning_smoke.py
uv run python e2e/smoke/lease_multiconn_smoke.py
uv run python e2e/smoke/cdc_wait_interrupt_smoke.py
uv run python e2e/smoke/toctou_expire_smoke.py
```

Optional adaptive listen probe (uses `ducklake-client` for attach):

```bash
uv run python e2e/smoke/adaptive_listen_coalesce_smoke.py
```
