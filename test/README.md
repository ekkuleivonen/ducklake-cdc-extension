# Testing this extension
This directory contains the test surfaces for `ducklake_cdc`.

- `sql/` holds extension behavior tests written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html).
- `smoke/` holds extension smoke probes that need Python/C++ harnesses.
- `upstream/` holds DuckDB/DuckLake contract probes that do not load this extension.
- `client_py/` and `client_go/` are reserved for future client bindings.

The root makefile contains targets for the SQLLogicTests. To run them:
```bash
make test
```
or 
```bash
make test_debug
```

Local debug loops can use narrower targets:

```bash
make test_debug_smoke    # extension load + DuckLake catalog compatibility
make test_debug_default  # skips the three slowest integration files
make test_debug_full     # alias for the full debug SQL suite
```

The slowest debug files are the broad DuckLake integration fixtures
(`sql/always_breaks.test`, `sql/consumer_state.test`, and
`sql/sugar.test`), not intentional sleep/timeout assertions.

Run Python smoke probes after `make debug`:

```bash
cd test
uv run python smoke/compat_warning_smoke.py
uv run python smoke/lease_multiconn_smoke.py
uv run python smoke/cdc_wait_interrupt_smoke.py
uv run python smoke/toctou_expire_smoke.py
```

Run upstream DuckLake contract checks:

```bash
cd test
uv run python upstream/enumerate_changes_map.py --check
```
