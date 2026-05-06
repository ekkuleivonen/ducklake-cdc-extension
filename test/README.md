# Testing this extension
This directory contains the test surfaces for `ducklake_cdc`.

- Extension [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) live as `test/*.test`.
- `../e2e/smoke/` holds extension smoke probes that need Python/C++ harnesses.
- `../e2e/ci_demo_assertions.py` runs the user-facing demos as correctness and
  conservative performance gates.
- `../e2e/upstream/` holds DuckDB/DuckLake contract probes that do not load this extension.
- `client_py/` is reserved for a future Python client.

The root makefile contains local targets for the two useful loops:
```bash
make test_local_full       # full SQLLogicTest coverage, release build
make test_local_sanitizer  # ASan/UBSan smoke, no DuckLake LOAD
```

The default/release SQL set intentionally includes every `test/*.test` file
so PR release CI exercises the full extension surface. Debug builds enable
ASan/UBSan; they can run only the no-DuckLake smoke because the official
prebuilt `ducklake.duckdb_extension` is not sanitizer-compatible.

Run Python smoke probes after `make debug`:

```bash
uv run python e2e/smoke/compat_warning_smoke.py
uv run python e2e/smoke/lease_multiconn_smoke.py
uv run python e2e/smoke/cdc_wait_interrupt_smoke.py
uv run python e2e/smoke/toctou_expire_smoke.py
```

Run the demo CI gates after `make release`:

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
uv run --project e2e python e2e/ci_demo_assertions.py
docker compose -f e2e/docker-compose.yml down -v
```

Run upstream DuckLake contract checks:

```bash
uv run python e2e/upstream/enumerate_changes_map.py --check
```
