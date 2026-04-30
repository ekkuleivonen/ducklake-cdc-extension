# Development

Short local loop for working on the C++ extension.

## First setup

```bash
git submodule update --init --recursive
```

The root `Makefile` includes `extension-ci-tools/makefiles/duckdb_extension.Makefile`. The `duckdb/` and `extension-ci-tools/` directories are Git submodules pinned to versions compatible with this extension.

## Supported DuckDB targets

The active development target is **DuckDB v1.5.1**. Keep the version tuple
explicit:

- `duckdb/` submodule: DuckDB `v1.5.1`
- `extension-ci-tools/` submodule and reusable workflows: `v1.5.1`
- `extension_config.cmake`: a pinned DuckLake commit known to compile against
  DuckDB `v1.5.1`

DuckDB extension binaries are version-specific, so "supported" means a full
tuple has been validated. Do not mark another DuckDB version supported just
because the C++ compiles locally.

### Validating an older DuckDB version

Add older DuckDB targets only after the active target is green. For a candidate
version `XX`:

1. Create a short-lived branch from `main`, for example
   `feature_support_duckdb_XX`.
2. Pin `duckdb/` to DuckDB `XX` and `extension-ci-tools/` to the matching
   `XX` branch documented by DuckDB's extension CI tools.
3. Pin `extension_config.cmake` to a DuckLake commit that compiles against
   DuckDB `XX`; never leave this dependency floating.
4. Run a clean local build and test loop:
   `make clean`, `make debug`, `make test_debug`, and `make test`.
5. Exercise the loadable extension in the built DuckDB shell:
   `LOAD ducklake; LOAD parquet; LOAD ducklake_cdc;`, then attach a real
   DuckLake catalog and run the CDC smoke path.
6. Run the Python smoke probes in `test/smoke/` for behaviours SQLLogicTest
   cannot express, especially notices, leases, waits, and backend-specific
   catalog behaviour. Run the upstream probes in `test/upstream/` when the
   DuckLake dependency changes.
7. Add a CI matrix leg for `XX` and require full CI to pass on the branch.
8. Update the compatibility docs and release notes with the exact tuple:
   DuckDB `XX`, extension-ci-tools `XX`, DuckLake commit, observed DuckLake
   catalog format, supported platforms, and any caveats.

Only after those steps pass should `XX` appear in a supported-version matrix.
If the candidate needs source changes, keep them behind the same validation
branch and merge them through the normal PR gate.

## Build

```bash
make debug
```

Configures and builds DuckDB plus the `ducklake_cdc` extension under `build/debug/`.

Use the bundled DuckDB binary for ad-hoc SQL:

```bash
./build/debug/duckdb
```

One-shot example:

```bash
./build/debug/duckdb -unsigned -c "SELECT cdc_version();"
```

(`-unsigned` matches typical local extension loads; adjust if your setup differs.)

## Test

Run all SQLLogicTests against the **debug** build:

```bash
make test_debug
```

Against the **release** build:

```bash
make test
```

Run a single SQLLogicTest file:

```bash
build/debug/test/unittest --test-dir . "test/sql/ducklake_cdc.test"
```

Swap the path for any file under `test/sql/`.

## Formatting

CI runs the DuckDB extension-template formatter gate:

```bash
make format-check
```

Apply the same formatter locally before committing:

```bash
make format-fix
```

The formatter requires `clang-format 11.0.1` on `PATH` and the Python
formatter dependencies used by DuckDB's `duckdb/scripts/format.py`.

To enable the local pre-commit hook:

```bash
make install-git-hooks
```

The hook runs `make format-check` with a formatter toolchain bootstrapped into
the ignored `.cache/pre-commit/` directory. That venv provides
`clang_format==11.0.1`, `black==24.10.0`, and `cmake-format`, so the check does
not depend on the developer's system `clang-format` version.

## Python smoke and upstream probes

Some behaviours are easier to smoke-test from Python (stderr notices,
multi-connection leases, explicit interrupts, etc.). Dependencies live in the root `pyproject.toml`; run from the repository root:

```bash
uv run python test/smoke/lease_multiconn_smoke.py
uv run python test/upstream/enumerate_changes_map.py --check
```

See [`test/smoke/README.md`](../test/smoke/README.md) and
[`test/upstream/README.md`](../test/upstream/README.md) for the full lists.

## Branch and CI flow

The branch flow is:

```text
feature_* -> main -> manual release
```

- Work on a short-lived branch created from `main`.
- Feature branches run day-to-day CI for fast feedback.
- Open the PR into `main`.
- `main` is protected and requires the required CI gate before merge.
- Releases are manual actions from `main` or the relevant maintenance branch.
- `release/0.x` branches are created only when an already-published line needs
  patch maintenance after `main` has moved on.

## Where to start

- `src/ducklake_cdc_extension.cpp` is the extension entry point. It registers
  `cdc_version()` and calls into the per-domain `Register*Functions`.
- `src/include/ducklake_metadata.hpp` is the shared facts layer (catalog
  table-name builders, snapshot lookups, JSON / quoting helpers, state-table
  DDL, lazy bootstrap). `src/ducklake_metadata.cpp` implements it.
- `src/include/consumer.hpp` + `src/consumer.cpp` own the consumer state
  machine (create / reset / drop / list / force-release / heartbeat) and the
  cursor primitives (cdc_window / cdc_commit / cdc_wait), plus the lease,
  audit-writer, and notice helpers.
- `src/include/ddl.hpp` + `src/ddl.cpp` own the schema-change reads
  (cdc_ddl, cdc_recent_ddl, cdc_schema_diff) and the per-snapshot DDL
  extraction / column-diff machinery.
- `src/include/dml.hpp` + `src/dml.cpp` own the row-level reads
  (cdc_events, cdc_changes, cdc_recent_changes).
- `src/include/stats.hpp` + `src/stats.cpp` own the observability surface
  (cdc_consumer_stats, cdc_audit_recent).
- `test/sql/ducklake_cdc.test` is the smallest extension smoke test.
- `test/sql/consumer_state.test` and `test/sql/sugar.test` are the best
  behavioural specs for the cursor and sugar surfaces.
- `test/smoke/README.md` documents Python smoke tools that do not fit
  SQLLogicTest well.
- `test/upstream/README.md` documents DuckLake compatibility probes that pin
  upstream behaviour this extension depends on.

For a minimal SQL walk-through of primitives, use the quickstart block in the
root [`README.md`](../README.md).
