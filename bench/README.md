# Benchmarks

This is a fast smoke benchmark, not a soak test. The goal is to
prove the extension path is measurable without making every PR wait for
the benchmark path. The manual benchmark workflow downloads a
Full-CI-built extension artifact and runs it against the supported
official DuckDB release.

```sh
uv run --locked python bench/runner.py \
  --runtime official \
  --cdc-extension build/benchmark/extensions/ducklake_cdc.duckdb_extension \
  --workload bench/light.yaml
```

`bench/light.yaml` is intentionally short: a 60-second flat profile,
0.5 target snapshots/second, 100 target rows/snapshot, one consumer. The
runner accepts command-line overrides for the same fields so local
experiments can run faster:

```sh
uv run --locked python bench/runner.py \
  --build debug \
  --duration-seconds 10 \
  --target-snapshots-per-second 1
```

Results are written as JSON under `bench/results/` by default. The
top-level `result_schema_version` makes future result history comparable.
The schema separates the configured workload from observed measurements:

- `workload`: stable input envelope (`load_profile`, duration, target
  snapshots/second, target rows/snapshot, consumers, max snapshots).
- `measurements`: observed duration, produced/consumed counts, actual
  rates, end-to-end latency summary, CDC call counts, catalog QPS, lag,
  and per-operation timing summaries where the runtime can collect them.

Long-running soak tests, variable load profiles, and medium/heavy
workloads land in later phases once this harness has enough history to
trust.
