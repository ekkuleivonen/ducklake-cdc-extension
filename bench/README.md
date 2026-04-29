# Benchmarks

Phase 1 ships a fast smoke benchmark, not a soak test. The goal is to
prove the extension path is measurable on every CI run:

```sh
make release
uv run --locked python bench/runner.py --build release --workload bench/light.yaml
```

`bench/light.yaml` is intentionally short: 60 seconds, 30 snapshots/min,
100 rows/snapshot, one consumer. The runner accepts command-line
overrides for the same fields so local experiments can run faster:

```sh
uv run --locked python bench/runner.py --build debug --duration-seconds 10
```

Results are written as JSON under `bench/results/` by default. CI also
uploads the result as an artifact. Long-running soak tests, variable load
profiles, and medium/heavy workloads land in later phases once this
harness has enough history to trust.
