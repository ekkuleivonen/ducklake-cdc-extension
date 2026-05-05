# E2E Benchmark

This benchmark runs the published Python packages (`ducklake-client` and
`ducklake-cdc-client`) against a locally built `ducklake_cdc` extension artifact.
It preserves the former Python demo's analytics summary and live dashboard, but
lives under `e2e/` because it exercises the shipped extension plus packaged
clients end to end.

Start shared E2E dependencies (**same compose file** for benchmark + catalog-matrix smoke; see `e2e/docker-compose.yml`):

```sh
docker compose -f e2e/docker-compose.yml up -d --wait
```

Run the light workload:

```sh
uv run --project e2e/benchmark python e2e/benchmark/runner.py \
  --cdc-extension build/release/extension/ducklake_cdc/ducklake_cdc.duckdb_extension \
  --workload e2e/benchmark/light.yaml
```

Results are written under `e2e/benchmark/results/` by default. The JSON mirrors
the rendered summary sections: workload, throughput, end-to-end latency, and
stage breakdown.
