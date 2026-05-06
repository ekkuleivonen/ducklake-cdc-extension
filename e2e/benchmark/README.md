# E2E Benchmark

> **Being replaced.** This synthetic benchmark harness is being superseded by
> the use-case examples at `e2e/01_pipeline_dag/`, `e2e/03_publish_redis/`,
> etc. Each example will carry its own `--mode bench` so perf characterization
> happens through the real use cases, not a synthetic shape.
>
> This directory stays runnable until the examples carry equivalent bench
> coverage. The 6 workload YAMLs (`light/throughput/latency/mutation_heavy/
> fanout/bursty`) and the synthetic load generator in `producer.py` will be
> migrated to `e2e/_lib/load.py` as part of building example 01. After that,
> this directory is removed.

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

## Workload library

Six profiles ship in `e2e/benchmark/`. Each one is designed to move **one**
primary axis far from `light` so the dashboard's `stage p95` bar becomes
diagnostic — the panel that widens tells you which stage owns the cost on
that profile.

| Workload | Primary axis | What it stresses | What to watch |
|---|---|---|---|
| `light.yaml` | — (baseline) | Idle-floor latency, smoke check | p50, that the pipeline is alive |
| `throughput.yaml` | Volume + producer concurrency | Sustained 4 k rows/s across 8 tables / 4 writers | `ch/s`, p99, which stage segment dominates |
| `latency.yaml` | Commit cadence + no coalescing | Per-snapshot listen overhead (`max_snapshots: 1`) | p50 / p99 — should be the lowest of any profile |
| `mutation_heavy.yaml` | Mutation mix | `update_preimage` + delete CDC paths (80/40 mix) | per-table `~` and `−` columns, `client` segment width |
| `fanout.yaml` | Consumer multiplicity | 8 consumers per table, catalog read amplification | total `changes` ≈ 8× producer rate, `ext` segment scaling |
| `bursty.yaml` | Burstiness | 100× batch-size spread + `profile: variate` jitter | how fast latency recovers between storms |

Run any of them by swapping the `--workload` flag:

```sh
uv run --project e2e/benchmark python e2e/benchmark/runner.py \
  --cdc-extension build/release/extension/ducklake_cdc/ducklake_cdc.duckdb_extension \
  --workload e2e/benchmark/throughput.yaml
```

Each run writes `results/<name>.json` so the six profiles produce six
side-by-side artifacts for comparison.

### Workload schema

Required: `duration_seconds`, `schemas`, `tables_per_schema`,
`target_snapshots_per_second`, `target_rows_per_snapshot`.
Optional (with defaults): `name`, `consumers_per_table` (1),
`producer_workers` (1), `update_percent` (0), `delete_percent` (0),
`batch_min` (1), `batch_max` (= `batch_min`), `max_snapshots` (100),
`profile` (`flat` — also accepts `ramp` or `variate` for inter-commit
gap shape).

## Storage backends

The benchmark defaults to local disk (`e2e/benchmark/.work/benchmark_data`).
Pick a different backend by setting `DUCKLAKE_BENCHMARK_STORAGE` (env or
`--storage` on the workers; the runner inherits env into both children):

```sh
# Local disk (default)
export DUCKLAKE_BENCHMARK_STORAGE='file:///some/abs/path'

# S3-compatible (Cloudflare R2, MinIO, AWS S3, …)
export DUCKLAKE_BENCHMARK_STORAGE='s3://<bucket>/<prefix>'
```

For S3-compatible endpoints the bucket/prefix come from the URL and the
remaining knobs come from `DUCKLAKE_BENCHMARK_S3_*` env vars (so secrets
never live on the command line or in workload yaml).

### Shared `e2e/.env`

Both `e2e/docker-compose.yml` and `runner.py` auto-load `e2e/.env`, so a
single file is the source of truth for the local stack and the benchmark.
It's gitignored — keep it on disk, not in version control.

Example `e2e/.env` for a Cloudflare R2 backend:

```sh
DUCKLAKE_BENCHMARK_STORAGE=s3://my-r2-bucket/benchmark
DUCKLAKE_BENCHMARK_S3_ENDPOINT=https://<account_id>.r2.cloudflarestorage.com
DUCKLAKE_BENCHMARK_S3_REGION=auto
DUCKLAKE_BENCHMARK_S3_URL_STYLE=path
DUCKLAKE_BENCHMARK_S3_KEY_ID=<r2 access key id>
DUCKLAKE_BENCHMARK_S3_SECRET=<r2 secret>
```

Parent-environment values always win, so an ad-hoc `export` or CI-injected
secret overrides the file. To switch back to disk for one run, prefix the
command: `DUCKLAKE_BENCHMARK_STORAGE=file:///tmp/x runner.py …`.

### A note on resets and S3

`reset_demo_state` only wipes catalog rows + local-disk data files; for S3
backends it's a no-op on purpose. The catalog drop alone is enough for
correctness (each new run gets fresh DuckLake snapshot/file IDs and never
reads the old objects), but data files accumulate as wasted bytes. Two
cheap mitigations:

- Configure a **lifecycle rule** on the bucket to expire objects under the
  benchmark prefix after N days.
- Use a **run-scoped prefix** so comparing runs is trivial and you can
  lifecycle the parent prefix:

  ```sh
  DUCKLAKE_BENCHMARK_STORAGE="s3://my-r2-bucket/benchmark/$(date +%s)" \
    uv run --project e2e/benchmark python e2e/benchmark/runner.py …
  ```
