# DuckLake CDC End-to-End

These examples now tell one product story:

> DuckLake gives you snapshots. `ducklake_cdc` gives you durable,
> schema-aware consumers over those snapshots.

This is not a Kafka/Flink replacement suite. The useful surface is
incremental maintenance, safe cursoring, backfill, schema-boundary handling,
and lightweight wakeups for systems that re-read from DuckLake.

## The Five Examples

| # | folder | story | status |
|---|--------|-------|--------|
| 01 | `01_schema_safe_consumer/` | Stop DML at schema boundaries, process DDL, migrate, then resume safely. | Spec only |
| 02 | `02_incremental_materialized_view/` | Maintain derived DuckLake tables incrementally instead of rescanning. | Spec only |
| 03 | `03_backfill_live_catchup/` | Start at snapshot 0, process history, then switch into live listen mode. | Spec only |
| 04 | `04_cache_refresh/` | Use metadata-only ticks to refresh caches, search indexes, or vector indexes. | Runnable |
| 05 | `05_pipeline_dag/` | Run a multi-stage lakehouse DAG with durable CDC cursors. | Runnable |

That order is intentional. The first three are the strongest alignment with
DuckLake's roadmap and current user pain. The last two are supporting demos:
ticks show the cheap wakeup primitive, and the DAG shows what a larger
application built on the primitives looks like.

## Run

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release

# Runnable today: cache/search refresh ticks
uv run --project e2e python e2e/04_cache_refresh/app.py --headless --duration 30

# Runnable today: multi-stage CDC DAG
uv run --project e2e python e2e/05_pipeline_dag/app.py --headless --duration 30
```

Most examples support the shared flags:

| flag | meaning |
|------|---------|
| `--headless` | No TUI; periodic stderr summary; exits after `--duration`. |
| `--duration N` | Headless run length in seconds. |
| `--catalog {duckdb,sqlite,postgres}` | Catalog backend. Multi-writer demos should use Postgres. |
| `--storage {disk,s3}` | Local disk or the Garage S3 service. |

## Catalog Reality

DuckLake's catalog backend matters more than these demos should hide.

| example | duckdb | sqlite | postgres | notes |
|---------|--------|--------|----------|-------|
| 01 schema-safe consumer | yes | no | yes | Demonstrates sink-write + `cdc_commit` transaction; SQLite lock behavior distracts from the schema story. |
| 02 incremental materialized view | yes | yes | yes | Single-process first; Postgres for concurrent writers. |
| 03 backfill/live catchup | yes | yes | yes | Backfill is bounded reads; listen mode follows catalog limits. |
| 04 cache refresh | yes | yes | yes | Postgres gets `LISTEN`/`NOTIFY`; others poll. |
| 05 pipeline DAG | no | no | yes | Five concurrent writers; embedded/file catalogs contend too hard. |

## What Counts As A Good Demo

Each example should prove a real operating property:

1. **Cursor correctness**: a committed consumer does not replay; an
   uncommitted consumer can replay safely.
2. **Schema awareness**: DDL is not an accidental footgun hidden inside DML.
3. **Backfill path**: historical snapshots and live snapshots use the same
   primitives.
4. **Honest numbers**: report the observed p50/p95/p99 or throughput from the
   demo, not aspirational streaming numbers.
5. **Cleanup**: reset catalog, storage, and generated load inputs before and
   after runs so examples do not contaminate each other.

## Supporting Directories

- `_lib/`: shared config, load generation, metrics, stage runner, and TUI glue.
- `benchmark/`: legacy synthetic harness kept only while useful for comparison.
- `catalog_matrix/`, `smoke/`, `upstream/`: narrow compatibility/probe suites
  that test extension invariants rather than end-user stories.

## S3 Storage

The shared dev S3 service is Garage. Start it through `docker-compose.yml`,
then bootstrap once:

```bash
./e2e/setup-garage.sh
```

Paste the printed credentials into `e2e/.env`; after that `--storage s3`
works for examples that support it.
