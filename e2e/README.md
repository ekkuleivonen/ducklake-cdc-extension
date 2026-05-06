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
| 01 | `01_schema_safe_consumer/` | Stop DML at schema boundaries, process DDL, migrate, then resume safely. | Runnable |
| 02 | `02_incremental_materialized_view/` | Maintain derived DuckLake tables incrementally instead of rescanning. | Runnable |
| 03 | `03_backfill_live_catchup/` | Restart a live consumer twice while producers keep writing, then catch up. | Runnable |
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

# Runnable today: schema-safe consumer
uv run --project e2e python e2e/01_schema_safe_consumer/app.py --headless

# Runnable today: incremental materialized view
uv run --project e2e python e2e/02_incremental_materialized_view/app.py --headless

# Runnable today: restart + catch-up (producer keeps writing during restarts)
uv run --project e2e python e2e/03_backfill_live_catchup/app.py --headless
```

Most examples support the shared flags:

| flag | meaning |
|------|---------|
| `--headless` | No TUI; periodic stderr summary; exits after `--duration`. |
| `--duration N` | Headless run length in seconds. |
| `--catalog {duckdb,sqlite,postgres}` | Catalog backend. Multi-writer demos should use Postgres. |
| `--storage {disk,s3}` | Local disk or the Garage S3 service. |

## CI Gates

CI runs these demos through `ci_demo_assertions.py`. Each app emits a final
`--json-summary`; the runner checks correctness first (`errors=0`, final
invariants, drained row counts) and then conservative performance floors for
the tick and DAG demos.

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
uv run --project e2e --locked python e2e/ci_demo_assertions.py
docker compose -f e2e/docker-compose.yml down -v
```

The thresholds are intentionally modest. They catch broken consumers, runaway
latency, and obvious throughput regressions without pretending CI hardware is a
benchmark lab.

## Catalog Reality

DuckLake's catalog backend matters more than these demos should hide.

| example | duckdb | sqlite | postgres | notes |
|---------|--------|--------|----------|-------|
| 01 schema-safe consumer | yes | no | yes | Demonstrates sink-write + `cdc_commit` transaction; SQLite lock behavior distracts from the schema story. |
| 02 incremental materialized view | yes | no | yes | Demonstrates aggregate update + `cdc_commit` transaction; SQLite lock behavior distracts from the incremental-maintenance story. |
| 03 backfill/live catchup | yes | no | yes | Restart gap + READ catch-up + LISTEN; SQLite excluded like 01/02 for transactional sink pattern. |
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
- `smoke/`: narrow extension probes that need Python/C++ harnesses.
- `smoke/enumerate_changes_map.py`: DuckDB/DuckLake contract probe for `snapshots().changes` MAP keys across catalog backends (does not load this extension).

## S3 Storage

The shared dev S3 service is Garage. Start it through the `s3` Compose profile,
then bootstrap once:

```bash
docker compose -f e2e/docker-compose.yml --profile s3 up -d --wait
./e2e/setup-garage.sh
```

Paste the printed credentials into `e2e/.env`; after that `--storage s3`
works for examples that support it.
