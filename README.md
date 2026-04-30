# ducklake-cdc

A DuckDB extension that adds a row-level change-data-capture cursor on top
of [DuckLake](https://ducklake.select).

> **Status: pre-alpha, Phase 1 in progress.** The SQL surface described
> below works against an embedded DuckDB-backed DuckLake catalog. There is
> no published binary, no language client, no reference sink, and the
> Postgres / SQLite catalog backends have not yet been validated. See
> [`docs/roadmap/`](./docs/roadmap/) for the full plan and
> [`VISION.md`](./VISION.md) for what this project intends to become.

## What works today

- The cursor primitives — `cdc_window`, `cdc_commit`, `cdc_wait`,
  `cdc_ddl`, and the `cdc_consumer_*` lifecycle, all enforced by an
  owner-token lease for single-reader-per-consumer.
- Stateless sugar — `cdc_recent_changes`, `cdc_recent_ddl`,
  `cdc_schema_diff` — for ad-hoc exploration without creating a consumer.
- Composed sugar — `cdc_events`, `cdc_changes` — built over the
  primitives, applied with the consumer's `change_types` filter.
- Observability — `cdc_consumer_stats`, `cdc_audit_recent`.
- Structured errors and notices: `CDC_GAP`, `CDC_BUSY`,
  `CDC_INVALID_TABLE_FILTER`, `CDC_INCOMPATIBLE_CATALOG`,
  `CDC_SCHEMA_BOUNDARY`, `CDC_WAIT_TIMEOUT_CLAMPED`,
  `CDC_WAIT_SHARED_CONNECTION` — full list in
  [`docs/errors.md`](./docs/errors.md).
- Embedded DuckDB catalog backend, validated against DuckDB **v1.5.1**
  and a pinned DuckLake commit (see [`docs/development.md`](./docs/development.md)).
- A bench smoke harness (`bench/runner.py` + `bench/light.yaml`) that
  reports end-to-end latency, throughput, catalog QPS, and lag drift.

## What does *not* work yet

- **No published extension binary.** `INSTALL ducklake_cdc FROM community`
  does not resolve. You must build from source. Community-extensions
  publishing is part of Phase 5.
- **No Python or Go client.** The `import ducklake_cdc` snippet you may
  have seen in older drafts is roadmap material (Phases 3 and 4).
- **No reference sinks.** Stdout, file, webhook, Kafka, Redis Streams,
  and Postgres-mirror sinks are roadmap material (Phases 3–5).
- **Only the embedded DuckDB catalog has been exercised.** SQLite and
  PostgreSQL catalog backends are part of the Phase 2 matrix; their
  behaviour is currently unproven.
- **No DLQ semantics.** The `__ducklake_cdc_dlq` table is created at
  bootstrap with the locked schema, but write/read/replay/acknowledge
  helpers and the DDL-blocks-DML policy land in Phase 2.
- **No `doctor` command.** Planned for Phase 2.
- **No release tags.** Release automation exists
  ([`.github/workflows/release.yml`](./.github/workflows/release.yml))
  but no `v0.0.x` tags have been cut yet.

## Build and run

You need a local clone with submodules and a working C++ toolchain:

```bash
git clone --recursive https://github.com/<this-repo>.git
cd ducklake-cdc-extension
make debug
./build/debug/duckdb -unsigned -c "SELECT cdc_version();"
```

The binary stamp is `git tag --points-at HEAD` if a tag points at the
build commit, otherwise the short SHA — so an unstable build reports
something like `ducklake_cdc 7a3b9c1`. Full build / test / sanitiser
guidance lives in [`docs/development.md`](./docs/development.md).

## Quickstart (against your local debug build)

![Producer SQL mutating a DuckLake table](./docs/demo/phase1/producer.png)
![Consumer SQL reading DuckLake CDC events](./docs/demo/phase1/consumer.png)

```sql
-- Preconditions: DuckDB v1.5.1, the official `ducklake` and `parquet`
-- extensions (`INSTALL`'d at runtime), and a local build of
-- `ducklake_cdc.duckdb_extension` (no community-extension publish yet).
INSTALL ducklake;
LOAD ducklake;
LOAD parquet;
LOAD 'build/debug/extension/ducklake_cdc/ducklake_cdc.duckdb_extension';
ATTACH 'ducklake:my.ducklake' AS lake (DATA_PATH 'my_data');

CREATE TABLE lake.orders(id INTEGER, status VARCHAR);
INSERT INTO lake.orders VALUES (1, 'new');

SELECT * FROM cdc_consumer_create('lake', 'demo');
INSERT INTO lake.orders VALUES (2, 'paid');

-- The cursor loop in three primitives. cdc_window and cdc_commit are
-- table functions, and DuckDB table functions do not accept subqueries
-- as arguments, so capture end_snapshot into a session variable first.
SELECT * FROM cdc_window('lake', 'demo');                         -- acquire single-reader window
SELECT * FROM cdc_changes('lake', 'demo', 'orders');              -- typed DML rows for the window

SET VARIABLE end_snapshot = (
  SELECT end_snapshot FROM cdc_window('lake', 'demo')
);
SELECT * FROM cdc_commit('lake', 'demo', getvariable('end_snapshot'));
```

That is the durable cursor loop: create a named consumer, acquire a
single-reader window, read typed DML rows via the `cdc_changes` sugar
(or DuckLake's `lake.table_changes()` directly), then commit the
returned `end_snapshot`. Consumers that care about schema also call
`cdc_ddl('lake', 'demo')` between the window and the commit.

For ad-hoc exploration without a cursor, the stateless sugar:

```sql
SELECT * FROM cdc_recent_changes('lake', 'orders', since_seconds := 3600);
SELECT * FROM cdc_recent_ddl('lake', since_seconds := 86400);
SELECT * FROM cdc_schema_diff('lake', 'orders', /* from */ 0, /* to */ 5);
```

Long-poll consumers wrap the loop with `cdc_wait`. It is a table
function returning one BIGINT row (the new snapshot id, or NULL on
timeout), so always read it from a `FROM` clause:

```sql
SELECT * FROM cdc_wait('lake', 'demo', timeout_ms := 30000);
```

Operational dashboards read from `cdc_consumer_stats('lake')` (cursor /
gap / lease columns) and `cdc_audit_recent('lake')` (lifecycle audit
trail).

The full primitive + sugar surface, with row shapes and behaviour notes
per function, is in [`docs/api.md`](./docs/api.md). Operational guidance
for the lease lifecycle and the `cdc_wait` connection-starvation foot
gun is in [`docs/operational/lease.md`](./docs/operational/lease.md) and
[`docs/operational/wait.md`](./docs/operational/wait.md).

## Examples

Runnable SQL examples ship under [`examples/`](./examples). Each one is a
self-contained script you can pipe into `./build/debug/duckdb`:

| File | Pattern |
| --- | --- |
| `01_basic_consumer.sql` | `cdc_window` + `table_changes` + `cdc_commit` directly |
| `02_outbox_demo.sql` | `cdc_events` dispatch on `commit_extra_info` |
| `03_long_poll.sql` | `cdc_wait` for a streaming consumer |
| `04_schema_change.sql` | `schema_changes_pending`, `cdc_ddl`, schema-boundary flow |
| `05_recent_changes.sql` | stateless `cdc_recent_changes` / `cdc_recent_ddl` |
| `06_parallel_readers.sql` | orchestrator + worker fan-out under one lease |
| `07_ddl_only_consumer.sql` | `event_categories := ['ddl']` schema-watcher |
| `08_cross_schema_window.sql` | `stop_at_schema_change := false` opt-out |
| `09_lease_recovery.sql` | force-release after a holder dies |

## Backends

DuckLake supports DuckDB, SQLite and PostgreSQL as metadata catalogs.
Today this extension has only been validated against the **embedded
DuckDB** catalog. SQLite and PostgreSQL coverage is the work item of
Phase 2 — see [`docs/roadmap/phase_2_backend_agnostic.md`](./docs/roadmap/phase_2_backend_agnostic.md).
Until that phase ships, treat the SQLite / Postgres paths as untested.

## Versioning and releases

Versioning policy and the one-button release workflow are documented in
[`docs/decisions/0013-versioning-and-release-automation.md`](./docs/decisions/0013-versioning-and-release-automation.md).
Day-to-day branch flow is in
[`docs/decisions/0012-branching-ci-release.md`](./docs/decisions/0012-branching-ci-release.md).

No `v0.0.x` tag has been cut yet. The first one lands as part of closing
out Phase 1.

## Where to go next

- **What this project intends to become** — [`VISION.md`](./VISION.md).
- **Roadmap and exit criteria per phase** — [`docs/roadmap/`](./docs/roadmap/).
- **Architecture decisions** — [`docs/decisions/`](./docs/decisions/).
- **Public API reference** — [`docs/api.md`](./docs/api.md).
- **Contributing** — [`CONTRIBUTING.md`](./CONTRIBUTING.md) and
  [`docs/development.md`](./docs/development.md).

## License

Apache-2.0 (see [`LICENSE`](./LICENSE)).
