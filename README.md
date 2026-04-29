# ducklake-cdc

**Stream changes from your DuckLake — to webhook, Postgres, Redis, Kafka, your own code, anywhere. No JVM. No coordinator. No SaaS bill.**

Postgres has logical replication. Iceberg has table-changes. [DuckLake](https://ducklake.select) has both — `ducklake-cdc` gives you the daemon to consume them.


<img src="docs/demo/phase1/producer.png" alt="Producer SQL mutating a DuckLake table">
<img src="docs/demo/phase1/consumer.png" alt="Consumer SQL reading DuckLake CDC events">


Create, insert, update, delete on the left. Discover the table and read the row-level change stream on the right.

```sql
-- Preconditions: DuckDB 1.5.1, the bundled `ducklake` and `parquet`
-- extensions, and a local build of `ducklake_cdc.duckdb_extension`
-- until the community-extension publish lands in Phase 5.
LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;
ATTACH 'ducklake:my.ducklake' AS lake (DATA_PATH 'my_data');

CREATE TABLE lake.orders(id INTEGER, status VARCHAR);
INSERT INTO lake.orders VALUES (1, 'new');

SELECT * FROM cdc_consumer_create('lake', 'demo');
INSERT INTO lake.orders VALUES (2, 'paid');

-- The cursor loop in three primitives:
SELECT * FROM cdc_window('lake', 'demo');                  -- acquire single-reader window
SELECT * FROM cdc_changes('lake', 'demo', 'orders');       -- typed DML rows for the window
SELECT * FROM cdc_commit('lake', 'demo', /* end_snapshot */);  -- advance the cursor
```

That is the durable cursor loop: create a named consumer, acquire a
single-reader window, read typed DML rows via the `cdc_changes` sugar
(or DuckLake's `lake.table_changes()` directly), then commit the returned
`end_snapshot`. Consumers that care about schema also call
`cdc_ddl('lake', 'demo')` between the window and the commit.

For ad-hoc exploration without a cursor, use the stateless sugar:

```sql
SELECT * FROM cdc_recent_changes('lake', 'orders', since_seconds = 3600);
SELECT * FROM cdc_recent_ddl('lake', since_seconds = 86400);
SELECT * FROM cdc_schema_diff('lake', 'orders', /* from */ 0, /* to */ 5);
```

Long-poll consumers wrap the loop with `cdc_wait`:

```sql
SELECT cdc_wait('lake', 'demo', timeout_ms = 30000);  -- BIGINT new_snap | NULL on timeout
```

Operational dashboards read from `cdc_consumer_stats('lake')` (cursor /
gap / lease columns) and `cdc_audit_recent('lake')` (lifecycle audit
trail). The full primitive + sugar surface, with row shapes and
behaviour notes per function, is in
[`docs/api.md`](./docs/api.md). Operational guidance for the lease
lifecycle and the `cdc_wait` connection-starvation foot gun is in
[`docs/operational/lease.md`](./docs/operational/lease.md) and
[`docs/operational/wait.md`](./docs/operational/wait.md). New contributors
should start with [`docs/development.md`](./docs/development.md) for the
local build and test loop.

## Use cases

- **Outbox without dual-writes.** Apps `set_commit_message(extra_info => '{"event":"OrderPlaced"}')`; downstream services tail the lake and route on `commit_extra_info`. The dual-writes-to-Kafka problem you've been working around — gone.
- **Reverse ETL.** Push row-level deltas to Kafka, NATS, Redis Streams, search indexes, vector stores, SaaS APIs. The Hightouch / Census shape, on top of a lakehouse you already have.
- **Cache invalidation.** Fan out updates to CDNs and application caches with at-least-once delivery.
- **ML feature stores.** Stream feature updates from the offline lakehouse to online serving stores.
- **Audit & compliance.** A durable, snapshot-precise change log decoupled from the producer.

### Schema-as-code (the differentiator)

Most CDC systems hand you DDL events as an afterthought. We promote them to first-class. A consumer subscribed with `event_categories := ['ddl']` sees a typed event for every `CREATE` / `ALTER` / `DROP` in the lake — column adds, column drops, type promotions, table renames, view-definition changes — with a structured `details` payload that downstream sinks can act on.

```python
import ducklake_cdc, subprocess, json

with ducklake_cdc.consumer("ducklake:lake.duckdb", name="schema-watcher",
                            event_categories=["ddl"]) as c:
    for batch in c.poll():
        for ddl in batch.ddl:
            path = f"schema/{ddl.schema_name}/{ddl.object_name}.json"
            with open(path, "w") as f:
                json.dump({"snapshot_id": ddl.snapshot_id,
                           "event_kind": ddl.event_kind,
                           "details": ddl.details}, f, indent=2)
            subprocess.run(["git", "add", path])
            subprocess.run(["git", "commit", "-m",
                            f"{ddl.event_kind} {ddl.object_name} @ snapshot {ddl.snapshot_id}"])
        c.commit(batch.snapshot_id)
```

Fifty lines of Python and your lake's schema history is a git repo. Same primitive feeds Slack notifications when columns drop, Confluent Schema Registry sync, or a "who changed what" audit trail. No Spark cluster. No Databricks bill.

## How it relates to existing tools

`ducklake-cdc` is the **consumer** side of a DuckLake — complementary to producer-side tools like `pg_duckpipe` (Postgres → DuckLake), and orthogonal to OLTP CDC (Debezium's home turf). If you already have data landing in a DuckLake, this is what reads it out as a change stream.

## What `ducklake-cdc` adds vs DuckLake itself

| What `ducklake-cdc` adds | Why DuckLake doesn't have it |
| --- | --- |
| Persistent consumer state (named cursors stored as a regular DuckLake table) | DuckLake has snapshots, but no "subscriber" concept |
| `cdc_window(consumer)` — atomic cursor read + gap-detected snapshot range, schema-version-bounded, single-reader-enforced via an owner-token lease | Has to be combined with a state table that DuckLake doesn't ship with |
| `cdc_wait(consumer, timeout)` — long-poll until a new snapshot appears | DuckLake exposes `current_snapshot()` but no blocking subscribe |
| `cdc_ddl(consumer)` — typed DDL event stream (created / altered / dropped for schema, table, view; RENAME folds into `altered` per spec) | `ducklake_snapshot_changes.changes_made` separates DDL from DML at the catalog level, but no typed surface exists |
| `cdc_consumer_stats()` — lag (snapshots + seconds), throughput, gap counters, DLQ pending | Pure-SQL is possible but unstandardised |
| Wire-format conventions for `variant` and `geometry` (JSON, GeoJSON) | DuckLake stores them; downstream sinks need a contract |
| Reference sinks (stdout, file, webhook, Kafka, Redis Streams, Postgres mirror) with retry, backoff, idempotency keys, and a DLQ | DuckLake stops at the read API |

Plus thin Python and Go clients that wrap these primitives and compose them with DuckLake's own functions.

## What we explicitly do *not* reinvent

This is a small project on purpose. Anything DuckLake already does, we use directly. The non-overlap principle is what keeps it shippable.

- **`table_changes` / `table_insertions` / `table_deletions`** already give you row-level inserts, update pre/post-images and deletes, with snapshot-id *or* timestamp bounds. Inlined data, encrypted files, partial files and post-compaction reads are handled transparently. We use these directly.
- **`snapshots()`** already returns the per-snapshot change manifest as a structured map plus `author`, `commit_message`, `commit_extra_info`. Multi-table change discovery is one `SELECT` away. We don't shadow it.
- **Time travel** (`AT (VERSION => ...)` / `AT (TIMESTAMP => ...)` / `ATTACH ... (SNAPSHOT_VERSION ...)`) is built in. We pass snapshot ids and timestamps through; we don't add a "replay" feature beyond thin glue in your sink or client.
- **Schema evolution** on the read path is built in — `table_changes` reads using the schema as of the end snapshot, with default values for added columns. We add typed *DDL events* on top for downstream sinks; we don't reimplement the read semantics.
- **Outbox metadata** is already first-class on the producer side via `CALL <lake>.set_commit_message('author', 'msg', extra_info => '{...}')`. We just surface it on every batch and recommend a JSON convention for `extra_info`.
- **Compaction maintenance** (`ducklake_expire_snapshots`, `ducklake_merge_adjacent_files`, ...) is DuckLake's job. Our role is detecting when compaction expired a consumer's cursor and raising a structured `CDC_GAP` with the recovery command in the error message.

## Backend agnostic by construction

The extension talks to DuckLake; DuckLake talks to your catalog. **DuckDB, SQLite, and PostgreSQL** catalogs all work — DuckLake's three supported metadata backends. Storage can be anything DuckDB can read.

## Performance, honestly

Designed for **~100-10k events/sec per consumer** with **sub-second to seconds latency**, on a Postgres catalog that comfortably serves **~50 consumers** without connection pooling (~500 with PgBouncer). Idle consumers are cheap by design (10s polling backoff). Multiple consumers in one process share snapshot polling via `LakeWatcher`, so N consumers ≠ N times the catalog load.

We're a poor fit for sub-10ms latency, OLTP CDC, and globally-distributed sub-second cache invalidation. Real numbers per workload, the four-axis discussion, and the honest "what we're bad at" list are in [`docs/performance.md`](./docs/performance.md).

## Status

Pre-alpha. The design notes and operational docs live under [`docs/`](./docs).

## Releasing

Releases are cut from a single button. Open the **Actions** tab, pick
**Release**, type a version (`v0.1.0`, `v0.1.1`, …), and click **Run
workflow**. The workflow tags `main`, creates the maintenance branch
`release/MAJOR.MINOR`, publishes a GitHub Release with auto-generated
notes, and opens a PR against
[`duckdb/community-extensions`](https://github.com/duckdb/community-extensions)
to update this extension's descriptor.

The first run of any new version should be a **dry run** (the default)
so the workflow prints the plan without pushing anything. Once the plan
looks right, untick `dry_run` and re-run.

If the GitHub Release succeeds but the community-extensions PR step
fails, re-run **Release** with the same version, `dry_run: false`, and
`community_pr_only: true` to retry only that PR.

The exact contract — version policy, validations, branch rules, and the
one-time GitHub setup (branch protection rules and the
`COMMUNITY_BOT_TOKEN` secret) — is in
[`docs/decisions/0013-versioning-and-release-automation.md`](./docs/decisions/0013-versioning-and-release-automation.md).
Day-to-day developer flow lives in
[`docs/decisions/0012-branching-ci-release.md`](./docs/decisions/0012-branching-ci-release.md).

## License

Apache-2.0 (see [`LICENSE`](./LICENSE)).
