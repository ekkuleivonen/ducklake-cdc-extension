# DuckLake CDC end-to-end

End-to-end surface area for the `ducklake_cdc` extension. The directory is
intentionally flat &mdash; the **five use-case examples are the headline** and
sit at the top level. A handful of supporting subdirectories live alongside
them for jobs the examples can't do.

## What's here

```
e2e/
├── README.md                  this file
├── docker-compose.yml         Postgres + PgBouncer + Garage S3 (+ Redis,
│                              + serving Postgres as examples 03 / 05 land)
├── pgbouncer/                 PgBouncer config for the catalog connection pool
├── garage.toml                Garage S3 single-tier dev config
├── setup-garage.sh            one-time Garage bootstrap (idempotent)
├── .env / .env.example        shared secrets; paste S3 creds here after setup
├── pyproject.toml             single uv project for examples + shared lib
│
├── 01_pipeline_dag/           in-process multi-stage transforms (flagship)
├── 02_ticks/                  sub-50 ms snapshot-only notifications
├── 03_publish_redis/          CDC -> Redis Streams (generalizes to Kafka)
├── 04_audit_log/              durable change history; DDL consumer too
├── 05_reverse_etl/            DuckLake -> Postgres serving with upsert+delete
│
├── _lib/                      reusable bits across examples (load generator,
│                              sink contract types, TUI helpers)
│
├── catalog_matrix/            extension portability gate: same flow against
│                              duckdb / sqlite / postgres backends
├── smoke/                     extension white-box probes (warning text, lease
│                              multiconn, interrupt propagation, TOCTOU,
│                              minimal repros for known bugs)
└── upstream/                  DuckLake upstream-CDC surveillance: tracks how
                               upstream's change shape moves across versions
```

The examples replace the synthetic `benchmark/` harness that lived here
previously. While that migration is underway, `benchmark/` may still be
present and runnable; it will be removed once the examples can produce
equivalent perf numbers from their headless runs.

## The five examples

| #  | folder                | one-line                                                            | status |
|----|-----------------------|---------------------------------------------------------------------|--------|
| 01 | `01_pipeline_dag/`    | In-process multi-stage transforms with exactly-once via the cursor. | TODO   |
| 02 | `02_ticks/`           | Snapshot-only notification &mdash; sub-50&nbsp;ms tap, no payload move. | TODO (blocked on extension long-poll primitive) |
| 03 | `03_publish_redis/`   | CDC &rarr; Redis Streams. Generalizes to Kafka, Pulsar, files.      | TODO   |
| 04 | `04_audit_log/`       | Every change to every row, queryable forever; DDL consumer too.     | TODO   |
| 05 | `05_reverse_etl/`     | DuckLake &rarr; Postgres serving layer with upsert + delete.        | TODO   |

Build order is **01 &rarr; 02 &rarr; 03 &rarr; 04 &rarr; 05**, picked to
front-load the architecturally novel work. If 01 and 02 don't feel beautiful
when finished, *that's the readiness signal* &mdash; pause and improve the
system before continuing the suite.

The suite has a dual job:

1. **Internal readiness gauge** &mdash; "is this product good enough to talk
   about publicly yet?" When all five examples meet their acceptance criteria,
   the answer is yes.
2. **Talk demos** &mdash; each example is shaped to be the live portion of a
   short technical talk.

## How to run an example

Every example exposes one entrypoint, `app.py`, with **one workload**.
Locally you get the live TUI; CI runs the same workload headless. There are
no scale modes &mdash; the workload is sized to be both meaningful as a
benchmark and tractable as a CI gate.

```bash
docker compose -f e2e/docker-compose.yml up -d --wait
make release   # build the extension

# local development / talk demo (default; live TUI, runs until Ctrl-C)
uv run --project e2e python e2e/01_pipeline_dag/app.py

# CI invocation (no TUI; runs to completion, asserts, writes metrics JSON)
uv run --project e2e python e2e/01_pipeline_dag/app.py --headless --catalog postgres

# Same workload, S3 storage instead of local disk
uv run --project e2e python e2e/01_pipeline_dag/app.py --storage s3
```

The flag set is deliberately tiny:

| flag | what it does | when to use |
|------|--------------|-------------|
| (none) | live TUI, runs until Ctrl-C, metrics emitted on clean shutdown | local dev, talks |
| `--headless` | no TUI; runs to completion; metrics JSON to `./.results/` | CI |
| `--catalog {duckdb,sqlite,postgres}` | catalog backend (per-example matrix below) | any |
| `--storage {disk,s3}` | local disk (default) vs S3 via the Garage service | any |

CI fans the headless invocation across each example's supported `--catalog`
values (and, where useful, both `--storage` values). Same code path as the
demo &mdash; if it passes locally with the TUI, it passes in CI.

## Catalog backend matrix per example

| example | duckdb | sqlite | postgres | rationale |
|---------|--------|--------|----------|-----------|
| 01 pipeline_dag    | yes      | yes      | yes | single-process by design; all backends apply |
| 02 ticks           | yes      | yes      | yes | single-process tick consumer; all backends apply |
| 03 publish_redis   | optional | yes (WAL)| yes | publisher can be in-process (any) or out-of-process (postgres/sqlite) |
| 04 audit_log       | yes      | yes      | yes | single-process; all backends apply |
| 05 reverse_etl     | optional | yes (WAL)| yes | typical deployment is multi-process; postgres is the realistic case |

CI runs each example's smoke against every backend it claims support for,
mirroring the `catalog_matrix/` fan-out pattern.

## What each example must clear to count as "done"

These are the suite-wide acceptance criteria. Per-example READMEs add specifics
(numbers, pattern shape) on top.

1. **Single-command bootstrap** &mdash; from a fresh clone:
   `docker compose -f e2e/docker-compose.yml up -d --wait` then
   `uv run --project e2e python e2e/NN_xxx/app.py` works.
   No env-var hunting, no missing services. Anything an example needs beyond
   Postgres + PgBouncer goes into the shared `e2e/docker-compose.yml`.
2. **Visible result** &mdash; the example produces something a viewer can
   *see* moving (a TUI, a downstream table updating live, a browser tab,
   a Redis `XREAD` tail). Not just a return code.
3. **Honest numbers in the README** &mdash; p50/p99 latency and sustained
   throughput, measured *from this very example* on a known machine class.
   No vendor-pitch numbers.
4. **Headless run passes against every catalog the example claims to
   support** (see matrix above).
5. **Survives the obvious follow-ups** &mdash; schema change mid-flight,
   consumer restart, producer crash. README states explicitly which it
   survives and which it does not yet.
6. **Reads as docs** &mdash; someone unfamiliar with the system can read
   `app.py` end-to-end in &le;5 minutes and understand the pattern.

## Visual style: bespoke per example, library-backed

Examples deliberately *don't* share a dashboard helper. Each one renders what
is most natural for it:

- 01 pipeline DAG &mdash; live DAG diagram, stage-by-stage row counts, end-to-end lag per branch.
- 02 ticks &mdash; latency histogram + a tiny browser tab that flashes on every tick.
- 03 publish-Redis &mdash; split-pane: producer commits left, `XREAD` tail right.
- 04 audit log &mdash; "rewind" UI: pick a row, see its full history and the DDL events around it.
- 05 reverse-ETL &mdash; OLAP table on the left, OLTP table on the right; show divergence-and-recovery on consumer restart.

**Use a TUI library, don't hand-roll.** The previous benchmark dashboard
hand-rolled ANSI escape codes, alt-screen handling, and panel layout. As the
TUI becomes more prominent (one per example, demo-quality), maintaining that
by hand will be a tax we don't want to pay. The intended choice is
[`rich`](https://rich.readthedocs.io/) (`Live` + `Layout` + `Panel`) for
declarative dashboards without an event loop, escalating to
[`textual`](https://textual.textualize.io/) only if an example needs
interactive widgets. The library lives in `e2e/pyproject.toml` so every
example shares the same version. See `_lib/README.md` for the rationale and
the planned helpers.

## Extension prerequisites

| example | extension changes required | notes |
|---------|---------------------------|-------|
| 01 pipeline DAG | none | uses `cdc_window`, `cdc_dml_changes_read`, `cdc_commit`, `cdc_dml_consumer_create`, `cdc_ddl_consumer_create` |
| 02 ticks | **YES &mdash; long-poll/blocking-wait variant of `cdc_window`** | see `02_ticks/README.md` for the proposed API and rationale; the example is blocked on this landing |
| 03 publish-Redis | none | |
| 04 audit log | none | |
| 05 reverse-ETL | none | |

The long-poll primitive for ticks is the only new extension surface the suite
demands. Everything else exercises today's API.

## CI integration

CI invokes each example with `--headless`, fanned across the supported
`--catalog` (and where useful, `--storage`) values. The same headless run
both gates merges (correctness assertions) and produces the perf numbers
release tooling collects.

Each headless run writes a metrics JSON to
`./.results/<example>-<catalog>-<storage>.json` on clean shutdown. Schema
lives in `_lib/README.md`. Release tooling aggregates these across the
matrix into a perf report.

Examples should size their workload so a single headless run completes in
**&lt;3 min**. That keeps the per-PR matrix sweep inside a normal CI budget
while still giving stable enough p50/p99 numbers to track regression.

## Storage backends

Examples default to local-disk storage. Add `--storage s3` to drive the
example against an S3-compatible object store. Storage matters here not
just for production realism &mdash; commit cost decomposes very differently
between disk and S3 (object PUT latency dominates the fixed overhead) and
the same workload produces different perf shapes on each.

The shared dev S3 is provided by [Garage](https://garagehq.deuxfleurs.fr/)
running as a service in `docker-compose.yml`. Single-tier, single-node,
local-only &mdash; this is a development convenience, not a production
deployment.

**One-time bootstrap** (after `docker compose up`):

```bash
./e2e/setup-garage.sh
```

The script is idempotent (safe to re-run). It waits for Garage to be
reachable, bootstraps the layout, creates a bucket (`e2e` by default) and
an access key (`e2e-app` by default), grants the key read+write+owner on
the bucket, and prints an env block at the end.

**Paste the printed block into `e2e/.env` once.** From then on the e2e
config layer auto-loads it and `--storage s3` just works on every example.
The same block is also saved to `e2e/.garage-bootstrap.env` (gitignored)
as a re-paste fallback.

The variables exposed by the bootstrap:

| variable | example value | meaning |
|----------|--------------|---------|
| `S3_ENDPOINT` | `http://localhost:3900` | Garage S3 API endpoint |
| `S3_REGION` | `garage` | DuckLake/AWS-compat region label |
| `S3_ACCESS_KEY` | `GK0000...` | access key generated by Garage |
| `S3_SECRET_KEY` | (32-byte secret) | secret key generated by Garage |
| `S3_BUCKET` | `e2e` | bucket the examples write into |
| `S3_USE_PATH_STYLE` | `true` | Garage requires path-style addressing |

Override container, bucket, key names via env vars at the top of
`setup-garage.sh`.

## Other directories

- **`_lib/`** &mdash; reusable code across examples: synthetic load
  generator (lifted from the old `benchmark/producer.py`), sink contract
  types, TUI helpers. See `_lib/README.md`.
- **`catalog_matrix/`** &mdash; portability gate: proves the same DDL+DML
  flow holds against duckdb / sqlite / postgres backends. Different job
  from examples (it tests *extension invariants* across catalogs;
  examples test *use cases*).
- **`smoke/`** &mdash; white-box probes for narrow extension behaviors:
  warning text, multi-process lease semantics, DuckDB interrupt
  propagation, TOCTOU avoidance, minimal repros for known bugs.
- **`upstream/`** &mdash; tracks DuckLake's upstream CDC-shape across
  versions; surveillance for compatibility regressions.

These three are kept because they do jobs the use-case examples cannot.
