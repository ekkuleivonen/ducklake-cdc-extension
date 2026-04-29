# Phase 3 — Python client

**Goal:** an ergonomic Python package that loads the extension, composes our primitives with DuckLake builtins (`snapshots()`, `table_changes()`, `set_commit_message`), interleaves typed DDL events from `cdc_ddl` with DML events from `cdc_changes` per the per-snapshot ordering contract, exposes a documented Sink protocol, and ships reference sinks. Async is first-class, not a footnote.

The client is deliberately thin. It is not a reimplementation of CDC; it's a Pythonic façade over `cdc_window` + `cdc_wait` + `cdc_ddl` + `cdc_changes` + `snapshots()`, plus arrow conversion, schema-boundary handling, DDL/DML interleaving, and sink adapters. Anything DuckLake or the extension already exposes is surfaced directly, not re-wrapped with new semantics.

## Per-snapshot ordering: the library's job

The extension exposes `cdc_ddl(consumer)` and `cdc_changes(consumer, table)` as separate table functions because that's the cleanest SQL surface. The Python library's job is to query both, interleave them by snapshot id with **DDL before DML within each snapshot** per ADR 0008, and present the result as a single ordered iterator of batches. Users who call `cdc_ddl` and `cdc_changes` separately and apply them in the wrong order have a bug in their own code; the docs name it.

## API hierarchy (lead-line first)

The first three pages of `docs/python.md` show, in this order:

1. **Explicit iterator + commit.** The user can see the cursor. This is what we lead with because every consumer-style API has the same two failure modes (forget to commit, commit before sink confirms), and showing the cursor explicitly forces the user to develop a mental model for both.
2. **`tail()` sugar.** Auto-commits *only* on sink-success confirmation. Documented as "the safe default once you understand what's happening." Not the lead-line API.
3. **Async equivalents** of both, with the same precedence (explicit first, sugar second).

If a casual reader looks at the README and sees `tail(...)` first, they will treat the library as a black box and be unable to debug it when something breaks. We avoid that by leading with the visible-cursor pattern.

## Work items

### Packaging

- [ ] Package skeleton: `clients/python/`, `pyproject.toml`, and the `ducklake_cdc` package initializer.
- [ ] Lazy-install the community extension on first connection; cache extension version pinning.
- [ ] **Explicit `ducklake_cdc.preload(con)` for production.** The lazy install hangs the first request after a deploy for ~30 seconds while it downloads, fails on air-gapped clusters, and breaks fail-fast health checks. `preload()` is documented as a startup hook (FastAPI lifespan, Django `AppConfig.ready`, Celery worker init, etc.) with copy-paste examples.
- [ ] PyPI publish workflow (TestPyPI on every tag, PyPI on `v*` releases).
- [ ] Type hints + `py.typed` marker.
- [ ] Document CGO / dynamic-linking deployment caveats for AWS Lambda, Vercel Edge, Cloudflare Workers, etc., with a "this will not work" matrix. Better to tell people up front than to debug their cold-start failures on Discord.

### Lead-line API: explicit iterator + commit

```python
import ducklake_cdc

with ducklake_cdc.consumer(
    "ducklake:metadata.duckdb",
    name="search-indexer",
    tables=["main.orders"],
    change_types=["insert", "update_postimage", "delete"],   # default = all four
    event_categories=["ddl", "dml"],                         # default = both
    stop_at_schema_change=True,                              # default
) as c:
    for batch in c.poll(max_snapshots=100):
        # DDL events first, per ADR 0008 ordering contract
        for ddl in batch.ddl:
            handle_ddl(ddl)                                  # e.g. provision sink schema

        for row in batch.rows:                               # or batch.to_arrow() for >10k rows
            apply(row)

        c.commit(batch.snapshot_id)
```

- [ ] **`Batch.ddl: list[DDLEvent]` and `Batch.rows: list[Row]`** are both populated. Either may be empty for a given batch. Per the ordering contract, callers process `ddl` before `rows` for each batch.
- [ ] **`c.poll(...)` is the unified iterator** that interleaves `cdc_ddl` and `cdc_changes` outputs per snapshot. Old-style `c.changes(...)` and `c.ddl(...)` are retained as DML-only / DDL-only views for users who explicitly want one stream — they are equivalent to `c.poll(...)` followed by accessing only `.rows` or only `.ddl`. The docs lead with `c.poll(...)` because it's the safe default.
- [ ] **No `SchemaChangedBatch` type.** Schema changes are just `altered.table` events in `batch.ddl`. The proactive-yield contract (Phase 0 ADR 0006) is enforced by `cdc_window`'s schema bounding, not by a separate batch class. `Batch.is_schema_change: bool` is a computed convenience (`any(d.event_kind == 'altered' for d in self.ddl)`).
- [ ] **Both iterators expose `commit_message` and `commit_extra_info` on every batch** — these come from DuckLake's `snapshots()`, not from us. Provide a `Batch.json_meta` shortcut for the common case of JSON-encoded `commit_extra_info`.
- [ ] **Lake-level event iterator** (uses `cdc_events` sugar internally, which composes `cdc_window` + `snapshots()`):

  ```python
  with ducklake_cdc.lake("ducklake:metadata.duckdb", consumer="indexer") as lake:
      for event in lake.events(max_snapshots=100):
          if "tables_changed" in event.changes and "orders" in event.changes["tables_changed"]:
              ...
          lake.commit(event.snapshot_id)
  ```

  `event.changes` is the structured map DuckLake's `snapshots()` already returns; we don't re-shape it.

- [ ] **Long-poll mode** uses `cdc_wait` internally on a *dedicated* connection (never shares a pool slot — see Phase 1 `cdc_wait` documentation):

  ```python
  for batch in c.changes(follow=True, idle_timeout=30):
      ...
      c.commit(batch.snapshot_id)
  ```

- [ ] **Forgot-to-commit warning.** If a `consumer` context exits with uncommitted batches that were yielded from `c.changes(...)`, log a `RuntimeWarning` (not `info`) saying so. Discoverable when users search for the warning text. Do not auto-commit on context exit — silently advancing the cursor on exit is exactly the at-least-once-violation we are designing against.
- [ ] **`Batch.uncommitted` property** so callers can inspect rather than guess.

### `LakeWatcher`: shared snapshot polling across consumers in one process

This is **the single highest-leverage Phase 3 work item for production deployments.** Without it, 50 consumers in one process produce 50× the catalog `cdc_wait` polling load. With it, they share **one** poll loop. Add it now (cheap to write, big payoff). Spec'd in `README.md` "Performance principles" → contract 4.

- [ ] `ducklake_cdc.LakeWatcher` is a per-`(catalog_uri)` singleton (registry keyed on the URI). Created lazily on first `consumer(catalog, ...)` call; reference-counted; torn down when the last consumer using it exits.
- [ ] Internally: a single dedicated `*duckdb.Connection` per `LakeWatcher` runs `cdc_wait(...)` style polling against `current_snapshot()` directly (not against any one consumer's `cdc_wait`). Backoff curve identical to the extension's `cdc_wait`. On wake, broadcasts the new `current_snapshot()` to all subscribed consumers via an `asyncio.Event` (or thread `Condition` for sync paths).
- [ ] Each `consumer(...)`'s `cdc_wait`-equivalent path **subscribes to the `LakeWatcher`'s broadcast** instead of running its own `cdc_wait` SQL call. Result: 50 consumers in one process produce **one** poll loop (~1 connection, ~1 QPS at idle backoff), not 50.
- [ ] Per-consumer `cdc_window` / `cdc_commit` still run on each consumer's own dedicated lease-holding connection (per the lease section below). The `LakeWatcher` only amortises the *snapshot-discovery* polling, not the per-consumer reads / commits.
- [ ] **Opt-out:** `consumer(..., shared_watcher=False)` for the rare case of needing isolated polling (e.g. the consumer is in a thread that should not see other consumers' wake events). Documented as power-user; default `True`.
- [ ] **Cross-process is out of scope** — `LakeWatcher` is per-process. Two Python workers in the same Kubernetes pod each get their own `LakeWatcher`. Cross-pod sharing is a v1.0 conversation. Document.
- [ ] Tested with: 50 consumers in one async event loop, all on the same lake; producer commits a snapshot; assert the catalog sees **one** `cdc_wait`-equivalent query around that wake event, not 50.

### Lease handling (the owner-token lease, transparent by default)

The owner-token lease (Phase 1 / ADR 0007) gives the extension single-reader-per-consumer enforcement. The Python client wires it up so users don't have to think about it in the common case.

- [ ] `consumer(...)` opens a dedicated `*duckdb.Connection` for the lifetime of the context. All `cdc_window`, `cdc_commit`, `cdc_consumer_heartbeat`, and `cdc_changes` / `cdc_ddl` reads run on that one connection. This is what makes the same-connection idempotence contract work.
- [ ] **Background heartbeat (always-on):** a daemon thread starts the moment a `consumer(...)` context is entered, calls `cdc_consumer_heartbeat(name)` every `lease_interval_seconds / 3` (default 20s), and stops automatically when the context exits. **Always-on regardless of expected batch duration** — short batches that finish before the next tick pay one extra UPDATE per ~20s of lease, which is cheap; the alternative ("only start heartbeating when the batch exceeds half the lease") requires either user-supplied hints or after-the-fact detection of overrun, both of which are reliably wrong. Tested with a fixture that sleeps long enough to require multiple heartbeats and a fixture that finishes before the first tick (assert: no error, no extra UPDATE if the batch beats the timer).
- [ ] **`CDC_BUSY` raised by `cdc_window`** surfaces as a typed `ducklake_cdc.ConsumerBusyError` carrying the holder's `owner_token`, `owner_acquired_at`, `owner_heartbeat_at`. The error message includes the suggested `cdc_consumer_force_release` recovery.
- [ ] **`CDC_BUSY` raised by `cdc_commit`** (lease lost mid-batch — heartbeat timed out, or operator force-released) surfaces as `ducklake_cdc.LeaseLostError` carrying the new holder's token (if any). Documented as recoverable: the user's batch is not committed; another worker is now the owner; the user's process should exit cleanly.
- [ ] `ducklake_cdc.force_release(catalog, name)` — operator escape hatch matching the SQL function. Documented as "use this when you know the holder is dead."
- [ ] `tail(...)` honors the same lease lifecycle — opens its own dedicated connection, heartbeats on a timer, exits cleanly on `LeaseLostError` (logging the event at WARNING level).

### Schema-boundary handling

With DDL as first-class events (Phase 0 ADR 0008) and `stop_at_schema_change=true` as the default (Phase 0 ADR 0006), schema-boundary handling is no longer a special-case API — it's just the natural flow.

- [ ] When `cdc_window` returns `schema_changes_pending=true`, the consumer reads the bounded window (DML under the current schema), commits, and the next `c.poll(...)` call returns a batch whose `batch.ddl` contains the `altered.table` event(s) at the schema-change snapshot. The consumer's loop applies the DDL via the sink, then processes any DML in that same batch under the new schema.
- [ ] **No special `SchemaChangedBatch` type.** Removed entirely. One concept (DDL events) instead of two.
- [ ] `Batch.is_schema_change: bool` is a computed property (`any(d.event_kind == 'altered' for d in self.ddl)`) for users who want a quick branch.
- [ ] Iterator idempotence: like `cdc_window`, `c.poll(...)` re-yields the same batch if the consumer doesn't commit before calling next(). (Mirrors the SQL-level idempotence contract.)

### Sugar: `tail()` with auto-commit-on-sink-success

```python
import ducklake_cdc
from ducklake_cdc.sinks import Webhook

ducklake_cdc.tail(
    "ducklake:metadata.duckdb",
    name="search-indexer",
    tables=["main.orders"],
    sink=Webhook("https://example.com/hook", retry=3),
)
```

- [ ] For each batch yielded by the underlying `c.poll(...)`, `tail()`:
  1. Calls `sink.apply_ddl(ddl_event)` for each event in `batch.ddl` (in order). On `PermanentError`, optionally DLQ and continue per sink config; on retryable error, retry per `sink.retry_policy`.
  2. Calls `sink.write(batch)` to flush DML rows. Same retry / DLQ policy.
  3. On all success, calls `c.commit(batch.snapshot_id)`.
- [ ] If a sink doesn't implement `apply_ddl` (the protocol's default no-op), `tail()` logs an `INFO`-level message describing the DDL event and proceeds. This is the right default for sinks like stdout / file / webhook where DDL is just another event in the stream.
- [ ] `tail()` is a one-liner because most users want a one-liner. Documented second, not first, so users have a mental model before they reach for the sugar.

### Async story (first-class, not afterthought)

```python
async with ducklake_cdc.aconsumer(...) as c:
    async for batch in c.achanges(follow=True):
        await handle(batch)
        await batch.ack()                    # explicit ack in async land
```

- [ ] `aconsumer`, `alake`, `achanges`, `aevents`, `atail` mirror the sync API one-for-one.
- [ ] Async context cancellation propagates into the underlying `cdc_wait` SQL call within hundreds of milliseconds. Test explicitly. (Same concern as Go's `ctx.Done()` — see Phase 4.)
- [ ] Document the FastAPI / litestar / sanic lifespan integration with a copy-paste example.

### Producer-side helpers

- [ ] `ducklake_cdc.commit_message(con, *, author=None, message=None, extra_info=None)` — thin wrapper around `CALL <lake>.set_commit_message(...)`. Exposed because most Python apps will use it on the producer side too; documented as "this is just DuckLake's existing function, here for symmetry."
- [ ] **`ducklake_cdc.outbox(...)` helper, takes the recommended-convention named args (Phase 0):**

  ```python
  with ducklake_cdc.outbox(
      con,
      event="OrderPlaced",        # recommended-required
      trace_id=tid,               # optional, propagated
      schema="orderplaced.v1",    # optional
      actor=f"user:{user_id}",    # optional
      extra={"region": "eu-west"} # optional, free-form
  ):
      con.execute("INSERT INTO orders VALUES (?, ?)", [oid, payload])
  ```

  Implemented as `BEGIN` + `set_commit_message(extra_info := json.dumps({...}))` + user code + `COMMIT`. **Does not enforce the convention** — `extra` is free-form — but the named args nudge everyone toward the same shape so reference sinks can route on `event`.

### Performance & ergonomics

- [ ] **Arrow zero-copy promoted as the recommended path.** `Batch.to_arrow() -> pyarrow.Table` returns directly from DuckDB without Python row materialization. Document explicitly: "for batches over ~10k rows, use `.to_arrow()`. The `for row in batch.rows:` form is convenient for small workflows and a footgun at scale."
- [ ] Pandas convenience: `Batch.to_pandas()` (lazy import).
- [ ] `Batch.iter_arrow_chunks(chunk_size=...)` for streaming over very large batches without materializing.

### Sink protocol (formalized — published as `ducklake_cdc.Sink`)

The sink protocol is a contract third parties implement. Stability matters. Lock both `write` and `apply_ddl` now even if v0.1 reference sinks have minimal `apply_ddl` implementations — adding it post-1.0 is breaking.

```python
from typing import Protocol
from dataclasses import dataclass

@dataclass
class RetryPolicy:
    max_attempts: int = 3
    backoff_initial_ms: int = 100
    backoff_max_ms: int = 30_000
    jitter: bool = True

class Sink(Protocol):
    retry_policy: RetryPolicy

    async def write(self, batch: "Batch") -> None:
        """Write the batch's DML rows durably.
        Raise on retryable failure; raise PermanentError on DLQ-bound failure.
        """

    async def apply_ddl(self, event: "DDLEvent") -> None:
        """Apply a single DDL event before the batch's DML is written.
        Default implementation logs and returns (most sinks don't need to react).
        Override for sinks that mirror schema (e.g. Postgres mirror, search index provisioning).
        Same retry / PermanentError / DLQ semantics as write().
        """
        ...

    async def close(self) -> None:
        ...

class PermanentError(Exception):
    """Raised by sinks when the failure should not be retried — event goes to DLQ."""
```

- [ ] Document the contract in `docs/python/sinks.md` with examples of writing a custom sink (one DML-only, one DDL-aware).
- [ ] `tail()` honors `RetryPolicy` for both `apply_ddl` and `write`: retry transient errors with exponential backoff; on `PermanentError` or attempts exhausted, write to DLQ via `cdc_dlq_record` (Phase 2 primitive); on success, commit.
- [ ] DLQ records distinguish DDL-failure from DML-failure via an `event_kind` column on `__ducklake_cdc_dlq` (added to the Phase 1 DLQ schema). Operators can query DDL failures separately — they're usually more critical than individual row failures.

### Reference sinks (`ducklake_cdc.sinks`)

DDL-handling support per sink is explicit. v0.1 ships full DML for everything; full DDL only for the trivial-passthrough sinks. Postgres-mirror, Kafka, and Redis DDL handling lands in v0.2 — v0.1 surfaces DDL events but doesn't translate them to target-side DDL.

| sink | DML | DDL in v0.1 |
| --- | --- | --- |
| `Stdout` | full | full (passthrough JSONL with `event_type` discriminator) |
| `File` | full | full (passthrough) |
| `Webhook` | full | full (passthrough JSON with `event_type` discriminator) |
| `Kafka` | full | **DML-only**; DDL events log a warning. v0.2 plan: same topic with `event_type` field, OR optional `--ddl-topic` flag. |
| `RedisStreams` | full | **DML-only**; DDL events log a warning. v0.2 plan: separate stream per category. |
| `Postgres` | full | **DML-only**; DDL events log a warning. v0.2 plan: translate `created.table` → `CREATE TABLE`, `altered.table` → `ALTER TABLE`, etc. on the mirror. Hard work; won't ship until well-tested. |

- [ ] `Stdout` — JSONL, one event per line, no banner garbage on stderr that would trip up shell scripts. JSONL shape:
  ```jsonl
  {"event_type":"ddl","kind":"altered","object_kind":"table","schema":"main","name":"orders","details":{...},"snapshot_id":47}
  {"event_type":"row","change_type":"insert","table":"orders","rowid":42,"data":{...},"snapshot_id":47}
  ```
  The top-level `event_type` discriminator means downstream `jq`/`awk` filters can split DDL from DML trivially.
- [ ] `Webhook` — POST JSON, exponential backoff, **idempotency keys built in**. For DML: `Idempotency-Key: <consumer>:<snapshot_id>:<table>:<rowid>:<change_type>`. For DDL: `Idempotency-Key: <consumer>:<snapshot_id>:ddl:<object_kind>:<object_id>:<event_kind>`. Receivers that bother to dedupe can; we ship it once correctly so no user gets it wrong.
- [ ] `File` — JSONL/Parquet append-mode, useful for backfills and debugging. Same JSONL shape as `Stdout`.
- [ ] `Kafka` (optional extra: `pip install ducklake-cdc[kafka]`). Routes DML on `commit_extra_info.event` when present (per the recommended outbox convention). DDL events in v0.1 log a warning and are dropped (unless `--ddl-topic` is provided as a passthrough escape hatch). Documented v0.2 plan in `docs/python/sinks/kafka.md`.
- [ ] `RedisStreams` (optional extra). Same routing convention; same v0.1 DDL behaviour.
- [ ] `Postgres` (optional extra) — INSERT into a target table for "DuckLake → Postgres mirror" use cases. **DML-only in v0.1.** Documented v0.2 plan: implement `apply_ddl` that translates each `cdc_ddl` event into target-side DDL. Until then, operators wanting full mirroring run a one-off schema-sync script before starting the consumer.

### CLI

The Python CLI is the operator-facing artifact for users in the Python ecosystem. It is *not* the only CLI we ship — the SQL CLI (Phase 1) and the Go binary CLI (Phase 4) are also CLIs. The Python CLI's job is convenience for users who already have `pip`.

- [ ] `python -m ducklake_cdc events ducklake:db.duckdb --consumer indexer` — tail lake-level events.
- [ ] `python -m ducklake_cdc tail ducklake:db.duckdb --table orders --consumer search-indexer --sink stdout`.
  - `--ddl-only` — show only DDL events (consumer is created with `event_categories := ['ddl']` if it doesn't exist).
  - `--no-ddl` — DML only (`event_categories := ['dml']`).
  - default: interleaved, with the JSONL `event_type` discriminator.
- [ ] `python -m ducklake_cdc recent ducklake:db.duckdb --table orders --since 1h` — **thin shell wrapper around the `cdc_recent_changes` SQL sugar function** (which itself expands to `table_changes(table, max(snapshot_id WHERE snapshot_time <= now() - since), current_snapshot())`). No consumer needed. Not a new SQL primitive — see Phase 0 / 1 sugar list.
- [ ] `python -m ducklake_cdc recent-ddl ducklake:db.duckdb --since 1d [--table orders]` — same: shell wrapper around the `cdc_recent_ddl` sugar passthrough.
- [ ] `python -m ducklake_cdc schema-diff ducklake:db.duckdb --table orders --from 47 --to 53` — wrapper around `cdc_schema_diff` sugar.
- [ ] `--sink webhook --url ...`, `--sink kafka --topic ...`, etc.
- [ ] `--follow` to opt into `cdc_wait`-driven long-poll mode.
- [ ] `--format jsonl` (default for stdout sink) — pipeable to `jq`, `awk`, anything Unix.
- [ ] `python -m ducklake_cdc consumers list/reset/drop ducklake:db.duckdb [--name N]`.
- [ ] Config file support: `~/.config/ducklake-cdc/config.toml` and `DUCKLAKE_CDC_CATALOG` env var so operators don't retype catalog URIs.

### Tests

- [ ] Tests against all three catalog backends from Phase 2 (testcontainers for Postgres).
- [ ] **Behavioural parity scenario file shared with the Go client (Phase 4).** Both produce byte-identical output for the same input, including DDL events. Lives at `clients/parity/scenarios/*.yaml`. Used in Python CI and Go CI alike.
- [ ] Schema-boundary test: producer alters a table mid-stream; first batch under `c.poll(...)` has empty `.ddl` and DML up to the boundary; consumer commits; next batch has the `altered.table` event in `.ddl` and post-alter data in `.rows`. Mock sink's `apply_ddl` runs before `write`.
- [ ] DDL-DML interleave-ordering test: snapshot containing both ALTER and INSERT (with `stop_at_schema_change=false` consumer); single batch has both populated; `apply_ddl` runs before `write`.
- [ ] DDL-only consumer test: `event_categories=['ddl']`; batches have populated `.ddl`, empty `.rows`.
- [ ] DLQ test: sink's `apply_ddl` raises `PermanentError`; DDL event lands in DLQ with `event_kind='ddl'`; cursor advances; subsequent `cdc_consumer_stats().dlq_pending` reflects it.
- [ ] Async cancellation test: `asyncio.CancelledError` during `cdc_wait` returns control within 500ms.
- [ ] Forgot-to-commit warning test: exiting a consumer with pending batches emits the documented `RuntimeWarning`.

## Exit criteria

- `pip install ducklake-cdc` works from TestPyPI.
- The 5-line example in the README runs against a fresh DuckLake instance with no extra setup. **The 5-line example uses the explicit iterator + commit pattern, not `tail()`.**
- A separate "go even faster" doc page shows `tail()`.
- 90%+ test coverage on the client surface.
- `--follow` mode survives killed connections and resumes cleanly via `cdc_wait`'s natural retry semantics.
- `Sink` protocol documented with at least two custom-sink examples (one DML-only, one DDL-aware) in `docs/python/sinks.md`.
- Per-sink DDL support matrix published; v0.1 limitations on Kafka / Redis / Postgres mirror DDL handling explicit.
- Streaming demo GIF/video exists once `tail --sink stdout --format jsonl --follow` works: two panes, producer mutates DuckLake, Python client streams interleaved DDL/DML JSONL with the `event_type` discriminator. This becomes the README hero asset candidate; Phase 1 only carries a screenshot walkthrough because SQL output is better read than watched.
- Async surface verified to cancel within 500ms on `asyncio.CancelledError`.
- Parity scenario green against the Go client (post Phase 4) — same input → byte-identical output, DDL included.
- Deployment caveat doc lists the serverless platforms where this won't work and why.
- `LakeWatcher` shared-polling test green: 50 consumers in one process produce one snapshot-poll loop, not 50. Per-process catalog QPS at idle backoff stays under 0.5 regardless of consumer count.
