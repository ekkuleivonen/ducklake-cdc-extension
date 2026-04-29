# Phase 4 — Go client + standalone CLI binary

**Goal:** Go module mirroring the Python surface, idiomatic to Go (channels, contexts, errors as values, typed structs), composing our primitives with DuckLake builtins, interleaving typed DDL events from `cdc_ddl` with DML from `cdc_changes` per the per-snapshot ordering contract. Plus a single statically-distributable `ducklake-cdc` CLI binary that bundles the extension and is the operator-facing artifact for users who don't want a Python install.

Same non-overlap principle as Python: the client wraps `cdc_window` / `cdc_wait` / `cdc_ddl` / `cdc_changes` plus DuckLake's `snapshots()` / `table_changes` / `set_commit_message`. Nothing is reimplemented.

## Lock first: the iterator-error contract

Go's "loop yields values" pattern has three plausible shapes for error handling and they all have real partisans. Lock the choice in this phase, document the rationale.

- **Choice (locked): `Err()` after the loop, à la `bufio.Scanner`.**

  ```go
  for batch := range c.Changes(ctx, ducklakecdc.PollOpts{...}) {
      if err := process(batch); err != nil { ... }
      if err := c.Commit(ctx, batch.SnapshotID); err != nil { ... }
  }
  if err := c.Err(); err != nil {
      // stream-level error: gap, schema mismatch, catalog dropped, etc.
  }
  ```

- **Why `Err()` and not range-over-func with `(batch, err)`:** the `Err()` pattern is the standard library's most universally recognised streaming idiom — `bufio.Scanner`, `sql.Rows`, `csv.Reader` all use it. Go users read it without thinking. Range-over-func is also fine ergonomically, but it's a younger idiom that splits the audience between users who recognise it instantly and users who reach for the spec. The `Err()` choice is about reach, not version compatibility.
- **Minimum Go version: 1.23+.** Go 1.22 is past EOL (Feb 2026); supporting it would mean writing around iter.Seq and the rest of 1.23's vocabulary for no real audience. We *use* range-over-func internally where it's clean (e.g. iterating DDL events inside a batch); the *public* iterator surface uses channels + `Err()` for the reach reason above.
- Document the rationale in `clients/go/docs/design.md`. This is a load-bearing API decision; users will hate switching it later.

## Work items

### Module layout

- [ ] `clients/go/`, `go.mod`, package `ducklakecdc`.
- [ ] Use `github.com/marcboeker/go-duckdb` (or the current canonical bindings) and load the extension on `sql.Open`.
- [ ] **`ducklakecdc.Preload(ctx, db)`** — analogous to Python's `preload()`. Operators call this at startup to download the extension before serving traffic.
- [ ] Module publishing: tag releases as `clients/go/v0.1.0` (Go module path discipline).

### Lead-line API: explicit iterator + commit

```go
c, err := ducklakecdc.NewConsumer(ctx, "ducklake:metadata.duckdb", ducklakecdc.Options{
    Name:                 "search-indexer",
    Tables:               []string{"main.orders"},
    ChangeTypes:          []ducklakecdc.ChangeType{ducklakecdc.ChangeInsert, ducklakecdc.ChangeDelete},
    EventCategories:      []ducklakecdc.EventCategory{ducklakecdc.CategoryDDL, ducklakecdc.CategoryDML}, // default
    StopAtSchemaChange:   true,                                                                          // default
})
if err != nil { ... }
defer c.Close()

for batch := range c.Poll(ctx, ducklakecdc.PollOpts{MaxSnapshots: 100}) {
    // DDL first, per ADR 0008 ordering contract
    for _, ddl := range batch.DDLEvents {
        if err := handleDDL(ctx, ddl); err != nil { ... }
    }

    for _, row := range batch.Rows {
        if err := process(ctx, row); err != nil { ... }
    }
    if err := c.Commit(ctx, batch.SnapshotID); err != nil { ... }
}
if err := c.Err(); err != nil { ... }
```

- [ ] **`Batch.DDLEvents []DDLEvent` and `Batch.Rows []Row`** are both populated. Either may be empty for a given batch. Per the ordering contract, callers process `DDLEvents` before `Rows`.
- [ ] **`Consumer.Poll(...)` is the unified iterator** that interleaves `cdc_ddl` and `cdc_changes` outputs per snapshot. Old-style `Consumer.Changes(...)` and new `Consumer.DDL(...)` are retained as DML-only / DDL-only views. Docs lead with `Poll`.
- [ ] **No `SchemaChangedBatch` type.** Removed. Schema changes are just `altered.table` events in `Batch.DDLEvents`.
- [ ] `Batch.IsSchemaChange() bool` — convenience boolean.

- [ ] **Lake-level events** (uses `cdc_events` sugar):

  ```go
  lake, err := ducklakecdc.OpenLake(ctx, "ducklake:metadata.duckdb", ducklakecdc.LakeOptions{
      Consumer: "indexer",
  })
  defer lake.Close()

  for ev := range lake.Events(ctx, ducklakecdc.PollOpts{MaxSnapshots: 100}) {
      if changed, ok := ev.Changes["tables_changed"]; ok && contains(changed, "orders") {
          ...
      }
      if err := lake.Commit(ctx, ev.SnapshotID); err != nil { ... }
  }
  if err := lake.Err(); err != nil { ... }
  ```

  `ev.Changes` mirrors the structured map DuckLake's `snapshots()` returns.

- [ ] **Long-poll mode** uses `cdc_wait` on a dedicated underlying `*sql.Conn` (never a pool slot):

  ```go
  for batch := range c.Changes(ctx, ducklakecdc.PollOpts{Follow: true, IdleTimeout: 30 * time.Second}) {
      ...
  }
  ```

### `LakeWatcher`: shared snapshot polling across consumers in one process

This is **the single highest-leverage Phase 4 work item for production deployments.** Without it, 50 consumers in one Go binary produce 50× the catalog `cdc_wait` polling load. With it, they share **one** poll loop. Mirrors the Python design (Phase 3); same contract; specced in `README.md` "Performance principles" → contract 4.

- [ ] `ducklakecdc.LakeWatcher` is a per-`(catalog_uri)` singleton (registry keyed on the URI; `sync.Map` or equivalent). Created lazily on first `NewConsumer(...)` for that catalog; reference-counted; the goroutine is torn down when the last consumer using it `Close()`s.
- [ ] Internally: a single dedicated `*sql.Conn` per `LakeWatcher` runs the polling loop against `current_snapshot()`. On wake, broadcasts via a `sync.Cond` or a slice of subscriber `chan int64` (the design that makes 50 subscribers cheap, not the design that allocates per wake).
- [ ] Each `Consumer.Wait(ctx)` and the `Follow: true` polling path subscribe to the `LakeWatcher`'s broadcast instead of running their own `cdc_wait` SQL call. Result: 50 consumers in one process produce **one** snapshot-poll goroutine, not 50.
- [ ] `cdc_window` / `cdc_commit` still run on each consumer's own dedicated lease-holding `*sql.Conn`. Only snapshot-discovery polling is amortised.
- [ ] **Opt-out:** `ducklakecdc.Options{SharedWatcher: false}` for the rare case of needing isolated polling. Default `true`.
- [ ] **Cross-process is out of scope** — `LakeWatcher` is per-process. Document.
- [ ] Tested with: 50 `Consumer`s on the same lake; producer commits a snapshot; assert via instrumented `*sql.Conn` that the catalog sees **one** `current_snapshot()` query around that wake event, not 50.
- [ ] `--metrics-port` (the Phase 4 CLI Prometheus endpoint) exposes `lakewatcher_subscribers` and `lakewatcher_polls_total` so operators can confirm the amortisation in production.

### Lease handling (the owner-token lease, transparent by default)

The owner-token lease (Phase 1 / ADR 0007) gives the extension single-reader-per-consumer enforcement. The Go client wires it up so users don't have to think about it in the common case.

- [ ] `NewConsumer(...)` opens a dedicated `*sql.Conn` for the consumer's lifetime. All `cdc_window`, `cdc_commit`, `cdc_consumer_heartbeat`, and `cdc_changes` / `cdc_ddl` reads run on that one connection. This is what makes the same-connection idempotence contract work; the connection is released on `Consumer.Close()`.
- [ ] **Background heartbeat (always-on):** a goroutine started by `NewConsumer` calls `cdc_consumer_heartbeat(name)` on the dedicated connection every `lease_interval_seconds / 3` (default 20s). Stopped on `Close()`. Always-on regardless of expected batch duration; mirror of the Python client (Phase 3 §"Background heartbeat") — short batches that finish before the next tick pay one tiny UPDATE per ~20s of lease, which is the right trade vs. the impossible-to-get-right alternative of "detect overrun and start heartbeating then."
- [ ] **`CDC_BUSY` raised by `cdc_window`** surfaces as `*ducklakecdc.ConsumerBusyError` carrying `OwnerToken`, `OwnerAcquiredAt`, `OwnerHeartbeatAt` typed fields. `errors.Is(err, ducklakecdc.ErrConsumerBusy)` works for callers that don't care about the details.
- [ ] **`CDC_BUSY` raised by `cdc_commit`** (lease lost mid-batch) surfaces as `*ducklakecdc.LeaseLostError`. `errors.Is(err, ducklakecdc.ErrLeaseLost)` works analogously. Documented as recoverable: the user's batch is not committed; the user's process should exit cleanly.
- [ ] `ducklakecdc.ForceRelease(ctx, db, name)` — operator escape hatch matching the SQL function.
- [ ] `Tail(...)` honors the same lease lifecycle — opens its own dedicated connection, heartbeats, exits cleanly on `*LeaseLostError` (logged at WARNING).

### Context cancellation propagates into `cdc_wait`

This is the Go-specific UX expectation that, if violated, gets you angry blog posts.

- [ ] When `ctx` is cancelled, the underlying `cdc_wait` SQL call must abort within hundreds of milliseconds — not seconds. Implementation: spawn a goroutine that watches `ctx.Done()` and calls `(*sql.Conn).Raw` to invoke DuckDB's interrupt API.
- [ ] Test explicitly: `ctx, cancel := context.WithCancel(...); go func(){ time.Sleep(50 * time.Millisecond); cancel() }(); start := time.Now(); _ = c.Wait(ctx); assert time.Since(start) < 500 * time.Millisecond`.

### Schema-boundary handling

With DDL as first-class events and `StopAtSchemaChange=true` as default, schema-boundary handling is just the natural flow: the schema change appears as an `altered.table` event in `Batch.DDLEvents` at the start of the new-schema window. No special types.

- [ ] `Batch.IsSchemaChange() bool` returns true when any DDL event in the batch is `Altered + Table`.
- [ ] Iterator idempotence: like `cdc_window`, `Poll` re-yields the same batch if the consumer doesn't commit before the next iteration.

### Strong typing

- [ ] `ChangeType` enum (`ChangeInsert`, `ChangeUpdatePreimage`, `ChangeUpdatePostimage`, `ChangeDelete`).
- [ ] `EventCategory` enum (`CategoryDDL`, `CategoryDML`).
- [ ] `DDLEvent` struct mirroring `cdc_ddl` output:

  ```go
  type DDLEvent struct {
      SnapshotID  int64
      EventKind   DDLEventKind   // Created, Altered, Dropped (per spec: RENAME = Altered)
      ObjectKind  DDLObjectKind  // Schema, Table, View
      SchemaID    sql.NullInt64
      SchemaName  string
      ObjectID    int64
      ObjectName  string
      Details     json.RawMessage // shape locked in ADR 0008
  }
  ```

  ```go
  // Typed accessors for common shapes:
  func (e *DDLEvent) AsCreatedTable() (CreatedTableDetails, error)
  func (e *DDLEvent) AsAlteredTable() (AlteredTableDetails, error)
  // Convenience predicate for the common rename case (which is an Altered event):
  func (e *DDLEvent) IsTableRename() bool   // true when EventKind=Altered, ObjectKind=Table, details.old_table_name != details.new_table_name
  func (e *DDLEvent) IsViewRename() bool    // analogous for View
  ```

  Typed accessors avoid forcing every consumer to re-parse the JSON. The raw `Details` is exposed for forward compatibility.
- [ ] `Batch.CommitMessage`, `Batch.CommitExtraInfo []byte`, with `Batch.UnmarshalExtraInfo(&v)` helper for typed JSON metadata.
- [ ] `Batch.ToArrow() arrow.Record` (via `arrow-go`) for high-throughput sinks. Returns DML rows only; DDL events stay as `[]DDLEvent` because they're heterogeneous.

### Producer-side helpers — typed `OutboxMeta`

The Phase 0 outbox metadata convention is enforced as a typed struct here, not a free-form `map[string]any`. Free-form maps are how schema fragmentation starts.

```go
type OutboxMeta struct {
    Event   string         // recommended-required
    TraceID string         // optional, propagated by reference sinks
    Schema  string         // optional, e.g. "orderplaced.v1"
    Actor   string         // optional, e.g. "user:42"
    Extra   map[string]any // optional, free-form
}

err := ducklakecdc.WithOutbox(ctx, db, ducklakecdc.OutboxMeta{
    Event:   "OrderPlaced",
    TraceID: traceID,
}, func(tx *sql.Tx) error {
    _, err := tx.ExecContext(ctx, "INSERT INTO orders ...")
    return err
})
```

- [ ] `ducklakecdc.SetCommitMessage(ctx, db, ducklakecdc.OutboxMeta{...})` — thin wrapper around `CALL <lake>.set_commit_message(...)` that serializes `OutboxMeta` per the recommended convention.
- [ ] `ducklakecdc.WithOutbox(ctx, db, meta, fn)` — runs `fn` inside a transaction with `set_commit_message` pre-applied.

### Sink interface (formalized — published as `ducklakecdc.Sink`)

Lock both `Write` and `ApplyDDL` now even if v0.1 reference sinks have minimal `ApplyDDL` implementations.

```go
type Sink interface {
    // Write writes the batch's DML rows durably.
    // Return *PermanentError to send the batch (or individual rows) to DLQ.
    Write(ctx context.Context, batch *Batch) error

    // ApplyDDL applies a single DDL event before the batch's DML is written.
    // Default implementations log and return nil. Override for sinks that
    // mirror schema (e.g. Postgres mirror, search index provisioning).
    ApplyDDL(ctx context.Context, event *DDLEvent) error

    Close() error
}

type RetryPolicy struct {
    MaxAttempts    int
    BackoffInitial time.Duration
    BackoffMax     time.Duration
    Jitter         bool
}

type PermanentError struct{ Err error }
func (e *PermanentError) Error() string { return e.Err.Error() }
```

- [ ] `Tail(ctx, catalog, opts, sink)` is the sugar form. For each batch:
  1. Call `ApplyDDL` for each event in `batch.DDLEvents` (in order). Retry transient errors per `RetryPolicy`; on `*PermanentError` or attempts exhausted, DLQ with `event_kind=ddl` and continue.
  2. Call `Write(batch)` for DML. Same retry / DLQ semantics with `event_kind=dml`.
  3. On all success, `Commit`.
- [ ] Documented in `clients/go/docs/sinks.md` with two custom-sink examples (one DML-only, one DDL-aware Postgres mirror as reference).

### Reference sinks (`clients/go/sinks/`)

DDL support per sink mirrors Python (see Phase 3 matrix). v0.1 ships full DML for everything; full DDL only for trivial-passthrough sinks.

- [ ] `stdout` — JSONL with `event_type` discriminator (full DDL passthrough).
- [ ] `webhook` — POST JSON, exponential backoff, `Idempotency-Key` header. Full DDL passthrough.
- [ ] `file` — JSONL/Parquet append. Full DDL passthrough for JSONL; DDL events skipped in Parquet mode (heterogeneous shapes don't fit Parquet).
- [ ] `kafka` (separate Go module to keep core dependency footprint small). Routes DML on `OutboxMeta.Event` when present. **DML-only in v0.1**; DDL events log a warning. Optional `--ddl-topic` passthrough escape hatch documented for v0.1.
- [ ] `redisstreams` (separate Go module). Same v0.1 DDL behaviour.
- [ ] `postgres` — `INSERT INTO` mirror. **DML-only in v0.1.** Documented v0.2 plan: translate `created.table` → `CREATE TABLE`, `altered.table` → `ALTER TABLE`, etc.

### Standalone CLI binary

The Go binary is the operator-facing CLI for users who don't want to install Python. It statically links DuckDB and the extension where possible.

- [ ] `go install ./cmd/ducklake-cdc` produces a single binary.
- [ ] `ducklake-cdc tail --catalog ducklake:db.duckdb --table orders --consumer my-tail --sink stdout`
  - `--ddl-only` — DDL only (`event_categories := ['ddl']`).
  - `--no-ddl` — DML only (`event_categories := ['dml']`).
  - default: interleaved with `event_type` discriminator in JSONL.
- [ ] `ducklake-cdc events --catalog ducklake:db.duckdb --consumer indexer`
- [ ] `ducklake-cdc recent --catalog ducklake:db.duckdb --table orders --since 1h` — **thin shell wrapper around the `cdc_recent_changes` SQL sugar function**, not a new SQL primitive. (Same for `recent-ddl` below; see the sugar list in Phase 0 / 1.)
- [ ] `ducklake-cdc recent-ddl --catalog ducklake:db.duckdb --since 1d [--table orders]` — shell wrapper around the `cdc_recent_ddl` sugar passthrough.
- [ ] `ducklake-cdc schema --catalog ... --table orders --at-snapshot 47` — show the table's schema at a specific snapshot. Pure passthrough over `ducklake_table` + `ducklake_column` at the snapshot bounds.
- [ ] `ducklake-cdc schema-diff --catalog ... --table orders --from 47 --to 53` — wrapper around the `cdc_schema_diff` sugar.
- [ ] `ducklake-cdc consumers list/reset/drop/create --catalog ... [--name N]`
- [ ] `ducklake-cdc snapshots --catalog ...` — pure passthrough to DuckLake's `snapshots()`, useful diagnostic.
- [ ] `--sink kafka://broker/topic`, `--sink webhook://example.com/hook`, `--sink redis-stream://host/key`, `--sink postgres://user@host/db?table=mirror`.
- [ ] `--follow` for `cdc_wait`-driven long-poll.
- [ ] `--format jsonl` (default for stdout sink). The JSONL shape is self-discriminating via top-level `event_type`:
  ```jsonl
  {"event_type":"ddl","kind":"altered","object_kind":"table","schema":"main","name":"orders","details":{...},"snapshot_id":47}
  {"event_type":"row","change_type":"insert","table":"orders","rowid":42,"data":{...},"snapshot_id":47}
  ```
- [ ] Config file: `~/.config/ducklake-cdc/config.toml` and `DUCKLAKE_CDC_CATALOG` env var.
- [ ] **`--metrics-port 9090` exposes `/metrics`** (Prometheus). Operators will deploy this in k8s; making them write a wrapper to scrape `cdc_consumer_stats()` is hostile.
- [ ] Document CGO/static-linking caveats per platform in `clients/go/README.md`.

### Tests

- [ ] Run the Phase 2 catalog matrix via testcontainers-go.
- [ ] **Behavioural parity test:** shared scenario file with the Python client (`clients/parity/scenarios/*.yaml`); both produce byte-identical output for the same input, including DDL events.
- [ ] DDL+DML interleave-ordering test: snapshot containing both ALTER and INSERT (with `StopAtSchemaChange=false`); single batch has both populated; `ApplyDDL` runs before `Write`.
- [ ] DDL-only consumer test: `EventCategories=[CategoryDDL]`; batches have populated `DDLEvents`, empty `Rows`.
- [ ] DLQ test: sink's `ApplyDDL` returns `*PermanentError`; DDL event lands in DLQ with `event_kind=ddl`.
- [ ] Context-cancellation latency test: `cdc_wait` aborts within 500ms of `cancel()`.
- [ ] CLI smoke tests against the embedded DuckDB catalog.

## Exit criteria

- `go get github.com/<org>/ducklake-cdc/clients/go@v0.1.0` compiles cleanly on Linux/macOS, **Go 1.23+** (1.22 is past EOL — see iterator-error rationale).
- `go install .../cmd/ducklake-cdc@v0.1.0` produces a working binary on Linux (x86_64, arm64) and macOS (x86_64, arm64).
- Behavioural parity with the Python client verified by the shared scenario file.
- `cdc_wait` cancellation latency under 500ms on cancelled contexts (test enforced in CI).
- CGO / static-linkage caveats for DuckDB documented in `clients/go/README.md` per platform.
- `Sink` interface stable, with at least two custom-sink examples (one DML-only, one DDL-aware) in `clients/go/docs/sinks.md`.
- Per-sink DDL support matrix published; v0.1 limitations on Kafka / Redis / Postgres mirror DDL handling explicit.
- Iterator-error pattern (`Err()` after the loop) documented and consistent across `Consumer.Poll`, `Consumer.Changes`, `Consumer.DDL`, `Lake.Events`, and any future iterators.
- `--metrics-port` Prometheus endpoint exposes the full `cdc_consumer_stats()` surface plus `lakewatcher_*` counters.
- `LakeWatcher` shared-polling test green: 50 consumers in one process produce one snapshot-poll goroutine, not 50. Per-process catalog QPS at idle backoff stays under 0.5 regardless of consumer count.
