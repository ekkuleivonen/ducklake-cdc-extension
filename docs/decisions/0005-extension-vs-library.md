# ADR 0005 — Extension vs SQL-helper library

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

The project's "form" question — DuckDB extension or pure SQL-helper
library — is the load-bearing decision that determines who can use the
project from where, what the C++/Rust build cost is, and whether
binding #3 (the third language client) costs days or months.

The earlier framing in `docs/roadmap/README.md` and an early draft of
`docs/roadmap/` argued that the extension wins because
`cdc_wait` is "magic". That framing is dead and is explicitly retired
here. `cdc_wait` is honest exponential-backoff polling; a pure SQL
library could implement the same loop with `WHILE / IF / SLEEP`-pattern
recipes per binding, and the latency story would be identical. Any
argument that rests on "but `cdc_wait` is magic" is wrong on its own
terms.

The real argument for the extension form rests on three load-bearing
claims:

1. **One canonical SQL surface across many bindings.** The same
   `CALL cdc_consumer_create(...)` works identically from `duckdb` CLI,
   Python, Go, R, Rust, Java, Node, and any future binding without
   re-implementation of the cursor write, the lease UPDATE, the
   schema-version bound, or the typed DDL extraction. Each of those
   four primitives is non-trivial; re-implementing them in N languages
   is the cost the extension is designed to avoid.
2. **The SQL CLI persona is the cleanest 60-second demo path.**
   `INSTALL ducklake_cdc FROM community; LOAD ducklake_cdc; CALL
   cdc_consumer_create('demo'); SELECT * FROM cdc_window('demo');` is
   a better first-impression artifact than any `pip install
   ducklake-cdc` story, because it requires zero language ecosystem
   commitment. The SQL CLI persona is also the only persona that can
   demo the project from a `psql` prompt with no scaffolding.
3. **Future pushdown opportunities** for change-feed reads. A
   library-only approach can only compose `snapshots()` and
   `table_changes` from the outside; an extension can register
   table-function variants that participate in DuckDB's predicate
   pushdown and projection-prune machinery, and can later ship
   notification-hook integration once the upstream feature lands. None
   of this is in the v0.1 scope, but the v1.0+ optimisation space is
   only available to the extension form.

The cost statement is the discipline that makes the argument honest.
The extension form is overhead unless the project commits to a
multi-language future. If only Python and Go ever ship, the extension
is overhead — both could call `snapshots()` and `table_changes()`
natively and reach the same place. The extension wins when binding #3
is contemplated and the cost of "do the same thing in C++ that's
already in two other languages" hits.

This ADR therefore commits to a **named third binding** in writing.
The recommendation is **R** — the DuckDB R community overlaps the
analytics audience cleanly and an R binding takes weeks not months
because R has first-class DBI bindings that wrap DuckDB's SQL surface
without needing a per-method C wrapper. If the appetite for the third
binding isn't there, this ADR's argument collapses and the project
should switch to a Python-first plan and revisit how it positions
itself for contributors and users.

## Decision

### The extension is primary

The project ships a DuckDB community extension as the canonical SQL
surface. The Python and Go clients (Phase 3, Phase 4) are bindings
**around** the extension, not re-implementations of its primitives.
Every primitive (`cdc_window`, `cdc_commit`, `cdc_wait`, `cdc_ddl`,
the `cdc_consumer_*` lifecycle, `cdc_consumer_stats`) lives in the
extension; bindings call the same `CALL ...` / `SELECT ...` surface a
SQL-CLI user would call.

### The three claims, locked

1. **One canonical SQL surface.** `CALL cdc_consumer_create('demo')`
   from `duckdb` CLI, from Python's `duckdb.connect()`, from Go's
   `database/sql`, from R's `DBI`, from Java's JDBC, from Node's
   `duckdb` package — produces the same result from the same SQL.
   No per-binding re-implementation of the lease UPDATE (ADR 0007),
   the schema-version bound (ADR 0006), or the typed DDL extraction
   (ADR 0008).
2. **The SQL CLI demo path.** The 60-second README quickstart is a
   `duckdb` CLI session, not a `pip install`. Phase 1 work item:
   the README leads with the `INSTALL ducklake_cdc FROM community`
   one-liner. Phase 1 also ships a 3-minute screencast (commit it
   under `docs/demos/quickstart.gif`).
3. **Future pushdown opportunities.** Documented as Phase 5+ /
   v1.0+ work items but available **only** to the extension form. v0.1
   does not implement them, but the architectural space is preserved.

### Pillar 12 reconciliation (binding form is the same)

`docs/roadmap/README.md` pillar 12 and this ADR say the
same thing. The older "extension wins because of `cdc_wait` magic"
framing is **dead**. The new framing — "one canonical SQL surface across
many bindings" — is the binding one and lives in both places.

When this ADR or pillar 12 is updated, the other follows in the same PR.
Drift between this ADR and pillar 12 is the failure mode
`docs/roadmap/` repeatedly calls out for ADR-pillar
pairs.

### Honest cost statement (must remain visible)

The argument above is only winning if the project ships and maintains
bindings beyond Python and Go. **If only Python and Go ever ship, the
extension is overhead** — both could call `snapshots()` and
`table_changes()` natively and reach the same place. The extension wins
when binding #3 is contemplated and the cost of "do the same thing in
C++ that's already in two other languages" hits.

Adopting the extension as the canonical surface (pillar 12) **commits
the project to a multi-language future**. If that appetite isn't there,
the project should switch to a Python-first plan and revisit how it is
framed for contributors and users.

This statement is non-negotiable in the public-facing positioning. The
README's "use cases" / "languages supported" lines must be honest about
which bindings ship and which are roadmap items, so a reader who only
needs Python doesn't adopt the project under the false impression that
the extension form is paying for itself in their use case.

### Named third binding: R, in the first 12 months post-`v0.1.0`

The third binding is **R**, targeted to ship within 12 months of
`v0.1.0`. Rationale:

- The DuckDB R community overlaps the analytics audience the project
  targets (`docs/roadmap/README.md` — reverse
  ETL, ML feature stores, analytics fan-out).
- R's `DBI` package wraps DuckDB cleanly; the binding is DBI calls
  around the extension's SQL surface, not C wrappers per primitive.
  Estimated implementation effort: **2 weeks** (per ADR 0004's
  "third-binding budget").
- The R community publishes its own conferences and Twitter circles
  that don't overlap heavily with the Python / Go ones; shipping the R
  binding doubles the project's natural-discovery audience for
  near-zero cost.
- An R binding is also the cleanest way to test that the extension
  surface really is binding-agnostic. If the R binding finds an edge
  where the SQL surface assumes a Python or Go idiom, that's a
  Phase 5 polish item the project wants to know about before
  binding #4.

If, by month 12 post-`v0.1.0`, R is unfeasible (DuckDB-R blockers,
maintainer bandwidth), the fallback is **Rust** (`duckdb-rs` extension
support is mature; the binding is a `duckdb` Rust crate wrapper).
**Java** (JDBC + extension function calls) is the further fallback. The
ADR commits to *some* third binding shipping; the specific language is
allowed to slip from R only if R-specific blockers prevent it.

If by month 12 the project realises *no* third binding is feasible
(team bandwidth, ecosystem rejection, DuckDB community pushback), this
ADR's argument has failed and the project must revisit:

- whether to drop the extension form and ship Python and Go as
  thick-client SQL-helper libraries;
- whether to remove the multi-language framing from the README and
  position as "Python and Go CDC for DuckLake";
- whether to retire the project entirely.

The cost discipline cuts both ways: the extension form is **only
worth its build complexity if binding #3 happens**. The 12-month
deadline is not a soft target.

### Counter-argument: pure-SQL recipes as an escape hatch

Every primitive must also be expressible as a documented pure-SQL
recipe so that:

a. Library implementations stay viable as a fallback for environments
   where extension install is blocked (locked-down DuckDB
   distributions, restricted Cloud sandboxes, regulated environments
   where loading community extensions is forbidden).
b. The project can prove the extension is **sugar over `snapshots()` +
   `table_changes` + a state table**, not magic.

The recipes live in `docs/api.md` alongside each primitive's signature.
For each primitive that is pure SQL composition (`cdc_window`,
`cdc_commit`, `cdc_wait`, the `cdc_consumer_*` lifecycle, the audit
emissions), a "pure-SQL recipe" subsection shows the equivalent
hand-written SQL. For `cdc_ddl` (which is the one primitive whose
stage-2 extraction cannot be expressed as one-line sugar — see ADR
0008's proof), the recipe is a multi-page pseudo-SQL transcript with
the explicit caveat: "this is the path you take if you cannot install
the extension and you want typed DDL events; expect ~200 lines of SQL
per event_kind."

The pure-SQL recipes' purpose is **inspectability**, not feature
parity. They are the answer to "is this extension secretly magic?"
(no — it's typed reconstruction over the same catalog tables you can
query directly). They are not a substitute for the extension; users
who go down the recipe path lose all the binding-side conveniences
(typed events, in-process amortisation via `LakeWatcher`, the lease
that prevents two consumers from racing).

## Consequences

- **Phase impact.**
  - **Phase 1** ships the extension as the only deliverable. The
    README quickstart is `duckdb` CLI. The structured-error contract
    (`docs/errors.md`), the API freeze (`docs/api.md`), and the audit
    table (ADR 0010) are all extension-side.
  - **Phase 2** extends the extension to the three-backend matrix
    (DuckDB / SQLite / Postgres — DuckLake's three supported metadata
    backends). The extension form means one C++/Rust extension across
    all three backends; a library form would have meant per-backend
    Python and Go code.
  - **Phase 3 (Python)** and **Phase 4 (Go)** ship bindings that call
    the extension's SQL surface. They do not re-implement the lease,
    the schema-bound, or the DDL extraction.
  - **Phase 5** publishes the demo, the launch post, and (per the
    work-item locked in this ADR) the README that names the third
    binding (R) and its 12-month delivery target.
  - **Post-v0.1.0** the third binding (R) ships within 12 months. If
    it doesn't, this ADR's argument fails and the project re-positions
    per § "Honest cost statement".
- **Reversibility.**
  - **Largely irreversible:** dropping the extension form after
    `v0.1.0` would invalidate every `INSTALL ducklake_cdc FROM
    community` recipe, every binding, and the SQL-CLI demo path —
    effectively a project re-launch.
  - **Two-way doors:** the named third binding (R can slip to Rust /
    Java); the pure-SQL recipe completeness (additional recipes can
    land in patch releases); the post-v0.1.0 timing for the third
    binding (12 months can extend with explicit ADR amendment).
- **Open questions deferred.**
  - **Pushdown integration timeline.** The pushdown opportunities
    are real but not measured. v1.0+ work item; benchmarked
    independently per ADR 0011's discipline.
  - **Notification-hook integration.** Depends on the upstream
    DuckLake hook landing. `docs/upstream-asks.md` (Phase 5) tracks
    the upstream conversation; the extension-side integration is
    post-1.0.
  - **Java / Node / Rust binding ordering.** After R, the order of
    Java / Node / Rust depends on demand; not pre-committed.
  - **Funded development model.** If the project ever takes funding
    (the private maintainer notes § 11 defers this), the funding model interacts with
    the extension-vs-library choice — a SaaS company shipping a
    proprietary extension fork is a business model the extension form
    enables that the library form does not. Out of v0.1 scope.

## Alternatives considered

- **SQL-helper library (Python primary, Go secondary, no extension).**
  Considered as the contrapositive case to the extension form.
  Cheaper to ship for Phase 1 (~40% less C++/Rust work) and faster to
  iterate during Phase 0. Rejected because:
  - **Binding #3 cost.** The lease UPDATE, the schema-version bound,
    and the typed DDL extraction must each be reimplemented per
    binding. Two implementations is manageable; three becomes the
    failure mode where Python and Go agree but R diverges in subtle
    ways and the bug only surfaces in pre-launch testing.
  - **SQL CLI persona.** A library form has no answer for "I want to
    try this from `duckdb` CLI without installing anything." That
    persona is the demo path; losing it is losing the cheapest user
    acquisition channel.
  - **Future pushdown.** Off-the-table for the library form.
- **Extension only, no language clients.** Rejected: the language
  clients ship the `tail()` / `Tail()` ergonomics, the in-process
  `LakeWatcher` amortisation (`docs/roadmap/README.md` point 4), and the typed-event surface that maps cleanly
  to user code. SQL-CLI users get the primitives; production users get
  the bindings.
- **Extension + Python only, drop Go.** Rejected: Go ecosystem
  overlaps the data-engineering audience the project targets
  (the private maintainer notes § 0 — Group B), and shipping Go alongside Python is
  what makes the "binding-agnostic SQL surface" claim demonstrable on
  day one. Two bindings is the minimum that proves the multi-language
  promise.
- **`cdc_wait` magic as the extension's reason to exist.** Rejected
  with prejudice. `cdc_wait` is honest exponential-backoff polling.
  Anyone arguing this framing is pointed at this ADR's Context
  section.

## References

- `docs/roadmap/` — the long-form discussion this ADR
  formalises, with the corrected "one canonical SQL surface"
  framing now leading.
- `docs/roadmap/README.md` — pillar 12 (canonical
  surface). Pillar 12 and this ADR say the same thing.
- ADR 0001 — Extension implementation language (C++ vs Rust). The
  language choice is downstream of this ADR (you only need to make
  it if the form is "extension").
- ADR 0004 — Overlap audit. The 10:5 primitive-vs-sugar ratio is
  the surface-count axis on which this ADR's "extension deserves
  to exist" argument is empirically tested.
- ADR 0006 — Schema-version boundaries (one of the three load-bearing
  pieces of logic that would have to be re-implemented per binding
  in the library-only form).
- ADR 0007 — Concurrency model + owner-token lease (second of the
  three).
- ADR 0008 — DDL as first-class events (third of the three; also the
  one whose stage-2 extraction cannot be expressed as one-line
  sugar).
- ADR 0009 — Consumer-state schema (the canonical schema all bindings
  consume; one definition, not N).
- `docs/roadmap/phase_3_python_client.md` and `docs/roadmap/phase_4_go_client.md`
  — the bindings that consume the extension's SQL surface.
- `docs/api.md` — the API freeze that includes pure-SQL recipes for
  each primitive (per § "Counter-argument").
- `docs/roadmap/` — the launch post and the README copy
  that names the third binding.
