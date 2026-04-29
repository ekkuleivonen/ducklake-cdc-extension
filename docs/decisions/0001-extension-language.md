# ADR 0001 — Extension implementation language: C++

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

Given that the project ships as a DuckDB community extension (ADR 0005),
the implementation language for the extension itself is one of:

- **C++** — DuckDB's native extension language. The
  [`duckdb/extension-template`](https://github.com/duckdb/extension-template)
  is C++; every official DuckDB extension (httpfs, json, postgres,
  iceberg, ducklake itself) is C++. The community extensions repository
  CI infrastructure (`extension-ci-tools`) is built around C++
  toolchains.
- **Rust** — DuckDB exposes Rust extension support via
  [`duckdb-rs`](https://github.com/duckdb/duckdb-rs)'s
  `vtab`/`scalar_function`/`aggregate_function` macros. Several
  community extensions (e.g. `duckdb_iceberg-rs` work-in-progress, the
  `prql` extension) ship Rust. The path is real but still maturing.

The choice has near-zero impact on the user-facing surface — both
languages produce the same `INSTALL ... FROM community; LOAD ...; CALL
...` experience. The choice has substantial impact on:

- **Build / CI complexity.** C++ rides the
  `extension-ci-tools` infrastructure with little customisation;
  Rust extensions need their own CI matrix entries and the
  community-extensions submission process is less battle-tested for
  Rust extensions.
- **DuckDB upstream integration.** C++ extensions are first-class:
  any DuckDB upstream behaviour change affects them on day one and
  is documented in DuckDB-internal language. Rust extensions
  consume the same upstream changes through `duckdb-rs`'s wrapper
  layer, which lags upstream by some delta (small, but non-zero).
- **Maintainer skill alignment.** The maintainer base for DuckDB
  extensions skews heavily C++. Bug reports and PRs from third
  parties on a Rust extension are likely to need translation. R
  binding work (per ADR 0005's third-binding commitment) is C-API
  consumption either way and is unaffected.
- **Memory-safety / iteration ergonomics.** Rust offers stronger
  compile-time guarantees against the class of bugs that bit DuckDB
  itself in its early years (UAF, double-free, dangling pointers in
  cross-extension boundaries). C++ requires the discipline; Rust
  offers it for free.

The decision must also be reversible-cost-aware: switching from C++ to
Rust mid-Phase-1 is a rewrite. Switching from Rust to C++ is the same.
This ADR locks the choice for the lifetime of the extension form
(i.e., until ADR 0005 is overturned).

## Decision

**Implement the extension in C++** against the DuckDB community
extension template (`duckdb/extension-template`).

The decision rests on three load-bearing factors:

1. **Community-extension CI parity.** The
   `duckdb/extension-ci-tools` submodule path is the
   well-trodden one: GitHub Actions matrices for Linux x86/arm,
   macOS, Windows, and the WASM build are off-the-shelf for C++
   extensions. Rust extensions exist in the matrix too but have
   fewer working examples; the project would spend Phase 1 budget
   debugging CI quirks rather than shipping primitives.
2. **DuckLake-internal API surface.** The
   `__ducklake_metadata_<lake>` catalog tables, the `snapshots()` and
   `table_changes()` table functions, and the version-range fields
   (`begin_snapshot` / `end_snapshot`) on `ducklake_table` /
   `ducklake_column` / `ducklake_view` / `ducklake_schema` are all
   surfaced to extensions through DuckDB's C++ catalog API. The
   stage-2 typed-DDL extraction (ADR 0008) is several hundred lines
   of catalog-walking code; doing it through `duckdb-rs`'s wrapper
   layer adds a translation step at every catalog access. Doing it
   in C++ is direct.
3. **Bug-report / PR triage friction.** The DuckDB community's
   muscle memory for "here's a community extension issue, here's
   how to reproduce" is C++-shaped. A Rust extension is a slower
   path through that triage funnel, and the project's first 12
   months depend on welcoming low-friction third-party
   contribution (the private maintainer notes § 2 — pre-launch community work).

The memory-safety advantages of Rust are real but mitigable:

- The extension's surface area is small (10 primitive callables, 1
  observability function, 5 sugar wrappers per ADR 0004; lifecycle
  CRUD is mostly straight-line code that does not own memory across
  `RETURNING` / table-function boundaries).
- `valgrind` / ASan / UBSan in CI catch the C++-class bugs that
  matter (Phase 1 work item: every CI build runs at least one
  pass under sanitiser).
- The DuckDB project's own C++ codebase has matured to the point
  where the cross-extension boundary surface is well-understood;
  the failure modes that were terrifying in DuckDB v0.5 are
  well-bounded in v1.x.

### Build choice and template alignment

- Use the `duckdb/extension-template` as the starting scaffold, not a
  hand-rolled CMake setup. Phase 1 work item.
- Pin to a specific DuckDB version range in `extension_config.cmake`
  per the catalog-version compatibility ADR (Phase 0 exit criterion;
  documented in `docs/compatibility.md`).
- Submit to `duckdb/community-extensions` once Phase 1's `cdc_window`,
  `cdc_commit`, and lease enforcement are landed and the `light`
  benchmark is green. Pre-submission, ship as a downloadable artifact
  via GitHub releases per `docs/roadmap/`
  § "Release cadence".

### Phase 0 vs Phase 1 skeleton scope (explicit deferral)

The Phase 0 exit criterion ("Project skeleton — build, CI, license,
governance — on `main`") splits as follows. This split is recorded
here so neither phase is closed on a misreading of the criterion.

**Phase 0 ships (closed):**

- `LICENSE` (Apache-2.0) — ratified by ADR 0003.
- `CONTRIBUTING.md` — coding-standard conventions for the C++17
  surface (RAII, header guards, formatting, sanitiser CI matrix
  layout).
- `CODE_OF_CONDUCT.md` — Contributor Covenant 2.1.
- `.github/ISSUE_TEMPLATE/` — bug-report and feature-request
  templates with diagnostic-information requirements aligned to
  `CONTRIBUTING.md` § "Reporting issues".

**Phase 1 ships (deferred, explicitly):**

- Top-level `CMakeLists.txt` instantiated from
  `duckdb/extension-template`.
- `extension_config.cmake` with the DuckLake-version range pinned
  against `docs/compatibility.md`.
- `src/include/ducklake_cdc_extension.hpp` and
  `src/ducklake_cdc_extension.cpp` (the extension entry point and
  function registration).
- `vcpkg.json` (template-default; the extension does not pull
  third-party deps in v0.1).
- `Makefile` (extension-template convenience wrapper for
  `make debug` / `make release` / `make test`).
- `.github/workflows/full-ci.yml` — the C++ build matrix (Linux x86,
  macOS arm64, Windows x86; Clang canonical, GCC + MSVC supported);
  ASan, UBSan, and TSan jobs per the sanitiser commitments above.

**Why deferring the C++ scaffold is the right call.**
`duckdb/extension-template` evolves; the Phase 0 → Phase 1 gap is
short; instantiating the template now without code to exercise it
risks shipping a configuration that Phase 1's first PR has to
replace anyway. The Phase 1 work item that owns the scaffold
(`docs/roadmap/`) has the
template instantiation and the first primitive landing in the same
PR sequence — the scaffold is exercised the moment it lands. That
is the right discipline, not a deferral-of-convenience.

This section is the explicit answer to "is Phase 0 closed even
though there's no `CMakeLists.txt`?" — yes, on the criterion's
*license + governance + skeleton-of-the-non-template parts* sense.
Phase 1's gate is the C++-scaffold parts.

### Compiler / toolchain commitments (the decisions)

These are the load-bearing toolchain choices this ADR locks. Each is a
decision (i.e., a thing future PRs reference rather than relitigate),
not a coding-standard preference:

- **C++17** as the language standard (matches DuckDB's own
  requirement). Promoting to C++20 in a minor release is allowed if a
  compelling DuckDB-side feature requires it; this ADR's amendment
  history would track the move.
- **Clang as the canonical compiler.** GCC and MSVC are supported by
  the template's CI matrix; the project does not introduce
  compiler-specific code paths. Switching the canonical compiler
  would amend this ADR.

### Coding style is not in this ADR

C++ coding-standard items (RAII discipline, header-guard convention,
`std::thread` exclusions, `clang-format` enforcement, sanitiser CI
matrix layout) are **conventions**, not decisions, and live in
[`CONTRIBUTING.md`](../../CONTRIBUTING.md). The split is deliberate:
ADRs lock decisions that future PRs reference rather than relitigate;
coding-standard items are appropriately argued in PR review and
appropriately changed via a `CONTRIBUTING.md` PR (not an ADR
amendment). A future PR proposing, say, a `std::thread` carve-out for
a specific perf-critical path is a `CONTRIBUTING.md` discussion; the
language and toolchain decisions above are unaffected.

## Consequences

- **Phase impact.**
  - **Phase 1** scaffolds against `duckdb/extension-template`,
    implements the primitives in C++17, runs ASan/UBSan in CI, and
    ships the first artifact via GitHub releases (no
    community-extensions submission until `cdc_window`, `cdc_commit`,
    and lease are merged and tested).
  - **Phase 2** (catalog matrix) extends the C++ implementation to
    SQLite and Postgres backends — the per-backend differences
    are SQL dialect choices (`make_interval` spelling, parameter
    markers per the lease UPDATE in ADR 0007), not language
    boundaries.
  - **Phase 5** submits the extension to
    `duckdb/community-extensions` for the launch beta. C++ is the
    well-trodden submission path; the documentation of the submission
    process is up-to-date for C++ extensions and a known-quantity
    process.
- **Reversibility.**
  - **Effectively one-way:** rewriting from C++ to Rust mid-flight is
    months of work and a re-launch event. The decision binds for the
    lifetime of the extension form (i.e., until ADR 0005 is
    overturned).
  - **Two-way:** the C++17 standard (could promote to C++20 in a
    minor release if a compelling DuckDB-side feature requires it);
    the choice of Clang as canonical compiler (could switch to GCC
    if the project ever ships a Linux-only optimisation that
    requires a GCC-specific intrinsic; not anticipated).
- **Open questions deferred.**
  - **WASM build matrix.** DuckDB's WASM build is in scope for
    extensions in principle; whether `ducklake_cdc` ships a WASM
    artifact for v0.1 is a Phase 5 polish question, not a Phase 1
    deliverable. Tracked in `docs/roadmap/`.
  - **DuckLake-extension-internal API stability.** The DuckLake
    extension's catalog-table layout (`ducklake_table`,
    `ducklake_column`, etc.) is the surface this extension reads.
    DuckLake is at v1.0; this extension's catalog-version
    compatibility matrix (`docs/compatibility.md`, Phase 0 exit
    criterion) tracks every DuckLake version we test against.
    DuckLake's commitment to its own catalog-table shapes is what
    bounds the cross-version compatibility cost.
  - **Cross-platform build cost.** macOS arm64, Linux arm64, and
    Windows x86_64 all matter for the SQL-CLI demo persona. Phase 1
    work item: ship at least Linux x86_64 + macOS arm64 in the first
    GitHub release; Windows x86_64 follows by Phase 1 cut.
  - **`duckdb-rs` re-evaluation in v1.0+.** The Rust extension story
    in DuckDB will mature. v1.0+ may revisit whether to ship a
    parallel Rust scaffold for some specific module (e.g., a
    parallel pushdown extension that benefits from Rust's lifetime
    discipline). Not a v0.1 fight.

## Alternatives considered

- **Rust via `duckdb-rs`.** Rejected for the three reasons in
  Decision § "load-bearing factors". The strongest counter would
  have been "Rust is the modern choice and would attract a
  different contributor pool"; that's true but the project's
  pre-launch community work
  (the private maintainer notes § 2) is concentrated in the DuckDB Discord, which is
  where the C++-shaped triage muscle memory is. Adopting Rust would
  be a deliberate choice to shift the contributor target audience
  away from the DuckDB regulars; doing so before having any users
  is a high-risk repositioning.
- **Hybrid (C++ extension scaffold + Rust core via FFI).** Rejected as
  scope creep. The core is small enough that the C++17 implementation
  is straightforward; the FFI boundary would add complexity for a
  benefit that doesn't exist at this surface size. Revisit if the
  extension grows past 10k LoC and needs serious memory-safety
  enforcement on a critical inner loop.
- **Pure Rust extension built outside `duckdb-rs`'s
  `vtab`/`scalar_function` macros.** Rejected: the template's macros
  do real work (parameter binding, type marshalling, error
  propagation through DuckDB's `Notice` / `Error` channels). Skipping
  them is a step backward into the era when extension authors
  re-implemented the same wiring code repeatedly.
- **Zig.** Considered for completeness; rejected as ecosystem-immature
  for production extensions today. Revisit in v1.0+ if the Zig+DuckDB
  story matures (it has not as of the spec used by Phase 0).

## References

- `docs/roadmap/` — the
  language-choice work item this ADR formalises.
- `docs/roadmap/` — implementation
  scaffold, CI matrix, sanitiser builds, and the
  community-extensions submission timing this ADR commits to.
- ADR 0005 — Extension vs SQL-helper library (the form decision this
  ADR is downstream of; if the form ever flips back to library-only,
  this ADR is superseded by being made moot).
- ADR 0008 — DDL as first-class events (the stage-2 extraction logic
  is the largest C++ piece this ADR's "direct catalog access"
  argument is concerned with).
- ADR 0011 — Performance model (the sanitiser / TSan CI matrix is
  what catches the regression class that ADR 0011's benchmark gates
  do not).
- DuckDB extension template
  — <https://github.com/duckdb/extension-template>
- DuckDB extension CI tools
  — <https://github.com/duckdb/extension-ci-tools>
- `duckdb-rs` — <https://github.com/duckdb/duckdb-rs> (the Rust
  alternative this ADR rejected for v0.1).
