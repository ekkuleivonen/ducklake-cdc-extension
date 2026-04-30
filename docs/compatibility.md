# Catalog-version compatibility matrix

> **Status: Phase 1 — runtime guard live (LOAD-time notice).** This
> file is the load-bearing compatibility matrix between
> `ducklake-cdc` versions and the upstream DuckLake catalog format
> versions they support. The Phase 1 LOAD-time runtime guard
> (`src/compat_check.cpp`) reads the supported set hardcoded in
> `SUPPORTED_DUCKLAKE_CATALOG_VERSIONS` and emits a
> `CDC_INCOMPATIBLE_CATALOG` notice (see `docs/errors.md`) per
> attached DuckLake catalog whose format version sits outside that
> set — *expanding the supported set requires bumping both the C++
> constant AND a column in the matrix below in the same PR*. Refusing
> to register `cdc_*` functions is a partner work item that lands
> with the next `cdc_*` function (consumer-state foundation); today
> only `cdc_version()` is registered, and it stays callable
> regardless of catalog state because it's a build stamp. Phase 2
> populates additional cells as the catalog backend matrix is
> exercised (`docs/roadmap/`). Phase 5 ships the doctor command that
> consults this matrix to render an actionable "you're on an
> unsupported version, here's what to do" report.

## The promise

We lag DuckLake major-version releases by **at most 4 weeks**.
Breaking catalog changes get a major-version bump in the extension.
Non-breaking additions (new tables, new MAP keys handled
gracefully, new optional columns) ship as minor bumps.

The matrix below is the **single source of truth** for "is this
version pair supported." Every release PR updates the matrix
before tagging.

## DuckDB target

The active binary target is **DuckDB v1.5.1**. DuckDB extension binaries are
not cross-version artefacts: a build for DuckDB `v1.5.1` is the only DuckDB
runtime currently marked supported by this branch.

The validated target tuple is:

- DuckDB submodule: `v1.5.1` (canonical source: `.github/duckdb-version`)
- `extension-ci-tools`: `v1.5.1`
- DuckLake: pulled at runtime via `INSTALL ducklake` against the official
  `extensions.duckdb.org` repository for the same DuckDB target. We do not
  pin a DuckLake source commit because we do not compile DuckLake; the
  upstream binary published for our DuckDB version is the one we test
  against.
- DuckLake catalog format observed and guarded below: `0.4`

Older DuckDB versions must be validated as their own tuple before they appear
here. The workflow for adding one is documented in
[`docs/development.md`](./development.md).

## Matrix

Rows are `ducklake-cdc` versions. Columns are **DuckLake catalog
format versions** — the literal string DuckLake stamps into
`__ducklake_metadata_<lake>.ducklake_metadata WHERE key='version'`
on first ATTACH. The runtime guard does an exact-string compare
against this column header set; it does not interpret the value as
a semantic-version range. Cells are one of:

- **`supported`** — fully tested; the runtime guard stays silent.
- **`unsupported`** — runtime guard emits a `CDC_INCOMPATIBLE_CATALOG`
  notice (see `docs/errors.md`) at LOAD time. Once the consumer-state
  slice lands, `cdc_*` calls also raise the structured error.
- **`EOL`** — was supported in a prior `ducklake-cdc` minor;
  superseded by a newer DuckLake catalog format version. Extension
  still loads but emits a deprecation notice on `LOAD ducklake_cdc;`
  (this severity has no users yet — added preemptively for the day
  the second column lands).

| ducklake-cdc \ DuckLake catalog format | `0.4` |
| --- | --- |
| `0.0.x` (development; this is what's on disk for the next ~10 release tags per the release-cadence section of `docs/roadmap/`) | `supported` |
| `0.1.0-beta.x` (forthcoming, Phase 5) | `supported` |

Today the matrix has exactly one column — `0.4` — because that is the only
catalog format observed for the DuckLake commit in the validated DuckDB
`v1.5.1` tuple above. When a DuckLake release that bumps the format version
lands, the workflow is:

1. Test `ducklake-cdc` against the new format. Failures get filed
   against the relevant primitive.
2. Add the new column to this matrix.
3. Append the new version string to
   `SUPPORTED_DUCKLAKE_CATALOG_VERSIONS` in `src/compat_check.cpp`.
4. Ship as a new `ducklake-cdc` patch / minor release.

These four steps land in one PR. **The matrix and the C++ constant
are not allowed to drift.**

### A lesson the previous matrix taught the hard way

The matrix that lived here through Phase 0 had columns
`0.x` / `1.0` / `1.1` / `1.5.x` — *all of them speculative*. The
Phase 1 implementation of the runtime guard discovered, with a
30-second probe against the actual DuckLake extension used by the
supported DuckDB tuple, that the only value DuckLake stamps is `0.4`. None of the
speculative columns existed. The guard would have pointed every
incompatible user at a `docs/compatibility.md` matrix that listed
fictional versions — a worse experience than no guard at all.

The lesson, codified for future maintainers: **never add a column
that hasn't been observed on disk**. Run the probe; copy the
literal string; commit. Speculation about upstream's versioning
scheme belongs in design docs, not in a load-bearing compatibility
matrix that the runtime guard's user-facing message points at.

## What "the catalog format version" means

DuckLake stores a version identifier in the catalog-resident
`__ducklake_metadata_<lake>.ducklake_metadata` row (per the spec).
The extension reads this on `LOAD ducklake_cdc;` (and in a future
slice, on every `cdc_*` call before catalog work) and matches it
against the matrix. The version we match on is the DuckLake catalog
format, **not** the DuckDB version or the DuckLake extension binary
version — the catalog format version is what determines whether our
SQL against `ducklake_*` and `__ducklake_metadata_*` is
well-formed. (The current validated DuckDB `v1.5.1` tuple observes
catalog format `0.4`, empirically; that mapping may change without notice in either
direction in future releases — which is exactly why we match on
the catalog format string, not the extension version.)

The runtime guard is straightforward: a single SELECT against the
metadata row per attached database, a string compare against
`SUPPORTED_DUCKLAKE_CATALOG_VERSIONS`, a `CDC_INCOMPATIBLE_CATALOG`
notice per incompatible attached catalog. The Phase 1 slice ships
the LOAD-time notice path; the call-time error path lands with the
next `cdc_*` function.

## Per-backend caveats

The matrix above is per-DuckLake-catalog-schema-version, but the
project also commits to a backend matrix (DuckDB / SQLite /
Postgres — DuckLake's three supported metadata backends, see
`README.md` § "Backend agnostic by construction"). Full CI runs
the default DuckDB + SQLite upstream probe from
`test/upstream/enumerate_changes_map.py`; run the Postgres leg locally
before changing the committed reference.

Currently known backend caveats:

- **SQLite type collapse.** ADR 0009's type-mapping table lowers
  `__ducklake_cdc_consumers.tables` (a `VARCHAR[]`) to a JSON-text
  column on SQLite. Functionally identical at the binding surface;
  flagged here for operators inspecting the catalog directly.
- **Postgres `JSONB` vs `JSON`.** The extension emits `JSONB` for
  `metadata` on Postgres and the canonical `JSON` type elsewhere.
  ADR 0009 § "Type mapping per backend" explains the lowering.

## Migration discipline

When a catalog format version transition lands upstream:

1. **Phase 0 of every release cycle** runs the upstream probe against the
   new DuckLake version. A divergence (per ADR 0008's recurring CI
   gate) triggers a triage step: bump the supported floor, add a
   new key to the extractor, or reject the upstream change with a
   filed `docs/upstream-asks.md` entry.
2. **The matrix above is updated in the same PR** that ships
   support for the new version. A row gains a new column;
   pre-existing rows pick up `EOL` or `unsupported` cells per the
   compatibility judgement.
3. **The extension's `LOAD` guard** picks up the new column when
   the corresponding string is appended to
   `SUPPORTED_DUCKLAKE_CATALOG_VERSIONS` in `src/compat_check.cpp`.
   The matrix and the C++ constant are kept in sync by convention,
   not generation — the constant is intentionally short, narrow,
   and uninteresting so a one-line edit per release stays
   forgettable. A future release can switch to a generated header
   sourced from this matrix when the supported set grows beyond ~5
   entries; today the trade-off favours zero codegen.

## See also

- ADR 0001 — extension language (the C++ codebase that owns the
  runtime guard).
- ADR 0009 — consumer-state schema § "Type mapping per backend"
  (the per-backend lowering this matrix's per-backend caveats
  cross-reference).
- `docs/roadmap/` § first work item — implements
  the `LOAD`-time guard.
- `docs/roadmap/` — Phase 2 work item that populates this
  matrix based on the catalog-matrix CI runs.
- `docs/roadmap/` — the operator-facing
  consumer of this matrix at runtime.
- [`docs/upstream-asks.md`](./upstream-asks.md) — the open issues
  blocking matrix completion.
- DuckLake spec — Catalog Metadata.
