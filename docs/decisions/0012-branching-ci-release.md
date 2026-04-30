# ADR 0012 — Branching, CI, and Release Flow

- **Status:** Accepted
- **Date:** 2026-04-29
- **Phase:** Phase 1 — Extension MVP
- **Deciders:** ekku

## Context

The project is a DuckDB extension with a heavier release surface than a
typical Python library: local debug builds are cheap enough for ordinary
development, but the full DuckDB extension matrix spans multiple operating
systems, architectures, and eventually Wasm. Running that matrix for every
feature branch would slow iteration and burn CI time.

At the same time, `main` needs a clear trust meaning. DuckDB Community
Extensions publish from exact source references in the extension's source
repository, and those refs are expected to build with DuckDB's
`extension-ci-tools`. Once `ducklake_cdc` is submitted to the community
repository, a release ref from this repository is also a distribution ref.

The project therefore needs a simple branch policy that keeps feature work
cheap while ensuring `main` is always fully working. The trust boundary should
be the required full CI gate before merge, not a long-lived integration branch.

## Decision

Use protected-trunk development with two CI levels:

```text
feature_* -> main -> manual release
```

### Branches

- `feature_*` branches are short-lived work branches created from `main`.
- `main` is the protected trunk. It must be fully working at all times.
- New code reaches `main` through pull requests that pass full CI before merge.
- Release tags are created manually from `main`.
- `release/0.x` branches are maintenance branches for already-published lines,
  not pre-release staging branches.

Pre-beta, there is normally no release branch. After an external release line
exists, create `release/0.x` only when `main` has moved on and that published
line needs a patch release. Hotfixes for an older line land on the relevant
`release/0.x` branch, are tagged from that branch, and are then merged or
cherry-picked back to `main` so trunk does not drift.

### CI

We run a single CI workflow on every push, modelled after
`duckdb/ducklake`'s `MainDistributionPipeline`. There is no
"light vs full" tier, because we no longer compile DuckLake from source —
the canonical extension-matrix build is fast enough that every push pays
the same cost.

Each push runs:

- DuckDB's `extension-ci-tools` full distribution matrix (binaries for
  every supported DuckDB platform)
- format + tidy
- a Linux debug integration smoke job (SQL tests, Python C++ harness
  probes, catalog matrix smoke against DuckDB / SQLite / Postgres)
- the upstream DuckLake contract check
- benchmark and soak gates as they become release blockers

Main branch protection requires the `CI Pass` aggregator job. A feature
branch may be noisy while CI is red; `main` may not.

The DuckDB target version lives in `.github/duckdb-version`. Bumping that
file is how we change release lines.

### Release automation

Manual release workflows run from `main` only for the active line, or from a
`release/0.x` branch for a maintenance patch. A release workflow may:

- validate that the selected ref is allowed for release
- bump or verify the extension version
- create a release tag
- create a GitHub Release
- produce GitHub release artifacts
- open a pull request to DuckDB Community Extensions with the exact source ref
  for the descriptor

The workflow opens the community-extension PR; it does not bypass DuckDB's
review process.

### DuckDB Community Extensions

Community-extension publication points at exact refs from this repository.
Those refs must come from `main` release tags for the active line, or from
`release/0.x` tags for maintained older lines.

During DuckDB release transitions, the project may add a temporary
DuckDB-next branch for compatibility work. If the community descriptor needs
both `ref` and `ref_next`, `ref` points at the current stable-compatible
release ref, and `ref_next` points at the DuckDB-next compatibility branch or
commit. This is an exception for DuckDB release compatibility, not a
replacement for the normal `feature_* -> main` flow.

## Consequences

This makes the day-to-day workflow easy to explain: feature branches get fast
feedback, and `main` stays trustworthy because merge attempts pay the full CI
cost.

The trade-off is that pull requests to `main` are more expensive than feature
branch pushes. That is intentional. The project avoids maintaining a separate
integration branch and makes the merge gate the source of truth.

This policy is reversible. It does not affect the extension's API, on-disk
state, or user-visible behavior. If the project later grows enough to need
mandatory stabilization branches, this ADR can be superseded without changing
shipped artifacts.

Open questions deferred:

- The exact contents of light and full CI may change as build times and test
  flakiness become measurable.
- Release branches should remain maintenance-only until external users depend
  on released tags.
- DuckDB-next branch naming can be chosen when the first release-transition
  compatibility cycle happens.

## Alternatives considered

### `feature_* -> dev -> main`

Feature branches merge to `dev`, `dev` runs a medium CI gate, and `dev` later
merges to `main` for the full CI gate. This is familiar and can reduce how
often full CI runs, but it adds a second long-lived branch whose meaning is
mostly "not yet fully trusted." For this project, the protected `main` merge
gate is the clearer trust boundary.

### Stabilization release branches

Cutting `release/0.x` before every release and hardening there keeps `main`
open for new work while the release stabilizes. That is useful for larger
teams with parallel release trains, but it is unnecessary coordination before
the project has that pressure. `release/0.x` branches are still useful as
maintenance branches once an already-published line needs patch support.

### Run full CI on every feature branch

This gives the strongest signal but wastes the most time. The full DuckDB
extension matrix is valuable precisely because it is expensive; it should gate
promotion to `main`, not every local iteration.

## References

- `CONTRIBUTING.md`
- `docs/development.md`
- `docs/roadmap/` § Distribution
- DuckDB Community Extension Development:
  <https://duckdb.org/community_extensions/development.html>
- DuckDB extension CI tooling: <https://github.com/duckdb/extension-ci-tools>
