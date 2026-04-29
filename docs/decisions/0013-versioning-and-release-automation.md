# ADR 0013 — Versioning and release automation

- **Status:** Accepted
- **Date:** 2026-04-29
- **Phase:** Phase 1 — Extension MVP
- **Deciders:** ekku

## Context

ADR 0012 sets the branching policy (`feature_* -> main -> manual release`)
and the two CI tiers (light on feature branches, full on `main` and
release tags). It deliberately leaves three concrete questions open:

1. **What version string does the binary report?**
   The official DuckDB extension versioning policy
   ([`extensions/versioning_of_extensions`](https://duckdb.org/docs/current/extensions/versioning_of_extensions))
   recognises three stability levels: unstable (git short SHA),
   pre-release (`v0.y.z`), and stable (`vX.Y.Z` with `X >= 1`). The
   build system can stamp this through `EXT_VERSION_<EXT>` automatically
   from `git tag --points-at HEAD || git rev-parse --short HEAD`. We had
   a hard-coded `0.0.0-dev` literal in two C++ files that drifted
   trivially and gave operators a misleading version.
2. **How does the release workflow concretely run?**
   ADR 0012 lists the steps a release workflow "may" do (validate the
   ref, bump or verify the version, tag, GitHub Release, open a
   community-extensions PR). The exact contract — inputs, validations,
   ordering, error modes — needed to be locked in so that the maintainer
   experience is "click `Run workflow`, type the version, done."
3. **Which manual GitHub UI steps are one-time?**
   Branch protection rules, the community-extensions bot token, and the
   first community-extensions PR are all created once and then never
   touched again. They needed a single checklist so a future maintainer
   does not re-derive them from the workflow YAML.

The goal is a release surface where the only artefact a maintainer
edits in steady state is a tag value in the workflow inputs.

## Decision

### Versioning

The version reported by `cdc_version()` and embedded in every
`CDC_INCOMPATIBLE_CATALOG` notice is **whatever
`EXT_VERSION_DUCKLAKE_CDC` resolves to at build time**. The
DuckDB build system stamps this from `git tag --points-at HEAD` if a
tag points at the build commit, else from the short SHA. There is no
in-source version literal to bump. The reported string is always
`ducklake_cdc <version>` where `<version>` is one of:

- a git short SHA on untagged builds (unstable, e.g. `ducklake_cdc 7a3b9c1`)
- a `v0.y.z` tag while the project is pre-1.0 (pre-release, e.g. `ducklake_cdc v0.1.0`)
- a `vX.Y.Z` tag with `X >= 1` once the public API is frozen (stable)

User-facing assertions about the version (tests in `test/sql/`, the
smoke probe in `test/smoke/compat_warning_smoke.py`, examples in
`docs/errors.md`) only pin the `ducklake_cdc ` prefix. The version
segment is a build-stamp that the release workflow updates by tagging.

### Release tag policy

Release tags carry the leading `v`: `v0.1.0`, `v0.1.1`, `v1.0.0`, etc.
The release workflow rejects any version that does not match
`^v(0\.[0-9]+\.[0-9]+|[1-9][0-9]*\.[0-9]+\.[0-9]+)$`. While pre-1.0,
`MAJOR` must stay `0`; after `v1.0.0` the workflow accepts any valid
SemVer with `MAJOR >= 1`.

`MAJOR.MINOR.0` releases are cut from `main`. `MAJOR.MINOR.PATCH` with
`PATCH > 0` are cut from `release/MAJOR.MINOR`. The release workflow
enforces both rules; it refuses to tag a `.0` from a `release/*`
branch and refuses to tag a `.PATCH` (with `PATCH > 0`) from `main`.

### Release workflow contract

`.github/workflows/release.yml` is `workflow_dispatch`-only with four
inputs:

- `version` — the tag to create (required).
- `dry_run` — defaults to `true`. A dry run validates everything,
  prints the plan to the job summary, and exits without pushing tags,
  branches, or PRs. The first run of any new version is a dry run.
- `skip_community_pr` — defaults to `false`. Set to `true` to release
  to GitHub only (e.g. while iterating on the community PR descriptor
  or before the bot token is configured).
- `community_pr_only` — defaults to `false`. Set to `true` with
  `dry_run: false` to retry only the `duckdb/community-extensions` PR
  for an existing release tag. This does not create or push tags,
  maintenance branches, or GitHub Releases, and it does not re-check the
  trigger SHA's `CI Pass` status.

On a non-dry-run, the workflow does this **in order**:

1. Validate the trigger ref is `main` or `release/MAJOR.MINOR`.
2. Validate the version string matches the policy above and matches
   the trigger ref's role.
3. Verify the tag does not already exist (locally or on `origin`).
4. Verify the `CI Pass` check has succeeded on the trigger SHA. This
   is the same aggregator that branch protection requires, so on
   `main` it is always green; on `release/X.Y` it confirms full CI
   has been run there too.
5. Create an annotated tag `vMAJOR.MINOR.PATCH` and push it.
6. If the maintenance branch `release/MAJOR.MINOR` does not exist,
   create it pointing at the same SHA. Existing maintenance branches
   are left untouched (patches must come from them, not main).
7. Create a GitHub Release titled by the tag, using GitHub's
   auto-generated notes driven by `.github/release.yml`'s label
   categories.
8. Open a PR against `duckdb/community-extensions` that updates
   `extensions/ducklake_cdc/description.yml` to point at the new tag's
   SHA. If the descriptor does not yet exist (the very first release),
   the workflow creates it with sensible defaults and the maintainer
   adjusts the maintainer list and description in the PR before
   merging.

Steady-state maintainer experience: open Actions, pick `Release`, type
`v0.2.0`, untick `dry_run`, click `Run workflow`. Done.

If the community PR step fails after the tag and GitHub Release have
already been created, re-run `Release` from a branch that contains the
workflow fix with the same `version`, `dry_run: false`, and
`community_pr_only: true`. The workflow resolves the existing tag SHA
and retries only the fork branch push plus upstream PR creation.

### One-time human setup

These are configured once in the GitHub UI / the repository's secrets
and never touched again. They are not workflows because GitHub does
not let workflows configure their own protection rules.

1. **Branch protection on `main` (Settings → Branches → Add rule):**
   - Pattern: `main`
   - Require a pull request before merging
   - Require status checks to pass: `CI Pass`
   - Require branches to be up to date before merging
   - Require linear history (optional but recommended)
   - Restrict who can push to matching branches (admins only)
2. **Branch protection on `release/**` (same place):**
   - Pattern: `release/**`
   - Require a pull request before merging
   - Require status checks to pass: `CI Pass`
3. **Community bot token (Settings → Secrets → Actions):**
   - Add a repository secret `COMMUNITY_BOT_TOKEN`
   - Use a PAT that can push branches to a fork of
     `duckdb/community-extensions` and open PRs against the upstream. The
     account that owns the PAT must already have that fork. The workflow
     pushes branches to the fork and opens PRs against the upstream via
     `gh pr create --repo duckdb/community-extensions --head OWNER:BRANCH`.
   - If this secret is missing, the release workflow's `community-pr`
     job fails fast with a pointer back to this ADR.
4. **First community descriptor:**
   - Either let the workflow's first non-dry-run release create
     `extensions/ducklake_cdc/description.yml` and adjust it in the
     opened PR, or pre-create it manually in
     `duckdb/community-extensions` before the first release.

Once 1–4 are done, releasing is the `Run workflow` button forever.

### CI tier mapping

ADR 0012 specified the policy. This ADR locks in the workflow files
that implement it:

| File | Trigger | Purpose |
| --- | --- | --- |
| `.github/workflows/light-ci.yml` | `push` to non-main, non-release branches | Tight feedback loop: format, tidy, single Linux debug build, smoke test against `test/sql/ducklake_cdc.test`. |
| `.github/workflows/full-ci.yml` | `pull_request` and `push` to `main` and `release/**`, `tag v*`, `merge_group`, `workflow_dispatch` | Full DuckDB extension matrix via `extension-ci-tools` reusable workflows, plus `format` and `tidy`. Aggregator job `CI Pass` is the single status check branch protection requires. |
| `.github/workflows/benchmark.yml` | `workflow_dispatch` only | Manual benchmark run. It downloads the `linux_amd64` extension artifact from a successful Full CI run and executes the light benchmark workload against the supported official DuckDB release; it can later grow into scheduled/nightly benchmark coverage without making PR and release gates wait for it. |
| `.github/workflows/release.yml` | `workflow_dispatch` only | The release contract above. |
| `.github/release.yml` | (config, not a workflow) | Maps PR labels to changelog sections for auto-generated release notes. |

## Consequences

- The version literal in source no longer drifts from the release tag.
  Releasing is decoupled from a code change; it is a pure git/GitHub
  operation. The compat-warning notice and `cdc_version()` always
  agree because they read the same macro.
- Branch protection requires a single status check name (`CI Pass`),
  so the upstream `extension-ci-tools` matrix can change shape without
  needing the protection rule edited. This keeps the "set up once"
  property intact.
- `dry_run: true` as the default forces the maintainer to opt into a
  destructive action, which is appropriate for the first run of any
  unfamiliar version. Experienced maintainers who know the version is
  correct flip it to `false` and ship.
- The community-extensions PR is opened automatically, but **merged
  manually** by upstream maintainers. The workflow does not bypass
  DuckDB's review process; it just removes the boilerplate of
  cloning, editing the descriptor, and opening the PR.
- One-way doors: release tags are immutable once pushed, so the
  workflow refuses to overwrite an existing tag. To re-release, bump
  the patch version. This is a feature: tags being load-bearing
  artefacts (every community-extensions descriptor pins a SHA) means
  re-using a tag would silently change what users see when they
  `INSTALL ducklake_cdc FROM community`.

Open questions deferred:

- Pre-release tags (`v0.1.0-rc.1`) are not supported by the current
  regex. If we need them, extend the regex and the policy mapping;
  the workflow shape does not need to change.
- Signed tags / signed commits are out of scope for the first release
  cycle but can be added by setting `git config user.signingkey` in
  the workflow and toggling `git tag -s`.
- The community-extensions PAT setup should eventually become a GitHub
  App so it is not tied to a single maintainer's account.

## Alternatives considered

### Keep `0.0.0-dev` as an in-source literal and bump it on release

Familiar from many projects, but it means every release requires both
a tag *and* a commit, the literal can drift from reality on local
builds, and the `cdc_version()` output disagrees with the tag for the
window between bumping the literal and tagging. The DuckDB build system
already stamps `EXT_VERSION_<EXT>` from git for free; using it removes
a class of mistakes for zero added complexity.

### Auto-bump the version from PR labels

Tools like `release-please` can compute the next version from
conventional commit prefixes or PR labels and open the release PR
themselves. The blocking concern: before-1.0, version bumps are policy
choices (does this break the API enough to be `v0.2.0` or is it a
`v0.1.x` patch?), and the maintainer must make that judgement. After
`v1.0.0` we can revisit; the manual `version` input is the simplest
contract that keeps the maintainer in the loop without forcing them to
write the rest of the workflow.

### Run full CI on every push

Captured in ADR 0012. Rejected there because the matrix cost dwarfs
the signal value for in-flight feature work.

## References

- ADR 0012 — Branching, CI, and Release Flow (`docs/decisions/0012-branching-ci-release.md`)
- DuckDB extension versioning: <https://duckdb.org/docs/current/extensions/versioning_of_extensions>
- DuckDB community extensions development: <https://duckdb.org/community_extensions/development.html>
- DuckDB extension CI tooling: <https://github.com/duckdb/extension-ci-tools>
- `.github/workflows/full-ci.yml`
- `.github/workflows/light-ci.yml`
- `.github/workflows/release.yml`
- `.github/release.yml`
