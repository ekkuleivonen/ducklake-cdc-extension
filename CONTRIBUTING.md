# Contributing

This project is an early community extension. The local build and test loop is
in [`docs/development.md`](./docs/development.md); this file covers
contribution policy and review expectations.

## License

Contributions are accepted under the project's
[Apache-2.0 license](./LICENSE) via the standard
["inbound = outbound"](https://www.apache.org/licenses/contributor-agreements.html)
GitHub norm: opening a PR represents that the contribution is
licensed under the project's license. There is no separate CLA.

## Branching and CI

The project uses a simple branch and release flow:

```text
feature_* -> main -> manual release
```

- Create feature branches from `main`.
- Open feature PRs from `feature_*` branches into `main`.
- Feature branches run day-to-day CI for fast feedback.
- `main` is protected and must pass the required CI gate before merge.
- Release tags are created manually from `main` or the relevant maintenance
  branch.
- `release/0.x` branches are maintenance branches for already-published lines,
  not pre-release staging branches.

The CI levels are intentionally simple: day-to-day CI stays cheap, while the
full DuckDB extension distribution matrix runs as a release gate. If an older
released line needs a patch after `main` has moved on, the patch lands on the
relevant `release/0.x` branch and is then merged or cherry-picked back to
`main`.

## C++ code style

The extension is implemented in C++17 against the
[`duckdb/extension-template`](https://github.com/duckdb/extension-template)
scaffold. The items below are coding-standard conventions that apply at PR
review time. Disagreement with a convention is appropriately resolved in PR
discussion or a CONTRIBUTING.md PR.

### Memory and lifetime

- **No raw `new` / `delete` outside RAII.** Allocations go through
  DuckDB's existing arena types (`unique_ptr<...>`, `shared_ptr<...>`,
  `data_t *` from the buffer manager). The cross-extension lifetime
  surface is where memory bugs land in C++ extensions; RAII discipline
  at every boundary is the project's bulwark.
- **No `using namespace duckdb;` at file scope.** Per the
  `extension-template`'s convention; keeps the extension's symbols
  separable.
- **No header-guard skipping.** Every header carries `#pragma once`
  *and* an `#ifndef` guard; the dual-guard convention matches DuckDB's
  own headers and survives unusual include orders.

### Threading

- **No threading primitives outside DuckDB's `TaskScheduler` /
  `Mutex` wrappers.** Raw `std::thread` / `std::mutex` is rejected at
  PR review. TSan-clean is the contract; DuckDB's wrappers are what
  satisfy it across the matrix.

### Formatting

- **`clang-format` enforced in pre-commit.** The
  `extension-template` ships a `.clang-format`; the project uses it
  without modification.
- The CI matrix runs the `clang-format --dry-run --Werror` gate; PRs
  that don't format cleanly fail before any build runs.

### Linting and sanitisers

- ASan + UBSan run as release-time smoke coverage. The lease contention
  scenarios are where deeper sanitizer work is most likely to pay off.
- New code that introduces a sanitiser-clean carve-out (a
  `__attribute__((no_sanitize(...)))` block, a TSan exclusion) must
  document the reason inline and reference the issue or design note
  that justified it.

## Reporting issues

Use [GitHub issues](https://github.com/<repo>/issues) for bug reports
and feature requests. Include the extension version
(`SELECT extension_version FROM duckdb_extensions() WHERE
extension_name = 'ducklake_cdc'`), the DuckDB version, the catalog
backend (DuckDB / SQLite / Postgres), and a minimal reproducer when
applicable.

For documentation issues, start with [`docs/design.md`](./docs/design.md),
[`docs/roadmap.md`](./docs/roadmap.md), and
[`docs/hazard-log.md`](./docs/hazard-log.md).
