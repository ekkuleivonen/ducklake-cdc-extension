#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/bump-duckdb-version.sh [--no-submodules] vX.Y.Z

Updates the DuckDB version tuple used by this repository:
  - .github/duckdb-version
  - duckdb Python package pins
  - extension-ci-tools workflow refs and ci_tools_version inputs
  - .gitmodules extension-ci-tools branch
  - active-target notes in docs/development.md

By default, the script also fetches and checks out matching tags in the
duckdb/ and extension-ci-tools/ submodules. Use --no-submodules to only edit
text files.
EOF
}

update_submodules=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-submodules)
      update_submodules=false
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

duckdb_tag="$1"
if [[ ! "$duckdb_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "DuckDB version must be a stable tag like v1.5.2" >&2
  exit 2
fi

duckdb_version="${duckdb_tag#v}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

cd "$repo_root"

printf '%s\n' "$duckdb_tag" > .github/duckdb-version

python3 - "$duckdb_tag" "$duckdb_version" <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

tag = sys.argv[1]
version = sys.argv[2]


def update(path: str, replacements: list[tuple[str, str]]) -> None:
    file_path = Path(path)
    text = file_path.read_text()
    original = text
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text, flags=re.MULTILINE)
    if text != original:
        file_path.write_text(text)


update(
    "pyproject.toml",
    [(r'"duckdb==[0-9]+\.[0-9]+\.[0-9]+"', f'"duckdb=={version}"')],
)

update(
    "clients/python/pyproject.toml",
    [(r'"duckdb==[0-9]+\.[0-9]+\.[0-9]+"', f'"duckdb=={version}"')],
)

for workflow in (".github/workflows/ci.yml", ".github/workflows/release.yml"):
    update(
        workflow,
        [
            (
                r"duckdb/extension-ci-tools/\.github/workflows/([^@\s]+)@v[0-9]+\.[0-9]+\.[0-9]+",
                rf"duckdb/extension-ci-tools/.github/workflows/\1@{tag}",
            ),
            (r"ci_tools_version: v[0-9]+\.[0-9]+\.[0-9]+", f"ci_tools_version: {tag}"),
        ],
    )

update(
    ".gitmodules",
    [(r"(\s*branch = )v[0-9]+\.[0-9]+\.[0-9]+", rf"\1{tag}")],
)

update(
    "docs/development.md",
    [
        (
            r"(The active development target is \*\*DuckDB )v[0-9]+\.[0-9]+\.[0-9]+(\*\*)",
            rf"\1{tag}\2",
        ),
        (
            r"(`duckdb/` submodule: DuckDB `)v[0-9]+\.[0-9]+\.[0-9]+(`)",
            rf"\1{tag}\2",
        ),
        (
            r"(`extension-ci-tools/` submodule and reusable workflows: `)v[0-9]+\.[0-9]+\.[0-9]+(`)",
            rf"\1{tag}\2",
        ),
        (
            r"(DuckDB `)v[0-9]+\.[0-9]+\.[0-9]+(`\n)",
            rf"\1{tag}\2",
        ),
    ],
)
PY

if [[ "$update_submodules" == true ]]; then
  if git -C duckdb rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -C duckdb fetch --tags origin "$duckdb_tag"
    git -C duckdb checkout "$duckdb_tag"
  else
    echo "warning: duckdb/ submodule is not initialized; skipped checkout" >&2
  fi

  if git -C extension-ci-tools rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -C extension-ci-tools fetch --tags origin "$duckdb_tag"
    git -C extension-ci-tools checkout "$duckdb_tag"
  else
    echo "warning: extension-ci-tools/ submodule is not initialized; skipped checkout" >&2
  fi
fi

echo "Updated DuckDB tuple to $duckdb_tag."
echo "Review with: git diff -- .github/duckdb-version pyproject.toml clients/python/pyproject.toml .github/workflows .gitmodules docs/development.md duckdb extension-ci-tools"
