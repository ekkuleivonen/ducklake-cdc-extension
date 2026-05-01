"""Enumerate DuckLake's snapshots().changes MAP keys per operation kind.

For each backend (DuckDB, SQLite, Postgres — DuckLake's three supported
catalog backends):
  1. Attach a fresh DuckLake catalog (DATA_INLINING_ROW_LIMIT = 10).
  2. Run a fixed sequence of DDL / DML / maintenance operations.
  3. After each op, capture the (snapshot_id, changes_made VARCHAR,
     snapshots().changes MAP) emitted by that snapshot.
  4. Write `output/<backend>.json` with the full transcript.

After all backends:
  5. Build a backend-by-backend matrix of "operation kind -> MAP keys
     observed" and write it as `output/snapshots_changes_map_reference.md`.
     This is the snippet committed under
     `docs/api.md#snapshots-changes-map-reference`.
  6. If MAP key sets diverge across backends, exit non-zero. Backend
     divergence is a real finding the project must surface (per ADR 0008's
     "fail the build on any diff" rule once promoted to recurring CI).

Usage:
    uv run python test/upstream/enumerate_changes_map.py --backends duckdb sqlite [postgres]
    uv run python test/upstream/enumerate_changes_map.py --backends duckdb sqlite [postgres] --check
    uv run python test/upstream/enumerate_changes_map.py --update-docs

Exit codes:
    0 = all backends ran cleanly and key sets agree
    1 = at least one backend failed
    2 = backends ran but key sets diverged (the finding case)
    3 = current key sets diverged from committed test/upstream/output/ reference

Phase 0 upstream probe (per docs/roadmap/). Promoted to a recurring CI job in Phase 1; see ADR 0008.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
UPSTREAM_DIR = Path(__file__).resolve().parent
DEFAULT_OUT_DIR = UPSTREAM_DIR / "output"
DOCS_API = REPO_ROOT / "docs" / "api.md"
INLINING_LIMIT = 10  # spec default

# Postgres connection params match `test/upstream/docker-compose.yml`.
PG_DSN = os.environ.get(
    "DLCDC_UPSTREAM_PG_DSN",
    "host=127.0.0.1 port=5433 user=ducklake password=ducklake dbname=ducklake",
)

# ---------------------------------------------------------------------------
# Operation script
# ---------------------------------------------------------------------------


@dataclass
class Op:
    """One operation in the upstream probe script."""

    kind: str  # short identifier for the kind of change
    sql: str  # SQL to execute
    note: str = ""  # human-readable note for the output table
    optional: bool = False  # if True, don't fail the upstream probe if backend rejects
    expect_no_snapshot: bool = False  # for the inline-on-inline case
    pre_snapshot_count: int | None = field(default=None, init=False)


# Ordered sequence. Each op intentionally produces exactly one snapshot
# (or, for the inline-on-inline case, intentionally produces zero — that's
# the documented edge case from docs/roadmap/ "deferred
# to Phase 5" notes).
def operation_script() -> list[Op]:
    return [
        # --- schema lifecycle ---
        Op("created_schema", "CREATE SCHEMA test_s", "schema-level DDL"),
        # --- table creation: main + qualified ---
        Op(
            "created_table_main",
            "CREATE TABLE main.t_main (id INTEGER, name VARCHAR)",
            "table in default 'main' schema",
        ),
        Op(
            "created_table_qualified",
            "CREATE TABLE test_s.t_sub (id INTEGER, val VARCHAR)",
            "table in user-created schema",
        ),
        # --- DML: regular insert above the inlining row limit ---
        Op(
            "inserted_into_table_regular",
            "INSERT INTO main.t_main " "SELECT i, 'name_'||i FROM range(50) AS r(i)",
            f"insert > DATA_INLINING_ROW_LIMIT={INLINING_LIMIT} → goes to Parquet",
        ),
        # --- DML: small insert at-or-below the inlining row limit ---
        Op(
            "inserted_into_table_inlined",
            "INSERT INTO main.t_main VALUES (999, 'tiny')",
            f"single-row insert ≤ DATA_INLINING_ROW_LIMIT={INLINING_LIMIT} → inlined",
        ),
        # --- DML: small delete (≤ DATA_INLINING_ROW_LIMIT delete tombstones) ---
        # Surfaces as `inlined_delete` regardless of whether the targeted
        # rows were originally inlined. The "inlining" decision applies to
        # the *delete tombstone batch*, not the underlying insert.
        Op(
            "deleted_from_table_small",
            "DELETE FROM main.t_main WHERE id < 5",
            f"delete of ≤ {INLINING_LIMIT} rows → inlined_delete tombstone batch",
        ),
        # --- DML: large delete (> DATA_INLINING_ROW_LIMIT tombstones) ---
        # Required to trigger `tables_deleted_from`. Without this op the
        # upstream probe never observes the non-inlined delete MAP key.
        Op(
            "deleted_from_table_large",
            "DELETE FROM main.t_main WHERE id BETWEEN 10 AND 49",
            f"delete of > {INLINING_LIMIT} rows → non-inlined delete file",
        ),
        # --- DML: delete from inlined rows (the inline-on-inline edge case) ---
        # Phase 0 doc cites a spec note predicting "this snapshot looks
        # like there was no DML" because end_snapshot is set on the
        # inlined-insert row directly. Empirically (DuckDB 1.5.2 / DuckLake
        # 415a9ebd) this is FALSE for the cross-transaction case: the
        # delete still produces a snapshot with `inlined_delete`. The spec
        # note may apply only to single-transaction insert+delete; not yet
        # verified by the upstream probe.
        Op(
            "inlined_delete_inline_on_inline",
            "DELETE FROM main.t_main WHERE id = 999",
            "delete targeting an inlined-insert row (spec edge case)",
            optional=True,
        ),
        # Add more rows so we have something non-trivial to compact.
        Op(
            "inserted_into_table_more",
            "INSERT INTO main.t_main " "SELECT i, 'more_'||i FROM range(50, 80) AS r(i)",
            "second non-inlined insert (sets up compaction input)",
        ),
        # --- maintenance: compact the catalog's adjacent files ---
        # We need a dedicated table with multiple Parquet files so the
        # default min/max file-size thresholds don't reject the merge.
        # Set up the table, then force the compaction regardless of file
        # size by passing `min_file_size => 1` (all files become merge
        # candidates).
        #
        # Empirically emits MAP key `merge_adjacent` (NOT `compacted_table`
        # as the spec text key would suggest). Important finding for
        # ADR 0008 — the typed MAP key disagrees with the singular
        # `changes_made` text key on this op.
        Op(
            "_setup_compact_table",
            "CREATE TABLE main.t_compact (id INTEGER, payload VARCHAR)",
            "set up dedicated table for the compaction test",
        ),
        Op(
            "_setup_compact_files_1",
            "INSERT INTO main.t_compact " "SELECT i, repeat('a', 50) FROM range(20) AS r(i)",
            "first Parquet file for compaction",
        ),
        Op(
            "_setup_compact_files_2",
            "INSERT INTO main.t_compact " "SELECT i, repeat('b', 50) FROM range(20, 40) AS r(i)",
            "second Parquet file for compaction",
        ),
        Op(
            "_setup_compact_files_3",
            "INSERT INTO main.t_compact " "SELECT i, repeat('c', 50) FROM range(40, 60) AS r(i)",
            "third Parquet file for compaction",
        ),
        Op(
            "compacted_table",
            "CALL ducklake_merge_adjacent_files(" "'lake', min_file_size => 1, max_file_size => 999999999)",
            "merge adjacent Parquet files (forced via small min_file_size)",
        ),
        # --- DDL: ALTER TABLE variants ---
        Op(
            "altered_table_add_column",
            "ALTER TABLE main.t_main ADD COLUMN extra INTEGER",
            "ADD COLUMN",
        ),
        Op(
            "altered_table_drop_column",
            "ALTER TABLE main.t_main DROP COLUMN extra",
            "DROP COLUMN",
        ),
        Op(
            "altered_table_rename_column",
            "ALTER TABLE main.t_main RENAME COLUMN name TO label",
            "RENAME COLUMN (per spec: surfaces as altered, not renamed)",
        ),
        Op(
            "altered_table_change_type",
            "ALTER TABLE main.t_main ALTER COLUMN id TYPE BIGINT",
            "ALTER COLUMN TYPE",
            optional=True,
        ),
        Op(
            "altered_table_rename_table",
            "ALTER TABLE main.t_main RENAME TO renamed_main",
            "RENAME TABLE (per spec: surfaces as altered, not renamed)",
        ),
        # --- DDL: VIEW lifecycle ---
        Op(
            "created_view",
            "CREATE VIEW main.v1 AS SELECT * FROM main.renamed_main",
            "CREATE VIEW",
        ),
        Op(
            "altered_view",
            "CREATE OR REPLACE VIEW main.v1 AS " "SELECT id FROM main.renamed_main",
            "CREATE OR REPLACE VIEW (the closest DuckDB analogue to ALTER VIEW)",
            optional=True,
        ),
        Op(
            "dropped_view",
            "DROP VIEW main.v1",
            "DROP VIEW",
        ),
        # --- DDL: drops ---
        Op(
            "dropped_table",
            "DROP TABLE main.renamed_main",
            "DROP TABLE",
        ),
        Op(
            "dropped_table_qualified",
            "DROP TABLE test_s.t_sub",
            "DROP TABLE in user-created schema",
        ),
        Op(
            "dropped_schema",
            "DROP SCHEMA test_s",
            "DROP SCHEMA",
        ),
    ]


# ---------------------------------------------------------------------------
# Backend setup — each yields a (con, attach_alias) pair with DuckLake ATTACHed.
# ---------------------------------------------------------------------------


@contextmanager
def _ducklake_con() -> Iterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    try:
        yield con
    finally:
        con.close()


@contextmanager
def backend_duckdb(workdir: Path) -> Iterator[tuple[duckdb.DuckDBPyConnection, str]]:
    catalog = workdir / "duckdb_catalog.duckdb"
    data = workdir / "duckdb_data"
    data.mkdir(parents=True, exist_ok=True)
    with _ducklake_con() as con:
        con.execute(
            f"ATTACH 'ducklake:{catalog}' AS lake " f"(DATA_PATH '{data}', DATA_INLINING_ROW_LIMIT {INLINING_LIMIT})"
        )
        con.execute("USE lake")
        yield con, "lake"


@contextmanager
def backend_sqlite(workdir: Path) -> Iterator[tuple[duckdb.DuckDBPyConnection, str]]:
    catalog = workdir / "sqlite_catalog.sqlite"
    data = workdir / "sqlite_data"
    data.mkdir(parents=True, exist_ok=True)
    with _ducklake_con() as con:
        con.execute("INSTALL sqlite; LOAD sqlite;")
        con.execute(
            f"ATTACH 'ducklake:sqlite:{catalog}' AS lake "
            f"(DATA_PATH '{data}', DATA_INLINING_ROW_LIMIT {INLINING_LIMIT})"
        )
        con.execute("USE lake")
        yield con, "lake"


def _reset_postgres_catalog() -> None:
    """Drop and recreate the public schema so the upstream probe starts from empty."""
    import psycopg

    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
            cur.execute("CREATE SCHEMA public")
        conn.commit()


@contextmanager
def backend_postgres(workdir: Path) -> Iterator[tuple[duckdb.DuckDBPyConnection, str]]:
    _reset_postgres_catalog()
    data = workdir / "postgres_data"
    data.mkdir(parents=True, exist_ok=True)
    with _ducklake_con() as con:
        con.execute("INSTALL postgres; LOAD postgres;")
        # `pg_pool_max_connections` is the real pool-size knob (default 8);
        # `pg_connection_limit` is a separate per-connection-string cap.
        # Both must be raised: the upstream probe accumulates DuckLake-on-Postgres
        # catalog work across ~25 ops in a single connection and exhausts
        # the default pool well before the run completes.
        con.execute("SET pg_pool_max_connections = 64")
        con.execute("SET pg_connection_limit = 64")
        con.execute(
            f"ATTACH 'ducklake:postgres:{PG_DSN}' AS lake "
            f"(DATA_PATH '{data}', DATA_INLINING_ROW_LIMIT {INLINING_LIMIT})"
        )
        con.execute("USE lake")
        yield con, "lake"


BACKENDS: dict[str, Callable[[Path], Any]] = {
    "duckdb": backend_duckdb,
    "sqlite": backend_sqlite,
    "postgres": backend_postgres,
}


# ---------------------------------------------------------------------------
# Per-backend upstream probe run
# ---------------------------------------------------------------------------


def _ext_version(con: duckdb.DuckDBPyConnection, name: str) -> str | None:
    row = con.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = ?",
        [name],
    ).fetchone()
    return row[0] if row else None


def _utc_now_isoformat() -> str:
    """UTC timestamp with second precision and a trailing `Z`. Stable across
    machines / locales so the Phase 1 CI diff job can attribute regressions
    to a single date range. Phase 0 upstream probe work item per the team-lead review
    of `docs/decisions/0009-consumer-state-schema.md`."""
    now = _dt.datetime.now(tz=_dt.timezone.utc).replace(microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _git_sha() -> str | None:
    """Short git SHA at HEAD when the upstream probe is run from inside a working
    tree, else None. Recorded in each backend's JSON so the future CI diff
    job can blame a specific commit when a backend MAP-key set changes
    (per ADR 0008's recurring-CI commitment)."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "--short=12", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    sha = proc.stdout.strip()
    return sha or None


def _platform_label() -> str:
    """Compact OS / arch identifier for the upstream probe's per-backend JSON. Phase
    1 CI runs the upstream probe on Linux x86_64 by default; this label exists so a
    macOS / arm64 maintainer-laptop run is identifiable in the JSON without
    having to grep for it later."""
    sysname = platform.system().lower() or "unknown"
    machine = platform.machine().lower() or "unknown"
    return f"{sysname}-{machine}"


def _max_snapshot_id(con: duckdb.DuckDBPyConnection, alias: str) -> int | None:
    """Latest snapshot id via the DuckLake table function (NOT the catalog
    metadata table). Going through `<alias>.snapshots()` keeps the query
    inside DuckLake's connection management — important for the postgres
    backend, where direct queries against the metadata table take
    connections from the backend extension's pool (default 8) and can
    exhaust it across the upstream probe's ~25 ops."""
    row = con.execute(f"SELECT max(snapshot_id) FROM {alias}.snapshots()").fetchone()
    return None if row is None or row[0] is None else int(row[0])


def _changes_map(con: duckdb.DuckDBPyConnection, alias: str, snapshot_id: int) -> dict[str, list[str]]:
    row = con.execute(
        f"SELECT changes FROM {alias}.snapshots() WHERE snapshot_id = ?",
        [snapshot_id],
    ).fetchone()
    if row is None or row[0] is None:
        return {}
    return {k: list(v) for k, v in dict(row[0]).items()}


def _changes_made_for(
    con: duckdb.DuckDBPyConnection,
    backend: str,
    alias: str,
    snapshot_id: int,
) -> str | None:
    """Fetch the `changes_made` text key for one snapshot from the DuckLake
    catalog metadata table.

    For embedded backends (`duckdb`, `sqlite`) the query goes through the
    DuckDB connection — the metadata table is just another in-process
    relation. For the network-backed `postgres` backend we route around
    DuckDB's per-extension connection pool by using the native driver
    directly: the postgres extension's pool exhausts reproducibly across
    the upstream probe's ~26-op run (the documented `pg_pool_max_connections = 64`
    / `pg_connection_limit = 64` SETs do not prevent accumulation between
    DuckLake bound-function calls and a follow-on direct catalog-table
    query). The native-driver path makes one short-lived connection per
    call and never competes with DuckLake for pool slots.
    """
    if backend in ("duckdb", "sqlite"):
        row = con.execute(
            f"SELECT changes_made "
            f"FROM __ducklake_metadata_{alias}.ducklake_snapshot_changes "
            f"WHERE snapshot_id = ?",
            [snapshot_id],
        ).fetchone()
        return row[0] if row and row[0] is not None else None
    if backend == "postgres":
        # Note: DuckDB's view of the catalog is `__ducklake_metadata_<alias>`;
        # the underlying postgres schema is just `public`. We bypass the
        # DuckLake-on-postgres pool by going direct.
        import psycopg

        with psycopg.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT changes_made " "FROM public.ducklake_snapshot_changes " "WHERE snapshot_id = %s",
                    [snapshot_id],
                )
                row = cur.fetchone()
                return row[0] if row and row[0] is not None else None
    raise ValueError(f"unknown backend {backend!r}")


def run_backend(
    name: str,
    workdir: Path,
    captured_at: str | None = None,
) -> dict[str, Any]:
    cm = BACKENDS[name]
    print(f"[{name}] starting", flush=True)
    with cm(workdir) as (con, alias):
        ducklake_ver = _ext_version(con, "ducklake")
        backend_ext_ver = _ext_version(con, name) if name != "duckdb" else None
        ops_results: list[dict[str, Any]] = []
        prior_snapshot = _max_snapshot_id(con, alias)

        # Capture the seed snapshot too (created when ATTACH initialises the
        # catalog). It's not produced by an op in the script, but it shows
        # the schemas_created=[main] baseline that all backends share.
        if prior_snapshot is not None:
            ops_results.append(
                {
                    "kind": "_seed_attach_initial_snapshot",
                    "sql": "(initial snapshot at ATTACH time)",
                    "note": "baseline snapshot when DuckLake initialises the catalog",
                    "supported": True,
                    "produced_snapshot": True,
                    "snapshot_id": prior_snapshot,
                    "changes_map": _changes_map(con, alias, prior_snapshot),
                    "changes_made": _changes_made_for(con, name, alias, prior_snapshot),
                }
            )

        for op in operation_script():
            try:
                con.execute(op.sql)
            except duckdb.Error as e:
                err = str(e).strip().splitlines()[0]
                ops_results.append(
                    {
                        "kind": op.kind,
                        "sql": op.sql,
                        "note": op.note,
                        "supported": False,
                        "error": err,
                    }
                )
                if not op.optional:
                    print(f"[{name}]   {op.kind}: HARD FAILURE — {err}", flush=True)
                else:
                    print(f"[{name}]   {op.kind}: not supported (optional)", flush=True)
                continue

            new_snapshot = _max_snapshot_id(con, alias)
            if new_snapshot is None or (prior_snapshot is not None and new_snapshot == prior_snapshot):
                ops_results.append(
                    {
                        "kind": op.kind,
                        "sql": op.sql,
                        "note": op.note,
                        "supported": True,
                        "produced_snapshot": False,
                        "changes_map": {},
                        "changes_made": None,
                    }
                )
                continue

            ops_results.append(
                {
                    "kind": op.kind,
                    "sql": op.sql,
                    "note": op.note,
                    "supported": True,
                    "produced_snapshot": True,
                    "snapshot_id": new_snapshot,
                    "changes_map": _changes_map(con, alias, new_snapshot),
                    "changes_made": _changes_made_for(con, name, alias, new_snapshot),
                }
            )
            prior_snapshot = new_snapshot

    return {
        "backend": name,
        "captured_at": captured_at or _utc_now_isoformat(),
        "git_sha": _git_sha(),
        "platform": _platform_label(),
        "python_version": sys.version.split()[0],
        "duckdb_version": duckdb.__version__,
        "ducklake_extension_version": ducklake_ver,
        "backend_extension_version": backend_ext_ver,
        "data_inlining_row_limit": INLINING_LIMIT,
        "operations": ops_results,
    }


# ---------------------------------------------------------------------------
# Output: per-backend JSON + cross-backend Markdown reference + diff check
# ---------------------------------------------------------------------------


def _write_json(out_dir: Path, name: str, result: dict[str, Any]) -> None:
    path = out_dir / f"{name}.json"
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(f"[{name}] wrote {_display_path(path)}", flush=True)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _project_to_key_sets(result: dict[str, Any]) -> dict[str, list[str]]:
    """Return the CI-gating projection: {op_kind: sorted(MAP keys)}.

    Provenance fields in the committed JSONs intentionally change between
    runs, so the recurring gate compares only the surface consumed by the
    future DDL/DML extractor work.
    """
    out: dict[str, list[str]] = {}
    for op in result["operations"]:
        if not op.get("supported", True):
            continue
        if not op.get("produced_snapshot", True):
            continue
        out[op["kind"]] = sorted(op.get("changes_map", {}).keys())
    return out


def _diff_key_sets(results: dict[str, dict[str, Any]]) -> dict[str, dict[str, set[str]]]:
    """Return per-op-kind {backend: {map_keys}} where backends DIFFER on
    the MAP key set, restricted to backends that actually ran the op.

    Backends that hard-failed (`supported = False`) or that produced no
    snapshot for the op are excluded from the comparison. This is what
    makes the divergence signal usable — "Postgres crashed mid-run" is
    not the same finding as "backends agree on what they observe but
    disagree on the keys", and the latter is the one Phase 1's CI gate
    cares about.
    """
    by_kind: dict[str, dict[str, set[str]]] = {}
    for backend, result in results.items():
        for kind, keys in _project_to_key_sets(result).items():
            by_kind.setdefault(kind, {})[backend] = set(keys)
    diffs: dict[str, dict[str, set[str]]] = {}
    for kind, per_backend in by_kind.items():
        if len(per_backend) < 2:
            continue
        unique_keysets = {frozenset(v) for v in per_backend.values()}
        if len(unique_keysets) > 1:
            diffs[kind] = per_backend
    return diffs


def _load_reference_projections(
    backends: list[str],
    reference_dir: Path = DEFAULT_OUT_DIR,
) -> dict[str, dict[str, list[str]] | None]:
    """Load committed projections before a check run overwrites output files."""
    projections: dict[str, dict[str, list[str]] | None] = {}
    for backend in backends:
        reference_path = reference_dir / f"{backend}.json"
        if not reference_path.exists():
            projections[backend] = None
            continue
        reference = json.loads(reference_path.read_text())
        projections[backend] = _project_to_key_sets(reference)
    return projections


def _reference_projection_deltas(
    backend: str,
    current: dict[str, Any],
    reference_projection: dict[str, list[str]] | None,
    reference_dir: Path = DEFAULT_OUT_DIR,
) -> dict[str, dict[str, list[str]]]:
    """Compare one backend's current projection to its committed reference."""
    reference_path = reference_dir / f"{backend}.json"
    if reference_projection is None:
        return {
            "__reference_file__": {
                "added": [],
                "removed": [str(reference_path.relative_to(REPO_ROOT))],
            }
        }

    current_projection = _project_to_key_sets(current)

    deltas: dict[str, dict[str, list[str]]] = {}
    for kind in sorted(set(current_projection) | set(reference_projection)):
        current_keys = set(current_projection.get(kind, []))
        reference_keys = set(reference_projection.get(kind, []))
        added = sorted(current_keys - reference_keys)
        removed = sorted(reference_keys - current_keys)
        if added or removed:
            deltas[kind] = {"added": added, "removed": removed}
    return deltas


def _diff_against_committed_reference(
    results: dict[str, dict[str, Any]],
    reference_projections: dict[str, dict[str, list[str]] | None],
    reference_dir: Path = DEFAULT_OUT_DIR,
) -> dict[str, dict[str, dict[str, list[str]]]]:
    """Return backend -> op_kind -> added/removed MAP keys for reference drift."""
    deltas: dict[str, dict[str, dict[str, list[str]]]] = {}
    for backend, result in sorted(results.items()):
        backend_deltas = _reference_projection_deltas(
            backend,
            result,
            reference_projections.get(backend),
            reference_dir,
        )
        if backend_deltas:
            deltas[backend] = backend_deltas
    return deltas


def _format_reference_drift(deltas: dict[str, dict[str, dict[str, list[str]]]]) -> str:
    out: list[str] = [
        "upstream probe CI gate FAILED - snapshots().changes MAP key set has diverged from",
        "test/upstream/output/ reference.",
        "",
    ]
    for backend, per_kind in sorted(deltas.items()):
        out.append(f"Backend: {backend}")
        for kind, delta in sorted(per_kind.items()):
            if kind == "__reference_file__":
                out.append("  Reference file missing:")
                for path in delta["removed"]:
                    out.append(f"    {path}")
                continue
            out.append(f"  Op kind: {kind}")
            if delta["added"]:
                out.append("    Added keys:   " + ", ".join(delta["added"]))
            if delta["removed"]:
                out.append("    Removed keys: " + ", ".join(delta["removed"]))
        out.append("")
    out.extend(
        [
            "What to do:",
            "  - If this divergence is the upstream change you expected (e.g. you",
            "    bumped the duckdb submodule's ducklake GIT_TAG): run",
            "      uv run python test/upstream/enumerate_changes_map.py \\",
            "          --backends duckdb sqlite postgres --update-docs",
            "    locally, review the resulting diff, and commit the regenerated",
            "    test/upstream/output/ files plus the docs/api.md splice. The DDL /",
            "    DML extractor work item also needs an update; see ADR 0008.",
            "  - If this divergence is unexpected: open a docs/upstream-asks.md",
            "    entry. Backend MAP-key drift between releases is exactly the class",
            "    of upstream behaviour ADR 0008's recurring CI gate exists to surface.",
        ]
    )
    return "\n".join(out)


def _backend_health(results: dict[str, dict[str, Any]]) -> dict[str, dict[str, int]]:
    """Per-backend op tally: ran-successfully, hard-failed, no-snapshot."""
    health: dict[str, dict[str, int]] = {}
    for backend, result in results.items():
        ok = sum(1 for o in result["operations"] if o.get("supported", True) and o.get("produced_snapshot", True))
        failed = sum(1 for o in result["operations"] if not o.get("supported", True))
        no_snap = sum(
            1 for o in result["operations"] if o.get("supported", True) and not o.get("produced_snapshot", True)
        )
        health[backend] = {"ok": ok, "failed": failed, "no_snapshot": no_snap}
    return health


def _render_reference_markdown(results: dict[str, dict[str, Any]]) -> str:
    backends = sorted(results.keys())
    out: list[str] = []
    out.append("<!-- Generated by test/upstream/enumerate_changes_map.py — do not edit by hand. -->")
    out.append("")
    duckdb_versions = sorted({r["duckdb_version"] for r in results.values()})
    ducklake_versions = sorted({r["ducklake_extension_version"] or "?" for r in results.values()})
    captured_ats = sorted({r.get("captured_at") or "?" for r in results.values()})
    git_shas = sorted({r.get("git_sha") or "uncommitted" for r in results.values()})
    platforms = sorted({r.get("platform") or "?" for r in results.values()})
    out.append(
        f"Captured against DuckDB `{', '.join(duckdb_versions)}` "
        f"+ DuckLake extension `{', '.join(ducklake_versions)}` "
        f"with `DATA_INLINING_ROW_LIMIT = {INLINING_LIMIT}`."
    )
    out.append("")
    # Provenance line, scannable for the Phase 1 CI diff job: when a
    # MAP-key set changes between two committed runs, the operator wants
    # to know `(when, on what commit, on what platform)` without having to
    # crack open the per-backend JSON.
    out.append(
        f"Run provenance: `captured_at={', '.join(captured_ats)}` · "
        f"`git_sha={', '.join(git_shas)}` · "
        f"`platform={', '.join(platforms)}`."
    )
    out.append("")
    out.append(
        "Each row is one operation kind from the upstream probe script. The "
        "`changes_made` column shows the per-spec text key as it appears "
        "in `__ducklake_metadata_<lake>.ducklake_snapshot_changes`. The "
        "MAP-key columns show the keys observed in `<lake>.snapshots().changes` "
        "for the same snapshot, per backend."
    )
    out.append("")

    header_cells = ["Kind", "Example `changes_made`"]
    header_cells.extend([f"`snapshots().changes` keys ({b})" for b in backends])
    out.append("| " + " | ".join(header_cells) + " |")
    out.append("| " + " | ".join(["---"] * len(header_cells)) + " |")

    # Use the first backend in alphabetic order as the canonical row order.
    canonical_backend = backends[0]
    canonical_ops = results[canonical_backend]["operations"]
    for canonical_op in canonical_ops:
        kind = canonical_op["kind"]
        cm = canonical_op.get("changes_made")
        cm_cell = f"`{cm}`" if cm else "_(no snapshot produced)_"
        keys_cells = []
        for b in backends:
            ops = {o["kind"]: o for o in results[b]["operations"]}
            op = ops.get(kind)
            if op is None:
                keys_cells.append("_(not run)_")
            elif not op.get("supported", True):
                keys_cells.append("_(not supported)_")
            elif not op.get("produced_snapshot", True):
                keys_cells.append("_(no snapshot)_")
            else:
                keys = sorted(op.get("changes_map", {}).keys())
                keys_cells.append(", ".join(f"`{k}`" for k in keys) if keys else "_(empty MAP)_")
        out.append("| `" + kind + "` | " + cm_cell + " | " + " | ".join(keys_cells) + " |")

    out.append("")
    out.append("### Per-backend health")
    out.append("")
    out.append("| Backend | Ops with snapshot | Hard failures | No-snapshot ops |")
    out.append("| --- | ---: | ---: | ---: |")
    health = _backend_health(results)
    for b in backends:
        h = health[b]
        out.append(f"| `{b}` | {h['ok']} | {h['failed']} | {h['no_snapshot']} |")
    out.append("")
    out.append(
        "Hard failures indicate a DuckLake-on-backend bug; the "
        "operation does not produce a snapshot and the backend is "
        "excluded from divergence comparison for that operation."
    )
    out.append("")
    out.append("### Backend divergence (only backends that ran the op)")
    out.append("")
    diffs = _diff_key_sets(results)
    if not diffs:
        ran_ok = [b for b in backends if health[b]["failed"] == 0]
        if len(ran_ok) == len(backends):
            out.append(
                f"All {len(backends)} backends ({', '.join(backends)}) emit "
                "the same MAP key set for every operation kind. Stage-1 "
                "discovery code is therefore backend-agnostic at the MAP-key "
                "level."
            )
        else:
            out.append(
                f"Among backends that ran each operation successfully, all "
                "agree on the MAP key set. Stage-1 discovery code is "
                "backend-agnostic at the MAP-key level for the operations "
                "every backend can perform."
            )
    else:
        out.append(
            "The following operation kinds emit different MAP keys "
            "across backends that all ran the op successfully. "
            "Stage-1 discovery code MUST handle the union (per "
            "ADR 0008's hard-fail-on-divergence CI gate):"
        )
        out.append("")
        for kind, per_backend in diffs.items():
            out.append(f"- `{kind}`:")
            for b in backends:
                if b not in per_backend:
                    continue
                keys = sorted(per_backend[b])
                out.append(f"  - `{b}`: " + (", ".join(f"`{k}`" for k in keys) if keys else "_(empty MAP)_"))
    out.append("")
    return "\n".join(out)


REFERENCE_BEGIN = "<!-- BEGIN: snapshots-changes-map-reference (auto-generated) -->"
REFERENCE_END = "<!-- END: snapshots-changes-map-reference (auto-generated) -->"


def _update_docs_api_md(reference_md: str) -> bool:
    """Splice the reference into docs/api.md between the BEGIN/END markers.

    Returns True if the file changed.
    """
    text = DOCS_API.read_text()
    block = f"{REFERENCE_BEGIN}\n\n{reference_md}\n{REFERENCE_END}"
    if REFERENCE_BEGIN in text and REFERENCE_END in text:
        before, _, rest = text.partition(REFERENCE_BEGIN)
        _, _, after = rest.partition(REFERENCE_END)
        new_text = before + block + after
    else:
        # Append under the existing anchor stub.
        anchor = '<a id="snapshots-changes-map-reference"></a>'
        if anchor in text:
            new_text = text.replace(
                "_To be populated by the upstream probe output in `test/upstream/enumerate_changes_map.py`",
                block + "\n\n_To be populated by the upstream probe output in `test/upstream/enumerate_changes_map.py`",
                1,
            )
        else:
            new_text = text.rstrip() + "\n\n" + block + "\n"
    if new_text == text:
        return False
    DOCS_API.write_text(new_text)
    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _run_one_backend_in_subprocess(name: str, workdir: Path, out_dir: Path, captured_at: str) -> dict[str, Any] | None:
    """Spawn this script under the active `uv run` Python for one backend
    so each backend gets a fresh Python process with no stale extension
    state. Returns the parsed JSON or None on failure.

    `captured_at` is forwarded to the subprocess so every per-backend JSON
    in one outer run records the same UTC timestamp — makes the diff job's
    "what changed between two runs" attribution unambiguous when several
    backends moved at once.

    Why subprocess isolation: DuckDB's `postgres` extension holds
    process-global state across `duckdb.connect()` instances (notably the
    connection pool). Running multiple backends in one Python process
    therefore pool-starves the network-backed backend when it runs after
    the embedded ones, even though each backend's DuckDB session is
    independent."""

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{name}.json"
    cmd = [
        sys.executable,
        __file__,
        "--internal-single-backend",
        name,
        "--out-dir",
        str(out_dir),
        "--internal-captured-at",
        captured_at,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
        for line in proc.stdout.splitlines():
            print(line, flush=True)
    if proc.stderr:
        for line in proc.stderr.splitlines():
            print(line, flush=True)
    if proc.returncode != 0:
        return None
    if not out_path.exists():
        print(f"[{name}] subprocess returned 0 but {out_path} missing", flush=True)
        return None
    return json.loads(out_path.read_text())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.strip().splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--backends",
        nargs="+",
        choices=sorted(BACKENDS.keys()),
        default=["duckdb", "sqlite"],
        help="Which catalog backends to run against. Postgres requires "
        "`docker compose up -d` from this directory first.",
    )
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument(
        "--update-docs",
        action="store_true",
        help="Also splice the cross-backend reference table into docs/api.md.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Compare current MAP-key projection against committed test/upstream/output/ JSON.",
    )
    parser.add_argument(
        "--keep-workdir",
        action="store_true",
        help="Don't delete the per-run temp dir (useful for debugging).",
    )
    parser.add_argument(
        "--internal-single-backend",
        default=None,
        help=argparse.SUPPRESS,  # internal: subprocess entry point
    )
    parser.add_argument(
        "--internal-captured-at",
        default=None,
        help=argparse.SUPPRESS,  # internal: forwarded UTC timestamp
    )
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Single-backend subprocess mode: do one backend in this process and
    # exit. Keeps DuckDB extension global state from leaking across
    # backends.
    if args.internal_single_backend:
        workdir = Path(tempfile.mkdtemp(prefix="dlcdc-upstream-"))
        try:
            result = run_backend(
                args.internal_single_backend,
                workdir,
                captured_at=args.internal_captured_at,
            )
            _write_json(out_dir, args.internal_single_backend, result)
            return 0
        finally:
            if not args.keep_workdir:
                shutil.rmtree(workdir, ignore_errors=True)

    reference_projections = _load_reference_projections(args.backends) if args.check else {}
    captured_at = _utc_now_isoformat()
    results: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    print(
        f"running {len(args.backends)} backend(s) in isolated subprocesses " f"(captured_at={captured_at})", flush=True
    )
    for name in args.backends:
        result = _run_one_backend_in_subprocess(name, Path("."), out_dir, captured_at)
        if result is None:
            failures.append(name)
        else:
            results[name] = result

    if failures:
        print(f"failed backends: {failures}", flush=True)

    if not results:
        print("no backends ran successfully", flush=True)
        return 1

    reference = _render_reference_markdown(results)
    ref_path = out_dir / "snapshots_changes_map_reference.md"
    ref_path.write_text(reference)
    print(f"wrote {_display_path(ref_path)}", flush=True)

    if args.update_docs:
        changed = _update_docs_api_md(reference)
        print(f"docs/api.md {'updated' if changed else 'unchanged'}", flush=True)

    if failures:
        # Backend hard failures win over projection checks: CI needs to show
        # "backend did not run" separately from "backend ran but keys changed".
        return 1

    if args.check:
        reference_deltas = _diff_against_committed_reference(
            results,
            reference_projections,
        )
        if reference_deltas:
            print(_format_reference_drift(reference_deltas), flush=True)
            return 3

    diffs = _diff_key_sets(results)
    if diffs:
        print(
            f"backend MAP-key divergence in {len(diffs)} op kinds: " + ", ".join(sorted(diffs.keys())),
            flush=True,
        )
        return 2

    print(f"all backends agree on MAP key sets ({len(results)} backends)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
