"""Pre-built synthetic load corpora: synthesise once, replay cheaply.

Why this module exists
======================

The producer in an e2e example is **test harness, not system under
test**. Its only job is to put rows into the source table. Every
catalog commit a producer does is a catalog commit a consumer
(the actual subject of measurement) doesn't get -- and the postgres
catalog has a hard ceiling on commits/s that all writers contend for
(see the tuning notes in ``05_pipeline_dag/app.py``).

So we want producers that:

1. Do as little Python work in the steady-state loop as possible
   (synthesise rows once at startup, not per-batch).
2. Issue as few catalog commits as possible per row delivered (one
   ``COPY``-shaped INSERT from a pre-built parquet file = one DuckLake
   commit, regardless of file size).
3. Are easy to share across examples 01-05 so each one isn't
   reinventing the same producer shape (synth, ID accounting, restart
   resume, throttle, metrics).

This module is the substrate. An example declares a :class:`LoadShape`
per source table, builds a :class:`LoadCorpus` once at startup, then
either calls :func:`prime` (one big load, for "drain a backlog" demos)
or :func:`replay` (paced commits, for "live load" demos).

The corpus persists under ``e2e/.work/<example>/load/`` and is
idempotent: a second invocation with the same shape and seed reuses
the existing parquet files. A ``--keep-state`` restart picks up at
``MAX(id) + 1`` automatically, so prebuilt files never collide with
already-emitted IDs.

What this module does NOT do
============================

It does not raise the consumer-side commit ceiling. Each consumer
stage still does one ``cdc_commit`` per batch; whatever the lake's
catalog can absorb is what consumers get. The win this module unlocks
is "consumer measurements aren't contaminated by producer competition
for the same catalog" -- you measure the extension more honestly. If
you want to raise the consumer ceiling itself, look at stage-side
commit coalescing (out of scope here).

Usage sketch
============

::

    purchases = LoadShape(
        name="purchases",
        table="lake.raw_purchases",
        parquet_columns=(("payload", "JSON"),),
        synth_row=lambda i, rng: (json.dumps({"kind": ..., "amount": ...}),),
        apply_sql='''
            INSERT INTO {table}
            SELECT
                $id_base + __row_offset AS id,
                payload                 AS payload,
                now()                   AS created_at
            FROM read_parquet('{file}')
        ''',
    )
    corpus = LoadCorpus(purchases, work_dir=load_dir, rows_per_file=5000, file_count=20)
    corpus.prebuild()

    # In a producer thread:
    replay(lake, corpus, stop=stop, interval_s=0.1, recorder=recorder, id_space=id_space)
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from itertools import cycle
from pathlib import Path
from threading import Lock
from typing import Any

import duckdb
from ducklake_cdc_client import prewarm

from _lib.metrics import MetricsRecorder
from _lib.tui import log

# Default size knobs. ``rows_per_file`` controls one apply's footprint
# (and, transitively, one consumer batch's size if the consumer hasn't
# split it). 5000 is a deliberate pick: large enough that the per-apply
# parquet-write + catalog-commit cost dominates the per-row cost (so
# you're measuring the catalog's actual shape rather than Python
# overhead), small enough that a stage's apply latency stays under a
# few seconds even at heavier downstream join cost. Adjust per-shape if
# an example needs different numbers.
DEFAULT_ROWS_PER_FILE = 5000
# ``file_count`` × ``rows_per_file`` = total unique rows in the corpus.
# Replay round-robins, so a long run repeats payloads but advances IDs
# monotonically. 20 files × 5000 rows = 100k unique payloads is enough
# variety that no consumer ever sees the same logical row twice in a
# typical 30s-3min headless run; longer runs do replay payloads, which
# is fine for a synthetic harness.
DEFAULT_FILE_COUNT = 20
# Deterministic seed so the same corpus + same example produces the
# same parquet bytes on every machine -- bench numbers compare cleanly.
DEFAULT_SEED = 42


# ---------------------------------------------------------------------------
# IdSpace -- thread-safe counter for cross-shape coordination
# ---------------------------------------------------------------------------


@dataclass
class IdSpace:
    """Thread-safe high-water mark of emitted IDs from one shape.

    Used to coordinate cross-shape references in :func:`replay`: e.g. a
    refunds shape's apply SQL wants to reference purchase IDs *that
    have actually been emitted in this run*. The producer for the
    upstream shape (purchases) calls :meth:`record_emission`; the
    producer for the downstream shape (refunds) reads
    :meth:`max_emitted` each loop and feeds it as a bound parameter
    via ``extra_params_fn``.

    On a ``--keep-state`` restart, the previous run's rows are still
    in the destination table. Seed from the existing high-water mark
    via :meth:`seed_from` so the downstream producer can immediately
    reference any already-emitted ID, including ones from previous
    runs -- otherwise it would only refer to what this run emitted.
    """

    _max: int = 0
    _lock: Lock = field(default_factory=Lock)

    def record_emission(self, count: int) -> None:
        with self._lock:
            self._max += count

    def seed_from(self, count: int) -> None:
        """Set the initial id-space size (e.g. from existing destination-table rows)."""
        with self._lock:
            self._max = count

    def max_emitted(self) -> int:
        with self._lock:
            return self._max


# ---------------------------------------------------------------------------
# LoadShape -- declarative description of one source table's synthetic load
# ---------------------------------------------------------------------------


SynthRow = Callable[[int, random.Random], tuple[Any, ...]]
"""``synth_row(corpus_index, rng) -> values_tuple``.

Returns one row's parquet-column values, in the order declared by
``LoadShape.parquet_columns``. ``corpus_index`` is monotonic across
the entire corpus (0..N-1 where N = ``rows_per_file * file_count``)
so synth can use it as a stable key if needed (e.g. for cross-shape
references). ``rng`` is a per-corpus seeded RNG -- reuse it for
deterministic builds.

The corpus adds an internal ``__row_offset`` column automatically;
synth doesn't need to produce it. ID and timestamp columns are
typically computed at apply time (see ``apply_sql``), not stored in
the parquet.
"""


@dataclass(frozen=True)
class LoadShape:
    """Declarative description of one synthetic source table's load corpus.

    The corpus pre-builds parquet files at startup (synthesis happens
    once, out of the steady-state path). At apply time, one INSERT per
    file is issued -- one DuckLake commit per file, regardless of file
    size.

    Parameters
    ----------
    name
        Stable identifier for metrics namespacing and corpus filenames,
        e.g. ``"purchases"``. Becomes ``producer_<name>_*`` keys in
        the metrics recorder and ``<name>_NNNN.parquet`` on disk.
    table
        Qualified destination table the apply SQL writes into,
        e.g. ``"lake.raw_purchases"``.
    parquet_columns
        Tuple of ``(column_name, sql_type)`` pairs the corpus
        materialises into the parquet files. Excludes ``__row_offset``
        (the corpus adds that automatically) and excludes any column
        you compute at apply time (typically ``id`` and timestamps).
    synth_row
        Callable that builds one row's parquet-column values. See
        :data:`SynthRow`.
    apply_sql
        SQL template applied per file by ``replay`` and ``prime``.
        Available placeholders (substituted via ``str.format``):

        - ``{table}``  - destination table (= ``self.table``)
        - ``{file}``   - absolute path to the parquet file (or, for
          :func:`prime`, a SQL list of file paths)

        Available bound parameters (passed via the executor):

        - ``$id_base``  - starting ID for this file (integer); compute
          per-row id as ``$id_base + __row_offset``.
        - any extras passed via ``extra_params_fn`` (replay) or
          ``extra_params`` (prime).

        The destination table's ID column should be derived from
        ``$id_base + __row_offset`` so prebuilt corpora are reusable
        across runs and ``--keep-state`` restarts (the runner resumes
        ``id_base`` from ``MAX(id) + 1`` automatically).
    """

    name: str
    table: str
    parquet_columns: tuple[tuple[str, str], ...]
    synth_row: SynthRow
    apply_sql: str


# ---------------------------------------------------------------------------
# LoadCorpus -- materialised set of parquet files for one shape
# ---------------------------------------------------------------------------


@dataclass
class LoadCorpus:
    """A set of pre-built parquet files for one :class:`LoadShape`.

    ``prebuild()`` materialises ``file_count`` parquet files of
    ``rows_per_file`` rows under ``work_dir / shape.name / ``. It is
    idempotent: existing files with the right names are reused. Delete
    the directory or pick a new ``work_dir`` to force a rebuild.

    The corpus is *just data on disk*; it doesn't hold any DuckDB
    state. Multiple producer threads can replay the same corpus
    concurrently if the destination tables can absorb concurrent
    INSERTs (most can; a single-table target with a PK constraint
    would be the exception).
    """

    shape: LoadShape
    work_dir: Path
    rows_per_file: int = DEFAULT_ROWS_PER_FILE
    file_count: int = DEFAULT_FILE_COUNT
    seed: int = DEFAULT_SEED
    _files: list[Path] = field(default_factory=list, init=False, repr=False)

    def total_rows(self) -> int:
        return self.rows_per_file * self.file_count

    def files(self) -> list[Path]:
        if not self._files:
            self._files = [self._file_path(i) for i in range(self.file_count)]
        return list(self._files)

    def prebuild(self) -> list[Path]:
        """Materialise the corpus to disk. Idempotent: keeps existing files.

        Returns the list of file paths in stable (file-index) order.
        Files are named ``<shape.name>_NNNN.parquet`` so a fresh process
        can decide "rebuild or reuse" by name alone. The synthesis loop
        runs in a fresh in-memory DuckDB connection -- no contact with
        the lake at all -- so prebuild can run before the lake is even
        opened.
        """
        corpus_dir = self.work_dir / self.shape.name / f"r{self.rows_per_file}"
        corpus_dir.mkdir(parents=True, exist_ok=True)

        # All files for one corpus share an RNG so the i-th row's
        # synthesis depends on the seed and corpus_index only -- a
        # rebuild produces identical bytes. Per-file isolated RNGs
        # would have the same property but make debugging "row N has
        # the wrong value" harder, since you'd need to know which file
        # contains row N first. One linear stream is simpler.
        rng = random.Random(self.seed)
        builder_conn = duckdb.connect(":memory:")
        try:
            for file_idx in range(self.file_count):
                path = self._file_path(file_idx)
                if path.exists():
                    # Trust the on-disk file: same shape + same seed +
                    # same file index = same bytes by construction. We
                    # still have to advance the RNG so subsequent files
                    # synthesise at the right offset.
                    self._advance_rng_for_file(rng)
                    continue
                self._build_file(builder_conn, path, file_idx, rng)
        finally:
            builder_conn.close()

        self._files = [self._file_path(i) for i in range(self.file_count)]
        return list(self._files)

    def _file_path(self, file_idx: int) -> Path:
        # ``rows_per_file`` is part of the path so a config change
        # (e.g. retuning a producer's batch size between runs) gets a
        # fresh corpus rather than silently reusing files of the wrong
        # size. Without this, ``replay()`` would COPY N rows per apply
        # while only advancing ``id_base`` by ``rows_per_file``, which
        # surfaces as ``cleanP > rawP`` (consumer reads more rows than
        # producer's metric reports). Old corpora are still on disk
        # under their old ``r{N}/`` dir; harmless and easy to ``rm -rf``.
        return (
            self.work_dir
            / self.shape.name
            / f"r{self.rows_per_file}"
            / f"{self.shape.name}_{file_idx:04d}.parquet"
        )

    def _build_file(
        self, conn: duckdb.DuckDBPyConnection, path: Path, file_idx: int, rng: random.Random
    ) -> None:
        # Synthesise rows in Python, then write via DuckDB's UNNEST-of-arrays
        # pattern. Same trick the producers used to use inline; here we
        # pay the cost once at startup and the steady-state path only
        # reads parquet.
        col_arrays: dict[str, list[Any]] = {name: [] for name, _ in self.shape.parquet_columns}
        row_offsets: list[int] = list(range(self.rows_per_file))
        for row_idx in range(self.rows_per_file):
            corpus_idx = file_idx * self.rows_per_file + row_idx
            values = self.shape.synth_row(corpus_idx, rng)
            if len(values) != len(self.shape.parquet_columns):
                raise ValueError(
                    f"shape {self.shape.name!r} synth_row returned "
                    f"{len(values)} values, expected {len(self.shape.parquet_columns)}"
                )
            for (name, _sql_type), value in zip(self.shape.parquet_columns, values, strict=True):
                col_arrays[name].append(value)

        projections = ["UNNEST(?::BIGINT[]) AS __row_offset"]
        params: list[Any] = [row_offsets]
        for name, sql_type in self.shape.parquet_columns:
            projections.append(f"UNNEST(?::{sql_type}[]) AS {name}")
            params.append(col_arrays[name])

        # COPY (...) TO 'path' is the canonical "write parquet without a
        # table" form. Single statement, no temp tables, no implicit
        # ordering surprises -- DuckDB writes the projection rows in
        # the order they're scanned, which for the UNNEST-of-arrays
        # form is the insertion order we built in the loop.
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        try:
            conn.execute(
                f"""
                COPY (
                    SELECT {", ".join(projections)}
                ) TO '{tmp_path}' (FORMAT PARQUET)
                """,
                params,
            )
            tmp_path.replace(path)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _advance_rng_for_file(self, rng: random.Random) -> None:
        """Burn the RNG forward as if we'd synthesised one file's worth of rows.

        Needed when a file already exists on disk: subsequent files'
        synthesis must still start at the correct RNG offset, otherwise
        a partially-built corpus would have correlated payloads across
        rebuilds. The cheapest "advance the RNG" call we can make
        without mirroring synth_row is to call it for the same row
        count and discard the results.
        """
        for row_idx in range(self.rows_per_file):
            self.shape.synth_row(row_idx, rng)


# ---------------------------------------------------------------------------
# prime() -- one transaction, all rows
# ---------------------------------------------------------------------------


def prime(
    lake: Any,
    corpus: LoadCorpus,
    *,
    id_base: int = 0,
    extra_params: dict[str, Any] | None = None,
    recorder: MetricsRecorder | None = None,
    id_space: IdSpace | None = None,
) -> int:
    """Apply the entire corpus in one transaction. Returns rows applied.

    Use this when you want to load the source tables once at startup
    and then measure the consumer's drain throughput in isolation --
    no producer competing for catalog-commit budget while the consumer
    runs. The fastest possible "fill the backlog" mode.

    Implementation: one ``BEGIN`` wrapping one INSERT-per-file plus one
    ``COMMIT``. DuckLake allocates a single catalog snapshot per
    transaction regardless of how many INSERT statements run inside,
    so the producer's catalog footprint here is exactly **one commit**
    independent of corpus size. Each file still produces its own
    parquet rowgroup in storage; that's storage cost, not catalog
    cost, and it isn't the contended resource.

    Per-file ``id_base`` is computed as ``id_base + file_idx *
    rows_per_file``, so the apply SQL's ``$id_base + __row_offset``
    expression yields globally unique IDs across the whole corpus.
    """
    files = corpus.files()
    if not files:
        raise RuntimeError(f"corpus {corpus.shape.name!r} has no files; call prebuild() first")

    rows_per_file = corpus.rows_per_file
    conn = lake.connection.cursor()
    # H-022 pre-warm on the derived cursor (see config.load_cdc_extension).
    prewarm(conn)

    t_start = time.monotonic()
    conn.execute("BEGIN")
    try:
        for file_idx, path in enumerate(files):
            params: dict[str, Any] = {"id_base": id_base + file_idx * rows_per_file}
            if extra_params:
                params.update(extra_params)
            conn.execute(
                corpus.shape.apply_sql.format(table=corpus.shape.table, file=str(path)),
                params,
            )
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:  # noqa: BLE001 - rollback failure is informational only
            pass
        raise
    elapsed_s = time.monotonic() - t_start

    total_rows = rows_per_file * len(files)
    if id_space is not None:
        id_space.record_emission(total_rows)
    if recorder is not None:
        recorder.record_rows(total_rows)
        recorder.set_detail(f"producer_{corpus.shape.name}_total", id_base + total_rows)
        recorder.set_detail(f"producer_{corpus.shape.name}_last_batch", total_rows)
    log(
        f"\\[load:{corpus.shape.name}] primed {total_rows:,} rows from "
        f"{len(files)} files in {elapsed_s:.2f}s (one catalog commit)"
    )
    return total_rows


# ---------------------------------------------------------------------------
# replay() -- paced apply loop, one file per tick
# ---------------------------------------------------------------------------


def replay(
    lake: Any,
    corpus: LoadCorpus,
    *,
    stop: threading.Event,
    interval_s: float,
    recorder: MetricsRecorder,
    id_space: IdSpace | None = None,
    extra_params_fn: Callable[[], dict[str, Any]] | None = None,
    wait_until: Callable[[], bool] | None = None,
) -> None:
    """Paced replay loop: COPY one prebuilt file per ``interval_s`` seconds.

    Cycles through the corpus indefinitely, advancing ``id_base`` by
    ``rows_per_file`` after each successful apply. ID continuity across
    process restarts is automatic: on entry, ``id_base`` is initialised
    to ``MAX(id) + 1`` from the destination table (or 0 on a fresh
    table), so a ``--keep-state`` restart picks up exactly where the
    previous run left off.

    Parameters
    ----------
    lake
        The :class:`ducklake_client.DuckLake` instance. A derived cursor
        is allocated per call so each producer thread has its own
        DuckDB worker pool (and so the H-022 pre-warm has somewhere to
        run).
    corpus
        Pre-built :class:`LoadCorpus`. Caller is responsible for having
        called ``corpus.prebuild()`` before starting the replay thread.
    stop
        Signal: producer exits cleanly on set.
    interval_s
        Sleep between applies. Effective producer cadence is
        ``min(interval_s, per_apply_catalog_cost)`` -- when the catalog
        is the bottleneck, the actual loop runs as fast as it can and
        ``interval_s`` becomes a no-op.
    recorder
        Metrics. Records ``producer_<shape.name>_*`` keys.
    id_space
        Optional :class:`IdSpace`. Incremented by ``rows_per_file``
        after each successful apply, so a downstream shape can read
        :meth:`IdSpace.max_emitted` in its own ``extra_params_fn``.
    extra_params_fn
        Optional callable returning a dict of bound parameters merged
        into the apply call (in addition to the always-present
        ``$id_base``). Called once per apply; useful for cross-shape
        references whose values change over time (e.g. a refund's
        ``$purchase_max`` reads ``id_space.max_emitted()``).
    wait_until
        Optional predicate. If supplied, the loop polls it before each
        apply and skips the apply if it returns False. Used for
        warmup gating ("don't emit refunds until N purchases exist").
    """
    if not corpus.files():
        raise RuntimeError(f"corpus {corpus.shape.name!r} has no files; call prebuild() first")

    conn = lake.connection.cursor()
    prewarm(conn)
    next_id = int(
        conn.execute(
            f"SELECT COALESCE(MAX(id), -1) + 1 FROM {corpus.shape.table}"
        ).fetchone()[0]
    )
    rows_per_file = corpus.rows_per_file

    log(f"\\[load:{corpus.shape.name}] replay started (next_id={next_id})")
    files_iter = cycle(corpus.files())

    while not stop.is_set():
        if wait_until is not None and not wait_until():
            stop.wait(0.1)
            continue

        path = next(files_iter)
        params: dict[str, Any] = {"id_base": next_id}
        if extra_params_fn is not None:
            params.update(extra_params_fn())

        try:
            conn.execute(
                corpus.shape.apply_sql.format(table=corpus.shape.table, file=str(path)),
                params,
            )
        except Exception as exc:  # noqa: BLE001 - producer death must not kill the demo
            recorder.record_error()
            log(
                f"\\[load:{corpus.shape.name}] {type(exc).__name__}: {exc}",
                level="warn",
            )
            stop.wait(0.5)
            continue

        next_id += rows_per_file
        if id_space is not None:
            id_space.record_emission(rows_per_file)
        # Producer commits are real DuckLake writes (one parquet file +
        # one catalog commit per apply), so they belong in the per-second
        # rows count alongside the consumer-side stage commits. Keeping
        # them counted here makes ``throughput.rows_per_s`` a faithful
        # end-to-end measure rather than a consumer-only one.
        recorder.record_rows(rows_per_file)
        recorder.set_detail(f"producer_{corpus.shape.name}_total", next_id)
        recorder.set_detail(f"producer_{corpus.shape.name}_last_batch", rows_per_file)
        stop.wait(interval_s)
    log(f"\\[load:{corpus.shape.name}] replay stopped")
