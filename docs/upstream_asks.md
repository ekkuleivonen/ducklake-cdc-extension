# Upstream Asks

These are DuckLake features or contracts that would make `ducklake-cdc` faster,
safer, or easier to maintain. They are not required for the current SQL surface,
but they would reduce private probing and backend-specific work.

## Stable Metadata State Namespace

`ducklake-cdc` plans to keep its hot-path persistence in a sibling schema in the
DuckLake metadata catalog, rather than in DuckLake-managed user tables. A
documented namespace contract would make that safer:

- How should extensions name private metadata schemas or tables so they do not
  collide with DuckLake internals or future DuckLake features?
- Are non-DuckLake tables in the metadata catalog expected to survive DuckLake
  migrations, cleanup, and attach/detach flows unchanged?
- Can DuckLake expose a supported way to discover the metadata backend type,
  database name, and schema name from SQL?

## Portable Backend DDL Contract

The sibling-schema approach only stays attractive if the extension can create
and update small state tables through DuckDB SQL across DuckDB, SQLite, and
PostgreSQL metadata catalogs. A small documented contract would help:

- Which SQL types should extension-owned metadata tables use for portable
  strings, booleans, integers, timestamps, UUID-like tokens, and repeated values?
- Are conditional updates with row-count checks supported consistently through
  the metadata backend bindings?
- Are `CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`, `ALTER TABLE`,
  and retention deletes expected to work across all supported catalog backends?

## Stable Extension-Level Metadata API

DuckLake has internal C++ classes such as `DuckLakeCatalog`,
`DuckLakeTransaction`, and `DuckLakeMetadataManager` that appear to provide the
right primitives for direct metadata access. They are not a stable
extension-to-extension API today.

Useful upstream options, in increasing order of ambition:

- A SQL function that returns stable metadata-catalog connection facts for an
  attached DuckLake catalog.
- A supported SQL procedure for executing statements in an extension-owned
  metadata namespace without creating DuckLake snapshots.
- A small C API, not a C++ ABI, that lets another extension acquire a metadata
  transaction or run parameterized statements against the metadata backend.

## Event-Driven Wait Hooks

`cdc_wait` currently polls DuckLake snapshots. For PostgreSQL-backed catalogs,
`LISTEN`/`NOTIFY` or an equivalent wakeup channel could make long-polling much
cheaper and lower latency.

The ideal upstream support would be a stable callback, table function, or
notification hook that fires after DuckLake commits a new snapshot. It should
include at least the attached catalog name and committed snapshot id, and it
should not require reading or writing DuckLake internal tables directly.

## CDC-Relevant Semantics

The extension depends on DuckLake snapshot and table-change behavior. These
contracts are especially valuable to document or stabilize:

- The keys that may appear in `snapshots().changes`, including inlined insert
  and delete variants.
- Whether `table_changes()` is the authoritative source for row visibility when
  `snapshots().changes` does not advertise a normal DML key.
- How long snapshot metadata and inlined data remain queryable after cleanup or
  compaction.
- Whether same-snapshot DDL and DML ordering can expose dropped-column old
  values, or whether consumers must continue using time travel recovery.
