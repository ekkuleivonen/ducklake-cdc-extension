# 01 &mdash; Schema-Safe Consumer

The first demo should prove the most important promise: a DML consumer does
not silently cross a schema boundary and corrupt its sink.

## Story

A producer writes orders. A consumer maintains a derived table. Mid-run, the
producer changes the source schema. The DML consumer drains only the snapshots
that match its pinned shape, stops at the boundary, and reports that DDL is
pending. A DDL consumer reads the schema event, the demo applies the sink
migration, and the DML consumer resumes from the next snapshot.

```
orders v1  ->  DML consumer  ->  derived_orders v1
ALTER TABLE orders ADD COLUMN tax
DDL consumer -> migration
orders v2  ->  DML consumer  ->  derived_orders v2
```

## Why This Is First

DuckLake already has snapshots, time travel, and a data change feed. The
missing operating layer is knowing when it is safe to consume changes and when
the schema contract changed underneath you. This demo should make that visible
in one run.

## What The Viewer Should See

- A DML window ending before the schema-changing snapshot.
- A DDL event describing the table change.
- The sink schema migration.
- DML consumption resuming after the boundary.
- A final invariant check showing no rows were projected through the wrong
  schema.

## API Surface

- `cdc_dml_consumer_create`
- `cdc_ddl_consumer_create`
- `cdc_dml_changes_listen` / `cdc_dml_changes_read`
- `cdc_ddl_changes_listen` / `cdc_ddl_changes_read`
- `cdc_commit`

## Status

Spec only. Build this before adding more sink demos; it is the clearest
product differentiator.
