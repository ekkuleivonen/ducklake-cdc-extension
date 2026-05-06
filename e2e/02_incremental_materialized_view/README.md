# 02 &mdash; Incremental Materialized View

Maintain a derived DuckLake table from CDC windows instead of rescanning the
source table.

## Story

An `orders` table receives inserts, updates, and deletes. A CDC consumer keeps
`daily_revenue` or `customer_ltv` current by applying only the rows changed
since its last committed snapshot.

```
orders  ->  cdc_dml_changes_listen  ->  daily_revenue
```

The demo should show the derived table changing live, then verify it matches a
full recomputation at the end.

## Why This Is Second

DuckLake's public roadmap lists materialized views and incremental maintenance
as future work. This example demonstrates the operator-facing version of that
need today: durable cursors, explicit commit points, replay after failure, and
no hidden full-table scan on every refresh.

## What The Viewer Should See

- Source commits arriving.
- A derived aggregate table updating incrementally.
- A deliberate consumer restart or replay.
- The derived table converging to the same result as a full SQL recompute.
- Throughput/latency numbers framed as maintenance cost, not streaming hype.

## API Surface

- `cdc_dml_consumer_create`
- `cdc_dml_changes_listen`
- `DMLBatch.transaction()` / `cdc_commit`
- DuckLake SQL for the full recompute correctness check

## Status

Spec only. This should become the primary “why would a data team care?” demo.
