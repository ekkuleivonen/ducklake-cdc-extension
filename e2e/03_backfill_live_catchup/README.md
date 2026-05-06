# 03 &mdash; Backfill Then Live Catchup

Start a consumer from old snapshots, drain history, and then stay current with
live listen calls.

## Story

A new downstream index or derived table is introduced after the source table
already has history. The consumer starts at snapshot 0, processes historical
windows in bounded batches, catches up to the catalog head, then switches into
listen mode for new commits.

```
snapshot 0 .. head  ->  read windows  ->  caught up
new commits         ->  listen        ->  stays current
```

## Why This Is Third

Most real consumers do not start on day zero. They are added after data exists,
or they need to rebuild after an operator drops a sink. This is where durable
snapshot cursors become more valuable than a one-off `table_changes` query.

## What The Viewer Should See

- Historical backlog count falling to zero.
- A clear transition from read/backfill mode to listen/live mode.
- The same sink code used in both modes.
- A restart during backfill that resumes from the last committed snapshot.
- A final invariant showing no gaps and no duplicate committed windows.

## API Surface

- `cdc_dml_consumer_create(start_at := 'beginning')`
- `cdc_dml_changes_read`
- `cdc_dml_changes_listen`
- `cdc_commit`

## Status

Spec only. Build after the materialized-view demo so it can reuse the same
derived table or index sink.
