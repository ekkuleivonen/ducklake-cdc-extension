# Phase 1 Screenshot Pair

Phase 1 proves the SQL CDC surface is real and composable: discover that a table was created, then read row-level changes for that table. Use a two-screenshot pair here, not a GIF: the README needs a compact visual proof, and SQL output is more useful when readers can inspect DDL, row events, and snapshot ids at their own pace.

## Capture Setup

- Use a clean terminal profile with a readable monospace font, 120-140 columns wide.
- Use a fresh embedded DuckLake catalog and local `ducklake_cdc` build.
- Keep the prompt short so the SQL and output dominate each screenshot.
- Capture PNG files and commit them in this directory.
- Use exactly two files: `producer.png` and `consumer.png`.

## Required Screenshots

1. `producer.png` — the left side of the story. Show the producer inserting three rows, updating one row, deleting one row, then selecting the final table state.
2. `consumer.png` — the right side of the story. Show `cdc_recent_ddl('lake')` discovering `created.table`, then durable `cdc_changes('lake', 'demo', 'orders')` returning the row-level CDC events. The full cursor loop belongs in the quickstart text, not the small README image.

## Screenshot Framing

Together, the two screenshots should answer five questions:

- What object appeared?
- Where is the cursor?
- Which row events were emitted?
- What did the final table state become?
- How little SQL is required to inspect DDL and row changes?

Prefer two separate screenshots over one crowded two-pane screenshot. In the README, place them side by side if the layout allows it; otherwise stack producer first, consumer second. The producer screenshot explains the workload. The consumer screenshot carries the proof.

## Deferred Video

The moving demo belongs to the client phase. Record it once Python or Go can run:

```shell
ducklake-cdc tail --catalog ducklake:demo.ducklake --table orders --consumer demo --sink stdout --format jsonl --follow
```

That video should show a producer mutating DuckLake in one pane and the client streaming interleaved DDL/DML JSONL in the other. That is the moment where motion makes the product easier to understand.
