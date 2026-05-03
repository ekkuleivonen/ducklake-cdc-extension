-- Minimal new-contract CDC consumer.
--
-- DML consumers are pinned to a single table by contract:
-- pass exactly one of `table_name` or `table_id` (the bind rejects
-- "both set" / "neither set"). The typed payload API is unified at
-- `cdc_dml_changes_read` / `_listen` — there's no separate "table
-- changes" function and no JSON `values` blob anymore; the row's
-- native columns project at the top level alongside the standard
-- metadata fields (`snapshot_id`, `rowid`, `change_type`,
-- `table_id`, `table_name`, ...).
INSTALL ducklake;
LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:example.ducklake' AS lake (DATA_PATH 'example_data');

CREATE TABLE IF NOT EXISTS lake.orders(id INTEGER, status VARCHAR);
INSERT INTO lake.orders VALUES (1, 'new');

SELECT *
FROM cdc_dml_consumer_create(
  'lake',
  'orders_sink',
  table_name := 'main.orders',
  change_types := ['insert', 'update_postimage', 'delete'],
  start_at := 'now'
);

INSERT INTO lake.orders VALUES (2, 'paid');

SELECT *
FROM cdc_dml_changes_read(
  'lake',
  'orders_sink'
);

SELECT *
FROM cdc_dml_ticks_read('lake', 'orders_sink');
