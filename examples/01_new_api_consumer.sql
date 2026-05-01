-- Minimal new-contract CDC consumer.
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
  table_names := ['main.orders'],
  change_types := ['insert', 'update_postimage', 'delete'],
  start_at := 'now'
);

INSERT INTO lake.orders VALUES (2, 'paid');

SELECT *
FROM cdc_dml_table_changes_read(
  'lake',
  'orders_sink',
  table_name := 'main.orders'
);

SELECT *
FROM cdc_dml_ticks_read('lake', 'orders_sink');
