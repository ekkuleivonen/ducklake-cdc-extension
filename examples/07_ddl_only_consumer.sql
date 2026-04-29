-- DDL-only consumer for schema-watcher workflows.
--
-- event_categories=['ddl'] projects only DDL events. The durable cursor still
-- advances through the stream, so this can run beside independent DML consumers.

LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:examples/07_ddl_only_consumer.ducklake' AS lake
  (DATA_PATH 'examples/07_ddl_only_consumer_data');

CREATE TABLE lake.orders(id INTEGER);
INSERT INTO lake.orders VALUES (1), (2);

SELECT * FROM cdc_consumer_create(
  'lake',
  'schema_watcher',
  start_at = '2',
  event_categories = ['ddl']
);

ALTER TABLE lake.orders ADD COLUMN status VARCHAR DEFAULT 'new';
INSERT INTO lake.orders VALUES (3, 'paid');

SELECT * FROM cdc_window('lake', 'schema_watcher');

SELECT snapshot_id, event_kind, object_kind, object_name, details
FROM cdc_ddl('lake', 'schema_watcher')
ORDER BY snapshot_id, object_kind, object_id;

-- DML projection is empty for this consumer by design.
SELECT count(*) AS dml_rows_visible_to_schema_watcher
FROM cdc_changes('lake', 'schema_watcher', 'orders');

SET VARIABLE end_snapshot = (
  SELECT end_snapshot FROM cdc_window('lake', 'schema_watcher')
);
SELECT * FROM cdc_commit('lake', 'schema_watcher', getvariable('end_snapshot'));
