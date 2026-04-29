-- Schema-boundary flow with typed DDL.
--
-- With stop_at_schema_change=true (the default), cdc_window never returns a
-- DML window that crosses a schema boundary. The consumer reads old-schema
-- rows, commits, then reads the DDL event and new-schema rows.

LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:examples/04_schema_change.ducklake' AS lake
  (DATA_PATH 'examples/04_schema_change_data');

CREATE TABLE lake.orders(id INTEGER);
SELECT * FROM cdc_consumer_create('lake', 'schema_demo', start_at = '1');

INSERT INTO lake.orders VALUES (1), (2);
ALTER TABLE lake.orders ADD COLUMN region VARCHAR DEFAULT 'UNKNOWN';
INSERT INTO lake.orders VALUES (3, 'EU');

-- First window: bounded before the ALTER, with schema_changes_pending=true.
SELECT * FROM cdc_window('lake', 'schema_demo');
SELECT snapshot_id, change_type, id
FROM cdc_changes('lake', 'schema_demo', 'orders')
ORDER BY snapshot_id, rowid, change_type;

SET VARIABLE old_schema_end = (
  SELECT end_snapshot FROM cdc_window('lake', 'schema_demo')
);
SELECT * FROM cdc_commit('lake', 'schema_demo', getvariable('old_schema_end'));

-- Second window starts at the ALTER snapshot under the new schema.
SELECT * FROM cdc_window('lake', 'schema_demo');
SELECT snapshot_id, event_kind, object_kind, object_name, details
FROM cdc_ddl('lake', 'schema_demo')
ORDER BY snapshot_id, object_kind, object_id;
SELECT snapshot_id, change_type, id, region
FROM cdc_changes('lake', 'schema_demo', 'orders')
ORDER BY snapshot_id, rowid, change_type;

SET VARIABLE new_schema_end = (
  SELECT end_snapshot FROM cdc_window('lake', 'schema_demo')
);
SELECT * FROM cdc_commit('lake', 'schema_demo', getvariable('new_schema_end'));
