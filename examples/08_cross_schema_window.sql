-- Cross-schema window opt-out.
--
-- stop_at_schema_change=false is for power users whose sinks can tolerate a
-- batch spanning schema versions. The window may include both the DDL event
-- and DML rows reconciled by DuckLake to the end-snapshot schema.

LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:examples/08_cross_schema_window.ducklake' AS lake
  (DATA_PATH 'examples/08_cross_schema_window_data');

CREATE TABLE lake.orders(id INTEGER);

SELECT * FROM cdc_consumer_create(
  'lake',
  'cross_schema',
  start_at = '1',
  stop_at_schema_change = false
);

INSERT INTO lake.orders VALUES (1), (2);
ALTER TABLE lake.orders ADD COLUMN region VARCHAR DEFAULT 'UNKNOWN';
INSERT INTO lake.orders VALUES (3, 'EU');

SELECT * FROM cdc_window('lake', 'cross_schema');

-- Informational DDL: the consumer opted into crossing the boundary.
SELECT snapshot_id, event_kind, object_kind, object_name, details
FROM cdc_ddl('lake', 'cross_schema')
ORDER BY snapshot_id, object_kind, object_id;

-- Rows before the ALTER are returned under DuckLake's end-snapshot schema.
SELECT snapshot_id, rowid, change_type, id, region
FROM cdc_changes('lake', 'cross_schema', 'orders')
ORDER BY snapshot_id, rowid, change_type;

SET VARIABLE end_snapshot = (
  SELECT end_snapshot FROM cdc_window('lake', 'cross_schema')
);
SELECT * FROM cdc_commit('lake', 'cross_schema', getvariable('end_snapshot'));
