-- Stateless ad-hoc exploration.
--
-- cdc_recent_changes and cdc_recent_ddl do not create or advance a consumer.
-- They are sugar for quick inspection when you do not need a durable cursor.

LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:examples/05_recent_changes.ducklake' AS lake
  (DATA_PATH 'examples/05_recent_changes_data');

CREATE TABLE lake.orders(id INTEGER, status VARCHAR);
INSERT INTO lake.orders VALUES (1, 'new'), (2, 'paid');
UPDATE lake.orders SET status = 'shipped' WHERE id = 2;
ALTER TABLE lake.orders ADD COLUMN region VARCHAR DEFAULT 'UNKNOWN';
INSERT INTO lake.orders VALUES (3, 'EU');

SELECT snapshot_id, event_kind, object_kind, object_name, details
FROM cdc_recent_ddl('lake', since_seconds = 86400)
ORDER BY snapshot_id, object_kind, object_id;

SELECT snapshot_id, rowid, change_type, id, status, region
FROM cdc_recent_changes('lake', 'orders', since_seconds = 86400)
ORDER BY snapshot_id, rowid, change_type;

-- For column-level inspection, cdc_schema_diff is also stateless.
SELECT snapshot_id, change_kind, old_name, new_name, old_type, new_type
FROM cdc_schema_diff(
  'lake',
  'orders',
  0,
  (SELECT max(snapshot_id) FROM __ducklake_metadata_lake.ducklake_snapshot)
)
ORDER BY snapshot_id, change_kind, column_id;
