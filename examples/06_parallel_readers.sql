-- Orchestrator fan-out pattern.
--
-- One connection owns the consumer lease and chooses the snapshot window.
-- Additional workers may run ordinary DuckLake table_changes SELECTs for the
-- same bounds. Only the orchestrator commits the cursor.

LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:examples/06_parallel_readers.ducklake' AS lake
  (DATA_PATH 'examples/06_parallel_readers_data');

CREATE TABLE lake.orders(id INTEGER, status VARCHAR);
CREATE TABLE lake.shipments(id INTEGER, order_id INTEGER);

SELECT * FROM cdc_consumer_create('lake', 'orchestrator', start_at = '2');

INSERT INTO lake.orders VALUES (1, 'paid'), (2, 'packed');
INSERT INTO lake.shipments VALUES (10, 1), (11, 2);

-- Orchestrator connection: acquire the lease once and publish the bounds.
SELECT * FROM cdc_window('lake', 'orchestrator');
SET VARIABLE start_snapshot = (
  SELECT start_snapshot FROM cdc_window('lake', 'orchestrator')
);
SET VARIABLE end_snapshot = (
  SELECT end_snapshot FROM cdc_window('lake', 'orchestrator')
);

-- Worker A can run this SELECT on another connection using the same numeric
-- start/end values. It does not touch the consumer lease.
SELECT snapshot_id, rowid, change_type, id, status
FROM lake.table_changes(
  'orders',
  getvariable('start_snapshot'),
  getvariable('end_snapshot')
)
ORDER BY snapshot_id, rowid, change_type;

-- Worker B can do the same for another table.
SELECT snapshot_id, rowid, change_type, id, order_id
FROM lake.table_changes(
  'shipments',
  getvariable('start_snapshot'),
  getvariable('end_snapshot')
)
ORDER BY snapshot_id, rowid, change_type;

-- Only the orchestrator connection advances the cursor after all workers
-- report success.
SELECT * FROM cdc_commit('lake', 'orchestrator', getvariable('end_snapshot'));
