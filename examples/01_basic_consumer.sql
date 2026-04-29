-- Basic durable consumer loop.
--
-- Run from the repository root after building/installing ducklake_cdc.
-- If the extension is not installed in DuckDB's extension path yet, replace
-- `LOAD ducklake_cdc;` with a LOAD of your local .duckdb_extension file.

LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:examples/01_basic_consumer.ducklake' AS lake
  (DATA_PATH 'examples/01_basic_consumer_data');

CREATE TABLE lake.orders(id INTEGER, status VARCHAR);
INSERT INTO lake.orders VALUES (1, 'new');

-- Start after the CREATE TABLE snapshot so this example focuses on DML.
SELECT * FROM cdc_consumer_create('lake', 'basic', start_at = '1');

INSERT INTO lake.orders VALUES (2, 'paid'), (3, 'packed');
UPDATE lake.orders SET status = 'shipped' WHERE id = 2;

-- Acquire the single-reader lease and inspect the window.
SELECT * FROM cdc_window('lake', 'basic');

-- Direct DuckLake read path: cdc_window gives the bounds, DuckLake's
-- table_changes reads the rows. cdc_changes is the packaged sugar over this.
SET VARIABLE start_snapshot = (
  SELECT start_snapshot FROM cdc_window('lake', 'basic')
);
SET VARIABLE end_snapshot = (
  SELECT end_snapshot FROM cdc_window('lake', 'basic')
);

SELECT snapshot_id, rowid, change_type, id, status
FROM lake.table_changes(
  'orders',
  getvariable('start_snapshot'),
  getvariable('end_snapshot')
)
ORDER BY snapshot_id, rowid, change_type;

-- Advance only after the sink has successfully processed the rows.
SELECT * FROM cdc_commit('lake', 'basic', getvariable('end_snapshot'));
