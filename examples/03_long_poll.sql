-- Long-polling with cdc_wait.
--
-- cdc_wait holds a DuckDB connection for the duration of the wait. Use a
-- dedicated connection in applications; do not call it from a shared pool slot.

LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:examples/03_long_poll.ducklake' AS lake
  (DATA_PATH 'examples/03_long_poll_data');

CREATE TABLE lake.orders(id INTEGER, status VARCHAR);
SELECT * FROM cdc_consumer_create('lake', 'poller', start_at = '1');

-- In another DuckDB session attached to the same catalog, run:
--
--   LOAD ducklake;
--   ATTACH 'ducklake:examples/03_long_poll.ducklake' AS lake
--     (DATA_PATH 'examples/03_long_poll_data');
--   INSERT INTO lake.orders VALUES (1, 'new');
--
-- cdc_wait is a table function returning one BIGINT row (the new snapshot
-- id, or NULL on timeout). DuckDB table functions must be read from a FROM
-- clause; SELECT cdc_wait(...) without FROM raises a binder error.
SELECT * FROM cdc_wait('lake', 'poller', timeout_ms := 30000);

SELECT * FROM cdc_window('lake', 'poller');
SELECT snapshot_id, rowid, change_type, id, status
FROM cdc_changes('lake', 'poller', 'orders')
ORDER BY snapshot_id, rowid, change_type;

SET VARIABLE end_snapshot = (
  SELECT end_snapshot FROM cdc_window('lake', 'poller')
);
SELECT * FROM cdc_commit('lake', 'poller', getvariable('end_snapshot'));
