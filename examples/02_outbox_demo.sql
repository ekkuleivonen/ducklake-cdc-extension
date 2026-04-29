-- Producer-side outbox metadata with DuckLake's set_commit_message.
--
-- The extension does not wrap set_commit_message. Producers call DuckLake
-- directly; ducklake-cdc surfaces author, commit_message, and commit_extra_info
-- on cdc_events and cdc_changes so consumers can route downstream work.

LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:examples/02_outbox_demo.ducklake' AS lake
  (DATA_PATH 'examples/02_outbox_demo_data');

CREATE TABLE lake.orders(id INTEGER, status VARCHAR);

SELECT * FROM cdc_consumer_create('lake', 'outbox', start_at = '1');

BEGIN TRANSACTION;
CALL lake.set_commit_message(
  'checkout-service',
  'order paid',
  extra_info => '{"event":"OrderPaid","sink":"billing"}'
);
INSERT INTO lake.orders VALUES (1001, 'paid');
COMMIT;

-- Lake-level event sugar is enough for dispatch decisions.
SELECT
  snapshot_id,
  author,
  commit_message,
  commit_extra_info,
  next_snapshot_id
FROM cdc_events('lake', 'outbox')
ORDER BY snapshot_id;

-- The row-level sugar carries the same metadata on every emitted row.
SELECT
  snapshot_id,
  change_type,
  id,
  status,
  author,
  commit_message,
  commit_extra_info,
  next_snapshot_id
FROM cdc_changes('lake', 'outbox', 'orders')
ORDER BY snapshot_id, rowid, change_type;

SET VARIABLE end_snapshot = (
  SELECT max(next_snapshot_id) FROM cdc_events('lake', 'outbox')
);
SELECT * FROM cdc_commit('lake', 'outbox', getvariable('end_snapshot'));
