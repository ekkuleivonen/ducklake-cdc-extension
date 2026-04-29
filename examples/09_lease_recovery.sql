-- Lease recovery with cdc_consumer_force_release.
--
-- The owner-token lease enforces one logical reader per consumer name. This
-- script shows the operator escape hatch. For a real two-connection demo, run
-- the "operator session" block below from a second DuckDB shell while this
-- session is holding the window.

LOAD ducklake;
LOAD parquet;
LOAD ducklake_cdc;

ATTACH 'ducklake:examples/09_lease_recovery.ducklake' AS lake
  (DATA_PATH 'examples/09_lease_recovery_data');

CREATE TABLE lake.orders(id INTEGER, status VARCHAR);
SELECT * FROM cdc_consumer_create('lake', 'leased', start_at = '1');
INSERT INTO lake.orders VALUES (1, 'paid');

-- Session A: acquire and hold the lease.
SELECT * FROM cdc_window('lake', 'leased');
SET VARIABLE end_snapshot = (
  SELECT end_snapshot FROM cdc_window('lake', 'leased')
);
SELECT consumer_name, owner_token, owner_acquired_at, owner_heartbeat_at, lease_alive
FROM cdc_consumer_stats('lake', consumer => 'leased');

-- Operator session, in a second DuckDB shell:
--
--   LOAD ducklake;
--   LOAD parquet;
--   LOAD ducklake_cdc;
--   ATTACH 'ducklake:examples/09_lease_recovery.ducklake' AS lake
--     (DATA_PATH 'examples/09_lease_recovery_data');
--   SELECT * FROM cdc_consumer_force_release('lake', 'leased');
--   SELECT action, consumer_name, details
--   FROM cdc_audit_recent('lake', consumer => 'leased')
--   ORDER BY ts DESC, audit_id DESC
--   LIMIT 3;

-- Back in Session A, after the force release above, commit is rejected because
-- this connection no longer owns the lease.
-- Expected after force-release from another session:
--   CDC_BUSY
SELECT * FROM cdc_commit('lake', 'leased', getvariable('end_snapshot'));

-- If this script is run in one shell without the operator block, the commit
-- above succeeds. Inspect the audit trail either way.
SELECT action, consumer_name, details
FROM cdc_audit_recent('lake', consumer => 'leased')
ORDER BY ts DESC, audit_id DESC;
