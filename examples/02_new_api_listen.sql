-- Long-polling with the new listen functions.
LOAD ducklake_cdc;

-- DML listeners wait for subscribed row changes.
SELECT *
FROM cdc_dml_changes_listen(
  'lake',
  'orders_sink',
  timeout_ms := 30000,
  max_snapshots := 100,
  auto_commit := false
);

-- Tick listeners are cheaper when the caller only needs a wakeup signal.
SELECT *
FROM cdc_dml_ticks_listen(
  'lake',
  'orders_sink',
  timeout_ms := 30000,
  auto_commit := false
);
