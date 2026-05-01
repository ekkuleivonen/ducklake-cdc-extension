-- DDL stream consumer.
LOAD ducklake_cdc;

SELECT *
FROM cdc_ddl_consumer_create(
  'lake',
  'schema_watch',
  schemas := ['main'],
  start_at := 'now'
);

SELECT *
FROM cdc_ddl_changes_listen(
  'lake',
  'schema_watch',
  timeout_ms := 30000,
  auto_commit := false
);

SELECT *
FROM cdc_ddl_ticks_query(
  'lake',
  0,
  schemas := ['main']
);
