//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// consumer.hpp
//
// Consumer lifecycle + cursor primitives. This module owns the consumer
// state machine: create / reset / drop / list / force-release / heartbeat,
// and the cursor primitives that read+advance under a single-reader lease
// (cdc_window, cdc_commit, listen calls). It also owns the in-process token
// cache, the audit log writer, and the lease/wait/schema-boundary notice
// helpers - everything that defines what "owning a consumer" means.
//
// The DDL and DML modules use `ReadWindow` and `LoadConsumerOrThrow` from
// here; the stats module reads `ConsumerRow` and friends. Nothing in this
// module depends on `ddl.hpp` or `dml.hpp`.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <string>
#include <vector>

namespace duckdb_cdc {

//! Single row from the `__ducklake_cdc_consumers` state table, normalised
//! into typed fields. Cross-cutting routing intent (schema_id, change_type
//! filters, rename/drop status) lives in
//! `__ducklake_cdc_consumer_subscriptions`. The single subscribed
//! `table_id` is denormalised onto this row because every DML consumer
//! has exactly one table — it lets `cdc_list_consumers` and the
//! per-listen hot path resolve the table without a join.
struct ConsumerRow {
	std::string consumer_name;
	std::string consumer_kind;
	int64_t consumer_id;
	//! Subscribed table_id. Populated for DML consumers (always exactly
	//! one); always NULL for DDL consumers.
	duckdb::Value table_id;
	int64_t last_committed_snapshot;
	int64_t last_committed_schema_version;
	duckdb::Value owner_token;
	duckdb::Value owner_acquired_at;
	duckdb::Value owner_heartbeat_at;
	int64_t lease_interval_seconds;
};

//! One durable row from `__ducklake_cdc_consumer_subscriptions`.
struct ConsumerSubscriptionRow {
	std::string consumer_name;
	std::string consumer_kind;
	int64_t consumer_id;
	int64_t subscription_id;
	std::string scope_kind;
	duckdb::Value schema_id;
	duckdb::Value table_id;
	std::string event_category;
	std::string change_type;
	duckdb::Value original_qualified_name;
	duckdb::Value current_qualified_name;
	std::string status;
};

//! Bind-time payload for `cdc_window`. Exposed in this header so DDL/DML
//! Init functions can construct one and call `ReadWindow(context, data)`
//! directly, which both acquires the lease for this transaction and
//! computes the visible `[start_snapshot, end_snapshot]` window.
struct CdcWindowData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t max_snapshots;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
	bool Equals(const duckdb::FunctionData &other) const override;
};

//! Read the consumer row by name. Throws `InvalidInputException` when the
//! consumer does not exist in the catalog's state table.
ConsumerRow LoadConsumerOrThrow(duckdb::Connection &conn, const std::string &catalog_name,
                                const std::string &consumer_name);

//! Load normalized subscription rows with current-name and status decoration.
std::vector<ConsumerSubscriptionRow> LoadConsumerSubscriptions(duckdb::Connection &conn,
                                                               const std::string &catalog_name,
                                                               const std::string &consumer_name = std::string());

//! Load raw active DML subscription facts for hot DML read/listen paths.
//! Unlike LoadConsumerSubscriptions, this does not decorate rows with current
//! object names or renamed/dropped status.
std::vector<ConsumerSubscriptionRow> LoadDmlConsumerSubscriptions(duckdb::Connection &conn,
                                                                  const std::string &catalog_name,
                                                                  const std::string &consumer_name);

bool SubscriptionCoversTable(const ConsumerSubscriptionRow &subscription, int64_t schema_id, int64_t table_id,
                             const std::string &event_category);

std::vector<std::string> MatchingDmlChangeTypes(const std::vector<ConsumerSubscriptionRow> &subscriptions,
                                                int64_t schema_id, int64_t table_id);

//! Per-subscribed-table schema-shape boundary for DML consumers.
//!
//! Returns the first snapshot in `(start_snapshot, current_snapshot]` whose
//! `changes_made` token list includes a shape-affecting DDL touching any of
//! `dml_subscriptions`' subscribed table_ids or schema_ids:
//!   - `altered_table:<id>` for a subscribed table_id (covers ALTER … ADD/DROP
//!     COLUMN and ALTER … RENAME — DuckLake encodes renames as ALTER per the
//!     `ducklake_snapshot_changes` spec),
//!   - `dropped_table:<id>` for a subscribed table_id,
//!   - `dropped_schema:<id>` for a subscription's schema_id (DROP SCHEMA
//!     CASCADE may bring the subscribed table down with it).
//!
//! Returns `-1` if no such snapshot exists in the range, or if the consumer
//! has no table-scoped DML subscriptions.
int64_t NextDmlSubscribedSchemaChangeSnapshot(duckdb::Connection &conn, const std::string &catalog_name,
                                              int64_t start_snapshot, int64_t current_snapshot,
                                              const std::vector<ConsumerSubscriptionRow> &dml_subscriptions);

std::string CurrentQualifiedTableName(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                                      int64_t snapshot_id);

bool ResolveCurrentTableName(duckdb::Connection &conn, const std::string &catalog_name,
                             const std::string &qualified_name, int64_t snapshot_id, int64_t &schema_id,
                             int64_t &table_id);

//! Pull the `max_snapshots` named parameter out of the bind input,
//! defaulting to `DEFAULT_MAX_SNAPSHOTS` when omitted.
int64_t MaxSnapshotsParameter(duckdb::TableFunctionBindInput &input);

//! Run the cdc_window state machine: reuse a fresh cached lease or
//! acquire/extend the consumer's lease and compute the visible
//! `[start_snapshot, end_snapshot]` range. For DML consumers the range
//! collapses at the first schema-shape boundary touching a subscribed
//! table (terminal state). For DDL consumers the range always includes
//! the schema-change snapshot itself; `schema_changes_pending` flags a
//! catalog-wide schema change inside the range. Emits a
//! `CDC_SCHEMA_BOUNDARY` notice when the window straddles a relevant
//! schema-version transition.
//!
//! Returns the row payload
//! `[start_snapshot, end_snapshot, has_changes, schema_version,
//!   schema_changes_pending, terminal, terminal_at_snapshot]`
//! callers can index directly. `terminal` is true only for DML consumers
//! pinned at a subscribed-table boundary; `terminal_at_snapshot` is the
//! snapshot id of the offending shape change (or NULL when not terminal).
std::vector<duckdb::Value> ReadWindow(duckdb::ClientContext &context, const CdcWindowData &data);
std::vector<duckdb::Value> ReadWindowWithConnection(duckdb::ClientContext &context, duckdb::Connection &conn,
                                                    const CdcWindowData &data);

std::vector<duckdb::Value> CommitConsumerSnapshot(duckdb::ClientContext &context, const std::string &catalog_name,
                                                  const std::string &consumer_name, int64_t snapshot_id);
std::vector<duckdb::Value> CommitConsumerSnapshotWithConnection(duckdb::ClientContext &context,
                                                                duckdb::Connection &conn,
                                                                const std::string &catalog_name,
                                                                const std::string &consumer_name, int64_t snapshot_id);

std::vector<duckdb::Value> WaitForConsumerSnapshot(duckdb::ClientContext &context, const std::string &catalog_name,
                                                   const std::string &consumer_name, int64_t timeout_ms);

std::vector<duckdb::Value> WaitForDmlConsumerSnapshot(duckdb::ClientContext &context, duckdb::Connection &conn,
                                                      const std::string &catalog_name, const std::string &consumer_name,
                                                      int64_t timeout_ms,
                                                      const std::vector<ConsumerSubscriptionRow> &subscriptions);

//! Reactively coalesce integrated listen calls after this process observes a
//! burst of quick, small non-empty results for the same consumer/stream.
void MaybeCoalesceConsumerListen(duckdb::ClientContext &context, const std::string &catalog_name,
                                 const std::string &consumer_name, const std::string &stream_key, int64_t timeout_ms,
                                 int64_t max_snapshots, int64_t first_matching_snapshot);
void MaybeCoalesceConsumerListenWithConnection(duckdb::ClientContext &context, duckdb::Connection &conn,
                                               const std::string &catalog_name, const std::string &consumer_name,
                                               const std::string &stream_key, int64_t timeout_ms, int64_t max_snapshots,
                                               int64_t first_matching_snapshot);

//! Update process-local adaptive listen state. This state is only a performance
//! hint; correctness remains entirely governed by cdc_window/cdc_commit.
void RecordConsumerListenResult(const std::string &catalog_name, const std::string &consumer_name,
                                const std::string &stream_key, bool has_rows, int64_t start_snapshot,
                                int64_t end_snapshot, int64_t row_count, int64_t max_snapshots);

//! Register all consumer-lifecycle and cursor table functions:
//! cdc_ddl_consumer_create/cdc_dml_consumer_create / reset / drop / force_release / heartbeat / list,
//! plus cdc_window / cdc_commit / listen calls. Called once at extension load.
void RegisterConsumerFunctions(duckdb::ExtensionLoader &loader);

} // namespace duckdb_cdc
