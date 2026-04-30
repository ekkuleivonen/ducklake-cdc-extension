//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// consumer.hpp
//
// Consumer lifecycle + cursor primitives. This module owns the consumer
// state machine: create / reset / drop / list / force-release / heartbeat,
// and the cursor primitives that read+advance under a single-reader lease
// (cdc_window, cdc_commit, cdc_wait). It also owns the in-process token
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
#include <unordered_set>
#include <vector>

namespace duckdb_cdc {

//! Single row from the `__ducklake_cdc_consumers` state table, normalised
//! into typed fields. List-typed columns (`tables`, `change_types`,
//! `event_categories`) are eagerly materialised into LIST values via
//! `StringListValue` so callers do not need to re-parse the on-disk JSON.
struct ConsumerRow {
	std::string consumer_name;
	int64_t consumer_id;
	int64_t last_committed_snapshot;
	int64_t last_committed_schema_version;
	duckdb::Value owner_token;
	duckdb::Value owner_acquired_at;
	duckdb::Value owner_heartbeat_at;
	int64_t lease_interval_seconds;
	duckdb::Value tables;
	duckdb::Value change_types;
	duckdb::Value event_categories;
	bool stop_at_schema_change = true;
};

//! One durable row from `__ducklake_cdc_consumer_subscriptions`.
struct ConsumerSubscriptionRow {
	std::string consumer_name;
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

//! Normalise the consumer's `tables` list value into a hash set for
//! per-snapshot membership checks. NULL / empty list -> empty set, which
//! every caller interprets as "no filter; take everything".
std::unordered_set<std::string> CollectFilterTables(const duckdb::Value &tables_value);

//! Normalise the consumer's `change_types` list into a vector. NULL -> empty
//! vector, which callers interpret as "no filter; take all DML kinds".
std::vector<std::string> CollectChangeTypes(const duckdb::Value &change_types_value);

//! Load normalized subscription rows with current-name and status decoration.
std::vector<ConsumerSubscriptionRow> LoadConsumerSubscriptions(duckdb::Connection &conn,
                                                              const std::string &catalog_name,
                                                              const std::string &consumer_name = std::string());

bool SubscriptionCoversTable(const ConsumerSubscriptionRow &subscription, int64_t schema_id, int64_t table_id,
                             const std::string &event_category);

std::vector<std::string> MatchingDmlChangeTypes(const std::vector<ConsumerSubscriptionRow> &subscriptions,
                                                int64_t schema_id, int64_t table_id);

std::string CurrentQualifiedTableName(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                                      int64_t snapshot_id);

bool ResolveCurrentTableName(duckdb::Connection &conn, const std::string &catalog_name,
                             const std::string &qualified_name, int64_t snapshot_id, int64_t &schema_id,
                             int64_t &table_id);

//! Pull the `max_snapshots` named parameter out of the bind input,
//! defaulting to `DEFAULT_MAX_SNAPSHOTS` when omitted.
int64_t MaxSnapshotsParameter(duckdb::TableFunctionBindInput &input);

//! Run the cdc_window state machine: acquire/extend the consumer's lease,
//! compute the visible `[start_snapshot, end_snapshot]` range under the
//! consumer's `stop_at_schema_change` policy, and emit a
//! `CDC_SCHEMA_BOUNDARY` notice if the window straddles a DDL boundary.
//! Returns the row payload `[start_snapshot, end_snapshot, has_changes,
//! schema_version, schema_changes_pending]` callers can index directly.
std::vector<duckdb::Value> ReadWindow(duckdb::ClientContext &context, const CdcWindowData &data);

//! Register all consumer-lifecycle and cursor table functions:
//! cdc_consumer_create / reset / drop / force_release / heartbeat / list,
//! plus cdc_window / cdc_commit / cdc_wait. Called once at extension load.
void RegisterConsumerFunctions(duckdb::ExtensionLoader &loader);

} // namespace duckdb_cdc
