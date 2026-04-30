//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// consumer.cpp
//
// Implementation of the consumer state machine: lifecycle (create / reset /
// drop / list / force-release / heartbeat), the in-process token cache,
// the audit log writer, and the cursor primitives (cdc_window /
// cdc_commit / cdc_wait). The schema-boundary policy lives here too: a
// `cdc_window` call consults `SnapshotIsExternalSchemaChange` /
// `NextExternalSchemaChangeSnapshot` (facts in `ducklake_metadata.cpp`)
// and *decides* whether to truncate the visible window, then emits the
// `CDC_SCHEMA_BOUNDARY` notice.
//===----------------------------------------------------------------------===//

#include "consumer.hpp"

#include "compat_check.hpp"
#include "ducklake_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace duckdb_cdc {

//===--------------------------------------------------------------------===//
// CdcWindowData (declared in consumer.hpp)
//===--------------------------------------------------------------------===//

duckdb::unique_ptr<duckdb::FunctionData> CdcWindowData::Copy() const {
	auto result = duckdb::make_uniq<CdcWindowData>();
	result->catalog_name = catalog_name;
	result->consumer_name = consumer_name;
	result->max_snapshots = max_snapshots;
	return std::move(result);
}

bool CdcWindowData::Equals(const duckdb::FunctionData &other) const {
	return this == &other;
}

namespace {

//===--------------------------------------------------------------------===//
// Process-wide caches: owner-token (cdc_window/commit/heartbeat lease
// continuity per (connection, catalog, consumer)) and the per-connection
// shared-connection warning latch (cdc_wait).
//===--------------------------------------------------------------------===//

std::mutex TOKEN_CACHE_MUTEX;
std::unordered_map<std::string, std::string> TOKEN_CACHE;

std::mutex WAIT_WARNING_MUTEX;
std::unordered_set<int64_t> WAIT_WARNED_CONNECTIONS;

//===--------------------------------------------------------------------===//
// Lease / cache helpers
//===--------------------------------------------------------------------===//

std::string TokenCacheKey(duckdb::ClientContext &context, const std::string &catalog_name,
                          const std::string &consumer_name) {
	return std::to_string(context.GetConnectionId()) + ":" + catalog_name + ":" + consumer_name;
}

std::string CachedToken(duckdb::ClientContext &context, const std::string &catalog_name,
                        const std::string &consumer_name) {
	std::lock_guard<std::mutex> guard(TOKEN_CACHE_MUTEX);
	auto entry = TOKEN_CACHE.find(TokenCacheKey(context, catalog_name, consumer_name));
	if (entry == TOKEN_CACHE.end()) {
		return "";
	}
	return entry->second;
}

void CacheToken(duckdb::ClientContext &context, const std::string &catalog_name, const std::string &consumer_name,
                const std::string &token) {
	std::lock_guard<std::mutex> guard(TOKEN_CACHE_MUTEX);
	TOKEN_CACHE[TokenCacheKey(context, catalog_name, consumer_name)] = token;
}

std::string TokenSqlOrNull(const std::string &token) {
	if (token.empty()) {
		return "NULL";
	}
	return QuoteLiteral(token) + "::UUID";
}

std::string BuildBusyMessage(const std::string &catalog_name, const std::string &consumer_name,
                             const duckdb::Value &owner_token, const duckdb::Value &owner_acquired_at,
                             const duckdb::Value &owner_heartbeat_at, int64_t lease_interval_seconds,
                             bool lease_likely_dead) {
	std::ostringstream out;
	out << "CDC_BUSY: consumer '" << consumer_name << "' is currently held by token ";
	out << (owner_token.IsNull() ? "<unknown>" : owner_token.ToString());
	out << " (acquired " << (owner_acquired_at.IsNull() ? "<unknown>" : owner_acquired_at.ToString())
	    << ", last heartbeat " << (owner_heartbeat_at.IsNull() ? "<unknown>" : owner_heartbeat_at.ToString())
	    << "; lease interval " << lease_interval_seconds << "s). ";
	if (lease_likely_dead) {
		out << "The holder's last heartbeat is older than 2x the lease interval; it is likely dead.";
	} else {
		out << "The holder appears alive; wait for it to release, or run ";
	}
	out << " CALL cdc_consumer_force_release('" << catalog_name << "', '" << consumer_name
	    << "') if you know it has died.";
	return out.str();
}

//===--------------------------------------------------------------------===//
// Notice emitters
//===--------------------------------------------------------------------===//

//! Format and emit a `CDC_SCHEMA_BOUNDARY` notice. Per ADR 0006 + the
//! `docs/errors.md` contract, this fires at the *current* `cdc_window`
//! call when a future snapshot inside the visible range carries a
//! different `schema_version` — telling the operator (a) what to expect
//! when they next call `cdc_window` and (b) that DDL events for the
//! schema change will arrive ahead of any DML on the new schema.
void EmitSchemaBoundaryNotice(const std::string &consumer_name, int64_t end_snapshot, int64_t end_schema_version,
                              int64_t next_snapshot, int64_t next_schema_version) {
	std::ostringstream out;
	out << "CDC_SCHEMA_BOUNDARY: consumer '" << consumer_name << "' window ends at snapshot " << end_snapshot
	    << " (schema_version " << end_schema_version << "); the next snapshot (" << next_snapshot
	    << ") is at schema_version " << next_schema_version << ". The next cdc_window call will start at snapshot "
	    << next_snapshot
	    << "; DDL events for the schema change will arrive first per the DDL-before-DML ordering "
	       "contract (ADR 0008).";
	duckdb::Printer::Print(duckdb::OutputStream::STREAM_STDERR, out.str());
}

//! `CDC_WAIT_TIMEOUT_CLAMPED` notice. Per ADR 0011 and `docs/errors.md`,
//! `cdc_wait` clamps `timeout_ms` to the session-wide hard cap; this
//! notice exists so the caller can tell the difference between "I asked
//! for 30 minutes and got NULL because nothing happened" vs "I asked
//! for 30 minutes and got NULL after 5 because the cap clamped me".
//! Includes the `SET ducklake_cdc_wait_max_timeout_ms = ...` recovery
//! line verbatim per the docs/errors.md contract.
void EmitWaitTimeoutClampedNotice(int64_t requested_ms, int64_t cap_ms) {
	std::ostringstream out;
	out << "CDC_WAIT_TIMEOUT_CLAMPED: cdc_wait was called with timeout_ms => " << requested_ms
	    << " but the session cap is " << cap_ms << " (5 minutes); the call will return after at most " << cap_ms
	    << "ms. To raise the cap for this session: SET ducklake_cdc_wait_max_timeout_ms = " << requested_ms
	    << "; Caps above 1 hour mean a single connection is blocked for that long; prefer shorter "
	       "waits with re-polling for long-lived consumers.";
	duckdb::Printer::Print(duckdb::OutputStream::STREAM_STDERR, out.str());
}

//! Best-effort one-time-per-connection `CDC_WAIT_SHARED_CONNECTION`
//! warning. DuckDB does not expose per-connection introspection that
//! lets us reliably detect when a connection is "shared" with other
//! query-issuing call sites (prepared statements, cursors, pooled
//! handles), so per the roadmap item this fires unconditionally on
//! first `cdc_wait` per connection. The warning is informational
//! ("if you're calling cdc_wait on a shared connection, here is why
//! that is a foot gun") and only emits once so SQL-CLI and notebook
//! users are not spammed.
void MaybeEmitWaitSharedConnectionWarning(duckdb::ClientContext &context, int64_t timeout_ms) {
	const auto connection_id = static_cast<int64_t>(context.GetConnectionId());
	{
		std::lock_guard<std::mutex> guard(WAIT_WARNING_MUTEX);
		if (WAIT_WARNED_CONNECTIONS.count(connection_id) > 0) {
			return;
		}
		WAIT_WARNED_CONNECTIONS.insert(connection_id);
	}
	std::ostringstream out;
	out << "CDC_WAIT_SHARED_CONNECTION: cdc_wait holds this DuckDB connection for up to " << timeout_ms
	    << "ms. Calling it from a connection that also serves other queries (a pool handle, a "
	       "shared notebook session, an HTTP request handler) can starve them. Hold a dedicated "
	       "connection for cdc_wait. The Python and Go clients open one for you internally; "
	       "SQL-CLI users must do this themselves. See docs/operational/wait.md.";
	duckdb::Printer::Print(duckdb::OutputStream::STREAM_STDERR, out.str());
}

//===--------------------------------------------------------------------===//
// Audit writer
//===--------------------------------------------------------------------===//

void InsertAuditRow(duckdb::Connection &conn, const std::string &catalog_name, const std::string &action,
                    const std::string &consumer_name, int64_t consumer_id, const std::string &details) {
	const auto audit = StateTable(conn, catalog_name, AUDIT_TABLE);
	auto next_audit_id_result = conn.Query("SELECT COALESCE(MAX(audit_id), 0) + 1 FROM " + audit);
	int64_t audit_id = SingleInt64(*next_audit_id_result, "next audit_id");
	ExecuteChecked(conn, "INSERT INTO " + audit +
	                         " (audit_id, ts, actor, action, consumer_name, consumer_id, details) VALUES (" +
	                         std::to_string(audit_id) + ", now(), current_user, " + QuoteLiteral(action) + ", " +
	                         QuoteLiteral(consumer_name) + ", " + std::to_string(consumer_id) + ", " +
	                         QuoteLiteral(details) + ")");
}

//===--------------------------------------------------------------------===//
// cdc_consumer_create
//===--------------------------------------------------------------------===//

struct ConsumerCreateData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	std::string start_at;
	bool tables_specified = false;
	std::vector<std::string> tables;
	bool change_types_specified = false;
	std::vector<std::string> change_types;
	bool event_categories_specified = false;
	std::vector<std::string> event_categories;
	bool stop_at_schema_change = true;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerCreateData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->start_at = start_at;
		result->tables_specified = tables_specified;
		result->tables = tables;
		result->change_types_specified = change_types_specified;
		result->change_types = change_types;
		result->event_categories_specified = event_categories_specified;
		result->event_categories = event_categories;
		result->stop_at_schema_change = stop_at_schema_change;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

std::string StartAtParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("start_at");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return "now";
	}
	return entry->second.GetValue<std::string>();
}

//! Pulls a `VARCHAR[]` named parameter out of the bind input and reports
//! whether it was supplied at all (vs. supplied as NULL or omitted).
//! Both "omitted" and "supplied as NULL" mean "no filter" downstream;
//! the `out_specified` flag exists only so the consumer-create row can
//! distinguish "user didn't pass change_types" (NULL on disk) from
//! "user explicitly passed []" (empty list on disk, which is a *deny
//! all* filter and almost certainly a misconfiguration).
std::vector<std::string> ListStringParameter(duckdb::TableFunctionBindInput &input, const std::string &name,
                                             bool &out_specified) {
	out_specified = false;
	std::vector<std::string> values;
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return values;
	}
	out_specified = true;
	for (const auto &child : duckdb::ListValue::GetChildren(entry->second)) {
		if (child.IsNull()) {
			throw duckdb::InvalidInputException("cdc_consumer_create %s entries must be non-NULL strings",
			                                    name.c_str());
		}
		values.push_back(child.ToString());
	}
	return values;
}

duckdb::unique_ptr<duckdb::FunctionData> ConsumerCreateBind(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_create requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerCreateData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->start_at = StartAtParameter(input);
	result->tables = ListStringParameter(input, "tables", result->tables_specified);
	result->change_types = ListStringParameter(input, "change_types", result->change_types_specified);
	result->event_categories = ListStringParameter(input, "event_categories", result->event_categories_specified);
	auto stop_entry = input.named_parameters.find("stop_at_schema_change");
	if (stop_entry != input.named_parameters.end() && !stop_entry->second.IsNull()) {
		result->stop_at_schema_change = stop_entry->second.GetValue<bool>();
	}

	names = {"consumer_name", "consumer_id", "last_committed_snapshot"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	return std::move(result);
}

//! Build a SQL literal for the portable on-disk representation of a
//! per-consumer filter list. The sibling metadata-state tables store
//! repeated strings as JSON text because SQLite metadata catalogs do not
//! preserve DuckDB LIST columns through the scanner layer.
std::string BuildVarcharListLiteral(const std::vector<std::string> &values, bool specified) {
	if (!specified || values.empty()) {
		return "NULL";
	}
	return QuoteLiteral(JsonStringArray(values));
}

void ValidateTablesFilterOrThrow(duckdb::Connection &conn, const std::string &catalog_name,
                                 const std::string &consumer_name, const std::vector<std::string> &tables,
                                 int64_t resolved_snapshot) {
	if (tables.empty()) {
		throw duckdb::InvalidInputException(
		    "CDC_INVALID_TABLE_FILTER: consumer '%s' was created with an empty tables list. Either omit tables "
		    "to take everything, or supply at least one fully-qualified <schema>.<table> name.",
		    consumer_name);
	}
	auto existing =
	    conn.Query("SELECT s.schema_name || '.' || t.table_name FROM " + MetadataTable(catalog_name, "ducklake_table") +
	               " t JOIN " + MetadataTable(catalog_name, "ducklake_schema") + " s USING (schema_id) " +
	               " WHERE t.begin_snapshot <= " + std::to_string(resolved_snapshot) +
	               " AND (t.end_snapshot IS NULL OR t.end_snapshot > " + std::to_string(resolved_snapshot) + ")");
	std::unordered_set<std::string> existing_set;
	if (existing && !existing->HasError()) {
		for (duckdb::idx_t i = 0; i < existing->RowCount(); ++i) {
			if (!existing->GetValue(0, i).IsNull()) {
				existing_set.insert(existing->GetValue(0, i).ToString());
			}
		}
	}
	std::vector<std::string> missing;
	for (const auto &name : tables) {
		if (existing_set.find(name) == existing_set.end()) {
			missing.push_back(name);
		}
	}
	if (missing.empty()) {
		return;
	}
	std::ostringstream supplied;
	for (size_t i = 0; i < tables.size(); ++i) {
		if (i > 0) {
			supplied << ", ";
		}
		supplied << "'" << tables[i] << "'";
	}
	std::ostringstream missing_list;
	for (size_t i = 0; i < missing.size(); ++i) {
		if (i > 0) {
			missing_list << ", ";
		}
		missing_list << "'" << missing[i] << "'";
	}
	std::ostringstream message;
	message << "CDC_INVALID_TABLE_FILTER: consumer '" << consumer_name << "' was created with tables := ["
	        << supplied.str() << "] but " << missing_list.str() << " do not exist in the catalog at snapshot "
	        << resolved_snapshot << ". Either: correct the table name and retry cdc_consumer_create, or omit "
	        << "the missing table; if it is created later, recreate the consumer with start_at => "
	        << "<its-creation-snapshot>. Tables in the lake at snapshot " << resolved_snapshot << ": "
	        << ListTablesAtSnapshot(conn, catalog_name, resolved_snapshot);
	throw duckdb::InvalidInputException(message.str());
}

void ValidateChangeTypesOrThrow(const std::string &consumer_name, const std::vector<std::string> &change_types) {
	// DuckLake's `table_changes()` emits `update_postimage` /
	// `update_preimage` rather than `update_image`. The allowed set
	// here mirrors the literal `change_type` values DuckLake produces,
	// so a consumer's filter can be passed straight through to the
	// downstream `WHERE change_type IN (...)` clause without a
	// translation layer.
	static const std::unordered_set<std::string> ALLOWED = {"insert", "update_postimage", "update_preimage", "delete"};
	if (change_types.empty()) {
		throw duckdb::InvalidInputException(
		    "cdc_consumer_create: consumer '%s' was created with an empty change_types list. Omit "
		    "change_types to take all four (insert / update_postimage / update_preimage / delete), or supply "
		    "a non-empty subset.",
		    consumer_name);
	}
	for (const auto &kind : change_types) {
		if (ALLOWED.find(kind) == ALLOWED.end()) {
			throw duckdb::InvalidInputException(
			    "cdc_consumer_create: consumer '%s' has invalid change_types entry '%s'. Allowed values: "
			    "insert, update_postimage, update_preimage, delete.",
			    consumer_name, kind);
		}
	}
}

void ValidateEventCategoriesOrThrow(const std::string &consumer_name,
                                    const std::vector<std::string> &event_categories) {
	static const std::unordered_set<std::string> ALLOWED = {"dml", "ddl"};
	if (event_categories.empty()) {
		throw duckdb::InvalidInputException(
		    "cdc_consumer_create: consumer '%s' was created with an empty event_categories list. Omit "
		    "event_categories to take both (dml + ddl), or supply a non-empty subset.",
		    consumer_name);
	}
	for (const auto &category : event_categories) {
		if (ALLOWED.find(category) == ALLOWED.end()) {
			throw duckdb::InvalidInputException(
			    "cdc_consumer_create: consumer '%s' has invalid event_categories entry '%s'. Allowed values: "
			    "dml, ddl.",
			    consumer_name, category);
		}
	}
}

std::vector<duckdb::Value> CreateConsumer(duckdb::ClientContext &context, const ConsumerCreateData &data) {
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto quoted_name = QuoteLiteral(data.consumer_name);
	const auto actor_sql = "current_user";
	int64_t resolved_snapshot = ResolveCreateSnapshot(conn, data.catalog_name, data.start_at);
	int64_t schema_version = ResolveSchemaVersion(conn, data.catalog_name, resolved_snapshot);

	if (data.tables_specified) {
		ValidateTablesFilterOrThrow(conn, data.catalog_name, data.consumer_name, data.tables, resolved_snapshot);
	}
	if (data.change_types_specified) {
		ValidateChangeTypesOrThrow(data.consumer_name, data.change_types);
	}
	if (data.event_categories_specified) {
		ValidateEventCategoriesOrThrow(data.consumer_name, data.event_categories);
	}

	const auto tables_literal = BuildVarcharListLiteral(data.tables, data.tables_specified);
	const auto change_types_literal = BuildVarcharListLiteral(data.change_types, data.change_types_specified);
	const auto event_categories_literal =
	    BuildVarcharListLiteral(data.event_categories, data.event_categories_specified);

	// Audit details JSON stays append-only — every key from the create
	// call lands here (resolved snapshot included so a later
	// cdc_consumer_reset retains forensic continuity even if the
	// consumer is dropped/recreated under the same name).
	std::ostringstream details;
	details << "{\"start_at\":\"" << JsonEscape(data.start_at)
	        << "\",\"start_at_resolved_snapshot\":" << resolved_snapshot;
	if (data.tables_specified) {
		details << ",\"tables\":" << JsonStringArray(data.tables);
	}
	if (data.change_types_specified) {
		details << ",\"change_types\":" << JsonStringArray(data.change_types);
	}
	if (data.event_categories_specified) {
		details << ",\"event_categories\":" << JsonStringArray(data.event_categories);
	}
	details << ",\"stop_at_schema_change\":" << (data.stop_at_schema_change ? "true" : "false");
	details << "}";

	try {
		ExecuteChecked(conn, "BEGIN TRANSACTION");
		auto duplicate_result =
		    conn.Query("SELECT 1 FROM " + consumers + " WHERE consumer_name = " + quoted_name + " LIMIT 1");
		if (duplicate_result && !duplicate_result->HasError() && duplicate_result->RowCount() > 0) {
			throw duckdb::ConstraintException(
			    "PRIMARY KEY or UNIQUE constraint violation: consumer '%s' already exists", data.consumer_name);
		}
		auto next_id_result = conn.Query("SELECT COALESCE(MAX(consumer_id), 0) + 1 FROM " + consumers);
		int64_t consumer_id = SingleInt64(*next_id_result, "next consumer_id");
		ExecuteChecked(conn, "INSERT INTO " + consumers +
		                         " (consumer_name, consumer_id, last_committed_snapshot, "
		                         "last_committed_schema_version, created_at, created_by, updated_at, "
		                         "lease_interval_seconds, stop_at_schema_change, dml_blocked_by_failed_ddl, "
		                         "tables, change_types, event_categories) VALUES (" +
		                         quoted_name + ", " + std::to_string(consumer_id) + ", " +
		                         std::to_string(resolved_snapshot) + ", " + std::to_string(schema_version) +
		                         ", now(), " + actor_sql + ", now(), 60, " +
		                         (data.stop_at_schema_change ? "TRUE" : "FALSE") + ", TRUE, " + tables_literal + ", " +
		                         change_types_literal + ", " + event_categories_literal + ")");
		InsertAuditRow(conn, data.catalog_name, "consumer_create", data.consumer_name, consumer_id, details.str());
		ExecuteChecked(conn, "COMMIT");
		return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(consumer_id),
		        duckdb::Value::BIGINT(resolved_snapshot)};
	} catch (...) {
		try {
			ExecuteChecked(conn, "ROLLBACK");
		} catch (...) {
		}
		throw;
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerCreateInit(duckdb::ClientContext &context,
                                                                        duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerCreateData>();
	result->rows.push_back(CreateConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_reset
//===--------------------------------------------------------------------===//

struct ConsumerResetData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	std::string to_snapshot;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerResetData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->to_snapshot = to_snapshot;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

std::string ToSnapshotParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("to_snapshot");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return "";
	}
	return entry->second.GetValue<std::string>();
}

std::string ResetKind(const std::string &to_snapshot) {
	if (to_snapshot.empty()) {
		return "oldest_available";
	}
	const auto lower = duckdb::StringUtil::Lower(to_snapshot);
	if (lower == "beginning" || lower == "oldest" || lower == "oldest_available") {
		return "oldest_available";
	}
	if (lower == "now") {
		return "now";
	}
	int64_t ignored = 0;
	if (TryParseInt64(to_snapshot, ignored)) {
		return "snapshot_id";
	}
	return "timestamp";
}

duckdb::unique_ptr<duckdb::FunctionData> ConsumerResetBind(duckdb::ClientContext &context,
                                                           duckdb::TableFunctionBindInput &input,
                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                           duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_reset requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerResetData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->to_snapshot = ToSnapshotParameter(input);

	names = {"consumer_name", "consumer_id", "previous_snapshot", "new_snapshot"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT};
	return std::move(result);
}

std::vector<duckdb::Value> ResetConsumer(duckdb::ClientContext &context, const ConsumerResetData &data) {
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	try {
		auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		int64_t resolved_snapshot = ResolveResetSnapshot(conn, data.catalog_name, data.to_snapshot);
		int64_t schema_version = ResolveSchemaVersion(conn, data.catalog_name, resolved_snapshot);
		const auto details = "{\"from_snapshot\":" + std::to_string(row.last_committed_snapshot) +
		                     ",\"to_snapshot\":" + std::to_string(resolved_snapshot) + ",\"reset_kind\":\"" +
		                     JsonEscape(ResetKind(data.to_snapshot)) + "\"}";
		ExecuteChecked(conn, "UPDATE " + consumers +
		                         " SET last_committed_snapshot = " + std::to_string(resolved_snapshot) +
		                         ", last_committed_schema_version = " + std::to_string(schema_version) +
		                         ", owner_token = NULL, owner_acquired_at = NULL, owner_heartbeat_at = NULL, "
		                         "updated_at = now() WHERE consumer_name = " +
		                         QuoteLiteral(data.consumer_name));
		InsertAuditRow(conn, data.catalog_name, "consumer_reset", data.consumer_name, row.consumer_id, details);
		return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id),
		        duckdb::Value::BIGINT(row.last_committed_snapshot), duckdb::Value::BIGINT(resolved_snapshot)};
	} catch (...) {
		throw;
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerResetInit(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerResetData>();
	result->rows.push_back(ResetConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_drop
//===--------------------------------------------------------------------===//

struct ConsumerDropData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerDropData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerDropBind(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_drop requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerDropData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");

	names = {"consumer_name", "consumer_id", "last_committed_snapshot"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	return std::move(result);
}

std::vector<duckdb::Value> DropConsumer(duckdb::ClientContext &context, const ConsumerDropData &data) {
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	try {
		auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		const auto details = "{\"last_committed_snapshot\":" + std::to_string(row.last_committed_snapshot) +
		                     ",\"last_committed_schema_version\":" + std::to_string(row.last_committed_schema_version) +
		                     "}";
		InsertAuditRow(conn, data.catalog_name, "consumer_drop", data.consumer_name, row.consumer_id, details);
		ExecuteChecked(conn, "DELETE FROM " + consumers + " WHERE consumer_name = " + QuoteLiteral(data.consumer_name));
		return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id),
		        duckdb::Value::BIGINT(row.last_committed_snapshot)};
	} catch (...) {
		throw;
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerDropInit(duckdb::ClientContext &context,
                                                                      duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerDropData>();
	result->rows.push_back(DropConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_force_release
//===--------------------------------------------------------------------===//

struct ConsumerForceReleaseData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerForceReleaseData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerForceReleaseBind(duckdb::ClientContext &context,
                                                                  duckdb::TableFunctionBindInput &input,
                                                                  duckdb::vector<duckdb::LogicalType> &return_types,
                                                                  duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_force_release requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerForceReleaseData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");

	names = {"consumer_name", "consumer_id", "previous_token"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

std::vector<duckdb::Value> ForceReleaseConsumer(duckdb::ClientContext &context, const ConsumerForceReleaseData &data) {
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	try {
		auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		const auto details = "{\"previous_token\":" + JsonValue(row.owner_token) +
		                     ",\"previous_acquired_at\":" + JsonValue(row.owner_acquired_at) +
		                     ",\"previous_heartbeat_at\":" + JsonValue(row.owner_heartbeat_at) + "}";
		ExecuteChecked(conn, "UPDATE " + consumers +
		                         " SET owner_token = NULL, owner_acquired_at = NULL, owner_heartbeat_at = NULL, "
		                         "updated_at = now() WHERE consumer_name = " +
		                         QuoteLiteral(data.consumer_name));
		InsertAuditRow(conn, data.catalog_name, "consumer_force_release", data.consumer_name, row.consumer_id, details);
		duckdb::Value previous_token =
		    row.owner_token.IsNull() ? duckdb::Value() : duckdb::Value(row.owner_token.ToString());
		return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id), previous_token};
	} catch (...) {
		throw;
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerForceReleaseInit(duckdb::ClientContext &context,
                                                                              duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerForceReleaseData>();
	result->rows.push_back(ForceReleaseConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_window
//===--------------------------------------------------------------------===//

duckdb::unique_ptr<duckdb::FunctionData> CdcWindowBind(duckdb::ClientContext &context,
                                                       duckdb::TableFunctionBindInput &input,
                                                       duckdb::vector<duckdb::LogicalType> &return_types,
                                                       duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_window requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<CdcWindowData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->max_snapshots = MaxSnapshotsParameter(input);

	names = {"start_snapshot", "end_snapshot", "has_changes", "schema_version", "schema_changes_pending"};
	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN};
	return std::move(result);
}

void ThrowMaxSnapshotsExceeded(int64_t requested) {
	throw duckdb::InvalidInputException(
	    "CDC_MAX_SNAPSHOTS_EXCEEDED: cdc_window was called with max_snapshots => %lld, but the session hard cap is "
	    "%lld. To raise the cap for this session: SET ducklake_cdc_max_snapshots_hard_cap = %lld; Caps above ~10000 "
	    "are likely to read megabytes of catalog state in a single transaction; consider committing in smaller "
	    "batches instead.",
	    static_cast<long long>(requested), static_cast<long long>(HARD_MAX_SNAPSHOTS),
	    static_cast<long long>(requested));
}

void ThrowBusy(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name) {
	auto row = LoadConsumerOrThrow(conn, catalog_name, consumer_name);
	bool likely_dead = false;
	if (!row.owner_heartbeat_at.IsNull()) {
		auto result = conn.Query("SELECT epoch(now()) - epoch(" + QuoteLiteral(row.owner_heartbeat_at.ToString()) +
		                         "::TIMESTAMP WITH TIME ZONE) > " + std::to_string(2 * row.lease_interval_seconds));
		likely_dead = result && !result->HasError() && result->RowCount() > 0 && !result->GetValue(0, 0).IsNull() &&
		              result->GetValue(0, 0).GetValue<bool>();
	}
	throw duckdb::InvalidInputException(BuildBusyMessage(catalog_name, consumer_name, row.owner_token,
	                                                     row.owner_acquired_at, row.owner_heartbeat_at,
	                                                     row.lease_interval_seconds, likely_dead));
}

bool LeaseExpired(duckdb::Connection &conn, const ConsumerRow &row, int64_t multiplier = 1) {
	if (row.owner_token.IsNull() || row.owner_heartbeat_at.IsNull()) {
		return false;
	}
	auto result =
	    conn.Query("SELECT epoch(now()) - epoch(" + QuoteLiteral(row.owner_heartbeat_at.ToString()) +
	               "::TIMESTAMP WITH TIME ZONE) > " + std::to_string(multiplier * row.lease_interval_seconds));
	return result && !result->HasError() && result->RowCount() > 0 && !result->GetValue(0, 0).IsNull() &&
	       result->GetValue(0, 0).GetValue<bool>();
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcWindowInit(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcWindowData>();
	result->rows.push_back(ReadWindow(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_commit
//===--------------------------------------------------------------------===//

struct CdcCommitData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t snapshot_id;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcCommitData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->snapshot_id = snapshot_id;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcCommitBind(duckdb::ClientContext &context,
                                                       duckdb::TableFunctionBindInput &input,
                                                       duckdb::vector<duckdb::LogicalType> &return_types,
                                                       duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 3) {
		throw duckdb::BinderException("cdc_commit requires catalog, consumer name, and snapshot_id");
	}
	auto result = duckdb::make_uniq<CdcCommitData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->snapshot_id = input.inputs[2].GetValue<int64_t>();

	names = {"consumer_name", "committed_snapshot", "schema_version"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	return std::move(result);
}

void ValidateCommitSnapshot(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name,
                            int64_t current_cursor, int64_t snapshot_id) {
	if (snapshot_id < current_cursor) {
		throw duckdb::InvalidInputException("cdc_commit snapshot_id %lld is behind consumer '%s' cursor %lld",
		                                    static_cast<long long>(snapshot_id), consumer_name,
		                                    static_cast<long long>(current_cursor));
	}
	const auto snapshot_table = MetadataTable(catalog_name, "ducklake_snapshot");
	auto result =
	    conn.Query("SELECT count(*) FROM " + snapshot_table + " WHERE snapshot_id = " + std::to_string(snapshot_id));
	if (SingleInt64(*result, "commit snapshot existence") == 0) {
		throw duckdb::InvalidInputException("cdc_commit snapshot_id %lld does not exist in catalog '%s'",
		                                    static_cast<long long>(snapshot_id), catalog_name);
	}
	const auto current_snapshot = CurrentSnapshot(conn, catalog_name);
	if (snapshot_id > current_snapshot) {
		throw duckdb::InvalidInputException(
		    "cdc_commit snapshot_id %lld is newer than current snapshot %lld for catalog '%s'",
		    static_cast<long long>(snapshot_id), static_cast<long long>(current_snapshot), catalog_name);
	}
}

std::vector<duckdb::Value> CommitWindow(duckdb::ClientContext &context, const CdcCommitData &data) {
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto cached_token = CachedToken(context, data.catalog_name, data.consumer_name);
	try {
		auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		if (cached_token.empty() || row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
			ThrowBusy(conn, data.catalog_name, data.consumer_name);
		}
		ValidateCommitSnapshot(conn, data.catalog_name, data.consumer_name, row.last_committed_snapshot,
		                       data.snapshot_id);
		const auto schema_version = ResolveSchemaVersion(conn, data.catalog_name, data.snapshot_id);
		ExecuteChecked(conn,
		               "UPDATE " + consumers + " SET last_committed_snapshot = " + std::to_string(data.snapshot_id) +
		                   ", last_committed_schema_version = " + std::to_string(schema_version) +
		                   ", owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
		                   QuoteLiteral(data.consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token));
		row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		if (row.last_committed_snapshot != data.snapshot_id || row.owner_token.IsNull() ||
		    row.owner_token.ToString() != cached_token) {
			ThrowBusy(conn, data.catalog_name, data.consumer_name);
		}
		return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(data.snapshot_id),
		        duckdb::Value::BIGINT(schema_version)};
	} catch (...) {
		throw;
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcCommitInit(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcCommitData>();
	result->rows.push_back(CommitWindow(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_heartbeat
//===--------------------------------------------------------------------===//

struct ConsumerHeartbeatData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerHeartbeatData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerHeartbeatBind(duckdb::ClientContext &context,
                                                               duckdb::TableFunctionBindInput &input,
                                                               duckdb::vector<duckdb::LogicalType> &return_types,
                                                               duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_consumer_heartbeat requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<ConsumerHeartbeatData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");

	names = {"heartbeat_extended"};
	return_types = {duckdb::LogicalType::BOOLEAN};
	return std::move(result);
}

std::vector<duckdb::Value> HeartbeatConsumer(duckdb::ClientContext &context, const ConsumerHeartbeatData &data) {
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto cached_token = CachedToken(context, data.catalog_name, data.consumer_name);
	try {
		auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		if (cached_token.empty() || row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
			ThrowBusy(conn, data.catalog_name, data.consumer_name);
		}
		ExecuteChecked(
		    conn, "UPDATE " + consumers + " SET owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
		              QuoteLiteral(data.consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token));
		row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		if (row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
			ThrowBusy(conn, data.catalog_name, data.consumer_name);
		}
		return {duckdb::Value::BOOLEAN(true)};
	} catch (...) {
		throw;
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerHeartbeatInit(duckdb::ClientContext &context,
                                                                           duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerHeartbeatData>();
	result->rows.push_back(HeartbeatConsumer(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_wait
//===--------------------------------------------------------------------===//

struct CdcWaitData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t timeout_ms;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcWaitData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->timeout_ms = timeout_ms;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

int64_t TimeoutMsParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("timeout_ms");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return DEFAULT_WAIT_TIMEOUT_MS;
	}
	return entry->second.GetValue<int64_t>();
}

duckdb::unique_ptr<duckdb::FunctionData> CdcWaitBind(duckdb::ClientContext &context,
                                                     duckdb::TableFunctionBindInput &input,
                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                     duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_wait requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<CdcWaitData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->timeout_ms = TimeoutMsParameter(input);

	names = {"snapshot_id"};
	return_types = {duckdb::LogicalType::BIGINT};
	return std::move(result);
}

std::vector<duckdb::Value> WaitForSnapshot(duckdb::ClientContext &context, const CdcWaitData &data) {
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);
	if (data.timeout_ms < 0) {
		throw duckdb::InvalidInputException("cdc_wait timeout_ms must be >= 0");
	}
	const auto timeout_ms = std::min<int64_t>(data.timeout_ms, HARD_WAIT_TIMEOUT_MS);
	if (data.timeout_ms > HARD_WAIT_TIMEOUT_MS) {
		EmitWaitTimeoutClampedNotice(data.timeout_ms, HARD_WAIT_TIMEOUT_MS);
	}
	MaybeEmitWaitSharedConnectionWarning(context, timeout_ms);
	auto interval_ms = WAIT_INITIAL_INTERVAL_MS;
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

	duckdb::Connection conn(*context.db);
	while (true) {
		if (context.interrupted) {
			throw duckdb::InterruptException();
		}
		const auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		const auto current_snapshot = CurrentSnapshot(conn, data.catalog_name);
		if (current_snapshot > row.last_committed_snapshot) {
			return {duckdb::Value::BIGINT(current_snapshot)};
		}
		const auto now = std::chrono::steady_clock::now();
		if (now >= deadline || timeout_ms == 0) {
			return {duckdb::Value()};
		}
		const auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
		std::this_thread::sleep_for(std::chrono::milliseconds(std::min(interval_ms, remaining_ms)));
		interval_ms = std::min<int64_t>(interval_ms * 2, WAIT_MAX_INTERVAL_MS);
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcWaitInit(duckdb::ClientContext &context,
                                                                 duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcWaitData>();
	result->rows.push_back(WaitForSnapshot(context, data));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_list
//===--------------------------------------------------------------------===//

void ConsumerListReturnTypes(duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names) {
	names = {"consumer_name",
	         "consumer_id",
	         "tables",
	         "change_types",
	         "event_categories",
	         "stop_at_schema_change",
	         "dml_blocked_by_failed_ddl",
	         "last_committed_snapshot",
	         "last_committed_schema_version",
	         "owner_token",
	         "owner_acquired_at",
	         "owner_heartbeat_at",
	         "lease_interval_seconds",
	         "created_at",
	         "created_by",
	         "updated_at",
	         "metadata"};
	return_types = {duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
	                duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
	                duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
	                duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::UUID,
	                duckdb::LogicalType::TIMESTAMP_TZ,
	                duckdb::LogicalType::TIMESTAMP_TZ,
	                duckdb::LogicalType::INTEGER,
	                duckdb::LogicalType::TIMESTAMP_TZ,
	                duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::TIMESTAMP_TZ,
	                duckdb::LogicalType::VARCHAR};
}

struct ConsumerListData : public duckdb::TableFunctionData {
	std::string catalog_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerListData>();
		result->catalog_name = catalog_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerListBind(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_consumer_list requires catalog");
	}
	auto result = duckdb::make_uniq<ConsumerListData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	ConsumerListReturnTypes(return_types, names);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerListInit(duckdb::ClientContext &context,
                                                                      duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerListData>();
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	auto query = "SELECT consumer_name, consumer_id, tables, change_types, event_categories, "
	             "stop_at_schema_change, dml_blocked_by_failed_ddl, last_committed_snapshot, "
	             "last_committed_schema_version, owner_token, owner_acquired_at, owner_heartbeat_at, "
	             "lease_interval_seconds, created_at, created_by, updated_at, metadata FROM " +
	             StateTable(conn, data.catalog_name, CONSUMERS_TABLE) + " ORDER BY consumer_id ASC";
	auto query_result = conn.Query(query);
	if (!query_result || query_result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, query_result ? query_result->GetError() : query);
	}
	for (duckdb::idx_t row_idx = 0; row_idx < query_result->RowCount(); ++row_idx) {
		std::vector<duckdb::Value> row;
		for (duckdb::idx_t col_idx = 0; col_idx < query_result->ColumnCount(); ++col_idx) {
			auto value = query_result->GetValue(col_idx, row_idx);
			if (col_idx >= 2 && col_idx <= 4) {
				value = StringListValue(value);
			}
			row.push_back(value);
		}
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

} // namespace

//===--------------------------------------------------------------------===//
// Public surface (declared in consumer.hpp)
//===--------------------------------------------------------------------===//

ConsumerRow LoadConsumerOrThrow(duckdb::Connection &conn, const std::string &catalog_name,
                                const std::string &consumer_name) {
	const auto consumers = StateTable(conn, catalog_name, CONSUMERS_TABLE);
	auto result = conn.Query("SELECT consumer_name, consumer_id, last_committed_snapshot, "
	                         "last_committed_schema_version, owner_token, owner_acquired_at, owner_heartbeat_at, "
	                         "lease_interval_seconds, tables, change_types, event_categories, "
	                         "stop_at_schema_change FROM " +
	                         consumers + " WHERE consumer_name = " + QuoteLiteral(consumer_name) + " LIMIT 1");
	if (!result || result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, result ? result->GetError() : "consumer lookup failed");
	}
	if (result->RowCount() == 0) {
		throw duckdb::InvalidInputException("consumer '%s' does not exist", consumer_name);
	}
	ConsumerRow row;
	row.consumer_name = result->GetValue(0, 0).ToString();
	row.consumer_id = result->GetValue(1, 0).GetValue<int64_t>();
	auto snapshot_value = result->GetValue(2, 0);
	row.last_committed_snapshot = snapshot_value.IsNull() ? -1 : snapshot_value.GetValue<int64_t>();
	auto schema_value = result->GetValue(3, 0);
	row.last_committed_schema_version = schema_value.IsNull() ? -1 : schema_value.GetValue<int64_t>();
	row.owner_token = result->GetValue(4, 0);
	row.owner_acquired_at = result->GetValue(5, 0);
	row.owner_heartbeat_at = result->GetValue(6, 0);
	row.lease_interval_seconds = result->GetValue(7, 0).GetValue<int64_t>();
	row.tables = StringListValue(result->GetValue(8, 0));
	row.change_types = StringListValue(result->GetValue(9, 0));
	row.event_categories = StringListValue(result->GetValue(10, 0));
	const auto stop_value = result->GetValue(11, 0);
	row.stop_at_schema_change = stop_value.IsNull() ? true : stop_value.GetValue<bool>();
	return row;
}

std::unordered_set<std::string> CollectFilterTables(const duckdb::Value &tables_value) {
	std::unordered_set<std::string> filter_tables;
	if (tables_value.IsNull()) {
		return filter_tables;
	}
	const auto normalized = StringListValue(tables_value);
	for (const auto &child : duckdb::ListValue::GetChildren(normalized)) {
		if (child.IsNull()) {
			continue;
		}
		filter_tables.insert(child.ToString());
	}
	return filter_tables;
}

std::vector<std::string> CollectChangeTypes(const duckdb::Value &change_types_value) {
	std::vector<std::string> change_types;
	if (change_types_value.IsNull()) {
		return change_types;
	}
	const auto normalized = StringListValue(change_types_value);
	for (const auto &child : duckdb::ListValue::GetChildren(normalized)) {
		if (child.IsNull()) {
			continue;
		}
		change_types.push_back(child.ToString());
	}
	return change_types;
}

int64_t MaxSnapshotsParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("max_snapshots");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return DEFAULT_MAX_SNAPSHOTS;
	}
	return entry->second.GetValue<int64_t>();
}

std::vector<duckdb::Value> ReadWindow(duckdb::ClientContext &context, const CdcWindowData &data) {
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);
	if (data.max_snapshots > HARD_MAX_SNAPSHOTS) {
		ThrowMaxSnapshotsExceeded(data.max_snapshots);
	}
	if (data.max_snapshots < 1) {
		throw duckdb::InvalidInputException("cdc_window max_snapshots must be >= 1");
	}

	duckdb::Connection conn(*context.db);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto cached_token = CachedToken(context, data.catalog_name, data.consumer_name);
	const auto new_token = GenerateUuid(conn);
	const auto cached_token_sql = TokenSqlOrNull(cached_token);
	try {
		auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		const bool expired = LeaseExpired(conn, row);
		const bool owns_lease =
		    !row.owner_token.IsNull() && !cached_token.empty() && row.owner_token.ToString() == cached_token;
		if (!row.owner_token.IsNull() && !expired && !owns_lease) {
			ThrowBusy(conn, data.catalog_name, data.consumer_name);
		}
		const auto owner_token =
		    row.owner_token.IsNull() || (expired && !owns_lease) ? new_token : row.owner_token.ToString();
		if (expired && !owns_lease) {
			const auto details = "{\"previous_token\":" + JsonValue(row.owner_token) +
			                     ",\"previous_acquired_at\":" + JsonValue(row.owner_acquired_at) +
			                     ",\"previous_heartbeat_at\":" + JsonValue(row.owner_heartbeat_at) +
			                     ",\"lease_interval_seconds\":" + std::to_string(row.lease_interval_seconds) + "}";
			InsertAuditRow(conn, data.catalog_name, "lease_force_acquire", data.consumer_name, row.consumer_id,
			               details);
		}
		ExecuteChecked(conn, "UPDATE " + consumers + " SET owner_token = " + TokenSqlOrNull(owner_token) +
		                         ", owner_acquired_at = CASE WHEN owner_token IS NULL OR epoch(now()) - "
		                         "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > lease_interval_seconds THEN "
		                         "now() ELSE owner_acquired_at::TIMESTAMP WITH TIME ZONE "
		                         "END, owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
		                         QuoteLiteral(data.consumer_name) +
		                         " AND (owner_token IS NULL OR epoch(now()) - "
		                         "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > lease_interval_seconds OR "
		                         "owner_token = " +
		                         cached_token_sql + ")");
		row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		if (row.owner_token.IsNull() || row.owner_token.ToString() != owner_token) {
			ThrowBusy(conn, data.catalog_name, data.consumer_name);
		}
		const auto last_snapshot = row.last_committed_snapshot;
		const auto stop_at_schema_change = row.stop_at_schema_change;
		CacheToken(context, data.catalog_name, data.consumer_name, owner_token);
		EnsureSnapshotExistsOrGap(conn, data.catalog_name, data.consumer_name, last_snapshot);

		const auto current_snapshot = CurrentSnapshot(conn, data.catalog_name);
		const auto first_snapshot = FirstSnapshotAfter(conn, data.catalog_name, last_snapshot, current_snapshot);
		const auto start_snapshot = first_snapshot == -1 ? last_snapshot + 1 : first_snapshot;
		int64_t end_snapshot = first_snapshot == -1
		                           ? last_snapshot
		                           : std::min(current_snapshot, start_snapshot + data.max_snapshots - 1);
		bool schema_changes_pending = false;
		int64_t boundary_next_snapshot = -1;
		int64_t boundary_next_schema_version = -1;
		auto schema_version = last_snapshot <= current_snapshot
		                          ? ResolveSchemaVersion(conn, data.catalog_name, last_snapshot)
		                          : row.last_committed_schema_version;
		if (first_snapshot != -1 && start_snapshot <= current_snapshot) {
			schema_version = ResolveSchemaVersion(conn, data.catalog_name, start_snapshot);
			const auto base_schema_version = ResolveSchemaVersion(conn, data.catalog_name, last_snapshot);
			// `start_snapshot` itself can be a schema-change snapshot
			// (e.g. consumer just committed past the previous boundary
			// and is now reading the ALTER). The window includes it
			// regardless of `stop_at_schema_change`; we only need to
			// surface `schema_changes_pending = true` so callers know
			// the window straddles a schema version transition.
			if (SnapshotIsExternalSchemaChange(conn, data.catalog_name, start_snapshot)) {
				schema_changes_pending = true;
			}
			const auto next_schema_change = NextExternalSchemaChangeSnapshot(conn, data.catalog_name, start_snapshot,
			                                                                 current_snapshot, base_schema_version);
			if (next_schema_change != -1 && next_schema_change <= end_snapshot) {
				schema_changes_pending = true;
				if (stop_at_schema_change) {
					end_snapshot = next_schema_change - 1;
				}
				boundary_next_snapshot = next_schema_change;
				boundary_next_schema_version = ResolveSchemaVersion(conn, data.catalog_name, next_schema_change);
			}
		}
		const bool has_changes = end_snapshot >= start_snapshot;
		// docs/errors.md spells the notice as "window ends at snapshot N
		// (schema_version X); the next snapshot is at schema_version Y".
		// `schema_version` is the schema at `start_snapshot` (kept that
		// way for the row payload contract); compute the schema at the
		// actual `end_snapshot` for the notice so X<Y reads correctly
		// even when the window collapsed to a single snapshot at the
		// boundary.
		int64_t end_schema_version_for_notice = -1;
		if (schema_changes_pending && boundary_next_snapshot != -1) {
			end_schema_version_for_notice = has_changes ? ResolveSchemaVersion(conn, data.catalog_name, end_snapshot)
			                                            : ResolveSchemaVersion(conn, data.catalog_name, last_snapshot);
		}
		// Emit the CDC_SCHEMA_BOUNDARY notice after COMMIT so a downstream
		// caller's logging side-effect can never roll back the metadata
		// write the lease UPDATE just landed.
		if (schema_changes_pending && boundary_next_snapshot != -1 &&
		    end_schema_version_for_notice != boundary_next_schema_version) {
			EmitSchemaBoundaryNotice(data.consumer_name, has_changes ? end_snapshot : last_snapshot,
			                         end_schema_version_for_notice, boundary_next_snapshot,
			                         boundary_next_schema_version);
		}
		return {duckdb::Value::BIGINT(start_snapshot), duckdb::Value::BIGINT(end_snapshot),
		        duckdb::Value::BOOLEAN(has_changes), duckdb::Value::BIGINT(schema_version),
		        duckdb::Value::BOOLEAN(schema_changes_pending)};
	} catch (...) {
		throw;
	}
}

void RegisterConsumerFunctions(duckdb::ExtensionLoader &loader) {
	for (const auto &name : {"cdc_consumer_create", "ducklake_cdc_consumer_create"}) {
		duckdb::TableFunction create_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerCreateBind, ConsumerCreateInit);
		create_function.named_parameters["start_at"] = duckdb::LogicalType::VARCHAR;
		create_function.named_parameters["tables"] = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		create_function.named_parameters["change_types"] = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		create_function.named_parameters["event_categories"] = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		create_function.named_parameters["stop_at_schema_change"] = duckdb::LogicalType::BOOLEAN;
		loader.RegisterFunction(create_function);
	}

	for (const auto &name : {"cdc_consumer_reset", "ducklake_cdc_consumer_reset"}) {
		duckdb::TableFunction reset_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerResetBind, ConsumerResetInit);
		reset_function.named_parameters["to_snapshot"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(reset_function);
	}

	for (const auto &name : {"cdc_consumer_drop", "ducklake_cdc_consumer_drop"}) {
		duckdb::TableFunction drop_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerDropBind, ConsumerDropInit);
		loader.RegisterFunction(drop_function);
	}

	for (const auto &name : {"cdc_consumer_force_release", "ducklake_cdc_consumer_force_release"}) {
		duckdb::TableFunction force_release_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerForceReleaseBind, ConsumerForceReleaseInit);
		loader.RegisterFunction(force_release_function);
	}

	for (const auto &name : {"cdc_consumer_heartbeat", "ducklake_cdc_consumer_heartbeat"}) {
		duckdb::TableFunction heartbeat_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerHeartbeatBind, ConsumerHeartbeatInit);
		loader.RegisterFunction(heartbeat_function);
	}

	for (const auto &name : {"cdc_window", "ducklake_cdc_window"}) {
		duckdb::TableFunction window_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, CdcWindowBind, CdcWindowInit);
		window_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		loader.RegisterFunction(window_function);
	}

	for (const auto &name : {"cdc_commit", "ducklake_cdc_commit"}) {
		duckdb::TableFunction commit_function(name,
		                                      duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR,
		                                                                           duckdb::LogicalType::VARCHAR,
		                                                                           duckdb::LogicalType::BIGINT},
		                                      RowScanExecute, CdcCommitBind, CdcCommitInit);
		loader.RegisterFunction(commit_function);
	}

	for (const auto &name : {"cdc_wait", "ducklake_cdc_wait"}) {
		duckdb::TableFunction wait_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, CdcWaitBind, CdcWaitInit);
		wait_function.named_parameters["timeout_ms"] = duckdb::LogicalType::BIGINT;
		loader.RegisterFunction(wait_function);
	}

	for (const auto &name : {"cdc_consumer_list", "ducklake_cdc_consumer_list"}) {
		duckdb::TableFunction list_function(name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
		                                    RowScanExecute, ConsumerListBind, ConsumerListInit);
		loader.RegisterFunction(list_function);
	}
}

} // namespace duckdb_cdc
