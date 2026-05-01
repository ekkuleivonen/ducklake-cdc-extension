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
//
// Lease and commit state have two execution paths. DuckDB-backed state uses
// single-statement `UPDATE ... RETURNING` for the hot path; attached catalog
// backends that do not support RETURNING through DuckDB's scanner fall back to
// the multi-statement legacy path. The legacy path is the reference behavior,
// and the fast path must match its semantics. Keep `AcquireLeaseReturning` /
// `AcquireLeaseLegacy` and the matching `cdc_commit` branches in sync when
// changing lease ownership, expiration, or audit semantics.
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

ConsumerRow ConsumerRowFromResult(duckdb::MaterializedQueryResult &result, duckdb::idx_t row_index) {
	ConsumerRow row;
	row.consumer_name = result.GetValue(0, row_index).ToString();
	row.consumer_id = result.GetValue(1, row_index).GetValue<int64_t>();
	auto snapshot_value = result.GetValue(2, row_index);
	row.last_committed_snapshot = snapshot_value.IsNull() ? -1 : snapshot_value.GetValue<int64_t>();
	auto schema_value = result.GetValue(3, row_index);
	row.last_committed_schema_version = schema_value.IsNull() ? -1 : schema_value.GetValue<int64_t>();
	row.owner_token = result.GetValue(4, row_index);
	row.owner_acquired_at = result.GetValue(5, row_index);
	row.owner_heartbeat_at = result.GetValue(6, row_index);
	row.lease_interval_seconds = result.GetValue(7, row_index).GetValue<int64_t>();
	const auto stop_value = result.GetValue(8, row_index);
	row.stop_at_schema_change = stop_value.IsNull() ? true : stop_value.GetValue<bool>();
	return row;
}

std::string ConsumerRowProjection() {
	return "consumer_name, consumer_id, last_committed_snapshot, last_committed_schema_version, owner_token, "
	       "owner_acquired_at, owner_heartbeat_at, lease_interval_seconds, stop_at_schema_change";
}

enum class StateBackendKind { DuckDB, SQLite, Postgres, Unknown };

std::mutex BACKEND_CACHE_MUTEX;
std::unordered_map<std::string, StateBackendKind> BACKEND_CACHE;

std::string BackendCacheKey(duckdb::ClientContext &context, const std::string &catalog_name) {
	return std::to_string(context.GetConnectionId()) + ":" + catalog_name;
}

StateBackendKind DetectStateBackend(duckdb::Connection &conn, const std::string &catalog_name) {
	const auto metadata_database = std::string("__ducklake_metadata_") + catalog_name;
	auto result = conn.Query(
	    "SELECT type FROM duckdb_databases() WHERE database_name = " + QuoteLiteral(metadata_database) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return StateBackendKind::Unknown;
	}
	const auto type = duckdb::StringUtil::Lower(result->GetValue(0, 0).ToString());
	if (type == "duckdb") {
		return StateBackendKind::DuckDB;
	}
	if (type == "sqlite") {
		return StateBackendKind::SQLite;
	}
	if (type == "postgres" || type == "postgres_scanner") {
		return StateBackendKind::Postgres;
	}
	return StateBackendKind::Unknown;
}

StateBackendKind CachedStateBackend(duckdb::ClientContext &context, duckdb::Connection &conn,
                                    const std::string &catalog_name) {
	const auto key = BackendCacheKey(context, catalog_name);
	{
		std::lock_guard<std::mutex> guard(BACKEND_CACHE_MUTEX);
		auto entry = BACKEND_CACHE.find(key);
		if (entry != BACKEND_CACHE.end()) {
			return entry->second;
		}
	}

	const auto backend = DetectStateBackend(conn, catalog_name);
	if (backend != StateBackendKind::Unknown) {
		std::lock_guard<std::mutex> guard(BACKEND_CACHE_MUTEX);
		BACKEND_CACHE[key] = backend;
	}
	return backend;
}

bool SupportsUpdateReturning(StateBackendKind backend) {
	// DuckDB's sqlite_scanner and postgres_scanner currently reject
	// `UPDATE ... RETURNING` for attached tables, even though both underlying
	// engines support it natively. `test/catalog_matrix/catalog_matrix_smoke.py`
	// has a small probe for this so the gate can be widened when scanners learn
	// to round-trip RETURNING correctly.
	return backend == StateBackendKind::DuckDB;
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
	       "connection for cdc_wait. SQL-CLI users must do this themselves. See docs/api.md#cdc_wait.";
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
	bool subscriptions_specified = false;
	duckdb::Value subscriptions;
	bool stop_at_schema_change = true;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerCreateData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->start_at = start_at;
		result->subscriptions_specified = subscriptions_specified;
		result->subscriptions = subscriptions;
		result->stop_at_schema_change = stop_at_schema_change;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

struct RawSubscriptionInput {
	std::string scope_kind;
	duckdb::Value schema_name;
	duckdb::Value table_name;
	duckdb::Value schema_id;
	duckdb::Value table_id;
	duckdb::Value event_category;
	duckdb::Value change_type;
};

struct ResolvedSubscriptionInput {
	std::string scope_kind;
	duckdb::Value schema_id;
	duckdb::Value table_id;
	std::string event_category;
	std::string change_type;
	duckdb::Value original_qualified_name;
};

std::string StartAtParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("start_at");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return "now";
	}
	return entry->second.GetValue<std::string>();
}

duckdb::Value NamedParameterValue(duckdb::TableFunctionBindInput &input, const std::string &name, bool &specified) {
	specified = false;
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return duckdb::Value();
	}
	specified = true;
	return entry->second;
}

std::string ValueStringOrEmpty(const duckdb::Value &value) {
	return value.IsNull() ? std::string() : value.ToString();
}

duckdb::Value StructChild(const duckdb::Value &value, const std::string &field_name) {
	const auto &children = duckdb::StructValue::GetChildren(value);
	const auto &type = value.type();
	for (duckdb::idx_t i = 0; i < duckdb::StructType::GetChildCount(type) && i < children.size(); ++i) {
		if (duckdb::StructType::GetChildName(type, i) == field_name) {
			return children[i];
		}
	}
	return duckdb::Value();
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
	result->subscriptions = NamedParameterValue(input, "subscriptions", result->subscriptions_specified);
	auto stop_entry = input.named_parameters.find("stop_at_schema_change");
	if (stop_entry != input.named_parameters.end() && !stop_entry->second.IsNull()) {
		result->stop_at_schema_change = stop_entry->second.GetValue<bool>();
	}

	names = {"consumer_name",
	         "consumer_id",
	         "last_committed_snapshot",
	         "subscription_id",
	         "scope_kind",
	         "schema_id",
	         "table_id",
	         "event_category",
	         "change_type",
	         "original_qualified_name",
	         "current_qualified_name"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

std::string QualifyTableInput(const std::string &table_name, const duckdb::Value &schema_name) {
	if (table_name.find('.') != std::string::npos) {
		return table_name;
	}
	const auto schema = schema_name.IsNull() ? std::string("main") : schema_name.ToString();
	return schema + "." + table_name;
}

duckdb::Value OptionalQualifiedName(const std::string &scope_kind, const duckdb::Value &schema_name,
                                    const duckdb::Value &table_name, const duckdb::Value &schema_id,
                                    const duckdb::Value &table_id, const std::string &current_name) {
	if (!table_name.IsNull()) {
		return duckdb::Value(QualifyTableInput(table_name.ToString(), schema_name));
	}
	if (!schema_name.IsNull()) {
		return duckdb::Value(schema_name.ToString());
	}
	if (!current_name.empty()) {
		return duckdb::Value(current_name);
	}
	if (scope_kind == "schema" && !schema_id.IsNull()) {
		return duckdb::Value("schema_id:" + schema_id.ToString());
	}
	if (scope_kind == "table" && !table_id.IsNull()) {
		return duckdb::Value("table_id:" + table_id.ToString());
	}
	return duckdb::Value();
}

bool ResolveSchemaByNameAt(duckdb::Connection &conn, const std::string &catalog_name, const std::string &schema_name,
                           int64_t snapshot_id, int64_t &schema_id) {
	auto result =
	    conn.Query("SELECT schema_id FROM " + MetadataTable(catalog_name, "ducklake_schema") + " WHERE schema_name = " +
	               QuoteLiteral(schema_name) + " AND begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (end_snapshot IS NULL OR end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	schema_id = result->GetValue(0, 0).GetValue<int64_t>();
	return true;
}

bool ResolveSchemaByIdAt(duckdb::Connection &conn, const std::string &catalog_name, int64_t schema_id,
                         int64_t snapshot_id, std::string &schema_name) {
	auto result =
	    conn.Query("SELECT schema_name FROM " + MetadataTable(catalog_name, "ducklake_schema") + " WHERE schema_id = " +
	               std::to_string(schema_id) + " AND begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (end_snapshot IS NULL OR end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	schema_name = result->GetValue(0, 0).ToString();
	return true;
}

bool ResolveTableByIdAt(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                        int64_t snapshot_id, int64_t &schema_id, std::string &qualified_name) {
	auto result = conn.Query(
	    "SELECT s.schema_id, s.schema_name || '.' || t.table_name FROM " +
	    MetadataTable(catalog_name, "ducklake_table") + " t JOIN " + MetadataTable(catalog_name, "ducklake_schema") +
	    " s USING (schema_id) WHERE t.table_id = " + std::to_string(table_id) + " AND t.begin_snapshot <= " +
	    std::to_string(snapshot_id) + " AND (t.end_snapshot IS NULL OR t.end_snapshot > " +
	    std::to_string(snapshot_id) + ") AND s.begin_snapshot <= " + std::to_string(snapshot_id) +
	    " AND (s.end_snapshot IS NULL OR s.end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull() ||
	    result->GetValue(1, 0).IsNull()) {
		return false;
	}
	schema_id = result->GetValue(0, 0).GetValue<int64_t>();
	qualified_name = result->GetValue(1, 0).ToString();
	return true;
}

std::vector<std::pair<std::string, std::string>> ExpandedEventChangePairs(const std::string &consumer_name,
                                                                          const std::string &event_category,
                                                                          const std::string &change_type) {
	static const std::unordered_set<std::string> EVENTS = {"*", "dml", "ddl"};
	static const std::unordered_set<std::string> CHANGES = {"*", "insert", "update_preimage", "update_postimage",
	                                                        "delete"};
	if (event_category.empty() || EVENTS.find(event_category) == EVENTS.end()) {
		throw duckdb::InvalidInputException("cdc_consumer_create: consumer '%s' has invalid event_category '%s'",
		                                    consumer_name, event_category);
	}
	if (change_type.empty() || CHANGES.find(change_type) == CHANGES.end()) {
		throw duckdb::InvalidInputException("cdc_consumer_create: consumer '%s' has invalid change_type '%s'",
		                                    consumer_name, change_type);
	}
	std::vector<std::pair<std::string, std::string>> pairs;
	auto add_dml = [&]() {
		if (change_type == "*") {
			for (const auto &kind : {"insert", "update_preimage", "update_postimage", "delete"}) {
				pairs.emplace_back("dml", kind);
			}
		} else {
			pairs.emplace_back("dml", change_type);
		}
	};
	if (event_category == "dml") {
		add_dml();
	} else if (event_category == "ddl") {
		if (change_type != "*") {
			throw duckdb::InvalidInputException("cdc_consumer_create: DDL subscriptions require change_type '*' ");
		}
		pairs.emplace_back("ddl", "*");
	} else {
		if (change_type != "*") {
			throw duckdb::InvalidInputException(
			    "cdc_consumer_create: wildcard event_category requires change_type '*'");
		}
		add_dml();
		pairs.emplace_back("ddl", "*");
	}
	return pairs;
}

std::vector<ResolvedSubscriptionInput> ResolveSubscriptions(duckdb::Connection &conn, const std::string &catalog_name,
                                                            const std::string &consumer_name,
                                                            const duckdb::Value &subscriptions_value,
                                                            int64_t snapshot_id) {
	if (subscriptions_value.IsNull()) {
		throw duckdb::InvalidInputException("cdc_consumer_create requires subscriptions");
	}
	std::vector<ResolvedSubscriptionInput> resolved;
	for (const auto &child : duckdb::ListValue::GetChildren(subscriptions_value)) {
		RawSubscriptionInput raw;
		raw.scope_kind = ValueStringOrEmpty(StructChild(child, "scope_kind"));
		raw.schema_name = StructChild(child, "schema_name");
		raw.table_name = StructChild(child, "table_name");
		raw.schema_id = StructChild(child, "schema_id");
		raw.table_id = StructChild(child, "table_id");
		raw.event_category = StructChild(child, "event_category");
		raw.change_type = StructChild(child, "change_type");
		if (raw.scope_kind != "catalog" && raw.scope_kind != "schema" && raw.scope_kind != "table") {
			throw duckdb::InvalidInputException("cdc_consumer_create: invalid scope_kind '%s'", raw.scope_kind);
		}
		const auto event_category = ValueStringOrEmpty(raw.event_category);
		const auto change_type = ValueStringOrEmpty(raw.change_type);
		duckdb::Value schema_id;
		duckdb::Value table_id;
		std::string current_name;
		if (raw.scope_kind == "catalog") {
			if (!raw.schema_name.IsNull() || !raw.table_name.IsNull() || !raw.schema_id.IsNull() ||
			    !raw.table_id.IsNull()) {
				throw duckdb::InvalidInputException(
				    "cdc_consumer_create: catalog subscription cannot specify identity");
			}
		} else if (raw.scope_kind == "schema") {
			if (!raw.table_name.IsNull() || !raw.table_id.IsNull() ||
			    (!raw.schema_name.IsNull() && !raw.schema_id.IsNull())) {
				throw duckdb::InvalidInputException("cdc_consumer_create: schema subscription has invalid identity");
			}
			int64_t sid = 0;
			std::string sname;
			if (!raw.schema_name.IsNull()) {
				sname = raw.schema_name.ToString();
				if (!ResolveSchemaByNameAt(conn, catalog_name, sname, snapshot_id, sid)) {
					throw duckdb::InvalidInputException("cdc_consumer_create: schema identity '%s' is not live", sname);
				}
			} else if (!raw.schema_id.IsNull()) {
				sid = raw.schema_id.GetValue<int64_t>();
				if (!ResolveSchemaByIdAt(conn, catalog_name, sid, snapshot_id, sname)) {
					throw duckdb::InvalidInputException("cdc_consumer_create: schema identity %lld is not live",
					                                    static_cast<long long>(sid));
				}
			} else {
				throw duckdb::InvalidInputException("cdc_consumer_create: schema subscription requires identity");
			}
			schema_id = duckdb::Value::BIGINT(sid);
			current_name = sname;
		} else {
			if ((!raw.table_name.IsNull() && !raw.table_id.IsNull()) ||
			    (!raw.schema_name.IsNull() && !raw.table_id.IsNull())) {
				throw duckdb::InvalidInputException("cdc_consumer_create: table subscription has conflicting identity");
			}
			int64_t sid = 0;
			int64_t tid = 0;
			if (!raw.table_name.IsNull()) {
				const auto qualified = QualifyTableInput(raw.table_name.ToString(), raw.schema_name);
				if (!ResolveCurrentTableName(conn, catalog_name, qualified, snapshot_id, sid, tid)) {
					throw duckdb::InvalidInputException("cdc_consumer_create: table identity '%s' is not live",
					                                    qualified);
				}
				current_name = qualified;
			} else if (!raw.table_id.IsNull()) {
				tid = raw.table_id.GetValue<int64_t>();
				if (!ResolveTableByIdAt(conn, catalog_name, tid, snapshot_id, sid, current_name)) {
					const auto current_snapshot = CurrentSnapshot(conn, catalog_name);
					if (!ResolveTableByIdAt(conn, catalog_name, tid, current_snapshot, sid, current_name)) {
						throw duckdb::InvalidInputException("cdc_consumer_create: table identity %lld is not live",
						                                    static_cast<long long>(tid));
					}
				}
			} else {
				throw duckdb::InvalidInputException("cdc_consumer_create: table subscription requires identity");
			}
			schema_id = duckdb::Value::BIGINT(sid);
			table_id = duckdb::Value::BIGINT(tid);
		}
		const auto original_name = OptionalQualifiedName(raw.scope_kind, raw.schema_name, raw.table_name, raw.schema_id,
		                                                 raw.table_id, current_name);
		for (const auto &pair : ExpandedEventChangePairs(consumer_name, event_category, change_type)) {
			ResolvedSubscriptionInput out;
			out.scope_kind = raw.scope_kind;
			out.schema_id = schema_id;
			out.table_id = table_id;
			out.event_category = pair.first;
			out.change_type = pair.second;
			out.original_qualified_name = original_name;
			resolved.push_back(std::move(out));
		}
	}
	if (resolved.empty()) {
		throw duckdb::InvalidInputException("cdc_consumer_create requires non-empty subscriptions");
	}
	return resolved;
}

std::string SubscriptionJsonArray(const std::vector<ResolvedSubscriptionInput> &subscriptions) {
	std::ostringstream out;
	out << "[";
	for (size_t i = 0; i < subscriptions.size(); ++i) {
		if (i > 0) {
			out << ",";
		}
		const auto &sub = subscriptions[i];
		out << "{\"scope_kind\":\"" << JsonEscape(sub.scope_kind)
		    << "\",\"schema_id\":" << (sub.schema_id.IsNull() ? "null" : sub.schema_id.ToString())
		    << ",\"table_id\":" << (sub.table_id.IsNull() ? "null" : sub.table_id.ToString())
		    << ",\"event_category\":\"" << JsonEscape(sub.event_category) << "\",\"change_type\":\""
		    << JsonEscape(sub.change_type) << "\",\"original_qualified_name\":";
		if (sub.original_qualified_name.IsNull()) {
			out << "null";
		} else {
			out << "\"" << JsonEscape(sub.original_qualified_name.ToString()) << "\"";
		}
		out << "}";
	}
	out << "]";
	return out.str();
}

std::vector<duckdb::Value> CreateConsumer(duckdb::ClientContext &context, const ConsumerCreateData &data) {
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto subscriptions_table = StateTable(conn, data.catalog_name, CONSUMER_SUBSCRIPTIONS_TABLE);
	const auto quoted_name = QuoteLiteral(data.consumer_name);
	const auto actor_sql = "current_user";
	int64_t resolved_snapshot = ResolveCreateSnapshot(conn, data.catalog_name, data.start_at);
	int64_t schema_version = ResolveSchemaVersion(conn, data.catalog_name, resolved_snapshot);
	if (!data.subscriptions_specified) {
		throw duckdb::InvalidInputException("cdc_consumer_create requires subscriptions");
	}
	const auto subscriptions =
	    ResolveSubscriptions(conn, data.catalog_name, data.consumer_name, data.subscriptions, resolved_snapshot);

	// Audit details JSON stays append-only — every key from the create
	// call lands here (resolved snapshot included so a later
	// cdc_consumer_reset retains forensic continuity even if the
	// consumer is dropped/recreated under the same name).
	std::ostringstream details;
	details << "{\"start_at\":\"" << JsonEscape(data.start_at)
	        << "\",\"start_at_resolved_snapshot\":" << resolved_snapshot;
	details << ",\"subscriptions\":" << SubscriptionJsonArray(subscriptions);
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
		                         "lease_interval_seconds, stop_at_schema_change) VALUES (" +
		                         quoted_name + ", " + std::to_string(consumer_id) + ", " +
		                         std::to_string(resolved_snapshot) + ", " + std::to_string(schema_version) +
		                         ", now(), " + actor_sql + ", now(), 60, " +
		                         (data.stop_at_schema_change ? "TRUE" : "FALSE") + ")");
		for (size_t i = 0; i < subscriptions.size(); ++i) {
			const auto &sub = subscriptions[i];
			ExecuteChecked(conn, "INSERT INTO " + subscriptions_table +
			                         " (consumer_id, subscription_id, scope_kind, schema_id, table_id, event_category, "
			                         "change_type, original_qualified_name, created_at, metadata) VALUES (" +
			                         std::to_string(consumer_id) + ", " + std::to_string(i + 1) + ", " +
			                         QuoteLiteral(sub.scope_kind) + ", " +
			                         (sub.schema_id.IsNull() ? "NULL" : sub.schema_id.ToString()) + ", " +
			                         (sub.table_id.IsNull() ? "NULL" : sub.table_id.ToString()) + ", " +
			                         QuoteLiteral(sub.event_category) + ", " + QuoteLiteral(sub.change_type) + ", " +
			                         (sub.original_qualified_name.IsNull()
			                              ? "NULL"
			                              : QuoteLiteral(sub.original_qualified_name.ToString())) +
			                         ", now(), NULL)");
		}
		InsertAuditRow(conn, data.catalog_name, "consumer_create", data.consumer_name, consumer_id, details.str());
		ExecuteChecked(conn, "COMMIT");
		std::vector<duckdb::Value> first_row;
		const auto decorated = LoadConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);
		if (decorated.empty()) {
			return {duckdb::Value(data.consumer_name),
			        duckdb::Value::BIGINT(consumer_id),
			        duckdb::Value::BIGINT(resolved_snapshot),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value(),
			        duckdb::Value()};
		}
		const auto &sub = decorated[0];
		return {duckdb::Value(data.consumer_name),
		        duckdb::Value::BIGINT(consumer_id),
		        duckdb::Value::BIGINT(resolved_snapshot),
		        duckdb::Value::BIGINT(sub.subscription_id),
		        duckdb::Value(sub.scope_kind),
		        sub.schema_id,
		        sub.table_id,
		        duckdb::Value(sub.event_category),
		        duckdb::Value(sub.change_type),
		        sub.original_qualified_name,
		        sub.current_qualified_name};
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
	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	int64_t resolved_snapshot = ResolveResetSnapshot(conn, data.catalog_name, data.to_snapshot);
	int64_t schema_version = ResolveSchemaVersion(conn, data.catalog_name, resolved_snapshot);
	const auto details = "{\"from_snapshot\":" + std::to_string(row.last_committed_snapshot) +
	                     ",\"to_snapshot\":" + std::to_string(resolved_snapshot) + ",\"reset_kind\":\"" +
	                     JsonEscape(ResetKind(data.to_snapshot)) + "\"}";
	ExecuteChecked(conn, "UPDATE " + consumers + " SET last_committed_snapshot = " + std::to_string(resolved_snapshot) +
	                         ", last_committed_schema_version = " + std::to_string(schema_version) +
	                         ", owner_token = NULL, owner_acquired_at = NULL, owner_heartbeat_at = NULL, "
	                         "updated_at = now() WHERE consumer_name = " +
	                         QuoteLiteral(data.consumer_name));
	InsertAuditRow(conn, data.catalog_name, "consumer_reset", data.consumer_name, row.consumer_id, details);
	return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id),
	        duckdb::Value::BIGINT(row.last_committed_snapshot), duckdb::Value::BIGINT(resolved_snapshot)};
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
	const auto subscriptions = StateTable(conn, data.catalog_name, CONSUMER_SUBSCRIPTIONS_TABLE);
	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	const auto details = "{\"last_committed_snapshot\":" + std::to_string(row.last_committed_snapshot) +
	                     ",\"last_committed_schema_version\":" + std::to_string(row.last_committed_schema_version) +
	                     "}";
	InsertAuditRow(conn, data.catalog_name, "consumer_drop", data.consumer_name, row.consumer_id, details);
	ExecuteChecked(conn, "DELETE FROM " + subscriptions + " WHERE consumer_id = " + std::to_string(row.consumer_id));
	ExecuteChecked(conn, "DELETE FROM " + consumers + " WHERE consumer_name = " + QuoteLiteral(data.consumer_name));
	return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id),
	        duckdb::Value::BIGINT(row.last_committed_snapshot)};
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

ConsumerRow AcquireLeaseLegacy(duckdb::Connection &conn, const std::string &catalog_name,
                               const std::string &consumer_name, const std::string &cached_token) {
	const auto consumers = StateTable(conn, catalog_name, CONSUMERS_TABLE);
	const auto new_token = GenerateUuid(conn);
	const auto cached_token_sql = TokenSqlOrNull(cached_token);
	auto row = LoadConsumerOrThrow(conn, catalog_name, consumer_name);
	const bool expired = LeaseExpired(conn, row);
	const bool owns_lease =
	    !row.owner_token.IsNull() && !cached_token.empty() && row.owner_token.ToString() == cached_token;
	if (!row.owner_token.IsNull() && !expired && !owns_lease) {
		ThrowBusy(conn, catalog_name, consumer_name);
	}
	const auto owner_token =
	    row.owner_token.IsNull() || (expired && !owns_lease) ? new_token : row.owner_token.ToString();
	if (expired && !owns_lease) {
		const auto details = "{\"previous_token\":" + JsonValue(row.owner_token) +
		                     ",\"previous_acquired_at\":" + JsonValue(row.owner_acquired_at) +
		                     ",\"previous_heartbeat_at\":" + JsonValue(row.owner_heartbeat_at) +
		                     ",\"lease_interval_seconds\":" + std::to_string(row.lease_interval_seconds) + "}";
		InsertAuditRow(conn, catalog_name, "lease_force_acquire", consumer_name, row.consumer_id, details);
	}
	ExecuteChecked(conn, "UPDATE " + consumers + " SET owner_token = " + TokenSqlOrNull(owner_token) +
	                         ", owner_acquired_at = CASE WHEN owner_token IS NULL OR epoch(now()) - "
	                         "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > lease_interval_seconds THEN "
	                         "now() ELSE owner_acquired_at::TIMESTAMP WITH TIME ZONE "
	                         "END, owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
	                         QuoteLiteral(consumer_name) +
	                         " AND (owner_token IS NULL OR epoch(now()) - "
	                         "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > lease_interval_seconds OR "
	                         "owner_token = " +
	                         cached_token_sql + ")");
	row = LoadConsumerOrThrow(conn, catalog_name, consumer_name);
	if (row.owner_token.IsNull() || row.owner_token.ToString() != owner_token) {
		ThrowBusy(conn, catalog_name, consumer_name);
	}
	return row;
}

ConsumerRow AcquireLeaseReturning(duckdb::Connection &conn, const std::string &catalog_name,
                                  const std::string &consumer_name, const std::string &cached_token) {
	const auto consumers = StateTable(conn, catalog_name, CONSUMERS_TABLE);
	const auto cached_token_sql = TokenSqlOrNull(cached_token);
	auto result =
	    conn.Query("UPDATE " + consumers +
	               " SET owner_token = CASE WHEN owner_token IS NULL THEN CAST(uuid() AS VARCHAR) "
	               "ELSE CAST(owner_token AS VARCHAR) END, "
	               "owner_acquired_at = CASE WHEN owner_token IS NULL OR epoch(now()) - "
	               "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > lease_interval_seconds THEN now() "
	               "ELSE owner_acquired_at::TIMESTAMP WITH TIME ZONE END, owner_heartbeat_at = now(), updated_at = "
	               "now() WHERE consumer_name = " +
	               QuoteLiteral(consumer_name) + " AND (owner_token IS NULL OR owner_token = " + cached_token_sql +
	               ") RETURNING " + ConsumerRowProjection());
	ThrowIfQueryFailed(result);
	if (result && result->RowCount() > 0) {
		return ConsumerRowFromResult(*result, 0);
	}

	auto previous = LoadConsumerOrThrow(conn, catalog_name, consumer_name);
	if (!LeaseExpired(conn, previous)) {
		ThrowBusy(conn, catalog_name, consumer_name);
	}

	const auto previous_token_sql =
	    TokenSqlOrNull(previous.owner_token.IsNull() ? std::string() : previous.owner_token.ToString());
	result =
	    conn.Query("UPDATE " + consumers +
	               " SET owner_token = CAST(uuid() AS VARCHAR), owner_acquired_at = now(), owner_heartbeat_at = now(), "
	               "updated_at = now() WHERE consumer_name = " +
	               QuoteLiteral(consumer_name) + " AND owner_token = " + previous_token_sql +
	               " AND epoch(now()) - epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) > "
	               "lease_interval_seconds RETURNING " +
	               ConsumerRowProjection());
	ThrowIfQueryFailed(result);
	if (!result || result->RowCount() == 0) {
		ThrowBusy(conn, catalog_name, consumer_name);
	}

	auto acquired = ConsumerRowFromResult(*result, 0);
	const auto details = "{\"previous_token\":" + JsonValue(previous.owner_token) +
	                     ",\"previous_acquired_at\":" + JsonValue(previous.owner_acquired_at) +
	                     ",\"previous_heartbeat_at\":" + JsonValue(previous.owner_heartbeat_at) +
	                     ",\"lease_interval_seconds\":" + std::to_string(previous.lease_interval_seconds) + "}";
	InsertAuditRow(conn, catalog_name, "lease_force_acquire", consumer_name, previous.consumer_id, details);
	return acquired;
}

ConsumerRow AcquireLease(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name,
                         const std::string &cached_token, StateBackendKind backend) {
	if (SupportsUpdateReturning(backend)) {
		return AcquireLeaseReturning(conn, catalog_name, consumer_name, cached_token);
	}
	return AcquireLeaseLegacy(conn, catalog_name, consumer_name, cached_token);
}

bool TryUseFreshCachedLease(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name,
                            const std::string &cached_token, ConsumerRow &row) {
	if (cached_token.empty()) {
		return false;
	}
	const auto consumers = StateTable(conn, catalog_name, CONSUMERS_TABLE);
	auto result = conn.Query("SELECT " + ConsumerRowProjection() + " FROM " + consumers + " WHERE consumer_name = " +
	                         QuoteLiteral(consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token) +
	                         " AND owner_heartbeat_at IS NOT NULL AND epoch(now()) - "
	                         "epoch(owner_heartbeat_at::TIMESTAMP WITH TIME ZONE) < "
	                         "lease_interval_seconds / 4.0 LIMIT 1");
	ThrowIfQueryFailed(result);
	if (!result || result->RowCount() == 0) {
		return false;
	}
	row = ConsumerRowFromResult(*result, 0);
	return true;
}

struct WindowResolution {
	int64_t current_snapshot;
	duckdb::Value oldest_snapshot;
	bool last_snapshot_exists;
	int64_t first_snapshot;
	int64_t start_snapshot;
	int64_t end_snapshot;
	duckdb::Value last_schema_version;
	duckdb::Value start_schema_version;
	duckdb::Value end_schema_version;
	bool start_is_schema_change = false;
	int64_t next_schema_change = -1;
	duckdb::Value next_schema_change_schema_version;
};

int64_t RequiredInt64(const duckdb::Value &value, const std::string &description) {
	if (value.IsNull()) {
		throw duckdb::InvalidInputException("Unable to resolve %s", description);
	}
	return value.GetValue<int64_t>();
}

WindowResolution ResolveWindowFast(duckdb::Connection &conn, const std::string &catalog_name, int64_t last_snapshot,
                                   int64_t max_snapshots) {
	const auto snapshot_table = MetadataTable(catalog_name, "ducklake_snapshot");
	const auto changes_table = MetadataTable(catalog_name, "ducklake_snapshot_changes");
	const auto last_snapshot_sql = std::to_string(last_snapshot);
	const auto max_snapshots_sql = std::to_string(max_snapshots);
	const auto schema_change_filter = "(sc.changes_made LIKE 'created_%' OR sc.changes_made LIKE '%,created_%' OR "
	                                  "sc.changes_made LIKE 'altered_%' OR sc.changes_made LIKE '%,altered_%' OR "
	                                  "sc.changes_made LIKE 'dropped_%' OR sc.changes_made LIKE '%,dropped_%' OR "
	                                  "sc.changes_made LIKE 'renamed_%' OR sc.changes_made LIKE '%,renamed_%')";
	auto result =
	    conn.Query("WITH snapshot_bounds AS ("
	               "SELECT max(snapshot_id) AS current_snapshot, min(snapshot_id) AS oldest_snapshot, "
	               "count(*) FILTER (WHERE snapshot_id = " +
	               last_snapshot_sql + ") AS last_exists FROM " + snapshot_table +
	               "), first_change AS ("
	               "SELECT min(sc.snapshot_id) AS first_snapshot FROM " +
	               changes_table + " sc, snapshot_bounds b WHERE sc.snapshot_id > " + last_snapshot_sql +
	               " AND sc.snapshot_id <= b.current_snapshot), computed AS ("
	               "SELECT b.current_snapshot, b.oldest_snapshot, b.last_exists > 0 AS last_snapshot_exists, "
	               "COALESCE(f.first_snapshot, -1) AS first_snapshot, "
	               "CASE WHEN f.first_snapshot IS NULL THEN " +
	               last_snapshot_sql +
	               " + 1 ELSE f.first_snapshot END AS start_snapshot, "
	               "CASE WHEN f.first_snapshot IS NULL THEN " +
	               last_snapshot_sql + " ELSE LEAST(b.current_snapshot, f.first_snapshot + " + max_snapshots_sql +
	               " - 1) END AS end_snapshot FROM snapshot_bounds b CROSS JOIN first_change f)"
	               " SELECT c.current_snapshot, c.oldest_snapshot, c.last_snapshot_exists, c.first_snapshot, "
	               "c.start_snapshot, c.end_snapshot, last_s.schema_version AS last_schema_version, "
	               "start_s.schema_version AS start_schema_version, end_s.schema_version AS end_schema_version, "
	               "sc.snapshot_id AS change_snapshot, sc.changes_made, change_s.schema_version AS "
	               "change_schema_version FROM computed c LEFT JOIN " +
	               snapshot_table + " last_s ON last_s.snapshot_id = " + last_snapshot_sql + " LEFT JOIN " +
	               snapshot_table + " start_s ON start_s.snapshot_id = c.start_snapshot LEFT JOIN " + snapshot_table +
	               " end_s ON end_s.snapshot_id = c.end_snapshot LEFT JOIN " + changes_table +
	               " sc ON sc.snapshot_id >= c.start_snapshot AND sc.snapshot_id <= c.current_snapshot AND " +
	               schema_change_filter + " LEFT JOIN " + snapshot_table +
	               " change_s ON change_s.snapshot_id = sc.snapshot_id ORDER BY sc.snapshot_id ASC");
	ThrowIfQueryFailed(result);
	if (!result || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		throw duckdb::InvalidInputException("Unable to resolve current snapshot");
	}

	WindowResolution resolution;
	resolution.current_snapshot = result->GetValue(0, 0).GetValue<int64_t>();
	resolution.oldest_snapshot = result->GetValue(1, 0);
	resolution.last_snapshot_exists = !result->GetValue(2, 0).IsNull() && result->GetValue(2, 0).GetValue<bool>();
	resolution.first_snapshot = result->GetValue(3, 0).GetValue<int64_t>();
	resolution.start_snapshot = result->GetValue(4, 0).GetValue<int64_t>();
	resolution.end_snapshot = result->GetValue(5, 0).GetValue<int64_t>();
	resolution.last_schema_version = result->GetValue(6, 0);
	resolution.start_schema_version = result->GetValue(7, 0);
	resolution.end_schema_version = result->GetValue(8, 0);
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		const auto snapshot_value = result->GetValue(9, i);
		const auto changes_value = result->GetValue(10, i);
		if (snapshot_value.IsNull() || changes_value.IsNull()) {
			continue;
		}
		const auto snapshot_id = snapshot_value.GetValue<int64_t>();
		if (!SnapshotHasSchemaChange(changes_value.ToString())) {
			continue;
		}
		if (snapshot_id == resolution.start_snapshot) {
			resolution.start_is_schema_change = true;
			continue;
		}
		if (resolution.next_schema_change == -1) {
			resolution.next_schema_change = snapshot_id;
			resolution.next_schema_change_schema_version = result->GetValue(11, i);
		}
	}
	return resolution;
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

	duckdb::Connection conn(*context.db);
	const auto backend = CachedStateBackend(context, conn, data.catalog_name);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto cached_token = CachedToken(context, data.catalog_name, data.consumer_name);
	const auto snapshot_table = MetadataTable(data.catalog_name, "ducklake_snapshot");
	if (SupportsUpdateReturning(backend)) {
		auto fast_commit = conn.Query(
		    "UPDATE " + consumers + " SET last_committed_snapshot = " + std::to_string(data.snapshot_id) +
		    ", last_committed_schema_version = (SELECT schema_version FROM " + snapshot_table +
		    " WHERE snapshot_id = " + std::to_string(data.snapshot_id) +
		    " LIMIT 1), owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
		    QuoteLiteral(data.consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token) +
		    " AND last_committed_snapshot <= " + std::to_string(data.snapshot_id) + " AND EXISTS (SELECT 1 FROM " +
		    snapshot_table + " WHERE snapshot_id = " + std::to_string(data.snapshot_id) +
		    ") RETURNING consumer_name, last_committed_snapshot, last_committed_schema_version");
		ThrowIfQueryFailed(fast_commit);
		if (fast_commit && fast_commit->RowCount() > 0) {
			return {fast_commit->GetValue(0, 0), fast_commit->GetValue(1, 0), fast_commit->GetValue(2, 0)};
		}
	}

	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (cached_token.empty() || row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
		ThrowBusy(conn, data.catalog_name, data.consumer_name);
	}
	ValidateCommitSnapshot(conn, data.catalog_name, data.consumer_name, row.last_committed_snapshot, data.snapshot_id);
	const auto schema_version = ResolveSchemaVersion(conn, data.catalog_name, data.snapshot_id);
	ExecuteChecked(conn, "UPDATE " + consumers + " SET last_committed_snapshot = " + std::to_string(data.snapshot_id) +
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

	duckdb::Connection conn(*context.db);
	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	const auto cached_token = CachedToken(context, data.catalog_name, data.consumer_name);
	auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (cached_token.empty() || row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
		ThrowBusy(conn, data.catalog_name, data.consumer_name);
	}
	ExecuteChecked(conn, "UPDATE " + consumers +
	                         " SET owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
	                         QuoteLiteral(data.consumer_name) + " AND owner_token = " + TokenSqlOrNull(cached_token));
	row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	if (row.owner_token.IsNull() || row.owner_token.ToString() != cached_token) {
		ThrowBusy(conn, data.catalog_name, data.consumer_name);
	}
	return {duckdb::Value::BOOLEAN(true)};
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
	if (data.timeout_ms < 0) {
		throw duckdb::InvalidInputException("cdc_wait timeout_ms must be >= 0");
	}
	const auto timeout_ms = std::min<int64_t>(data.timeout_ms, HARD_WAIT_TIMEOUT_MS);
	if (data.timeout_ms > HARD_WAIT_TIMEOUT_MS) {
		EmitWaitTimeoutClampedNotice(data.timeout_ms, HARD_WAIT_TIMEOUT_MS);
	}
	if (timeout_ms > 0) {
		MaybeEmitWaitSharedConnectionWarning(context, timeout_ms);
	}
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
	         "subscription_count",
	         "subscriptions_active",
	         "subscriptions_renamed",
	         "subscriptions_dropped",
	         "stop_at_schema_change",
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
	return_types = {
	    duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,
	    duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,
	    duckdb::LogicalType::BOOLEAN,      duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,
	    duckdb::LogicalType::UUID,         duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::TIMESTAMP_TZ,
	    duckdb::LogicalType::INTEGER,      duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR,
	    duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR};
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
	auto query = "SELECT consumer_name, consumer_id, stop_at_schema_change, "
	             "last_committed_snapshot, last_committed_schema_version, owner_token, owner_acquired_at, "
	             "owner_heartbeat_at, lease_interval_seconds, created_at, created_by, updated_at, metadata FROM " +
	             StateTable(conn, data.catalog_name, CONSUMERS_TABLE) + " ORDER BY consumer_id ASC";
	auto query_result = conn.Query(query);
	if (!query_result || query_result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, query_result ? query_result->GetError() : query);
	}
	for (duckdb::idx_t row_idx = 0; row_idx < query_result->RowCount(); ++row_idx) {
		const auto consumer_name = query_result->GetValue(0, row_idx).ToString();
		const auto subscriptions = LoadConsumerSubscriptions(conn, data.catalog_name, consumer_name);
		int64_t active = 0;
		int64_t renamed = 0;
		int64_t dropped = 0;
		for (const auto &sub : subscriptions) {
			if (sub.status == "active") {
				active++;
			} else if (sub.status == "renamed") {
				renamed++;
			} else if (sub.status == "dropped") {
				dropped++;
			}
		}
		std::vector<duckdb::Value> row;
		row.push_back(query_result->GetValue(0, row_idx));
		row.push_back(query_result->GetValue(1, row_idx));
		row.push_back(duckdb::Value::BIGINT(static_cast<int64_t>(subscriptions.size())));
		row.push_back(duckdb::Value::BIGINT(active));
		row.push_back(duckdb::Value::BIGINT(renamed));
		row.push_back(duckdb::Value::BIGINT(dropped));
		for (duckdb::idx_t col_idx = 0; col_idx < query_result->ColumnCount(); ++col_idx) {
			if (col_idx < 2) {
				continue;
			}
			row.push_back(query_result->GetValue(col_idx, row_idx));
		}
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_consumer_subscriptions
//===--------------------------------------------------------------------===//

struct ConsumerSubscriptionsData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<ConsumerSubscriptionsData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> ConsumerSubscriptionsBind(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionBindInput &input,
                                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                                   duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_consumer_subscriptions requires catalog");
	}
	auto result = duckdb::make_uniq<ConsumerSubscriptionsData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	auto entry = input.named_parameters.find("name");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		result->consumer_name = entry->second.GetValue<std::string>();
	}
	names = {"consumer_name",
	         "consumer_id",
	         "subscription_id",
	         "scope_kind",
	         "schema_id",
	         "table_id",
	         "event_category",
	         "change_type",
	         "original_qualified_name",
	         "current_qualified_name",
	         "status"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ConsumerSubscriptionsInit(duckdb::ClientContext &context,
                                                                               duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<ConsumerSubscriptionsData>();
	CheckCatalogOrThrow(context, data.catalog_name);
	BootstrapConsumerStateOrThrow(context, data.catalog_name);
	duckdb::Connection conn(*context.db);
	for (const auto &sub : LoadConsumerSubscriptions(conn, data.catalog_name, data.consumer_name)) {
		result->rows.push_back({duckdb::Value(sub.consumer_name), duckdb::Value::BIGINT(sub.consumer_id),
		                        duckdb::Value::BIGINT(sub.subscription_id), duckdb::Value(sub.scope_kind),
		                        sub.schema_id, sub.table_id, duckdb::Value(sub.event_category),
		                        duckdb::Value(sub.change_type), sub.original_qualified_name, sub.current_qualified_name,
		                        duckdb::Value(sub.status)});
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
	auto result = conn.Query("SELECT " + ConsumerRowProjection() + " FROM " + consumers +
	                         " WHERE consumer_name = " + QuoteLiteral(consumer_name) + " LIMIT 1");
	if (!result || result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, result ? result->GetError() : "consumer lookup failed");
	}
	if (result->RowCount() == 0) {
		throw duckdb::InvalidInputException("consumer '%s' does not exist", consumer_name);
	}
	return ConsumerRowFromResult(*result, 0);
}

std::string CurrentQualifiedTableName(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                                      int64_t snapshot_id) {
	int64_t schema_id = 0;
	std::string qualified;
	if (!ResolveTableByIdAt(conn, catalog_name, table_id, snapshot_id, schema_id, qualified)) {
		return std::string();
	}
	return qualified;
}

bool ResolveCurrentTableName(duckdb::Connection &conn, const std::string &catalog_name,
                             const std::string &qualified_name, int64_t snapshot_id, int64_t &schema_id,
                             int64_t &table_id) {
	const auto dot = qualified_name.find('.');
	if (dot == std::string::npos) {
		return ResolveCurrentTableName(conn, catalog_name, std::string("main.") + qualified_name, snapshot_id,
		                               schema_id, table_id);
	}
	const auto schema_name = qualified_name.substr(0, dot);
	const auto table_name = qualified_name.substr(dot + 1);
	auto result =
	    conn.Query("SELECT s.schema_id, t.table_id FROM " + MetadataTable(catalog_name, "ducklake_table") + " t JOIN " +
	               MetadataTable(catalog_name, "ducklake_schema") + " s USING (schema_id) WHERE s.schema_name = " +
	               QuoteLiteral(schema_name) + " AND t.table_name = " + QuoteLiteral(table_name) +
	               " AND t.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (t.end_snapshot IS NULL OR t.end_snapshot > " + std::to_string(snapshot_id) +
	               ") AND s.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (s.end_snapshot IS NULL OR s.end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull() ||
	    result->GetValue(1, 0).IsNull()) {
		return false;
	}
	schema_id = result->GetValue(0, 0).GetValue<int64_t>();
	table_id = result->GetValue(1, 0).GetValue<int64_t>();
	return true;
}

std::string CurrentSchemaName(duckdb::Connection &conn, const std::string &catalog_name, int64_t schema_id,
                              int64_t snapshot_id) {
	std::string schema_name;
	if (!ResolveSchemaByIdAt(conn, catalog_name, schema_id, snapshot_id, schema_name)) {
		return std::string();
	}
	return schema_name;
}

std::vector<ConsumerSubscriptionRow>
LoadConsumerSubscriptions(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name) {
	std::vector<ConsumerSubscriptionRow> out;
	const auto current_snapshot = CurrentSnapshot(conn, catalog_name);
	std::ostringstream query;
	query << "SELECT c.consumer_name, c.consumer_id, s.subscription_id, s.scope_kind, s.schema_id, s.table_id, "
	      << "s.event_category, s.change_type, s.original_qualified_name FROM "
	      << StateTable(conn, catalog_name, CONSUMERS_TABLE) << " c JOIN "
	      << StateTable(conn, catalog_name, CONSUMER_SUBSCRIPTIONS_TABLE) << " s ON s.consumer_id = c.consumer_id";
	if (!consumer_name.empty()) {
		query << " WHERE c.consumer_name = " << QuoteLiteral(consumer_name);
	}
	query << " ORDER BY c.consumer_id ASC, s.subscription_id ASC";
	auto result = conn.Query(query.str());
	if (!result || result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        result ? result->GetError() : "consumer subscription lookup failed");
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		ConsumerSubscriptionRow row;
		row.consumer_name = result->GetValue(0, i).ToString();
		row.consumer_id = result->GetValue(1, i).GetValue<int64_t>();
		row.subscription_id = result->GetValue(2, i).GetValue<int64_t>();
		row.scope_kind = result->GetValue(3, i).ToString();
		row.schema_id = result->GetValue(4, i);
		row.table_id = result->GetValue(5, i);
		row.event_category = result->GetValue(6, i).ToString();
		row.change_type = result->GetValue(7, i).ToString();
		row.original_qualified_name = result->GetValue(8, i);
		row.status = "active";
		if (row.scope_kind == "catalog") {
			row.current_qualified_name = duckdb::Value();
		} else if (row.scope_kind == "schema" && !row.schema_id.IsNull()) {
			const auto current =
			    CurrentSchemaName(conn, catalog_name, row.schema_id.GetValue<int64_t>(), current_snapshot);
			if (current.empty()) {
				row.status = "dropped";
				row.current_qualified_name = duckdb::Value();
			} else {
				row.current_qualified_name = duckdb::Value(current);
				if (!row.original_qualified_name.IsNull() && row.original_qualified_name.ToString() != current) {
					row.status = "renamed";
				}
			}
		} else if (row.scope_kind == "table" && !row.table_id.IsNull()) {
			const auto current =
			    CurrentQualifiedTableName(conn, catalog_name, row.table_id.GetValue<int64_t>(), current_snapshot);
			if (current.empty()) {
				row.status = "dropped";
				row.current_qualified_name = duckdb::Value();
			} else {
				row.current_qualified_name = duckdb::Value(current);
				if (!row.original_qualified_name.IsNull() && row.original_qualified_name.ToString() != current) {
					row.status = "renamed";
				}
			}
		}
		out.push_back(std::move(row));
	}
	return out;
}

bool SubscriptionCoversTable(const ConsumerSubscriptionRow &subscription, int64_t schema_id, int64_t table_id,
                             const std::string &event_category) {
	if (subscription.status == "dropped" || subscription.event_category != event_category) {
		return false;
	}
	if (subscription.scope_kind == "catalog") {
		return true;
	}
	if (subscription.scope_kind == "schema" && !subscription.schema_id.IsNull()) {
		return subscription.schema_id.GetValue<int64_t>() == schema_id;
	}
	if (subscription.scope_kind == "table" && !subscription.table_id.IsNull()) {
		return subscription.table_id.GetValue<int64_t>() == table_id;
	}
	return false;
}

std::vector<std::string> MatchingDmlChangeTypes(const std::vector<ConsumerSubscriptionRow> &subscriptions,
                                                int64_t schema_id, int64_t table_id) {
	std::vector<std::string> change_types;
	for (const auto &subscription : subscriptions) {
		if (SubscriptionCoversTable(subscription, schema_id, table_id, "dml")) {
			change_types.push_back(subscription.change_type);
		}
	}
	std::sort(change_types.begin(), change_types.end());
	change_types.erase(std::unique(change_types.begin(), change_types.end()), change_types.end());
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
	if (data.max_snapshots > HARD_MAX_SNAPSHOTS) {
		ThrowMaxSnapshotsExceeded(data.max_snapshots);
	}
	if (data.max_snapshots < 1) {
		throw duckdb::InvalidInputException("cdc_window max_snapshots must be >= 1");
	}

	duckdb::Connection conn(*context.db);
	const auto cached_token = CachedToken(context, data.catalog_name, data.consumer_name);
	ConsumerRow row;
	if (!TryUseFreshCachedLease(conn, data.catalog_name, data.consumer_name, cached_token, row)) {
		const auto backend = CachedStateBackend(context, conn, data.catalog_name);
		row = AcquireLease(conn, data.catalog_name, data.consumer_name, cached_token, backend);
	}
	const auto last_snapshot = row.last_committed_snapshot;
	const auto stop_at_schema_change = row.stop_at_schema_change;
	const auto owner_token = row.owner_token.ToString();
	CacheToken(context, data.catalog_name, data.consumer_name, owner_token);

	const auto resolved = ResolveWindowFast(conn, data.catalog_name, last_snapshot, data.max_snapshots);
	if (!resolved.last_snapshot_exists) {
		const auto oldest_snapshot = RequiredInt64(resolved.oldest_snapshot, "oldest snapshot");
		throw duckdb::InvalidInputException(
		    "CDC_GAP: consumer '%s' is at snapshot %lld, but the oldest available snapshot is %lld. To recover "
		    "and skip the gap: CALL cdc_consumer_reset('%s', '%s', to_snapshot => 'oldest_available'); To "
		    "preserve all events, run consumers more frequently than your expire_older_than setting.",
		    data.consumer_name, static_cast<long long>(last_snapshot), static_cast<long long>(oldest_snapshot),
		    data.catalog_name, data.consumer_name);
	}
	const auto current_snapshot = resolved.current_snapshot;
	const auto first_snapshot = resolved.first_snapshot;
	const auto start_snapshot = resolved.start_snapshot;
	int64_t end_snapshot = resolved.end_snapshot;
	bool schema_changes_pending = false;
	int64_t boundary_next_snapshot = -1;
	duckdb::Value boundary_next_schema_version;
	auto schema_version = last_snapshot <= current_snapshot
	                          ? RequiredInt64(resolved.last_schema_version, "schema_version")
	                          : row.last_committed_schema_version;
	if (first_snapshot != -1 && start_snapshot <= current_snapshot) {
		schema_version = RequiredInt64(resolved.start_schema_version, "schema_version");
		// `start_snapshot` itself can be a schema-change snapshot
		// (e.g. consumer just committed past the previous boundary
		// and is now reading the ALTER). The window includes it
		// regardless of `stop_at_schema_change`; we only need to
		// surface `schema_changes_pending = true` so callers know
		// the window straddles a schema version transition.
		if (resolved.start_is_schema_change) {
			schema_changes_pending = true;
		}
		const auto next_schema_change = resolved.next_schema_change;
		if (next_schema_change != -1 && next_schema_change <= end_snapshot) {
			schema_changes_pending = true;
			if (stop_at_schema_change) {
				end_snapshot = next_schema_change - 1;
			}
			boundary_next_snapshot = next_schema_change;
			boundary_next_schema_version = resolved.next_schema_change_schema_version;
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
		end_schema_version_for_notice =
		    RequiredInt64(has_changes ? resolved.end_schema_version : resolved.last_schema_version, "schema_version");
	}
	// Emit the CDC_SCHEMA_BOUNDARY notice after window resolution so callers
	// see the boundary before deciding which DDL/DML paths to drain.
	if (schema_changes_pending && boundary_next_snapshot != -1 &&
	    end_schema_version_for_notice != RequiredInt64(boundary_next_schema_version, "schema_version")) {
		EmitSchemaBoundaryNotice(data.consumer_name, has_changes ? end_snapshot : last_snapshot,
		                         end_schema_version_for_notice, boundary_next_snapshot,
		                         RequiredInt64(boundary_next_schema_version, "schema_version"));
	}
	return {duckdb::Value::BIGINT(start_snapshot), duckdb::Value::BIGINT(end_snapshot),
	        duckdb::Value::BOOLEAN(has_changes), duckdb::Value::BIGINT(schema_version),
	        duckdb::Value::BOOLEAN(schema_changes_pending)};
}

void RegisterConsumerFunctions(duckdb::ExtensionLoader &loader) {
	duckdb::child_list_t<duckdb::LogicalType> subscription_fields;
	subscription_fields.emplace_back("scope_kind", duckdb::LogicalType::VARCHAR);
	subscription_fields.emplace_back("schema_name", duckdb::LogicalType::VARCHAR);
	subscription_fields.emplace_back("table_name", duckdb::LogicalType::VARCHAR);
	subscription_fields.emplace_back("schema_id", duckdb::LogicalType::BIGINT);
	subscription_fields.emplace_back("table_id", duckdb::LogicalType::BIGINT);
	subscription_fields.emplace_back("event_category", duckdb::LogicalType::VARCHAR);
	subscription_fields.emplace_back("change_type", duckdb::LogicalType::VARCHAR);
	const auto subscriptions_type = duckdb::LogicalType::LIST(duckdb::LogicalType::STRUCT(subscription_fields));
	for (const auto &name : {"cdc_consumer_create", "ducklake_cdc_consumer_create"}) {
		duckdb::TableFunction create_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, ConsumerCreateBind, ConsumerCreateInit);
		create_function.named_parameters["start_at"] = duckdb::LogicalType::VARCHAR;
		create_function.named_parameters["subscriptions"] = subscriptions_type;
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

	for (const auto &name : {"cdc_consumer_subscriptions", "ducklake_cdc_consumer_subscriptions"}) {
		duckdb::TableFunction subscriptions_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR}, RowScanExecute,
		    ConsumerSubscriptionsBind, ConsumerSubscriptionsInit);
		subscriptions_function.named_parameters["name"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(subscriptions_function);
	}
}

} // namespace duckdb_cdc
