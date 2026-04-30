//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// consumer_state.cpp
//
// Lazy catalog-resident consumer-state bootstrap plus the first consumer
// lifecycle functions.
//===----------------------------------------------------------------------===//

#include "consumer_state.hpp"

#include "compat_check.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace duckdb_cdc {

namespace {

constexpr const char *CONSUMERS_TABLE = "__ducklake_cdc_consumers";
constexpr const char *AUDIT_TABLE = "__ducklake_cdc_audit";
constexpr const char *DLQ_TABLE = "__ducklake_cdc_dlq";
constexpr int64_t DEFAULT_MAX_SNAPSHOTS = 100;
constexpr int64_t HARD_MAX_SNAPSHOTS = 1000;
constexpr int64_t DEFAULT_WAIT_TIMEOUT_MS = 30000;
constexpr int64_t HARD_WAIT_TIMEOUT_MS = 300000;
constexpr int64_t WAIT_INITIAL_INTERVAL_MS = 100;
constexpr int64_t WAIT_MAX_INTERVAL_MS = 10000;

std::mutex TOKEN_CACHE_MUTEX;
std::unordered_map<std::string, std::string> TOKEN_CACHE;

std::mutex WAIT_WARNING_MUTEX;
std::unordered_set<int64_t> WAIT_WARNED_CONNECTIONS;

std::string QuoteIdentifier(const std::string &identifier) {
	return duckdb::KeywordHelper::WriteOptionallyQuoted(identifier);
}

std::string QuoteLiteral(const std::string &value) {
	return duckdb::KeywordHelper::WriteQuoted(value);
}

std::string JsonEscape(const std::string &value) {
	std::ostringstream out;
	for (auto c : value) {
		switch (c) {
		case '\\':
			out << "\\\\";
			break;
		case '"':
			out << "\\\"";
			break;
		default:
			out << c;
			break;
		}
	}
	return out.str();
}

std::string MainTable(const std::string &catalog_name, const std::string &table_name) {
	return QuoteIdentifier(catalog_name) + ".main." + QuoteIdentifier(table_name);
}

std::string MetadataTable(const std::string &catalog_name, const std::string &table_name) {
	return QuoteIdentifier("__ducklake_metadata_" + catalog_name) + "." + QuoteIdentifier(table_name);
}

std::string GetStringArg(const duckdb::Value &value, const std::string &name) {
	if (value.IsNull()) {
		throw duckdb::BinderException("%s cannot be NULL", name);
	}
	return value.GetValue<std::string>();
}

bool TryParseInt64(const std::string &input, int64_t &out) {
	if (input.empty()) {
		return false;
	}
	size_t i = 0;
	if (input[0] == '+' || input[0] == '-') {
		if (input.size() == 1) {
			return false;
		}
		i = 1;
	}
	for (; i < input.size(); ++i) {
		if (!std::isdigit(static_cast<unsigned char>(input[i]))) {
			return false;
		}
	}
	try {
		out = std::stoll(input);
		return true;
	} catch (...) {
		return false;
	}
}

int64_t SingleInt64(duckdb::MaterializedQueryResult &result, const std::string &description) {
	if (result.HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, result.GetError());
	}
	if (result.RowCount() == 0) {
		throw duckdb::InvalidInputException("Unable to resolve %s", description);
	}
	auto value = result.GetValue(0, 0);
	if (value.IsNull()) {
		throw duckdb::InvalidInputException("Unable to resolve %s", description);
	}
	return value.GetValue<int64_t>();
}

int64_t ResolveSnapshot(duckdb::Connection &conn, const std::string &catalog_name, const std::string &literal,
                        const std::string &argument_name, const std::string &feature_name, bool null_means_oldest) {
	const auto snapshot_table = MetadataTable(catalog_name, "ducklake_snapshot");
	const auto lower_literal = duckdb::StringUtil::Lower(literal);
	if (literal.empty() && null_means_oldest) {
		auto result = conn.Query("SELECT min(snapshot_id) FROM " + snapshot_table);
		return SingleInt64(*result, argument_name + " => NULL");
	}
	if (lower_literal == "now") {
		auto result = conn.Query("SELECT max(snapshot_id) FROM " + snapshot_table);
		return SingleInt64(*result, argument_name + " => 'now'");
	}
	if (lower_literal == "beginning" || lower_literal == "oldest" || lower_literal == "oldest_available") {
		auto result = conn.Query("SELECT min(snapshot_id) FROM " + snapshot_table);
		return SingleInt64(*result, argument_name + " => 'beginning'");
	}
	int64_t snapshot = 0;
	if (TryParseInt64(literal, snapshot)) {
		auto result = conn.Query("SELECT snapshot_id FROM " + snapshot_table +
		                         " WHERE snapshot_id = " + std::to_string(snapshot) + " LIMIT 1");
		if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
			throw duckdb::InvalidInputException("%s %s snapshot %lld does not exist", feature_name, argument_name,
			                                    static_cast<long long>(snapshot));
		}
		return snapshot;
	}
	throw duckdb::NotImplementedException(
	    "CDC_FEATURE_NOT_YET_IMPLEMENTED: " + feature_name + " " + argument_name +
	    " as TIMESTAMP is deferred; use 'now', 'beginning', 'oldest_available', or a BIGINT snapshot id");
}

int64_t ResolveCreateSnapshot(duckdb::Connection &conn, const std::string &catalog_name, const std::string &start_at) {
	return ResolveSnapshot(conn, catalog_name, start_at, "start_at", "cdc_consumer_create", false);
}

int64_t ResolveResetSnapshot(duckdb::Connection &conn, const std::string &catalog_name,
                             const std::string &to_snapshot) {
	return ResolveSnapshot(conn, catalog_name, to_snapshot, "to_snapshot", "cdc_consumer_reset", true);
}

int64_t ResolveSchemaVersion(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id) {
	const auto snapshot_table = MetadataTable(catalog_name, "ducklake_snapshot");
	auto result = conn.Query("SELECT schema_version FROM " + snapshot_table +
	                         " WHERE snapshot_id = " + std::to_string(snapshot_id) + " LIMIT 1");
	return SingleInt64(*result, "schema_version");
}

int64_t CurrentSnapshot(duckdb::Connection &conn, const std::string &catalog_name) {
	auto result = conn.Query("SELECT max(snapshot_id) FROM " + MetadataTable(catalog_name, "ducklake_snapshot"));
	return SingleInt64(*result, "current snapshot");
}

void EnsureSnapshotExistsOrGap(duckdb::Connection &conn, const std::string &catalog_name,
                               const std::string &consumer_name, int64_t snapshot_id) {
	const auto snapshot_table = MetadataTable(catalog_name, "ducklake_snapshot");
	auto exists =
	    conn.Query("SELECT count(*) FROM " + snapshot_table + " WHERE snapshot_id = " + std::to_string(snapshot_id));
	if (SingleInt64(*exists, "snapshot existence") > 0) {
		return;
	}
	auto oldest = conn.Query("SELECT min(snapshot_id) FROM " + snapshot_table);
	const auto oldest_snapshot = SingleInt64(*oldest, "oldest snapshot");
	throw duckdb::InvalidInputException(
	    "CDC_GAP: consumer '%s' is at snapshot %lld, but the oldest available snapshot is %lld. To recover and skip "
	    "the gap: CALL cdc_consumer_reset('%s', '%s', to_snapshot => 'oldest_available'); To preserve all events, "
	    "run consumers more frequently than your expire_older_than setting.",
	    consumer_name, static_cast<long long>(snapshot_id), static_cast<long long>(oldest_snapshot), catalog_name,
	    consumer_name);
}

std::unordered_set<int64_t> InternalTableIds(duckdb::Connection &conn, const std::string &catalog_name) {
	std::unordered_set<int64_t> ids;
	auto result = conn.Query("SELECT table_id FROM " + MetadataTable(catalog_name, "ducklake_table") +
	                         " WHERE table_name LIKE '\\_\\_ducklake\\_cdc\\_%' ESCAPE '\\'");
	if (!result || result->HasError()) {
		return ids;
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		auto value = result->GetValue(0, i);
		if (!value.IsNull()) {
			ids.insert(value.GetValue<int64_t>());
		}
	}
	return ids;
}

bool StartsWith(const std::string &value, const std::string &prefix) {
	return value.rfind(prefix, 0) == 0;
}

bool InternalChangeToken(const std::string &token, const std::unordered_set<int64_t> &internal_ids) {
	if (token.find("__ducklake_cdc_") != std::string::npos) {
		return true;
	}
	auto colon = token.rfind(':');
	if (colon == std::string::npos || colon + 1 >= token.size()) {
		return false;
	}
	int64_t table_id = 0;
	if (!TryParseInt64(token.substr(colon + 1), table_id)) {
		return false;
	}
	return internal_ids.find(table_id) != internal_ids.end();
}

bool InternalOnlyChanges(const std::string &changes_made, const std::unordered_set<int64_t> &internal_ids) {
	if (changes_made.empty()) {
		return false;
	}
	size_t start = 0;
	bool saw_token = false;
	while (start < changes_made.size()) {
		auto comma = changes_made.find(',', start);
		auto token = changes_made.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
		saw_token = true;
		if (!InternalChangeToken(token, internal_ids)) {
			return false;
		}
		if (comma == std::string::npos) {
			break;
		}
		start = comma + 1;
	}
	return saw_token;
}

int64_t CurrentExternalSnapshot(duckdb::Connection &conn, const std::string &catalog_name, int64_t last_snapshot) {
	const auto internal_ids = InternalTableIds(conn, catalog_name);
	auto result =
	    conn.Query("SELECT snapshot_id, changes_made FROM " + MetadataTable(catalog_name, "ducklake_snapshot_changes") +
	               " WHERE snapshot_id > " + std::to_string(last_snapshot) + " ORDER BY snapshot_id ASC");
	if (!result || result->HasError()) {
		return CurrentSnapshot(conn, catalog_name);
	}
	int64_t current_external = last_snapshot;
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		auto snapshot_value = result->GetValue(0, i);
		auto changes_value = result->GetValue(1, i);
		if (snapshot_value.IsNull() || changes_value.IsNull()) {
			continue;
		}
		if (!InternalOnlyChanges(changes_value.ToString(), internal_ids)) {
			current_external = snapshot_value.GetValue<int64_t>();
		}
	}
	return current_external;
}

int64_t FirstExternalSnapshotAfter(duckdb::Connection &conn, const std::string &catalog_name, int64_t last_snapshot,
                                   int64_t current_snapshot) {
	const auto internal_ids = InternalTableIds(conn, catalog_name);
	auto result =
	    conn.Query("SELECT snapshot_id, changes_made FROM " + MetadataTable(catalog_name, "ducklake_snapshot_changes") +
	               " WHERE snapshot_id > " + std::to_string(last_snapshot) +
	               " AND snapshot_id <= " + std::to_string(current_snapshot) + " ORDER BY snapshot_id ASC");
	if (!result || result->HasError()) {
		return -1;
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		auto snapshot_value = result->GetValue(0, i);
		auto changes_value = result->GetValue(1, i);
		if (!snapshot_value.IsNull() && !changes_value.IsNull() &&
		    !InternalOnlyChanges(changes_value.ToString(), internal_ids)) {
			return snapshot_value.GetValue<int64_t>();
		}
	}
	return -1;
}

//! True iff `changes_made` contains at least one DDL token that
//! references a non-internal entity (i.e. an external schema change at
//! this snapshot). Bootstrap CREATE TABLE on `__ducklake_cdc_*` bumps
//! DuckLake's `schema_version` but is invisible here — both because
//! `InternalChangeToken` recognises the table_id and because the
//! token's literal name carries the `__ducklake_cdc_` prefix.
bool SnapshotHasExternalSchemaChange(const std::string &changes_made, const std::unordered_set<int64_t> &internal_ids) {
	static const std::vector<std::string> DDL_PREFIXES = {
	    "created_table:", "altered_table:", "dropped_table:",  "created_view:",
	    "altered_view:",  "dropped_view:",  "created_schema:", "dropped_schema:",
	};
	if (changes_made.empty()) {
		return false;
	}
	size_t start = 0;
	while (start < changes_made.size()) {
		auto comma = changes_made.find(',', start);
		auto token = changes_made.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
		for (const auto &prefix : DDL_PREFIXES) {
			if (StartsWith(token, prefix) && !InternalChangeToken(token, internal_ids)) {
				return true;
			}
		}
		if (comma == std::string::npos) {
			break;
		}
		start = comma + 1;
	}
	return false;
}

//! True iff the snapshot at `snapshot_id` carries at least one
//! external DDL token (i.e. it is itself a schema-change snapshot).
//! Used by ReadWindow to surface `schema_changes_pending = true` even
//! when the schema change is at `start_snapshot` (in which case
//! `NextExternalSchemaChangeSnapshot` returns -1, since it searches
//! strictly AFTER start).
bool SnapshotIsExternalSchemaChange(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id) {
	const auto internal_ids = InternalTableIds(conn, catalog_name);
	auto result = conn.Query("SELECT changes_made FROM " + MetadataTable(catalog_name, "ducklake_snapshot_changes") +
	                         " WHERE snapshot_id = " + std::to_string(snapshot_id));
	if (!result || result->HasError() || result->RowCount() == 0) {
		return false;
	}
	auto changes_value = result->GetValue(0, 0);
	if (changes_value.IsNull()) {
		return false;
	}
	return SnapshotHasExternalSchemaChange(changes_value.ToString(), internal_ids);
}

//! Returns the first snapshot in `(start_snapshot, current_snapshot]`
//! whose `changes_made` contains an external DDL token. Comparing
//! `ResolveSchemaVersion` directly was wrong: bootstrap-time CREATE
//! TABLE on `__ducklake_cdc_*` bumps DuckLake's physical
//! `schema_version`, which would falsely flag every subsequent INSERT
//! as a schema boundary for any consumer started before bootstrap.
//! `base_schema_version` is unused now (kept for source compat with
//! older callers); the predicate is structural — "did this snapshot do
//! external DDL?" — not numeric.
int64_t NextExternalSchemaChangeSnapshot(duckdb::Connection &conn, const std::string &catalog_name,
                                         int64_t start_snapshot, int64_t current_snapshot,
                                         int64_t /*base_schema_version*/) {
	// Search strictly AFTER `start_snapshot`: the snapshot at `start` is
	// the first one in this window. If it carries a schema change itself
	// (e.g. the consumer just committed past the previous boundary and is
	// about to read the ALTER), the window's schema_version is the
	// post-change version (set by ResolveSchemaVersion(start) in the
	// caller); bounding the window at `start - 1` here would be wrong.
	const auto internal_ids = InternalTableIds(conn, catalog_name);
	auto result =
	    conn.Query("SELECT snapshot_id, changes_made FROM " + MetadataTable(catalog_name, "ducklake_snapshot_changes") +
	               " WHERE snapshot_id > " + std::to_string(start_snapshot) +
	               " AND snapshot_id <= " + std::to_string(current_snapshot) + " ORDER BY snapshot_id ASC");
	if (!result || result->HasError()) {
		return -1;
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		auto snapshot_value = result->GetValue(0, i);
		auto changes_value = result->GetValue(1, i);
		if (snapshot_value.IsNull() || changes_value.IsNull()) {
			continue;
		}
		if (SnapshotHasExternalSchemaChange(changes_value.ToString(), internal_ids)) {
			return snapshot_value.GetValue<int64_t>();
		}
	}
	return -1;
}

void ThrowIfQueryFailed(const duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result) {
	if (result && result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, result->GetError());
	}
}

void ExecuteChecked(duckdb::Connection &conn, const std::string &sql) {
	auto result = conn.Query(sql);
	ThrowIfQueryFailed(result);
}

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

ConsumerRow LoadConsumerOrThrow(duckdb::Connection &conn, const std::string &catalog_name,
                                const std::string &consumer_name) {
	const auto consumers = MainTable(catalog_name, CONSUMERS_TABLE);
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
	row.tables = result->GetValue(8, 0);
	row.change_types = result->GetValue(9, 0);
	row.event_categories = result->GetValue(10, 0);
	const auto stop_value = result->GetValue(11, 0);
	row.stop_at_schema_change = stop_value.IsNull() ? true : stop_value.GetValue<bool>();
	return row;
}

std::unordered_set<std::string> CollectFilterTables(const duckdb::Value &tables_value) {
	std::unordered_set<std::string> filter_tables;
	if (tables_value.IsNull()) {
		return filter_tables;
	}
	for (const auto &child : duckdb::ListValue::GetChildren(tables_value)) {
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
	for (const auto &child : duckdb::ListValue::GetChildren(change_types_value)) {
		if (child.IsNull()) {
			continue;
		}
		change_types.push_back(child.ToString());
	}
	return change_types;
}

std::string JsonValue(const duckdb::Value &value) {
	if (value.IsNull()) {
		return "null";
	}
	return "\"" + JsonEscape(value.ToString()) + "\"";
}

std::string TokenCacheKey(duckdb::ClientContext &context, const std::string &catalog_name,
                          const std::string &consumer_name) {
	return std::to_string(context.GetConnectionId()) + ":" + catalog_name + ":" + consumer_name;
}

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

std::string GenerateUuid(duckdb::Connection &conn) {
	auto result = conn.Query("SELECT uuid()");
	if (!result || result->HasError() || result->RowCount() == 0) {
		throw duckdb::InvalidInputException("Unable to generate owner token");
	}
	return result->GetValue(0, 0).ToString();
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

void InsertAuditRow(duckdb::Connection &conn, const std::string &catalog_name, const std::string &action,
                    const std::string &consumer_name, int64_t consumer_id, const std::string &details) {
	const auto audit = MainTable(catalog_name, AUDIT_TABLE);
	auto next_audit_id_result = conn.Query("SELECT COALESCE(MAX(audit_id), 0) + 1 FROM " + audit);
	int64_t audit_id = SingleInt64(*next_audit_id_result, "next audit_id");
	ExecuteChecked(conn, "INSERT INTO " + audit +
	                         " (audit_id, ts, actor, action, consumer_name, consumer_id, details) VALUES (" +
	                         std::to_string(audit_id) + ", now(), current_user, " + QuoteLiteral(action) + ", " +
	                         QuoteLiteral(consumer_name) + ", " + std::to_string(consumer_id) + ", " +
	                         QuoteLiteral(details) + ")");
}

std::string ConsumersDdl(const std::string &catalog_name) {
	return "CREATE TABLE IF NOT EXISTS " + MainTable(catalog_name, CONSUMERS_TABLE) +
	       " ("
	       "consumer_name VARCHAR, "
	       "consumer_id BIGINT NOT NULL, "
	       "tables VARCHAR[], "
	       "change_types VARCHAR[], "
	       "event_categories VARCHAR[], "
	       "stop_at_schema_change BOOLEAN NOT NULL, "
	       "dml_blocked_by_failed_ddl BOOLEAN NOT NULL, "
	       "last_committed_snapshot BIGINT, "
	       "last_committed_schema_version BIGINT, "
	       "owner_token UUID, "
	       "owner_acquired_at TIMESTAMP WITH TIME ZONE, "
	       "owner_heartbeat_at TIMESTAMP WITH TIME ZONE, "
	       "lease_interval_seconds INTEGER NOT NULL, "
	       "created_at TIMESTAMP WITH TIME ZONE NOT NULL, "
	       "created_by VARCHAR, "
	       "updated_at TIMESTAMP WITH TIME ZONE NOT NULL, "
	       "metadata VARCHAR"
	       ")";
}

std::string AuditDdl(const std::string &catalog_name) {
	return "CREATE TABLE IF NOT EXISTS " + MainTable(catalog_name, AUDIT_TABLE) +
	       " ("
	       "audit_id BIGINT, "
	       "ts TIMESTAMP WITH TIME ZONE NOT NULL, "
	       "actor VARCHAR, "
	       "action VARCHAR NOT NULL, "
	       "consumer_name VARCHAR, "
	       "consumer_id BIGINT, "
	       "details VARCHAR"
	       ")";
}

std::string DlqDdl(const std::string &catalog_name) {
	return "CREATE TABLE IF NOT EXISTS " + MainTable(catalog_name, DLQ_TABLE) +
	       " ("
	       "consumer_name VARCHAR, "
	       "consumer_id BIGINT, "
	       "snapshot_id BIGINT, "
	       "event_kind VARCHAR, "
	       "table_name VARCHAR, "
	       "object_kind VARCHAR, "
	       "object_id BIGINT, "
	       "rowid BIGINT, "
	       "change_type VARCHAR, "
	       "failed_at TIMESTAMP WITH TIME ZONE, "
	       "attempts INTEGER, "
	       "last_error VARCHAR, "
	       "payload VARCHAR"
	       ")";
}

struct RowScanState : public duckdb::GlobalTableFunctionState {
	std::vector<std::vector<duckdb::Value>> rows;
	duckdb::idx_t offset = 0;
};

void RowScanExecute(duckdb::ClientContext &context, duckdb::TableFunctionInput &input, duckdb::DataChunk &output) {
	auto &state = input.global_state->Cast<RowScanState>();
	if (state.offset >= state.rows.size()) {
		return;
	}
	duckdb::idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset++];
		if (row.size() != output.ColumnCount()) {
			throw duckdb::InternalException("Unaligned consumer-state row in table function result");
		}
		for (duckdb::idx_t col = 0; col < row.size(); ++col) {
			output.SetValue(col, count, row[col]);
		}
		count++;
	}
	output.SetCardinality(count);
}

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

//! Build a SQL `VARCHAR[]` literal — `['a', 'b']` or `NULL` — from a
//! per-consumer filter list. Empty + unspecified collapse to NULL on
//! disk so the consumer-state row is unambiguous; the create-time
//! validator enforces "if specified, must be non-empty" so this branch
//! is only ever hit for unspecified inputs.
std::string BuildVarcharListLiteral(const std::vector<std::string> &values, bool specified) {
	if (!specified || values.empty()) {
		return "NULL";
	}
	std::ostringstream out;
	out << "[";
	for (size_t i = 0; i < values.size(); ++i) {
		if (i > 0) {
			out << ", ";
		}
		out << QuoteLiteral(values[i]);
	}
	out << "]";
	return out.str();
}

//! Build the human-readable list of qualified table names that exist at
//! `snapshot_id`, used to fill out the `CDC_INVALID_TABLE_FILTER`
//! reference message per docs/errors.md ("Tables in the lake at
//! snapshot N: ..."). Empty list ⇒ "(no tables)".
std::string ListTablesAtSnapshot(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id) {
	auto result =
	    conn.Query("SELECT s.schema_name || '.' || t.table_name FROM " + MetadataTable(catalog_name, "ducklake_table") +
	               " t JOIN " + MetadataTable(catalog_name, "ducklake_schema") +
	               " s USING (schema_id) WHERE t.begin_snapshot <= " + std::to_string(snapshot_id) +
	               " AND (t.end_snapshot IS NULL OR t.end_snapshot > " + std::to_string(snapshot_id) + ") ORDER BY 1");
	if (!result || result->HasError()) {
		return "(unable to enumerate tables)";
	}
	std::ostringstream out;
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		if (i > 0) {
			out << ", ";
		}
		out << result->GetValue(0, i).ToString();
	}
	const auto rendered = out.str();
	return rendered.empty() ? "(no tables)" : rendered;
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
	const auto consumers = MainTable(data.catalog_name, CONSUMERS_TABLE);
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
		details << ",\"tables\":" << tables_literal;
	}
	if (data.change_types_specified) {
		details << ",\"change_types\":" << change_types_literal;
	}
	if (data.event_categories_specified) {
		details << ",\"event_categories\":" << event_categories_literal;
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
	const auto consumers = MainTable(data.catalog_name, CONSUMERS_TABLE);
	try {
		ExecuteChecked(conn, "BEGIN TRANSACTION");
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
		ExecuteChecked(conn, "COMMIT");
		return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id),
		        duckdb::Value::BIGINT(row.last_committed_snapshot), duckdb::Value::BIGINT(resolved_snapshot)};
	} catch (...) {
		try {
			ExecuteChecked(conn, "ROLLBACK");
		} catch (...) {
		}
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
	const auto consumers = MainTable(data.catalog_name, CONSUMERS_TABLE);
	try {
		ExecuteChecked(conn, "BEGIN TRANSACTION");
		auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		const auto details = "{\"last_committed_snapshot\":" + std::to_string(row.last_committed_snapshot) +
		                     ",\"last_committed_schema_version\":" + std::to_string(row.last_committed_schema_version) +
		                     "}";
		InsertAuditRow(conn, data.catalog_name, "consumer_drop", data.consumer_name, row.consumer_id, details);
		ExecuteChecked(conn, "DELETE FROM " + consumers + " WHERE consumer_name = " + QuoteLiteral(data.consumer_name));
		ExecuteChecked(conn, "COMMIT");
		return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id),
		        duckdb::Value::BIGINT(row.last_committed_snapshot)};
	} catch (...) {
		try {
			ExecuteChecked(conn, "ROLLBACK");
		} catch (...) {
		}
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
	const auto consumers = MainTable(data.catalog_name, CONSUMERS_TABLE);
	try {
		ExecuteChecked(conn, "BEGIN TRANSACTION");
		auto row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		const auto details = "{\"previous_token\":" + JsonValue(row.owner_token) +
		                     ",\"previous_acquired_at\":" + JsonValue(row.owner_acquired_at) +
		                     ",\"previous_heartbeat_at\":" + JsonValue(row.owner_heartbeat_at) + "}";
		ExecuteChecked(conn, "UPDATE " + consumers +
		                         " SET owner_token = NULL, owner_acquired_at = NULL, owner_heartbeat_at = NULL, "
		                         "updated_at = now() WHERE consumer_name = " +
		                         QuoteLiteral(data.consumer_name));
		InsertAuditRow(conn, data.catalog_name, "consumer_force_release", data.consumer_name, row.consumer_id, details);
		ExecuteChecked(conn, "COMMIT");
		duckdb::Value previous_token =
		    row.owner_token.IsNull() ? duckdb::Value() : duckdb::Value(row.owner_token.ToString());
		return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(row.consumer_id), previous_token};
	} catch (...) {
		try {
			ExecuteChecked(conn, "ROLLBACK");
		} catch (...) {
		}
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

struct CdcWindowData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t max_snapshots;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcWindowData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->max_snapshots = max_snapshots;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

int64_t MaxSnapshotsParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("max_snapshots");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return DEFAULT_MAX_SNAPSHOTS;
	}
	return entry->second.GetValue<int64_t>();
}

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
	const auto consumers = MainTable(data.catalog_name, CONSUMERS_TABLE);
	const auto cached_token = CachedToken(context, data.catalog_name, data.consumer_name);
	const auto new_token = GenerateUuid(conn);
	const auto cached_token_sql = TokenSqlOrNull(cached_token);
	try {
		ExecuteChecked(conn, "BEGIN TRANSACTION");
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
		                         "epoch(owner_heartbeat_at) > lease_interval_seconds THEN now() ELSE owner_acquired_at "
		                         "END, owner_heartbeat_at = now(), updated_at = now() WHERE consumer_name = " +
		                         QuoteLiteral(data.consumer_name) +
		                         " AND (owner_token IS NULL OR epoch(now()) - epoch(owner_heartbeat_at) > "
		                         "lease_interval_seconds OR owner_token = " +
		                         cached_token_sql + ")");
		row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
		if (row.owner_token.IsNull() || row.owner_token.ToString() != owner_token) {
			ThrowBusy(conn, data.catalog_name, data.consumer_name);
		}
		const auto last_snapshot = row.last_committed_snapshot;
		const auto stop_at_schema_change = row.stop_at_schema_change;
		CacheToken(context, data.catalog_name, data.consumer_name, owner_token);
		EnsureSnapshotExistsOrGap(conn, data.catalog_name, data.consumer_name, last_snapshot);

		const auto current_snapshot = CurrentExternalSnapshot(conn, data.catalog_name, last_snapshot);
		const auto first_external_snapshot =
		    FirstExternalSnapshotAfter(conn, data.catalog_name, last_snapshot, current_snapshot);
		const auto start_snapshot = first_external_snapshot == -1 ? last_snapshot + 1 : first_external_snapshot;
		int64_t end_snapshot = first_external_snapshot == -1
		                           ? last_snapshot
		                           : std::min(current_snapshot, start_snapshot + data.max_snapshots - 1);
		bool schema_changes_pending = false;
		int64_t boundary_next_snapshot = -1;
		int64_t boundary_next_schema_version = -1;
		auto schema_version = last_snapshot <= current_snapshot
		                          ? ResolveSchemaVersion(conn, data.catalog_name, last_snapshot)
		                          : row.last_committed_schema_version;
		if (first_external_snapshot != -1 && start_snapshot <= current_snapshot) {
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
		ExecuteChecked(conn, "COMMIT");
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
		try {
			ExecuteChecked(conn, "ROLLBACK");
		} catch (...) {
		}
		throw;
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcWindowInit(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcWindowData>();
	result->rows.push_back(ReadWindow(context, data));
	return std::move(result);
}

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
	const auto consumers = MainTable(data.catalog_name, CONSUMERS_TABLE);
	const auto cached_token = CachedToken(context, data.catalog_name, data.consumer_name);
	try {
		ExecuteChecked(conn, "BEGIN TRANSACTION");
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
		ExecuteChecked(conn, "COMMIT");
		return {duckdb::Value(data.consumer_name), duckdb::Value::BIGINT(data.snapshot_id),
		        duckdb::Value::BIGINT(schema_version)};
	} catch (...) {
		try {
			ExecuteChecked(conn, "ROLLBACK");
		} catch (...) {
		}
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
	const auto consumers = MainTable(data.catalog_name, CONSUMERS_TABLE);
	const auto cached_token = CachedToken(context, data.catalog_name, data.consumer_name);
	try {
		ExecuteChecked(conn, "BEGIN TRANSACTION");
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
		ExecuteChecked(conn, "COMMIT");
		return {duckdb::Value::BOOLEAN(true)};
	} catch (...) {
		try {
			ExecuteChecked(conn, "ROLLBACK");
		} catch (...) {
		}
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
		const auto current_snapshot = CurrentExternalSnapshot(conn, data.catalog_name, row.last_committed_snapshot);
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

struct DdlObject {
	duckdb::Value schema_id;
	duckdb::Value schema_name;
	duckdb::Value object_id;
	duckdb::Value object_name;
};

std::vector<std::string> SplitChangeTokens(const std::string &changes_made) {
	std::vector<std::string> tokens;
	size_t start = 0;
	while (start < changes_made.size()) {
		auto comma = changes_made.find(',', start);
		tokens.push_back(changes_made.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
		if (comma == std::string::npos) {
			break;
		}
		start = comma + 1;
	}
	return tokens;
}

bool ParseIdToken(const std::string &token, const std::string &prefix, int64_t &id) {
	if (!StartsWith(token, prefix)) {
		return false;
	}
	return TryParseInt64(token.substr(prefix.size()), id);
}

std::string UnquoteChangeName(const std::string &quoted) {
	if (quoted.size() >= 2 && quoted.front() == '"' && quoted.back() == '"') {
		return quoted.substr(1, quoted.size() - 2);
	}
	return quoted;
}

bool ParseQualifiedNameToken(const std::string &token, const std::string &prefix, std::string &schema_name,
                             std::string &object_name) {
	if (!StartsWith(token, prefix)) {
		return false;
	}
	const auto value = token.substr(prefix.size());
	const auto dot = value.find("\".\"");
	if (dot == std::string::npos) {
		return false;
	}
	schema_name = UnquoteChangeName(value.substr(0, dot + 1));
	object_name = UnquoteChangeName(value.substr(dot + 2));
	return true;
}

bool ParseNameToken(const std::string &token, const std::string &prefix, std::string &name) {
	if (!StartsWith(token, prefix)) {
		return false;
	}
	name = UnquoteChangeName(token.substr(prefix.size()));
	return true;
}

//! Translate a `changes_made` token to the fully-qualified `schema.table`
//! name as of `snapshot_id`. Returns the empty string for tokens that do
//! not name a single user table (schema-level DDL, view changes, anything
//! we can't or shouldn't resolve to a table). Used by `cdc_events` to
//! filter snapshots against the consumer's `tables` filter.
std::string TableQualifiedNameForToken(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                                       const std::string &token) {
	std::string schema_name;
	std::string object_name;
	if (ParseQualifiedNameToken(token, "created_table:", schema_name, object_name)) {
		return schema_name + "." + object_name;
	}
	if (StartsWith(token, "created_view:") || StartsWith(token, "altered_view:") ||
	    StartsWith(token, "dropped_view:") || StartsWith(token, "created_schema:") ||
	    StartsWith(token, "dropped_schema:")) {
		return std::string();
	}
	const std::vector<std::string> id_prefixes = {
	    "inserted_into_table:", "deleted_from_table:", "tables_inserted_into:", "tables_deleted_from:",
	    "inlined_insert:",      "inlined_delete:",     "altered_table:",        "dropped_table:",
	    "compacted_table:",     "merge_adjacent:"};
	int64_t table_id = 0;
	for (const auto &prefix : id_prefixes) {
		if (ParseIdToken(token, prefix, table_id)) {
			auto result = conn.Query(
			    "SELECT s.schema_name, t.table_name FROM " + MetadataTable(catalog_name, "ducklake_table") +
			    " t JOIN " + MetadataTable(catalog_name, "ducklake_schema") +
			    " s USING (schema_id) WHERE t.table_id = " + std::to_string(table_id) +
			    " AND t.begin_snapshot <= " + std::to_string(snapshot_id) +
			    " AND (t.end_snapshot IS NULL OR t.end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
			if (result && !result->HasError() && result->RowCount() > 0 && !result->GetValue(0, 0).IsNull() &&
			    !result->GetValue(1, 0).IsNull()) {
				return result->GetValue(0, 0).ToString() + "." + result->GetValue(1, 0).ToString();
			}
			return std::string();
		}
	}
	return std::string();
}

//! True iff `changes_made` references at least one of the consumer's filter
//! tables. Returns true unconditionally when the consumer has no filter.
bool ChangesTouchConsumerTables(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                                const std::string &changes_made, const std::unordered_set<std::string> &filter_tables) {
	if (filter_tables.empty()) {
		return true;
	}
	for (const auto &token : SplitChangeTokens(changes_made)) {
		const auto qualified = TableQualifiedNameForToken(conn, catalog_name, snapshot_id, token);
		if (!qualified.empty() && filter_tables.find(qualified) != filter_tables.end()) {
			return true;
		}
	}
	return false;
}

duckdb::Value SnapshotTime(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id) {
	auto result = conn.Query("SELECT snapshot_time FROM " + MetadataTable(catalog_name, "ducklake_snapshot") +
	                         " WHERE snapshot_id = " + std::to_string(snapshot_id));
	if (!result || result->HasError() || result->RowCount() == 0) {
		return duckdb::Value();
	}
	return result->GetValue(0, 0);
}

DdlObject LookupSchemaByName(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                             const std::string &schema_name) {
	auto result = conn.Query("SELECT schema_id, schema_name FROM " + MetadataTable(catalog_name, "ducklake_schema") +
	                         " WHERE begin_snapshot = " + std::to_string(snapshot_id) +
	                         " AND schema_name = " + QuoteLiteral(schema_name) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(schema_name), duckdb::Value(), duckdb::Value(schema_name)};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(0, 0), result->GetValue(1, 0)};
}

DdlObject LookupSchemaById(duckdb::Connection &conn, const std::string &catalog_name, int64_t schema_id) {
	auto result = conn.Query("SELECT schema_id, schema_name FROM " + MetadataTable(catalog_name, "ducklake_schema") +
	                         " WHERE schema_id = " + std::to_string(schema_id) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value::BIGINT(schema_id), duckdb::Value(), duckdb::Value::BIGINT(schema_id), duckdb::Value()};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(0, 0), result->GetValue(1, 0)};
}

DdlObject LookupTableByName(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                            const std::string &schema_name, const std::string &table_name) {
	auto result = conn.Query("SELECT s.schema_id, s.schema_name, t.table_id, t.table_name FROM " +
	                         MetadataTable(catalog_name, "ducklake_table") + " t JOIN " +
	                         MetadataTable(catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE t.begin_snapshot = " + std::to_string(snapshot_id) +
	                         " AND s.schema_name = " + QuoteLiteral(schema_name) +
	                         " AND t.table_name = " + QuoteLiteral(table_name) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(schema_name), duckdb::Value(), duckdb::Value(table_name)};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(2, 0), result->GetValue(3, 0)};
}

DdlObject LookupTableById(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id) {
	auto result = conn.Query("SELECT s.schema_id, s.schema_name, t.table_id, t.table_name FROM " +
	                         MetadataTable(catalog_name, "ducklake_table") + " t JOIN " +
	                         MetadataTable(catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE t.table_id = " + std::to_string(table_id) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(), duckdb::Value::BIGINT(table_id), duckdb::Value()};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(2, 0), result->GetValue(3, 0)};
}

DdlObject LookupViewByName(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                           const std::string &schema_name, const std::string &view_name) {
	auto result = conn.Query("SELECT s.schema_id, s.schema_name, v.view_id, v.view_name FROM " +
	                         MetadataTable(catalog_name, "ducklake_view") + " v JOIN " +
	                         MetadataTable(catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE v.begin_snapshot = " + std::to_string(snapshot_id) +
	                         " AND s.schema_name = " + QuoteLiteral(schema_name) +
	                         " AND v.view_name = " + QuoteLiteral(view_name) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(schema_name), duckdb::Value(), duckdb::Value(view_name)};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(2, 0), result->GetValue(3, 0)};
}

DdlObject LookupViewById(duckdb::Connection &conn, const std::string &catalog_name, int64_t view_id) {
	auto result = conn.Query("SELECT s.schema_id, s.schema_name, v.view_id, v.view_name FROM " +
	                         MetadataTable(catalog_name, "ducklake_view") + " v JOIN " +
	                         MetadataTable(catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE v.view_id = " + std::to_string(view_id) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return {duckdb::Value(), duckdb::Value(), duckdb::Value::BIGINT(view_id), duckdb::Value()};
	}
	return {result->GetValue(0, 0), result->GetValue(1, 0), result->GetValue(2, 0), result->GetValue(3, 0)};
}

std::vector<duckdb::Value> DdlRow(int64_t snapshot_id, const duckdb::Value &snapshot_time,
                                  const std::string &event_kind, const std::string &object_kind,
                                  const DdlObject &object, const std::string &details_json = "{}") {
	return {duckdb::Value::BIGINT(snapshot_id),
	        snapshot_time,
	        duckdb::Value(event_kind),
	        duckdb::Value(object_kind),
	        object.schema_id,
	        object.schema_name,
	        object.object_id,
	        object.object_name,
	        duckdb::Value(details_json)};
}

//! Snapshot of a single column at a specific snapshot.
struct ColumnInfo {
	int64_t column_id;
	int64_t column_order;
	std::string column_name;
	std::string column_type;
	bool nullable;
	duckdb::Value initial_default;
	duckdb::Value default_value;
	duckdb::Value parent_column;
};

//! Difference between two ColumnInfo lists (one at snapshot S-1 vs S).
//! `parent_column_id` is the struct-parent's column_id; valid only when
//! `has_parent` is true (i.e. the diffed column is a nested-struct field
//! per `ducklake_column.parent_column`). Phase 2 deferral 3: the field
//! is populated for ADDED and DROPPED kinds so downstream consumers can
//! reconstruct the parent/child relationship without re-querying the
//! catalog.
struct ColumnDiff {
	enum class Kind { ADDED, DROPPED, RENAMED, TYPE_CHANGED, DEFAULT_CHANGED, NULLABLE_CHANGED };
	Kind kind;
	int64_t column_id;
	std::string old_name;
	std::string new_name;
	std::string old_type;
	std::string new_type;
	duckdb::Value old_default;
	duckdb::Value new_default;
	bool old_nullable;
	bool new_nullable;
	int64_t parent_column_id = 0;
	bool has_parent = false;
};

//! Read every column of `table_id` that is live at `snapshot_id` (i.e.
//! `begin_snapshot <= snapshot_id` and `end_snapshot IS NULL OR
//! end_snapshot >= snapshot_id`). Returned in column_order so the
//! per-table column list is stable for diffs.
std::vector<ColumnInfo> LookupTableColumnsAt(duckdb::Connection &conn, const std::string &catalog_name,
                                             int64_t table_id, int64_t snapshot_id) {
	std::vector<ColumnInfo> columns;
	// DuckLake's `end_snapshot` is *exclusive*: it stamps the snapshot at
	// which the row becomes invisible. So "alive at snap N" means
	// `begin <= N AND (end IS NULL OR end > N)`. SELECT ... AT (VERSION =>
	// dropped_snap) returns the post-drop column set, which matches.
	auto result = conn.Query(
	    "SELECT column_id, column_order, column_name, column_type, nulls_allowed, "
	    "initial_default, default_value, parent_column FROM " +
	    MetadataTable(catalog_name, "ducklake_column") + " WHERE table_id = " + std::to_string(table_id) +
	    " AND begin_snapshot <= " + std::to_string(snapshot_id) + " AND (end_snapshot IS NULL OR end_snapshot > " +
	    std::to_string(snapshot_id) + ")" + " ORDER BY column_order ASC");
	if (!result || result->HasError()) {
		return columns;
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		ColumnInfo column;
		const auto col_id_value = result->GetValue(0, i);
		const auto col_order_value = result->GetValue(1, i);
		const auto col_name_value = result->GetValue(2, i);
		const auto col_type_value = result->GetValue(3, i);
		const auto nulls_allowed_value = result->GetValue(4, i);
		if (col_id_value.IsNull() || col_name_value.IsNull() || col_type_value.IsNull()) {
			continue;
		}
		column.column_id = col_id_value.GetValue<int64_t>();
		column.column_order = col_order_value.IsNull() ? 0 : col_order_value.GetValue<int64_t>();
		column.column_name = col_name_value.ToString();
		column.column_type = col_type_value.ToString();
		column.nullable = nulls_allowed_value.IsNull() ? true : nulls_allowed_value.GetValue<bool>();
		column.initial_default = result->GetValue(5, i);
		column.default_value = result->GetValue(6, i);
		column.parent_column = result->GetValue(7, i);
		columns.push_back(std::move(column));
	}
	return columns;
}

//! Compare two ColumnInfo lists and emit per-column diffs. The diff rule
//! lives entirely on column_id (DuckLake preserves the id across renames /
//! type changes / default changes / nullable flips) — names alone are not
//! enough because two distinct rename+add operations could produce the
//! same name set with different identities.
std::vector<ColumnDiff> DiffColumns(const std::vector<ColumnInfo> &old_columns,
                                    const std::vector<ColumnInfo> &new_columns) {
	std::vector<ColumnDiff> diffs;
	std::unordered_map<int64_t, const ColumnInfo *> by_id_old;
	std::unordered_map<int64_t, const ColumnInfo *> by_id_new;
	for (const auto &c : old_columns) {
		by_id_old[c.column_id] = &c;
	}
	for (const auto &c : new_columns) {
		by_id_new[c.column_id] = &c;
	}
	for (const auto &c : new_columns) {
		auto it = by_id_old.find(c.column_id);
		if (it == by_id_old.end()) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::ADDED;
			d.column_id = c.column_id;
			d.new_name = c.column_name;
			d.new_type = c.column_type;
			d.new_default = c.default_value;
			d.new_nullable = c.nullable;
			if (!c.parent_column.IsNull()) {
				d.parent_column_id = c.parent_column.GetValue<int64_t>();
				d.has_parent = true;
			}
			diffs.push_back(std::move(d));
			continue;
		}
		const auto &old_col = *it->second;
		if (old_col.column_name != c.column_name) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::RENAMED;
			d.column_id = c.column_id;
			d.old_name = old_col.column_name;
			d.new_name = c.column_name;
			diffs.push_back(std::move(d));
		}
		if (old_col.column_type != c.column_type) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::TYPE_CHANGED;
			d.column_id = c.column_id;
			d.old_name = old_col.column_name;
			d.new_name = c.column_name;
			d.old_type = old_col.column_type;
			d.new_type = c.column_type;
			diffs.push_back(std::move(d));
		}
		const auto old_default_str = old_col.default_value.IsNull() ? std::string() : old_col.default_value.ToString();
		const auto new_default_str = c.default_value.IsNull() ? std::string() : c.default_value.ToString();
		const bool old_default_null = old_col.default_value.IsNull();
		const bool new_default_null = c.default_value.IsNull();
		if (old_default_null != new_default_null || old_default_str != new_default_str) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::DEFAULT_CHANGED;
			d.column_id = c.column_id;
			d.old_name = old_col.column_name;
			d.new_name = c.column_name;
			d.old_default = old_col.default_value;
			d.new_default = c.default_value;
			diffs.push_back(std::move(d));
		}
		if (old_col.nullable != c.nullable) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::NULLABLE_CHANGED;
			d.column_id = c.column_id;
			d.old_name = old_col.column_name;
			d.new_name = c.column_name;
			d.old_nullable = old_col.nullable;
			d.new_nullable = c.nullable;
			diffs.push_back(std::move(d));
		}
	}
	for (const auto &c : old_columns) {
		if (by_id_new.find(c.column_id) == by_id_new.end()) {
			ColumnDiff d;
			d.kind = ColumnDiff::Kind::DROPPED;
			d.column_id = c.column_id;
			d.old_name = c.column_name;
			d.old_type = c.column_type;
			d.old_default = c.default_value;
			d.old_nullable = c.nullable;
			if (!c.parent_column.IsNull()) {
				d.parent_column_id = c.parent_column.GetValue<int64_t>();
				d.has_parent = true;
			}
			diffs.push_back(std::move(d));
		}
	}
	return diffs;
}

//! Phase 2 deferral 3: stable-sort ADDED diffs so a struct parent appears
//! before any of its nested-field children, and DROPPED diffs so children
//! appear before their parent. Mirrors the cross-snapshot ordering rule
//! cdc_ddl already uses for `(event_kind, object_kind)` (ADR 0008): a
//! consumer building / tearing down typed schemas observes the parent
//! struct exists before its fields are added, and observes a struct's
//! fields disappear before the struct itself is dropped.
//!
//! The sort is *stable* so the natural column_order ordering is preserved
//! among siblings; we only reorder when a parent/child pair is out of
//! position. Within ADDED, the predicate is "parents before children"
//! (top-level columns get rank 0; child columns get rank = depth from
//! root); within DROPPED, the rank is negated so the deepest leaves go
//! first.
void NormaliseDiffParentChildOrdering(std::vector<ColumnDiff> &diffs) {
	std::unordered_map<int64_t, int64_t> parent_of;
	for (const auto &d : diffs) {
		if ((d.kind == ColumnDiff::Kind::ADDED || d.kind == ColumnDiff::Kind::DROPPED) && d.has_parent) {
			parent_of[d.column_id] = d.parent_column_id;
		}
	}
	auto depth_of = [&](int64_t column_id) {
		int depth = 0;
		auto cursor = column_id;
		while (true) {
			auto it = parent_of.find(cursor);
			if (it == parent_of.end()) {
				return depth;
			}
			++depth;
			cursor = it->second;
			if (depth > 64) {
				// Defensive: cycles in parent_column would be a DuckLake
				// catalog corruption; cap traversal so we don't spin.
				return depth;
			}
		}
	};
	std::stable_sort(diffs.begin(), diffs.end(), [&](const ColumnDiff &a, const ColumnDiff &b) {
		if (a.kind != b.kind) {
			// Preserve grouping by kind — the JSON serializer sweeps each
			// kind independently anyway, but a stable order across kinds
			// keeps debug dumps deterministic.
			return false;
		}
		if (a.kind == ColumnDiff::Kind::ADDED) {
			return depth_of(a.column_id) < depth_of(b.column_id);
		}
		if (a.kind == ColumnDiff::Kind::DROPPED) {
			return depth_of(a.column_id) > depth_of(b.column_id);
		}
		return false;
	});
}

const char *DiffKindToString(ColumnDiff::Kind kind) {
	switch (kind) {
	case ColumnDiff::Kind::ADDED:
		return "added";
	case ColumnDiff::Kind::DROPPED:
		return "dropped";
	case ColumnDiff::Kind::RENAMED:
		return "renamed";
	case ColumnDiff::Kind::TYPE_CHANGED:
		return "type_changed";
	case ColumnDiff::Kind::DEFAULT_CHANGED:
		return "default_changed";
	case ColumnDiff::Kind::NULLABLE_CHANGED:
		return "nullable_changed";
	}
	return "unknown";
}

//! Serialize a ColumnInfo as a JSON object:
//! `{"id":N,"order":N,"name":"...","type":"...","nullable":bool,"default":"..."|null}`.
std::string ColumnInfoToJson(const ColumnInfo &c) {
	std::ostringstream out;
	out << "{\"id\":" << c.column_id << ",\"order\":" << c.column_order << ",\"name\":\"" << JsonEscape(c.column_name)
	    << "\",\"type\":\"" << JsonEscape(c.column_type) << "\",\"nullable\":" << (c.nullable ? "true" : "false")
	    << ",\"default\":";
	if (c.default_value.IsNull()) {
		out << "null";
	} else {
		out << "\"" << JsonEscape(c.default_value.ToString()) << "\"";
	}
	if (!c.parent_column.IsNull()) {
		// Field name aligned with ADR 0008 / Phase 2 deferral 3: the
		// nested-struct parent reference is `parent_column_id` (the
		// referenced column_id is itself a column_id, not the column's
		// name). Phase 1 emitted this as `parent_column` which conflicted
		// with the ADR text and with the cdc_schema_diff column name.
		out << ",\"parent_column_id\":" << c.parent_column.ToString();
	}
	out << "}";
	return out.str();
}

std::string JsonOptionalString(const duckdb::Value &v) {
	if (v.IsNull()) {
		return "null";
	}
	return std::string("\"") + JsonEscape(v.ToString()) + "\"";
}

//! Serialize the diff list as a JSON object grouped by diff kind. Empty
//! groups are omitted so the payload stays compact for the common case
//! (most ALTER TABLE commits change exactly one column in exactly one
//! way). Phase 2 deferral 3: ADDED / DROPPED entries gain a
//! `parent_column_id` field for nested-struct children so consumers can
//! reconstruct the parent/child relationship without re-querying the
//! catalog; the diff list is also normalised so parents precede children
//! on ADDED and children precede parents on DROPPED.
std::string DiffsToJson(std::vector<ColumnDiff> diffs) {
	NormaliseDiffParentChildOrdering(diffs);
	auto kind_filter = [&diffs](ColumnDiff::Kind kind, std::ostringstream &out) {
		bool first = true;
		for (const auto &d : diffs) {
			if (d.kind != kind) {
				continue;
			}
			if (!first) {
				out << ",";
			}
			first = false;
			out << "{\"id\":" << d.column_id;
			switch (kind) {
			case ColumnDiff::Kind::ADDED:
				out << ",\"name\":\"" << JsonEscape(d.new_name) << "\",\"type\":\"" << JsonEscape(d.new_type)
				    << "\",\"nullable\":" << (d.new_nullable ? "true" : "false")
				    << ",\"default\":" << JsonOptionalString(d.new_default);
				if (d.has_parent) {
					out << ",\"parent_column_id\":" << d.parent_column_id;
				}
				break;
			case ColumnDiff::Kind::DROPPED:
				out << ",\"name\":\"" << JsonEscape(d.old_name) << "\",\"type\":\"" << JsonEscape(d.old_type)
				    << "\",\"nullable\":" << (d.old_nullable ? "true" : "false")
				    << ",\"default\":" << JsonOptionalString(d.old_default);
				if (d.has_parent) {
					out << ",\"parent_column_id\":" << d.parent_column_id;
				}
				break;
			case ColumnDiff::Kind::RENAMED:
				out << ",\"old_name\":\"" << JsonEscape(d.old_name) << "\",\"new_name\":\"" << JsonEscape(d.new_name)
				    << "\"";
				break;
			case ColumnDiff::Kind::TYPE_CHANGED:
				out << ",\"name\":\"" << JsonEscape(d.new_name) << "\",\"old_type\":\"" << JsonEscape(d.old_type)
				    << "\",\"new_type\":\"" << JsonEscape(d.new_type) << "\"";
				break;
			case ColumnDiff::Kind::DEFAULT_CHANGED:
				out << ",\"name\":\"" << JsonEscape(d.new_name)
				    << "\",\"old_default\":" << JsonOptionalString(d.old_default)
				    << ",\"new_default\":" << JsonOptionalString(d.new_default);
				break;
			case ColumnDiff::Kind::NULLABLE_CHANGED:
				out << ",\"name\":\"" << JsonEscape(d.new_name)
				    << "\",\"old_nullable\":" << (d.old_nullable ? "true" : "false")
				    << ",\"new_nullable\":" << (d.new_nullable ? "true" : "false");
				break;
			}
			out << "}";
		}
	};
	std::ostringstream out;
	const char *separator = "";
	const std::vector<std::pair<ColumnDiff::Kind, const char *>> kinds = {
	    {ColumnDiff::Kind::ADDED, "added"},
	    {ColumnDiff::Kind::DROPPED, "dropped"},
	    {ColumnDiff::Kind::RENAMED, "renamed"},
	    {ColumnDiff::Kind::TYPE_CHANGED, "type_changed"},
	    {ColumnDiff::Kind::DEFAULT_CHANGED, "default_changed"},
	    {ColumnDiff::Kind::NULLABLE_CHANGED, "nullable_changed"}};
	for (const auto &kind_pair : kinds) {
		bool any = false;
		for (const auto &d : diffs) {
			if (d.kind == kind_pair.first) {
				any = true;
				break;
			}
		}
		if (!any) {
			continue;
		}
		out << separator << "\"" << kind_pair.second << "\":[";
		kind_filter(kind_pair.first, out);
		out << "]";
		separator = ",";
	}
	return out.str();
}

//! Find the most-recent table_name DuckLake had for `table_id` strictly
//! before `snapshot_id`. Used for Finding-1 rename detection: if a
//! `created_table:"X"."Y"` token at snapshot S maps to a table_id whose
//! immediately-prior name is different, we surface the event as
//! `altered.table` with a rename payload rather than as `created.table`.
duckdb::Value PreviousTableName(duckdb::Connection &conn, const std::string &catalog_name, int64_t table_id,
                                int64_t snapshot_id) {
	auto result = conn.Query("SELECT table_name FROM " + MetadataTable(catalog_name, "ducklake_table") +
	                         " WHERE table_id = " + std::to_string(table_id) + " AND begin_snapshot < " +
	                         std::to_string(snapshot_id) + " ORDER BY begin_snapshot DESC LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return duckdb::Value();
	}
	return result->GetValue(0, 0);
}

//! Read view metadata (definition + dialect + column aliases) at the
//! snapshot the view first appears in.
struct ViewInfo {
	std::string sql;
	std::string dialect;
	duckdb::Value column_aliases;
};

ViewInfo LookupViewMetadata(duckdb::Connection &conn, const std::string &catalog_name, int64_t view_id,
                            int64_t snapshot_id) {
	ViewInfo info;
	auto result = conn.Query(
	    "SELECT sql, dialect, column_aliases FROM " + MetadataTable(catalog_name, "ducklake_view") +
	    " WHERE view_id = " + std::to_string(view_id) + " AND begin_snapshot <= " + std::to_string(snapshot_id) +
	    " AND (end_snapshot IS NULL OR end_snapshot > " + std::to_string(snapshot_id) + ") LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return info;
	}
	const auto sql_value = result->GetValue(0, 0);
	const auto dialect_value = result->GetValue(1, 0);
	info.sql = sql_value.IsNull() ? std::string() : sql_value.ToString();
	info.dialect = dialect_value.IsNull() ? std::string() : dialect_value.ToString();
	info.column_aliases = result->GetValue(2, 0);
	return info;
}

//! Build the typed `details` JSON payload for a `created.table` event.
//! Payload shape: `{"columns":[{...ColumnInfoToJson...}, ...]}`.
std::string BuildCreatedTableDetails(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                                     int64_t table_id) {
	const auto columns = LookupTableColumnsAt(conn, catalog_name, table_id, snapshot_id);
	std::ostringstream out;
	out << "{\"columns\":[";
	for (size_t i = 0; i < columns.size(); ++i) {
		if (i > 0) {
			out << ",";
		}
		out << ColumnInfoToJson(columns[i]);
	}
	out << "]}";
	return out.str();
}

//! Build the typed `details` JSON payload for an `altered.table` event.
//! When `old_table_name` is non-empty (Finding-1 rename), the rename pair
//! is included alongside the column-level diff. The diff itself uses
//! `LookupTableColumnsAt(snapshot_id - 1)` vs `LookupTableColumnsAt(snapshot_id)`
//! to pick up everything DuckLake committed at this snapshot.
std::string BuildAlteredTableDetails(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                                     int64_t table_id, const std::string &old_table_name,
                                     const std::string &new_table_name) {
	const auto old_columns = LookupTableColumnsAt(conn, catalog_name, table_id, snapshot_id - 1);
	const auto new_columns = LookupTableColumnsAt(conn, catalog_name, table_id, snapshot_id);
	const auto diffs = DiffColumns(old_columns, new_columns);
	std::ostringstream out;
	out << "{";
	const char *separator = "";
	if (!old_table_name.empty() && old_table_name != new_table_name) {
		out << separator << "\"old_table_name\":\"" << JsonEscape(old_table_name) << "\",\"new_table_name\":\""
		    << JsonEscape(new_table_name) << "\"";
		separator = ",";
	}
	const auto diffs_json = DiffsToJson(diffs);
	if (!diffs_json.empty()) {
		out << separator << diffs_json;
	}
	out << "}";
	return out.str();
}

//! Build the typed `details` JSON payload for a `created.view` event.
//! Payload shape: `{"definition":"...","dialect":"...","column_aliases":"..."|null}`.
std::string BuildCreatedViewDetails(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                                    int64_t view_id) {
	const auto info = LookupViewMetadata(conn, catalog_name, view_id, snapshot_id);
	std::ostringstream out;
	out << "{\"definition\":\"" << JsonEscape(info.sql) << "\",\"dialect\":\"" << JsonEscape(info.dialect)
	    << "\",\"column_aliases\":" << JsonOptionalString(info.column_aliases) << "}";
	return out.str();
}

void AddDdlRowsForToken(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                        const duckdb::Value &snapshot_time, const std::string &token,
                        std::vector<std::vector<duckdb::Value>> &rows) {
	std::string schema_name;
	std::string object_name;
	int64_t id = 0;
	if (ParseNameToken(token, "created_schema:", object_name)) {
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "created", "schema",
		                      LookupSchemaByName(conn, catalog_name, snapshot_id, object_name)));
	} else if (ParseQualifiedNameToken(token, "created_table:", schema_name, object_name)) {
		// Finding-1 (ADR 0008): a `created_table:"X"."Y"` whose table_id
		// already had a different name in the previous snapshot is a
		// RENAME, not a creation. Emit `altered.table` with a rename
		// payload in that case so downstream consumers see the rename
		// explicitly rather than as a paired drop + create that would
		// confuse identity.
		const auto object = LookupTableByName(conn, catalog_name, snapshot_id, schema_name, object_name);
		if (!object.object_id.IsNull()) {
			const auto table_id = object.object_id.GetValue<int64_t>();
			const auto previous_name = PreviousTableName(conn, catalog_name, table_id, snapshot_id);
			if (!previous_name.IsNull() && previous_name.ToString() != object_name) {
				const auto details = BuildAlteredTableDetails(conn, catalog_name, snapshot_id, table_id,
				                                              previous_name.ToString(), object_name);
				rows.push_back(DdlRow(snapshot_id, snapshot_time, "altered", "table", object, details));
			} else {
				const auto details = BuildCreatedTableDetails(conn, catalog_name, snapshot_id, table_id);
				rows.push_back(DdlRow(snapshot_id, snapshot_time, "created", "table", object, details));
			}
		} else {
			rows.push_back(DdlRow(snapshot_id, snapshot_time, "created", "table", object));
		}
	} else if (ParseIdToken(token, "altered_table:", id)) {
		const auto object = LookupTableById(conn, catalog_name, id);
		const auto details =
		    BuildAlteredTableDetails(conn, catalog_name, snapshot_id, id, std::string(),
		                             object.object_name.IsNull() ? std::string() : object.object_name.ToString());
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "altered", "table", object, details));
	} else if (ParseQualifiedNameToken(token, "created_view:", schema_name, object_name)) {
		const auto object = LookupViewByName(conn, catalog_name, snapshot_id, schema_name, object_name);
		std::string details = "{}";
		if (!object.object_id.IsNull()) {
			details = BuildCreatedViewDetails(conn, catalog_name, snapshot_id, object.object_id.GetValue<int64_t>());
		}
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "created", "view", object, details));
	} else if (ParseIdToken(token, "dropped_schema:", id)) {
		rows.push_back(
		    DdlRow(snapshot_id, snapshot_time, "dropped", "schema", LookupSchemaById(conn, catalog_name, id)));
	} else if (ParseIdToken(token, "dropped_table:", id)) {
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "dropped", "table", LookupTableById(conn, catalog_name, id)));
	} else if (ParseIdToken(token, "dropped_view:", id)) {
		rows.push_back(DdlRow(snapshot_id, snapshot_time, "dropped", "view", LookupViewById(conn, catalog_name, id)));
	}
}

struct CdcDdlData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t max_snapshots;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcDdlData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->max_snapshots = max_snapshots;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcDdlBind(duckdb::ClientContext &context,
                                                    duckdb::TableFunctionBindInput &input,
                                                    duckdb::vector<duckdb::LogicalType> &return_types,
                                                    duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_ddl requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<CdcDdlData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->max_snapshots = MaxSnapshotsParameter(input);

	names = {"snapshot_id", "snapshot_time", "event_kind",  "object_kind", "schema_id",
	         "schema_name", "object_id",     "object_name", "details"};
	return_types = {duckdb::LogicalType::BIGINT,  duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,       duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

//! Scan `ducklake_snapshot_changes` over `[start_snapshot, end_snapshot]`,
//! reconstruct typed DDL rows for every recognised token, and append them
//! to `rows`. When `table_filter` is non-empty, only rows whose
//! `schema.table` matches are kept (table-scoped DDL only — schema-level
//! events have an empty `schema.table` identity and are dropped). Used by
//! both `cdc_ddl` and the stateless `cdc_recent_ddl` sugar.
//! Per-snapshot DDL ordering rank per ADR 0008. Within one snapshot the
//! contract is `(object_kind, object_id)` ascending, with `object_kind`
//! sort order `schema, view, table` for `created`/`altered` so dependents
//! land after their parents, and the reverse order for `dropped` so
//! tables and views are dropped before their containing schemas. Within
//! the same `(event_kind, object_kind)` we tie-break on `object_id`.
int DdlObjectKindRank(const std::string &event_kind, const std::string &object_kind) {
	const bool reversed = event_kind == "dropped";
	if (object_kind == "schema") {
		return reversed ? 2 : 0;
	}
	if (object_kind == "view") {
		return 1;
	}
	if (object_kind == "table") {
		return reversed ? 0 : 2;
	}
	return 3;
}

//! Stable sort the rows in `[begin, end)` (one snapshot's worth of DDL
//! events) by the ADR 0008 per-snapshot ordering contract. The DdlRow
//! layout is `[snapshot_id, snapshot_time, event_kind, object_kind,
//! schema_id, schema_name, object_id, object_name, details]`.
void SortDdlRowsForSnapshot(std::vector<std::vector<duckdb::Value>> &rows, size_t begin, size_t end) {
	if (end - begin < 2) {
		return;
	}
	std::stable_sort(rows.begin() + static_cast<std::ptrdiff_t>(begin), rows.begin() + static_cast<std::ptrdiff_t>(end),
	                 [](const std::vector<duckdb::Value> &a, const std::vector<duckdb::Value> &b) {
		                 const auto a_event = a[2].IsNull() ? std::string() : a[2].ToString();
		                 const auto b_event = b[2].IsNull() ? std::string() : b[2].ToString();
		                 const auto a_kind = a[3].IsNull() ? std::string() : a[3].ToString();
		                 const auto b_kind = b[3].IsNull() ? std::string() : b[3].ToString();
		                 const auto a_rank = DdlObjectKindRank(a_event, a_kind);
		                 const auto b_rank = DdlObjectKindRank(b_event, b_kind);
		                 if (a_rank != b_rank) {
			                 return a_rank < b_rank;
		                 }
		                 const auto a_id = a[6].IsNull() ? INT64_MAX : a[6].GetValue<int64_t>();
		                 const auto b_id = b[6].IsNull() ? INT64_MAX : b[6].GetValue<int64_t>();
		                 return a_id < b_id;
	                 });
}

void ExtractDdlRows(duckdb::Connection &conn, const std::string &catalog_name, int64_t start_snapshot,
                    int64_t end_snapshot, const std::string &table_filter,
                    std::vector<std::vector<duckdb::Value>> &rows) {
	auto changes =
	    conn.Query("SELECT snapshot_id, changes_made FROM " + MetadataTable(catalog_name, "ducklake_snapshot_changes") +
	               " WHERE snapshot_id BETWEEN " + std::to_string(start_snapshot) + " AND " +
	               std::to_string(end_snapshot) + " ORDER BY snapshot_id ASC");
	if (!changes || changes->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, changes ? changes->GetError() : "DDL scan failed");
	}
	for (duckdb::idx_t row_idx = 0; row_idx < changes->RowCount(); ++row_idx) {
		const auto snapshot_id = changes->GetValue(0, row_idx).GetValue<int64_t>();
		const auto snapshot_time = SnapshotTime(conn, catalog_name, snapshot_id);
		auto before_size = rows.size();
		for (const auto &token : SplitChangeTokens(changes->GetValue(1, row_idx).ToString())) {
			AddDdlRowsForToken(conn, catalog_name, snapshot_id, snapshot_time, token, rows);
		}
		if (!table_filter.empty()) {
			// Keep only rows whose schema.object_name matches; everything
			// emitted in this snapshot lives between [before_size, rows.size())
			// in the result so we can do an in-place rewrite cheaply.
			auto out_idx = before_size;
			for (auto i = before_size; i < rows.size(); ++i) {
				const auto &row = rows[i];
				if (row.size() < 8) {
					continue;
				}
				const auto schema_value = row[5];
				const auto name_value = row[7];
				if (schema_value.IsNull() || name_value.IsNull()) {
					continue;
				}
				const auto qualified = schema_value.ToString() + "." + name_value.ToString();
				if (qualified == table_filter) {
					if (out_idx != i) {
						rows[out_idx] = rows[i];
					}
					out_idx++;
				}
			}
			rows.resize(out_idx);
		}
		SortDdlRowsForSnapshot(rows, before_size, rows.size());
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcDdlInit(duckdb::ClientContext &context,
                                                                duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcDdlData>();

	CdcWindowData window_data;
	window_data.catalog_name = data.catalog_name;
	window_data.consumer_name = data.consumer_name;
	window_data.max_snapshots = data.max_snapshots;
	auto window = ReadWindow(context, window_data);

	// `cdc_window` already ran `CheckCatalogOrThrow` and acquired the
	// lease, so the consumer is loaded safely now. Honor the consumer's
	// `event_categories` filter: a DDL-only consumer (`['ddl']`) is the
	// common case, but a DML-only consumer (`['dml']`) must see zero
	// DDL rows. The cursor still advanced via cdc_window above; the
	// filter is a *post hoc* projection, not a window-bounding
	// decision.
	duckdb::Connection conn_filter(*context.db);
	const auto consumer_row = LoadConsumerOrThrow(conn_filter, data.catalog_name, data.consumer_name);
	bool ddl_enabled = true;
	if (!consumer_row.event_categories.IsNull()) {
		ddl_enabled = false;
		for (const auto &child : duckdb::ListValue::GetChildren(consumer_row.event_categories)) {
			if (!child.IsNull() && child.ToString() == "ddl") {
				ddl_enabled = true;
				break;
			}
		}
	}
	if (!ddl_enabled) {
		return std::move(result);
	}
	const auto start_snapshot = window[0].GetValue<int64_t>();
	auto end_snapshot = window[1].GetValue<int64_t>();
	const auto has_changes = window[2].GetValue<bool>();
	const auto schema_changes_pending = window[4].GetValue<bool>();
	if (!has_changes && schema_changes_pending) {
		end_snapshot = start_snapshot;
	} else if (!has_changes) {
		return std::move(result);
	}

	duckdb::Connection conn(*context.db);
	ExtractDdlRows(conn, data.catalog_name, start_snapshot, end_snapshot, std::string(), result->rows);
	return std::move(result);
}

struct CdcEventsData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t max_snapshots;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcEventsData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->max_snapshots = max_snapshots;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcEventsBind(duckdb::ClientContext &context,
                                                       duckdb::TableFunctionBindInput &input,
                                                       duckdb::vector<duckdb::LogicalType> &return_types,
                                                       duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_events requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<CdcEventsData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->max_snapshots = MaxSnapshotsParameter(input);

	names = {"snapshot_id",      "snapshot_time",  "changes_made",
	         "author",           "commit_message", "commit_extra_info",
	         "next_snapshot_id", "schema_version", "schema_changes_pending"};
	return_types = {duckdb::LogicalType::BIGINT,  duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BOOLEAN};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcEventsInit(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcEventsData>();

	CdcWindowData window_data;
	window_data.catalog_name = data.catalog_name;
	window_data.consumer_name = data.consumer_name;
	window_data.max_snapshots = data.max_snapshots;
	auto window = ReadWindow(context, window_data);
	const auto start_snapshot = window[0].GetValue<int64_t>();
	const auto end_snapshot = window[1].GetValue<int64_t>();
	const auto has_changes = window[2].GetValue<bool>();
	const auto schema_version = window[3].GetValue<int64_t>();
	const auto schema_changes_pending = window[4].GetValue<bool>();
	if (!has_changes) {
		return std::move(result);
	}

	duckdb::Connection conn(*context.db);
	auto consumer_row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);
	const auto filter_tables = CollectFilterTables(consumer_row.tables);
	const auto internal_ids = InternalTableIds(conn, data.catalog_name);

	// Project across the snapshot/changes join. `snapshots()` is the
	// upstream-typed variant we'd switch to once it is exposed in a way
	// Phase 1 can rely on; until then we read the same columns directly
	// from the metadata catalog so the schema is stable across DuckLake
	// versions in the supported window. `ducklake_snapshot_changes` is the
	// table that carries the per-snapshot commit metadata (author /
	// commit_message / commit_extra_info), not `ducklake_snapshot` itself.
	auto query = std::string("SELECT s.snapshot_id, s.snapshot_time, c.changes_made, c.author, "
	                         "c.commit_message, c.commit_extra_info FROM ") +
	             MetadataTable(data.catalog_name, "ducklake_snapshot") + " s JOIN " +
	             MetadataTable(data.catalog_name, "ducklake_snapshot_changes") +
	             " c USING (snapshot_id) WHERE s.snapshot_id BETWEEN " + std::to_string(start_snapshot) + " AND " +
	             std::to_string(end_snapshot) + " ORDER BY s.snapshot_id ASC";
	auto rows = conn.Query(query);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, rows ? rows->GetError() : "cdc_events scan failed");
	}
	for (duckdb::idx_t i = 0; i < rows->RowCount(); ++i) {
		const auto snapshot_value = rows->GetValue(0, i);
		if (snapshot_value.IsNull()) {
			continue;
		}
		const auto snapshot_id = snapshot_value.GetValue<int64_t>();
		const auto changes_value = rows->GetValue(2, i);
		const auto changes_str = changes_value.IsNull() ? std::string() : changes_value.ToString();
		if (InternalOnlyChanges(changes_str, internal_ids)) {
			continue;
		}
		if (!ChangesTouchConsumerTables(conn, data.catalog_name, snapshot_id, changes_str, filter_tables)) {
			continue;
		}
		std::vector<duckdb::Value> row;
		row.push_back(snapshot_value);
		row.push_back(rows->GetValue(1, i));
		row.push_back(changes_value);
		row.push_back(rows->GetValue(3, i));
		row.push_back(rows->GetValue(4, i));
		row.push_back(rows->GetValue(5, i));
		row.push_back(duckdb::Value::BIGINT(end_snapshot));
		row.push_back(duckdb::Value::BIGINT(schema_version));
		row.push_back(duckdb::Value::BOOLEAN(schema_changes_pending));
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

struct CdcChangesData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	std::string table_name;
	int64_t max_snapshots;
	// Column names + types belonging to the underlying DuckLake table (the
	// `<table cols>` segment of `table_changes(...)` output, after the
	// fixed `(snapshot_id, rowid, change_type)` prefix). Discovered during
	// Bind via a `LIMIT 0` probe so that the bind result can carry an
	// accurate schema for the planner.
	std::vector<std::string> table_column_names;
	std::vector<duckdb::LogicalType> table_column_types;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcChangesData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->table_name = table_name;
		result->max_snapshots = max_snapshots;
		result->table_column_names = table_column_names;
		result->table_column_types = table_column_types;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcChangesBind(duckdb::ClientContext &context,
                                                        duckdb::TableFunctionBindInput &input,
                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                        duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 3) {
		throw duckdb::BinderException("cdc_changes requires catalog, consumer name, and table");
	}
	auto result = duckdb::make_uniq<CdcChangesData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->table_name = GetStringArg(input.inputs[2], "table");
	result->max_snapshots = MaxSnapshotsParameter(input);

	CheckCatalogOrThrow(context, result->catalog_name);
	BootstrapConsumerStateOrThrow(context, result->catalog_name);

	// Probe the underlying `<lake>.table_changes(...)` schema with a `LIMIT 0`
	// query so the planner sees the table's actual columns. `table_changes`
	// projects the schema as of the END snapshot of the range, so the probe
	// snapshot must match the END snapshot the runtime scan will use.
	//
	// We replicate `ReadWindow`'s window-end computation read-only here
	// (no lease acquisition, no metadata writes). Two simpler strategies that
	// don't work:
	//
	//   * `last_committed_snapshot + 1` (the historical strategy): often
	//     lands on a CDC self-write snapshot — a lease UPDATE or an audit
	//     row. Those never carry a schema change for the user's table, so
	//     the probe returns the old column set even when the next external
	//     snapshot is the ALTER itself. This was the catalog-matrix smoke
	//     regression: post-commit, then ALTER + new-schema INSERT, then
	//     `cdc_changes` bound the OLD schema while the runtime range
	//     started at the ALTER and projected the NEW schema.
	//
	//   * `current_snapshot()` (intermediate fix): correct when the runtime
	//     range extends to the latest snapshot, wrong when the window is
	//     bounded BEFORE a pending schema change (the default
	//     `stop_at_schema_change=true` path). The runtime range projects the
	//     OLD schema; bind would over-project NEW columns and the runtime
	//     query would fail to bind `tc.<new column>`.
	//
	// Trade-off that remains: a producer ALTER landing between Bind and Init
	// can still drift the runtime schema past the bind columns. The
	// single-reader lease keeps this as a strictly producer/consumer race;
	// fixing it would require rebinding on every Init.
	duckdb::Connection conn(*context.db);
	int64_t probe_snapshot;
	{
		auto consumer_row = LoadConsumerOrThrow(conn, result->catalog_name, result->consumer_name);
		const auto last_snapshot = consumer_row.last_committed_snapshot;
		const auto current_snapshot = CurrentExternalSnapshot(conn, result->catalog_name, last_snapshot);
		const auto first_external_snapshot =
		    FirstExternalSnapshotAfter(conn, result->catalog_name, last_snapshot, current_snapshot);
		if (first_external_snapshot == -1) {
			// Consumer is caught up to head (or to the last external snap):
			// the next runtime range will be empty. Bind to the latest
			// known schema so a brand-new consumer or a fully-drained
			// consumer can still resolve columns.
			probe_snapshot = current_snapshot > 0 ? current_snapshot : CurrentSnapshot(conn, result->catalog_name);
		} else {
			const auto start_snapshot = first_external_snapshot;
			int64_t end_snapshot = std::min(current_snapshot, start_snapshot + result->max_snapshots - 1);
			if (start_snapshot <= current_snapshot && consumer_row.stop_at_schema_change) {
				const auto base_schema_version = ResolveSchemaVersion(conn, result->catalog_name, last_snapshot);
				const auto next_schema_change = NextExternalSchemaChangeSnapshot(
				    conn, result->catalog_name, start_snapshot, current_snapshot, base_schema_version);
				if (next_schema_change != -1 && next_schema_change <= end_snapshot) {
					end_snapshot = next_schema_change - 1;
				}
			}
			// `end_snapshot < start_snapshot` is the boundary-collapse case
			// (start IS the schema change AND stop_at_schema_change=true).
			// The runtime range will be empty; probe at `start` so bind
			// resolves under the schema that the *next* non-empty window
			// will see (the schema starting at the ALTER).
			probe_snapshot = end_snapshot >= start_snapshot ? end_snapshot : start_snapshot;
		}
	}
	auto probe = conn.Query("SELECT * FROM " + QuoteIdentifier(result->catalog_name) + ".table_changes(" +
	                        QuoteLiteral(result->table_name) + ", " + std::to_string(probe_snapshot) + ", " +
	                        std::to_string(probe_snapshot) + ") LIMIT 0");
	if (!probe || probe->HasError()) {
		throw duckdb::InvalidInputException(
		    "cdc_changes failed to resolve table '%s' in catalog '%s': %s", result->table_name, result->catalog_name,
		    probe ? probe->GetError() : std::string("table_changes probe returned no result"));
	}
	if (probe->ColumnCount() < 3) {
		throw duckdb::InvalidInputException(
		    "cdc_changes: unexpected table_changes schema for table '%s' (got %u columns; expected at least 3)",
		    result->table_name, static_cast<unsigned>(probe->ColumnCount()));
	}
	for (duckdb::idx_t i = 3; i < probe->ColumnCount(); ++i) {
		result->table_column_names.push_back(probe->ColumnName(i));
		result->table_column_types.push_back(probe->types[i]);
	}

	names = {"snapshot_id", "rowid", "change_type"};
	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR};
	for (duckdb::idx_t i = 0; i < result->table_column_names.size(); ++i) {
		names.push_back(result->table_column_names[i]);
		return_types.push_back(result->table_column_types[i]);
	}
	for (const auto &extra_name : {"snapshot_time", "author", "commit_message", "commit_extra_info"}) {
		names.push_back(extra_name);
	}
	return_types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	names.push_back("next_snapshot_id");
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcChangesInit(duckdb::ClientContext &context,
                                                                    duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcChangesData>();

	CdcWindowData window_data;
	window_data.catalog_name = data.catalog_name;
	window_data.consumer_name = data.consumer_name;
	window_data.max_snapshots = data.max_snapshots;
	auto window = ReadWindow(context, window_data);
	const auto start_snapshot = window[0].GetValue<int64_t>();
	const auto end_snapshot = window[1].GetValue<int64_t>();
	const auto has_changes = window[2].GetValue<bool>();
	if (!has_changes) {
		return std::move(result);
	}

	duckdb::Connection conn(*context.db);
	auto consumer_row = LoadConsumerOrThrow(conn, data.catalog_name, data.consumer_name);

	// Honor event_categories: a DDL-only consumer (`['ddl']`) sees zero
	// DML rows. The cursor still advances through the window because
	// cdc_window already ran above; this filter is the symmetric
	// counterpart to the one in cdc_ddl.
	if (!consumer_row.event_categories.IsNull()) {
		bool dml_enabled = false;
		for (const auto &child : duckdb::ListValue::GetChildren(consumer_row.event_categories)) {
			if (!child.IsNull() && child.ToString() == "dml") {
				dml_enabled = true;
				break;
			}
		}
		if (!dml_enabled) {
			return std::move(result);
		}
	}

	// Reject calls that ask for a table the consumer was created to ignore.
	// Better to fail loudly here than to silently return the empty result a
	// post-hoc filter would produce.
	const auto filter_tables = CollectFilterTables(consumer_row.tables);
	if (!filter_tables.empty()) {
		bool table_in_filter = false;
		const auto qualified =
		    data.table_name.find('.') == std::string::npos ? std::string("main.") + data.table_name : data.table_name;
		if (filter_tables.find(qualified) != filter_tables.end() ||
		    filter_tables.find(data.table_name) != filter_tables.end()) {
			table_in_filter = true;
		}
		if (!table_in_filter) {
			throw duckdb::InvalidInputException("cdc_changes: table '%s' is not in consumer '%s' tables filter",
			                                    data.table_name, data.consumer_name);
		}
	}

	std::ostringstream column_list;
	column_list << "tc.snapshot_id, tc.rowid, tc.change_type";
	for (const auto &col_name : data.table_column_names) {
		column_list << ", tc." << QuoteIdentifier(col_name);
	}

	std::ostringstream where_clause;
	const auto change_types = CollectChangeTypes(consumer_row.change_types);
	if (!change_types.empty()) {
		where_clause << " WHERE tc.change_type IN (";
		for (size_t i = 0; i < change_types.size(); ++i) {
			if (i > 0) {
				where_clause << ", ";
			}
			where_clause << QuoteLiteral(change_types[i]);
		}
		where_clause << ")";
	}

	auto query =
	    std::string("SELECT ") + column_list.str() +
	    ", s.snapshot_time, c.author, c.commit_message, c.commit_extra_info, " + std::to_string(end_snapshot) +
	    " AS next_snapshot_id FROM " + QuoteIdentifier(data.catalog_name) + ".table_changes(" +
	    QuoteLiteral(data.table_name) + ", " + std::to_string(start_snapshot) + ", " + std::to_string(end_snapshot) +
	    ") tc LEFT JOIN " + MetadataTable(data.catalog_name, "ducklake_snapshot") +
	    " s ON s.snapshot_id = tc.snapshot_id LEFT JOIN " +
	    MetadataTable(data.catalog_name, "ducklake_snapshot_changes") + " c ON c.snapshot_id = tc.snapshot_id" +
	    where_clause.str() + " ORDER BY tc.snapshot_id ASC, tc.rowid ASC";
	auto rows = conn.Query(query);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, rows ? rows->GetError() : "cdc_changes scan failed");
	}
	for (duckdb::idx_t row_idx = 0; row_idx < rows->RowCount(); ++row_idx) {
		std::vector<duckdb::Value> values;
		values.reserve(rows->ColumnCount());
		for (duckdb::idx_t col_idx = 0; col_idx < rows->ColumnCount(); ++col_idx) {
			values.push_back(rows->GetValue(col_idx, row_idx));
		}
		result->rows.push_back(std::move(values));
	}
	return std::move(result);
}

int64_t SinceSecondsParameter(duckdb::TableFunctionBindInput &input, int64_t default_value) {
	auto entry = input.named_parameters.find("since_seconds");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return entry->second.GetValue<int64_t>();
}

//! Resolve the start snapshot for stateless `cdc_recent_*` calls. The start
//! is `MAX(snapshot_id)` whose `snapshot_time <= now() - since_seconds`,
//! so the returned snapshot is the most-recent commit *before* the cutoff
//! and the read-out range `[start, current_snapshot()]` covers everything
//! that happened in the lookback window. When no snapshot is older than
//! the cutoff (typical for a brand-new catalog), start at 0 to include the
//! catalog from the beginning.
int64_t ResolveSinceStartSnapshot(duckdb::Connection &conn, const std::string &catalog_name, int64_t since_seconds) {
	// Epoch math instead of `now() - INTERVAL X SECOND`: DuckDB has no
	// `-(TIMESTAMP_TZ, INTERVAL)` binder, but `epoch(...)` works on any
	// timestamp variant and the comparison is straight DOUBLE arithmetic.
	auto result =
	    conn.Query("SELECT COALESCE(max(snapshot_id), 0) FROM " + MetadataTable(catalog_name, "ducklake_snapshot") +
	               " WHERE epoch(snapshot_time) <= epoch(now()) - " + std::to_string(since_seconds));
	return SingleInt64(*result, "since_seconds start snapshot");
}

struct CdcRecentChangesData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string table_name;
	int64_t since_seconds;
	std::vector<std::string> table_column_names;
	std::vector<duckdb::LogicalType> table_column_types;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcRecentChangesData>();
		result->catalog_name = catalog_name;
		result->table_name = table_name;
		result->since_seconds = since_seconds;
		result->table_column_names = table_column_names;
		result->table_column_types = table_column_types;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcRecentChangesBind(duckdb::ClientContext &context,
                                                              duckdb::TableFunctionBindInput &input,
                                                              duckdb::vector<duckdb::LogicalType> &return_types,
                                                              duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_recent_changes requires catalog and table");
	}
	auto result = duckdb::make_uniq<CdcRecentChangesData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->table_name = GetStringArg(input.inputs[1], "table");
	result->since_seconds = SinceSecondsParameter(input, 300);
	if (result->since_seconds < 0) {
		throw duckdb::InvalidInputException("cdc_recent_changes since_seconds must be >= 0");
	}
	CheckCatalogOrThrow(context, result->catalog_name);

	duckdb::Connection conn(*context.db);
	const auto current = CurrentSnapshot(conn, result->catalog_name);
	auto probe = conn.Query("SELECT * FROM " + QuoteIdentifier(result->catalog_name) + ".table_changes(" +
	                        QuoteLiteral(result->table_name) + ", " + std::to_string(current) + ", " +
	                        std::to_string(current) + ") LIMIT 0");
	if (!probe || probe->HasError()) {
		throw duckdb::InvalidInputException(
		    "cdc_recent_changes failed to resolve table '%s' in catalog '%s': %s", result->table_name,
		    result->catalog_name, probe ? probe->GetError() : std::string("table_changes probe returned no result"));
	}
	if (probe->ColumnCount() < 3) {
		throw duckdb::InvalidInputException(
		    "cdc_recent_changes: unexpected table_changes schema for table '%s' (got %u columns; expected at least 3)",
		    result->table_name, static_cast<unsigned>(probe->ColumnCount()));
	}
	for (duckdb::idx_t i = 3; i < probe->ColumnCount(); ++i) {
		result->table_column_names.push_back(probe->ColumnName(i));
		result->table_column_types.push_back(probe->types[i]);
	}

	names = {"snapshot_id", "rowid", "change_type"};
	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR};
	for (duckdb::idx_t i = 0; i < result->table_column_names.size(); ++i) {
		names.push_back(result->table_column_names[i]);
		return_types.push_back(result->table_column_types[i]);
	}
	for (const auto &extra : {"snapshot_time", "author", "commit_message", "commit_extra_info"}) {
		names.push_back(extra);
	}
	return_types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcRecentChangesInit(duckdb::ClientContext &context,
                                                                          duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcRecentChangesData>();

	duckdb::Connection conn(*context.db);
	const auto start_snapshot = ResolveSinceStartSnapshot(conn, data.catalog_name, data.since_seconds);
	const auto end_snapshot = CurrentSnapshot(conn, data.catalog_name);
	if (start_snapshot > end_snapshot) {
		return std::move(result);
	}

	std::ostringstream column_list;
	column_list << "tc.snapshot_id, tc.rowid, tc.change_type";
	for (const auto &col_name : data.table_column_names) {
		column_list << ", tc." << QuoteIdentifier(col_name);
	}

	auto query = std::string("SELECT ") + column_list.str() +
	             ", s.snapshot_time, c.author, c.commit_message, c.commit_extra_info FROM " +
	             QuoteIdentifier(data.catalog_name) + ".table_changes(" + QuoteLiteral(data.table_name) + ", " +
	             std::to_string(start_snapshot) + ", " + std::to_string(end_snapshot) + ") tc LEFT JOIN " +
	             MetadataTable(data.catalog_name, "ducklake_snapshot") +
	             " s ON s.snapshot_id = tc.snapshot_id LEFT JOIN " +
	             MetadataTable(data.catalog_name, "ducklake_snapshot_changes") +
	             " c ON c.snapshot_id = tc.snapshot_id" + " ORDER BY tc.snapshot_id ASC, tc.rowid ASC";
	auto rows = conn.Query(query);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "cdc_recent_changes scan failed");
	}
	for (duckdb::idx_t row_idx = 0; row_idx < rows->RowCount(); ++row_idx) {
		std::vector<duckdb::Value> values;
		values.reserve(rows->ColumnCount());
		for (duckdb::idx_t col_idx = 0; col_idx < rows->ColumnCount(); ++col_idx) {
			values.push_back(rows->GetValue(col_idx, row_idx));
		}
		result->rows.push_back(std::move(values));
	}
	return std::move(result);
}

struct CdcSchemaDiffData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string table_filter;
	int64_t from_snapshot;
	int64_t to_snapshot;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcSchemaDiffData>();
		result->catalog_name = catalog_name;
		result->table_filter = table_filter;
		result->from_snapshot = from_snapshot;
		result->to_snapshot = to_snapshot;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcSchemaDiffBind(duckdb::ClientContext &context,
                                                           duckdb::TableFunctionBindInput &input,
                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                           duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 4) {
		throw duckdb::BinderException("cdc_schema_diff requires catalog, table, from_snapshot, and to_snapshot");
	}
	auto result = duckdb::make_uniq<CdcSchemaDiffData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->table_filter = GetStringArg(input.inputs[1], "table");
	if (result->table_filter.find('.') == std::string::npos) {
		result->table_filter = std::string("main.") + result->table_filter;
	}
	result->from_snapshot = input.inputs[2].GetValue<int64_t>();
	result->to_snapshot = input.inputs[3].GetValue<int64_t>();
	if (result->from_snapshot < 0 || result->to_snapshot < result->from_snapshot) {
		throw duckdb::InvalidInputException(
		    "cdc_schema_diff: invalid snapshot range [%lld, %lld]; require 0 <= from <= to",
		    static_cast<long long>(result->from_snapshot), static_cast<long long>(result->to_snapshot));
	}
	CheckCatalogOrThrow(context, result->catalog_name);

	names = {"snapshot_id", "snapshot_time", "change_kind",  "column_id",    "old_name",    "new_name",
	         "old_type",    "new_type",      "old_default",  "new_default",  "old_nullable", "new_nullable",
	         "parent_column_id"};
	return_types = {duckdb::LogicalType::BIGINT,  duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN,      duckdb::LogicalType::BOOLEAN,
	                duckdb::LogicalType::BIGINT};
	return std::move(result);
}

//! Resolve the user-supplied `schema.table` filter into the set of
//! `table_id` values that *ever* had that fully-qualified name within
//! `[from_snapshot, to_snapshot]` (or that have it now). Renames preserve
//! `table_id`, so a single id is the typical case; the set form is here to
//! survive drop+recreate-with-same-name sequences without silently
//! conflating two distinct tables. Returning by id (not by name) means
//! cdc_schema_diff catches the original CREATE event under the old name
//! AND the rename + subsequent ALTERs under the new name in one sweep.
std::unordered_set<int64_t> ResolveTableIdsForFilter(duckdb::Connection &conn, const std::string &catalog_name,
                                                     const std::string &qualified_name, int64_t from_snapshot,
                                                     int64_t to_snapshot) {
	std::unordered_set<int64_t> ids;
	const auto dot = qualified_name.find('.');
	if (dot == std::string::npos) {
		return ids;
	}
	const auto schema_name = qualified_name.substr(0, dot);
	const auto table_name = qualified_name.substr(dot + 1);
	auto result = conn.Query("SELECT DISTINCT t.table_id FROM " + MetadataTable(catalog_name, "ducklake_table") +
	                         " t JOIN " + MetadataTable(catalog_name, "ducklake_schema") +
	                         " s USING (schema_id) WHERE s.schema_name = " + QuoteLiteral(schema_name) +
	                         " AND t.table_name = " + QuoteLiteral(table_name) +
	                         " AND t.begin_snapshot <= " + std::to_string(to_snapshot) +
	                         " AND (t.end_snapshot IS NULL OR t.end_snapshot > " + std::to_string(from_snapshot) + ")");
	if (!result || result->HasError()) {
		return ids;
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		const auto v = result->GetValue(0, i);
		if (!v.IsNull()) {
			ids.insert(v.GetValue<int64_t>());
		}
	}
	return ids;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcSchemaDiffInit(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcSchemaDiffData>();

	duckdb::Connection conn(*context.db);
	const auto target_ids =
	    ResolveTableIdsForFilter(conn, data.catalog_name, data.table_filter, data.from_snapshot, data.to_snapshot);
	if (target_ids.empty()) {
		// No table_id ever bore this qualified name within the range.
		// Empty result - safer than guessing an id.
		return std::move(result);
	}

	// Use the same Stage-1 token scan as cdc_ddl. Each token is checked
	// against `target_ids` by table_id (not by name) so renames don't
	// fragment the timeline. For each surfaced create/altered pair we emit
	// per-column diff rows plus an explicit `table_rename` row when the
	// table itself was renamed at that snapshot.
	auto changes = conn.Query("SELECT snapshot_id, changes_made FROM " +
	                          MetadataTable(data.catalog_name, "ducklake_snapshot_changes") +
	                          " WHERE snapshot_id BETWEEN " + std::to_string(data.from_snapshot) + " AND " +
	                          std::to_string(data.to_snapshot) + " ORDER BY snapshot_id ASC");
	if (!changes || changes->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        changes ? changes->GetError() : "cdc_schema_diff scan failed");
	}
	for (duckdb::idx_t row_idx = 0; row_idx < changes->RowCount(); ++row_idx) {
		const auto snapshot_id = changes->GetValue(0, row_idx).GetValue<int64_t>();
		const auto snapshot_time = SnapshotTime(conn, data.catalog_name, snapshot_id);
		for (const auto &token : SplitChangeTokens(changes->GetValue(1, row_idx).ToString())) {
			std::string schema_name;
			std::string object_name;
			int64_t table_id = 0;
			std::string old_table_name;
			std::string new_table_name;
			bool is_pure_create = false;

			if (ParseQualifiedNameToken(token, "created_table:", schema_name, object_name)) {
				const auto object = LookupTableByName(conn, data.catalog_name, snapshot_id, schema_name, object_name);
				if (object.object_id.IsNull()) {
					continue;
				}
				table_id = object.object_id.GetValue<int64_t>();
				if (target_ids.find(table_id) == target_ids.end()) {
					continue;
				}
				const auto previous = PreviousTableName(conn, data.catalog_name, table_id, snapshot_id);
				if (previous.IsNull()) {
					is_pure_create = true;
				} else if (previous.ToString() != object_name) {
					old_table_name = previous.ToString();
					new_table_name = object_name;
				}
			} else if (ParseIdToken(token, "altered_table:", table_id)) {
				if (target_ids.find(table_id) == target_ids.end()) {
					continue;
				}
			} else {
				continue;
			}

			if (is_pure_create) {
				const auto new_cols = LookupTableColumnsAt(conn, data.catalog_name, table_id, snapshot_id);
				// Stable order: parents before children, by depth in the
				// nested-struct tree. LookupTableColumnsAt already returns
				// in column_order which puts the parent struct ahead of
				// its fields naturally; the explicit sort makes the
				// guarantee independent of catalog ordering.
				std::unordered_map<int64_t, int64_t> create_parent_of;
				for (const auto &c : new_cols) {
					if (!c.parent_column.IsNull()) {
						create_parent_of[c.column_id] = c.parent_column.GetValue<int64_t>();
					}
				}
				auto create_depth = [&](int64_t column_id) {
					int depth = 0;
					auto cursor = column_id;
					while (true) {
						auto it = create_parent_of.find(cursor);
						if (it == create_parent_of.end()) {
							return depth;
						}
						++depth;
						cursor = it->second;
						if (depth > 64) {
							return depth;
						}
					}
				};
				std::vector<ColumnInfo> ordered = new_cols;
				std::stable_sort(ordered.begin(), ordered.end(),
				                 [&](const ColumnInfo &a, const ColumnInfo &b) {
					                 return create_depth(a.column_id) < create_depth(b.column_id);
				                 });
				for (const auto &c : ordered) {
					duckdb::Value parent_value =
					    c.parent_column.IsNull() ? duckdb::Value() : duckdb::Value::BIGINT(c.parent_column.GetValue<int64_t>());
					result->rows.push_back(
					    {duckdb::Value::BIGINT(snapshot_id), snapshot_time, duckdb::Value("added"),
					     duckdb::Value::BIGINT(c.column_id), duckdb::Value(), duckdb::Value(c.column_name),
					     duckdb::Value(), duckdb::Value(c.column_type), duckdb::Value(),
					     c.default_value.IsNull() ? duckdb::Value() : duckdb::Value(c.default_value.ToString()),
					     duckdb::Value(), duckdb::Value::BOOLEAN(c.nullable), parent_value});
				}
				continue;
			}

			if (!old_table_name.empty() && old_table_name != new_table_name) {
				result->rows.push_back({duckdb::Value::BIGINT(snapshot_id), snapshot_time,
				                        duckdb::Value("table_rename"), duckdb::Value(), duckdb::Value(old_table_name),
				                        duckdb::Value(new_table_name), duckdb::Value(), duckdb::Value(),
				                        duckdb::Value(), duckdb::Value(), duckdb::Value(), duckdb::Value(),
				                        duckdb::Value()});
			}

			const auto old_cols = LookupTableColumnsAt(conn, data.catalog_name, table_id, snapshot_id - 1);
			const auto new_cols = LookupTableColumnsAt(conn, data.catalog_name, table_id, snapshot_id);
			auto diffs = DiffColumns(old_cols, new_cols);
			NormaliseDiffParentChildOrdering(diffs);
			for (const auto &d : diffs) {
				duckdb::Value old_default =
				    d.old_default.IsNull() ? duckdb::Value() : duckdb::Value(d.old_default.ToString());
				duckdb::Value new_default =
				    d.new_default.IsNull() ? duckdb::Value() : duckdb::Value(d.new_default.ToString());
				duckdb::Value old_name = d.old_name.empty() ? duckdb::Value() : duckdb::Value(d.old_name);
				duckdb::Value new_name = d.new_name.empty() ? duckdb::Value() : duckdb::Value(d.new_name);
				duckdb::Value old_type = d.old_type.empty() ? duckdb::Value() : duckdb::Value(d.old_type);
				duckdb::Value new_type = d.new_type.empty() ? duckdb::Value() : duckdb::Value(d.new_type);
				duckdb::Value old_nullable =
				    (d.kind == ColumnDiff::Kind::DROPPED || d.kind == ColumnDiff::Kind::NULLABLE_CHANGED)
				        ? duckdb::Value::BOOLEAN(d.old_nullable)
				        : duckdb::Value();
				duckdb::Value new_nullable =
				    (d.kind == ColumnDiff::Kind::ADDED || d.kind == ColumnDiff::Kind::NULLABLE_CHANGED)
				        ? duckdb::Value::BOOLEAN(d.new_nullable)
				        : duckdb::Value();
				duckdb::Value parent_value =
				    d.has_parent ? duckdb::Value::BIGINT(d.parent_column_id) : duckdb::Value();
				result->rows.push_back({duckdb::Value::BIGINT(snapshot_id), snapshot_time,
				                        duckdb::Value(DiffKindToString(d.kind)), duckdb::Value::BIGINT(d.column_id),
				                        old_name, new_name, old_type, new_type, old_default, new_default, old_nullable,
				                        new_nullable, parent_value});
			}
		}
	}
	return std::move(result);
}

struct CdcConsumerStatsData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcConsumerStatsData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcConsumerStatsBind(duckdb::ClientContext &context,
                                                              duckdb::TableFunctionBindInput &input,
                                                              duckdb::vector<duckdb::LogicalType> &return_types,
                                                              duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_consumer_stats requires catalog");
	}
	auto result = duckdb::make_uniq<CdcConsumerStatsData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	auto entry = input.named_parameters.find("consumer");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		result->consumer_name = entry->second.GetValue<std::string>();
	}

	names = {"consumer_name",
	         "consumer_id",
	         "last_committed_snapshot",
	         "current_snapshot",
	         "lag_snapshots",
	         "lag_seconds",
	         "oldest_available_snapshot",
	         "gap_distance",
	         "tables",
	         "tables_unresolved",
	         "change_types",
	         "owner_token",
	         "owner_acquired_at",
	         "owner_heartbeat_at",
	         "lease_interval_seconds",
	         "lease_alive"};
	return_types = {duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::DOUBLE,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
	                duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
	                duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
	                duckdb::LogicalType::UUID,
	                duckdb::LogicalType::TIMESTAMP_TZ,
	                duckdb::LogicalType::TIMESTAMP_TZ,
	                duckdb::LogicalType::INTEGER,
	                duckdb::LogicalType::BOOLEAN};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcConsumerStatsInit(duckdb::ClientContext &context,
                                                                          duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcConsumerStatsData>();
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	const auto current_snapshot = CurrentSnapshot(conn, data.catalog_name);
	auto oldest_result = conn.Query("SELECT COALESCE(min(snapshot_id), 0) FROM " +
	                                MetadataTable(data.catalog_name, "ducklake_snapshot"));
	const auto oldest_snapshot = SingleInt64(*oldest_result, "oldest snapshot");

	const auto consumers = MainTable(data.catalog_name, CONSUMERS_TABLE);
	std::ostringstream where;
	if (!data.consumer_name.empty()) {
		where << " WHERE c.consumer_name = " << QuoteLiteral(data.consumer_name);
	}

	// Single big projection so the planner JOIN-and-aggregates against
	// ducklake_snapshot in one pass per consumer.
	auto query = std::string("SELECT c.consumer_name, c.consumer_id, c.last_committed_snapshot, "
	                         "s.snapshot_time, c.tables, c.change_types, c.owner_token, c.owner_acquired_at, "
	                         "c.owner_heartbeat_at, c.lease_interval_seconds FROM ") +
	             consumers + " c LEFT JOIN " + MetadataTable(data.catalog_name, "ducklake_snapshot") +
	             " s ON s.snapshot_id = c.last_committed_snapshot" + where.str() + " ORDER BY c.consumer_name ASC";
	auto rows = conn.Query(query);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "cdc_consumer_stats scan failed");
	}

	for (duckdb::idx_t i = 0; i < rows->RowCount(); ++i) {
		const auto consumer_name_value = rows->GetValue(0, i);
		const auto consumer_id = rows->GetValue(1, i).GetValue<int64_t>();
		const auto last_committed_snapshot = rows->GetValue(2, i).GetValue<int64_t>();
		const auto last_snapshot_time = rows->GetValue(3, i);
		const auto tables_value = rows->GetValue(4, i);
		const auto change_types_value = rows->GetValue(5, i);
		const auto owner_token_value = rows->GetValue(6, i);
		const auto owner_acquired_value = rows->GetValue(7, i);
		const auto owner_heartbeat_value = rows->GetValue(8, i);
		const auto lease_interval = rows->GetValue(9, i).GetValue<int64_t>();

		const auto lag_snapshots = current_snapshot - last_committed_snapshot;
		duckdb::Value lag_seconds_value;
		if (!last_snapshot_time.IsNull()) {
			auto lag_query = conn.Query(std::string("SELECT epoch(now()) - epoch(") +
			                            QuoteLiteral(last_snapshot_time.ToString()) + "::TIMESTAMP WITH TIME ZONE)");
			if (lag_query && !lag_query->HasError() && lag_query->RowCount() > 0 &&
			    !lag_query->GetValue(0, 0).IsNull()) {
				lag_seconds_value = duckdb::Value::DOUBLE(lag_query->GetValue(0, 0).GetValue<double>());
			}
		}

		const auto gap_distance = last_committed_snapshot - oldest_snapshot;

		// tables_unresolved: every table in `c.tables` that does not exist
		// at current_snapshot. Empty filter -> empty unresolved list (the
		// consumer takes everything, so nothing is unresolved by definition).
		const auto filter_tables = CollectFilterTables(tables_value);
		duckdb::vector<duckdb::Value> unresolved;
		if (!filter_tables.empty()) {
			std::ostringstream existing_query;
			existing_query << "SELECT s.schema_name || '.' || t.table_name FROM " +
			                      MetadataTable(data.catalog_name, "ducklake_table") + " t JOIN " +
			                      MetadataTable(data.catalog_name, "ducklake_schema") +
			                      " s USING (schema_id) WHERE t.begin_snapshot <= "
			               << current_snapshot << " AND (t.end_snapshot IS NULL OR t.end_snapshot > "
			               << current_snapshot << ")";
			auto existing = conn.Query(existing_query.str());
			std::unordered_set<std::string> existing_set;
			if (existing && !existing->HasError()) {
				for (duckdb::idx_t j = 0; j < existing->RowCount(); ++j) {
					if (!existing->GetValue(0, j).IsNull()) {
						existing_set.insert(existing->GetValue(0, j).ToString());
					}
				}
			}
			for (const auto &name : filter_tables) {
				if (existing_set.find(name) == existing_set.end()) {
					unresolved.push_back(duckdb::Value(name));
				}
			}
		}

		// `lease_alive`: the lease is held AND the heartbeat is within
		// the lease interval. NULL owner_token means no holder, so dead
		// (false; not NULL) — the column is more useful as a 2-state
		// "is anyone reading right now" than a 3-state.
		bool lease_alive = false;
		if (!owner_token_value.IsNull() && !owner_heartbeat_value.IsNull()) {
			auto alive_query = conn.Query(std::string("SELECT epoch(now()) - epoch(") +
			                              QuoteLiteral(owner_heartbeat_value.ToString()) +
			                              "::TIMESTAMP WITH TIME ZONE) < " + std::to_string(lease_interval));
			if (alive_query && !alive_query->HasError() && alive_query->RowCount() > 0 &&
			    !alive_query->GetValue(0, 0).IsNull()) {
				lease_alive = alive_query->GetValue(0, 0).GetValue<bool>();
			}
		}

		std::vector<duckdb::Value> row;
		row.push_back(consumer_name_value);
		row.push_back(duckdb::Value::BIGINT(consumer_id));
		row.push_back(duckdb::Value::BIGINT(last_committed_snapshot));
		row.push_back(duckdb::Value::BIGINT(current_snapshot));
		row.push_back(duckdb::Value::BIGINT(lag_snapshots));
		row.push_back(lag_seconds_value);
		row.push_back(duckdb::Value::BIGINT(oldest_snapshot));
		row.push_back(duckdb::Value::BIGINT(gap_distance));
		row.push_back(tables_value);
		row.push_back(duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, unresolved));
		row.push_back(change_types_value);
		row.push_back(owner_token_value);
		row.push_back(owner_acquired_value);
		row.push_back(owner_heartbeat_value);
		row.push_back(duckdb::Value::INTEGER(static_cast<int32_t>(lease_interval)));
		row.push_back(duckdb::Value::BOOLEAN(lease_alive));
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

struct CdcAuditRecentData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t since_seconds;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcAuditRecentData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->since_seconds = since_seconds;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcAuditRecentBind(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_audit_recent requires catalog");
	}
	auto result = duckdb::make_uniq<CdcAuditRecentData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->since_seconds = SinceSecondsParameter(input, 86400);
	if (result->since_seconds < 0) {
		throw duckdb::InvalidInputException("cdc_audit_recent since_seconds must be >= 0");
	}
	auto entry = input.named_parameters.find("consumer");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		result->consumer_name = entry->second.GetValue<std::string>();
	}

	names = {"ts", "audit_id", "actor", "action", "consumer_name", "consumer_id", "details"};
	return_types = {duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcAuditRecentInit(duckdb::ClientContext &context,
                                                                        duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcAuditRecentData>();
	BootstrapConsumerStateOrThrow(context, data.catalog_name);

	duckdb::Connection conn(*context.db);
	const auto audit = MainTable(data.catalog_name, AUDIT_TABLE);
	std::ostringstream where;
	where << " WHERE epoch(ts) >= epoch(now()) - " << data.since_seconds;
	if (!data.consumer_name.empty()) {
		where << " AND consumer_name = " << QuoteLiteral(data.consumer_name);
	}
	auto query = std::string("SELECT ts, audit_id, actor, action, consumer_name, consumer_id, details FROM ") + audit +
	             where.str() + " ORDER BY ts DESC, audit_id DESC";
	auto rows = conn.Query(query);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "cdc_audit_recent scan failed");
	}
	for (duckdb::idx_t i = 0; i < rows->RowCount(); ++i) {
		std::vector<duckdb::Value> row;
		row.reserve(rows->ColumnCount());
		for (duckdb::idx_t col_idx = 0; col_idx < rows->ColumnCount(); ++col_idx) {
			row.push_back(rows->GetValue(col_idx, i));
		}
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

struct CdcRecentDdlData : public duckdb::TableFunctionData {
	std::string catalog_name;
	int64_t since_seconds;
	std::string table_filter;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcRecentDdlData>();
		result->catalog_name = catalog_name;
		result->since_seconds = since_seconds;
		result->table_filter = table_filter;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcRecentDdlBind(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 1) {
		throw duckdb::BinderException("cdc_recent_ddl requires catalog");
	}
	auto result = duckdb::make_uniq<CdcRecentDdlData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->since_seconds = SinceSecondsParameter(input, 86400);
	if (result->since_seconds < 0) {
		throw duckdb::InvalidInputException("cdc_recent_ddl since_seconds must be >= 0");
	}
	// `for_table` instead of the roadmap's `table := ...` because `table`
	// is a reserved word in DuckDB's parser and would force callers into
	// awkward `"table" = '...'` quoting at every call site.
	auto entry = input.named_parameters.find("for_table");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		result->table_filter = entry->second.GetValue<std::string>();
		if (result->table_filter.find('.') == std::string::npos) {
			result->table_filter = std::string("main.") + result->table_filter;
		}
	}
	CheckCatalogOrThrow(context, result->catalog_name);

	names = {"snapshot_id", "snapshot_time", "event_kind",  "object_kind", "schema_id",
	         "schema_name", "object_id",     "object_name", "details"};
	return_types = {duckdb::LogicalType::BIGINT,  duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,       duckdb::LogicalType::VARCHAR,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,      duckdb::LogicalType::VARCHAR};
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcRecentDdlInit(duckdb::ClientContext &context,
                                                                      duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcRecentDdlData>();

	duckdb::Connection conn(*context.db);
	const auto start_snapshot = ResolveSinceStartSnapshot(conn, data.catalog_name, data.since_seconds);
	const auto end_snapshot = CurrentSnapshot(conn, data.catalog_name);
	if (start_snapshot > end_snapshot) {
		return std::move(result);
	}
	ExtractDdlRows(conn, data.catalog_name, start_snapshot, end_snapshot, data.table_filter, result->rows);
	return std::move(result);
}

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
	             MainTable(data.catalog_name, CONSUMERS_TABLE) + " ORDER BY consumer_id ASC";
	auto query_result = conn.Query(query);
	if (!query_result || query_result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, query_result ? query_result->GetError() : query);
	}
	for (duckdb::idx_t row_idx = 0; row_idx < query_result->RowCount(); ++row_idx) {
		std::vector<duckdb::Value> row;
		for (duckdb::idx_t col_idx = 0; col_idx < query_result->ColumnCount(); ++col_idx) {
			row.push_back(query_result->GetValue(col_idx, row_idx));
		}
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

} // namespace

void BootstrapConsumerStateOrThrow(duckdb::ClientContext &context, const std::string &catalog_name) {
	CheckCatalogOrThrow(context, catalog_name);
	duckdb::Connection conn(*context.db);
	ExecuteChecked(conn, ConsumersDdl(catalog_name));
	ExecuteChecked(conn, AuditDdl(catalog_name));
	ExecuteChecked(conn, DlqDdl(catalog_name));
}

void RegisterConsumerStateFunctions(duckdb::ExtensionLoader &loader) {
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

	for (const auto &name : {"cdc_ddl", "ducklake_cdc_ddl"}) {
		duckdb::TableFunction ddl_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, CdcDdlBind, CdcDdlInit);
		ddl_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		loader.RegisterFunction(ddl_function);
	}

	for (const auto &name : {"cdc_consumer_list", "ducklake_cdc_consumer_list"}) {
		duckdb::TableFunction list_function(name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
		                                    RowScanExecute, ConsumerListBind, ConsumerListInit);
		loader.RegisterFunction(list_function);
	}

	for (const auto &name : {"cdc_events", "ducklake_cdc_events"}) {
		duckdb::TableFunction events_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, CdcEventsBind, CdcEventsInit);
		events_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		loader.RegisterFunction(events_function);
	}

	for (const auto &name : {"cdc_changes", "ducklake_cdc_changes"}) {
		duckdb::TableFunction changes_function(name,
		                                       duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR,
		                                                                            duckdb::LogicalType::VARCHAR,
		                                                                            duckdb::LogicalType::VARCHAR},
		                                       RowScanExecute, CdcChangesBind, CdcChangesInit);
		changes_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		loader.RegisterFunction(changes_function);
	}

	for (const auto &name : {"cdc_recent_changes", "ducklake_cdc_recent_changes"}) {
		duckdb::TableFunction recent_changes_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, CdcRecentChangesBind, CdcRecentChangesInit);
		recent_changes_function.named_parameters["since_seconds"] = duckdb::LogicalType::BIGINT;
		loader.RegisterFunction(recent_changes_function);
	}

	for (const auto &name : {"cdc_recent_ddl", "ducklake_cdc_recent_ddl"}) {
		duckdb::TableFunction recent_ddl_function(name,
		                                          duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
		                                          RowScanExecute, CdcRecentDdlBind, CdcRecentDdlInit);
		recent_ddl_function.named_parameters["since_seconds"] = duckdb::LogicalType::BIGINT;
		recent_ddl_function.named_parameters["for_table"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(recent_ddl_function);
	}

	for (const auto &name : {"cdc_schema_diff", "ducklake_cdc_schema_diff"}) {
		duckdb::TableFunction schema_diff_function(
		    name,
		    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
		                                         duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
		    RowScanExecute, CdcSchemaDiffBind, CdcSchemaDiffInit);
		loader.RegisterFunction(schema_diff_function);
	}

	for (const auto &name : {"cdc_consumer_stats", "ducklake_cdc_consumer_stats"}) {
		duckdb::TableFunction stats_function(name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
		                                     RowScanExecute, CdcConsumerStatsBind, CdcConsumerStatsInit);
		stats_function.named_parameters["consumer"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(stats_function);
	}

	for (const auto &name : {"cdc_audit_recent", "ducklake_cdc_audit_recent"}) {
		duckdb::TableFunction audit_function(name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR},
		                                     RowScanExecute, CdcAuditRecentBind, CdcAuditRecentInit);
		audit_function.named_parameters["since_seconds"] = duckdb::LogicalType::BIGINT;
		audit_function.named_parameters["consumer"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(audit_function);
	}
}

} // namespace duckdb_cdc
