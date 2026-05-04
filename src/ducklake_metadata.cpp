//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// ducklake_metadata.cpp
//
// Implementation of the shared "facts about the lake" layer. See
// ducklake_metadata.hpp for the facts-vs-policy boundary that governs
// what does and does not belong in this file.
//===----------------------------------------------------------------------===//

#include "ducklake_metadata.hpp"

#include "compat_check.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <cctype>
#include <mutex>
#include <sstream>
#include <unordered_set>

namespace duckdb_cdc {

//===--------------------------------------------------------------------===//
// State-schema constants and tunables (declared in ducklake_metadata.hpp)
//===--------------------------------------------------------------------===//

const char *const CONSUMERS_TABLE = "__ducklake_cdc_consumers";
const char *const CONSUMER_SUBSCRIPTIONS_TABLE = "__ducklake_cdc_consumer_subscriptions";
const char *const AUDIT_TABLE = "__ducklake_cdc_audit";
const char *const STATE_SCHEMA = "__ducklake_cdc";

const int64_t DEFAULT_MAX_SNAPSHOTS = 100;
const int64_t HARD_MAX_SNAPSHOTS = 1000;
const int64_t DEFAULT_WAIT_TIMEOUT_MS = 30000;
const int64_t HARD_WAIT_TIMEOUT_MS = 300000;
const int64_t WAIT_INITIAL_INTERVAL_MS = 50;
const int64_t WAIT_MAX_INTERVAL_MS = 10000;

//===--------------------------------------------------------------------===//
// SQL identifier / literal helpers
//===--------------------------------------------------------------------===//

std::string QuoteIdentifier(const std::string &identifier) {
	return duckdb::KeywordHelper::WriteOptionallyQuoted(identifier);
}

std::string QuoteLiteral(const std::string &value) {
	return duckdb::KeywordHelper::WriteQuoted(value);
}

//===--------------------------------------------------------------------===//
// JSON / string helpers
//===--------------------------------------------------------------------===//

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

std::string TrimCopy(const std::string &value) {
	size_t start = 0;
	while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start]))) {
		start++;
	}
	size_t end = value.size();
	while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
		end--;
	}
	return value.substr(start, end - start);
}

bool StartsWith(const std::string &value, const std::string &prefix) {
	return value.rfind(prefix, 0) == 0;
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

std::string JsonValue(const duckdb::Value &value) {
	if (value.IsNull()) {
		return "null";
	}
	return "\"" + JsonEscape(value.ToString()) + "\"";
}

std::string JsonOptionalString(const duckdb::Value &v) {
	if (v.IsNull()) {
		return "null";
	}
	return std::string("\"") + JsonEscape(v.ToString()) + "\"";
}

//===--------------------------------------------------------------------===//
// Bind-input / query-result helpers
//===--------------------------------------------------------------------===//

std::string GetStringArg(const duckdb::Value &value, const std::string &name) {
	if (value.IsNull()) {
		throw duckdb::BinderException("%s cannot be NULL", name);
	}
	return value.GetValue<std::string>();
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

void ThrowIfQueryFailed(const duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result) {
	if (result && result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, result->GetError());
	}
}

void ExecuteChecked(duckdb::Connection &conn, const std::string &sql) {
	auto result = conn.Query(sql);
	ThrowIfQueryFailed(result);
}

std::string GenerateUuid(duckdb::Connection &conn) {
	auto result = conn.Query("SELECT uuid()");
	if (!result || result->HasError() || result->RowCount() == 0) {
		throw duckdb::InvalidInputException("Unable to generate owner token");
	}
	return result->GetValue(0, 0).ToString();
}

void ConfigureCdcInternalConnection(duckdb::Connection &conn) {
	// CDC internal connections execute short metadata lookups and materialize
	// table-function results. Letting DuckDB parallelize those queries can make
	// one logical consumer occupy most of the postgres-scanner connection pool.
	// Keep the internal work serial; user analytical queries still use the
	// caller's configured thread count.
	ExecuteChecked(conn, "SET threads = 1");
}

//===--------------------------------------------------------------------===//
// Catalog table-name builders + state-schema introspection
//===--------------------------------------------------------------------===//

std::string MetadataDatabase(const std::string &catalog_name) {
	return QuoteIdentifier("__ducklake_metadata_" + catalog_name);
}

std::string MetadataTable(const std::string &catalog_name, const std::string &table_name) {
	return MetadataDatabase(catalog_name) + "." + QuoteIdentifier(table_name);
}

std::string MetadataAttachmentCacheKey(duckdb::Connection &conn, const std::string &catalog_name) {
	const auto metadata_database = "__ducklake_metadata_" + catalog_name;
	auto result = conn.Query("SELECT database_oid, type, path FROM duckdb_databases() WHERE database_name = " +
	                         QuoteLiteral(metadata_database) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return "";
	}
	const auto oid = result->GetValue(0, 0).ToString();
	const auto type = result->GetValue(1, 0).IsNull() ? "" : result->GetValue(1, 0).ToString();
	const auto path = result->GetValue(2, 0).IsNull() ? "" : result->GetValue(2, 0).ToString();
	return metadata_database + "|" + oid + "|" + type + "|" + path;
}

std::string StateTable(const std::string &catalog_name, const std::string &table_name, bool use_state_schema) {
	if (use_state_schema) {
		return MetadataDatabase(catalog_name) + "." + QuoteIdentifier(STATE_SCHEMA) + "." + QuoteIdentifier(table_name);
	}
	return MetadataDatabase(catalog_name) + ".main." + QuoteIdentifier(table_name);
}

namespace {

// Cache attached metadata databases whose `__ducklake_cdc` state schema we've
// observed to exist. Bootstrap creates the schema once and never drops it for a
// given attachment, so this is monotonic per attached database. The key must not
// be just `catalog_name`: CI exercises multiple backend attachments with the
// same logical alias (`lake`) in one process.
//
// We rely on the cache because DuckDB's catalog enumeration over
// postgres-attached databases (`duckdb_schemas()` and `duckdb_tables()`) is not
// always self-consistent across consecutive calls on the same connection:
// building one SQL string with two `StateTable(...)` calls has been observed to
// yield `__ducklake_cdc` for the first lookup and `main` for the second,
// producing
//   Catalog Error: schema "main" does not exist
// at the listen call site.
std::mutex STATE_SCHEMA_CACHE_MUTEX;
std::unordered_set<std::string> STATE_SCHEMA_CACHE;

bool StateSchemaCacheLookup(const std::string &cache_key) {
	if (cache_key.empty()) {
		return false;
	}
	std::lock_guard<std::mutex> guard(STATE_SCHEMA_CACHE_MUTEX);
	return STATE_SCHEMA_CACHE.count(cache_key) > 0;
}

void StateSchemaCacheRemember(const std::string &cache_key) {
	if (cache_key.empty()) {
		return;
	}
	std::lock_guard<std::mutex> guard(STATE_SCHEMA_CACHE_MUTEX);
	STATE_SCHEMA_CACHE.insert(cache_key);
}

bool ProbeStateSchema(duckdb::Connection &conn, const std::string &catalog_name) {
	const auto md_db = QuoteLiteral("__ducklake_metadata_" + catalog_name);
	auto schemas = conn.Query("SELECT count(*) FROM duckdb_schemas() WHERE database_name = " + md_db +
	                          " AND schema_name = " + QuoteLiteral(STATE_SCHEMA));
	if (schemas && !schemas->HasError() && schemas->RowCount() > 0 && !schemas->GetValue(0, 0).IsNull() &&
	    schemas->GetValue(0, 0).GetValue<int64_t>() > 0) {
		return true;
	}
	// `duckdb_schemas()` did not see it. Try the table-level enumeration
	// for the consumers table that bootstrap always creates.
	auto tables = conn.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = " + md_db +
	                         " AND schema_name = " + QuoteLiteral(STATE_SCHEMA) +
	                         " AND table_name = " + QuoteLiteral(CONSUMERS_TABLE));
	return tables && !tables->HasError() && tables->RowCount() > 0 && !tables->GetValue(0, 0).IsNull() &&
	       tables->GetValue(0, 0).GetValue<int64_t>() > 0;
}

} // namespace

bool StateSchemaExists(duckdb::Connection &conn, const std::string &catalog_name) {
	const auto cache_key = MetadataAttachmentCacheKey(conn, catalog_name);
	if (StateSchemaCacheLookup(cache_key)) {
		return true;
	}
	if (ProbeStateSchema(conn, catalog_name)) {
		StateSchemaCacheRemember(cache_key);
		return true;
	}
	return false;
}

std::string StateTable(duckdb::Connection &conn, const std::string &catalog_name, const std::string &table_name) {
	return StateTable(catalog_name, table_name, StateSchemaExists(conn, catalog_name));
}

//===--------------------------------------------------------------------===//
// Snapshot fact lookups
//===--------------------------------------------------------------------===//

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
	return ResolveSnapshot(conn, catalog_name, start_at, "start_at", "cdc_ddl_consumer_create/cdc_dml_consumer_create",
	                       false);
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

duckdb::Value SnapshotTime(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id) {
	auto result = conn.Query("SELECT snapshot_time FROM " + MetadataTable(catalog_name, "ducklake_snapshot") +
	                         " WHERE snapshot_id = " + std::to_string(snapshot_id));
	if (!result || result->HasError() || result->RowCount() == 0) {
		return duckdb::Value();
	}
	return result->GetValue(0, 0);
}

int64_t FirstSnapshotAfter(duckdb::Connection &conn, const std::string &catalog_name, int64_t last_snapshot,
                           int64_t current_snapshot) {
	auto result = conn.Query("SELECT snapshot_id FROM " + MetadataTable(catalog_name, "ducklake_snapshot_changes") +
	                         " WHERE snapshot_id > " + std::to_string(last_snapshot) +
	                         " AND snapshot_id <= " + std::to_string(current_snapshot) + " ORDER BY snapshot_id ASC");
	if (!result || result->HasError()) {
		return -1;
	}
	for (duckdb::idx_t i = 0; i < result->RowCount(); ++i) {
		auto snapshot_value = result->GetValue(0, i);
		if (!snapshot_value.IsNull()) {
			return snapshot_value.GetValue<int64_t>();
		}
	}
	return -1;
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

//===--------------------------------------------------------------------===//
// Structural schema-change predicates
//===--------------------------------------------------------------------===//

//! True iff `changes_made` contains at least one DDL token.
bool SnapshotHasSchemaChange(const std::string &changes_made) {
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
			if (StartsWith(token, prefix)) {
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
//! DDL token (i.e. it is itself a schema-change snapshot).
//! Used by ReadWindow to surface `schema_changes_pending = true` even
//! when the schema change is at `start_snapshot` (in which case
//! `NextExternalSchemaChangeSnapshot` returns -1, since it searches
//! strictly AFTER start).
bool SnapshotIsExternalSchemaChange(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id) {
	auto result = conn.Query("SELECT changes_made FROM " + MetadataTable(catalog_name, "ducklake_snapshot_changes") +
	                         " WHERE snapshot_id = " + std::to_string(snapshot_id));
	if (!result || result->HasError() || result->RowCount() == 0) {
		return false;
	}
	auto changes_value = result->GetValue(0, 0);
	if (changes_value.IsNull()) {
		return false;
	}
	return SnapshotHasSchemaChange(changes_value.ToString());
}

//! Returns the first snapshot in `(start_snapshot, current_snapshot]`
//! whose `changes_made` contains a DDL token. Comparing
//! `ResolveSchemaVersion` directly is too broad because data-only commits can
//! still advance DuckLake's physical schema version.
//! `base_schema_version` is unused now (kept for source compat with
//! older callers); the predicate is structural — "did this snapshot do
//! DDL?" — not numeric.
int64_t NextExternalSchemaChangeSnapshot(duckdb::Connection &conn, const std::string &catalog_name,
                                         int64_t start_snapshot, int64_t current_snapshot,
                                         int64_t /*base_schema_version*/) {
	// Search strictly AFTER `start_snapshot`: the snapshot at `start` is
	// the first one in this window. If it carries a schema change itself
	// (e.g. the consumer just committed past the previous boundary and is
	// about to read the ALTER), the window's schema_version is the
	// post-change version (set by ResolveSchemaVersion(start) in the
	// caller); bounding the window at `start - 1` here would be wrong.
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
		if (SnapshotHasSchemaChange(changes_value.ToString())) {
			return snapshot_value.GetValue<int64_t>();
		}
	}
	return -1;
}

//===--------------------------------------------------------------------===//
// `since_seconds` lookback helpers
//===--------------------------------------------------------------------===//

int64_t SinceSecondsParameter(duckdb::TableFunctionBindInput &input, int64_t default_value) {
	auto entry = input.named_parameters.find("since_seconds");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return entry->second.GetValue<int64_t>();
}

//! Resolve the start snapshot for bounded stateless query calls. The start
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

//===--------------------------------------------------------------------===//
// State-table DDL strings + lazy bootstrap
//===--------------------------------------------------------------------===//

std::string ConsumersDdl(const std::string &catalog_name, bool use_state_schema) {
	// `table_id` is the single subscribed table for DML consumers (one
	// DML consumer = one table, by contract). It is NULL for DDL
	// consumers — DDL consumers can subscribe to schemas, tables, or the
	// whole catalog and route those facts through
	// `__ducklake_cdc_consumer_subscriptions`.
	return "CREATE TABLE IF NOT EXISTS " + StateTable(catalog_name, CONSUMERS_TABLE, use_state_schema) +
	       " ("
	       "consumer_name VARCHAR, "
	       "consumer_kind VARCHAR NOT NULL, "
	       "consumer_id BIGINT NOT NULL, "
	       "table_id BIGINT, "
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

std::string ConsumerSubscriptionsDdl(const std::string &catalog_name, bool use_state_schema) {
	return "CREATE TABLE IF NOT EXISTS " + StateTable(catalog_name, CONSUMER_SUBSCRIPTIONS_TABLE, use_state_schema) +
	       " ("
	       "consumer_id BIGINT NOT NULL, "
	       "subscription_id BIGINT NOT NULL, "
	       "scope_kind VARCHAR NOT NULL, "
	       "schema_id BIGINT, "
	       "table_id BIGINT, "
	       "event_category VARCHAR NOT NULL, "
	       "change_type VARCHAR NOT NULL, "
	       "original_qualified_name VARCHAR, "
	       "created_at TIMESTAMP WITH TIME ZONE NOT NULL, "
	       "metadata VARCHAR"
	       ")";
}

std::string AuditDdl(const std::string &catalog_name, bool use_state_schema) {
	return "CREATE TABLE IF NOT EXISTS " + StateTable(catalog_name, AUDIT_TABLE, use_state_schema) +
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

std::string SnapshotNotifyChannel(const std::string &catalog_name) {
	std::string out = "ducklake_cdc_snapshot_";
	for (auto c : catalog_name) {
		if (out.size() >= 63) {
			break;
		}
		const auto ch = static_cast<unsigned char>(c);
		out.push_back(std::isalnum(ch) ? static_cast<char>(std::tolower(ch)) : '_');
	}
	return out;
}

bool MetadataBackendIsPostgres(duckdb::Connection &conn, const std::string &catalog_name) {
	auto result = conn.Query("SELECT type FROM duckdb_databases() WHERE database_name = " +
	                         QuoteLiteral("__ducklake_metadata_" + catalog_name) + " LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	const auto type = duckdb::StringUtil::Lower(result->GetValue(0, 0).ToString());
	return type == "postgres" || type == "postgres_scanner";
}

void PostgresExecuteBestEffort(duckdb::Connection &conn, const std::string &catalog_name, const std::string &sql) {
	auto result = conn.Query("CALL postgres_execute(" + QuoteLiteral("__ducklake_metadata_" + catalog_name) + ", " +
	                         QuoteLiteral(sql) + ")");
	(void)result;
}

void InstallPostgresSnapshotNotifyBestEffort(duckdb::Connection &conn, const std::string &catalog_name) {
	if (!MetadataBackendIsPostgres(conn, catalog_name)) {
		return;
	}
	const auto channel = SnapshotNotifyChannel(catalog_name);
	PostgresExecuteBestEffort(conn, catalog_name, "CREATE SCHEMA IF NOT EXISTS " + QuoteIdentifier(STATE_SCHEMA));
	PostgresExecuteBestEffort(
	    conn, catalog_name,
	    "CREATE OR REPLACE FUNCTION " + QuoteIdentifier(STATE_SCHEMA) +
	        ".ducklake_cdc_notify_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_notify(" +
	        QuoteLiteral(channel) + ", NEW.snapshot_id::text); RETURN NEW; END; $$");
	PostgresExecuteBestEffort(
	    conn, catalog_name,
	    "DO $$ BEGIN IF NOT EXISTS ("
	    "SELECT 1 FROM pg_trigger WHERE tgname = 'ducklake_cdc_snapshot_notify' "
	    "AND tgrelid = 'public.ducklake_snapshot'::regclass"
	    ") THEN EXECUTE 'CREATE TRIGGER ducklake_cdc_snapshot_notify AFTER INSERT ON public.ducklake_snapshot "
	    "FOR EACH ROW EXECUTE FUNCTION " +
	        QuoteIdentifier(STATE_SCHEMA) + ".ducklake_cdc_notify_snapshot()'; END IF; END $$");
}

void BootstrapConsumerStateOrThrow(duckdb::ClientContext &context, const std::string &catalog_name) {
	CheckCatalogOrThrow(context, catalog_name);
	duckdb::Connection conn(*context.db);
	auto create_schema = conn.Query("CREATE SCHEMA IF NOT EXISTS " + MetadataDatabase(catalog_name) + "." +
	                                QuoteIdentifier(STATE_SCHEMA));
	const bool use_state_schema = create_schema && !create_schema->HasError();
	ExecuteChecked(conn, ConsumersDdl(catalog_name, use_state_schema));
	ExecuteChecked(conn, ConsumerSubscriptionsDdl(catalog_name, use_state_schema));
	ExecuteChecked(conn, AuditDdl(catalog_name, use_state_schema));
	if (use_state_schema) {
		// Pre-warm the StateSchemaExists cache so subsequent listen calls
		// in this process never have to re-probe the catalog enumerator.
		StateSchemaCacheRemember(MetadataAttachmentCacheKey(conn, catalog_name));
	}
	InstallPostgresSnapshotNotifyBestEffort(conn, catalog_name);
}

//===--------------------------------------------------------------------===//
// Row-scan glue
//===--------------------------------------------------------------------===//

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

void MaterializedResultScanExecute(duckdb::ClientContext &context, duckdb::TableFunctionInput &input,
                                   duckdb::DataChunk &output) {
	auto &state = input.global_state->Cast<MaterializedResultScanState>();
	if (!state.result || state.offset >= state.result->RowCount()) {
		return;
	}
	duckdb::idx_t count = 0;
	while (state.offset < state.result->RowCount() && count < STANDARD_VECTOR_SIZE) {
		if (state.result->ColumnCount() != output.ColumnCount()) {
			throw duckdb::InternalException("Unaligned materialized query result in table function result");
		}
		for (duckdb::idx_t col = 0; col < state.result->ColumnCount(); ++col) {
			output.SetValue(col, count, state.result->GetValue(col, state.offset));
		}
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace duckdb_cdc
