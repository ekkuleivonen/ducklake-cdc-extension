//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// dml.cpp
//
// Implementation of the row-level change-event surface: cdc_events,
// cdc_changes, and cdc_recent_changes. cdc_events and cdc_changes both
// open the consumer's window via `ReadWindow` from `consumer.cpp`
// (acquiring the lease for the duration); cdc_recent_changes is
// stateless and runs `[since_start, current_snapshot]` directly.
//
// `cdc_events` defers per-snapshot table-filter decisions to
// `ChangesTouchConsumerTables` in `ddl.cpp` because that predicate
// walks DuckLake-emitted change tokens and we don't want token
// vocabulary leaking into this file.
//===----------------------------------------------------------------------===//

#include "dml.hpp"

#include "compat_check.hpp"
#include "consumer.hpp"
#include "ddl.hpp"
#include "ducklake_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <algorithm>
#include <sstream>
#include <unordered_set>

namespace duckdb_cdc {

namespace {

//===--------------------------------------------------------------------===//
// cdc_events
//===--------------------------------------------------------------------===//

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

std::vector<std::string> SplitEventTokens(const std::string &changes_made) {
	std::vector<std::string> tokens;
	size_t start = 0;
	while (start < changes_made.size()) {
		const auto comma = changes_made.find(',', start);
		tokens.push_back(changes_made.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
		if (comma == std::string::npos) {
			break;
		}
		start = comma + 1;
	}
	return tokens;
}

bool ParseTableIdToken(const std::string &token, const std::string &prefix, int64_t &table_id) {
	if (token.rfind(prefix, 0) != 0) {
		return false;
	}
	return TryParseInt64(token.substr(prefix.size()), table_id);
}

bool DmlTokenInfo(const std::string &token, int64_t &table_id, std::string &change_type) {
	for (const auto &prefix : {"inserted_into_table:", "tables_inserted_into:", "inlined_insert:"}) {
		if (ParseTableIdToken(token, prefix, table_id)) {
			change_type = "insert";
			return true;
		}
	}
	for (const auto &prefix : {"deleted_from_table:", "tables_deleted_from:", "inlined_delete:"}) {
		if (ParseTableIdToken(token, prefix, table_id)) {
			change_type = "delete";
			return true;
		}
	}
	return false;
}

bool DdlTokenInfo(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                  const std::string &token, int64_t &schema_id, int64_t &table_id, bool &has_table) {
	has_table = false;
	if (token.rfind("created_schema:", 0) == 0 || token.rfind("dropped_schema:", 0) == 0) {
		return false;
	}
	for (const auto &prefix : {"altered_table:", "dropped_table:"}) {
		if (ParseTableIdToken(token, prefix, table_id)) {
			const auto qualified = CurrentQualifiedTableName(conn, catalog_name, table_id, snapshot_id);
			if (!qualified.empty()) {
				int64_t ignored = 0;
				ResolveCurrentTableName(conn, catalog_name, qualified, snapshot_id, schema_id, ignored);
			}
			has_table = true;
			return true;
		}
	}
	return false;
}

bool ChangesTouchSubscriptions(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                               const std::string &changes_made,
                               const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	for (const auto &token : SplitEventTokens(changes_made)) {
		int64_t table_id = 0;
		std::string change_type;
		if (DmlTokenInfo(token, table_id, change_type)) {
			const auto qualified = CurrentQualifiedTableName(conn, catalog_name, table_id, snapshot_id);
			if (qualified.empty()) {
				continue;
			}
			int64_t schema_id = 0;
			int64_t ignored_table = 0;
			ResolveCurrentTableName(conn, catalog_name, qualified, snapshot_id, schema_id, ignored_table);
			for (const auto &subscription : subscriptions) {
				if (subscription.change_type == change_type &&
				    SubscriptionCoversTable(subscription, schema_id, table_id, "dml")) {
					return true;
				}
			}
			continue;
		}
		int64_t schema_id = 0;
		bool has_table = false;
		if (DdlTokenInfo(conn, catalog_name, snapshot_id, token, schema_id, table_id, has_table)) {
			for (const auto &subscription : subscriptions) {
				if (has_table && SubscriptionCoversTable(subscription, schema_id, table_id, "ddl")) {
					return true;
				}
				if (!has_table && subscription.event_category == "ddl" && subscription.scope_kind == "catalog") {
					return true;
				}
			}
		}
	}
	return false;
}

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
	const auto subscriptions = LoadConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);

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
		if (!ChangesTouchSubscriptions(conn, data.catalog_name, snapshot_id, changes_str, subscriptions)) {
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

//===--------------------------------------------------------------------===//
// cdc_changes
//===--------------------------------------------------------------------===//

struct CdcChangesData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	std::string table_name;
	int64_t table_id = -1;
	int64_t schema_id = -1;
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
		result->table_id = table_id;
		result->schema_id = schema_id;
		result->max_snapshots = max_snapshots;
		result->table_column_names = table_column_names;
		result->table_column_types = table_column_types;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

bool ConsumerHasDmlSubscriptions(const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category == "dml") {
			return true;
		}
	}
	return false;
}

std::string DuckLakeTableChangesName(const std::string &catalog_name, const std::string &table_name) {
	if (table_name.rfind(catalog_name + ".", 0) == 0) {
		return table_name;
	}
	return catalog_name + "." + table_name;
}

std::pair<std::string, std::string> SplitQualifiedTableName(const std::string &table_name) {
	const auto dot = table_name.find('.');
	if (dot == std::string::npos) {
		return {"main", table_name};
	}
	return {table_name.substr(0, dot), table_name.substr(dot + 1)};
}

std::string DuckLakeTableChangesCall(const std::string &catalog_name, const std::string &table_name,
                                     int64_t start_snapshot, int64_t end_snapshot) {
	const auto parts = SplitQualifiedTableName(table_name);
	return "ducklake_table_changes(" + QuoteLiteral(catalog_name) + ", " + QuoteLiteral(parts.first) + ", " +
	       QuoteLiteral(parts.second) + ", " + std::to_string(start_snapshot) + ", " + std::to_string(end_snapshot) +
	       ")";
}

bool OriginalNameWasRenamed(const std::vector<ConsumerSubscriptionRow> &subscriptions, const std::string &table_name) {
	for (const auto &subscription : subscriptions) {
		if (subscription.scope_kind == "table" && subscription.status == "renamed" &&
		    !subscription.original_qualified_name.IsNull() && subscription.original_qualified_name.ToString() == table_name) {
			return true;
		}
	}
	return false;
}

void ResolveSingleTableSubscription(const std::vector<ConsumerSubscriptionRow> &subscriptions, int64_t &schema_id,
                                    int64_t &table_id, std::string &table_name) {
	std::unordered_set<int64_t> table_ids;
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category == "dml" && subscription.scope_kind == "table" &&
		    subscription.status != "dropped" && !subscription.table_id.IsNull()) {
			table_ids.insert(subscription.table_id.GetValue<int64_t>());
			if (!subscription.schema_id.IsNull()) {
				schema_id = subscription.schema_id.GetValue<int64_t>();
			}
			if (!subscription.current_qualified_name.IsNull()) {
				table_name = subscription.current_qualified_name.ToString();
			}
		}
	}
	if (table_ids.size() != 1) {
		throw duckdb::InvalidInputException("cdc_changes requires table_id or table_name for multi-table consumers");
	}
	table_id = *table_ids.begin();
}

duckdb::unique_ptr<duckdb::FunctionData> CdcChangesBind(duckdb::ClientContext &context,
                                                        duckdb::TableFunctionBindInput &input,
                                                        duckdb::vector<duckdb::LogicalType> &return_types,
                                                        duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2 && input.inputs.size() != 3) {
		throw duckdb::BinderException("cdc_changes requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<CdcChangesData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	if (input.inputs.size() == 3) {
		result->table_name = GetStringArg(input.inputs[2], "table");
	}
	auto table_name_entry = input.named_parameters.find("table_name");
	if (table_name_entry != input.named_parameters.end() && !table_name_entry->second.IsNull()) {
		result->table_name = table_name_entry->second.GetValue<std::string>();
	}
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
	//   * `last_committed_snapshot + 1` (the historical strategy): assumes
	//     snapshot ids are dense and ignores `max_snapshots` and schema-boundary
	//     truncation. Bind must match the exact runtime end snapshot instead.
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
	const auto current_snapshot = CurrentSnapshot(conn, result->catalog_name);
	const auto subscriptions = LoadConsumerSubscriptions(conn, result->catalog_name, result->consumer_name);
	auto table_id_entry = input.named_parameters.find("table_id");
	if (table_id_entry != input.named_parameters.end() && !table_id_entry->second.IsNull()) {
		result->table_id = table_id_entry->second.GetValue<int64_t>();
		result->table_name = CurrentQualifiedTableName(conn, result->catalog_name, result->table_id, current_snapshot);
		if (result->table_name.empty()) {
			throw duckdb::InvalidInputException("cdc_changes: table_id %lld is not active",
			                                    static_cast<long long>(result->table_id));
		}
		int64_t ignored_table_id = 0;
		ResolveCurrentTableName(conn, result->catalog_name, result->table_name, current_snapshot, result->schema_id,
		                        ignored_table_id);
	} else if (!result->table_name.empty()) {
		if (result->table_name.find('.') == std::string::npos) {
			result->table_name = std::string("main.") + result->table_name;
		}
		if (!ResolveCurrentTableName(conn, result->catalog_name, result->table_name, current_snapshot, result->schema_id,
		                             result->table_id)) {
			if (OriginalNameWasRenamed(subscriptions, result->table_name)) {
				throw duckdb::InvalidInputException("cdc_changes: table '%s' was renamed; use the current name",
				                                    result->table_name);
			}
			throw duckdb::InvalidInputException("cdc_changes failed to resolve table '%s'", result->table_name);
		}
	} else {
		ResolveSingleTableSubscription(subscriptions, result->schema_id, result->table_id, result->table_name);
	}
	const auto matching_change_types = MatchingDmlChangeTypes(subscriptions, result->schema_id, result->table_id);
	if (matching_change_types.empty() && ConsumerHasDmlSubscriptions(subscriptions)) {
		throw duckdb::InvalidInputException("cdc_changes: table '%s' is not covered by consumer '%s' subscriptions",
		                                    result->table_name, result->consumer_name);
	}
	int64_t probe_snapshot;
	{
		auto consumer_row = LoadConsumerOrThrow(conn, result->catalog_name, result->consumer_name);
		const auto last_snapshot = consumer_row.last_committed_snapshot;
		const auto current_snapshot = CurrentSnapshot(conn, result->catalog_name);
		const auto first_snapshot = FirstSnapshotAfter(conn, result->catalog_name, last_snapshot, current_snapshot);
		if (first_snapshot == -1) {
			// Consumer is caught up to head (or to the last external snap):
			// the next runtime range will be empty. Bind to the latest
			// known schema so a brand-new consumer or a fully-drained
			// consumer can still resolve columns.
			probe_snapshot = current_snapshot > 0 ? current_snapshot : CurrentSnapshot(conn, result->catalog_name);
		} else {
			const auto start_snapshot = first_snapshot;
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
	const auto table_name_at_probe = CurrentQualifiedTableName(conn, result->catalog_name, result->table_id, probe_snapshot);
	if (table_name_at_probe.empty() || table_name_at_probe != result->table_name) {
		probe_snapshot = current_snapshot;
	}
	auto probe = conn.Query("SELECT * FROM " +
	                        DuckLakeTableChangesCall(result->catalog_name, result->table_name, probe_snapshot,
	                                                 probe_snapshot) +
	                        " LIMIT 0");
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
	auto effective_end_snapshot = end_snapshot;
	const auto table_name_at_end = CurrentQualifiedTableName(conn, data.catalog_name, data.table_id, effective_end_snapshot);
	if (table_name_at_end.empty() || table_name_at_end != data.table_name) {
		effective_end_snapshot = CurrentSnapshot(conn, data.catalog_name);
	}
	const auto subscriptions = LoadConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);
	const auto change_types = MatchingDmlChangeTypes(subscriptions, data.schema_id, data.table_id);
	if (change_types.empty()) {
		return std::move(result);
	}

	std::ostringstream column_list;
	column_list << "tc.snapshot_id, tc.rowid, tc.change_type";
	for (const auto &col_name : data.table_column_names) {
		column_list << ", tc." << QuoteIdentifier(col_name);
	}

	std::ostringstream where_clause;
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
	    ", s.snapshot_time, c.author, c.commit_message, c.commit_extra_info, " + std::to_string(effective_end_snapshot) +
	    " AS next_snapshot_id FROM " +
	    DuckLakeTableChangesCall(data.catalog_name, data.table_name, start_snapshot, effective_end_snapshot) +
	    " tc LEFT JOIN " + MetadataTable(data.catalog_name, "ducklake_snapshot") +
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

//===--------------------------------------------------------------------===//
// cdc_recent_changes
//===--------------------------------------------------------------------===//

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

} // namespace

void RegisterDmlFunctions(duckdb::ExtensionLoader &loader) {
	for (const auto &name : {"cdc_events", "ducklake_cdc_events"}) {
		duckdb::TableFunction events_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, CdcEventsBind, CdcEventsInit);
		events_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		loader.RegisterFunction(events_function);
	}

	for (const auto &name : {"cdc_changes", "ducklake_cdc_changes"}) {
		duckdb::TableFunction changes_function_new(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, CdcChangesBind, CdcChangesInit);
		changes_function_new.named_parameters["table_id"] = duckdb::LogicalType::BIGINT;
		changes_function_new.named_parameters["table_name"] = duckdb::LogicalType::VARCHAR;
		changes_function_new.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		loader.RegisterFunction(changes_function_new);
		duckdb::TableFunction changes_function(name,
		                                       duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR,
		                                                                            duckdb::LogicalType::VARCHAR,
		                                                                            duckdb::LogicalType::VARCHAR},
		                                       RowScanExecute, CdcChangesBind, CdcChangesInit);
		changes_function.named_parameters["table_id"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["table_name"] = duckdb::LogicalType::VARCHAR;
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
}

} // namespace duckdb_cdc
