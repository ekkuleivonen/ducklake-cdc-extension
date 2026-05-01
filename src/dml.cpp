//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// dml.cpp
//
// DML stream implementation. The generic DML functions return stable
// multi-table rows with JSON payload text, while typed table functions expose
// one subscribed table's native DuckDB columns.
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
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

namespace duckdb_cdc {

namespace {

//===--------------------------------------------------------------------===//
// DML ticks
//===--------------------------------------------------------------------===//

struct DmlTicksData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t max_snapshots;
	bool auto_commit = false;
	bool listen = false;
	int64_t timeout_ms = DEFAULT_WAIT_TIMEOUT_MS;
	bool explicit_window = false;
	int64_t start_snapshot = -1;
	int64_t end_snapshot = -1;
	bool stateless = false;
	int64_t from_snapshot = -1;
	int64_t to_snapshot = -1;
	std::vector<int64_t> table_ids;
	std::vector<std::string> table_names;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<DmlTicksData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->max_snapshots = max_snapshots;
		result->auto_commit = auto_commit;
		result->listen = listen;
		result->timeout_ms = timeout_ms;
		result->explicit_window = explicit_window;
		result->start_snapshot = start_snapshot;
		result->end_snapshot = end_snapshot;
		result->stateless = stateless;
		result->from_snapshot = from_snapshot;
		result->to_snapshot = to_snapshot;
		result->table_ids = table_ids;
		result->table_names = table_names;
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

int64_t TimeoutMsParameter(duckdb::TableFunctionBindInput &input);

duckdb::Value BigIntListValue(const std::vector<int64_t> &values) {
	duckdb::vector<duckdb::Value> children;
	children.reserve(values.size());
	for (const auto value : values) {
		children.push_back(duckdb::Value::BIGINT(value));
	}
	return duckdb::Value::LIST(duckdb::LogicalType::BIGINT, children);
}

std::pair<std::string, std::string> SplitQualifiedTableName(const std::string &table_name);
std::string DuckLakeTableChangesCall(const std::string &catalog_name, const std::string &table_name,
                                     int64_t start_snapshot, int64_t end_snapshot);
std::vector<std::string> StringListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name);
std::vector<int64_t> Int64ListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name);

void DmlTicksReturnTypes(duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names,
                         bool stateful) {
	if (stateful) {
		names = {"consumer_name", "start_snapshot", "end_snapshot"};
		return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	} else {
		names.clear();
		return_types.clear();
	}
	for (const auto &name : {"snapshot_id", "snapshot_time", "schema_version", "table_ids", "insert_count",
	                         "update_count", "delete_count", "change_count"}) {
		names.push_back(name);
	}
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT));
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::BIGINT);
}

duckdb::unique_ptr<duckdb::FunctionData> DmlTicksBindBase(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names, bool listen) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_dml_ticks_read/listen requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<DmlTicksData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->max_snapshots = MaxSnapshotsParameter(input);
	result->listen = listen;
	result->timeout_ms = TimeoutMsParameter(input);
	auto auto_commit_entry = input.named_parameters.find("auto_commit");
	if (auto_commit_entry != input.named_parameters.end() && !auto_commit_entry->second.IsNull()) {
		result->auto_commit = auto_commit_entry->second.GetValue<bool>();
	}
	auto start_snapshot_entry = input.named_parameters.find("start_snapshot");
	auto end_snapshot_entry = input.named_parameters.find("end_snapshot");
	const bool has_start_snapshot =
	    start_snapshot_entry != input.named_parameters.end() && !start_snapshot_entry->second.IsNull();
	const bool has_end_snapshot =
	    end_snapshot_entry != input.named_parameters.end() && !end_snapshot_entry->second.IsNull();
	if (has_start_snapshot != has_end_snapshot) {
		throw duckdb::BinderException(
		    "cdc_dml_ticks_read requires both start_snapshot and end_snapshot when either is set");
	}
	if (has_start_snapshot) {
		result->explicit_window = true;
		result->start_snapshot = start_snapshot_entry->second.GetValue<int64_t>();
		result->end_snapshot = end_snapshot_entry->second.GetValue<int64_t>();
	}

	DmlTicksReturnTypes(return_types, names, true);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::FunctionData> DmlTicksReadBind(duckdb::ClientContext &context,
                                                          duckdb::TableFunctionBindInput &input,
                                                          duckdb::vector<duckdb::LogicalType> &return_types,
                                                          duckdb::vector<duckdb::string> &names) {
	return DmlTicksBindBase(context, input, return_types, names, false);
}

duckdb::unique_ptr<duckdb::FunctionData> DmlTicksListenBind(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names) {
	return DmlTicksBindBase(context, input, return_types, names, true);
}

void AppendDmlTickRows(duckdb::Connection &conn, const std::string &catalog_name, const std::string &consumer_name,
                       int64_t start_snapshot, int64_t end_snapshot,
                       const std::vector<ConsumerSubscriptionRow> *subscriptions,
                       const std::unordered_set<int64_t> *filter_table_ids,
                       std::vector<std::vector<duckdb::Value>> &out, bool stateful) {
	auto rows =
	    conn.Query(std::string("SELECT s.snapshot_id, s.snapshot_time, s.schema_version, c.changes_made FROM ") +
	               MetadataTable(catalog_name, "ducklake_snapshot") + " s JOIN " +
	               MetadataTable(catalog_name, "ducklake_snapshot_changes") +
	               " c USING (snapshot_id) WHERE s.snapshot_id BETWEEN " + std::to_string(start_snapshot) + " AND " +
	               std::to_string(end_snapshot) + " ORDER BY s.snapshot_id ASC");
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, rows ? rows->GetError() : "DML ticks scan failed");
	}
	for (duckdb::idx_t i = 0; i < rows->RowCount(); ++i) {
		const auto snapshot_id = rows->GetValue(0, i).GetValue<int64_t>();
		const auto changes_value = rows->GetValue(3, i);
		const auto changes = changes_value.IsNull() ? std::string() : changes_value.ToString();
		std::unordered_set<int64_t> touched_table_ids;
		std::unordered_set<int64_t> counted_table_ids;
		int64_t insert_count = 0;
		int64_t update_count = 0;
		int64_t delete_count = 0;
		for (const auto &token : SplitEventTokens(changes)) {
			int64_t table_id = 0;
			std::string change_type;
			if (!DmlTokenInfo(token, table_id, change_type)) {
				continue;
			}
			if (filter_table_ids != nullptr && filter_table_ids->count(table_id) == 0) {
				continue;
			}
			const auto qualified = CurrentQualifiedTableName(conn, catalog_name, table_id, snapshot_id);
			if (qualified.empty()) {
				continue;
			}
			int64_t schema_id = 0;
			int64_t ignored_table = 0;
			ResolveCurrentTableName(conn, catalog_name, qualified, snapshot_id, schema_id, ignored_table);
			if (subscriptions != nullptr) {
				bool covered = false;
				for (const auto &subscription : *subscriptions) {
					if (subscription.event_category == "dml" &&
					    SubscriptionCoversTable(subscription, schema_id, table_id, "dml")) {
						covered = true;
						break;
					}
				}
				if (!covered) {
					continue;
				}
			}
			touched_table_ids.insert(table_id);
			if (!counted_table_ids.insert(table_id).second) {
				continue;
			}
			std::vector<std::string> counted_change_types = {"insert", "update_preimage", "update_postimage", "delete"};
			if (subscriptions != nullptr) {
				counted_change_types = MatchingDmlChangeTypes(*subscriptions, schema_id, table_id);
			}
			std::ostringstream type_filter;
			if (!counted_change_types.empty()) {
				type_filter << " WHERE change_type IN (";
				for (size_t type_idx = 0; type_idx < counted_change_types.size(); ++type_idx) {
					if (type_idx > 0) {
						type_filter << ", ";
					}
					type_filter << QuoteLiteral(counted_change_types[type_idx]);
				}
				type_filter << ")";
			}
			auto counts = conn.Query("SELECT change_type, count(*) FROM " +
			                         DuckLakeTableChangesCall(catalog_name, qualified, snapshot_id, snapshot_id) +
			                         type_filter.str() + " GROUP BY change_type");
			if (!counts || counts->HasError()) {
				throw duckdb::Exception(duckdb::ExceptionType::INVALID,
				                        counts ? counts->GetError() : "DML tick count scan failed");
			}
			for (duckdb::idx_t count_idx = 0; count_idx < counts->RowCount(); ++count_idx) {
				const auto counted_type = counts->GetValue(0, count_idx).ToString();
				const auto counted_rows = counts->GetValue(1, count_idx).GetValue<int64_t>();
				if (counted_type == "insert") {
					insert_count += counted_rows;
				} else if (counted_type == "update_preimage" || counted_type == "update_postimage") {
					update_count += counted_rows;
				} else if (counted_type == "delete") {
					delete_count += counted_rows;
				}
			}
		}
		if (touched_table_ids.empty()) {
			continue;
		}
		std::vector<int64_t> table_ids(touched_table_ids.begin(), touched_table_ids.end());
		std::sort(table_ids.begin(), table_ids.end());
		std::vector<duckdb::Value> row;
		if (stateful) {
			row.push_back(duckdb::Value(consumer_name));
			row.push_back(duckdb::Value::BIGINT(start_snapshot));
			row.push_back(duckdb::Value::BIGINT(end_snapshot));
		}
		row.push_back(rows->GetValue(0, i));
		row.push_back(rows->GetValue(1, i));
		row.push_back(rows->GetValue(2, i));
		row.push_back(BigIntListValue(table_ids));
		row.push_back(duckdb::Value::BIGINT(insert_count));
		row.push_back(duckdb::Value::BIGINT(update_count));
		row.push_back(duckdb::Value::BIGINT(delete_count));
		row.push_back(duckdb::Value::BIGINT(insert_count + update_count + delete_count));
		out.push_back(std::move(row));
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DmlTicksInit(duckdb::ClientContext &context,
                                                                  duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<DmlTicksData>();
	auto max_snapshots = data.max_snapshots;
	if (data.listen && !data.explicit_window) {
		auto ready = WaitForConsumerSnapshot(context, data.catalog_name, data.consumer_name, data.timeout_ms);
		if (ready.empty() || ready[0].IsNull()) {
			return std::move(result);
		}
		if (ready.size() > 1 && !ready[1].IsNull()) {
			max_snapshots = std::max<int64_t>(max_snapshots, ready[1].GetValue<int64_t>());
		}
	}

	duckdb::Connection conn(*context.db);
	int64_t start_snapshot;
	int64_t end_snapshot;
	bool has_changes;
	if (data.explicit_window) {
		start_snapshot = data.start_snapshot;
		end_snapshot = data.end_snapshot;
		has_changes = end_snapshot >= start_snapshot;
	} else {
		CdcWindowData window_data;
		window_data.catalog_name = data.catalog_name;
		window_data.consumer_name = data.consumer_name;
		window_data.max_snapshots = max_snapshots;
		auto window = ReadWindow(context, window_data);
		start_snapshot = window[0].GetValue<int64_t>();
		end_snapshot = window[1].GetValue<int64_t>();
		has_changes = window[2].GetValue<bool>();
	}
	if (!has_changes) {
		return std::move(result);
	}
	const auto subscriptions = LoadConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);
	AppendDmlTickRows(conn, data.catalog_name, data.consumer_name, start_snapshot, end_snapshot, &subscriptions,
	                  nullptr, result->rows, true);
	if (data.auto_commit && !data.explicit_window) {
		CommitConsumerSnapshot(context, data.catalog_name, data.consumer_name, end_snapshot);
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Generic DML changes
//===--------------------------------------------------------------------===//

struct GenericDmlChangesData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	int64_t max_snapshots;
	bool explicit_window = false;
	int64_t start_snapshot = -1;
	int64_t end_snapshot = -1;
	bool auto_commit = false;
	bool listen = false;
	int64_t timeout_ms = DEFAULT_WAIT_TIMEOUT_MS;
	bool stateless = false;
	int64_t from_snapshot = -1;
	int64_t to_snapshot = -1;
	std::vector<int64_t> table_ids;
	std::vector<std::string> table_names;
	std::vector<std::string> change_types;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<GenericDmlChangesData>();
		result->catalog_name = catalog_name;
		result->consumer_name = consumer_name;
		result->max_snapshots = max_snapshots;
		result->explicit_window = explicit_window;
		result->start_snapshot = start_snapshot;
		result->end_snapshot = end_snapshot;
		result->auto_commit = auto_commit;
		result->listen = listen;
		result->timeout_ms = timeout_ms;
		result->stateless = stateless;
		result->from_snapshot = from_snapshot;
		result->to_snapshot = to_snapshot;
		result->table_ids = table_ids;
		result->table_names = table_names;
		result->change_types = change_types;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

std::vector<std::string> StringListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name) {
	std::vector<std::string> out;
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return out;
	}
	for (const auto &child : duckdb::ListValue::GetChildren(entry->second)) {
		if (!child.IsNull()) {
			out.push_back(child.GetValue<std::string>());
		}
	}
	return out;
}

std::vector<int64_t> Int64ListNamedParameter(duckdb::TableFunctionBindInput &input, const std::string &name) {
	std::vector<int64_t> out;
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return out;
	}
	for (const auto &child : duckdb::ListValue::GetChildren(entry->second)) {
		if (!child.IsNull()) {
			out.push_back(child.GetValue<int64_t>());
		}
	}
	return out;
}

std::vector<std::string> ChangeTypesNamedParameter(duckdb::TableFunctionBindInput &input) {
	auto values = StringListNamedParameter(input, "change_types");
	if (values.empty()) {
		values.push_back("*");
	}
	std::vector<std::string> out;
	for (const auto &value : values) {
		if (value == "*") {
			for (const auto &kind : {"insert", "update_preimage", "update_postimage", "delete"}) {
				out.push_back(kind);
			}
		} else {
			out.push_back(value);
		}
	}
	std::sort(out.begin(), out.end());
	out.erase(std::unique(out.begin(), out.end()), out.end());
	return out;
}

void GenericDmlReturnTypes(duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names,
                           bool stateful) {
	if (stateful) {
		names = {"consumer_name", "start_snapshot", "end_snapshot"};
		return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT};
	} else {
		names.clear();
		return_types.clear();
	}
	for (const auto &name : {"snapshot_id", "snapshot_time", "schema_id", "schema_name", "table_id", "table_name",
	                         "rowid", "change_type", "values", "author", "commit_message", "commit_extra_info"}) {
		names.push_back(name);
	}
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
	return_types.push_back(duckdb::LogicalType::VARCHAR);
}

duckdb::unique_ptr<duckdb::FunctionData> GenericDmlBindBase(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names, bool listen) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_dml_changes_read/listen requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<GenericDmlChangesData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->max_snapshots = MaxSnapshotsParameter(input);
	result->listen = listen;
	result->timeout_ms = TimeoutMsParameter(input);
	result->change_types = ChangeTypesNamedParameter(input);
	auto auto_commit_entry = input.named_parameters.find("auto_commit");
	if (auto_commit_entry != input.named_parameters.end() && !auto_commit_entry->second.IsNull()) {
		result->auto_commit = auto_commit_entry->second.GetValue<bool>();
	}
	auto start_snapshot_entry = input.named_parameters.find("start_snapshot");
	auto end_snapshot_entry = input.named_parameters.find("end_snapshot");
	const bool has_start_snapshot =
	    start_snapshot_entry != input.named_parameters.end() && !start_snapshot_entry->second.IsNull();
	const bool has_end_snapshot =
	    end_snapshot_entry != input.named_parameters.end() && !end_snapshot_entry->second.IsNull();
	if (has_start_snapshot != has_end_snapshot) {
		throw duckdb::BinderException(
		    "cdc_dml_changes_read requires both start_snapshot and end_snapshot when either is set");
	}
	if (has_start_snapshot) {
		result->explicit_window = true;
		result->start_snapshot = start_snapshot_entry->second.GetValue<int64_t>();
		result->end_snapshot = end_snapshot_entry->second.GetValue<int64_t>();
	}
	GenericDmlReturnTypes(return_types, names, true);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::FunctionData> GenericDmlReadBind(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names) {
	return GenericDmlBindBase(context, input, return_types, names, false);
}

duckdb::unique_ptr<duckdb::FunctionData> GenericDmlListenBind(duckdb::ClientContext &context,
                                                              duckdb::TableFunctionBindInput &input,
                                                              duckdb::vector<duckdb::LogicalType> &return_types,
                                                              duckdb::vector<duckdb::string> &names) {
	return GenericDmlBindBase(context, input, return_types, names, true);
}

std::string JsonPayloadExpression(const std::vector<std::string> &column_names) {
	if (column_names.empty()) {
		return "'{}'";
	}
	std::ostringstream out;
	out << "json_object(";
	for (size_t i = 0; i < column_names.size(); ++i) {
		if (i > 0) {
			out << ", ";
		}
		out << QuoteLiteral(column_names[i]) << ", tc." << QuoteIdentifier(column_names[i]);
	}
	out << ")::VARCHAR";
	return out.str();
}

void AppendGenericDmlTableRows(duckdb::Connection &conn, const GenericDmlChangesData &data,
                               const std::string &consumer_name, int64_t start_snapshot, int64_t end_snapshot,
                               int64_t schema_id, int64_t table_id, const std::string &table_name,
                               const std::vector<std::string> &change_types,
                               std::vector<std::vector<duckdb::Value>> &out, bool stateful) {
	auto probe =
	    conn.Query("SELECT * FROM " +
	               DuckLakeTableChangesCall(data.catalog_name, table_name, end_snapshot, end_snapshot) + " LIMIT 0");
	if (!probe || probe->HasError()) {
		throw duckdb::InvalidInputException("cdc_dml_changes failed to resolve table '%s'", table_name);
	}
	std::vector<std::string> column_names;
	for (duckdb::idx_t i = 3; i < probe->ColumnCount(); ++i) {
		column_names.push_back(probe->ColumnName(i));
	}
	const auto parts = SplitQualifiedTableName(table_name);
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
	auto rows = conn.Query(
	    std::string("SELECT tc.snapshot_id, s.snapshot_time, tc.rowid, tc.change_type, ") +
	    JsonPayloadExpression(column_names) + ", c.author, c.commit_message, c.commit_extra_info FROM " +
	    DuckLakeTableChangesCall(data.catalog_name, table_name, start_snapshot, end_snapshot) + " tc LEFT JOIN " +
	    MetadataTable(data.catalog_name, "ducklake_snapshot") + " s ON s.snapshot_id = tc.snapshot_id LEFT JOIN " +
	    MetadataTable(data.catalog_name, "ducklake_snapshot_changes") + " c ON c.snapshot_id = tc.snapshot_id" +
	    where_clause.str() + " ORDER BY tc.snapshot_id ASC, tc.rowid ASC");
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "cdc_dml_changes scan failed");
	}
	for (duckdb::idx_t row_idx = 0; row_idx < rows->RowCount(); ++row_idx) {
		std::vector<duckdb::Value> row;
		if (stateful) {
			row.push_back(duckdb::Value(consumer_name));
			row.push_back(duckdb::Value::BIGINT(start_snapshot));
			row.push_back(duckdb::Value::BIGINT(end_snapshot));
		}
		row.push_back(rows->GetValue(0, row_idx));
		row.push_back(rows->GetValue(1, row_idx));
		row.push_back(duckdb::Value::BIGINT(schema_id));
		row.push_back(duckdb::Value(parts.first));
		row.push_back(duckdb::Value::BIGINT(table_id));
		row.push_back(duckdb::Value(table_name));
		for (duckdb::idx_t col_idx = 2; col_idx < rows->ColumnCount(); ++col_idx) {
			row.push_back(rows->GetValue(col_idx, row_idx));
		}
		out.push_back(std::move(row));
	}
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> GenericDmlInit(duckdb::ClientContext &context,
                                                                    duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<GenericDmlChangesData>();
	auto max_snapshots = data.max_snapshots;
	if (data.listen && !data.explicit_window) {
		auto ready = WaitForConsumerSnapshot(context, data.catalog_name, data.consumer_name, data.timeout_ms);
		if (ready.empty() || ready[0].IsNull()) {
			return std::move(result);
		}
		if (ready.size() > 1 && !ready[1].IsNull()) {
			max_snapshots = std::max<int64_t>(max_snapshots, ready[1].GetValue<int64_t>());
		}
	}
	int64_t start_snapshot;
	int64_t end_snapshot;
	bool has_changes;
	if (data.explicit_window) {
		start_snapshot = data.start_snapshot;
		end_snapshot = data.end_snapshot;
		has_changes = end_snapshot >= start_snapshot;
	} else {
		CdcWindowData window_data;
		window_data.catalog_name = data.catalog_name;
		window_data.consumer_name = data.consumer_name;
		window_data.max_snapshots = max_snapshots;
		auto window = ReadWindow(context, window_data);
		start_snapshot = window[0].GetValue<int64_t>();
		end_snapshot = window[1].GetValue<int64_t>();
		has_changes = window[2].GetValue<bool>();
	}
	if (!has_changes) {
		return std::move(result);
	}
	duckdb::Connection conn(*context.db);
	const auto subscriptions = LoadConsumerSubscriptions(conn, data.catalog_name, data.consumer_name);
	std::unordered_set<int64_t> visited;
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category != "dml" || subscription.table_id.IsNull() || subscription.schema_id.IsNull()) {
			continue;
		}
		const auto table_id = subscription.table_id.GetValue<int64_t>();
		if (!visited.insert(table_id).second) {
			continue;
		}
		const auto table_name = CurrentQualifiedTableName(conn, data.catalog_name, table_id, end_snapshot);
		if (table_name.empty()) {
			continue;
		}
		const auto change_types =
		    MatchingDmlChangeTypes(subscriptions, subscription.schema_id.GetValue<int64_t>(), table_id);
		AppendGenericDmlTableRows(conn, data, data.consumer_name, start_snapshot, end_snapshot,
		                          subscription.schema_id.GetValue<int64_t>(), table_id, table_name, change_types,
		                          result->rows, true);
	}
	std::sort(result->rows.begin(), result->rows.end(),
	          [](const std::vector<duckdb::Value> &lhs, const std::vector<duckdb::Value> &rhs) {
		          return lhs[3].GetValue<int64_t>() < rhs[3].GetValue<int64_t>();
	          });
	if (data.auto_commit && !data.explicit_window) {
		CommitConsumerSnapshot(context, data.catalog_name, data.consumer_name, end_snapshot);
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Typed table DML changes
//===--------------------------------------------------------------------===//

struct CdcChangesData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string consumer_name;
	std::string table_name;
	int64_t table_id = -1;
	int64_t schema_id = -1;
	int64_t max_snapshots;
	bool explicit_window = false;
	int64_t start_snapshot = -1;
	int64_t end_snapshot = -1;
	int64_t probe_schema_version = -1;
	std::vector<std::string> change_types;
	bool auto_commit = false;
	bool listen = false;
	int64_t timeout_ms = DEFAULT_WAIT_TIMEOUT_MS;
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
		result->explicit_window = explicit_window;
		result->start_snapshot = start_snapshot;
		result->end_snapshot = end_snapshot;
		result->probe_schema_version = probe_schema_version;
		result->change_types = change_types;
		result->auto_commit = auto_commit;
		result->listen = listen;
		result->timeout_ms = timeout_ms;
		result->table_column_names = table_column_names;
		result->table_column_types = table_column_types;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

struct TableChangesSchemaCacheEntry {
	std::vector<std::string> column_names;
	std::vector<duckdb::LogicalType> column_types;
};

std::mutex TABLE_CHANGES_SCHEMA_CACHE_MUTEX;
std::unordered_map<std::string, TableChangesSchemaCacheEntry> TABLE_CHANGES_SCHEMA_CACHE;

std::string TableChangesSchemaCacheKey(const std::string &catalog_name, int64_t table_id, int64_t schema_version) {
	return catalog_name + ":" + std::to_string(table_id) + ":" + std::to_string(schema_version);
}

bool ConsumerHasDmlSubscriptions(const std::vector<ConsumerSubscriptionRow> &subscriptions) {
	for (const auto &subscription : subscriptions) {
		if (subscription.event_category == "dml") {
			return true;
		}
	}
	return false;
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
		    !subscription.original_qualified_name.IsNull() &&
		    subscription.original_qualified_name.ToString() == table_name) {
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
		throw duckdb::InvalidInputException(
		    "cdc_dml_table_changes_read requires table_id or table_name for multi-table consumers");
	}
	table_id = *table_ids.begin();
}

int64_t TimeoutMsParameter(duckdb::TableFunctionBindInput &input) {
	auto entry = input.named_parameters.find("timeout_ms");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return DEFAULT_WAIT_TIMEOUT_MS;
	}
	return entry->second.GetValue<int64_t>();
}

duckdb::unique_ptr<duckdb::FunctionData> CdcChangesBindBase(duckdb::ClientContext &context,
                                                            duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types,
                                                            duckdb::vector<duckdb::string> &names, bool listen) {
	if (input.inputs.size() != 2) {
		throw duckdb::BinderException("cdc_dml_table_changes_read requires catalog and consumer name");
	}
	auto result = duckdb::make_uniq<CdcChangesData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->consumer_name = GetStringArg(input.inputs[1], "consumer name");
	result->listen = listen;
	result->timeout_ms = TimeoutMsParameter(input);
	auto auto_commit_entry = input.named_parameters.find("auto_commit");
	if (auto_commit_entry != input.named_parameters.end() && !auto_commit_entry->second.IsNull()) {
		result->auto_commit = auto_commit_entry->second.GetValue<bool>();
	}
	auto table_name_entry = input.named_parameters.find("table_name");
	if (table_name_entry != input.named_parameters.end() && !table_name_entry->second.IsNull()) {
		result->table_name = table_name_entry->second.GetValue<std::string>();
	}
	result->max_snapshots = MaxSnapshotsParameter(input);
	auto start_snapshot_entry = input.named_parameters.find("start_snapshot");
	auto end_snapshot_entry = input.named_parameters.find("end_snapshot");
	const bool has_start_snapshot =
	    start_snapshot_entry != input.named_parameters.end() && !start_snapshot_entry->second.IsNull();
	const bool has_end_snapshot =
	    end_snapshot_entry != input.named_parameters.end() && !end_snapshot_entry->second.IsNull();
	if (has_start_snapshot != has_end_snapshot) {
		throw duckdb::BinderException(
		    "cdc_dml_table_changes_read requires both start_snapshot and end_snapshot when either is set");
	}
	if (has_start_snapshot) {
		result->explicit_window = true;
		result->start_snapshot = start_snapshot_entry->second.GetValue<int64_t>();
		result->end_snapshot = end_snapshot_entry->second.GetValue<int64_t>();
	}

	CheckCatalogOrThrow(context, result->catalog_name);

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
	const auto reference_snapshot =
	    result->explicit_window
	        ? (result->end_snapshot >= result->start_snapshot ? result->end_snapshot : result->start_snapshot)
	        : CurrentSnapshot(conn, result->catalog_name);
	const auto subscriptions = LoadConsumerSubscriptions(conn, result->catalog_name, result->consumer_name);
	auto table_id_entry = input.named_parameters.find("table_id");
	if (table_id_entry != input.named_parameters.end() && !table_id_entry->second.IsNull()) {
		result->table_id = table_id_entry->second.GetValue<int64_t>();
		result->table_name =
		    CurrentQualifiedTableName(conn, result->catalog_name, result->table_id, reference_snapshot);
		if (result->table_name.empty()) {
			throw duckdb::InvalidInputException("cdc_dml_table_changes_read: table_id %lld is not active",
			                                    static_cast<long long>(result->table_id));
		}
		int64_t ignored_table_id = 0;
		ResolveCurrentTableName(conn, result->catalog_name, result->table_name, reference_snapshot, result->schema_id,
		                        ignored_table_id);
	} else if (!result->table_name.empty()) {
		if (result->table_name.find('.') == std::string::npos) {
			result->table_name = std::string("main.") + result->table_name;
		}
		if (!ResolveCurrentTableName(conn, result->catalog_name, result->table_name, reference_snapshot,
		                             result->schema_id, result->table_id)) {
			if (OriginalNameWasRenamed(subscriptions, result->table_name)) {
				throw duckdb::InvalidInputException(
				    "cdc_dml_table_changes_read: table '%s' was renamed; use the current name", result->table_name);
			}
			throw duckdb::InvalidInputException("cdc_dml_table_changes_read failed to resolve table '%s'",
			                                    result->table_name);
		}
	} else {
		ResolveSingleTableSubscription(subscriptions, result->schema_id, result->table_id, result->table_name);
	}
	const auto matching_change_types = MatchingDmlChangeTypes(subscriptions, result->schema_id, result->table_id);
	if (matching_change_types.empty() && ConsumerHasDmlSubscriptions(subscriptions)) {
		throw duckdb::InvalidInputException(
		    "cdc_dml_table_changes_read: table '%s' is not covered by consumer '%s' subscriptions", result->table_name,
		    result->consumer_name);
	}
	result->change_types = matching_change_types;
	int64_t probe_snapshot;
	if (result->explicit_window) {
		probe_snapshot = result->end_snapshot >= result->start_snapshot ? result->end_snapshot : result->start_snapshot;
	} else {
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
	if (!result->explicit_window) {
		const auto table_name_at_probe =
		    CurrentQualifiedTableName(conn, result->catalog_name, result->table_id, probe_snapshot);
		if (table_name_at_probe.empty() || table_name_at_probe != result->table_name) {
			probe_snapshot = reference_snapshot;
		}
	}
	result->probe_schema_version = ResolveSchemaVersion(conn, result->catalog_name, probe_snapshot);
	const auto schema_cache_key =
	    TableChangesSchemaCacheKey(result->catalog_name, result->table_id, result->probe_schema_version);
	{
		std::lock_guard<std::mutex> guard(TABLE_CHANGES_SCHEMA_CACHE_MUTEX);
		const auto entry = TABLE_CHANGES_SCHEMA_CACHE.find(schema_cache_key);
		if (entry != TABLE_CHANGES_SCHEMA_CACHE.end()) {
			result->table_column_names = entry->second.column_names;
			result->table_column_types = entry->second.column_types;
		}
	}
	if (result->table_column_names.empty() && result->table_column_types.empty()) {
		auto probe = conn.Query(
		    "SELECT * FROM " +
		    DuckLakeTableChangesCall(result->catalog_name, result->table_name, probe_snapshot, probe_snapshot) +
		    " LIMIT 0");
		if (!probe || probe->HasError()) {
			throw duckdb::InvalidInputException(
			    "cdc_dml_table_changes_read failed to resolve table '%s' in catalog '%s': %s", result->table_name,
			    result->catalog_name,
			    probe ? probe->GetError() : std::string("table_changes probe returned no result"));
		}
		if (probe->ColumnCount() < 3) {
			throw duckdb::InvalidInputException("cdc_dml_table_changes_read: unexpected table_changes schema for table "
			                                    "'%s' (got %u columns; expected at least 3)",
			                                    result->table_name, static_cast<unsigned>(probe->ColumnCount()));
		}
		for (duckdb::idx_t i = 3; i < probe->ColumnCount(); ++i) {
			result->table_column_names.push_back(probe->ColumnName(i));
			result->table_column_types.push_back(probe->types[i]);
		}
		TableChangesSchemaCacheEntry cache_entry;
		cache_entry.column_names = result->table_column_names;
		cache_entry.column_types = result->table_column_types;
		std::lock_guard<std::mutex> guard(TABLE_CHANGES_SCHEMA_CACHE_MUTEX);
		TABLE_CHANGES_SCHEMA_CACHE[schema_cache_key] = std::move(cache_entry);
	}

	names = {"consumer_name", "start_snapshot", "end_snapshot", "snapshot_id", "rowid", "change_type"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR};
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
	return std::move(result);
}

duckdb::unique_ptr<duckdb::FunctionData> CdcTableChangesReadBind(duckdb::ClientContext &context,
                                                                 duckdb::TableFunctionBindInput &input,
                                                                 duckdb::vector<duckdb::LogicalType> &return_types,
                                                                 duckdb::vector<duckdb::string> &names) {
	return CdcChangesBindBase(context, input, return_types, names, false);
}

duckdb::unique_ptr<duckdb::FunctionData> CdcTableChangesListenBind(duckdb::ClientContext &context,
                                                                   duckdb::TableFunctionBindInput &input,
                                                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                                                   duckdb::vector<duckdb::string> &names) {
	return CdcChangesBindBase(context, input, return_types, names, true);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcChangesInit(duckdb::ClientContext &context,
                                                                    duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<MaterializedResultScanState>();
	auto &data = input.bind_data->Cast<CdcChangesData>();
	auto max_snapshots = data.max_snapshots;
	if (data.listen && !data.explicit_window) {
		auto ready = WaitForConsumerSnapshot(context, data.catalog_name, data.consumer_name, data.timeout_ms);
		if (ready.empty() || ready[0].IsNull()) {
			return std::move(result);
		}
		if (ready.size() > 1 && !ready[1].IsNull()) {
			max_snapshots = std::max<int64_t>(max_snapshots, ready[1].GetValue<int64_t>());
		}
	}

	int64_t start_snapshot;
	int64_t end_snapshot;
	bool has_changes;
	if (data.explicit_window) {
		start_snapshot = data.start_snapshot;
		end_snapshot = data.end_snapshot;
		has_changes = end_snapshot >= start_snapshot;
	} else {
		CdcWindowData window_data;
		window_data.catalog_name = data.catalog_name;
		window_data.consumer_name = data.consumer_name;
		window_data.max_snapshots = max_snapshots;
		auto window = ReadWindow(context, window_data);
		start_snapshot = window[0].GetValue<int64_t>();
		end_snapshot = window[1].GetValue<int64_t>();
		has_changes = window[2].GetValue<bool>();
	}
	if (!has_changes) {
		return std::move(result);
	}

	duckdb::Connection conn(*context.db);
	if (data.change_types.empty()) {
		return std::move(result);
	}
	auto scan_table_name = data.table_name;
	const auto table_name_at_end = CurrentQualifiedTableName(conn, data.catalog_name, data.table_id, end_snapshot);
	if (!table_name_at_end.empty()) {
		scan_table_name = table_name_at_end;
	}

	std::ostringstream column_list;
	column_list << QuoteLiteral(data.consumer_name) << " AS consumer_name, " << std::to_string(start_snapshot)
	            << " AS start_snapshot, " << std::to_string(end_snapshot)
	            << " AS end_snapshot, tc.snapshot_id, tc.rowid, tc.change_type";
	for (const auto &col_name : data.table_column_names) {
		column_list << ", tc." << QuoteIdentifier(col_name);
	}

	std::ostringstream where_clause;
	if (!data.change_types.empty()) {
		where_clause << " WHERE tc.change_type IN (";
		for (size_t i = 0; i < data.change_types.size(); ++i) {
			if (i > 0) {
				where_clause << ", ";
			}
			where_clause << QuoteLiteral(data.change_types[i]);
		}
		where_clause << ")";
	}

	auto query =
	    std::string("SELECT ") + column_list.str() +
	    ", s.snapshot_time, c.author, c.commit_message, c.commit_extra_info FROM " +
	    DuckLakeTableChangesCall(data.catalog_name, scan_table_name, start_snapshot, end_snapshot) + " tc LEFT JOIN " +
	    MetadataTable(data.catalog_name, "ducklake_snapshot") + " s ON s.snapshot_id = tc.snapshot_id LEFT JOIN " +
	    MetadataTable(data.catalog_name, "ducklake_snapshot_changes") + " c ON c.snapshot_id = tc.snapshot_id" +
	    where_clause.str() + " ORDER BY tc.snapshot_id ASC, tc.rowid ASC";
	auto rows = conn.Query(query);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "cdc_dml_table_changes_read scan failed");
	}
	result->result = std::move(rows);
	if (data.auto_commit && !data.explicit_window) {
		CommitConsumerSnapshot(context, data.catalog_name, data.consumer_name, end_snapshot);
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_dml_table_changes_query
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// Stateless range helpers
//===--------------------------------------------------------------------===//

int64_t RangeToSnapshotParameter(duckdb::Connection &conn, duckdb::TableFunctionBindInput &input,
                                 const std::string &catalog_name, duckdb::idx_t positional_index) {
	if (input.inputs.size() > positional_index) {
		if (input.inputs[positional_index].IsNull()) {
			return CurrentSnapshot(conn, catalog_name);
		}
		return input.inputs[positional_index].GetValue<int64_t>();
	}
	auto entry = input.named_parameters.find("to_snapshot");
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return CurrentSnapshot(conn, catalog_name);
	}
	return entry->second.GetValue<int64_t>();
}

void ValidateRangeBounds(duckdb::Connection &conn, const std::string &catalog_name, int64_t from_snapshot,
                         int64_t to_snapshot, const std::string &feature_name) {
	if (to_snapshot < from_snapshot) {
		throw duckdb::InvalidInputException("%s: from_snapshot must be <= to_snapshot", feature_name);
	}
	auto oldest_result =
	    conn.Query("SELECT COALESCE(min(snapshot_id), 0) FROM " + MetadataTable(catalog_name, "ducklake_snapshot"));
	const auto oldest_snapshot = SingleInt64(*oldest_result, "oldest available snapshot");
	if (from_snapshot < oldest_snapshot) {
		throw duckdb::InvalidInputException("%s: from_snapshot %lld is older than oldest available snapshot %lld",
		                                    feature_name, static_cast<long long>(from_snapshot),
		                                    static_cast<long long>(oldest_snapshot));
	}
}

duckdb::unique_ptr<duckdb::FunctionData> GenericDmlQueryBind(duckdb::ClientContext &context,
                                                             duckdb::TableFunctionBindInput &input,
                                                             duckdb::vector<duckdb::LogicalType> &return_types,
                                                             duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2 && input.inputs.size() != 3) {
		throw duckdb::BinderException(
		    "cdc_dml_changes_query requires catalog, from_snapshot, and optional to_snapshot");
	}
	auto result = duckdb::make_uniq<GenericDmlChangesData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->from_snapshot = input.inputs[1].GetValue<int64_t>();
	result->stateless = true;
	result->table_ids = Int64ListNamedParameter(input, "table_ids");
	result->table_names = StringListNamedParameter(input, "table_names");
	result->change_types = ChangeTypesNamedParameter(input);
	CheckCatalogOrThrow(context, result->catalog_name);
	duckdb::Connection conn(*context.db);
	result->to_snapshot = RangeToSnapshotParameter(conn, input, result->catalog_name, 2);
	ValidateRangeBounds(conn, result->catalog_name, result->from_snapshot, result->to_snapshot,
	                    "cdc_dml_changes_query");
	GenericDmlReturnTypes(return_types, names, false);
	return std::move(result);
}

std::vector<int64_t> DmlTablesTouchedInRange(duckdb::Connection &conn, const std::string &catalog_name,
                                             int64_t from_snapshot, int64_t to_snapshot) {
	std::unordered_set<int64_t> ids;
	auto rows = conn.Query("SELECT changes_made FROM " + MetadataTable(catalog_name, "ducklake_snapshot_changes") +
	                       " WHERE snapshot_id BETWEEN " + std::to_string(from_snapshot) + " AND " +
	                       std::to_string(to_snapshot));
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID, rows ? rows->GetError() : "DML table discovery failed");
	}
	for (duckdb::idx_t i = 0; i < rows->RowCount(); ++i) {
		const auto value = rows->GetValue(0, i);
		const auto changes = value.IsNull() ? std::string() : value.ToString();
		for (const auto &token : SplitEventTokens(changes)) {
			int64_t table_id = 0;
			std::string change_type;
			if (DmlTokenInfo(token, table_id, change_type)) {
				ids.insert(table_id);
			}
		}
	}
	std::vector<int64_t> out(ids.begin(), ids.end());
	std::sort(out.begin(), out.end());
	return out;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> GenericDmlQueryInit(duckdb::ClientContext &context,
                                                                         duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<GenericDmlChangesData>();
	duckdb::Connection conn(*context.db);
	std::unordered_set<int64_t> table_ids(data.table_ids.begin(), data.table_ids.end());
	for (const auto &table_name_input : data.table_names) {
		auto table_name = table_name_input.find('.') == std::string::npos ? std::string("main.") + table_name_input
		                                                                  : table_name_input;
		int64_t schema_id = 0;
		int64_t table_id = 0;
		if (!ResolveCurrentTableName(conn, data.catalog_name, table_name, data.to_snapshot, schema_id, table_id)) {
			throw duckdb::InvalidInputException("cdc_dml_changes_query failed to resolve table '%s'", table_name);
		}
		table_ids.insert(table_id);
	}
	if (table_ids.empty()) {
		for (const auto table_id :
		     DmlTablesTouchedInRange(conn, data.catalog_name, data.from_snapshot, data.to_snapshot)) {
			table_ids.insert(table_id);
		}
	}
	for (const auto table_id : table_ids) {
		const auto table_name = CurrentQualifiedTableName(conn, data.catalog_name, table_id, data.to_snapshot);
		if (table_name.empty()) {
			continue;
		}
		int64_t schema_id = 0;
		int64_t ignored_table = 0;
		ResolveCurrentTableName(conn, data.catalog_name, table_name, data.to_snapshot, schema_id, ignored_table);
		AppendGenericDmlTableRows(conn, data, std::string(), data.from_snapshot, data.to_snapshot, schema_id, table_id,
		                          table_name, data.change_types, result->rows, false);
	}
	std::sort(result->rows.begin(), result->rows.end(),
	          [](const std::vector<duckdb::Value> &lhs, const std::vector<duckdb::Value> &rhs) {
		          return lhs[0].GetValue<int64_t>() < rhs[0].GetValue<int64_t>();
	          });
	return std::move(result);
}

duckdb::unique_ptr<duckdb::FunctionData> DmlTicksQueryBind(duckdb::ClientContext &context,
                                                           duckdb::TableFunctionBindInput &input,
                                                           duckdb::vector<duckdb::LogicalType> &return_types,
                                                           duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2 && input.inputs.size() != 3) {
		throw duckdb::BinderException("cdc_dml_ticks_query requires catalog, from_snapshot, and optional to_snapshot");
	}
	auto result = duckdb::make_uniq<DmlTicksData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->from_snapshot = input.inputs[1].GetValue<int64_t>();
	result->stateless = true;
	result->table_ids = Int64ListNamedParameter(input, "table_ids");
	result->table_names = StringListNamedParameter(input, "table_names");
	CheckCatalogOrThrow(context, result->catalog_name);
	duckdb::Connection conn(*context.db);
	result->to_snapshot = RangeToSnapshotParameter(conn, input, result->catalog_name, 2);
	ValidateRangeBounds(conn, result->catalog_name, result->from_snapshot, result->to_snapshot, "cdc_dml_ticks_query");
	DmlTicksReturnTypes(return_types, names, false);
	return std::move(result);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> DmlTicksQueryInit(duckdb::ClientContext &context,
                                                                       duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<DmlTicksData>();
	duckdb::Connection conn(*context.db);
	std::unordered_set<int64_t> filter_table_ids(data.table_ids.begin(), data.table_ids.end());
	for (const auto &table_name_input : data.table_names) {
		auto table_name = table_name_input.find('.') == std::string::npos ? std::string("main.") + table_name_input
		                                                                  : table_name_input;
		int64_t schema_id = 0;
		int64_t table_id = 0;
		if (!ResolveCurrentTableName(conn, data.catalog_name, table_name, data.to_snapshot, schema_id, table_id)) {
			throw duckdb::InvalidInputException("cdc_dml_ticks_query failed to resolve table '%s'", table_name);
		}
		filter_table_ids.insert(table_id);
	}
	const auto *filter = filter_table_ids.empty() ? nullptr : &filter_table_ids;
	AppendDmlTickRows(conn, data.catalog_name, std::string(), data.from_snapshot, data.to_snapshot, nullptr, filter,
	                  result->rows, false);
	return std::move(result);
}

struct CdcRangeChangesData : public duckdb::TableFunctionData {
	std::string catalog_name;
	std::string table_name;
	int64_t table_id = -1;
	int64_t from_snapshot;
	int64_t to_snapshot;
	bool allow_history_fallback = false;
	std::vector<std::string> table_column_names;
	std::vector<duckdb::LogicalType> table_column_types;

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		auto result = duckdb::make_uniq<CdcRangeChangesData>();
		result->catalog_name = catalog_name;
		result->table_name = table_name;
		result->table_id = table_id;
		result->from_snapshot = from_snapshot;
		result->to_snapshot = to_snapshot;
		result->allow_history_fallback = allow_history_fallback;
		result->table_column_names = table_column_names;
		result->table_column_types = table_column_types;
		return std::move(result);
	}

	bool Equals(const duckdb::FunctionData &other) const override {
		return this == &other;
	}
};

duckdb::unique_ptr<duckdb::FunctionData> CdcRangeChangesBind(duckdb::ClientContext &context,
                                                             duckdb::TableFunctionBindInput &input,
                                                             duckdb::vector<duckdb::LogicalType> &return_types,
                                                             duckdb::vector<duckdb::string> &names) {
	if (input.inputs.size() != 2 && input.inputs.size() != 3) {
		throw duckdb::BinderException(
		    "cdc_dml_table_changes_query requires catalog, from_snapshot, and optional to_snapshot");
	}
	auto result = duckdb::make_uniq<CdcRangeChangesData>();
	result->catalog_name = GetStringArg(input.inputs[0], "catalog");
	result->from_snapshot = input.inputs[1].GetValue<int64_t>();
	CheckCatalogOrThrow(context, result->catalog_name);
	duckdb::Connection conn(*context.db);
	result->to_snapshot = RangeToSnapshotParameter(conn, input, result->catalog_name, 2);
	ValidateRangeBounds(conn, result->catalog_name, result->from_snapshot, result->to_snapshot,
	                    "cdc_dml_table_changes_query");

	auto table_id_entry = input.named_parameters.find("table_id");
	auto table_name_entry = input.named_parameters.find("table_name");
	if (table_id_entry != input.named_parameters.end() && !table_id_entry->second.IsNull()) {
		result->table_id = table_id_entry->second.GetValue<int64_t>();
		result->table_name =
		    CurrentQualifiedTableName(conn, result->catalog_name, result->table_id, result->to_snapshot);
		if (result->table_name.empty()) {
			throw duckdb::InvalidInputException(
			    "cdc_dml_table_changes_query: table_id %lld is not active at to_snapshot",
			    static_cast<long long>(result->table_id));
		}
	} else if (table_name_entry != input.named_parameters.end() && !table_name_entry->second.IsNull()) {
		result->table_name = table_name_entry->second.GetValue<std::string>();
		result->allow_history_fallback = true;
		if (result->table_name.find('.') == std::string::npos) {
			result->table_name = std::string("main.") + result->table_name;
		}
		int64_t schema_id = 0;
		if (!ResolveCurrentTableName(conn, result->catalog_name, result->table_name, result->to_snapshot, schema_id,
		                             result->table_id)) {
			throw duckdb::InvalidInputException("cdc_dml_table_changes_query failed to resolve table '%s'",
			                                    result->table_name);
		}
	} else {
		throw duckdb::BinderException("cdc_dml_table_changes_query requires table_id or table_name");
	}

	auto probe = conn.Query(
	    "SELECT * FROM " +
	    DuckLakeTableChangesCall(result->catalog_name, result->table_name, result->to_snapshot, result->to_snapshot) +
	    " LIMIT 0");
	if (!probe || probe->HasError()) {
		throw duckdb::InvalidInputException(
		    "cdc_dml_table_changes_query failed to resolve table '%s' in catalog '%s': %s", result->table_name,
		    result->catalog_name, probe ? probe->GetError() : std::string("table_changes probe returned no result"));
	}
	if (probe->ColumnCount() < 3) {
		throw duckdb::InvalidInputException("cdc_dml_table_changes_query: unexpected table_changes schema for table "
		                                    "'%s' (got %u columns; expected at least 3)",
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

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> CdcRangeChangesInit(duckdb::ClientContext &context,
                                                                         duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<RowScanState>();
	auto &data = input.bind_data->Cast<CdcRangeChangesData>();
	duckdb::Connection conn(*context.db);

	std::ostringstream column_list;
	column_list << "tc.snapshot_id, tc.rowid, tc.change_type";
	for (const auto &col_name : data.table_column_names) {
		column_list << ", tc." << QuoteIdentifier(col_name);
	}
	auto scan_range = [&](int64_t from_snapshot) {
		auto query = std::string("SELECT ") + column_list.str() +
		             ", s.snapshot_time, c.author, c.commit_message, c.commit_extra_info FROM " +
		             DuckLakeTableChangesCall(data.catalog_name, data.table_name, from_snapshot, data.to_snapshot) +
		             " tc LEFT JOIN " + MetadataTable(data.catalog_name, "ducklake_snapshot") +
		             " s ON s.snapshot_id = tc.snapshot_id LEFT JOIN " +
		             MetadataTable(data.catalog_name, "ducklake_snapshot_changes") +
		             " c ON c.snapshot_id = tc.snapshot_id ORDER BY tc.snapshot_id ASC, tc.rowid ASC";
		return conn.Query(query);
	};
	auto rows = scan_range(data.from_snapshot);
	if (!rows || rows->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::INVALID,
		                        rows ? rows->GetError() : "cdc_dml_table_changes_query scan failed");
	}
	if (rows->RowCount() == 0 && data.allow_history_fallback) {
		auto begin_result = conn.Query("SELECT COALESCE(min(begin_snapshot), " + std::to_string(data.from_snapshot) +
		                               ") FROM " + MetadataTable(data.catalog_name, "ducklake_table") +
		                               " WHERE table_id = " + std::to_string(data.table_id));
		if (begin_result && !begin_result->HasError() && begin_result->RowCount() > 0 &&
		    !begin_result->GetValue(0, 0).IsNull()) {
			const auto begin_snapshot = begin_result->GetValue(0, 0).GetValue<int64_t>();
			if (begin_snapshot < data.from_snapshot) {
				rows = scan_range(begin_snapshot);
				if (!rows || rows->HasError()) {
					throw duckdb::Exception(duckdb::ExceptionType::INVALID,
					                        rows ? rows->GetError() : "cdc_dml_table_changes_query scan failed");
				}
			}
		}
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
	for (const auto &name : {"cdc_dml_ticks_read", "cdc_dml_ticks_listen"}) {
		const auto bind =
		    std::string(name).find("_listen") == std::string::npos ? DmlTicksReadBind : DmlTicksListenBind;
		duckdb::TableFunction events_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, bind, DmlTicksInit);
		events_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		events_function.named_parameters["timeout_ms"] = duckdb::LogicalType::BIGINT;
		events_function.named_parameters["auto_commit"] = duckdb::LogicalType::BOOLEAN;
		loader.RegisterFunction(events_function);
	}

	for (const auto &name : {"cdc_dml_changes_read", "cdc_dml_changes_listen"}) {
		const auto bind =
		    std::string(name).find("_listen") == std::string::npos ? GenericDmlReadBind : GenericDmlListenBind;
		duckdb::TableFunction changes_function(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    RowScanExecute, bind, GenericDmlInit);
		changes_function.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["start_snapshot"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["end_snapshot"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["timeout_ms"] = duckdb::LogicalType::BIGINT;
		changes_function.named_parameters["auto_commit"] = duckdb::LogicalType::BOOLEAN;
		loader.RegisterFunction(changes_function);
	}

	for (const auto &name : {"cdc_dml_table_changes_read", "cdc_dml_table_changes_listen"}) {
		const auto bind = std::string(name).find("_listen") == std::string::npos ? CdcTableChangesReadBind
		                                                                         : CdcTableChangesListenBind;
		duckdb::TableFunction changes_function_new(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
		    MaterializedResultScanExecute, bind, CdcChangesInit);
		changes_function_new.named_parameters["table_id"] = duckdb::LogicalType::BIGINT;
		changes_function_new.named_parameters["table_name"] = duckdb::LogicalType::VARCHAR;
		changes_function_new.named_parameters["max_snapshots"] = duckdb::LogicalType::BIGINT;
		changes_function_new.named_parameters["start_snapshot"] = duckdb::LogicalType::BIGINT;
		changes_function_new.named_parameters["end_snapshot"] = duckdb::LogicalType::BIGINT;
		changes_function_new.named_parameters["timeout_ms"] = duckdb::LogicalType::BIGINT;
		changes_function_new.named_parameters["auto_commit"] = duckdb::LogicalType::BOOLEAN;
		loader.RegisterFunction(changes_function_new);
	}

	for (const auto &name : {"cdc_dml_ticks_query"}) {
		duckdb::TableFunction dml_ticks_query_function_2(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
		    RowScanExecute, DmlTicksQueryBind, DmlTicksQueryInit);
		dml_ticks_query_function_2.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		dml_ticks_query_function_2.named_parameters["table_ids"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		dml_ticks_query_function_2.named_parameters["table_names"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		loader.RegisterFunction(dml_ticks_query_function_2);
		duckdb::TableFunction dml_ticks_query_function_3(
		    name,
		    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
		                                         duckdb::LogicalType::BIGINT},
		    RowScanExecute, DmlTicksQueryBind, DmlTicksQueryInit);
		dml_ticks_query_function_3.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		dml_ticks_query_function_3.named_parameters["table_ids"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		dml_ticks_query_function_3.named_parameters["table_names"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		loader.RegisterFunction(dml_ticks_query_function_3);
	}

	for (const auto &name : {"cdc_dml_changes_query"}) {
		duckdb::TableFunction generic_query_function_2(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
		    RowScanExecute, GenericDmlQueryBind, GenericDmlQueryInit);
		generic_query_function_2.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		generic_query_function_2.named_parameters["table_ids"] = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		generic_query_function_2.named_parameters["table_names"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		generic_query_function_2.named_parameters["change_types"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		loader.RegisterFunction(generic_query_function_2);
		duckdb::TableFunction generic_query_function_3(
		    name,
		    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
		                                         duckdb::LogicalType::BIGINT},
		    RowScanExecute, GenericDmlQueryBind, GenericDmlQueryInit);
		generic_query_function_3.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		generic_query_function_3.named_parameters["table_ids"] = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
		generic_query_function_3.named_parameters["table_names"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		generic_query_function_3.named_parameters["change_types"] =
		    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
		loader.RegisterFunction(generic_query_function_3);
	}

	for (const auto &name : {"cdc_dml_table_changes_query"}) {
		duckdb::TableFunction table_changes_query_function_2(
		    name, duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
		    RowScanExecute, CdcRangeChangesBind, CdcRangeChangesInit);
		table_changes_query_function_2.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		table_changes_query_function_2.named_parameters["table_id"] = duckdb::LogicalType::BIGINT;
		table_changes_query_function_2.named_parameters["table_name"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(table_changes_query_function_2);
		duckdb::TableFunction table_changes_query_function_3(
		    name,
		    duckdb::vector<duckdb::LogicalType> {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
		                                         duckdb::LogicalType::BIGINT},
		    RowScanExecute, CdcRangeChangesBind, CdcRangeChangesInit);
		table_changes_query_function_3.named_parameters["to_snapshot"] = duckdb::LogicalType::BIGINT;
		table_changes_query_function_3.named_parameters["table_id"] = duckdb::LogicalType::BIGINT;
		table_changes_query_function_3.named_parameters["table_name"] = duckdb::LogicalType::VARCHAR;
		loader.RegisterFunction(table_changes_query_function_3);
	}
}

} // namespace duckdb_cdc
