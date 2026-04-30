//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// stats.cpp
//
// Implementation of the observability surface: cdc_consumer_stats and
// cdc_audit_recent. These functions are read-only views over the same
// state tables `consumer.cpp` writes to: the consumer-state row for
// stats (cursor / lag / lease / unresolved tables) and the audit log
// for cdc_audit_recent.
//===----------------------------------------------------------------------===//

#include "stats.hpp"

#include "consumer.hpp"
#include "ducklake_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <sstream>
#include <unordered_set>

namespace duckdb_cdc {

namespace {

//===--------------------------------------------------------------------===//
// cdc_consumer_stats
//===--------------------------------------------------------------------===//

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

	names = {"consumer_name",      "consumer_id",          "last_committed_snapshot",   "current_snapshot",
	         "lag_snapshots",      "lag_seconds",          "oldest_available_snapshot", "gap_distance",
	         "subscription_count", "subscriptions_active", "subscriptions_renamed",     "subscriptions_dropped",
	         "owner_token",        "owner_acquired_at",    "owner_heartbeat_at",        "lease_interval_seconds",
	         "lease_alive"};
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,       duckdb::LogicalType::DOUBLE,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::BIGINT,  duckdb::LogicalType::BIGINT,       duckdb::LogicalType::BIGINT,
	                duckdb::LogicalType::UUID,    duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::TIMESTAMP_TZ,
	                duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN};
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

	const auto consumers = StateTable(conn, data.catalog_name, CONSUMERS_TABLE);
	std::ostringstream where;
	if (!data.consumer_name.empty()) {
		where << " WHERE c.consumer_name = " << QuoteLiteral(data.consumer_name);
	}

	// Single big projection so the planner JOIN-and-aggregates against
	// ducklake_snapshot in one pass per consumer.
	auto query = std::string("SELECT c.consumer_name, c.consumer_id, c.last_committed_snapshot, "
	                         "s.snapshot_time, c.owner_token, c.owner_acquired_at, "
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
		const auto owner_token_value = rows->GetValue(4, i);
		const auto owner_acquired_value = rows->GetValue(5, i);
		const auto owner_heartbeat_value = rows->GetValue(6, i);
		const auto lease_interval = rows->GetValue(7, i).GetValue<int64_t>();

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

		const auto subscriptions = LoadConsumerSubscriptions(conn, data.catalog_name, consumer_name_value.ToString());
		int64_t active = 0;
		int64_t renamed = 0;
		int64_t dropped = 0;
		for (const auto &subscription : subscriptions) {
			if (subscription.status == "active") {
				active++;
			} else if (subscription.status == "renamed") {
				renamed++;
			} else if (subscription.status == "dropped") {
				dropped++;
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
		row.push_back(duckdb::Value::BIGINT(static_cast<int64_t>(subscriptions.size())));
		row.push_back(duckdb::Value::BIGINT(active));
		row.push_back(duckdb::Value::BIGINT(renamed));
		row.push_back(duckdb::Value::BIGINT(dropped));
		row.push_back(owner_token_value);
		row.push_back(owner_acquired_value);
		row.push_back(owner_heartbeat_value);
		row.push_back(duckdb::Value::INTEGER(static_cast<int32_t>(lease_interval)));
		row.push_back(duckdb::Value::BOOLEAN(lease_alive));
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// cdc_audit_recent
//===--------------------------------------------------------------------===//

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
	const auto audit = StateTable(conn, data.catalog_name, AUDIT_TABLE);
	std::ostringstream where;
	where << " WHERE epoch(ts::TIMESTAMP WITH TIME ZONE) >= epoch(now()) - " << data.since_seconds;
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

} // namespace

void RegisterStatsFunctions(duckdb::ExtensionLoader &loader) {
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
