//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// stats.hpp
//
// Observability surface. Owns cdc_consumer_stats (per-consumer cursor /
// lag / lease / unresolved-table view) and cdc_audit_recent (recent rows
// from the consumer-state audit trail). Read-only to consumer state -
// audit *writes* are owned by `consumer.cpp` because they are side
// effects of consumer-lifecycle actions.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb_cdc {

//! Register the observability table functions: cdc_consumer_stats,
//! cdc_audit_recent. Called once at extension load.
void RegisterStatsFunctions(duckdb::ExtensionLoader &loader);

} // namespace duckdb_cdc
