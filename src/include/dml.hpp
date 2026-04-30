//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// dml.hpp
//
// Row-level change-event surface. Owns cdc_events (snapshot-by-snapshot
// commit metadata filtered by consumer subscriptions), cdc_changes (typed
// DML rows from `<lake>.table_changes()` projected through subscription
// change-type rules), and the stateless cdc_recent_changes sugar for
// ad-hoc lookback queries.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb_cdc {

//! Register the row-level change-event table functions: cdc_events,
//! cdc_changes, cdc_recent_changes. Called once at extension load.
void RegisterDmlFunctions(duckdb::ExtensionLoader &loader);

} // namespace duckdb_cdc
