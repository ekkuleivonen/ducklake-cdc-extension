//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// dml.hpp
//
// Row-level change-event surface. Owns cdc_dml_ticks_read (snapshot-by-snapshot
// commit metadata filtered by consumer subscriptions), cdc_dml_table_changes_read (typed
// DML rows from `<lake>.table_changes()` projected through subscription
// change-type rules), and the stateless cdc_dml_table_changes_query sugar for
// ad-hoc lookback queries.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb_cdc {

//! Register the row-level change-event table functions: cdc_dml_ticks_read,
//! cdc_dml_table_changes_read, cdc_dml_table_changes_query. Called once at extension load.
void RegisterDmlFunctions(duckdb::ExtensionLoader &loader);

} // namespace duckdb_cdc
