//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// dml.hpp
//
// Row-level change-event surface. Owns cdc_dml_ticks_read (snapshot-by-
// snapshot commit metadata filtered by consumer subscriptions),
// cdc_dml_changes_read / cdc_dml_changes_listen (typed DML rows from the
// consumer's pinned table — one DML consumer = one table — projected
// through subscription change-type rules), and the stateless
// cdc_dml_changes_query sugar for ad-hoc single-table lookback.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb_cdc {

//! Register the row-level change-event table functions:
//! cdc_dml_ticks_read / cdc_dml_ticks_listen / cdc_dml_ticks_query,
//! cdc_dml_changes_read / cdc_dml_changes_listen / cdc_dml_changes_query.
//! Called once at extension load.
void RegisterDmlFunctions(duckdb::ExtensionLoader &loader);

} // namespace duckdb_cdc
