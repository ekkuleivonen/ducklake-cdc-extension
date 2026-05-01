//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// ddl.hpp
//
// Schema-change information surface. Owns the typed DDL extraction path
// (cdc_ddl_changes_read/listen/query and cdc_ddl_ticks_read/listen/query)
// over `<lake>.snapshots().changes` MAP entries, and the per-table
// column-level diff (cdc_schema_diff).
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <string>

namespace duckdb_cdc {

//! Register the schema-change table functions. Called once at extension load.
void RegisterDdlFunctions(duckdb::ExtensionLoader &loader);

} // namespace duckdb_cdc
