//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// ddl.hpp
//
// Schema-change information surface. Owns the typed DDL extraction path
// (cdc_ddl, cdc_recent_ddl) over `<lake>.snapshots().changes` MAP entries,
// and the per-table column-level diff (cdc_schema_diff).
//
// `dml.cpp` uses subscription-aware helpers from here to decide which
// snapshots touch a consumer's subscribed object identities.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <string>
#include <unordered_set>

namespace duckdb_cdc {

//! True iff `changes_made` references at least one subscribed table identity.
//! Used by `cdc_events` in dml.cpp to filter snapshot rows before emitting
//! them - the predicate intentionally lives in ddl.cpp because it walks
//! DuckLake-emitted change tokens.
bool ChangesTouchConsumerTables(duckdb::Connection &conn, const std::string &catalog_name, int64_t snapshot_id,
                                const std::string &changes_made, const std::unordered_set<std::string> &filter_tables);

//! Register the schema-change table functions: cdc_ddl, cdc_recent_ddl,
//! cdc_schema_diff. Called once at extension load.
void RegisterDdlFunctions(duckdb::ExtensionLoader &loader);

} // namespace duckdb_cdc
