//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// consumer_state.hpp
//
// Lazy bootstrap and first consumer lifecycle functions.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <string>

namespace duckdb_cdc {

//! Idempotently create the catalog-resident CDC state tables in a DuckLake
//! catalog. Throws `CDC_INCOMPATIBLE_CATALOG` before any write when the
//! target catalog format is outside the supported set.
void BootstrapConsumerStateOrThrow(duckdb::ClientContext &context, const std::string &catalog_name);

//! Register the Phase-1 consumer-state entry points.
void RegisterConsumerStateFunctions(duckdb::ExtensionLoader &loader);

} // namespace duckdb_cdc
