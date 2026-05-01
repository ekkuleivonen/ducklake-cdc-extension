//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// compat_check.hpp
//
// Catalog-version compatibility probe for attached DuckLake catalogs.
//
// Background: every DuckLake catalog stores a format version string in its
// metadata table at `__ducklake_metadata_<lake>.ducklake_metadata WHERE
// key='version'`. The DuckLake extension binaries validated with DuckDB v1.5.x
// stamp either `'0.4'` or `'1.0'` there. This extension's SQL against
// `ducklake_*` and `__ducklake_metadata_*` is only known to be well-formed on
// the catalog versions explicitly listed in `docs/development.md`. Loading the
// extension against an untested catalog format and then watching it fail with a
// cryptic `column 'foo' does not exist` 30 minutes later is the worst possible UX.
//
// This module:
//
// 1. Reads the catalog format version of every attached DuckLake catalog.
// 2. Compares against the hardcoded supported set
//    (`SUPPORTED_DUCKLAKE_CATALOG_VERSIONS`).
// 3. Emits a structured `CDC_INCOMPATIBLE_CATALOG` notice (one per
//    incompatible catalog) so the operator hits the matrix in
//    `docs/development.md` immediately on `LOAD ducklake_cdc;`,
//    not after the first cdc_* call against the catalog.
//
// The probe is intentionally **best-effort** at LOAD time: catalogs may be
// attached after LOAD, and the future cdc_* functions that touch a
// DuckLake catalog re-run this check before any catalog work. The build
// stamp `cdc_version()` is exempt — it stays callable so users can
// debug regardless of catalog state.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <string>
#include <vector>

namespace duckdb_cdc {

//! One probe outcome per attached DuckLake catalog.
struct CompatStatus {
	//! Attached database name (the `AS lake` in `ATTACH ... AS lake`).
	std::string catalog_name;
	//! Catalog format version read from the metadata row, or empty string
	//! when the version row could not be read (partially-initialised
	//! catalog mid-ATTACH, permissions, etc. — treated as "skip silently").
	std::string observed_version;
	//! False iff `observed_version` is non-empty and outside
	//! `SUPPORTED_DUCKLAKE_CATALOG_VERSIONS`.
	bool compatible;
	//! Populated only on incompatible catalogs. Already prefixed with
	//! `CDC_INCOMPATIBLE_CATALOG:` per docs/errors.md so callers can pass
	//! it to a notice channel verbatim.
	std::string message;
};

//! Hardcoded supported catalog format versions.
//!
//! Narrow on purpose: an unrecognised version is louder than a wrong
//! version. Add a string here when (and only when) a release is tested
//! against that catalog format and the matrix in `docs/development.md`
//! is bumped in the same PR. See ADR 0001-style discipline notes in the
//! plan that birthed this guard for why we don't open this to a regex.
extern const std::vector<std::string> SUPPORTED_DUCKLAKE_CATALOG_VERSIONS;

//! For each attached database whose name has a sibling
//! `__ducklake_metadata_<name>` schema, read the catalog format version
//! and produce a `CompatStatus`. Used by `LoadInternal()` at extension-
//! load time and (in a future slice) by every cdc_* function before it
//! touches the catalog. Non-DuckLake attachments are skipped silently.
std::vector<CompatStatus> CheckAllAttachedCatalogs(duckdb::ClientContext &context);

//! Walks `CheckAllAttachedCatalogs(context)` and emits one stderr notice
//! per incompatible catalog. No-op on empty / all-compatible result.
//! Catches and swallows DuckDB exceptions so this stays best-effort:
//! a probe failure must never break extension load.
void EmitCompatNoticesIfAny(duckdb::DatabaseInstance &db);

//! Authoritative call-time gate for cdc_* functions that touch a DuckLake
//! catalog. Unlike the LOAD-time notice path, this throws a structured
//! `CDC_INCOMPATIBLE_CATALOG` exception before any catalog write.
void CheckCatalogOrThrow(duckdb::ClientContext &context, const std::string &catalog_name);

} // namespace duckdb_cdc
