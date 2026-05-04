//===----------------------------------------------------------------------===//
//                         ducklake_cdc
//
// compat_check.cpp
//
// See `compat_check.hpp` for the design notes. This file is the
// implementation only.
//===----------------------------------------------------------------------===//

#include "compat_check.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <mutex>
#include <sstream>
#include <utility>

namespace duckdb_cdc {

// Supported catalog format versions. Probed empirically against the DuckLake
// extension binaries shipped for the validated DuckDB v1.5.x patch releases.
// The compatibility check is intentionally narrow: an unrecognised version is
// loud; a wrong version is silent.
//
// Maintenance discipline: when a new DuckLake catalog format is tested
// against, append the new string to this vector AND add the corresponding
// row/column to docs/development.md in the same PR.
const std::vector<std::string> SUPPORTED_DUCKLAKE_CATALOG_VERSIONS = {"0.4", "1.0"};

namespace {

// Build-stamp literal that mirrors CdcVersionScalarFun() in
// `src/ducklake_cdc_extension.cpp` so the user-facing notice and
// `cdc_version()` agree on what extension build is reporting. Driven
// entirely by `EXT_VERSION_DUCKLAKE_CDC` (tag or short SHA) per
// docs/decisions/0013-versioning-and-release-automation.md.
#ifdef EXT_VERSION_DUCKLAKE_CDC
constexpr const char *EXTENSION_VERSION_LITERAL = "ducklake_cdc " EXT_VERSION_DUCKLAKE_CDC;
#else
constexpr const char *EXTENSION_VERSION_LITERAL = "ducklake_cdc unknown";
#endif

std::string SupportedSetForMessage() {
	std::ostringstream out;
	out << "{";
	for (size_t i = 0; i < SUPPORTED_DUCKLAKE_CATALOG_VERSIONS.size(); ++i) {
		if (i > 0) {
			out << ", ";
		}
		out << SUPPORTED_DUCKLAKE_CATALOG_VERSIONS[i];
	}
	out << "}";
	return out.str();
}

bool IsSupported(const std::string &version) {
	for (const auto &v : SUPPORTED_DUCKLAKE_CATALOG_VERSIONS) {
		if (v == version) {
			return true;
		}
	}
	return false;
}

std::mutex &CompatProbeMutex() {
	static std::mutex mutex;
	return mutex;
}

// Builds the docs/errors.md `CDC_INCOMPATIBLE_CATALOG` reference message.
// Keep in lock-step with the example in docs/errors.md — the bindings
// parse on the prefix and operator runbooks copy the body verbatim.
std::string BuildIncompatibleMessage(const std::string &catalog_name, const std::string &observed_version) {
	std::ostringstream out;
	out << "CDC_INCOMPATIBLE_CATALOG: attached DuckLake catalog '" << catalog_name
	    << "' reports catalog format version '" << observed_version << "' but " << EXTENSION_VERSION_LITERAL
	    << " supports " << SupportedSetForMessage() << ". Either downgrade your DuckLake extension to a version that"
	    << " writes one of the supported versions, or upgrade ducklake_cdc to a release that supports '"
	    << observed_version << "'. See docs/development.md for the full matrix.";
	return out.str();
}

std::string ReadCatalogVersion(duckdb::Connection &conn, const std::string &catalog_name) {
	const std::string quoted_catalog = duckdb::KeywordHelper::WriteOptionallyQuoted(catalog_name);
	const std::string quoted_schema =
	    duckdb::KeywordHelper::WriteOptionallyQuoted("__ducklake_metadata_" + catalog_name);
	const std::string query_with_catalog = "SELECT value FROM " + quoted_catalog + "." + quoted_schema +
	                                       ".ducklake_metadata WHERE key = 'version' LIMIT 1";
	const std::string query_without_catalog =
	    "SELECT value FROM " + quoted_schema + ".ducklake_metadata WHERE key = 'version' LIMIT 1";
	auto result = conn.Query(query_with_catalog);
	if (!result || result->HasError()) {
		result = conn.Query(query_without_catalog);
	}
	if (!result || result->HasError() || result->RowCount() == 0) {
		return "";
	}
	auto value = result->GetValue(0, 0);
	if (value.IsNull()) {
		return "";
	}
	return value.ToString();
}

// Single Connection.Query that returns the catalog format version for
// every attached database with a sibling `__ducklake_metadata_<name>`
// schema. One round-trip per probe instead of per-database; relies on
// `duckdb_schemas()` to discover candidates without dirtying the
// catalog cache with per-non-DuckLake-attachment lookups.
//
// Returns rows of (catalog_name VARCHAR, version VARCHAR). `version`
// is NULL if the metadata row is missing.
std::vector<std::pair<std::string, std::string>> ReadAllCatalogVersions(duckdb::Connection &conn) {
	// Two-stage probe.
	//
	// Stage 1 — discovery: one query against `duckdb_schemas()` listing
	// every attached database whose name has a sibling
	// `__ducklake_metadata_<name>` schema. Filtering at this stage keeps
	// the catalog cache cold for non-DuckLake attachments (sqlite_scanner,
	// plain duckdb files, etc.) and means we don't issue per-database
	// SELECTs against schemas that don't exist.
	//
	// Stage 2 — read: for each candidate, one targeted SELECT against
	// `<db>.__ducklake_metadata_<db>.ducklake_metadata` for the
	// `key='version'` row. We can't fold this into the discovery query
	// because the schema name is dynamic in the `database_name` value
	// returned by stage 1. In practice users have a handful of attached
	// databases — the per-attachment cost is negligible.
	std::vector<std::pair<std::string, std::string>> out;
	auto schemas = conn.Query("SELECT database_name, schema_name "
	                          "FROM duckdb_schemas() "
	                          "WHERE schema_name LIKE '\\_\\_ducklake\\_metadata\\_%' ESCAPE '\\' "
	                          "  AND schema_name = '__ducklake_metadata_' || database_name");
	if (!schemas || schemas->HasError()) {
		return out;
	}
	for (duckdb::idx_t i = 0; i < schemas->RowCount(); ++i) {
		auto db_value = schemas->GetValue(0, i);
		if (db_value.IsNull()) {
			continue;
		}
		const std::string catalog_name = db_value.ToString();
		std::string version;
		try {
			version = ReadCatalogVersion(conn, catalog_name);
		} catch (...) {
			// Partially-initialised catalog mid-ATTACH, transient lock,
			// permission failure — none are fatal at LOAD time. Fall
			// through with empty version → treated as "skip silently"
			// per the CompatStatus contract.
		}
		out.emplace_back(catalog_name, version);
	}
	return out;
}

} // namespace

std::vector<CompatStatus> CheckAllAttachedCatalogs(duckdb::ClientContext &context) {
	std::vector<CompatStatus> results;
	duckdb::Connection conn(*context.db);
	auto rows = ReadAllCatalogVersions(conn);
	for (auto &row : rows) {
		CompatStatus status;
		status.catalog_name = std::move(row.first);
		status.observed_version = std::move(row.second);
		if (status.observed_version.empty()) {
			// Treat "couldn't read the version row" as compatible-but-
			// unknown; not a notice. The future cdc_* call-time check
			// will retry once the catalog is fully initialised, and
			// will surface a real error if the query still fails.
			status.compatible = true;
		} else {
			status.compatible = IsSupported(status.observed_version);
			if (!status.compatible) {
				status.message = BuildIncompatibleMessage(status.catalog_name, status.observed_version);
			}
		}
		results.push_back(std::move(status));
	}
	return results;
}

void EmitCompatNoticesIfAny(duckdb::DatabaseInstance &db) {
	try {
		duckdb::Connection conn(db);
		auto &context = *conn.context;
		auto results = CheckAllAttachedCatalogs(context);
		for (const auto &status : results) {
			if (!status.compatible && !status.message.empty()) {
				duckdb::Printer::Print(duckdb::OutputStream::STREAM_STDERR, status.message);
			}
		}
	} catch (...) {
		// Best-effort. A probe failure during LoadInternal must never
		// break extension load; the cdc_* call-time re-check is the
		// authoritative gate.
	}
}

void CheckCatalogOrThrow(duckdb::Connection &conn, const std::string &catalog_name) {
	std::string observed_version;
	try {
		std::lock_guard<std::mutex> lock(CompatProbeMutex());
		observed_version = ReadCatalogVersion(conn, catalog_name);
	} catch (...) {
		// A missing metadata schema means this is not a DuckLake catalog. Let
		// the subsequent DuckDB/DuckLake statement raise the precise catalog
		// error instead of rewriting it as a compatibility failure.
		return;
	}
	if (!observed_version.empty() && !IsSupported(observed_version)) {
		throw duckdb::CatalogException(BuildIncompatibleMessage(catalog_name, observed_version));
	}
}

void CheckCatalogOrThrow(duckdb::ClientContext &context, const std::string &catalog_name) {
	// Legacy entry point: opens its own connection. Internal cdc_*
	// callers prefer the `Connection&` overload so the version probe,
	// the bootstrap CREATE writes, and the subsequent main work all run
	// on a single DuckDB connection (and therefore a single SQLite file
	// handle when the metadata catalog is SQLite-backed). See H-022 in
	// `docs/hazard-log.md` for the Windows MinGW lock-handoff that this
	// addresses.
	duckdb::Connection conn(*context.db);
	CheckCatalogOrThrow(conn, catalog_name);
}

} // namespace duckdb_cdc
