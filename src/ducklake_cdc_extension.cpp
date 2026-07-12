#define DUCKDB_EXTENSION_MAIN

#include "ducklake_cdc_extension.hpp"

#include "compat_check.hpp"
#include "consumer.hpp"
#include "ddl.hpp"
#include "dml.hpp"
#include "stats.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

// Stable semantic version returned by `cdc_version()`. This is deliberately
// separate from EXT_VERSION_DUCKLAKE_CDC: community-extensions checks out a
// commit SHA without the release tag, so DuckDB's generated extension version
// is a build revision rather than the descriptor's semantic version.
inline void CdcVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
#ifdef DUCKLAKE_CDC_SEMVER
	const char *version_string = "ducklake_cdc " DUCKLAKE_CDC_SEMVER;
#else
	const char *version_string = "ducklake_cdc unknown";
#endif
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto out = ConstantVector::GetData<string_t>(result);
	out[0] = StringVector::AddString(result, version_string);
}

// Source/build identity for exact artifact review and diagnostics. Unlike
// cdc_version(), this is expected to vary between tagged and community builds.
inline void CdcBuildRevisionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
#ifdef EXT_VERSION_DUCKLAKE_CDC
	const char *revision = EXT_VERSION_DUCKLAKE_CDC;
#else
	const char *revision = "unknown";
#endif
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto out = ConstantVector::GetData<string_t>(result);
	out[0] = StringVector::AddString(result, revision);
}

static void LoadInternal(ExtensionLoader &loader) {
	auto cdc_version_function = ScalarFunction("cdc_version", {}, LogicalType::VARCHAR, CdcVersionScalarFun);
	loader.RegisterFunction(cdc_version_function);
	auto cdc_build_revision_function =
	    ScalarFunction("cdc_build_revision", {}, LogicalType::VARCHAR, CdcBuildRevisionScalarFun);
	loader.RegisterFunction(cdc_build_revision_function);
	// Function registration is split across the four CDC domains:
	//   - consumer: lifecycle (create/reset/drop/list/force_release/
	//     heartbeat) + cursor primitives (window/commit).
	//   - ddl: schema-change reads/listens/queries and cdc_schema_diff.
	//   - dml: row-level reads/listens/queries and snapshot ticks.
	//   - stats: observability (cdc_consumer_stats, cdc_audit_events).
	duckdb_cdc::RegisterConsumerFunctions(loader);
	duckdb_cdc::RegisterDdlFunctions(loader);
	duckdb_cdc::RegisterDmlFunctions(loader);
	duckdb_cdc::RegisterStatsFunctions(loader);

	// Best-effort catalog-version compatibility probe. If a DuckLake
	// catalog is already attached at LOAD time, every incompatible one
	// gets a `CDC_INCOMPATIBLE_CATALOG` notice on stderr now (see
	// docs/errors.md). Catalogs attached after LOAD are picked up by the
	// re-check the next cdc_* function will run before touching the
	// catalog (cdc_version() is exempt — it's a build stamp). The probe
	// itself is wrapped in a catch-all in EmitCompatNoticesIfAny so a
	// probe failure cannot break extension load.
	duckdb_cdc::EmitCompatNoticesIfAny(loader.GetDatabaseInstance());
}

void DucklakeCdcExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DucklakeCdcExtension::Name() {
	return "ducklake_cdc";
}

std::string DucklakeCdcExtension::Version() const {
#ifdef DUCKLAKE_CDC_SEMVER
	return DUCKLAKE_CDC_SEMVER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ducklake_cdc, loader) {
	duckdb::LoadInternal(loader);
}
}
