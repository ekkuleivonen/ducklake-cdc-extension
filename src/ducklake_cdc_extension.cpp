#define DUCKDB_EXTENSION_MAIN

#include "ducklake_cdc_extension.hpp"

#include "compat_check.hpp"
#include "consumer_state.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

// Build-stamp string returned by `cdc_version()`. The version comes
// from `EXT_VERSION_DUCKLAKE_CDC`, which the build system stamps from
// `git tag --points-at HEAD` when releasing or from the short SHA on
// untagged builds (see `extension-ci-tools/scripts/configure_helper.py`).
// This matches the official DuckDB extension versioning convention:
// untagged builds report an unstable identifier (short SHA), tagged
// builds report the tag (pre-release while `v0.y.z`, stable from
// `v1.0.0`). Per docs/decisions/0013-versioning-and-release-automation.md
// there is no in-source version literal to bump on release.
inline void CdcVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
#ifdef EXT_VERSION_DUCKLAKE_CDC
	const char *version_string = "ducklake_cdc " EXT_VERSION_DUCKLAKE_CDC;
#else
	const char *version_string = "ducklake_cdc unknown";
#endif
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto out = ConstantVector::GetData<string_t>(result);
	out[0] = StringVector::AddString(result, version_string);
}

static void LoadInternal(ExtensionLoader &loader) {
	auto cdc_version_function = ScalarFunction("cdc_version", {}, LogicalType::VARCHAR, CdcVersionScalarFun);
	loader.RegisterFunction(cdc_version_function);
	duckdb_cdc::RegisterConsumerStateFunctions(loader);

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
#ifdef EXT_VERSION_DUCKLAKE_CDC
	return EXT_VERSION_DUCKLAKE_CDC;
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
