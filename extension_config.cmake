# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(ducklake_cdc
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# DuckLake CDC test paths exercise DuckLake queries that require ICU. Build it
# with the test binary instead of relying on extension autoloading, which has no
# local linux_amd64_musl binary in the CI container.
duckdb_extension_load(icu)

# Test-time dependency: the upstream `ducklake` extension. The compat-check
# tests under test/sql/ ATTACH a real DuckLake catalog so they exercise the
# same code path users hit at LOAD time. Pin this to the DuckLake commit from
# the DuckDB v1.5.1 submodule's bundled extension config
# (`duckdb/.github/config/extensions/ducklake.cmake`). Floating DuckLake here
# makes local builds fail as upstream DuckDB extension APIs move.
duckdb_extension_load(ducklake
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/ducklake
    GIT_TAG 67480b1d5c76f29276b4195bdd1175e2fe066236
)
