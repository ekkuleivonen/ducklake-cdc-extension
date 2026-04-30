# This file is included by DuckDB's build system. It specifies which extension to load
# into the build, following the canonical DuckDB community-extension shape.
#
# We intentionally do NOT compile DuckLake from source here. Tests that need
# DuckLake load it at runtime via `INSTALL ducklake; LOAD ducklake;` against the
# official prebuilt binary for the DuckDB target version. This is the pattern
# DuckLake itself follows for sqlite_scanner / postgres_scanner — leaf
# extensions only build themselves and pull dependencies as binaries.

duckdb_extension_load(ducklake_cdc
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# Bundled DuckDB extensions used by DuckLake-touching tests. These live in the
# DuckDB submodule (no external git pull) and are stable enough that compiling
# them in is the cheapest way to make `require icu` pass without depending on
# runtime extension loading — which is broken in debug builds with ASAN, since
# loaded shared libraries don't carry the sanitizer runtime metadata.
duckdb_extension_load(icu)
