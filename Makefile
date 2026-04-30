PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ducklake_cdc
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# DuckLake is loaded at runtime from the official extensions.duckdb.org
# repository instead of compiled from source. The unittest binary needs the
# autoinstall/autoload code paths compiled in so that explicit INSTALL/LOAD
# statements work; SQL tests that need DuckLake do `INSTALL ducklake;
# LOAD ducklake;` explicitly rather than `require ducklake`, because the
# `require <ext>` directive in sqllogictest only checks for statically-loaded
# extensions and we don't statically link DuckLake.
ENABLE_EXTENSION_AUTOLOADING=1
ENABLE_EXTENSION_AUTOINSTALL=1

SQL_TEST_SMOKE=test/sql/ducklake_cdc.test, test/sql/compat_check.test
SQL_TEST_DEFAULT=test/sql/ducklake_cdc.test, test/sql/compat_check.test, test/sql/recent_sugar.test, test/sql/notices_validation.test, test/sql/observability.test, test/sql/ddl_stage2.test

# DuckLake binary cache layout: ~/.duckdb/extensions/<version>/<platform>/
# Pre-staging the binary here lets sqllogictest's `INSTALL ducklake` resolve
# to a local file instead of hitting extensions.duckdb.org over HTTP, which
# (a) is what causes the silent "skip on error_message matching 'HTTP'" path
# in CI runners and (b) keeps the iteration loop offline-friendly.
DUCKDB_VERSION ?= $(shell cat ${PROJ_DIR}.github/duckdb-version)
DUCKDB_PLATFORM ?= $(shell uname -s | tr '[:upper:]' '[:lower:]' | sed -e 's/darwin/osx/')_$(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')
EXT_CACHE_DIR ?= $(HOME)/.duckdb/extensions/${DUCKDB_VERSION}/${DUCKDB_PLATFORM}
EXT_REPO_BASE ?= http://extensions.duckdb.org

# Force libduckdb to embed the pinned DuckDB version so it doesn't fall back
# to `git describe` on the submodule (which returns "v0.0.1" in CI when the
# fetch-depth is shallow and no tag is local). Pinning here also keeps
# DuckLake's LOAD-time version check happy: the prebuilt `ducklake.duckdb_extension`
# we cache via `prepare_tests` is stamped for `$(DUCKDB_VERSION)` and DuckDB
# refuses to load it if the host disagrees.
OVERRIDE_GIT_DESCRIBE ?= ${DUCKDB_VERSION}
export OVERRIDE_GIT_DESCRIBE

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: prepare_tests test_debug_smoke test_debug_default test_debug_full test_release_smoke test_release_default test_release_full

# Pre-fetch the official ducklake binary for our DuckDB target into the local
# extension cache so the `INSTALL ducklake` statement in SQL tests resolves
# from disk instead of going over HTTP. CI runners trip the sqllogictest
# default "skip on error_message matching 'HTTP'" rule otherwise, which
# silently turns failing tests into green-skipped ones.
prepare_tests:
	@mkdir -p "$(EXT_CACHE_DIR)"
	@if [ ! -f "$(EXT_CACHE_DIR)/ducklake.duckdb_extension" ]; then \
		echo "Fetching ducklake.duckdb_extension for $(DUCKDB_VERSION)/$(DUCKDB_PLATFORM)"; \
		curl -fsSL "$(EXT_REPO_BASE)/$(DUCKDB_VERSION)/$(DUCKDB_PLATFORM)/ducklake.duckdb_extension.gz" \
			| gunzip > "$(EXT_CACHE_DIR)/ducklake.duckdb_extension.tmp"; \
		mv "$(EXT_CACHE_DIR)/ducklake.duckdb_extension.tmp" "$(EXT_CACHE_DIR)/ducklake.duckdb_extension"; \
	fi

test_debug_smoke: prepare_tests
	./build/debug/$(TEST_PATH) "$(SQL_TEST_SMOKE)"

test_debug_default: prepare_tests
	./build/debug/$(TEST_PATH) "$(SQL_TEST_DEFAULT)"

test_debug_full: prepare_tests test_debug

test_release_smoke: prepare_tests
	./build/release/$(TEST_PATH) "$(SQL_TEST_SMOKE)"

test_release_default: prepare_tests
	./build/release/$(TEST_PATH) "$(SQL_TEST_DEFAULT)"

test_release_full: prepare_tests test
