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

# `_NO_DUCKLAKE` set is the subset that does not `INSTALL ducklake; LOAD ducklake;`
# at runtime. It exists so the sanitiser CI lane (`make debug` + ASan/UBSan
# default flags) has something to run: the official prebuilt
# `ducklake.duckdb_extension` is a release build with no sanitiser runtime,
# and ASan refuses to LOAD a shared library that wasn't built with the
# matching runtime. Tests in the broader smoke / default sets that do
# `INSTALL ducklake` therefore can't run on the sanitiser binary; this
# subset is what does.
SQL_TEST_SMOKE_NO_DUCKLAKE=test/sql/ducklake_cdc.test
SQL_TEST_SMOKE=test/sql/ducklake_cdc.test, test/sql/compat_check.test
SQL_TEST_DEFAULT=test/sql/ducklake_cdc.test, test/sql/compat_check.test, test/sql/new_api_contract.test

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

.PHONY: prepare_tests install-git-hooks test_local_sanitizer test_local_full test_debug_smoke_no_ducklake test_debug_smoke test_debug_default test_debug_full test_release_smoke test_release_default test_release_full

install-git-hooks:
	git config core.hooksPath .githooks
	@echo "Installed git hooks from .githooks"

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

# Sanitiser-friendly smoke: no DuckLake LOAD, no `prepare_tests` cache fetch
# (we don't need the prebuilt). Used by the release-time sanitiser job.
test_debug_smoke_no_ducklake:
	./build/debug/$(TEST_PATH) "$(SQL_TEST_SMOKE_NO_DUCKLAKE)"

test_local_sanitizer:
	$(MAKE) debug
	$(MAKE) test_debug_smoke_no_ducklake

test_local_full:
	$(MAKE) release
	$(MAKE) test_release_default

test_debug_smoke:
	@echo "test_debug_smoke loads the prebuilt DuckLake extension, which is not sanitizer-compatible."
	@echo "Run 'make test_local_sanitizer' for ASan/UBSan coverage or 'make test_local_full' for full SQL coverage."
	@exit 2

test_debug_default:
	@echo "test_debug_default loads the prebuilt DuckLake extension, which is not sanitizer-compatible."
	@echo "Run 'make test_local_sanitizer' for ASan/UBSan coverage or 'make test_local_full' for full SQL coverage."
	@exit 2

test_debug_full:
	@echo "test_debug_full loads the prebuilt DuckLake extension, which is not sanitizer-compatible."
	@echo "Run 'make test_local_sanitizer' for ASan/UBSan coverage or 'make test_local_full' for full SQL coverage."
	@exit 2

test_release_smoke: prepare_tests
	./build/release/$(TEST_PATH) "$(SQL_TEST_SMOKE)"

test_release_default: prepare_tests
	./build/release/$(TEST_PATH) "$(SQL_TEST_DEFAULT)"

test_release_full: prepare_tests test
