PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ducklake_cdc
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

SQL_TEST_SMOKE=test/sql/ducklake_cdc.test, test/sql/compat_check.test
SQL_TEST_DEFAULT=test/sql/ducklake_cdc.test, test/sql/compat_check.test, test/sql/recent_sugar.test, test/sql/notices_validation.test, test/sql/observability.test, test/sql/ddl_stage2.test

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: test_debug_smoke test_debug_default test_debug_full test_release_smoke test_release_default test_release_full

test_debug_smoke:
	./build/debug/$(TEST_PATH) "$(SQL_TEST_SMOKE)"

test_debug_default:
	./build/debug/$(TEST_PATH) "$(SQL_TEST_DEFAULT)"

test_debug_full: test_debug

test_release_smoke:
	./build/release/$(TEST_PATH) "$(SQL_TEST_SMOKE)"

test_release_default:
	./build/release/$(TEST_PATH) "$(SQL_TEST_DEFAULT)"

test_release_full: test
