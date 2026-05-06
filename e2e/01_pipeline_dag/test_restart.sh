#!/usr/bin/env bash
#
# Kill+restart correctness pass for 01_pipeline_dag.
#
# What this proves:
#
# 1. Each stage's CDC cursor is durable across process restarts: the
#    second run picks up at the snapshot the first run last committed,
#    not at "now".
# 2. Exactly-once at every stage edge survives the restart:
#    joined_orders has no duplicate purchase_ids (count(DISTINCT
#    purchase_id) == count(*)), and clean_* row counts equal raw_*
#    row counts (graceful drain on shutdown means no in-flight
#    batches were lost).
# 3. The cursor advances strictly monotonically: each consumer's
#    last_committed_snapshot in run 2 is strictly greater than its
#    last_committed_snapshot in run 1.
#
# How:
#
# - Reset the lake once at the top so we start hermetically.
# - Run 1 with ``--keep-state`` (so cleanup-on-exit is skipped),
#   ``--duration N``. Snapshot row counts + cursor positions.
# - Run 2 with the same flags. Snapshot row counts + cursor positions.
# - Assert: rows monotonically grew, distinct purchase_ids in
#   joined_orders equals total joined_orders rows (no replay
#   duplicates), and every cursor advanced.
# - Reset on success so the next run is hermetic again. (On failure,
#   the lake is left dirty so the operator can poke around.)
#
# Run from anywhere:
#   bash e2e/01_pipeline_dag/test_restart.sh
#
# Or from e2e/:
#   bash 01_pipeline_dag/test_restart.sh

set -euo pipefail

# Always operate from the e2e/ root so ``uv run`` picks up the
# pyproject.toml and the per-example imports work.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_ROOT="$(cd "$HERE/.." && pwd)"
cd "$E2E_ROOT"

DURATION="${TEST_RESTART_DURATION:-8}"
APP="01_pipeline_dag/app.py"
INSPECT="01_pipeline_dag/inspect_lake.py"

run_app() {
    # Filter out the known-noise CDC_WAIT_SHARED_CONNECTION advisory
    # so the test output stays focused on real signals.
    uv run python "$APP" --headless --duration "$DURATION" --keep-state \
        2>&1 | grep -v CDC_WAIT_SHARED_CONNECTION
}

inspect() {
    uv run python "$INSPECT"
}

# Helpers: parse "key=value" lines into bash-friendly assertions.
get() {
    # $1 = inspect output, $2 = key
    echo "$1" | grep "^$2=" | head -n1 | cut -d= -f2-
}

assert_eq() {
    local label="$1" actual="$2" expected="$3"
    if [[ "$actual" != "$expected" ]]; then
        echo "FAIL: $label: expected $expected, got $actual" >&2
        exit 1
    fi
    echo "  ok   $label = $actual"
}

assert_gt() {
    local label="$1" actual="$2" floor="$3"
    if (( actual <= floor )); then
        echo "FAIL: $label: expected > $floor, got $actual" >&2
        exit 1
    fi
    echo "  ok   $label = $actual (> $floor)"
}

# ---------------------------------------------------------------------------
# Hermetic start
# ---------------------------------------------------------------------------
echo ">>> resetting lake (hermetic start)"
uv run python -c "
import sys
from pathlib import Path
sys.path.insert(0, '.')
from _lib.config import reset_lake
reset_lake(example='01_pipeline_dag', catalog='postgres', storage='disk')
"

# ---------------------------------------------------------------------------
# Run 1
# ---------------------------------------------------------------------------
echo ">>> run 1 (${DURATION}s --keep-state)"
run_app
SNAP1="$(inspect)"
echo "$SNAP1" | sed 's/^/  /'

R1_RAW_P=$(get "$SNAP1" raw_purchases)
R1_RAW_R=$(get "$SNAP1" raw_refunds)
R1_CLEAN_P=$(get "$SNAP1" clean_purchases)
R1_CLEAN_R=$(get "$SNAP1" clean_refunds)
R1_JOINED=$(get "$SNAP1" joined_orders)
R1_JOINED_DISTINCT=$(get "$SNAP1" joined_orders_distinct_purchase_ids)
R1_CUR_NP=$(get "$SNAP1" cursor_normalize_purchases)
R1_CUR_NR=$(get "$SNAP1" cursor_normalize_refunds)
R1_CUR_J=$(get "$SNAP1" cursor_join_orders)

echo ">>> run 1 invariants"
assert_eq "raw_purchases  == clean_purchases (drain ok)" "$R1_RAW_P" "$R1_CLEAN_P"
assert_eq "raw_refunds    == clean_refunds (drain ok)"   "$R1_RAW_R" "$R1_CLEAN_R"
assert_eq "clean_purchases == joined_orders (drain ok)"  "$R1_CLEAN_P" "$R1_JOINED"
assert_eq "joined_orders  == distinct purchase_ids (no dup)" "$R1_JOINED" "$R1_JOINED_DISTINCT"

# ---------------------------------------------------------------------------
# Run 2 (continues from where run 1 left off)
# ---------------------------------------------------------------------------
echo ">>> run 2 (${DURATION}s --keep-state, continues from run 1)"
run_app
SNAP2="$(inspect)"
echo "$SNAP2" | sed 's/^/  /'

R2_RAW_P=$(get "$SNAP2" raw_purchases)
R2_RAW_R=$(get "$SNAP2" raw_refunds)
R2_CLEAN_P=$(get "$SNAP2" clean_purchases)
R2_CLEAN_R=$(get "$SNAP2" clean_refunds)
R2_JOINED=$(get "$SNAP2" joined_orders)
R2_JOINED_DISTINCT=$(get "$SNAP2" joined_orders_distinct_purchase_ids)
R2_CUR_NP=$(get "$SNAP2" cursor_normalize_purchases)
R2_CUR_NR=$(get "$SNAP2" cursor_normalize_refunds)
R2_CUR_J=$(get "$SNAP2" cursor_join_orders)

echo ">>> run 2 invariants (cumulative across restart)"
assert_eq "raw_purchases  == clean_purchases (drain ok)" "$R2_RAW_P" "$R2_CLEAN_P"
assert_eq "raw_refunds    == clean_refunds (drain ok)"   "$R2_RAW_R" "$R2_CLEAN_R"
assert_eq "clean_purchases == joined_orders (drain ok)"  "$R2_CLEAN_P" "$R2_JOINED"
assert_eq "joined_orders  == distinct purchase_ids (no dup, no replay)" \
    "$R2_JOINED" "$R2_JOINED_DISTINCT"

echo ">>> cross-run invariants (durable cursor advanced)"
assert_gt "raw_purchases   row growth"        "$R2_RAW_P" "$R1_RAW_P"
assert_gt "clean_purchases row growth"        "$R2_CLEAN_P" "$R1_CLEAN_P"
assert_gt "joined_orders   row growth"        "$R2_JOINED" "$R1_JOINED"
assert_gt "normalize_purchases cursor advanced" "$R2_CUR_NP" "$R1_CUR_NP"
assert_gt "normalize_refunds   cursor advanced" "$R2_CUR_NR" "$R1_CUR_NR"
assert_gt "join_orders         cursor advanced" "$R2_CUR_J"  "$R1_CUR_J"

# ---------------------------------------------------------------------------
# Cleanup on success (leave dirty on failure for inspection)
# ---------------------------------------------------------------------------
echo ">>> cleaning up lake"
uv run python -c "
import sys
from pathlib import Path
sys.path.insert(0, '.')
from _lib.config import reset_lake
reset_lake(example='01_pipeline_dag', catalog='postgres', storage='disk')
"

echo ">>> PASS: kill+restart correctness verified"
echo "    run 1: ${R1_JOINED} joined orders, cursor join_orders=${R1_CUR_J}"
echo "    run 2: ${R2_JOINED} joined orders, cursor join_orders=${R2_CUR_J}"
echo "    delta: $((R2_JOINED - R1_JOINED)) new joined orders across the restart"
