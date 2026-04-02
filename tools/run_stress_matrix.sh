#!/bin/bash
#
# RocksDB Stress Test Matrix (General-purpose, Sandcastle-aligned)
#
# Mirrors the Meta internal Sandcastle/Skycastle crash test coverage:
# 3 build variants (debug, asan, tsan) x 22 test modes = 66 total test runs.
#
# Test modes match crash_test.mk targets used by Sandcastle:
#   Core (4): blackbox, simple-blackbox, whitebox, simple-whitebox
#   Atomic flush (2): cf_consistency blackbox/whitebox
#   Transactions (6): wc/wp/wup_txn blackbox/whitebox
#   Optimistic txn (2): optimistic_txn blackbox/whitebox
#   Best efforts recovery (1): blackbox only
#   Timestamps (2): enable_ts blackbox/whitebox
#   Tiered storage (2): tiered_storage blackbox/whitebox
#   Multi-ops txn (3): multiops wc/wp/wup blackbox only
#
# Concurrent-safe: every temp path is isolated by a repo-path SLUG, so
# separate worktrees on the same commit can run in parallel without colliding.
#
# Usage:
#   tools/run_stress_matrix.sh [OPTIONS]
#
# Options:
#   --repo DIR       Path to RocksDB repo (default: current directory)
#   --parallel N     Number of parallel test slots (default: 8)
#   --batches LIST   Comma-separated durations in seconds (default: 300,600,1800,3600)
#   --variants LIST  Comma-separated variants (default: debug,asan,tsan)
#   --modes LIST     Mode groups to run (default: all). Options: core,atomic_flush,
#                    txn,optimistic_txn,best_efforts,ts,tiered_storage,multiops
#   --jobs N         Build parallelism (default: detected CPU count)
#   --extra-flags F  Extra flags passed to db_crashtest.py
#   --skip-build     Skip building, reuse existing worktree binaries
#   --stop           Stop a running matrix for this repo and clean repo-scoped
#                    temp worktrees/DB dirs
#   --help           Show this help
#
# Examples:
#   tools/run_stress_matrix.sh --repo ~/workspace/ws21/rocksdb --modes core --batches 300
#   tools/run_stress_matrix.sh --repo ~/workspace/ws21/rocksdb
#   tools/run_stress_matrix.sh --variants asan --modes core,txn --batches 1800
#

resolve_path() {
    if command -v readlink >/dev/null 2>&1; then
        readlink -f -- "$1" 2>/dev/null && return 0
    fi
    (
        cd -- "$(dirname -- "$1")" >/dev/null 2>&1 &&
        printf '%s/%s\n' "$(pwd -P)" "$(basename -- "$1")"
    )
}

usage() {
    cat <<'EOF'
Usage:
  tools/run_stress_matrix.sh [OPTIONS]

Options:
  --repo DIR       Path to RocksDB repo (default: current directory)
  --parallel N     Number of parallel test slots (default: 8)
  --batches LIST   Comma-separated durations in seconds (default: 300,600,1800,3600)
  --variants LIST  Comma-separated variants (default: debug,asan,tsan)
  --modes LIST     Mode groups to run (default: all)
  --jobs N         Build parallelism (default: detected CPU count)
  --extra-flags F  Extra flags passed to db_crashtest.py
  --skip-build     Skip building, reuse existing worktree binaries
  --stop           Stop a running matrix for this repo and clean repo-scoped temp dirs
  --help           Show this help
EOF
}

detect_cpu_count() {
    local cpu_count
    cpu_count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    if [ -z "$cpu_count" ]; then
        cpu_count="$(nproc 2>/dev/null || true)"
    fi
    if [ -z "$cpu_count" ]; then
        cpu_count="$(sysctl -n hw.logicalcpu 2>/dev/null || true)"
    fi
    case "$cpu_count" in
        ''|*[!0-9]*) cpu_count=8 ;;
    esac
    printf '%s\n' "$cpu_count"
}

hash_string() {
    if command -v md5sum >/dev/null 2>&1; then
        printf '%s' "$1" | md5sum | awk '{print substr($1, 1, 8)}'
        return 0
    fi
    if command -v md5 >/dev/null 2>&1; then
        printf '%s' "$1" | md5 | awk '{print substr($NF, 1, 8)}'
        return 0
    fi
    printf '%s' "$1" | sha256sum | awk '{print substr($1, 1, 8)}'
}

require_positive_integer() {
    local name="$1"
    local value="$2"
    case "$value" in
        ''|*[!0-9]*)
            echo "ERROR: ${name} must be a positive integer, got '${value}'"
            exit 1
            ;;
    esac
    if [ "$value" -le 0 ]; then
        echo "ERROR: ${name} must be > 0, got '${value}'"
        exit 1
    fi
}

list_child_pids() {
    local parent_pid="$1"
    ps -o pid= --ppid "$parent_pid" 2>/dev/null | awk '{print $1}'
}

collect_process_tree() {
    local root_pid="$1"
    local child_pid

    if ! kill -0 "$root_pid" 2>/dev/null; then
        return 0
    fi

    for child_pid in $(list_child_pids "$root_pid"); do
        collect_process_tree "$child_pid"
    done
    printf '%s\n' "$root_pid"
}

terminate_process_tree() {
    local root_pid="$1"
    local targets
    local pid
    local survivors=()

    targets="$(collect_process_tree "$root_pid")"
    if [ -z "$targets" ]; then
        return 0
    fi

    # shellcheck disable=SC2086
    kill -TERM $targets 2>/dev/null || true
    sleep 2

    for pid in $targets; do
        if kill -0 "$pid" 2>/dev/null; then
            survivors+=("$pid")
        fi
    done

    if [ "${#survivors[@]}" -gt 0 ]; then
        kill -KILL "${survivors[@]}" 2>/dev/null || true
        sleep 1
    fi
}

terminate_direct_children() {
    local child_pid
    for child_pid in $(list_child_pids "$$"); do
        terminate_process_tree "$child_pid"
    done
}

cleanup_owned_resources() {
    local wt

    rm -rf "${STRESS_TMPDIR:-}" 2>/dev/null || true

    for wt in /tmp/stress-wt-"${SLUG}"-*; do
        if [ ! -e "$wt" ]; then
            continue
        fi
        git -C "$REPO_DIR" worktree remove --force "$wt" 2>/dev/null || true
        rm -rf "$wt" 2>/dev/null || true
    done
}

shell_join() {
    local arg
    local quoted
    local parts=()
    for arg in "$@"; do
        printf -v quoted '%q' "$arg"
        parts+=("$quoted")
    done
    printf '%s' "${parts[*]}"
}

terminate_active_jobs() {
    local slot
    local pid
    for slot in "${!SLOT_PID[@]}"; do
        pid="${SLOT_PID[$slot]}"
        if [ -n "$pid" ]; then
            terminate_process_tree "$pid"
        fi
    done
}

cleanup_on_exit() {
    local exit_code="$?"

    if [ "${CLEANUP_DONE}" = true ]; then
        return
    fi
    CLEANUP_DONE=true
    trap - EXIT INT TERM

    terminate_active_jobs
    terminate_direct_children
    rm -f "$PIDFILE"

    if [ "${INTERRUPTED}" = true ]; then
        cleanup_owned_resources
    fi

    if [ "${INTERRUPTED}" = true ]; then
        echo ""
        echo "Interrupted. Cleaned up repo-scoped temp resources for SLUG ${SLUG}."
        if [ -n "$RESULTS_DIR" ]; then
            echo "Partial logs remain in: ${RESULTS_DIR}"
        fi
    fi

    exit "$exit_code"
}

SCRIPT_PATH="$(resolve_path "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd -- "$(dirname -- "$SCRIPT_PATH")" && pwd -P)"
CPU_COUNT="$(detect_cpu_count)"

PARALLEL=8
BATCHES="300,600,1800,3600"
VARIANTS="debug,asan,tsan"
MODES="all"
JOBS="$CPU_COUNT"
EXTRA_FLAGS=""
SKIP_BUILD=false
REPO_DIR=""
STOP_MODE=false

# Mode arrays
CORE_MODES=("blackbox|blackbox" "simple-blackbox|--simple blackbox" "whitebox|whitebox" "simple-whitebox|--simple whitebox")
ATOMIC_FLUSH_MODES=("atomic-flush-blackbox|--cf_consistency blackbox" "atomic-flush-whitebox|--cf_consistency whitebox")
TXN_MODES=("wc-txn-blackbox|--txn blackbox --txn_write_policy 0" "wc-txn-whitebox|--txn whitebox --txn_write_policy 0" "wp-txn-blackbox|--txn blackbox --txn_write_policy 1" "wp-txn-whitebox|--txn whitebox --txn_write_policy 1" "wup-txn-blackbox|--txn blackbox --txn_write_policy 2" "wup-txn-whitebox|--txn whitebox --txn_write_policy 2")
OPTIMISTIC_TXN_MODES=("optimistic-txn-blackbox|--optimistic_txn blackbox" "optimistic-txn-whitebox|--optimistic_txn whitebox")
BEST_EFFORTS_MODES=("best-efforts-blackbox|--test_best_efforts_recovery blackbox")
TS_MODES=("ts-blackbox|--enable_ts blackbox" "ts-whitebox|--enable_ts whitebox")
TIERED_STORAGE_MODES=("tiered-blackbox|--test_tiered_storage blackbox" "tiered-whitebox|--test_tiered_storage whitebox")
MULTIOPS_MODES=("multiops-wc-blackbox|--test_multiops_txn --txn_write_policy 0 blackbox" "multiops-wp-blackbox|--test_multiops_txn --txn_write_policy 1 blackbox" "multiops-wup-blackbox|--test_multiops_txn --txn_write_policy 2 blackbox")

# Priority-ordered for coverage-optimal scheduling
PRIORITY_ORDERED_MODES=(
    "blackbox|blackbox" "whitebox|whitebox"
    "atomic-flush-blackbox|--cf_consistency blackbox"
    "wc-txn-blackbox|--txn blackbox --txn_write_policy 0"
    "optimistic-txn-blackbox|--optimistic_txn blackbox"
    "ts-blackbox|--enable_ts blackbox"
    "tiered-blackbox|--test_tiered_storage blackbox"
    "best-efforts-blackbox|--test_best_efforts_recovery blackbox"
    "multiops-wc-blackbox|--test_multiops_txn --txn_write_policy 0 blackbox"
    "atomic-flush-whitebox|--cf_consistency whitebox"
    "wc-txn-whitebox|--txn whitebox --txn_write_policy 0"
    "optimistic-txn-whitebox|--optimistic_txn whitebox"
    "ts-whitebox|--enable_ts whitebox"
    "tiered-whitebox|--test_tiered_storage whitebox"
    "simple-blackbox|--simple blackbox"
    "wp-txn-blackbox|--txn blackbox --txn_write_policy 1"
    "wup-txn-blackbox|--txn blackbox --txn_write_policy 2"
    "multiops-wp-blackbox|--test_multiops_txn --txn_write_policy 1 blackbox"
    "multiops-wup-blackbox|--test_multiops_txn --txn_write_policy 2 blackbox"
    "simple-whitebox|--simple whitebox"
    "wp-txn-whitebox|--txn whitebox --txn_write_policy 1"
    "wup-txn-whitebox|--txn whitebox --txn_write_policy 2"
)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo) REPO_DIR="$2"; shift 2 ;;
        --parallel) PARALLEL="$2"; shift 2 ;;
        --batches) BATCHES="$2"; shift 2 ;;
        --variants) VARIANTS="$2"; shift 2 ;;
        --modes) MODES="$2"; shift 2 ;;
        --jobs) JOBS="$2"; shift 2 ;;
        --extra-flags) EXTRA_FLAGS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --stop) STOP_MODE=true; shift ;;
        --help) usage; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

require_positive_integer "parallel" "$PARALLEL"
require_positive_integer "jobs" "$JOBS"

if [ -z "$REPO_DIR" ]; then
    REPO_DIR="$(pwd -P)"
fi
REPO_DIR="$(cd "$REPO_DIR" && pwd -P)"

if [ ! -f "$REPO_DIR/tools/db_crashtest.py" ]; then
    echo "ERROR: $REPO_DIR does not look like a RocksDB repo"
    exit 1
fi

cd "$REPO_DIR"
SLUG="$(hash_string "$REPO_DIR")"
PIDFILE="/tmp/stress-matrix-${SLUG}.pid"
STRESS_TMPDIR="/tmp/stress-db-${SLUG}"
RESULTS_DIR=""
INTERRUPTED=false
CLEANUP_DONE=false

declare -A SLOT_PID=()
declare -A SLOT_LABEL=()

if [ "$STOP_MODE" = true ]; then
    if [ ! -f "$PIDFILE" ]; then
        echo "No running matrix found for $REPO_DIR (no PID file at $PIDFILE)"
        cleanup_owned_resources
        exit 0
    fi

    MATRIX_PID="$(cat "$PIDFILE")"
    if ! kill -0 "$MATRIX_PID" 2>/dev/null; then
        echo "Matrix PID $MATRIX_PID is not running (stale PID file). Cleaning up."
        rm -f "$PIDFILE"
        cleanup_owned_resources
        exit 0
    fi

    echo "Stopping stress matrix for $REPO_DIR (PID $MATRIX_PID, SLUG $SLUG)..."
    terminate_process_tree "$MATRIX_PID"
    rm -f "$PIDFILE"
    cleanup_owned_resources
    echo "Stopped and cleaned up repo-scoped temp resources."
    exit 0
fi

if [ -f "$PIDFILE" ]; then
    OLD_PID="$(cat "$PIDFILE")"
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "ERROR: Another stress matrix for this repo is already running (PID $OLD_PID)"
        echo "Stop it first: $(shell_join "$SCRIPT_PATH" --repo "$REPO_DIR" --stop)"
        exit 1
    fi
    rm -f "$PIDFILE"
fi

echo $$ > "$PIDFILE"
trap 'INTERRUPTED=true; exit 130' INT
trap 'INTERRUPTED=true; exit 143' TERM
trap 'cleanup_on_exit' EXIT

ALL_TEST_MODES=()
IFS=',' read -ra MODE_GROUPS <<< "$MODES"
for group in "${MODE_GROUPS[@]}"; do
    case "$group" in
        all)             ALL_TEST_MODES+=("${PRIORITY_ORDERED_MODES[@]}") ;;
        core)            ALL_TEST_MODES+=("${CORE_MODES[@]}") ;;
        atomic_flush)    ALL_TEST_MODES+=("${ATOMIC_FLUSH_MODES[@]}") ;;
        txn)             ALL_TEST_MODES+=("${TXN_MODES[@]}") ;;
        optimistic_txn)  ALL_TEST_MODES+=("${OPTIMISTIC_TXN_MODES[@]}") ;;
        best_efforts)    ALL_TEST_MODES+=("${BEST_EFFORTS_MODES[@]}") ;;
        ts)              ALL_TEST_MODES+=("${TS_MODES[@]}") ;;
        tiered_storage)  ALL_TEST_MODES+=("${TIERED_STORAGE_MODES[@]}") ;;
        multiops)        ALL_TEST_MODES+=("${MULTIOPS_MODES[@]}") ;;
        *) echo "ERROR: Unknown mode group '$group'"; exit 1 ;;
    esac
done

IFS=',' read -ra VARIANT_ARR <<< "$VARIANTS"
IFS=',' read -ra BATCH_ARR <<< "$BATCHES"
NUM_MODES=${#ALL_TEST_MODES[@]}
TOTAL_TESTS=$(( ${#VARIANT_ARR[@]} * NUM_MODES ))

echo "============================================="
echo "RocksDB Stress Test Matrix (Sandcastle-aligned)"
echo "============================================="
echo "Script:     $SCRIPT_PATH"
echo "Repo:       $REPO_DIR"
echo "SLUG:       $SLUG"
echo "Variants:   ${VARIANT_ARR[*]}"
echo "Modes:      $NUM_MODES test modes"
echo "Parallel:   $PARALLEL concurrent tests"
echo "Batches:    ${BATCH_ARR[*]} seconds"
echo "Build jobs: $JOBS"
echo "Extra:      $EXTRA_FLAGS"
echo "Start:      $(date)"
echo "Total tests per batch: ${#VARIANT_ARR[@]} variants x $NUM_MODES modes = ${TOTAL_TESTS}"
echo "============================================="

FREE_GB="$(free -g 2>/dev/null | awk '/Mem/{print $7}')"
if [ -n "$FREE_GB" ] && [ "$FREE_GB" -lt 50 ] 2>/dev/null; then
    echo ""
    echo "WARNING: Only ${FREE_GB}GB free memory. Builds may OOM. Consider killing stale buck2/EdenFS."
fi

if [ "$SKIP_BUILD" = false ]; then
    echo ""
    echo "=== Building ${#VARIANT_ARR[@]} variants SEQUENTIALLY ==="
    for variant in "${VARIANT_ARR[@]}"; do
        WT="/tmp/stress-wt-${SLUG}-${variant}"
        git -C "$REPO_DIR" worktree remove --force "$WT" 2>/dev/null || true
        rm -rf "$WT" 2>/dev/null || true
        git -C "$REPO_DIR" worktree add "$WT" "$(git -C "$REPO_DIR" rev-parse HEAD)" 2>/dev/null
        BUILD_LOG="${WT}/build.log"
        echo "  Building ${variant} in ${WT}..."
        (
            cd "$WT"
            case "$variant" in
                debug) make -j"${JOBS}" db_stress 2>&1 ;;
                asan)  COMPILE_WITH_ASAN=1 CC=clang CXX=clang++ USE_CLANG=1 make -j"${JOBS}" db_stress 2>&1 ;;
                tsan)  COMPILE_WITH_TSAN=1 CC=clang CXX=clang++ USE_CLANG=1 make -j"${JOBS}" db_stress 2>&1 ;;
                *) echo "Unknown variant: ${variant}"; exit 1 ;;
            esac
        ) > "$BUILD_LOG" 2>&1
        tail -3 "$BUILD_LOG"
        BIN="${WT}/db_stress"
        if [ ! -f "$BIN" ]; then
            echo "FATAL: $BIN not found after build!"
            echo "--- Build errors (last 30 lines) ---"
            grep -i 'error' "$BUILD_LOG" | tail -10
            tail -30 "$BUILD_LOG"
            echo "Full build log: $BUILD_LOG"
            exit 1
        fi
        echo "  OK: ${variant} ($(du -sh "$BIN" | cut -f1))"
    done
    echo "Builds done: $(date)"
else
    echo "=== Skipping build (--skip-build) ==="
    for variant in "${VARIANT_ARR[@]}"; do
        BIN="/tmp/stress-wt-${SLUG}-${variant}/db_stress"
        if [ ! -f "$BIN" ]; then
            echo "FATAL: $BIN not found!"
            exit 1
        fi
    done
fi

RESULTS_DIR="/tmp/stress-results-${SLUG}-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"
echo "Results: $RESULTS_DIR"

mkdir -p "$STRESS_TMPDIR"

MATRIX_START_TIME="$(date +%s)"
GLOBAL_PASSED=0
GLOBAL_FAILED=0

launch_job() {
    local slot="$1"
    local idx="$2"
    local variant="${JOB_VARIANTS[$idx]}"
    local mode_name="${JOB_MODE_NAMES[$idx]}"
    local mode_args="${JOB_MODE_ARGS[$idx]}"
    local WT="/tmp/stress-wt-${SLUG}-${variant}"
    local LABEL="${variant}-${mode_name}"
    local LOG="${BATCH_DIR}/${LABEL}.log"
    local TEST_DB_DIR="${STRESS_TMPDIR}/${LABEL}"
    local VARIANT_OPS_FLAG=""

    mkdir -p "$TEST_DB_DIR"

    if [ "$duration" -le 600 ]; then
        case "$mode_name" in
            *whitebox*)
                case "$variant" in
                    asan|tsan) VARIANT_OPS_FLAG="--ops_per_thread=100 --reopen=5" ;;
                esac
                ;;
        esac
    fi

    (
        cd "$WT"
        export TEST_TMPDIR="$TEST_DB_DIR"
        # shellcheck disable=SC2086
        python3 tools/db_crashtest.py \
            --stress_cmd="$WT/db_stress" \
            --duration="$duration" \
            --destroy_db_initially=1 \
            $VARIANT_OPS_FLAG $mode_args $EXTRA_FLAGS \
            > "$LOG" 2>&1
        EXIT_CODE=$?
        echo "EXIT: $EXIT_CODE" >> "$LOG"
        exit "$EXIT_CODE"
    ) &

    SLOT_PID[$slot]=$!
    SLOT_LABEL[$slot]=$LABEL
    echo "  [slot $slot] Started [$(( idx + 1 ))/${TOTAL_JOBS}]: ${LABEL} (pid=${SLOT_PID[$slot]})"
}

for duration in "${BATCH_ARR[@]}"; do
    BATCH_DIR="${RESULTS_DIR}/batch-${duration}s"
    mkdir -p "$BATCH_DIR"

    echo ""
    echo "============================================="
    echo "=== BATCH: ${duration}s -- PIPELINE MODE, ${PARALLEL} slots ($(date)) ==="
    echo "============================================="

    declare -a JOB_VARIANTS=()
    declare -a JOB_MODE_NAMES=()
    declare -a JOB_MODE_ARGS=()
    declare -A SLOT_PID=()
    declare -A SLOT_LABEL=()

    NUM_VARIANTS=${#VARIANT_ARR[@]}
    for (( pass=0; pass<NUM_VARIANTS; pass++ )); do
        for (( mi=0; mi<${#ALL_TEST_MODES[@]}; mi++ )); do
            mode_entry="${ALL_TEST_MODES[$mi]}"
            mode_name="${mode_entry%%|*}"
            mode_args="${mode_entry#*|}"
            vi=$(( (mi + pass) % NUM_VARIANTS ))
            JOB_VARIANTS+=("${VARIANT_ARR[$vi]}")
            JOB_MODE_NAMES+=("$mode_name")
            JOB_MODE_ARGS+=("$mode_args")
        done
    done

    TOTAL_JOBS=${#JOB_VARIANTS[@]}
    NEXT_JOB=0
    ACTIVE=0
    ANY_FAIL=false
    FAILURES=()

    for (( slot=0; slot<PARALLEL && NEXT_JOB<TOTAL_JOBS; slot++ )); do
        launch_job "$slot" "$NEXT_JOB"
        NEXT_JOB=$(( NEXT_JOB + 1 ))
        ACTIVE=$(( ACTIVE + 1 ))
    done

    while [ "$ACTIVE" -gt 0 ]; do
        for slot in "${!SLOT_PID[@]}"; do
            pid="${SLOT_PID[$slot]}"
            label="${SLOT_LABEL[$slot]}"

            if kill -0 "$pid" 2>/dev/null; then
                continue
            fi

            wait "$pid"
            exit_code=$?
            unset "SLOT_PID[$slot]" "SLOT_LABEL[$slot]"
            ACTIVE=$(( ACTIVE - 1 ))

            if [ "$exit_code" -ne 0 ]; then
                echo "  FAILED: ${label} (exit=${exit_code})"
                ANY_FAIL=true
                FAILURES+=("$label")
                GLOBAL_FAILED=$(( GLOBAL_FAILED + 1 ))
                echo "  Stopping all running tests..."
                terminate_active_jobs
                ACTIVE=0
                break
            fi

            echo "  PASSED: ${label}"
            GLOBAL_PASSED=$(( GLOBAL_PASSED + 1 ))

            if [ "$NEXT_JOB" -lt "$TOTAL_JOBS" ] && [ "$ANY_FAIL" = false ]; then
                launch_job "$slot" "$NEXT_JOB"
                NEXT_JOB=$(( NEXT_JOB + 1 ))
                ACTIVE=$(( ACTIVE + 1 ))
            fi
        done

        if [ "$ACTIVE" -gt 0 ]; then
            sleep 2
        fi
    done

    unset JOB_VARIANTS JOB_MODE_NAMES JOB_MODE_ARGS SLOT_PID SLOT_LABEL

    if [ "$ANY_FAIL" = true ]; then
        echo ""
        echo "!!! FAILURES in batch ${duration}s: ${FAILURES[*]} !!!"
        echo ""
        echo "Preserving crash DB LOG files from failed tests..."
        for label in "${FAILURES[@]}"; do
            TEST_DB_DIR="${STRESS_TMPDIR}/${label}"
            for db_dir in "$TEST_DB_DIR"/rocksdb_crashtest_*; do
                if [ -d "$db_dir" ] && [ -f "$db_dir/LOG" ]; then
                    db_name="$(basename "$db_dir")"
                    cp "$db_dir/LOG" "${BATCH_DIR}/${label}-${db_name}.LOG" 2>/dev/null
                    for old_log in "$db_dir"/LOG.old.*; do
                        if [ -f "$old_log" ]; then
                            cp "$old_log" "${BATCH_DIR}/${label}-${db_name}.$(basename "$old_log")" 2>/dev/null
                        fi
                    done
                    echo "  Saved LOG from ${label}: $db_dir"
                fi
            done
        done
        echo ""
        for label in "${FAILURES[@]}"; do
            echo "--- ${label} (last 30 lines) ---"
            tail -30 "${BATCH_DIR}/${label}.log"
            echo ""
        done
        echo "Full logs: ${BATCH_DIR}/"
        echo "DB artifacts preserved in: ${STRESS_TMPDIR}/"

        FAILURES_FILE="${BATCH_DIR}/failures.txt"
        echo "Stress matrix failures -- batch ${duration}s" > "$FAILURES_FILE"
        echo "Date: $(date)" >> "$FAILURES_FILE"
        echo "" >> "$FAILURES_FILE"
        for label in "${FAILURES[@]}"; do
            echo "=== $label ===" >> "$FAILURES_FILE"
            logf="${BATCH_DIR}/${label}.log"
            grep -m5 "SUMMARY\|Corruption\|Invalid blob\|Verification failed\|No such file\|SafeTerminate\|ERROR.*Sanitizer\|data race\|Assertion\|stack-use-after\|heap-use-after" "$logf" >> "$FAILURES_FILE" 2>/dev/null
            echo "" >> "$FAILURES_FILE"
        done

        ELAPSED=$(( $(date +%s) - MATRIX_START_TIME ))
        echo ""
        echo "=== FINAL SUMMARY ==="
        echo "Result:       FAILED"
        echo "Tests run:    $((GLOBAL_PASSED + GLOBAL_FAILED))"
        echo "Passed:       $GLOBAL_PASSED"
        echo "Failed:       $GLOBAL_FAILED (${FAILURES[*]})"
        echo "Elapsed:      ${ELAPSED}s"
        echo "Results dir:  $RESULTS_DIR"
        echo "Failures:     ${FAILURES_FILE}"
        echo "=== END SUMMARY ==="

        exit 1
    fi

    echo "=== Batch ${duration}s: ALL ${TOTAL_JOBS} PASSED (pipeline) ==="
    rm -rf "${STRESS_TMPDIR:?}"/* 2>/dev/null || true
done

echo ""
echo "============================================="
echo "=== ALL BATCHES PASSED! ==="
echo "=== ${#BATCH_ARR[@]} batches x ${TOTAL_TESTS} tests each ==="
echo "=== ${#VARIANT_ARR[@]} variants x ${NUM_MODES} modes ==="
echo "=== Results: ${RESULTS_DIR} ==="
echo "============================================="

ELAPSED=$(( $(date +%s) - MATRIX_START_TIME ))
echo ""
echo "=== FINAL SUMMARY ==="
echo "Result:       PASSED"
echo "Tests run:    $GLOBAL_PASSED"
echo "Passed:       $GLOBAL_PASSED"
echo "Failed:       0"
echo "Elapsed:      ${ELAPSED}s"
echo "Results dir:  $RESULTS_DIR"
echo "=== END SUMMARY ==="

rm -rf "${STRESS_TMPDIR:?}" 2>/dev/null || true
