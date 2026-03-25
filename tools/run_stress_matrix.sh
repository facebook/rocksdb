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
# Concurrent-safe: each run is isolated by a SLUG derived from the repo's
# HEAD commit hash. Multiple runs from different branches/commits can execute
# simultaneously without interfering with each other.
#
# Usage:
#   run_stress_matrix.sh [OPTIONS]
#
# Options:
#   --repo DIR       Path to RocksDB repo (default: current directory)
#   --parallel N     Number of parallel test slots (default: 8)
#   --batches LIST   Comma-separated durations in seconds (default: 300,600,1800,3600)
#   --variants LIST  Comma-separated variants (default: debug,asan,tsan)
#   --modes LIST     Mode groups to run (default: all). Options: core,atomic_flush,
#                    txn,optimistic_txn,best_efforts,ts,tiered_storage,multiops
#   --jobs N         Build parallelism (default: 128)
#   --extra-flags F  Extra flags passed to db_crashtest.py
#   --skip-build     Skip building, reuse existing worktree binaries
#   --stop           Stop a running matrix for this repo (requires --repo)
#   --help           Show this help
#
# Examples:
#   run_stress_matrix.sh --repo ~/workspace/ws21/rocksdb --modes core --batches 300
#   run_stress_matrix.sh --repo ~/workspace/ws21/rocksdb
#   run_stress_matrix.sh --variants asan --modes core,txn --batches 1800
#

PARALLEL=8
BATCHES="300,600,1800,3600"
VARIANTS="debug,asan,tsan"
MODES="all"
JOBS=128
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
    case $1 in
        --repo) REPO_DIR="$2"; shift 2 ;;
        --parallel) PARALLEL="$2"; shift 2 ;;
        --batches) BATCHES="$2"; shift 2 ;;
        --variants) VARIANTS="$2"; shift 2 ;;
        --modes) MODES="$2"; shift 2 ;;
        --jobs) JOBS="$2"; shift 2 ;;
        --extra-flags) EXTRA_FLAGS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --stop) STOP_MODE=true; shift ;;
        --help) sed -n '2,/^$/p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$REPO_DIR" ]; then REPO_DIR="$(pwd)"; fi
REPO_DIR="$(cd "$REPO_DIR" && pwd)"

if [ ! -f "$REPO_DIR/tools/db_crashtest.py" ]; then
    echo "ERROR: $REPO_DIR does not look like a RocksDB repo"; exit 1
fi

cd "$REPO_DIR"
SLUG=$(git rev-parse HEAD | head -c 8)

# --stop mode: kill a running matrix and its process group, then exit.
if [ "$STOP_MODE" = true ]; then
    PIDFILE="/tmp/stress-matrix-${SLUG}.pid"
    if [ ! -f "$PIDFILE" ]; then
        echo "No running matrix found for $REPO_DIR (no PID file at $PIDFILE)"
        exit 0
    fi
    MATRIX_PID=$(cat "$PIDFILE")
    if ! kill -0 "$MATRIX_PID" 2>/dev/null; then
        echo "Matrix PID $MATRIX_PID is not running (stale PID file). Cleaning up."
        rm -f "$PIDFILE"
        exit 0
    fi
    echo "Stopping stress matrix for $REPO_DIR (PID $MATRIX_PID, SLUG $SLUG)..."
    kill -TERM -- -"$MATRIX_PID" 2>/dev/null
    sleep 2
    if kill -0 "$MATRIX_PID" 2>/dev/null; then
        kill -KILL -- -"$MATRIX_PID" 2>/dev/null
        sleep 1
    fi
    rm -f "$PIDFILE"
    echo "Stopped."
    exit 0
fi

# Register PID for --stop support
PIDFILE="/tmp/stress-matrix-${SLUG}.pid"
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "ERROR: Another stress matrix for this repo is already running (PID $OLD_PID)"
        echo "Stop it first: $0 --repo $REPO_DIR --stop"
        exit 1
    fi
    rm -f "$PIDFILE"
fi
echo $$ > "$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT

# Build mode list
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

echo "============================================="
echo "RocksDB Stress Test Matrix (Sandcastle-aligned)"
echo "============================================="
echo "Repo:       $REPO_DIR"
echo "SLUG:       $SLUG"
echo "Variants:   ${VARIANT_ARR[*]}"
echo "Modes:      $NUM_MODES test modes"
echo "Parallel:   $PARALLEL concurrent tests"
echo "Batches:    ${BATCH_ARR[*]} seconds"
echo "Build jobs: $JOBS"
echo "Extra:      $EXTRA_FLAGS"
echo "Start:      $(date)"
echo "Total tests per batch: ${#VARIANT_ARR[@]} variants x $NUM_MODES modes = $(( ${#VARIANT_ARR[@]} * NUM_MODES ))"
echo "============================================="

# === BUILD PHASE ===
# Memory check: warn if <50GB available
FREE_GB=$(free -g 2>/dev/null | awk '/Mem/{print $7}')
if [ -n "$FREE_GB" ] && [ "$FREE_GB" -lt 50 ] 2>/dev/null; then
    echo ""
    echo "WARNING: Only ${FREE_GB}GB free memory. Builds may OOM. Consider killing stale buck2/EdenFS."
fi

if [ "$SKIP_BUILD" = false ]; then
    echo ""
    echo "=== Building ${#VARIANT_ARR[@]} variants SEQUENTIALLY ==="
    for variant in "${VARIANT_ARR[@]}"; do
        WT="/tmp/stress-wt-${SLUG}-${variant}"
        git worktree remove --force "$WT" 2>/dev/null || true
        git worktree add "$WT" "$(git rev-parse HEAD)" 2>/dev/null
        BUILD_LOG="${WT}/build.log"
        echo "  Building ${variant} in ${WT}..."
        (
            cd "$WT"
            case "$variant" in
                debug) make -j${JOBS} db_stress 2>&1 ;;
                asan)  COMPILE_WITH_ASAN=1 CC=clang CXX=clang++ USE_CLANG=1 make -j${JOBS} db_stress 2>&1 ;;
                tsan)  COMPILE_WITH_TSAN=1 CC=clang CXX=clang++ USE_CLANG=1 make -j${JOBS} db_stress 2>&1 ;;
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
        if [ ! -f "$BIN" ]; then echo "FATAL: $BIN not found!"; exit 1; fi
    done
fi

# === TEST PHASE ===
RESULTS_DIR="/tmp/stress-results-${SLUG}-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"
echo "Results: $RESULTS_DIR"

# SLUG-scoped temp dir for db_crashtest.py (via TEST_TMPDIR env var)
STRESS_TMPDIR="/tmp/stress-db-${SLUG}"
mkdir -p "$STRESS_TMPDIR"

TOTAL_TESTS=$(( ${#VARIANT_ARR[@]} * NUM_MODES ))
MATRIX_START_TIME=$(date +%s)
GLOBAL_PASSED=0
GLOBAL_FAILED=0

for duration in "${BATCH_ARR[@]}"; do
    BATCH_DIR="${RESULTS_DIR}/batch-${duration}s"
    mkdir -p "$BATCH_DIR"

    echo ""
    echo "============================================="
    echo "=== BATCH: ${duration}s -- ${TOTAL_TESTS} tests, ${PARALLEL} concurrent ($(date)) ==="
    echo "============================================="

    # 3-pass variant interleave over priority-ordered modes
    declare -a JOB_VARIANTS=()
    declare -a JOB_MODE_NAMES=()
    declare -a JOB_MODE_ARGS=()
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
    COMPLETED=0
    ANY_FAIL=false
    FAILURES=()

    for (( start=0; start<TOTAL_JOBS; start+=PARALLEL )); do
        BATCH_PIDS=()
        BATCH_LABELS=()

        end=$(( start + PARALLEL ))
        if [ "$end" -gt "$TOTAL_JOBS" ]; then end=$TOTAL_JOBS; fi

        for (( i=start; i<end; i++ )); do
            variant="${JOB_VARIANTS[$i]}"
            mode_name="${JOB_MODE_NAMES[$i]}"
            mode_args="${JOB_MODE_ARGS[$i]}"
            WT="/tmp/stress-wt-${SLUG}-${variant}"
            LABEL="${variant}-${mode_name}"
            LOG="${BATCH_DIR}/${LABEL}.log"
            TEST_DB_DIR="${STRESS_TMPDIR}/${LABEL}"
            mkdir -p "$TEST_DB_DIR"

            (
                cd "$WT"
                export TEST_TMPDIR="$TEST_DB_DIR"

                # For short batches (<=600s), reduce ops_per_thread for
                # instrumented WHITEBOX variants so iterations finish within
                # the duration window instead of hitting the 900s grace period.
                # Whitebox check_mode=0 multiplies ops_per_thread by 100x,
                # so 100 → 10,000 effective ops.
                # Whitebox check_mode=2 divides by 5, so need ops > reopen×5.
                # With reopen=5: 100/5=20 > 5. Safe.
                #
                # BLACKBOX tests must NOT reduce ops_per_thread because they
                # rely on db_stress running until killed. If db_stress finishes
                # early, the crash test framework treats it as a failure
                # ("Exit Before Killing").
                VARIANT_OPS_FLAG=""
                if [ "$duration" -le 600 ]; then
                    case "$mode_name" in
                        *whitebox*)
                            case "$variant" in
                                asan) VARIANT_OPS_FLAG="--ops_per_thread=100 --reopen=5" ;;
                                tsan) VARIANT_OPS_FLAG="--ops_per_thread=100 --reopen=5" ;;
                            esac
                            ;;
                    esac
                fi

                # shellcheck disable=SC2086
                python3 tools/db_crashtest.py \
                    --stress_cmd="$WT/db_stress" \
                    --duration=$duration \
                    $EXTRA_FLAGS \
                    $VARIANT_OPS_FLAG \
                    $mode_args \
                    > "$LOG" 2>&1
                EXIT=$?
                echo "EXIT: $EXIT" >> "$LOG"
                exit $EXIT
            ) &
            BATCH_PIDS+=($!)
            BATCH_LABELS+=("$LABEL")
        done

        echo "  Running $(( end - start )) tests [$(( start + 1 ))-${end}/${TOTAL_JOBS}]: ${BATCH_LABELS[*]}"

        for j in "${!BATCH_PIDS[@]}"; do
            label="${BATCH_LABELS[$j]}"
            pid="${BATCH_PIDS[$j]}"
            if ! wait "$pid"; then
                echo "    FAILED: ${label}"
                ANY_FAIL=true
                FAILURES+=("$label")
                GLOBAL_FAILED=$((GLOBAL_FAILED + 1))
            else
                echo "    PASSED: ${label}"
                GLOBAL_PASSED=$((GLOBAL_PASSED + 1))
            fi
            COMPLETED=$((COMPLETED + 1))
        done
        unset BATCH_PIDS BATCH_LABELS

        # Early exit after each parallel batch
        if [ "$ANY_FAIL" = true ]; then
            echo ""
            echo "  Failure detected -- stopping remaining tests"
            break
        fi
    done
    unset JOB_VARIANTS JOB_MODE_NAMES JOB_MODE_ARGS

    if [ "$ANY_FAIL" = true ]; then
        echo ""
        echo "!!! FAILURES in batch ${duration}s: ${FAILURES[*]} !!!"
        echo ""
        echo "Preserving crash DB LOG files from failed tests..."
        for label in "${FAILURES[@]}"; do
            TEST_DB_DIR="${STRESS_TMPDIR}/${label}"
            for db_dir in "$TEST_DB_DIR"/rocksdb_crashtest_*; do
                if [ -d "$db_dir" ] && [ -f "$db_dir/LOG" ]; then
                    db_name=$(basename "$db_dir")
                    cp "$db_dir/LOG" "${BATCH_DIR}/${label}-${db_name}.LOG" 2>/dev/null
                    for old_log in "$db_dir"/LOG.old.*; do
                        [ -f "$old_log" ] && cp "$old_log" "${BATCH_DIR}/${label}-${db_name}.$(basename "$old_log")" 2>/dev/null
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

        # Write failures.txt summary
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

        # FINAL SUMMARY (monitors watch for this marker)
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

        # Do NOT clean up on failure -- preserve everything for debugging
        exit 1
    fi

    echo "=== Batch ${duration}s: ALL ${TOTAL_TESTS} PASSED ==="

    # Clean up ONLY our own SLUG-scoped test DB dirs between batches
    rm -rf "${STRESS_TMPDIR:?}"/* 2>/dev/null || true
done

echo ""
echo "============================================="
echo "=== ALL BATCHES PASSED! ==="
echo "=== ${#BATCH_ARR[@]} batches x ${TOTAL_TESTS} tests each ==="
echo "=== ${#VARIANT_ARR[@]} variants x ${NUM_MODES} modes ==="
echo "=== Results: ${RESULTS_DIR} ==="
echo "============================================="

# FINAL SUMMARY (monitors watch for this marker)
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

# Clean up our SLUG-scoped test DB dirs on full success
rm -rf "${STRESS_TMPDIR:?}" 2>/dev/null || true
