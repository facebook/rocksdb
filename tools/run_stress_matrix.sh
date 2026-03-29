#!/bin/bash
#
# RocksDB Extensive Crash Test Matrix
#
# Builds 4 binary variants (debug, asan, tsan, release) and runs N parallel
# crash tests per variant in escalating duration batches. Stops at first failure.
#
# Each variant runs multiple test modes matching Sandcastle contrun coverage:
#   - blackbox: external kill (SIGKILL at random intervals)
#   - blackbox --simple: single CF, simpler config
#   - whitebox: internal kill (random_kill_odd + reopen=20)
#   - whitebox --cf_consistency: multi-CF atomic flush consistency
#
# Usage:
#   ./tools/run_stress_matrix.sh [OPTIONS]
#
# Options:
#   --parallel N     Number of parallel runs per variant (default: 4)
#   --batches LIST   Comma-separated durations in seconds (default: 300,600,1800,3600,7200)
#   --variants LIST  Comma-separated variants (default: debug,asan,tsan,release)
#   --jobs N         Build parallelism (default: 128)
#   --extra-flags F  Extra flags passed to db_crashtest.py
#   --skip-build     Skip building, reuse existing worktree binaries
#   --help           Show this help
#
# Examples:
#   # Quick smoke test
#   ./tools/run_stress_matrix.sh --parallel 2 --batches 300
#
#   # Full matrix for blob direct write
#   ./tools/run_stress_matrix.sh --parallel 4 \
#     --extra-flags "--enable_blob_direct_write=1 --enable_blob_files=1"
#
#   # Just TSAN, 30min
#   ./tools/run_stress_matrix.sh --variants tsan --batches 1800
#

set -e

# Defaults
PARALLEL=4
BATCHES="300,600,1800,3600,7200"
VARIANTS="debug,asan,tsan,release"
JOBS=128
EXTRA_FLAGS=""
SKIP_BUILD=false
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Test modes: type|crashtest_args
# Each parallel slot cycles through these modes
TEST_MODES=(
    "blackbox|blackbox"
    "blackbox-simple|--simple blackbox"
    "whitebox|whitebox"
    "whitebox-cf|--cf_consistency whitebox"
)

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --parallel) PARALLEL="$2"; shift 2 ;;
        --batches) BATCHES="$2"; shift 2 ;;
        --variants) VARIANTS="$2"; shift 2 ;;
        --jobs) JOBS="$2"; shift 2 ;;
        --extra-flags) EXTRA_FLAGS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

IFS=',' read -ra VARIANT_ARR <<< "$VARIANTS"
IFS=',' read -ra BATCH_ARR <<< "$BATCHES"
NUM_MODES=${#TEST_MODES[@]}

echo "============================================="
echo "RocksDB Stress Test Matrix"
echo "============================================="
echo "Repo:       $REPO_DIR"
echo "Variants:   ${VARIANT_ARR[*]}"
echo "Parallel:   $PARALLEL per variant"
echo "Modes:      ${NUM_MODES} (blackbox, blackbox-simple, whitebox, whitebox-cf)"
echo "Batches:    ${BATCH_ARR[*]} seconds"
echo "Build jobs: $JOBS"
echo "Extra:      $EXTRA_FLAGS"
echo "Start:      $(date)"
echo "============================================="

cd "$REPO_DIR"

# === BUILD PHASE ===
if [ "$SKIP_BUILD" = false ]; then
    echo ""
    echo "=== Building ${#VARIANT_ARR[@]} variants in parallel ==="

    # Build variants SEQUENTIALLY to avoid OOM from 4 parallel builds
    # each using -j${JOBS}. 4 x 128 = 512 concurrent compile jobs overwhelms I/O.
    for variant in "${VARIANT_ARR[@]}"; do
        WT="/tmp/stress-wt-${variant}"
        git worktree remove --force "$WT" 2>/dev/null || true
        git worktree add "$WT" $(git rev-parse HEAD) 2>/dev/null

        (
            cd "$WT"
            case "$variant" in
                debug)
                    make -j${JOBS} db_stress 2>&1 | tail -3
                    ;;
                asan)
                    COMPILE_WITH_ASAN=1 CC=clang CXX=clang++ USE_CLANG=1 \
                        make -j${JOBS} db_stress 2>&1 | tail -3
                    ;;
                tsan)
                    COMPILE_WITH_TSAN=1 CC=clang CXX=clang++ USE_CLANG=1 \
                        make -j${JOBS} db_stress 2>&1 | tail -3
                    ;;
                release)
                    DEBUG_LEVEL=0 make -j${JOBS} db_stress 2>&1 | tail -3
                    ;;
            esac
            echo "${variant^^} BUILD: $?"
        )
        echo "  ${variant} build done"
    done

    echo "Builds done: $(date)"
    for variant in "${VARIANT_ARR[@]}"; do
        BIN="/tmp/stress-wt-${variant}/db_stress"
        if [ ! -f "$BIN" ]; then
            echo "FATAL: $BIN not found!"
            exit 1
        fi
        echo "  OK: ${variant} ($(du -sh "$BIN" | cut -f1))"
    done
else
    echo ""
    echo "=== Skipping build (--skip-build) ==="
    for variant in "${VARIANT_ARR[@]}"; do
        BIN="/tmp/stress-wt-${variant}/db_stress"
        if [ ! -f "$BIN" ]; then
            echo "FATAL: $BIN not found! Run without --skip-build first."
            exit 1
        fi
    done
fi

# === TEST PHASE ===
RESULTS_DIR="/tmp/stress-results-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"
echo "Results: $RESULTS_DIR"

TOTAL_VARIANTS=${#VARIANT_ARR[@]}
TOTAL_PER_BATCH=$((TOTAL_VARIANTS * PARALLEL))

for duration in "${BATCH_ARR[@]}"; do
    BATCH_DIR="${RESULTS_DIR}/batch-${duration}s"
    mkdir -p "$BATCH_DIR"

    echo ""
    echo "============================================="
    echo "=== BATCH: ${duration}s x ${TOTAL_PER_BATCH} runs ($(date)) ==="
    echo "============================================="

    ALL_PIDS=()
    ALL_LABELS=()

    for variant in "${VARIANT_ARR[@]}"; do
        WT="/tmp/stress-wt-${variant}"
        for run in $(seq 1 $PARALLEL); do
            # Cycle through test modes: run 1 → blackbox, run 2 → blackbox-simple,
            # run 3 → whitebox, run 4 → whitebox-cf, run 5 → blackbox, ...
            MODE_IDX=$(( (run - 1) % NUM_MODES ))
            MODE_ENTRY="${TEST_MODES[$MODE_IDX]}"
            MODE_NAME="${MODE_ENTRY%%|*}"
            MODE_ARGS="${MODE_ENTRY#*|}"

            LABEL="${variant}-${MODE_NAME}-run${run}"
            LOG="${BATCH_DIR}/${LABEL}.log"

            (
                cd "$WT"
                # Set DEBUG_LEVEL=0 for release so db_crashtest.py's
                # is_release_mode() correctly disables read fault injection.
                if [ "$variant" = "release" ]; then
                    export DEBUG_LEVEL=0
                fi
                # shellcheck disable=SC2086
                python3 tools/db_crashtest.py \
                    --stress_cmd="$WT/db_stress" \
                    --duration=$duration \
                    $EXTRA_FLAGS \
                    $MODE_ARGS \
                    > "$LOG" 2>&1
                EXIT=$?
                echo "EXIT: $EXIT" >> "$LOG"
                exit $EXIT
            ) &
            ALL_PIDS+=($!)
            ALL_LABELS+=("$LABEL")
        done
    done

    echo "Running ${#ALL_PIDS[@]} crashtests in parallel..."
    echo "  Modes per variant: $(for m in "${TEST_MODES[@]}"; do echo -n "${m%%|*} "; done)"

    ANY_FAIL=false
    FAILURES=()
    for i in "${!ALL_PIDS[@]}"; do
        label="${ALL_LABELS[$i]}"
        pid="${ALL_PIDS[$i]}"
        if ! wait "$pid"; then
            echo "  ❌ ${label}: FAILED"
            ANY_FAIL=true
            FAILURES+=("$label")
        else
            echo "  ✅ ${label}: PASSED"
        fi
    done

    if [ "$ANY_FAIL" = true ]; then
        echo ""
        echo "!!! FAILURES in batch ${duration}s: ${FAILURES[*]} !!!"
        echo ""
        # Preserve crash DB dirs and copy LOG files for analysis
        echo "Preserving crash DB LOG files..."
        for db_dir in /tmp/rocksdb_crashtest_blackbox* /tmp/rocksdb_crashtest_whitebox*; do
            if [ -d "$db_dir" ] && [ -f "$db_dir/LOG" ]; then
                db_name=$(basename "$db_dir")
                cp "$db_dir/LOG" "${BATCH_DIR}/${db_name}.LOG" 2>/dev/null
                # Also copy LOG.old files
                for old_log in "$db_dir"/LOG.old.*; do
                    [ -f "$old_log" ] && cp "$old_log" "${BATCH_DIR}/${db_name}.$(basename $old_log)" 2>/dev/null
                done
                echo "  Saved LOG from $db_dir"
            fi
        done
        echo ""
        for label in "${FAILURES[@]}"; do
            echo "--- ${label} (last 30 lines) ---"
            tail -30 "${BATCH_DIR}/${label}.log"
            echo ""
        done
        echo "Full logs + DB LOGs: ${BATCH_DIR}/"
        exit 1
    fi

    echo "=== Batch ${duration}s: ALL ${#ALL_PIDS[@]} PASSED ==="

    # Clean up tmpdir DB dirs to save space
    rm -rf /dev/shm/rocksdb_crashtest_* /tmp/rocksdb_crashtest_* 2>/dev/null || true
done

echo ""
echo "============================================="
echo "=== ALL BATCHES PASSED! ==="
echo "=== ${#BATCH_ARR[@]} batches x ${TOTAL_PER_BATCH} runs each ==="
echo "=== Modes: blackbox, blackbox-simple, whitebox, whitebox-cf ==="
echo "=== Results: ${RESULTS_DIR} ==="
echo "============================================="
