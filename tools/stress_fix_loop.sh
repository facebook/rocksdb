#!/bin/bash
#
# RocksDB Stress-Fix Loop (General-purpose, concurrent-safe)
#
# Automated loop that runs crash tests, analyzes failures with Claude Code,
# applies fixes, and repeats until stress tests pass cleanly at the target
# duration. Once clean, optionally pushes to GitHub.
#
# All /tmp paths are prefixed with a repo-derived SLUG so multiple instances
# (from different repos) can run concurrently without collision.
#
# This script is repo-agnostic — pass --repo to point at any RocksDB checkout.
# If not specified, defaults to current directory.
#
# Usage:
#   stress_fix_loop.sh --repo /path/to/rocksdb [OPTIONS]
#
# Options:
#   --repo DIR            Path to RocksDB repo (default: current directory)
#   --target-duration N   Duration (seconds) that must pass clean to exit (default: 3600)
#   --parallel N          Parallel runs per variant (default: 4)
#   --variants LIST       Comma-separated variants (default: debug,asan,tsan)
#   --extra-flags F       Extra flags for db_crashtest.py
#   --max-iterations N    Max fix iterations before giving up (default: 10)
#   --push                Push to GitHub after passing (default: no)
#   --skip-first-build    Skip initial build (reuse existing binaries)
#   --stop                Stop a running loop for this repo (kills entire process group)
#   --help                Show this help
#
# Key learnings (from PR #14457 stress testing):
#   - db_crashtest.py randomizes params. extra-flags are appended to the
#     db_stress command line (last occurrence wins in gflags), BUT
#     finalize_and_sanitize() can force flags to 0 based on other random
#     params (e.g., enable_blob_files=0 forces enable_blob_direct_write=0).
#     Always pass ALL required flags together.
#   - CC should only run unit tests, not stress tests. CC runs stress tests
#     one at a time and is slow. The loop runs 8-16 in parallel.
#   - Worktrees must use explicit commit hash: git worktree add $WT $(git rev-parse HEAD)
#   - Build variants sequentially (not parallel) to avoid 512-process I/O storms.
#   - Sandcastle never runs release crash tests (no assertions, no fault injection).
#   - Features with lower durability (e.g., blob direct write deferred mode)
#     need db_crashtest.py to treat them as data-loss modes (like disable_wal).
#
# Examples:
#   # Fix loop for blob direct write until 1hr clean
#   stress_fix_loop.sh --repo ~/workspace/ws21/rocksdb --parallel 4 \
#     --extra-flags "--enable_blob_direct_write=1 --enable_blob_files=1 \
#       --blob_direct_write_partitions=4 --blob_direct_write_buffer_size=1048576"
#
#   # Quick loop: 30min target, 2 parallel, push when done
#   stress_fix_loop.sh --repo ~/workspace/ws22/rocksdb --target-duration 1800 --parallel 2 --push
#
#   # Just debug+asan variants (from inside the repo)
#   cd ~/workspace/ws21/rocksdb && stress_fix_loop.sh --variants debug,asan
#

# Defaults
TARGET_DURATION=3600
PARALLEL=4
VARIANTS="debug,asan,tsan"
MODES="all"
EXTRA_FLAGS=""
MAX_ITERATIONS=10
PUSH_ON_SUCCESS=false
SKIP_FIRST_BUILD=false
REPO_DIR=""
JOBS=128
STOP_MODE=false

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --repo) REPO_DIR="$2"; shift 2 ;;
        --target-duration) TARGET_DURATION="$2"; shift 2 ;;
        --parallel) PARALLEL="$2"; shift 2 ;;
        --variants) VARIANTS="$2"; shift 2 ;;
        --modes) MODES="$2"; shift 2 ;;
        --extra-flags) EXTRA_FLAGS="$2"; shift 2 ;;
        --max-iterations) MAX_ITERATIONS="$2"; shift 2 ;;
        --push) PUSH_ON_SUCCESS=true; shift ;;
        --skip-first-build) SKIP_FIRST_BUILD=true; shift ;;
        --jobs) JOBS="$2"; shift 2 ;;
        --stop)
            STOP_MODE=true; shift ;;
        --help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Default repo to current directory if not specified
if [ -z "$REPO_DIR" ]; then
    REPO_DIR="$(pwd)"
fi
REPO_DIR="$(cd "$REPO_DIR" && pwd)"

# Validate it's a RocksDB repo
if [ ! -f "$REPO_DIR/tools/db_crashtest.py" ]; then
    echo "ERROR: $REPO_DIR does not look like a RocksDB repo (missing tools/db_crashtest.py)"
    echo "Use --repo /path/to/rocksdb to specify the repo location."
    exit 1
fi

# Derive a short slug from repo path for /tmp namespace isolation.
SLUG=$(echo "$REPO_DIR" | md5sum | cut -c1-8)

# --stop mode: kill the running loop and its entire process group, then exit.
if [ "$STOP_MODE" = true ]; then
    PIDFILE="/tmp/stress-fix-loop-${SLUG}.pid"
    if [ ! -f "$PIDFILE" ]; then
        echo "No running loop found for $REPO_DIR (no PID file at $PIDFILE)"
        exit 0
    fi
    LOOP_PID=$(cat "$PIDFILE")
    if ! kill -0 "$LOOP_PID" 2>/dev/null; then
        echo "Loop PID $LOOP_PID is not running (stale PID file). Cleaning up."
        rm -f "$PIDFILE"
        exit 0
    fi
    echo "Stopping stress-fix loop for $REPO_DIR (PID $LOOP_PID, SLUG $SLUG)..."
    # Kill the entire process group (loop + matrix + crashtest + db_stress).
    # The loop runs under setsid, so it's the session leader.
    kill -TERM -"$LOOP_PID" 2>/dev/null
    sleep 2
    # Force-kill any survivors
    if kill -0 "$LOOP_PID" 2>/dev/null; then
        kill -KILL -"$LOOP_PID" 2>/dev/null
        sleep 1
    fi
    rm -f "$PIDFILE"
    echo "Stopped."
    exit 0
fi

# PID file for safe concurrent operation on shared devvms.
# Scoped to SLUG (repo path) so multiple repos can run independently.
# Prevents blanket "pkill -f stress_fix_loop" from killing unrelated loops.
PIDFILE="/tmp/stress-fix-loop-${SLUG}.pid"

# Check for an existing loop on the same repo
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "ERROR: Another stress-fix loop for this repo is already running (PID $OLD_PID)"
        echo "Kill it first: kill $OLD_PID"
        exit 1
    else
        echo "Removing stale PID file (PID $OLD_PID no longer running)"
        rm -f "$PIDFILE"
    fi
fi

echo $$ > "$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT

# Build escalating batch list up to target duration
BATCHES=""
for d in 300 600 1800 3600 7200; do
    if [ -z "$BATCHES" ]; then
        BATCHES="$d"
    else
        BATCHES="$BATCHES,$d"
    fi
    [ "$d" -ge "$TARGET_DURATION" ] && break
done

cd "$REPO_DIR"

echo "============================================="
echo "RocksDB Stress-Fix Loop"
echo "============================================="
echo "Repo:         $REPO_DIR"
echo "Slug:         $SLUG"
echo "Target:       ${TARGET_DURATION}s clean"
echo "Batches:      $BATCHES"
echo "Variants:     $VARIANTS"
echo "Parallel:     $PARALLEL per variant"
echo "Max iters:    $MAX_ITERATIONS"
echo "Push on pass: $PUSH_ON_SUCCESS"
echo "Start:        $(date)"
echo "============================================="

for iteration in $(seq 1 $MAX_ITERATIONS); do
    echo ""
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    echo ">>>> ITERATION $iteration / $MAX_ITERATIONS ($(date))"
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"

    # === BUILD ===
    BUILD_FLAG=""
    if [ "$iteration" -eq 1 ] && [ "$SKIP_FIRST_BUILD" = true ]; then
        BUILD_FLAG="--skip-build"
    fi

    # === RUN STRESS MATRIX ===
    echo ""
    echo "--- Running stress matrix ---"
    STRESS_LOG="/tmp/stress-${SLUG}-fix-iter${iteration}.log"

    # Use run_stress_matrix.sh from the same directory as this script,
    # or from PATH if not found alongside
    MATRIX_SCRIPT="$(dirname "$0")/run_stress_matrix.sh"
    if [ ! -f "$MATRIX_SCRIPT" ]; then
        MATRIX_SCRIPT="$(command -v run_stress_matrix.sh 2>/dev/null)"
    fi
    if [ -z "$MATRIX_SCRIPT" ] || [ ! -f "$MATRIX_SCRIPT" ]; then
        echo "ERROR: Cannot find run_stress_matrix.sh. Place it next to this script or on PATH."
        exit 1
    fi

    bash "$MATRIX_SCRIPT" \
        --repo "$REPO_DIR" \
        --parallel "$PARALLEL" \
        --variants "$VARIANTS" \
        --modes "$MODES" \
        --batches "$BATCHES" \
        --jobs "$JOBS" \
        --extra-flags "$EXTRA_FLAGS" \
        $BUILD_FLAG \
        > "$STRESS_LOG" 2>&1
    STRESS_EXIT=$?

    if [ $STRESS_EXIT -eq 0 ]; then
        echo ""
        echo "============================================="
        echo "=== STRESS TESTS PASSED on iteration $iteration! ==="
        echo "============================================="

        if [ "$PUSH_ON_SUCCESS" = true ]; then
            echo "Pushing to GitHub..."
            cd "$REPO_DIR"
            git push origin HEAD
            echo "Pushed."
        else
            echo "All tests clean. Ready to push when you want."
        fi
        exit 0
    fi

    echo ""
    echo "--- Stress test FAILED on iteration $iteration ---"
    echo "Analyzing failures..."

    # === GATHER FAILURE LOGS ===
    RESULTS_DIR=$(grep "^Results:" "$STRESS_LOG" | awk '{print $2}')
    FAILURE_SUMMARY="/tmp/stress-${SLUG}-failures-iter${iteration}.txt"
    echo "Iteration $iteration failures:" > "$FAILURE_SUMMARY"
    echo "" >> "$FAILURE_SUMMARY"

    FAILED_BATCH_DIR=$(ls -d "$RESULTS_DIR"/batch-*/ 2>/dev/null | tail -1)

    if [ -z "$FAILED_BATCH_DIR" ]; then
        echo "ERROR: No batch directory found in $RESULTS_DIR"
        tail -30 "$STRESS_LOG"
        exit 1
    fi

    echo "Failed batch: $FAILED_BATCH_DIR" >> "$FAILURE_SUMMARY"
    echo "" >> "$FAILURE_SUMMARY"

    for logfile in "$FAILED_BATCH_DIR"/*.log; do
        label=$(basename "$logfile" .log)
        exit_line=$(grep "^EXIT:" "$logfile" 2>/dev/null)

        has_error=false
        for pattern in "SUMMARY.*Sanitizer" "Corruption" "Invalid blob" \
                       "Verification failed" "No such file" "SafeTerminate" \
                       "stack-use-after" "heap-use-after" "data race" \
                       "Assertion" "SIGABRT\|SIGSEGV"; do
            if grep -q "$pattern" "$logfile" 2>/dev/null; then
                has_error=true
                break
            fi
        done

        if [ "$has_error" = true ] || [ "$exit_line" != "EXIT: 0" ]; then
            echo "=== $label ===" >> "$FAILURE_SUMMARY"

            # Categorize the failure
            CATEGORY="unknown"
            if grep -q "SUMMARY.*AddressSanitizer\|heap-use-after\|stack-use-after\|heap-buffer-overflow" "$logfile" 2>/dev/null; then
                CATEGORY="sanitizer:asan"
            elif grep -q "SUMMARY.*ThreadSanitizer\|data race" "$logfile" 2>/dev/null; then
                CATEGORY="sanitizer:tsan"
            elif grep -q "SUMMARY.*UndefinedBehaviorSanitizer\|runtime error:" "$logfile" 2>/dev/null; then
                CATEGORY="sanitizer:ubsan"
            elif grep -q "Assertion\|assert\|SIGABRT" "$logfile" 2>/dev/null; then
                CATEGORY="assertion"
            elif grep -q "Verification failed\|Corruption\|Invalid blob" "$logfile" 2>/dev/null; then
                CATEGORY="verification"
            elif grep -q "SIGSEGV\|SafeTerminate\|Killed" "$logfile" 2>/dev/null; then
                CATEGORY="crash"
            fi
            echo "Category: $CATEGORY" >> "$FAILURE_SUMMARY"

            # Extract the db_stress command line
            CMD_LINE=$(grep -m1 "Running.*db_stress\|Executing.*db_stress\|db_stress " "$logfile" 2>/dev/null | head -1)
            if [ -n "$CMD_LINE" ]; then
                echo "Command: $CMD_LINE" >> "$FAILURE_SUMMARY"
            fi
            echo "" >> "$FAILURE_SUMMARY"

            grep -m3 "SUMMARY\|Corruption\|Invalid blob\|Verification failed\|No such file\|SafeTerminate\|ERROR.*Sanitizer\|data race\|Assertion" "$logfile" >> "$FAILURE_SUMMARY" 2>/dev/null
            echo "" >> "$FAILURE_SUMMARY"
            grep -B 2 -A 10 "SUMMARY\|Corruption.*blob\|SafeTerminate\|Assertion\|data race" "$logfile" 2>/dev/null | head -30 >> "$FAILURE_SUMMARY"
            echo "" >> "$FAILURE_SUMMARY"
        fi
    done

    echo "Failure summary: $FAILURE_SUMMARY ($(wc -l < "$FAILURE_SUMMARY") lines)"

    # === STOP AND REPORT ===
    echo ""
    echo "============================================="
    echo "=== STRESS TEST FAILED (iteration $iteration) ==="
    echo "============================================="
    echo "Failure summary: $FAILURE_SUMMARY"
    echo ""
    cat "$FAILURE_SUMMARY"
    echo ""
    echo "--- Artifacts ---"
    echo "  Failure summary:  $FAILURE_SUMMARY"
    echo "  Stress log:       $STRESS_LOG"
    echo "  Results dir:      $RESULTS_DIR"
    echo ""
    echo "--- Re-run commands ---"
    # Build the re-run command with all current flags
    RERUN_CMD="stress_fix_loop.sh --repo $REPO_DIR --skip-first-build --target-duration $TARGET_DURATION --parallel $PARALLEL --variants $VARIANTS --modes $MODES --jobs $JOBS --max-iterations $MAX_ITERATIONS"
    [ -n "$EXTRA_FLAGS" ] && RERUN_CMD="$RERUN_CMD --extra-flags \"$EXTRA_FLAGS\""
    echo "  Re-run:  $RERUN_CMD"
    echo "  Stop:    stress_fix_loop.sh --repo $REPO_DIR --stop"
    echo ""
    exit 1

done

echo ""
echo "============================================="
echo "=== MAX ITERATIONS ($MAX_ITERATIONS) REACHED ==="
echo "=== Stress tests still failing. Manual fix needed. ==="
echo "============================================="
exit 1
