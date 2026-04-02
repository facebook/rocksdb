#!/bin/bash
#
# RocksDB Stress-Fix Loop (General-purpose, concurrent-safe)
#
# Automated loop that runs crash tests, summarizes failures, and repeats
# until stress tests pass cleanly at the target duration. The repo-local
# scripts are self-contained and can be run directly from tools/ without
# requiring any host-specific PATH setup.
#
# All /tmp paths are prefixed with a repo-derived SLUG so multiple repos or
# worktrees can run concurrently without collision.
#
# Usage:
#   tools/stress_fix_loop.sh --repo /path/to/rocksdb [OPTIONS]
#
# Options:
#   --repo DIR            Path to RocksDB repo (default: current directory)
#   --target-duration N   Duration (seconds) that must pass clean to exit (default: 3600)
#   --parallel N          Parallel runs per variant (default: 4)
#   --variants LIST       Comma-separated variants (default: debug,asan,tsan)
#   --modes LIST          Comma-separated mode groups (default: all)
#   --extra-flags F       Extra flags for db_crashtest.py
#   --max-iterations N    Max fix iterations before giving up (default: 10)
#   --jobs N              Build parallelism (default: detected CPU count)
#   --push                Push to GitHub after passing (default: no)
#   --skip-first-build    Skip initial build (reuse existing binaries)
#   --stop                Stop a running loop for this repo and clean repo-scoped
#                         temp worktrees/DB dirs
#   --help                Show this help
#
# Key learnings:
#   - db_crashtest.py randomizes params. extra-flags are appended to the
#     db_stress command line (last occurrence wins in gflags), BUT
#     finalize_and_sanitize() can force flags to 0 based on other random
#     params. Always pass all required flags together.
#   - Worktrees must use explicit commit hashes when created.
#   - Build variants sequentially (not parallel) to avoid I/O storms.
#   - Features with lower durability need db_crashtest.py to treat them as
#     data-loss modes (like disable_wal).
#
# Examples:
#   tools/stress_fix_loop.sh --repo ~/workspace/ws21/rocksdb --parallel 4 \
#     --extra-flags "--enable_blob_direct_write=1 --enable_blob_files=1 \
#       --blob_direct_write_partitions=4 --blob_direct_write_buffer_size=1048576"
#
#   tools/stress_fix_loop.sh --repo ~/workspace/ws22/rocksdb \
#     --target-duration 1800 --parallel 2 --push
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
  tools/stress_fix_loop.sh --repo /path/to/rocksdb [OPTIONS]

Options:
  --repo DIR            Path to RocksDB repo (default: current directory)
  --target-duration N   Duration in seconds that must pass clean to exit
  --parallel N          Parallel runs per variant (default: 4)
  --variants LIST       Comma-separated variants (default: debug,asan,tsan)
  --modes LIST          Comma-separated mode groups (default: all)
  --extra-flags F       Extra flags for db_crashtest.py
  --max-iterations N    Max fix iterations before giving up (default: 10)
  --jobs N              Build parallelism (default: detected CPU count)
  --push                Push to GitHub after passing
  --skip-first-build    Skip the initial build and reuse existing worktree binaries
  --stop                Stop a running loop for this repo and clean temp dirs
  --help                Show this help
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

cleanup_on_exit() {
    local exit_code="$?"

    if [ "${CLEANUP_DONE}" = true ]; then
        return
    fi
    CLEANUP_DONE=true
    trap - EXIT INT TERM

    terminate_direct_children
    rm -f "$PIDFILE" "$MATRIX_PIDFILE"

    if [ "${INTERRUPTED}" = true ]; then
        cleanup_owned_resources
    fi

    if [ "${INTERRUPTED}" = true ]; then
        echo ""
        echo "Interrupted. Cleaned up repo-scoped temp resources for SLUG ${SLUG}."
    fi

    exit "$exit_code"
}

SCRIPT_PATH="$(resolve_path "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd -- "$(dirname -- "$SCRIPT_PATH")" && pwd -P)"
MATRIX_SCRIPT="${SCRIPT_DIR}/run_stress_matrix.sh"
CPU_COUNT="$(detect_cpu_count)"

TARGET_DURATION=3600
PARALLEL=4
VARIANTS="debug,asan,tsan"
MODES="all"
EXTRA_FLAGS=""
MAX_ITERATIONS=10
PUSH_ON_SUCCESS=false
SKIP_FIRST_BUILD=false
REPO_DIR=""
JOBS="$CPU_COUNT"
STOP_MODE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
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
        --stop) STOP_MODE=true; shift ;;
        --help) usage; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

require_positive_integer "target-duration" "$TARGET_DURATION"
require_positive_integer "parallel" "$PARALLEL"
require_positive_integer "max-iterations" "$MAX_ITERATIONS"
require_positive_integer "jobs" "$JOBS"

if [ -z "$REPO_DIR" ]; then
    REPO_DIR="$(pwd -P)"
fi
REPO_DIR="$(cd "$REPO_DIR" && pwd -P)"

if [ ! -f "$REPO_DIR/tools/db_crashtest.py" ]; then
    echo "ERROR: $REPO_DIR does not look like a RocksDB repo (missing tools/db_crashtest.py)"
    echo "Use --repo /path/to/rocksdb to specify the repo location."
    exit 1
fi

if [ ! -f "$MATRIX_SCRIPT" ]; then
    echo "ERROR: Cannot find sibling run_stress_matrix.sh at $MATRIX_SCRIPT"
    exit 1
fi

SLUG="$(hash_string "$REPO_DIR")"
PIDFILE="/tmp/stress-fix-loop-${SLUG}.pid"
MATRIX_PIDFILE="/tmp/stress-matrix-${SLUG}.pid"
STRESS_TMPDIR="/tmp/stress-db-${SLUG}"
INTERRUPTED=false
CLEANUP_DONE=false

if [ "$STOP_MODE" = true ]; then
    if [ ! -f "$PIDFILE" ]; then
        echo "No running loop found for $REPO_DIR (no PID file at $PIDFILE)"
        rm -f "$MATRIX_PIDFILE"
        cleanup_owned_resources
        exit 0
    fi

    LOOP_PID="$(cat "$PIDFILE")"
    if ! kill -0 "$LOOP_PID" 2>/dev/null; then
        echo "Loop PID $LOOP_PID is not running (stale PID file). Cleaning up."
        rm -f "$PIDFILE" "$MATRIX_PIDFILE"
        cleanup_owned_resources
        exit 0
    fi

    echo "Stopping stress-fix loop for $REPO_DIR (PID $LOOP_PID, SLUG $SLUG)..."
    terminate_process_tree "$LOOP_PID"
    rm -f "$PIDFILE" "$MATRIX_PIDFILE"
    cleanup_owned_resources
    echo "Stopped and cleaned up repo-scoped temp resources."
    exit 0
fi

if [ -f "$PIDFILE" ]; then
    OLD_PID="$(cat "$PIDFILE")"
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "ERROR: Another stress-fix loop for this repo is already running (PID $OLD_PID)"
        echo "Stop it first: $(shell_join "$SCRIPT_PATH" --repo "$REPO_DIR" --stop)"
        exit 1
    fi
    echo "Removing stale PID file (PID $OLD_PID no longer running)"
    rm -f "$PIDFILE"
fi

echo $$ > "$PIDFILE"
trap 'INTERRUPTED=true; exit 130' INT
trap 'INTERRUPTED=true; exit 143' TERM
trap 'cleanup_on_exit' EXIT

BATCHES=""
LAST_BATCH=0
for d in 300 600 1800 3600 7200; do
    if [ -z "$BATCHES" ]; then
        BATCHES="$d"
    else
        BATCHES="${BATCHES},${d}"
    fi
    LAST_BATCH="$d"
    if [ "$d" -ge "$TARGET_DURATION" ]; then
        break
    fi
done
if [ "$LAST_BATCH" -lt "$TARGET_DURATION" ]; then
    BATCHES="${BATCHES},${TARGET_DURATION}"
fi

cd "$REPO_DIR"

echo "============================================="
echo "RocksDB Stress-Fix Loop"
echo "============================================="
echo "Script:       $SCRIPT_PATH"
echo "Matrix script: $MATRIX_SCRIPT"
echo "Repo:         $REPO_DIR"
echo "Slug:         $SLUG"
echo "Target:       ${TARGET_DURATION}s clean"
echo "Batches:      $BATCHES"
echo "Variants:     $VARIANTS"
echo "Modes:        $MODES"
echo "Parallel:     $PARALLEL per variant"
echo "Build jobs:   $JOBS"
echo "Max iters:    $MAX_ITERATIONS"
echo "Push on pass: $PUSH_ON_SUCCESS"
echo "Start:        $(date)"
echo "============================================="

for iteration in $(seq 1 "$MAX_ITERATIONS"); do
    echo ""
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    echo ">>>> ITERATION $iteration / $MAX_ITERATIONS ($(date))"
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"

    MATRIX_CMD=(bash "$MATRIX_SCRIPT" \
        --repo "$REPO_DIR" \
        --parallel "$PARALLEL" \
        --variants "$VARIANTS" \
        --modes "$MODES" \
        --batches "$BATCHES" \
        --jobs "$JOBS")
    if [ -n "$EXTRA_FLAGS" ]; then
        MATRIX_CMD+=(--extra-flags "$EXTRA_FLAGS")
    fi
    if [ "$iteration" -eq 1 ] && [ "$SKIP_FIRST_BUILD" = true ]; then
        MATRIX_CMD+=(--skip-build)
    fi

    echo ""
    echo "--- Running stress matrix ---"
    STRESS_LOG="/tmp/stress-${SLUG}-fix-iter${iteration}.log"
    "${MATRIX_CMD[@]}" > "$STRESS_LOG" 2>&1
    STRESS_EXIT=$?

    if [ "$STRESS_EXIT" -eq 0 ]; then
        echo ""
        echo "============================================="
        echo "=== STRESS TESTS PASSED on iteration $iteration! ==="
        echo "============================================="

        if [ "$PUSH_ON_SUCCESS" = true ]; then
            echo "Pushing to GitHub..."
            git -C "$REPO_DIR" push origin HEAD
            echo "Pushed."
        else
            echo "All tests clean. Ready to push when you want."
        fi
        exit 0
    fi

    echo ""
    echo "--- Stress test FAILED on iteration $iteration ---"
    echo "Analyzing failures..."

    RESULTS_DIR="$(awk '/^Results:/ {print $2; exit}' "$STRESS_LOG")"
    MATRIX_FAILURES_FILE="$(awk '/^Failures:/ {print $2; exit}' "$STRESS_LOG")"
    FAILURE_SUMMARY="/tmp/stress-${SLUG}-failures-iter${iteration}.txt"

    echo "Iteration $iteration failures:" > "$FAILURE_SUMMARY"
    echo "" >> "$FAILURE_SUMMARY"

    FAILED_BATCH_DIR=""
    if [ -n "$MATRIX_FAILURES_FILE" ] && [ -f "$MATRIX_FAILURES_FILE" ]; then
        FAILED_BATCH_DIR="$(dirname "$MATRIX_FAILURES_FILE")"
    elif [ -n "$RESULTS_DIR" ] && [ -d "$RESULTS_DIR" ]; then
        FAILED_BATCH_DIR="$(find "$RESULTS_DIR" -mindepth 1 -maxdepth 1 -type d -name 'batch-*' -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)"
    fi

    if [ -z "$FAILED_BATCH_DIR" ]; then
        echo "ERROR: No batch directory found in $RESULTS_DIR"
        tail -30 "$STRESS_LOG"
        exit 1
    fi

    echo "Failed batch: $FAILED_BATCH_DIR" >> "$FAILURE_SUMMARY"
    if [ -n "$MATRIX_FAILURES_FILE" ] && [ -f "$MATRIX_FAILURES_FILE" ]; then
        echo "Matrix failure summary: $MATRIX_FAILURES_FILE" >> "$FAILURE_SUMMARY"
    fi
    echo "" >> "$FAILURE_SUMMARY"

    FOUND_FAILURE_LOG=false
    shopt -s nullglob
    for logfile in "$FAILED_BATCH_DIR"/*.log; do
        label="$(basename "$logfile" .log)"
        exit_line="$(grep "^EXIT:" "$logfile" 2>/dev/null)"

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
            FOUND_FAILURE_LOG=true
            echo "=== $label ===" >> "$FAILURE_SUMMARY"

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

            CMD_LINE="$(grep -m1 "Running.*db_stress\|Executing.*db_stress\|db_stress " "$logfile" 2>/dev/null | head -1)"
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
    shopt -u nullglob

    if [ "$FOUND_FAILURE_LOG" = false ] && [ -n "$MATRIX_FAILURES_FILE" ] && [ -f "$MATRIX_FAILURES_FILE" ]; then
        echo "--- Raw matrix summary ---" >> "$FAILURE_SUMMARY"
        cat "$MATRIX_FAILURES_FILE" >> "$FAILURE_SUMMARY"
        echo "" >> "$FAILURE_SUMMARY"
    fi

    echo "Failure summary: $FAILURE_SUMMARY ($(wc -l < "$FAILURE_SUMMARY") lines)"

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
    RERUN_ARGS=("$SCRIPT_PATH" \
        --repo "$REPO_DIR" \
        --skip-first-build \
        --target-duration "$TARGET_DURATION" \
        --parallel "$PARALLEL" \
        --variants "$VARIANTS" \
        --modes "$MODES" \
        --jobs "$JOBS" \
        --max-iterations "$MAX_ITERATIONS")
    if [ -n "$EXTRA_FLAGS" ]; then
        RERUN_ARGS+=(--extra-flags "$EXTRA_FLAGS")
    fi
    echo "  Re-run:  $(shell_join "${RERUN_ARGS[@]}")"
    echo "  Stop:    $(shell_join "$SCRIPT_PATH" --repo "$REPO_DIR" --stop)"
    echo ""
    exit 1
done

echo ""
echo "============================================="
echo "=== MAX ITERATIONS ($MAX_ITERATIONS) REACHED ==="
echo "=== Stress tests still failing. Manual fix needed. ==="
echo "============================================="
exit 1
