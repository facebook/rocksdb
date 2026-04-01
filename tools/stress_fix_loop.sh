#!/bin/bash
#
# RocksDB Stress-Fix Loop
#
# Automated loop that runs crash tests, analyzes failures with Claude Code,
# applies fixes, and repeats until stress tests pass cleanly at the target
# duration. Once clean, optionally pushes to GitHub.
#
# Usage:
#   ./tools/stress_fix_loop.sh [OPTIONS]
#
# Options:
#   --target-duration N   Duration (seconds) that must pass clean to exit (default: 3600)
#   --parallel N          Parallel runs per variant (default: 4)
#   --variants LIST       Comma-separated variants (default: debug,asan,tsan,release)
#   --extra-flags F       Extra flags for db_crashtest.py
#   --max-iterations N    Max fix iterations before giving up (default: 10)
#   --push                Push to GitHub after passing (default: no)
#   --skip-first-build    Skip initial build (reuse existing binaries)
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
#   - release variant rejects --read_fault_one_in in db_stress. Not a bug.
#   - Features with lower durability (e.g., blob direct write deferred mode)
#     need db_crashtest.py to treat them as data-loss modes (like disable_wal).
#
# Examples:
#   # Fix loop for blob direct write until 1hr clean
#   ./tools/stress_fix_loop.sh --parallel 4 \
#     --extra-flags "--enable_blob_direct_write=1 --enable_blob_files=1 \
#       --blob_direct_write_partitions=4 --blob_direct_write_buffer_size=1048576"
#
#   # Quick loop: 30min target, 2 parallel, push when done
#   ./tools/stress_fix_loop.sh --target-duration 1800 --parallel 2 --push
#
#   # Just debug+asan variants
#   ./tools/stress_fix_loop.sh --variants debug,asan --extra-flags "--enable_blob_direct_write=1"
#

set -e

# Defaults
TARGET_DURATION=3600
PARALLEL=4
VARIANTS="debug,asan,tsan,release"
EXTRA_FLAGS=""
MAX_ITERATIONS=10
PUSH_ON_SUCCESS=false
SKIP_FIRST_BUILD=false
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JOBS=128

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --target-duration) TARGET_DURATION="$2"; shift 2 ;;
        --parallel) PARALLEL="$2"; shift 2 ;;
        --variants) VARIANTS="$2"; shift 2 ;;
        --extra-flags) EXTRA_FLAGS="$2"; shift 2 ;;
        --max-iterations) MAX_ITERATIONS="$2"; shift 2 ;;
        --push) PUSH_ON_SUCCESS=true; shift ;;
        --skip-first-build) SKIP_FIRST_BUILD=true; shift ;;
        --jobs) JOBS="$2"; shift 2 ;;
        --help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

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
    STRESS_LOG="/tmp/stress-fix-loop-iter${iteration}.log"

    bash "$REPO_DIR/tools/run_stress_matrix.sh" \
        --parallel "$PARALLEL" \
        --variants "$VARIANTS" \
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
    FAILURE_SUMMARY="/tmp/stress-fix-loop-failures-iter${iteration}.txt"
    echo "Iteration $iteration failures:" > "$FAILURE_SUMMARY"
    echo "" >> "$FAILURE_SUMMARY"

    # Find which batch failed
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

        # Check for errors
        has_error=false
        for pattern in "SUMMARY.*Sanitizer" "Corruption" "Invalid blob" \
                       "Verification failed" "No such file" "SafeTerminate" \
                       "stack-use-after" "heap-use-after" "data race"; do
            if grep -q "$pattern" "$logfile" 2>/dev/null; then
                has_error=true
                break
            fi
        done

        if [ "$has_error" = true ] || [ "$exit_line" != "EXIT: 0" ]; then
            echo "=== $label ===" >> "$FAILURE_SUMMARY"
            # Get the key error lines
            grep -m3 "SUMMARY\|Corruption\|Invalid blob\|Verification failed\|No such file\|SafeTerminate\|ERROR.*Sanitizer\|data race" "$logfile" >> "$FAILURE_SUMMARY" 2>/dev/null
            echo "" >> "$FAILURE_SUMMARY"
            # Get stack trace context
            grep -B 2 -A 10 "SUMMARY\|Corruption.*blob\|SafeTerminate" "$logfile" 2>/dev/null | head -30 >> "$FAILURE_SUMMARY"
            echo "" >> "$FAILURE_SUMMARY"
        fi
    done

    echo "Failure summary: $FAILURE_SUMMARY ($(wc -l < "$FAILURE_SUMMARY") lines)"

    # === LAUNCH CC TO FIX ===
    echo ""
    echo "--- Launching Claude Code to fix (iteration $iteration) ---"

    CC_PROMPT="/tmp/cc-stressfix-iter${iteration}-prompt.txt"
    cat > "$CC_PROMPT" << CCEOF
You are fixing crash test failures in RocksDB blob direct write (iteration $iteration).
Repo: /home/xbw/workspace/ws21/rocksdb

The crash test was run with:
  $EXTRA_FLAGS

Failure details are in $FAILURE_SUMMARY — read that file first.

Previous iterations may have partially fixed issues. Focus on the NEW failures.

Instructions:
1. Read $FAILURE_SUMMARY for failure details
2. Analyze root causes systematically
3. Fix all bugs found
4. Build: make -j${JOBS} db_blob_direct_write_test db_stress
5. Run unit tests: ./db_blob_direct_write_test
6. Run a quick 2-minute stress test to verify:
   python3 tools/db_crashtest.py --stress_cmd=./db_stress --duration=120 \
     $EXTRA_FLAGS blackbox
7. If quick stress test fails, analyze and fix, then retry step 6 (up to 3 retries)
8. Run: make format-auto
9. Do NOT commit — leave changes unstaged.
CCEOF

    CC_RESULT="/tmp/cc-stressfix-iter${iteration}-result.json"
    CC_SENTINEL="/tmp/cc-stressfix-iter${iteration}-done.sentinel"
    rm -f "$CC_SENTINEL"

    cat > "/tmp/cc-stressfix-iter${iteration}-run.sh" << RUNEOF
#!/bin/bash
source ~/.bashrc 2>/dev/null
cd /home/xbw/workspace/ws21/rocksdb
claude -p --dangerously-skip-permissions --output-format json "\$(cat $CC_PROMPT)" < /dev/null \
  > $CC_RESULT 2>&1
echo "\$?" > $CC_SENTINEL
RUNEOF
    chmod +x "/tmp/cc-stressfix-iter${iteration}-run.sh"

    tmux kill-session -t cc-stressfix 2>/dev/null
    tmux new-session -d -s cc-stressfix "/tmp/cc-stressfix-iter${iteration}-run.sh"

    echo "Waiting for CC to finish..."
    while [ ! -f "$CC_SENTINEL" ]; do
        sleep 15
        # Check if tmux died
        if ! tmux has-session -t cc-stressfix 2>/dev/null; then
            echo "ERROR: CC tmux session died!"
            break
        fi
    done

    CC_EXIT=$(cat "$CC_SENTINEL" 2>/dev/null || echo "unknown")
    echo "CC finished with exit: $CC_EXIT"

    if [ "$CC_EXIT" != "0" ]; then
        echo "CC failed! Manual intervention needed."
        echo "Result: $CC_RESULT"
        exit 1
    fi

    # Print CC summary
    python3 -c "
import json
d = json.load(open('$CC_RESULT'))
print(f'CC turns: {d.get(\"num_turns\", \"?\")}, cost: \${d.get(\"cost_usd\", 0):.2f}')
r = d.get('result', '')
print(r[:1500])
" 2>/dev/null || tail -20 "$CC_RESULT"

    # === COMMIT LOCALLY (no push) ===
    echo ""
    echo "--- Committing fixes locally ---"
    cd "$REPO_DIR"
    git add -A -- '*.cc' '*.h' '*.py'
    CHANGED=$(git diff --cached --stat | tail -1)
    if [ -n "$CHANGED" ]; then
        git commit -m "Stress-fix iteration $iteration: fix crash test failures

Auto-generated by stress_fix_loop.sh iteration $iteration.
$(head -20 "$FAILURE_SUMMARY" | sed 's/^/  /')"
        echo "Committed: $CHANGED"
    else
        echo "WARNING: No changes to commit. CC may not have modified any files."
    fi

    echo ""
    echo "--- Rebuilding variants for next iteration ---"
    # Variants need to be rebuilt with the new code
    # (Don't use --skip-build on next iteration)

done

echo ""
echo "============================================="
echo "=== MAX ITERATIONS ($MAX_ITERATIONS) REACHED ==="
echo "=== Stress tests still failing. Manual fix needed. ==="
echo "============================================="
exit 1
