#!/bin/bash
# Output test progress in JSON format for machine parsing
# Usage: build_tools/check_progress.sh

LOG_FILE="LOG"
T_DIR="t"
SRC_MK="src.mk"

# Maximum lines of test output to include per failed test
MAX_OUTPUT_LINES=50

# Helper to escape string for JSON (handles newlines, quotes, backslashes, tabs)
json_escape() {
    local str="$1"
    # Use python for reliable JSON escaping if available, otherwise use sed
    if command -v python3 &>/dev/null; then
        printf '%s' "$str" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1], end="")'
    else
        printf '%s' "$str" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g; s/\r/\\r/g' | awk '{printf "%s\\n", $0}' | sed 's/\\n$//'
    fi
}

# Helper to output JSON and exit
output_json() {
    local status="$1"
    local completed="${2:-0}"
    local total="${3:-0}"
    local failed="${4:-0}"
    local percent="${5:-0}"
    local eta="${6:-0}"
    local avg_time="${7:-0}"
    local last_item="${8:-}"
    local phase="${9:-}"
    local failed_tests="${10:-}"

    # Build JSON output
    local json="{\"status\":\"$status\""

    if [[ -n "$phase" ]]; then
        json="$json,\"phase\":\"$phase\""
    fi

    json="$json,\"completed\":$completed,\"total\":$total,\"failed\":$failed,\"percent\":$percent"
    json="$json,\"eta_seconds\":$eta,\"avg_time\":\"$avg_time\",\"last_item\":\"$(json_escape "$last_item")\""

    if [[ -n "$failed_tests" ]]; then
        json="$json,\"failed_tests\":[$failed_tests]"
    fi

    json="$json}"
    echo "$json"
}

# Get failed test info with log output
get_failed_tests_json() {
    local log_file="$1"
    local t_dir="$2"
    local max_failures=10
    local count=0
    local first=true

    # Get failed tests from LOG file
    while IFS=$'\t' read -r seq host starttime runtime send recv exitval signal cmd; do
        # Skip header line
        [[ "$seq" == "Seq" ]] && continue

        # Check if failed (exitval != 0 or signal != 0)
        if [[ "$exitval" != "0" || "$signal" != "0" ]]; then
            # Extract test name from command
            test_name=$(echo "$cmd" | sed 's,.*/run-,,;s, .*,,')

            # Get log file path
            log_path="$t_dir/log-run-$test_name"

            # Read test output (last N lines)
            if [[ -f "$log_path" ]]; then
                output=$(tail -n "$MAX_OUTPUT_LINES" "$log_path" 2>/dev/null)
            else
                output="(log file not found: $log_path)"
            fi

            # Escape output for JSON
            escaped_output=$(json_escape "$output")

            # Build JSON object for this failure
            if [[ "$first" == "true" ]]; then
                first=false
            else
                printf ","
            fi
            printf '{"test":"%s","exit_code":%d,"signal":%d,"output":"%s"}' \
                "$test_name" "$exitval" "$signal" "$escaped_output"

            ((count++))
            if [[ $count -ge $max_failures ]]; then
                break
            fi
        fi
    done < "$log_file"
}

# Check if tests are running (LOG file exists)
if [[ -f "$LOG_FILE" ]]; then
    # Count total tests from t/run-* files
    if [[ -d "$T_DIR" ]]; then
        total=$(find "$T_DIR" -name 'run-*' -type f 2>/dev/null | wc -l)
    else
        total=0
    fi

    # If no parallel tests generated yet
    if [[ "$total" -eq 0 ]]; then
        output_json "running" 0 0 0 0 0 "0" "" "generating"
        exit 0
    fi

    # Parse LOG file (skip header line)
    # LOG format: Seq Host Starttime JobRuntime Send Receive Exitval Signal Command
    completed=$(tail -n +2 "$LOG_FILE" 2>/dev/null | wc -l)

    # Count failures
    failed=$(awk -F'\t' 'NR>1 && ($7 != 0 || $8 != 0) {count++} END {print count+0}' "$LOG_FILE" 2>/dev/null)

    # Get failed tests JSON with output (only if there are failures)
    if [[ "$failed" -gt 0 ]]; then
        failed_tests=$(get_failed_tests_json "$LOG_FILE" "$T_DIR")
    else
        failed_tests=""
    fi

    # Calculate percentage
    if [[ "$total" -gt 0 ]]; then
        percent=$((completed * 100 / total))
    else
        percent=0
    fi

    # Get last completed test name (extract from command column)
    last_test=$(tail -1 "$LOG_FILE" 2>/dev/null | awk -F'\t' '{print $9}' | sed 's,.*/run-,,;s, .*,,;s,^./,,')

    # Calculate ETA based on average time
    if [[ "$completed" -gt 0 ]]; then
        avg_time=$(awk -F'\t' 'NR>1 {sum+=$4; count++} END {if(count>0) printf "%.1f", sum/count; else print "0"}' "$LOG_FILE")
        remaining=$((total - completed))
        eta=$(awk "BEGIN {printf \"%.0f\", $avg_time * $remaining}")
    else
        avg_time="0"
        eta="0"
    fi

    # Determine status
    if [[ "$completed" -ge "$total" ]]; then
        status="completed"
    elif [[ "$completed" -gt 0 ]]; then
        status="running"
    else
        status="starting"
    fi

    output_json "$status" "$completed" "$total" "$failed" "$percent" "$eta" "$avg_time" "$last_test" "testing" "$failed_tests"
    exit 0
fi

# No LOG file - check if we're in compilation/linking phase
# Count expected source files from src.mk
if [[ -f "$SRC_MK" ]]; then
    # Count LIB_SOURCES (library object files to compile)
    expected_lib_objects=$(grep -E '\.cc\s*\\?$' "$SRC_MK" | grep -v '^#' | wc -l)

    # Count TEST_MAIN_SOURCES (test binaries to link)
    expected_test_binaries=$(sed -n '/^TEST_MAIN_SOURCES =/,/^[^ ]/p' "$SRC_MK" | grep -cE '\.cc\s*\\?$' 2>/dev/null || echo 0)
else
    expected_lib_objects=0
    expected_test_binaries=0
fi

# Check for test generation phase (t/ directory being created)
if [[ -d "$T_DIR" ]]; then
    total=$(find "$T_DIR" -name 'run-*' -type f 2>/dev/null | wc -l)
    if [[ "$total" -gt 0 ]]; then
        output_json "running" 0 "$total" 0 0 0 "0" "" "generating"
        exit 0
    fi
fi

# Count compiled object files (in subdirectories matching source structure)
# Object files are created as dir/file.o (e.g., cache/cache.o, db/db_impl.o)
compiled_objects=0
if [[ "$expected_lib_objects" -gt 0 ]]; then
    # Count .o files in source directories
    compiled_objects=$(find cache db env file logging memory memtable monitoring options port table test_util trace_replay util utilities -name '*.o' -type f 2>/dev/null | wc -l)
fi

# Count linked test binaries (test binaries are in current directory with _test suffix)
linked_tests=0
if [[ "$expected_test_binaries" -gt 0 ]]; then
    linked_tests=$(find . -maxdepth 1 -name '*_test' -type f -executable 2>/dev/null | wc -l)
fi

# Determine phase based on what exists
if [[ "$compiled_objects" -eq 0 && "$linked_tests" -eq 0 ]]; then
    # Nothing compiled yet - not started or just beginning
    output_json "not_started" 0 0 0 0 0 "0" ""
    exit 0
fi

# Calculate total work units: compiling + linking
total_work=$((expected_lib_objects + expected_test_binaries))
completed_work=$((compiled_objects + linked_tests))

if [[ "$total_work" -gt 0 ]]; then
    percent=$((completed_work * 100 / total_work))
else
    percent=0
fi

# Determine phase
if [[ "$compiled_objects" -lt "$expected_lib_objects" ]]; then
    phase="compiling"
    # Get most recently modified .o file as last_item
    last_item=$(find cache db env file logging memory memtable monitoring options port table test_util trace_replay util utilities -name '*.o' -type f -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2- | sed 's,^\./,,;s,\.o$,,')
elif [[ "$linked_tests" -lt "$expected_test_binaries" ]]; then
    phase="linking"
    # Get most recently modified test binary as last_item
    last_item=$(find . -maxdepth 1 -name '*_test' -type f -executable -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2- | sed 's,^\./,,')
else
    phase="generating"
    last_item=""
fi

output_json "running" "$completed_work" "$total_work" 0 "$percent" 0 "0" "$last_item" "$phase"
