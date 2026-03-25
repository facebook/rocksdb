#!/bin/bash
#
# stress_trace_files.sh — Determine which .cc files to instrument for
# STRESS_TRACE based on recent git activity.
#
# Logic:
#   1. Find source files (.cc/.h) changed in commits from the last N days
#   2. Map each file to its "component" (directory-level subsystem)
#   3. Expand: include ALL non-test .cc files in those components
#   4. Exclude infrastructure (tools, build, benchmarks, tests)
#
# Usage:
#   # Default: last 7 days
#   STRESS_TRACE=1 STRESS_TRACE_FILES="$(tools/stress_trace_files.sh)" make db_stress
#
#   # Custom window:
#   tools/stress_trace_files.sh --days 14
#
#   # Show components and reasoning:
#   tools/stress_trace_files.sh --verbose
#
#   # From a specific ref:
#   tools/stress_trace_files.sh --ref upstream/main

set -euo pipefail

DAYS=7
REF="HEAD"
VERBOSE=0
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --days|-d)    DAYS="$2"; shift 2 ;;
        --ref|-r)     REF="$2"; shift 2 ;;
        --verbose|-v) VERBOSE=1; shift ;;
        --help|-h)
            echo "Usage: $0 [--days N] [--ref REF] [--verbose]"
            echo ""
            echo "Output .cc files to instrument based on recent git commits."
            echo "Designed for use with STRESS_TRACE_FILES= make variable."
            echo ""
            echo "Options:"
            echo "  --days N     Look back N days (default: 7)"
            echo "  --ref REF    Git ref to examine (default: HEAD)"
            echo "  --verbose    Show component mapping and file counts to stderr"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

cd "$REPO_ROOT"

# ── Step 1: Find files changed in the time window ──────────────────────
SINCE="$(date -d "$DAYS days ago" +%Y-%m-%d 2>/dev/null || date -v-${DAYS}d +%Y-%m-%d)"
CHANGED_FILES=$(git log --since="$SINCE" "$REF" --name-only --pretty=format: -- '*.cc' '*.h' \
    | grep -v '^$' \
    | sort -u)

if [ -z "$CHANGED_FILES" ]; then
    [ "$VERBOSE" -eq 1 ] && echo "No files changed in last $DAYS days" >&2
    exit 0
fi

# ── Step 2: Map files to components ────────────────────────────────────
# A "component" is the directory that represents a logical subsystem.
# We skip directories that are infrastructure/test-only.

# Directories to always exclude from instrumentation (even if changed):
EXCLUDE_DIRS="tools|build_tools|cmake|docs|buckifier|plugin|claude_md|java|examples|microbench|fuzz|third-party|include|unreleased_history|coverage"

get_component() {
    local f="$1"
    local dir
    dir=$(dirname "$f")

    # Exclude infrastructure directories
    if echo "$dir" | grep -qE "^($EXCLUDE_DIRS)(/|$)"; then
        return
    fi

    # Exclude test files by name
    case "$(basename "$f")" in
        *_test.cc|*_test.h|*_bench.cc|mock_*.cc|mock_*.h)
            return
            ;;
    esac
    # Exclude test utility files (but keep db_stress_tool)
    case "$dir" in
        test_util*) return ;;
    esac

    echo "$dir"
}

declare -A COMPONENTS
while IFS= read -r file; do
    comp=$(get_component "$file")
    if [ -n "$comp" ]; then
        COMPONENTS["$comp"]=1
    fi
done <<< "$CHANGED_FILES"

if [ ${#COMPONENTS[@]} -eq 0 ]; then
    [ "$VERBOSE" -eq 1 ] && echo "No instrumentable components found" >&2
    exit 0
fi

# ── Step 3: Expand components to .cc files ─────────────────────────────
if [ "$VERBOSE" -eq 1 ]; then
    echo "=== Changed files (last $DAYS days, excluding infra) ===" >&2
    echo "$CHANGED_FILES" | while read -r f; do
        comp=$(get_component "$f")
        [ -n "$comp" ] && echo "  $f -> $comp/" >&2
    done
    echo "" >&2
    echo "=== Components to instrument ===" >&2
fi

OUTPUT=""
TOTAL_FILES=0
for comp in $(echo "${!COMPONENTS[@]}" | tr ' ' '\n' | sort); do
    if [ -d "$comp" ]; then
        FILES=$(find "$comp" -maxdepth 1 -name "*.cc" \
            ! -name "*_test.cc" \
            ! -name "*_bench.cc" \
            ! -name "mock_*.cc" \
            ! -name "*test_util*" \
            2>/dev/null | sort)
        N=0
        for f in $FILES; do
            OUTPUT="$OUTPUT $f"
            N=$((N + 1))
        done
        TOTAL_FILES=$((TOTAL_FILES + N))
        [ "$VERBOSE" -eq 1 ] && echo "  $comp/ ($N files)" >&2
    fi
done

OUTPUT="${OUTPUT# }"

if [ "$VERBOSE" -eq 1 ]; then
    echo "" >&2
    echo "=== Total: $TOTAL_FILES files to instrument ===" >&2
fi

echo "$OUTPUT"
