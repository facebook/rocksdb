#!/bin/bash
#
# stress_trace_files.sh — List .cc files changed in recent commits for
# selective -finstrument-functions instrumentation.
#
# Only outputs .cc files that were directly modified (not headers).
# Excludes test files and infrastructure.
#
# Usage:
#   STRESS_TRACE=1 STRESS_TRACE_FILES="$(tools/stress_trace_files.sh)" make db_stress
#   tools/stress_trace_files.sh --days 14 --verbose

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
            echo "Output .cc files modified in recent commits, suitable for"
            echo "STRESS_TRACE_FILES. Only .cc files, no headers, no tests."
            echo ""
            echo "Options:"
            echo "  --days N     Look back N days (default: 7)"
            echo "  --ref REF    Git ref to examine (default: HEAD)"
            echo "  --verbose    Show file list to stderr"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

cd "$REPO_ROOT"

SINCE="$(date -d "$DAYS days ago" +%Y-%m-%d 2>/dev/null || date -v-${DAYS}d +%Y-%m-%d)"

# Get only .cc files (not .h), deduplicate, exclude tests/infra
FILES=$(git log --since="$SINCE" "$REF" --name-only --pretty=format: -- '*.cc' \
    | grep -v '^$' \
    | sort -u \
    | grep -v '_test\.cc$' \
    | grep -v '_bench\.cc$' \
    | grep -v '^java/' \
    | grep -v '^examples/' \
    | grep -v '^microbench/' \
    | grep -v '^fuzz/' \
    | grep -v '^third-party/' \
    | grep -v '^build_tools/' \
    | grep -v '^cmake/')

if [ -z "$FILES" ]; then
    [ "$VERBOSE" -eq 1 ] && echo "No .cc files changed in last $DAYS days" >&2
    exit 0
fi

if [ "$VERBOSE" -eq 1 ]; then
    N=$(echo "$FILES" | wc -l)
    echo "=== $N .cc files changed in last $DAYS days ===" >&2
    echo "$FILES" >&2
fi

# Output as space-separated list
echo "$FILES" | tr '\n' ' '
