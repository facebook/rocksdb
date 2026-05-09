#!/bin/bash
# Trim ccache to keep only entries accessed during the current build.
#
# Usage: 
#   1. Before build: touch "$CCACHE_DIR/.build_marker"
#   2. Run build (ccache updates mtime on hits, creates new files for misses)
#   3. After build: .github/scripts/ccache-trim.sh
#
# This removes cache files not accessed during the build (stale entries from
# previous commits). Only intended for CI where each run builds one commit.
# Do NOT use on local builds where multiple worktrees may share the cache.

set -e

CCACHE_DIR="${CCACHE_DIR:?CCACHE_DIR must be set}"
MARKER="$CCACHE_DIR/.build_marker"

if [ ! -f "$MARKER" ]; then
    echo "ccache-trim: No .build_marker found, skipping (was the marker created before build?)"
    exit 0
fi

# Count files before cleanup
before=$(find "$CCACHE_DIR" -type f \( -name '*R' -o -name '*M' \) | wc -l)

# Delete cache files (results and manifests) older than the marker
find "$CCACHE_DIR" -type f \( -name '*R' -o -name '*M' \) ! -newer "$MARKER" -delete

# Clean up empty directories
find "$CCACHE_DIR" -mindepth 2 -type d -empty -delete 2>/dev/null || true

# Recalculate size counters
ccache -c 2>/dev/null || true

after=$(find "$CCACHE_DIR" -type f \( -name '*R' -o -name '*M' \) | wc -l)
echo "ccache-trim: $before -> $after cache files (removed $((before - after)) stale entries)"

# Clean up marker
rm -f "$MARKER"
