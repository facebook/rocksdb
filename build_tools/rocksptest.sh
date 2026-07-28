#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
#
# Build RocksDB unit test binaries and run them under gtest-parallel,
# which shards the test cases across CPUs for faster execution.
#
# Hardened version of a simple `make <bin> && gtest-parallel ./<bin>` helper:
#  * AUTO_CLEAN=1 so the Makefile automatically runs a clean when the build
#    parameters (DEBUG_LEVEL, sanitizers, ASSERT_STATUS_CHECKED, RTTI, etc.)
#    have changed since the last build, preventing stale/mixed object files.
#  * Parallel build with -j<NCORES>, computed the same way the Makefile does.
#  * Uses the gtest-parallel checked in alongside this script (build_tools/),
#    so it works regardless of PATH.
#  * `set -euo pipefail` so any failure stops the script.
#
# Run from the repository root.
#
# Accepts one or more test binaries (gtest-parallel pools all their test cases
# into one shared worker queue). The leading non-option arguments are treated as
# binaries; everything from the first option onward is forwarded to
# gtest-parallel / the test binaries.
#
# Example usage:
#   build_tools/rocksptest.sh db_test
#   build_tools/rocksptest.sh db_test -r1000 --gtest_filter=*MixedSlowdown*
#   build_tools/rocksptest.sh db_test env_test db_basic_test
#   build_tools/rocksptest.sh db_test env_test --gtest_filter=*Foo*

set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <test_binary> [more_test_binaries...] [gtest-parallel/test args...]" >&2
  echo "example: $0 db_test env_test -r1000 --gtest_filter=*MixedSlowdown*" >&2
  exit 1
fi

# First argument must be a test binary, not an option (catches a forgotten name).
if [ "${1#-}" != "$1" ]; then
  echo "$0: first argument must be a test binary name, not an option ('$1')" >&2
  exit 1
fi

# Collect the leading non-option arguments as test binaries; everything from the
# first option onward is forwarded to gtest-parallel / the test binaries.
BINS=()
while [ "$#" -gt 0 ] && [ "${1#-}" = "$1" ]; do
  BINS+=("$1")
  shift
done

# Paths as gtest-parallel expects them (relative to the current directory).
BIN_PATHS=()
for b in "${BINS[@]}"; do
  BIN_PATHS+=("./$b")
done

# Directory of this script, so we use the gtest-parallel checked in next to it.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Compute parallelism the same way the Makefile computes NCORES.
NCORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

make AUTO_CLEAN=1 -j"$NCORES" "${BINS[@]}"
"$SCRIPT_DIR/gtest-parallel" "${BIN_PATHS[@]}" "$@"
