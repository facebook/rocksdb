#!/bin/bash
# Compute test shard for parallel CI execution.
# Distributes tests round-robin across N shards for balanced load.
# Outputs ROCKSDBTESTS_SUBSET — the list of test binaries for this shard.
# The Makefile uses this to build and run only the assigned tests.
#
# Usage: compute-test-shard.sh <shard_index> <num_shards>

set -euo pipefail

shard=${1:?Usage: compute-test-shard.sh <shard_index> <num_shards>}
nshards=${2:?Usage: compute-test-shard.sh <shard_index> <num_shards>}

# Get sorted test list (db_test first since it's the heaviest, then alpha)
make -s list_all_tests 2>/dev/null | tr ' ' '\n' | grep '_test$' | sort -u > /tmp/sorted.txt
(echo db_test; grep -v '^db_test$' /tmp/sorted.txt) > /tmp/all_tests.txt

total=$(wc -l < /tmp/all_tests.txt)

# Round-robin: assign test i to shard (i % nshards).
# This spreads heavy tests (which are scattered alphabetically) evenly.
awk -v s="$shard" -v n="$nshards" 'NR > 0 && (NR - 1) % n == s' /tmp/all_tests.txt > /tmp/include.txt

included=$(wc -l < /tmp/include.txt)
first=$(head -1 /tmp/include.txt)
last=$(tail -1 /tmp/include.txt)

# Output space-separated list for ROCKSDBTESTS_SUBSET
subset=$(tr '\n' ' ' < /tmp/include.txt)
echo "subset=${subset}" >> "$GITHUB_OUTPUT"

echo "Shard $shard/$nshards: $included tests (round-robin), first=$first last=$last (total $total)"
