#!/bin/bash
# Compute test shard boundaries for parallel CI execution.
# Distributes tests round-robin across N shards for balanced load.
# Outputs an EXCLUDE_TESTS_REGEX that filters out tests NOT in the given shard.
#
# Usage: compute-test-shard.sh <shard_index> <num_shards>
# Output: Sets 'exclude_regex' in $GITHUB_OUTPUT

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
awk -v s="$shard" -v n="$nshards" 'NR > 0 && (NR - 1) % n != s' /tmp/all_tests.txt > /tmp/exclude.txt

included=$(wc -l < /tmp/include.txt)

if [ -s /tmp/exclude.txt ]; then
  # Use ^./ anchor for direct entries, /run- prefix for shard entries
  regex=$(awk '{printf "^\\./%s$|/run-%s-|", $1, $1}' /tmp/exclude.txt | sed 's/|$//')
  echo "exclude_regex=${regex}" >> "$GITHUB_OUTPUT"
else
  echo 'exclude_regex=^NOTHING_TO_EXCLUDE$' >> "$GITHUB_OUTPUT"
fi

first=$(head -1 /tmp/include.txt)
last=$(tail -1 /tmp/include.txt)
echo "Shard $shard/$nshards: $included tests (round-robin), first=$first last=$last (total $total)"
echo "Excluding $(wc -l < /tmp/exclude.txt) tests"
