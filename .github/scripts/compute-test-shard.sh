#!/bin/bash
# Compute test shard boundaries for parallel CI execution.
# Splits test list into N shards and outputs an EXCLUDE_TESTS_REGEX
# that filters out tests NOT in the given shard.
#
# Usage: compute-test-shard.sh <shard_index> <num_shards>
# Output: Sets 'exclude_regex' in $GITHUB_OUTPUT

set -euo pipefail

shard=${1:?Usage: compute-test-shard.sh <shard_index> <num_shards>}
nshards=${2:?Usage: compute-test-shard.sh <shard_index> <num_shards>}

# Get sorted test list matching Makefile order (db_test first, then alpha)
make -s list_all_tests 2>/dev/null | tr ' ' '\n' | grep '_test$' | sort -u > /tmp/sorted.txt
(echo db_test; grep -v '^db_test$' /tmp/sorted.txt) > /tmp/all_tests.txt

total=$(wc -l < /tmp/all_tests.txt)
shard_size=$(( (total + nshards - 1) / nshards ))
start_idx=$(( shard * shard_size + 1 ))
end_idx=$(( (shard + 1) * shard_size ))
if [ "$end_idx" -gt "$total" ]; then end_idx=$total; fi

# Build EXCLUDE regex for tests NOT in this shard
# Must match both ./test_name and t/run-test_name-shard-N formats
(if [ "$start_idx" -gt 1 ]; then sed -n "1,$((start_idx - 1))p" /tmp/all_tests.txt; fi
 if [ "$end_idx" -lt "$total" ]; then sed -n "$((end_idx + 1)),${total}p" /tmp/all_tests.txt; fi
) > /tmp/exclude.txt

if [ -s /tmp/exclude.txt ]; then
  # Use ^./ anchor for direct entries, /run- prefix for shard entries
  regex=$(awk '{printf "^\\./%s$|/run-%s-|", $1, $1}' /tmp/exclude.txt | sed 's/|$//')
  # Double $ signs so Make passes literal $ to shell (Make interprets $| as special)
  regex_escaped=$(echo "$regex" | sed 's/\$/\$\$/g')
  echo "exclude_regex=${regex_escaped}" >> "$GITHUB_OUTPUT"
else
  echo 'exclude_regex=^NOTHING_TO_EXCLUDE$' >> "$GITHUB_OUTPUT"
fi

first=$(sed -n "${start_idx}p" /tmp/all_tests.txt)
last=$(sed -n "${end_idx}p" /tmp/all_tests.txt)
echo "Shard $shard/$nshards: $first .. $last (tests $start_idx-$end_idx of $total)"
echo "Excluding $(wc -l < /tmp/exclude.txt) tests"
