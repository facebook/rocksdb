# Debates: threading_model

## Summary

No genuine disagreements were found between the CC and Codex reviews. Both reviewers identified overlapping issues and agreed on all shared findings. The reviews were complementary -- CC found some issues Codex missed and vice versa, but neither contradicted the other on any factual claim.

## Areas of Overlap (Both Agree)

The following issues were independently identified by both reviewers with consistent conclusions:

1. **Ch4: GetThreadLocalSuperVersion version_number comparison** -- Both agree it is fabricated. Code only checks kSVObsolete.
2. **Ch7: VectorRepFactory concurrent insert** -- Both agree the doc is wrong; VectorRepFactory also supports concurrent insert.
3. **Ch7: filter_deletes stale reference** -- Both agree it should be removed.
4. **Ch8: Subcompactions not on thread pool** -- Both agree sub-jobs use port::Thread, not the LOW pool.
5. **Ch5: options_mutex_ ordering** -- Both agree it is acquired before mutex_, not independently.

## Complementary Findings (No Contradiction)

CC-only findings (not contradicted by Codex):
- Ch1: Four pools not three (USER pool)
- Ch2: "3 consecutive" should be "3 total" slow yields
- Ch2: Dummy writer at tail not head
- Ch4: Installation ordering reversed
- Ch10: Compression context uses CoreLocalArray, not ThreadLocalPtr

Codex-only findings (not contradicted by CC):
- Ch2: MANIFEST amortization claim misleading (only sync writes)
- Ch3: two_write_queues does not trade consistency
- Ch3: skip_concurrency_control is optional
- Ch3: Pipelined write + atomic_flush incompatibility missing
- Ch3: unordered_write + max_successive_merges incompatibility missing
- Ch4: Fast path still uses atomic operations (not just load+compare)
- Ch8: max_subcompactions not a hard cap (round-robin exception)
- Ch9: Close() calls CancelAllBackgroundWork(false), not (true)
- Ch9: WAL Flush vs memtable flush confusion
- Ch9: DB shutdown does not terminate Env thread pools
- Ch9: Shutdown flag memory ordering is not uniformly acquire/release
- Ch10: CreateSnapshot should be GetSnapshot
- Ch10: Reference counting memory ordering description wrong
- Ch1: BOTTOM pool described too narrowly
- Ch1: max_background_jobs is scheduling limits, not pool sizes
