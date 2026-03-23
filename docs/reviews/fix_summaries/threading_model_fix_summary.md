# Fix Summary: threading_model

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 15 |
| Completeness | 6 |
| Structure/Style | 3 |
| **Total** | **24** |

## Disagreements Found

0 -- Both reviewers agreed on all shared findings. See threading_model_debates.md for details.

## Changes Made

### index.md
- Fixed chapter 3 summary to clarify only unordered_write relaxes consistency
- Updated thread pool description to include USER pool
- Changed "eliminates atomic ops" to "avoids mutex and ref-count contention" for lock-free reads
- Separated Key Invariants (true correctness invariants) from Key Design Properties (behavioral facts)
- Added options_mutex_ ordering to invariant list
- Updated shutdown chapter summary

### 01_thread_pools.md
- Changed "three priority-based thread pools" to "four" and added USER pool
- Added USER row to priority levels table with description
- Clarified max_background_jobs sets scheduling limits, not Env thread pool sizes
- Expanded BOTTOM pool description (last-level-oriented, not just "bottommost")

### 02_write_thread_group_commit.md
- Removed MANIFEST amortization claim from overview; noted it only occurs on sync writes
- Changed "3 consecutive slow yields" to "3 total slow yields"
- Removed code block (backtick-fenced formula) per style guidelines
- Changed dummy writer position from "head" to "tail of the writer queue"

### 03_write_modes.md
- Rewrote overview to distinguish throughput modes from consistency-relaxing modes
- Added atomic_flush to pipelined write incompatibility list
- Changed skip_concurrency_control from required to optional optimization
- Added unordered_write + max_successive_merges incompatibility note

### 04_lock_free_read_path.md
- Removed fabricated version_number comparison step from Read Acquisition Flow
- Rewrote flow to match actual code: Swap -> check kSVObsolete -> use or refresh
- Added note about GetReferencedSuperVersion() vs GetThreadLocalSuperVersion()
- Fixed Performance Benefit to describe atomic exchange/CAS instead of "load + compare"
- Added cfd field to SuperVersion structure list
- Fixed Installation ordering: sweep -> Unref -> increment (was: increment -> Unref)

### 05_mutex_hierarchy.md
- Changed options_mutex_ from "acquired independently" to "acquired before mutex_ when both needed"

### 07_concurrent_memtable_writes.md
- Added VectorRepFactory to supported memtable list
- Removed stale filter_deletes reference

### 08_subcompactions.md
- Changed overview from "thread pool workers" to "port::Thread spawned directly"
- Fixed parallel execution to describe first sub-job on caller thread, additional on port::Thread
- Rewrote thread pool interaction section to distinguish job scheduling limits from sub-job fan-out
- Changed max_subcompactions from "hard upper bound" to "baseline limit" with round-robin exception

### 09_shutdown_and_cleanup.md
- Rewrote overview to mention three shutdown flags and clarify DB close doesn't terminate Env pools
- Added two-phase shutdown description (shutdown_initiated_ then shutting_down_)
- Added reject_new_background_jobs_ for WaitForCompact close path
- Fixed shutdown flag memory ordering description (not uniformly acquire/release)
- Expanded WaitForCompact with full WaitForCompactOptions fields
- Renamed "Thread Pool Shutdown" to "Thread Pool Lifecycle" -- clarified it's Env-level, not DB-level
- Fixed Close() sequence: calls CancelAllBackgroundWork(false), not (true)
- Added expanded wait conditions in CloseHelper
- Added note that Close() does not fsync WAL
- Fixed shutdown table: changed "WAL Flush" column to "Memtable Flush", corrected Close() row

### 10_thread_safety_reference.md
- Expanded Files: line to include iterator.h, write_batch.h, snapshot.h, thread_local.h, threadpool_imp.cc
- Fixed CreateSnapshot() to GetSnapshot()
- Rewrote memory ordering table with accurate orderings per operation
- Moved compression context from ThreadLocalPtr to separate CoreLocalArray section
