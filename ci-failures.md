# PR #14386 CI Failures (Run 24046546165)
# https://github.com/facebook/rocksdb/actions/runs/24046546165

## One failing test across all jobs

Test: DBWideBlobDirectWriteTest.SingleCfCoalescingIteratorMatchesDirectIteratorAfterReuseAndSeekRefresh
File: db/wide/db_wide_blob_direct_write_test.cc:132

## Affected jobs (all same root cause)
  build-linux
  build-linux-cmake-with-folly-coroutines
  build-linux-encrypted_env-no_compression
  build-linux-make-with-folly
  build-linux-static_lib-alt_namespace-status_checked
  build-linux-cmake-with-benchmark-no-thread-status (2)
  build-macos-cmake (0)(1)(2)(3)
  build-linux-clang18-asan-ubsan (0)
  build-linux-clang18-mini-tsan (0)

## Test scenario

The test: VerifySingleCfCoalescingIteratorMatchesDirectIterator with:
  - use_trie_index = true
  - refresh_before_seek = true      (Flush + CompactRange before main Seek)
  - reuse_iterator_before_refresh = true  (iterator used BEFORE flush/compact)
  - create_control_before_refresh = false (control iterator created AFTER flush)

Setup:
  1. Write 32 keys with PutEntity, columns: {kDefaultWideColumnName: 128*'A'+"_N", meta: 96*'B'+"_N"}
  2. Flush + MoveFilesToLevel(1)
  3. Write same 32 keys again with columns: {kDefaultWideColumnName: 128*'C'+"_N", meta: 96*'D'+"_N"}
     (still in memtable, min_blob_size=64 so values go to blob files)
  4. Create coalescing iterator with allow_unprepared_value=true,
     auto_refresh_iterator_with_snapshot=true, use_trie_index=true
  5. REUSE: Seek to key[4], PrepareValue(), verify columns == 'C' fill -- PASSES
  6. Next() -- one more step
  7. Flush + CompactRange (triggers auto_refresh on next Seek)
  8. Create control iterator (after flush)
  9. Seek both iterators to key[8]
  10. Iterate from key[8] to key[31], comparing coalescing vs control

Failure at line 132 (inside the reuse block at step 5, first_index = kNumKeys/8 = 4):
  ASSERT_EQ(MakeIteratorStressColumns('C', first_index), coalescing->columns())

  Expected: { :|CCCC...CCC_4, meta:DDDD...DDD_4 }
  Got:      { :|CCCC...CCC_4, meta:<8 bytes garbage>\0\0\0\0\0\0\0\0D<4 bytes garbage>DDDD...DDD_4 }

## Analysis

The 'meta' column value is wrong after PrepareValue() on the coalesced iterator.
The default column (kDefaultWideColumnName, :|) is resolved correctly.
The 'meta' column contains:
  - Correct blob-index prefix bytes (same as expected prefix, pointing to blob file)
  - Then 8 null bytes (\0\0\0\0\0\0\0\0)
  - Then 4-5 bytes of what looks like a partially decoded BlobIndex struct
  - Then the correct DDDD data

This suggests the blob value for the 'meta' column is NOT being resolved
through the write-path fallback (BlobFilePartitionManager) correctly when
accessed via the coalescing iterator + trie index + allow_unprepared_value.

The sequence that triggers it:
  1. Iterator is created before flush (data in memtable + blob files)
  2. Iterator is used (Seek + PrepareValue) -- first resolve works? No -- FAILS here
  3. The failure is at step 5 (reuse before refresh), not in the main iteration

Wait -- re-reading: failure is at line 132 which is inside the
reuse_iterator_before_refresh block. So the FIRST use of the iterator
already returns wrong data for the meta column.

The coalescing iterator with trie_index + allow_unprepared_value is NOT
resolving the meta blob column correctly on PrepareValue() -- it returns
the raw BlobIndex bytes (or partially decoded value) instead of fetching
from the write-path blob file partition manager.

## Passing (new in this run vs previous)
  - DBOpenWithConfigTest SEGFAULTs: appear to be gone
  - build-windows-vs2022: FIXED

## Passing (consistent)
  check-format-and-targets, clang-tidy, build-linux-arm,
  build-linux-clang-*-no_test_run, build-linux-release,
  build-linux-java*, build-linux-mini-crashtest, Meta CLA Check
