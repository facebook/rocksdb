# Blob Direct Write Feedback Response

H1: Fixed. `BlobFilePartitionManager::MarkBlobWriteAsGarbage()` now returns
`Status::Corruption` when it cannot match the target file, and logs at
`ERROR` level instead of silently succeeding in release builds. The rollback
caller still treats rollback as best-effort, but the mismatch is no longer a
release-build no-op. Regression coverage:
`DBBlobBasicTest.DirectWriteRollbackMismatchReturnsCorruption`.

H2: Clarified and regression-tested. The sealed blob files prepared for a
flush are intentionally kept in `pending_generations_` until a manifest commit
consumes them. Added comments in the single-CF and atomic flush paths to make
that retry contract explicit. Added
`DBBlobBasicTest.DirectWritePreparedGenerationsStayReusableUntilCommit`, which
repeats `PrepareFlushAdditions()` before commit, verifies it returns the same
sealed blob file metadata again, and then verifies a normal flush succeeds.
No cleanup/garbage marking was added here because the immutable memtables
still reference those blob files until the flush is committed.

H3: Fixed. `GetImpl()` now skips `MaybeResolveBlobForWritePath()` entirely
when BDW resolution is impossible, so non-BDW column families no longer pay
that function-call overhead on the memtable/read path.

H4: Fixed. Added a DB-level `blob_direct_write_cf_count_` fast path so the
write path can test for "any BDW CF" in O(1). The count is now initialized for
BDW CFs opened at DB startup, updated when BDW CFs are created at runtime, and
decremented when they are dropped. Regression coverage:
`DBBlobBasicTest.DirectWriteCountTracksCreatedAndDroppedColumnFamily`. No
SetOptions hook was needed because `enable_blob_direct_write` is not
dynamically changeable.

M1: Fixed. `IsValidBlobOffset()` now uses separate bounds checks instead of
relying on `||` short-circuit ordering to avoid unsigned underflow.

M2: Addressed with clarification. Kept compression outside the manager mutex
for hot-path lock avoidance, and added a comment documenting that the
compressed buffer may occasionally be discarded if the active file must roll
over before the lock is acquired.

M3: Fixed. Expanded the public option docs for
`advanced_options.h::enable_blob_direct_write` to cover the reduced-scope v1
restrictions: single-writer write modes only, no WAL replay recovery for
active direct-write files, MemPurge/timestamp incompatibility,
`IngestWriteBatchWithIndex()` restriction, and read-only/secondary behavior.

M4: Fixed via documentation. The public option docs now explicitly note that
read-only and secondary opens only resolve manifest-visible flushed blob files,
not still-active direct-write files.

M5: Addressed with clarification. `RefreshBlobFileReader()` still uses the
existing lookup/erase/insert sequence, but a comment now explains that these
operations are serialized by the per-key mutex, which is why the same-file
refresh path is not racy despite the cache API lacking an atomic replace.

M6: Not changed in this patch. The per-batch heap allocations on the BDW
transform path are real, but this patch focused on correctness, option
sanitization clarity, and the always-off fast paths. Switching to
`SmallVector`/`InlineVector`-style containers is a reasonable follow-up.

S1: Fixed by the H4 DB-level O(1) BDW flag/count.

S2: Fixed. Added a comment next to `kDoFlushEachRecord` clarifying that it
flushes `WritableFileWriter` buffers for visibility, not durability; durability
still comes from `SyncAllOpenFiles()`/`AppendFooter()`.

S3: No code change. `WriteBatchInternal::PutEntitySerialized()` does not have a
missing `save.Rollback()` path because there is no fallible work after the
`LocalSavePoint` is created; the only exit is `save.commit()`.

S4: Deferred. I did not increase BDW stress frequency in this patch because it
would change suite mix while the recent BDW fixes are still stabilizing. That
is better handled as a follow-up once the current failures are soaked.

Verification:
- `make format-auto`
- `make -j"$(nproc)" db_blob_basic_test`
- `timeout 60s ./db_blob_basic_test --gtest_filter='DBBlobBasicTest.DirectWriteFailedBatchTrackedAsInitialGarbage:DBBlobBasicTest.DirectWriteRollbackMismatchReturnsCorruption:DBBlobBasicTest.DirectWriteCountTracksCreatedAndDroppedColumnFamily:DBBlobBasicTest.DirectWritePreparedGenerationsStayReusableUntilCommit:DBBlobBasicTest.OrderedTraceUsesLogicalBatchForBlobDirectWrite:DBBlobBasicTest.OrderedTraceUsesLogicalBatchForBlobDirectWritePipelinedWrite'`
