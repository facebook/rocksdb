# Review: file_io — Codex

## Summary
Overall quality rating: significant issues

The chapter split is sensible, the index is within the requested size range, and several sections do point readers at the right implementation layers. The direct I/O chapter is mostly grounded in the current code, and the `IOStatus` overview is close to the header/API reality.

The main problem is trustworthiness. Multiple chapters overstate invariants, blur internal helper defaults with user-facing defaults, or describe old async/rate-limiter behavior as if it were current. The cross-component call chains are also incomplete in places that matter for debugging today, especially async I/O, checksum handoff enablement, and directory fsync reasons.

## Correctness Issues

### [WRONG] Sticky writer errors do not bypass `Close()` cleanup
- **File:** `docs/components/file_io/11_error_handling.md`, section `Error Handling in Writers`
- **Claim:** "Step 2: All subsequent operations (`Append`, `Flush`, `Sync`, `Close`) check `seen_error_` first and return an error immediately if set."
- **Reality:** `WritableFileWriter::Close()` has a special path for `seen_error()`: it still calls the underlying `FSWritableFile::Close()` and resets the file handle before returning an error. If that close succeeds, it returns an `IOError` explaining that the file was closed after a prior write error; if the close itself fails, it returns that close error instead.
- **Source:** `file/writable_file_writer.cc`, `WritableFileWriter::Close`
- **Fix:** Document `Append`/`Flush`/`Sync` as the short-circuiting operations, and call out that `Close()` still performs cleanup before reporting failure.

### [WRONG] The index turns `seen_error_` behavior into a false invariant
- **File:** `docs/components/file_io/index.md`, section `Key Invariants`
- **Claim:** "Once `WritableFileWriter::seen_error_` is set, all subsequent operations return error until `reset_seen_error()` is called"
- **Reality:** That statement is not true for `Close()`, which still closes the underlying file, and it is not a correctness invariant in the style-guide sense because `reset_seen_error()` explicitly breaks it. This is a sticky-error policy with exceptions, not a corruption-preventing invariant.
- **Source:** `file/writable_file_writer.cc`, `WritableFileWriter::Close`; `file/writable_file_writer.h`, `reset_seen_error`
- **Fix:** Remove this bullet from `Key Invariants`, or rewrite it as a behavioral note with the `Close()` and `reset_seen_error()` exceptions spelled out.

### [WRONG] The documented custom `IOActivity` range is truncated
- **File:** `docs/components/file_io/08_io_classification.md`, section `IOActivity`
- **Claim:** "| `kCustomIOActivity80`-`kCustomIOActivityAE` | 0x80-0xAE | Reserved for custom/internal use |"
- **Reality:** The public enum currently defines custom values from `kCustomIOActivity80` through `kCustomIOActivityFE`, with `kUnknown = 0xFF`.
- **Source:** `include/rocksdb/env.h`, enum `Env::IOActivity`
- **Fix:** Update the range to `0x80-0xFE` and mention that `0xFF` is reserved for `kUnknown`.

### [MISLEADING] The readahead defaults table mixes raw helper defaults with effective table-reader defaults
- **File:** `docs/components/file_io/04_sequential_reader_and_prefetch.md`, section `Readahead Parameters`
- **Claim:** "| `num_file_reads_for_auto_readahead` | 0 | Sequential reads before auto-enabling readahead |"
- **Reality:** `ReadaheadParams` as a plain helper struct does default this field to `0`, but the normal block-based-table path does not use that raw default. `BlockPrefetcher::PrefetchIfNeeded()` copies values from `BlockBasedTableOptions`, whose public defaults are `initial_auto_readahead_size = 8KB`, `max_auto_readahead_size = 256KB`, and `num_file_reads_for_auto_readahead = 2`. As written, the table reads like RocksDB's effective defaults are all-zero.
- **Source:** `file/file_prefetch_buffer.h`, struct `ReadaheadParams`; `include/rocksdb/table.h`, struct `BlockBasedTableOptions`; `table/block_based/block_prefetcher.cc`, `BlockPrefetcher::PrefetchIfNeeded`
- **Fix:** Separate "raw `ReadaheadParams` defaults" from "defaults used by block-based table readers," and mention the option sanitization rules from `BlockBasedTableOptions`.

### [MISLEADING] The sync fallback on `ReadAsync(...)=NotSupported` is not a `RandomAccessFileReader` contract
- **File:** `docs/components/file_io/03_random_access_file_reader.md`, section `Async Read`
- **Claim:** "Step 2: If `ReadAsync()` returns `NotSupported` (e.g., io_uring failed to initialize), the caller falls back to synchronous `Read()`."
- **Reality:** `RandomAccessFileReader::ReadAsync()` itself does not implement a fallback. It prepares callback state and returns the underlying file status. The synchronous fallback is implemented by `FilePrefetchBuffer::ReadAsync()`, which explicitly catches `Status::NotSupported()` and then issues a blocking `Read()`. Other callers must handle `NotSupported` themselves.
- **Source:** `file/random_access_file_reader.cc`, `RandomAccessFileReader::ReadAsync`; `file/file_prefetch_buffer.cc`, `FilePrefetchBuffer::ReadAsync`; `file/prefetch_test.cc`, test `FilePrefetchBufferTest.ReadAsyncSyncFallbackOnNotSupported`
- **Fix:** Move the fallback description into the `FilePrefetchBuffer`/async-prefetch discussion and avoid implying it is guaranteed for all `RandomAccessFileReader::ReadAsync()` call sites.

### [STALE] The `Poll` section describes a `min_completions` guarantee that current Posix code does not implement
- **File:** `docs/components/file_io/06_async_io.md`, section `Poll`
- **Claim:** "`FileSystem::Poll(io_handles, min_completions)` checks for completion of async reads. The implementation should invoke callbacks for completed requests and return only after at least `min_completions` requests have completed."
- **Reality:** Current `PosixFileSystem::Poll()` ignores `min_completions` entirely. The parameter is intentionally unnamed in the function signature, and the implementation has a TODO saying the API still needs to be updated to honor it.
- **Source:** `env/fs_posix.cc`, `PosixFileSystem::Poll`; `include/rocksdb/file_system.h`, virtual `FileSystem::Poll`
- **Fix:** Document current Posix behavior as "waits for the listed handles to finish, without honoring `min_completions` yet," and mention the TODO instead of presenting the stronger behavior as current fact.

### [MISLEADING] FS-buffer reuse is described as generally available in synchronous mode, but explicit `Prefetch()` disables it
- **File:** `docs/components/file_io/04_sequential_reader_and_prefetch.md`, section `FS Buffer Reuse Optimization`
- **Claim:** "When the `FileSystem` supports `kFSBuffer`, `FilePrefetchBuffer` can avoid an extra copy by reusing the buffer allocated by the filesystem. This optimization requires all of: ... Synchronous mode (`num_buffers_ == 1`)."
- **Reality:** That is not true for all synchronous prefetch paths. The explicit `FilePrefetchBuffer::Prefetch()` path currently hard-codes `use_fs_buffer = false` with a TODO explaining that overlap-buffer handling is still broken there. Reuse is available only in the sync paths that go through `UseFSBuffer()` and `FSBufferDirectRead()`, not every `num_buffers_ == 1` case.
- **Source:** `file/file_prefetch_buffer.cc`, `FilePrefetchBuffer::Prefetch`; `file/file_prefetch_buffer.h`, `UseFSBuffer` and `FSBufferDirectRead`
- **Fix:** Narrow the claim to the synchronous read paths that actually consult `UseFSBuffer()`, and call out that the explicit `Prefetch()` path still disables the optimization.

### [WRONG] Rate-limiter fairness is reduced to a compaction-vs-flush "1/10 chance" rule
- **File:** `docs/components/file_io/07_rate_limiter.md`, section `Priority and Fairness`; `docs/components/file_io/12_best_practices.md`, section `Rate Limiter Configuration`
- **Claim:** "`IO_USER` is always served first. For the remaining priorities (HIGH, MID, LOW), the iteration order is randomized using `rnd_.OneIn(fairness_)`. With the default `fairness_ = 10`, lower-priority requests get a 1/10 chance of being served before higher-priority ones, preventing starvation." Also: "The default `fairness = 10` works well in practice; it gives lower-priority requests (compaction) a 1/10 chance of being served before flush, preventing starvation."
- **Reality:** The implementation is not a single compaction-before-flush lottery. It always serves `IO_USER` first, then makes two separate random decisions: whether `IO_HIGH` is iterated after both `IO_MID` and `IO_LOW`, and whether `IO_MID` is iterated after `IO_LOW`. The behavior includes `IO_MID` and is more complex than "1/10 chance before flush."
- **Source:** `util/rate_limiter.cc`, `GenericRateLimiter::GeneratePriorityIterationOrderLocked`
- **Fix:** Describe the actual randomized ordering among `IO_HIGH`, `IO_MID`, and `IO_LOW`, or simplify the prose to "fairness occasionally inverts the remaining background-priority order."

### [WRONG] The best-practices chapter warns about a current overflow bug that the rate limiter now guards against
- **File:** `docs/components/file_io/12_best_practices.md`, section `Common Pitfalls`
- **Claim:** "**Rate limiter with auto_tuned and extreme upper bound**: Setting `rate_bytes_per_sec` to `INT64_MAX` with `auto_tuned = true` causes the lower bound to overflow, producing unpredictable behavior."
- **Reality:** Current rate-limiter code has explicit overflow guards in both refill-byte computation and tuning math. Using `INT64_MAX` is still a poor practical configuration, but the specific "overflow causes unpredictable behavior" claim is not supported by the current implementation.
- **Source:** `util/rate_limiter.cc`, `GenericRateLimiter::CalculateRefillBytesPerPeriodLocked`; `GenericRateLimiter::TuneLocked`
- **Fix:** Remove the overflow claim. If you want to discourage extreme values, frame it as a poor tuning choice rather than a current arithmetic bug.

### [UNVERIFIABLE] The docs attach specific CPU-overhead numbers to coroutine MultiGet without code-backed evidence
- **File:** `docs/components/file_io/06_async_io.md`, section `Known Limitations`; `docs/components/file_io/12_best_practices.md`, section `Async I/O Configuration`
- **Claim:** "- CPU overhead of MultiGet coroutines: 6-15% increase (realistic multi-threaded case: ~6%)" and "`optimize_multiget_for_io = true` (default) enables multi-level parallelism in MultiGet but increases CPU usage by 6-15%."
- **Reality:** The current option comments and implementation only say the feature comes with "slightly higher CPU overhead." There is no benchmark, constant, or test in the tree that substantiates the numeric `6-15%` range.
- **Source:** `include/rocksdb/options.h`, field `ReadOptions::optimize_multiget_for_io`; `db/version_set.cc`, coroutine MultiGet path
- **Fix:** Either cite the benchmark/source for those numbers or remove the percentages and keep the qualitative warning.

### [MISLEADING] `advise_random_on_open` is documented as a direct SST-only `fadvise` call, but the real boundary is the `FSRandomAccessFile::Hint` API
- **File:** `docs/components/file_io/12_best_practices.md`, section `Read I/O Hints`
- **Claim:** "`advise_random_on_open = true` (default, see `DBOptions` in `include/rocksdb/options.h`) calls `fadvise(FADV_RANDOM)` when opening SST files."
- **Reality:** DB code calls `FSRandomAccessFile::Hint(kRandom)` after opening non-sequential table files, and blob-file open uses the same hint path. On Posix that hint maps to `POSIX_FADV_RANDOM`, but custom file systems can implement it differently or ignore it entirely.
- **Source:** `include/rocksdb/options.h`, field `advise_random_on_open`; `db/table_cache.cc`, table-file open path; `db/blob/blob_file_reader.cc`, `BlobFileReader::OpenFile`; `env/io_posix.cc`, `PosixRandomAccessFile::Hint`
- **Fix:** Document this as a file-hint request on table/blob readers, with Posix behavior called out as the current built-in implementation rather than the abstract contract.

## Completeness Gaps

### `checksum_handoff_file_types` is the real user-facing switch for data-verification handoff, but chapter 9 never names it
- **Why it matters:** chapter 9 currently reads like checksum handoff is controlled only by internal `WritableFileWriter` constructor booleans. Someone trying to enable or debug the feature needs the public option and the file-type mapping.
- **Where to look:** `include/rocksdb/options.h`, field `checksum_handoff_file_types`; `db/builder.cc`; `db/db_impl/db_impl_open.cc`; `db/version_set.cc`; `table/sst_file_writer.cc`; `db/blob/blob_file_builder.cc`
- **Suggested scope:** expand chapter 9's configuration section and add a short table showing which file types currently pass `perform_data_verification` and `buffered_data_with_checksum`

### `FilePrefetchBuffer` is documented as iterator machinery, but it is also used for table open, partitioned metadata, blob reads, and compaction/user-scan modes
- **Why it matters:** the current text makes it sound like `FilePrefetchBuffer` is mostly a `BlockBasedTableIterator` helper. That misses the table-open tail-prefetch path, partitioned index/filter prefetch, blob-file reuse, and the separate `FilePrefetchBufferUsage` modes with different stats.
- **Where to look:** `table/block_based/block_based_table_reader.cc`, `BlockBasedTable::PrefetchTail`; `table/block_based/partitioned_index_reader.cc`; `table/block_based/partitioned_filter_block.cc`; `db/blob/prefetch_buffer_collection.cc`; `db/blob/blob_source.cc`; `table/block_based/block_prefetcher.cc`
- **Suggested scope:** broaden chapter 4 with a usage-mode subsection instead of centering only the iterator path

### The async-I/O docs omit the 2024-2025 abort/poll fixes and the tests that define the current contracts
- **Why it matters:** `AbortIO()` and `Poll()` are where the current Posix async implementation is most subtle. The docs describe the happy path but not the real completion-accounting edge cases that were recently fixed.
- **Where to look:** `env/fs_posix.cc`, `PosixFileSystem::Poll` and `PosixFileSystem::AbortIO`; `env/env_test.cc`, tests `InterleavingIOUringOperations`, `AbortIOStress`, `AbortIOReversedHandles`, and `AbortIOPartialHandlesBug`; `file/prefetch_test.cc`, `ReadAsyncSyncFallbackOnNotSupported`; recent history entries `5f692d747`, `27d70ecd7`, `3695cb676`, `f84351de9`, and `56cb88ec7`
- **Suggested scope:** add a failure-handling subsection to chapter 6 that covers fallback, poll error propagation, and mixed aborted/non-aborted handle completion

### The directory-fsync chapter barely covers `kNewFileSynced`, but current code uses `DirFsyncOptions` for rename, delete, and directory-rename durability too
- **Why it matters:** the btrfs-specific behavior only makes sense once readers see the full set of `DirFsyncOptions` call sites. Right now the chapter frames directory fsync mostly as "after creating a new file."
- **Where to look:** `file/filename.cc`; `db/db_impl/db_impl.cc`; `db/db_impl/db_impl_compaction_flush.cc`; `file/delete_scheduler.cc`; `utilities/checkpoint/checkpoint_impl.cc`; `env/io_posix_test.cc`; recent history entry `1e1097d8d`
- **Suggested scope:** add a call-site table to chapter 10 keyed by `DirFsyncOptions::reason`

### The async-I/O docs miss the newer MultiScan path entirely
- **Why it matters:** chapter 6 says async I/O is used in two primary scenarios, but current RocksDB also uses async prefetch/IODispatcher in MultiScan through `MultiScanArgs::use_async_io`. Anyone debugging scan-prepare latency or async prefetch coverage will miss a substantial consumer of the file-I/O layer.
- **Where to look:** `include/rocksdb/options.h`, field `MultiScanArgs::use_async_io`; `table/block_based/block_based_table_iterator.cc`, `BlockBasedTableIterator::Prepare`; `db/version_set.cc`; `table/block_based/block_based_table_reader_sync_and_async.h`; `db/db_iterator_test.cc`, `DBMultiScanIteratorTest.AsyncPrefetch*`; `table/block_based/block_based_table_reader_test.cc`, `BlockBasedTableReaderMultiScanAsyncIOTest`
- **Suggested scope:** add at least a short subsection to chapter 6, or broaden the overview so it no longer implies iterators and MultiGet are the only async consumers

## Depth Issues

### The async-I/O chapter collapses three different layers into one happy-path narrative
- **Current:** chapter 6 discusses `ReadAsync`, `Poll`, iterator prefetch, and MultiGet together as if they were one unified mechanism.
- **Missing:** the docs need to distinguish the default `FSRandomAccessFile::ReadAsync` behavior, the `RandomAccessFileReader` wrapper responsibilities, the `FilePrefetchBuffer` fallback/error propagation logic, and the extra gating on coroutine MultiGet in `VersionSet`.
- **Source:** `include/rocksdb/file_system.h`, `FSRandomAccessFile::ReadAsync`; `file/random_access_file_reader.cc`; `file/file_prefetch_buffer.cc`; `db/version_set.cc`

### The readahead chapter needs the actual option-interaction rules, not just the steady-state growth story
- **Current:** chapter 4 explains exponential growth and adaptive decrease.
- **Missing:** it does not spell out that `max_auto_readahead_size = 0` disables internal auto-readahead, `initial_auto_readahead_size = 0` also disables it, values are sanitized when initial exceeds max, and `num_file_reads_for_auto_readahead` is exercised by tests across multiple values.
- **Source:** `include/rocksdb/table.h`, `BlockBasedTableOptions`; `table/block_based/block_prefetcher.cc`; `file/prefetch_test.cc`

### The directory-fsync chapter is too shallow about btrfs rename behavior and failure handling
- **Current:** the doc says btrfs uses renamed-file fsync instead of directory fsync.
- **Missing:** it does not explain that the implementation opens the renamed file, fsyncs it, closes it, and returns an error if the open or close step fails. That detail matters because there was a recent fix specifically for the failed-open path.
- **Source:** `env/io_posix.cc`, `PosixDirectory::FsyncWithDirOptions`; `env/io_posix_test.cc`

## Structure and Style Violations

### Inline code quotes are used throughout the file-io doc set
- **File:** all files under `docs/components/file_io/`
- **Details:** the docs rely heavily on inline backticks for types, methods, fields, options, and literals even though the component-doc style guide explicitly says not to use inline code quotes.

### `Key Invariants` contains a non-invariant and one that is factually wrong
- **File:** `docs/components/file_io/index.md`
- **Details:** the `seen_error_` bullet is not a true correctness invariant and is also inaccurate because `Close()` still performs cleanup. The invariant section should be limited to properties whose violation risks corruption or crash-safety failure.

### Several `Files:` lines omit the main implementation files discussed by the chapter
- **File:** `docs/components/file_io/06_async_io.md`; `docs/components/file_io/09_checksums_and_verification.md`; `docs/components/file_io/10_directory_fsync.md`
- **Details:** chapter 6 discusses `Poll()` and `AbortIO()` but omits `env/fs_posix.cc` and `file/random_access_file_reader.cc`; chapter 9 discusses public checksum configuration without listing `include/rocksdb/options.h`; chapter 10 describes broader `DirFsyncOptions` behavior while listing only the low-level directory implementation files.

## Undocumented Complexity

### Posix async abort has to account for completions belonging to handles that are not being aborted
- **What it is:** `AbortIO()` can consume CQEs for handles outside the abort set while it waits for cancellation and original-read completions. The `is_being_aborted` state on `Posix_IOHandle` is what prevents lost callbacks and hangs in mixed-handle scenarios.
- **Why it matters:** this is the core subtlety behind the recent AbortIO fixes. Anyone modifying async I/O or implementing a similar filesystem backend will need this mental model.
- **Key source:** `env/fs_posix.cc`, `PosixFileSystem::AbortIO`; `env/env_test.cc`, `AbortIOPartialHandlesBug`
- **Suggested placement:** chapter 6

### Tail prefetch is a separate mode with its own heuristic sizing and fallback path
- **What it is:** `BlockBasedTable::PrefetchTail()` first tries filesystem prefetch, then falls back to an internal `FilePrefetchBuffer` in `kTableOpenPrefetchTail` mode. Tail size can come from manifest metadata, `TailPrefetchStats`, or a 4KB/512KB heuristic depending on context.
- **Why it matters:** table-open latency, partitioned index/filter loading, and prefetch statistics are hard to reason about without knowing this mode exists.
- **Key source:** `table/block_based/block_based_table_reader.cc`, `BlockBasedTable::PrefetchTail`
- **Suggested placement:** chapter 4

### Checksum handoff behavior differs by file type, and MANIFEST/WAL paths use buffered checksum accumulation
- **What it is:** the booleans passed to `WritableFileWriter` are not uniform. SST/blob writers enable handoff without buffered accumulation, while MANIFEST and WAL writers can enable both `perform_data_verification` and `buffered_data_with_checksum`.
- **Why it matters:** this affects what a custom `FSWritableFile` will actually receive in `DataVerificationInfo`, and it is the missing bridge between the public option and the low-level checksum-handoff mechanism.
- **Key source:** `db/db_impl/db_impl_open.cc`; `db/version_set.cc`; `db/builder.cc`; `db/blob/blob_file_builder.cc`; `table/sst_file_writer.cc`; `file/writable_file_writer.cc`
- **Suggested placement:** chapter 9

### MultiScan has its own async-prefetch path and state machine on top of block iterators
- **What it is:** MultiScan does not just "use iterator async I/O." It wires `MultiScanArgs::use_async_io` into `BlockBasedTableIterator::Prepare`, builds a `ReadSet`, and uses a separate index-iterator mode to prefetch and release blocks across multiple scan ranges.
- **Why it matters:** without this, the async-I/O chapter gives the wrong impression that range scans and MultiGet are the only higher-level consumers, and it leaves the MultiScan stats/caching behavior unexplained.
- **Key source:** `table/block_based/block_based_table_iterator.cc`, `BlockBasedTableIterator::Prepare`; `db/version_set.cc`; `table/block_based/multi_scan_index_iterator.cc`
- **Suggested placement:** chapter 6

## Positive Notes

- The index file is within the requested 40-80 line range and has a usable overview/chapter map structure.
- `docs/components/file_io/05_direct_io.md` is mostly aligned with the current alignment and partial-tail rewrite behavior in `WritableFileWriter` and `RandomAccessFileReader`.
- The `IOStatus` overview in chapter 11 correctly captures the extra retryable/data-loss/scope state and the checked-status behavior.
- Chapter 4 does at least cover adaptive readahead decrease, discarded-prefetch accounting, and async-prefetch overlap, all of which are exercised by current tests in `file/prefetch_test.cc`.
