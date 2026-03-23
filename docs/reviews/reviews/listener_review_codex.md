# Review: listener — Codex

## Summary
Overall quality rating: needs work

The doc set covers most of the public listener surface and it gets some important themes right, especially the `OnFlushBegin()` snapshot rationale and the deadlock/latency warnings around callbacks. The chapter split is also sensible.

The main problem is that several chapters collapse multiple implementation branches into one simplified story. That simplification breaks in exactly the places people rely on these docs for debugging: compaction type differences, file-I/O error delivery, shutdown behavior, and error-recovery sequencing. A few newer or less obvious behaviors are also missing entirely, including MANIFEST-verification `OnIOError()` callbacks and the full `SubcompactionJobInfo` surface.

## Correctness Issues

### [WRONG] `OnManualFlushScheduled()` is documented as covering `FlushWAL()`
- **File:** `docs/components/listener/02_flush_compaction_events.md`, `OnManualFlushScheduled`
- **Claim:** "Called after a manual flush is scheduled (via `DB::Flush()` or `DB::FlushWAL()`)."
- **Reality:** The callback is only dispatched from the memtable-flush paths. `DBImpl::NotifyOnManualFlushScheduled()` is called from `DBImpl::FlushMemTable()` and `DBImpl::AtomicFlushMemTables()`. `FlushWAL()` does not invoke it.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::NotifyOnManualFlushScheduled`, `DBImpl::FlushMemTable`, `DBImpl::AtomicFlushMemTables`; `db/db_impl/db_impl.cc`, `DBImpl::FlushWAL`
- **Fix:** Say the callback is for manual memtable flush scheduling only, not WAL flush.

### [WRONG] `OnCompactionBegin()` is said to skip trivial-move-style compactions
- **File:** `docs/components/listener/02_flush_compaction_events.md`, `OnCompactionBegin`
- **Claim:** "Step 2: Check if the compaction has inputs (skip if no inputs, e.g., trivial move edge cases)"
- **Reality:** `DBImpl::NotifyOnCompactionBegin()` does not check for "has inputs" at all. The callback is invoked for FIFO deletion compactions, FIFO trivial-copy compactions, trivial moves, and normal compactions. Tests explicitly assert `OnCompactionBegin()` fires for trivial moves.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::NotifyOnCompactionBegin`, `DBImpl::BackgroundCompaction`; `db/db_compaction_test.cc`, `DBCompactionTestWithParam::TrivialMoveEventListener`
- **Fix:** Replace the invented "no inputs" step with the real guards: empty listener list, `shutting_down_`, and paused manual compaction.

### [WRONG] The docs say every compaction generates subcompaction callbacks
- **File:** `docs/components/listener/02_flush_compaction_events.md`, `OnSubcompactionBegin / OnSubcompactionCompleted`
- **Claim:** "Every compaction triggers at least one subcompaction event pair, since compactions are always internally processed as subcompactions."
- **Reality:** Only compactions that run through `CompactionJob::Run()` emit subcompaction callbacks. Trivial moves, FIFO deletion compactions, and FIFO trivial-copy compactions do not.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::BackgroundCompaction`; `db/compaction/compaction_job.cc`, `CompactionJob::NotifyOnSubcompactionBegin`, `CompactionJob::NotifyOnSubcompactionCompleted`
- **Fix:** Scope the claim to non-trivial compactions handled by `CompactionJob`.

### [MISLEADING] `CompactionJobInfo::num_l0_files` is described as if one callback sees both before and after values
- **File:** `docs/components/listener/02_flush_compaction_events.md`, `CompactionJobInfo` table
- **Claim:** "`num_l0_files` | L0 file count right before and after compaction"
- **Reality:** There is only one `int num_l0_files`. In `OnCompactionBegin()` it is populated from the input version before the compaction runs; in `OnCompactionCompleted()` it is populated from the current version after completion.
- **Source:** `include/rocksdb/listener.h`, `CompactionJobInfo`; `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::NotifyOnCompactionBegin`, `DBImpl::NotifyOnCompactionCompleted`; `db/db_compaction_test.cc`, `DBCompactionTest::SubcompactionEvent`
- **Fix:** Document the field as callback-dependent: pre-compaction in begin, post-compaction in completed.

### [STALE] The flush-reason table omits current enum values used by recovery paths
- **File:** `docs/components/listener/02_flush_compaction_events.md`, `FlushReason Enum`
- **Claim:** "The `FlushReason` enum ... identifies why a flush was triggered:" followed by a table that omits newer values.
- **Reality:** The current enum also includes `kOthers`, `kErrorRecoveryRetryFlush`, and `kCatchUpAfterErrorRecovery`, and those values are used in current error-recovery and catch-up code paths.
- **Source:** `include/rocksdb/listener.h`, `FlushReason`; `db/flush_job.cc`, `GetFlushReasonString`; `db/error_handler.cc`, `ErrorHandler::SetBGError`, `ErrorHandler::RecoverFromBGError`; `db/db_impl/db_impl.cc`, recovery catch-up path
- **Fix:** Add the missing enum values and explain that `kErrorRecoveryRetryFlush` / `kCatchUpAfterErrorRecovery` are internal recovery-related flushes.

### [WRONG] The SST file-creation call path is misattributed to `BuildTable()` for compactions
- **File:** `docs/components/listener/03_file_lifecycle_events.md`, `OnTableFileCreationStarted`
- **Claim:** "Invoked from `BuildTable()` in `db/builder.cc` during flush and compaction."
- **Reality:** Flush and recovery use `BuildTable()`, but compaction has its own path in `CompactionJob::OpenCompactionOutputFile()` / finish handling. `BuildTable()` is also used for recovery and misc table creation, not just flush.
- **Source:** `db/builder.cc`, `BuildTable`; `db/compaction/compaction_job.cc`, `CompactionJob::OpenCompactionOutputFile`; `db/db_impl/db_impl_open.cc`, recovery `BuildTable` call
- **Fix:** Split the call paths: flush/recovery/misc use `BuildTable()`, compaction uses `CompactionJob`.

### [WRONG] The documented flush ordering of blob and SST creation events is reversed
- **File:** `docs/components/listener/03_file_lifecycle_events.md`, `Event Ordering`
- **Claim:** "Step 2: `OnBlobFileCreationStarted` ... Step 3: `OnTableFileCreationStarted`"
- **Reality:** `OnTableFileCreationStarted()` fires first in `BuildTable()`. Blob-file creation starts later, only if/when the flush actually opens a blob file while building table contents. There can also be multiple blob-file start/create pairs before the single `OnTableFileCreated()`.
- **Source:** `db/builder.cc`, `BuildTable`; `db/blob/blob_file_builder.cc`, `BlobFileBuilder::OpenBlobFileIfNeeded`, `BlobFileBuilder::CloseBlobFile`
- **Fix:** Document the actual order as `OnFlushBegin` -> `OnTableFileCreationStarted` -> zero or more blob-file start/create pairs -> `OnTableFileCreated` -> `OnFlushCompleted`.

### [WRONG] The compaction ordering section ignores compaction types that skip file-creation or subcompaction callbacks
- **File:** `docs/components/listener/03_file_lifecycle_events.md`, `Event Ordering`
- **Claim:** "For a compaction: Step 2: For each subcompaction: `OnSubcompactionBegin` -> file creation events -> `OnSubcompactionCompleted`"
- **Reality:** That is only true for non-trivial compactions. Trivial moves and FIFO deletion compactions have no subcompaction callbacks. FIFO trivial-copy compactions create new SSTs but intentionally skip the table-file creation listener callbacks.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::BackgroundCompaction` trivial-move, deletion-compaction, and trivial-copy branches; comments around skipped `EventHelpers::NotifyTableFileCreationStarted` / `LogAndNotifyTableFileCreationFinished`
- **Fix:** State that ordering depends on compaction type, and call out the trivial-copy exception explicitly.

### [WRONG] The file-I/O chapter says `SequentialFileReader` follows the same `OnIOError()` pattern as the other wrappers
- **File:** `docs/components/listener/04_file_io_events.md`, `Dispatch Flow (Read Path Example)`
- **Claim:** "The same pattern applies to write, sync, flush, truncate, and close operations in `WritableFileWriter` and `SequentialFileReader`."
- **Reality:** `SequentialFileReader` only emits `OnFileReadFinish()`. It does not call `OnIOError()` on read failures.
- **Source:** `file/sequence_file_reader.cc`, `SequentialFileReader::Read`; compare with `file/random_access_file_reader.cc`, `RandomAccessFileReader::Read`
- **Fix:** Separate sequential-read behavior from random-read / writable-file behavior and note that `SequentialFileReader` currently lacks `OnIOError()` notifications.

### [WRONG] The docs say `OnIOError()` always requires `ShouldBeNotifiedOnFileIO()`
- **File:** `docs/components/listener/04_file_io_events.md`, `OnIOError`
- **Claim:** "Called whenever an I/O operation fails. This callback also requires `ShouldBeNotifiedOnFileIO()` to return `true`."
- **Reality:** `VersionSet::Close()` directly invokes `OnIOError()` for MANIFEST size/content verification failures, without filtering listeners through `ShouldBeNotifiedOnFileIO()`. There is a dedicated test covering this.
- **Source:** `db/version_set.cc`, `VersionSet::Close`; `db/version_set_test.cc`, MANIFEST content validation tests
- **Fix:** Document the two delivery modes separately: file-wrapper callbacks are gated by `ShouldBeNotifiedOnFileIO()`, but MANIFEST verification on close is not.

### [MISLEADING] The docs overstate when finish callbacks carry the failing status
- **File:** `docs/components/listener/04_file_io_events.md`, `OnIOError`
- **Claim:** "The finish callback receives the error status as well."
- **Reality:** That is true for the normal read/write/flush/sync paths, but not reliably for truncate/fsync/close inside `WritableFileWriter::Close()`. Those finish callbacks are notified with the pre-existing aggregate status `s`, while the underlying operation's failure is reported via `OnIOError(interim, ...)`.
- **Source:** `file/writable_file_writer.cc`, `WritableFileWriter::Close`
- **Fix:** Narrow the statement or explicitly call out the current `Close()` behavior so users do not treat all finish-callback statuses as authoritative.

### [WRONG] `OnBackgroundError()` is documented as only covering transitions to read-only mode
- **File:** `docs/components/listener/05_error_recovery_events.md`, `OnBackgroundError`
- **Claim:** "Called when a background operation ... encounters an error that would put the DB into read-only mode."
- **Reality:** The callback also fires for retryable compaction I/O errors that do not set `bg_error_` and do not move the DB into read-only mode; RocksDB just reports the error and lets compaction reschedule itself.
- **Source:** `db/error_handler.cc`, `ErrorHandler::SetBGError` retryable-compaction branch
- **Fix:** Describe `OnBackgroundError()` as the generic listener hook for background-error handling, not only read-only transitions.

### [WRONG] The background-error flow is documented with the wrong listener ordering
- **File:** `docs/components/listener/05_error_recovery_events.md`, `OnBackgroundError`
- **Claim:** "Step 3: For each listener, call `OnBackgroundError(reason, bg_error)` Step 4: If `*auto_recovery` is still `true`, call `OnErrorRecoveryBegin(reason, *bg_error, auto_recovery)`"
- **Reality:** The implementation interleaves the two callbacks per listener: for each listener, RocksDB calls `OnBackgroundError()`, then immediately calls `OnErrorRecoveryBegin()` for that same listener if `*auto_recovery` is still true. An earlier listener can therefore suppress auto-recovery before later listeners ever see `OnErrorRecoveryBegin()`.
- **Source:** `db/event_helpers.cc`, `EventHelpers::NotifyOnBackgroundError`
- **Fix:** Rewrite the flow to show the per-listener interleaving and shared mutable state.

### [WRONG] `OnErrorRecoveryEnd()` is documented as automatic-only
- **File:** `docs/components/listener/05_error_recovery_events.md`, `OnErrorRecoveryEnd`
- **Claim:** "Called when the automatic recovery attempt completes, regardless of whether recovery succeeded."
- **Reality:** The callback is also used for manual `DB::Resume()` / `ClearBGError()` paths, for shutdown interruption, and for auto-resume retry exhaustion. The final `new_bg_error` can therefore be `Status::OK()`, a background error, `Status::ShutdownInProgress()`, or `Status::Aborted(...)`.
- **Source:** `db/error_handler.cc`, `ErrorHandler::ClearBGError`, `ErrorHandler::RecoverFromBGError`, `ErrorHandler::RecoverFromRetryableBGIOError`; `db/error_handler_fs_test.cc`, shutdown race test
- **Fix:** Broaden the description to "recovery attempt / recovery shutdown path end" and mention non-OK terminal statuses beyond persistent bg errors.

### [WRONG] `write_stall_proximity_pct` is documented as depending on memtable count
- **File:** `docs/components/listener/06_write_stall_pressure.md`, `BackgroundJobPressure Struct`
- **Claim:** "The `write_stall_proximity_pct` is computed as the maximum across all column families based on write-stall triggers (L0 file count, pending compaction bytes, memtable count)."
- **Reality:** The current implementation only considers L0 delay-trigger proximity and pending-compaction-bytes proximity. It does not include memtable count in this metric.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::CaptureBackgroundJobPressure`
- **Fix:** Remove "memtable count" from this definition and document the slowdown-trigger / stop-trigger and soft-limit / hard-limit fallback logic instead.

### [WRONG] `OnMemTableSealed()` is documented as only a "memtable full" event
- **File:** `docs/components/listener/07_other_events.md`, `OnMemTableSealed`
- **Claim:** "This happens when the active memtable is full and a new one is created to accept writes."
- **Reality:** The callback is tied to `DBImpl::SwitchMemtable()`, which is used by manual flush and recovery-related paths as well, not only "memtable full" rollover.
- **Source:** `db/db_impl/db_impl_write.cc`, `DBImpl::SwitchMemtable`, `DBImpl::NotifyOnMemTableSealed`; `db/db_impl/db_impl_compaction_flush.cc`, manual/recovery flush paths calling `SwitchMemtable`
- **Fix:** Describe it as a memtable-switch callback, with "write-buffer full" as just one common trigger.

### [MISLEADING] The `newest_udt` population rule is incomplete
- **File:** `docs/components/listener/07_other_events.md`, `MemTableInfo` table
- **Claim:** "`newest_udt` | Newest user-defined timestamp (only populated when `persist_user_defined_timestamps` is false)"
- **Reality:** RocksDB also requires the comparator to have a non-zero timestamp size. If timestamps are not enabled for the CF, the field remains empty even when `persist_user_defined_timestamps` is false.
- **Source:** `db/db_impl/db_impl_write.cc`, `DBImpl::SwitchMemtable`
- **Fix:** Add the timestamp-size precondition.

### [MISLEADING] Shutdown skipping is described as a universal listener property
- **File:** `docs/components/listener/01_registration.md`, `Listener Lifecycle`; `docs/components/listener/index.md`, `Key Characteristics`
- **Claim:** "Shutdown | Callbacks skipped when `shutting_down_` flag is set" and "Shutdown guard: Callbacks are skipped if `shutting_down_` is set"
- **Reality:** Only some notification sites check `shutting_down_`. File I/O callbacks, table/blob lifecycle callbacks, `OnExternalFileIngested()`, `OnColumnFamilyHandleDeletionStarted()`, `OnBackgroundError()`, `OnErrorRecoveryEnd()`, and `OnBackgroundJobPressureChanged()` do not all use that guard.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc`, flush/compaction guards; `db/compaction/compaction_job.cc`, subcompaction guards; `db/event_helpers.cc`; `db/db_impl/db_impl.cc`, `DBImpl::NotifyOnExternalFileIngested`; `db/column_family.cc`, `ColumnFamilyHandleImpl::~ColumnFamilyHandleImpl`; `file/random_access_file_reader.cc`; `file/sequence_file_reader.cc`; `file/writable_file_writer.cc`
- **Fix:** Narrow the shutdown discussion to the callbacks that actually check `shutting_down_`, and explicitly call out that several listener families do not.

### [WRONG] The threading table says `OnBackgroundError()` is a background-thread-only callback
- **File:** `docs/components/listener/08_threading_safety.md`, `Threading Model`
- **Claim:** "`OnBackgroundError` | Background thread where the error occurred"
- **Reality:** The public header and implementation allow it to run on flush threads, compaction threads, and user write threads. Write-path memtable and write-callback failures call `ErrorHandler::SetBGError()` directly from the write path.
- **Source:** `include/rocksdb/listener.h`, `EventListener::OnBackgroundError` comment; `db/db_impl/db_impl_write.cc`, `ErrorHandler::SetBGError` call sites for `kMemTable` and `kWriteCallback`
- **Fix:** Change the threading description to "flush/compaction/user-write thread where the error is surfaced."

## Completeness Gaps

### `OnIOError()` from MANIFEST verification on close
- **Why it matters:** This is the easiest way for an application to observe MANIFEST corruption or size mismatch at `DB::Close()`, and it bypasses the normal file-I/O listener gate.
- **Where to look:** `include/rocksdb/options.h` (`verify_manifest_content_on_close`), `db/version_set.cc` (`VersionSet::Close`), `db/version_set_test.cc`
- **Suggested scope:** Add a subsection to `04_file_io_events.md`

### Full `SubcompactionJobInfo` surface
- **Why it matters:** The docs currently imply only `subcompaction_job_id` and `stats` exist, but the struct also carries CF identity, status, job/thread IDs, level information, reason, and compression settings.
- **Where to look:** `include/rocksdb/listener.h`, `db/compaction/subcompaction_state.h`
- **Suggested scope:** Expand the `OnSubcompactionBegin / OnSubcompactionCompleted` section in `02_flush_compaction_events.md`

### Listener behavior for failed or empty SST outputs
- **Why it matters:** `OnTableFileCreated()` can report `Status::Aborted("Empty SST file not kept")`, and the path can be rewritten to `"(nil)"`. Users debugging file callbacks need this edge case spelled out.
- **Where to look:** `db/builder.cc`, `db/listener_test.cc` (`TableFileCreationListenersTest`)
- **Suggested scope:** Add a failure/edge-case note to `03_file_lifecycle_events.md`

### Trivial-copy compactions skipping table-file lifecycle callbacks
- **Why it matters:** FIFO `kChangeTemperature` trivial-copy compactions create new files but intentionally skip `OnTableFileCreationStarted()` / `OnTableFileCreated()`. Anyone correlating compaction callbacks with file callbacks will otherwise get a false gap.
- **Where to look:** `db/db_impl/db_impl_compaction_flush.cc`, trivial-copy branch comments
- **Suggested scope:** Add a prominent exception note in `03_file_lifecycle_events.md` and `02_flush_compaction_events.md`

### Per-file `OnExternalFileIngested()` behavior
- **Why it matters:** The callback fires once per ingested file, not once per API call. Batch ingest users need that to size their listener state correctly.
- **Where to look:** `db/db_impl/db_impl.cc`, `DBImpl::NotifyOnExternalFileIngested`; `db/external_sst_file_test.cc`
- **Suggested scope:** Add a note to `07_other_events.md`

### Error-recovery option interactions
- **Why it matters:** The docs explain the callbacks but not the options and conditions that determine whether recovery is attempted at all: single-path DBs, `sst_file_manager`, `allow_2pc`, `manual_wal_flush`, and `max_bgerror_resume_count`.
- **Where to look:** `db/error_handler.cc`, `db/error_handler.h`, `db/db_impl/db_impl_open.cc`, `include/rocksdb/options.h`
- **Suggested scope:** Add a compact "recovery decision matrix" subsection to `05_error_recovery_events.md`

### Blob-reference metadata in flush/compaction info structs
- **Why it matters:** `FlushJobInfo::oldest_blob_file_number` and `CompactionFileInfo::oldest_blob_file_number` are important when correlating SST events with integrated BlobDB behavior.
- **Where to look:** `include/rocksdb/listener.h`, `db/flush_job.cc`, `db/db_impl/db_impl_compaction_flush.cc`
- **Suggested scope:** Extend the field tables in `02_flush_compaction_events.md`

## Depth Issues

### `FlushJobInfo` semantics are not separated by callback
- **Current:** The docs list one `FlushJobInfo` field table without distinguishing `OnFlushBegin()` from `OnFlushCompleted()`.
- **Missing:** Which fields are meaningful at begin time versus completed time, and that `OnFlushCompleted()` fires only after the flush result is committed, sometimes by another flush thread.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::NotifyOnFlushBegin`, `DBImpl::NotifyOnFlushCompleted`; `db/flush_job.h`; `db/db_flush_test.cc`, `FireOnFlushCompletedAfterCommittedResult`

### The file-I/O chapter conflates wrapper-generated events with cross-component verification events
- **Current:** The chapter presents one unified story for read/write/sync callbacks and `OnIOError()`.
- **Missing:** A split between wrapper-generated finish/error callbacks (`RandomAccessFileReader`, `SequentialFileReader`, `WritableFileWriter`) and `VersionSet::Close()` verification callbacks using `FileOperationType::kVerify`.
- **Source:** `file/random_access_file_reader.cc`, `file/sequence_file_reader.cc`, `file/writable_file_writer.cc`, `db/version_set.cc`

### The recovery chapter hides the listener-order dependency
- **Current:** The flow is written as if all listeners see the same `bg_error` and `auto_recovery` state.
- **Missing:** The fact that listeners are called sequentially with shared mutable state, so earlier listeners can rewrite `bg_error` or disable `auto_recovery` before later listeners run.
- **Source:** `db/event_helpers.cc`, `EventHelpers::NotifyOnBackgroundError`

## Structure and Style Violations

### Index file is shorter than the required range
- **File:** `docs/components/listener/index.md`
- **Details:** The file is 37 lines, below the requested 40-80 line range.

### Inline code quoting rule is violated throughout the chapter set
- **File:** `docs/components/listener/index.md` and all chapter files
- **Details:** The docs use inline code formatting extensively for function names, fields, options, and prose despite the stated "NO inline code quotes" constraint for this doc set.

## Undocumented Complexity

### `OnFlushCompleted()` can be emitted by a different flush job than the one that wrote the SST
- **What it is:** Flush result installation is delegatable. One flush job can commit another concurrent flush job's results and then emit `OnFlushCompleted()` for both.
- **Why it matters:** The callback thread is not a safe proxy for the flush job identity; `FlushJobInfo::thread_id` and "current callback thread" can diverge.
- **Key source:** `db/flush_job.h` (`committed_flush_jobs_info_` comment), `db/db_impl/db_impl_compaction_flush.cc`, `db/db_flush_test.cc` (`FireOnFlushCompletedAfterCommittedResult`)
- **Suggested placement:** `02_flush_compaction_events.md`

### Slow background-pressure listeners delay DB shutdown
- **What it is:** `DBImpl` tracks `bg_pressure_callback_in_progress_` and waits for it during shutdown/close.
- **Why it matters:** `OnBackgroundJobPressureChanged()` is not just "background callback overhead"; a slow implementation can lengthen `Close()`.
- **Key source:** `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::NotifyOnBackgroundJobPressureChanged`; `db/db_impl/db_impl.cc`, `WaitForBackgroundWork`, `CloseHelper`
- **Suggested placement:** `06_write_stall_pressure.md` and `08_threading_safety.md`

### Failed/empty SST cleanup is not mirrored by `OnTableFileDeleted()`
- **What it is:** `BuildTable()` can delete a failed or empty output file immediately without going through the obsolete-file deletion helper, so users may see `OnTableFileCreated()` failure without a matching delete callback.
- **Why it matters:** Consumers trying to reconcile file-creation and file-deletion callbacks will otherwise assume they lost an event.
- **Key source:** `db/builder.cc`, cleanup path after failed/empty output; compare with `db/db_impl/db_impl_files.cc`, `DBImpl::DeleteObsoleteFileImpl`
- **Suggested placement:** `03_file_lifecycle_events.md`

### FIFO trivial-copy compactions are listener-asymmetric by design
- **What it is:** These compactions still emit compaction begin/completed callbacks but intentionally skip table-file creation callbacks.
- **Why it matters:** It is a surprising cross-component exception that can look like a bug in user instrumentation.
- **Key source:** `db/db_impl/db_impl_compaction_flush.cc`, trivial-copy compaction comments
- **Suggested placement:** `02_flush_compaction_events.md` and `03_file_lifecycle_events.md`

## Positive Notes

- The `OnFlushBegin()` section correctly captures the snapshot-safety reason it runs after memtable picking, and that explanation matches the implementation comments.
- The deadlock guidance around compaction callbacks in `08_threading_safety.md` is directionally correct and consistent with the public `EventListener` contract.
- The docs already cover several less-common callbacks that are easy to miss, including `OnManualFlushScheduled()`, blob-file lifecycle events, and `OnBackgroundJobPressureChanged()`.
