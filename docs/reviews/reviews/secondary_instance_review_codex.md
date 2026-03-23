# Review: secondary_instance - Codex

## Summary
(Overall quality rating: needs work)

The chapter breakdown is sensible, and the docs do cover several recent and non-obvious behaviors correctly, especially the `recover_wal=false` path for remote compaction, the WAL checksumming override in secondary replay, and the resumable remote-compaction constraints around `paranoid_file_checks` / `verify_output_flags`.

The main problem is precision. Several sections overstate guarantees that the code does not provide, and a few claims are simply wrong when checked against the current implementation. The biggest issues are constructor/open semantics, dropped-column-family handling, simplified snapshot/consistency claims, and cross-component descriptions of follower and remote-compaction behavior.

## Correctness Issues

### [WRONG] Opening workflow invents a "secondary mode" flag in the `DBImpl` constructor
- **File:** `01_opening_and_recovery.md`, Opening Workflow
- **Claim:** "Step 1: **Create DBImplSecondary**. The constructor calls `DBImpl(db_options, dbname, false, true, true)` -- the second `true` marks this as a read-only instance, and the third `true` marks it as a secondary instance."
- **Reality:** `DBImpl` only takes five constructor arguments: `options`, `dbname`, `seq_per_batch`, `batch_per_txn`, and `read_only`. There is no secondary-mode boolean in the base constructor. Secondary-specific behavior comes from the derived `DBImplSecondary` type plus its overrides and `ReactiveVersionSet` usage.
- **Source:** `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::DBImplSecondary`; `db/db_impl/db_impl.cc` `DBImpl::DBImpl`
- **Fix:** Explain that the final `true` only marks the base `DBImpl` as read-only. Secondary behavior is established by the derived class, `ReactiveVersionSet`, `OwnTablesAndLogs()`, and the overridden read/write APIs.

### [WRONG] The docs say both `OpenAsSecondary` overloads support arbitrary CF subsets
- **File:** `05_column_family_handling.md`, Opening with a Subset of Column Families
- **Claim:** "Both `DB::OpenAsSecondary()` overloads support opening a **subset** of the primary's column families."
- **Reality:** Only the multi-CF overload accepts a caller-specified CF subset. The single-CF overload always constructs a one-element vector for the default CF and delegates.
- **Source:** `include/rocksdb/db.h` `DB::OpenAsSecondary` overloads; `db/db_impl/db_impl_secondary.cc` `DB::OpenAsSecondary(const Options&, ...)`
- **Fix:** Say the single-CF overload opens only the default CF. Use the multi-CF overload to open a subset.

### [WRONG] Dropped CFs are not skipped during `TryCatchUpWithPrimary()`
- **File:** `05_column_family_handling.md`, CF Drop Handling in TryCatchUpWithPrimary
- **Claim:** "Dropped CFs are skipped during SuperVersion installation -- there is no point in creating a new SuperVersion for a CF that has been dropped."
- **Reality:** `TryCatchUpWithPrimary()` installs a new `SuperVersion` for every `ColumnFamilyData*` in `cfds_changed`, including dropped CFs. This matches the documented behavior that a retained handle can continue reading dropped-CF data.
- **Source:** `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::TryCatchUpWithPrimary`; `db/db_secondary_test.cc` `DBSecondaryTest.PrimaryDropColumnFamily`
- **Fix:** State that dropped CFs are logged earlier in the function, but the later install loop still runs for them so existing handles remain usable.

### [WRONG] `SstFileManager` truncation conditions are misstated
- **File:** `04_file_deletion.md`, SstFileManager Constraints
- **Claim:** "If the primary's `SstFileManager` is configured with `rate_bytes_per_sec > 0` or `bytes_max_delete_chunk > 0` (via `NewSstFileManager()`), SST files are deleted in chunks (truncated then deleted)."
- **Reality:** Chunked truncation only happens in the trash-delete path when background deletion is active and `bytes_max_delete_chunk != 0` and the file is larger than the chunk. `bytes_max_delete_chunk > 0` by itself does not force chunked deletion.
- **Source:** `file/delete_scheduler.cc` `DeleteScheduler::DeleteFile`; `file/delete_scheduler.cc` `DeleteScheduler::DeleteTrashFile`; `include/rocksdb/db.h` `DB::OpenAsSecondary` API comment
- **Fix:** Rewrite this as a conjunction, not a disjunction, and keep the warning scoped to the background-delete/truncation path.

### [WRONG] The remote-compaction chapter says priority is serialized in `CompactionServiceInput`
- **File:** `06_remote_compaction.md`, Integration with CompactionService
- **Claim:** "Compaction priority information is included in the input to allow remote scheduling prioritization."
- **Reality:** `CompactionServiceInput` has no priority field. Priority is carried in `CompactionServiceJobInfo` for the primary-side `CompactionService` scheduling callbacks, not in the serialized `DB::OpenAndCompact()` input blob.
- **Source:** `db/compaction/compaction_job.h` `CompactionServiceInput`; `include/rocksdb/options.h` `CompactionServiceJobInfo`
- **Fix:** Move the priority discussion to the primary-side `CompactionService` API and remove it from the serialized-input description.

### [MISLEADING] The comparison table flattens snapshot support into an incorrect blanket "Not supported"
- **File:** `07_comparison.md`, Three Read-Only Instance Modes
- **Claim:** `Explicit snapshots | Not supported | Supported | Not supported (inherits from Secondary)`
- **Reality:** Secondary/follower iterators reject `ReadOptions.snapshot`, and `Get()` ignores it, but inherited `MultiGet`/`MultiGetEntity` still use `DBImpl::MultiCFSnapshot()` and do honor explicit snapshots.
- **Source:** `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::NewIterator`; `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::GetImpl`; `db/db_impl/db_impl.cc` `DBImpl::MultiGetCommon`; `db/db_impl/db_impl.cc` `DBImpl::MultiCFSnapshot`
- **Fix:** Make snapshot support API-specific. The current row is too broad to be correct.

### [MISLEADING] Cross-CF consistency is described as unconditional even though the secondary read path does not build a shared CF snapshot
- **File:** `03_read_operations.md`, Snapshot Semantics
- **Claim:** `Cross-CF consistency | Yes (all CFs updated atomically in TryCatchUpWithPrimary())`
- **Reality:** `DBImplSecondary::NewIterators()` collects per-CF `SuperVersion`s directly and then calls `NewIteratorImpl()` per iterator. It does not go through `DBImpl::MultiCFSnapshot()` or any equivalent retry/locking path to guarantee a shared consistent sequence across CFs during concurrent catch-up.
- **Source:** `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::NewIterators`; `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::NewIteratorImpl`; `db/db_impl/db_impl.cc` `DBImpl::MultiCFSnapshot`
- **Fix:** Scope the guarantee to quiescent periods without concurrent catch-up, or document that `NewIterators()` does not currently construct a DBImpl-style multi-CF consistent snapshot.

### [MISLEADING] The docs present read visibility as "at TryCatchUpWithPrimary time" without the concurrent catch-up caveat
- **File:** `03_read_operations.md`, Get Implementation; `03_read_operations.md`, Snapshot Semantics
- **Claim:** "The snapshot is always `versions_->LastSequence()`" and `Implicit snapshot isolation | Yes, at TryCatchUpWithPrimary time`
- **Reality:** `DBImplSecondary::GetImpl()` reads `versions_->LastSequence()` before acquiring a `SuperVersion`, unlike base `DBImpl::GetImpl()`, which first refs the `SuperVersion` and then derives a published snapshot sequence. Reads and catch-up are not serialized with each other, so the docs should not imply a linearizable cutover while `TryCatchUpWithPrimary()` is in flight.
- **Source:** `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::GetImpl`; `db/db_impl/db_impl.cc` `DBImpl::GetImpl`; `db/column_family.cc` `ColumnFamilyData::GetThreadLocalSuperVersion`
- **Fix:** Describe the mode as best-effort and eventually consistent unless the application serializes reads and catch-up itself.

### [MISLEADING] Follower "hard links to the primary's files" is broader than what `OnDemandFileSystem` actually does
- **File:** `07_comparison.md`, Follower Instance; `07_comparison.md`, Key Architectural Differences
- **Claim:** "Has its own directory with hard links to the primary's files, tolerating file deletions by the primary"
- **Reality:** Only SSTs are hard-linked locally, and only on demand. Non-SST files such as MANIFEST, CURRENT, WAL, IDENTITY, and OPTIONS are read directly from the leader path.
- **Source:** `env/fs_on_demand.cc` `OnDemandFileSystem::NewRandomAccessFile`; `env/fs_on_demand.cc` `OnDemandFileSystem::NewSequentialFile`; `env/fs_on_demand.cc` `OnDemandFileSystem::FileExists`
- **Fix:** Narrow the statement to on-demand SST linking and explicitly distinguish it from how sequential metadata/WAL files are accessed.

### [MISLEADING] The index's "MANIFEST reads are safe without locking" invariant hides the real rotation/retry behavior
- **File:** `index.md`, Key Invariants
- **Claim:** "MANIFEST reads are safe without locking because MANIFEST records are append-only and atomic within a VersionEdit group"
- **Reality:** The append-only property helps, but `ReactiveVersionSet::MaybeSwitchManifest()` still has explicit `TryAgain()` paths for `CURRENT`/MANIFEST rollover races, and the secondary still synchronizes its own in-memory application of edits.
- **Source:** `db/version_set.cc` `ReactiveVersionSet::MaybeSwitchManifest`; `db/version_set.cc` `ReactiveVersionSet::ReadAndApply`
- **Fix:** Rephrase this as "the secondary reads MANIFEST without coordinating with primary writers, but must still handle manifest-switch races via retry."

## Completeness Gaps

### Remote-compaction override semantics are under-documented
- **Why it matters:** `CompactionServiceOptionsOverride` is the cross-component boundary between the primary scheduler and the remote worker. Missing override details are exactly where operators get surprised.
- **Where to look:** `include/rocksdb/options.h` `CompactionServiceOptionsOverride`; `db/db_impl/db_impl_secondary.cc` `DB::OpenAndCompact`; `db/compaction/compaction_service_test.cc` `RemoteEventListener`
- **Suggested scope:** Expand `06_remote_compaction.md` to cover `listeners`, `statistics`, `file_checksum_gen_factory`, `info_log`, `env`, and the fact that `options_map` is parsed with `ignore_unknown_options=true` so typos are silently ignored.

### The read chapter does not document the inherited APIs and option behaviors that differ from `Get()`/iterators
- **Why it matters:** A developer reading `03_read_operations.md` will come away with the wrong mental model for `MultiGet`, `MultiGetEntity`, `GetMergeOperands`, and `ReadOptions.read_tier`.
- **Where to look:** `db/db_impl/db_impl.cc` `DBImpl::MultiGetCommon`; `db/db_impl/db_impl.cc` `DBImpl::MultiCFSnapshot`; `db/db_secondary_test.cc` `DBSecondaryTest.GetMergeOperands`; `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::GetImpl`
- **Suggested scope:** Add an API matrix covering `Get`, `MultiGet`, `GetMergeOperands`, `NewIterator`, `NewIterators`, and `Refresh()`, including explicit-snapshot behavior and the `kPersistedTier` caveat.

### The docs largely skip the concrete failure modes on open and catch-up
- **Why it matters:** Secondary users need to know whether a given failure is expected-and-retriable (`TryAgain`, missing WAL) or fatal (`Corruption`, logger creation failure).
- **Where to look:** `db/db_secondary_test.cc` `FailOpenIfLoggerCreationFail`; `db/db_secondary_test.cc` `NonExistingDb`; `db/db_secondary_test.cc` `SwitchToNewManifestDuringOpen`; `db/db_secondary_test.cc` `StartFromInconsistent`; `db/db_secondary_test.cc` `InconsistencyDuringCatchUp`
- **Suggested scope:** Add a short "failure modes" subsection to chapters 1 and 2 with retry guidance and expected status families.

### Follower option semantics need more precision than just defaults
- **Why it matters:** The current comparison chapter mentions default values but not mutability or zero-value behavior, which are the first things someone tuning follower refresh needs to know.
- **Where to look:** `include/rocksdb/options.h` `follower_refresh_catchup_period_ms`, `follower_catchup_retry_count`, `follower_catchup_retry_wait_ms`; `options/db_options.cc` option registration; `db/db_impl/db_impl_follower.cc` `PeriodicRefresh`
- **Suggested scope:** Add a short note that these are immutable DB options, are not sanitized/clamped, `0` period means immediate refresh wakeups, `0` retry count means no retry attempts, and `0` retry wait removes the inter-retry delay.

### The docs describe behaviors that the current secondary tests do not cover
- **Why it matters:** Several nuanced behaviors are presented as settled contracts even though the local test file does not exercise them.
- **Where to look:** `db/db_secondary_test.cc`; `db/db_impl/db_impl_secondary.cc`; `db/version_edit_handler.cc` `ManifestTailer::OnColumnFamilyAdd`
- **Suggested scope:** Either add tests or call out these items as implementation details: `WriteOptions::disableWAL` visibility gaps, CF creation after secondary open being ignored, iterator snapshot/tailing rejection, and concurrent read-vs-catch-up semantics.

## Depth Issues

### The snapshot section needs an API-by-API matrix instead of a single generalized story
- **Current:** The chapter mixes `Get()`, iterators, and DB-wide statements like "implicit snapshot isolation" and "cross-CF consistency."
- **Missing:** Which APIs honor `ReadOptions.snapshot`, which ignore it, which reject it, and what changes when `TryCatchUpWithPrimary()` runs concurrently.
- **Source:** `db/db_impl/db_impl_secondary.cc` `GetImpl`, `NewIterator`, `NewIterators`; `db/db_impl/db_impl.cc` `MultiGetCommon`

### The remote-compaction workflow section hides the split between global DB-option overrides and target-CF-only overrides
- **Current:** The override discussion is written as a flat list.
- **Missing:** `DBOptions` overrides apply globally, while the shared-pointer CF overrides are only applied to the target CF selected from `CompactionServiceInput::cf_name`.
- **Source:** `db/db_impl/db_impl_secondary.cc` `DB::OpenAndCompact`

### The comparison chapter needs a more explicit file-access model for follower vs secondary
- **Current:** The table reduces the difference to "uses hard links" vs "max_open_files=-1".
- **Missing:** Secondary directly opens primary files and relies on pinned FDs; follower links SSTs on demand but still reads MANIFEST/CURRENT/WAL from the leader path.
- **Source:** `db/db_impl/db_impl_secondary.h` `OwnTablesAndLogs`; `env/fs_on_demand.cc` `NewSequentialFile`; `env/fs_on_demand.cc` `NewRandomAccessFile`

## Structure and Style Violations

### `index.md` is shorter than the required index size
- **File:** `docs/components/secondary_instance/index.md`
- **Details:** The file is 36 lines, below the requested 40-80 line range.

### Inline code spans are pervasive across the entire subcomponent doc set
- **File:** `docs/components/secondary_instance/index.md` and all `NN_*.md` files
- **Details:** The review prompt explicitly says "NO inline code quotes." The current docs use inline code formatting throughout headings, tables, and prose.

### Several chapter `Files:` lines are incomplete for the code they actually discuss
- **File:** `docs/components/secondary_instance/02_catching_up.md`
- **Details:** The chapter discusses `ManifestTailer::Iterate()` and manifest-tailing behavior, but the `Files:` line omits `db/version_edit_handler.h` and `db/version_edit_handler.cc`.

### Several chapter `Files:` lines are incomplete for the code they actually discuss
- **File:** `docs/components/secondary_instance/05_column_family_handling.md`
- **Details:** The chapter describes post-open CF creation being ignored, which is implemented in `ManifestTailer::OnColumnFamilyAdd`, but the `Files:` line omits `db/version_edit_handler.cc`.

### Several chapter `Files:` lines are incomplete for the code they actually discuss
- **File:** `docs/components/secondary_instance/06_remote_compaction.md`
- **Details:** The chapter relies on `OpenAndCompactOptions` / `CompactionServiceOptionsOverride` in `include/rocksdb/options.h` and on `CompactionServiceInput`/`CompactionServiceResult` serialization in `db/compaction/compaction_service_job.cc`, but those paths are not listed.

## Undocumented Complexity

### `ReadOptions.read_tier = kPersistedTier` is not honored on secondary `Get()`
- **What it is:** Secondary iterators explicitly reject `kPersistedTier`, but secondary `Get()` does not reject it and does not implement the base `DBImpl` memtable-skipping logic either.
- **Why it matters:** Callers may assume "persisted only" excludes WAL-replayed memtables, but secondary `Get()` can still return unflushed data from replayed memtables.
- **Key source:** `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::GetImpl`; `db/db_impl/db_impl.cc` `DBImpl::GetImpl`
- **Suggested placement:** Add to `03_read_operations.md`

### `MultiGet` and `MultiGetEntity` behave differently from `Get()` with snapshots
- **What it is:** Secondary does not override the batched read APIs, so they keep the base `DBImpl::MultiCFSnapshot()` behavior and can honor explicit snapshots even though `Get()` ignores them and iterators reject them.
- **Why it matters:** Without documenting this split, the chapter teaches the wrong API contract and makes the comparison table inaccurate.
- **Key source:** `db/db_impl/db_impl.cc` `DBImpl::MultiGetCommon`; `db/db_impl/db_impl.cc` `DBImpl::MultiGetWithCallbackImpl`
- **Suggested placement:** Add to `03_read_operations.md`

### Remote-worker observability is intentionally partial
- **What it is:** Remote listeners only see a subset of events, and remote `statistics` are local to the worker unless the caller explicitly wires them up.
- **Why it matters:** People debugging remote compaction often assume the worker gets the same event stream and statistics behavior as a local compaction. It does not.
- **Key source:** `include/rocksdb/options.h` `CompactionServiceOptionsOverride`; `db/compaction/compaction_service_test.cc` `RemoteEventListener`
- **Suggested placement:** Add to `06_remote_compaction.md`

### `CompactionServiceInput` carries subcompaction key bounds that the docs never mention
- **What it is:** The serialized remote input includes `has_begin` / `begin` and `has_end` / `end`, which are used to prepare the compaction job for a bounded sub-range.
- **Why it matters:** Anyone trying to understand remote subcompaction partitioning or resume behavior needs to know the input is not just "CF + files + output level."
- **Key source:** `db/compaction/compaction_job.h` `CompactionServiceInput`; `db/compaction/compaction_service_job.cc`
- **Suggested placement:** Add to `06_remote_compaction.md`

### `output_directory` is also the worker's secondary path
- **What it is:** Remote compaction uses `output_directory` as the `secondary_path` passed into `OpenAsSecondaryImpl()`, so the directory is not just for output SSTs; it also houses the worker's info log and any progress files.
- **Why it matters:** This affects cleanup expectations, disk usage, and the "must be empty" guidance when resumption is off.
- **Key source:** `db/db_impl/db_impl_secondary.cc` `DB::OpenAndCompact`; `db/db_impl/db_impl_secondary.cc` `OpenAsSecondaryImpl`
- **Suggested placement:** Add to `06_remote_compaction.md`

### Secondary can open a `TransactionDB` primary, but the docs never mention the cross-component implication
- **What it is:** There is an explicit test covering `OpenAsSecondary()` on a DB created through `TransactionDB::Open()`.
- **Why it matters:** This is the sort of integration question a developer will ask immediately when secondary docs claim compatibility with an "active primary."
- **Key source:** `db/db_secondary_test.cc` `DBSecondaryTest.OpenWithTransactionDB`
- **Suggested placement:** Brief note in `01_opening_and_recovery.md` or `07_comparison.md`

## Positive Notes

- The docs correctly reflect the current `OpenAndCompact()` behavior of skipping WAL recovery and the dedicated test coverage for that path.
- The WAL replay chapter correctly calls out forced checksumming even when `paranoid_checks` is off, which matches the current `LogReaderContainer` implementation.
- The remote-compaction chapter already picked up the recent resumption constraint involving `verify_output_flags` and `paranoid_file_checks`, which is easy to miss if you only read older external materials.
- The overall chapter decomposition is reasonable: opening, catch-up, reads, file deletion, CF handling, remote compaction, and comparison is a good shape for this component.
