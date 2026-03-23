# Review: version_management — Codex

## Summary
Overall quality rating: needs work

The doc set is organized well. The index is within the requested size, the chapter split follows the real object graph, and the high-level coverage of VersionEdit, Version, MANIFEST, atomic groups, and SuperVersion gives readers a useful map of the subsystem.

The main problem is that several of the hardest parts of the component are described with the wrong control flow or outdated semantics. The biggest errors are in best-effort recovery, VersionBuilder save points, where consistency checks actually happen, and how secondary catch-up behaves. There are also stale or too-absolute statements around skip-manifest-write and SuperVersion TLS handling. Those are high-impact mistakes because they target the code paths maintainers rely on when debugging recovery and concurrency bugs.

## Correctness Issues

### [WRONG] Best-effort recovery save-point flow is reversed
- **File:** `08_recovery.md`, section `Best-Effort Recovery (VersionEditHandlerPointInTime)`
- **Claim:** "Step 1: As edits are replayed, `MaybeCreateVersionBeforeApplyEdit()` checks file existence via `VerifyFile()` before applying each edit." and "Step 2: If all files are present, `CreateOrReplaceSavePoint()` captures the current state as a save point."
- **Reality:** `VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit()` first snapshots the current builder state with `CreateOrReplaceSavePoint()`, then applies the edit. Missing-file detection happens during `VersionBuilder::Apply()` through `ApplyFileAddition()` and `ApplyBlobFileAddition()`, which call `VerifyFile()` and `VerifyBlobFile()`. The handler only materializes the save point when the just-applied edit made a previously valid point invalid, or when finalization is forced.
- **Source:** `db/version_edit_handler.cc`, `VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit`; `db/version_builder.cc`, `Rep::Apply`, `ApplyFileAddition`, `ApplyBlobFileAddition`
- **Fix:** Rewrite the flow so it says the handler snapshots the pre-edit builder state, applies the edit, and then decides whether the pre-edit save point should be materialized.

### [WRONG] VersionBuilder save points are not "known-good because all files are present"
- **File:** `05_version_builder.md`, section `Save Points`
- **Claim:** "Step 1: As edits are replayed, `CreateOrReplaceSavePoint()` captures a known-good state whenever all files are present."
- **Reality:** `CreateOrReplaceSavePoint()` does not verify file existence or completeness. It moves the current `Rep` into `savepoint_` and clones it. Whether that snapshot is usable is decided later by `ValidVersionAvailable()`, which can accept either a complete version or, under restricted conditions, an allowed incomplete one.
- **Source:** `db/version_builder.cc`, `VersionBuilder::CreateOrReplaceSavePoint`, `VersionBuilder::SaveSavePointTo`, `Rep::ValidVersionAvailable`
- **Fix:** Say the save point is a snapshot of the builder's pre-edit mutable state, and separately explain that `ValidVersionAvailable()` determines whether it is usable.

### [MISLEADING] `allow_incomplete_valid_version` is documented as a simple "L0 suffix" rule
- **File:** `05_version_builder.md`, section `Incomplete Valid Versions`
- **Claim:** "When `allow_incomplete_valid_version` is true, a Version missing only a suffix of L0 SST files (and their associated blob files) is still considered valid."
- **Reality:** the implementation is stricter. `ValidVersionAvailable()` refuses any version touched by atomic-group edits, refuses any non-L0 missing SST, requires missing L0 SSTs to form a suffix in expected L0 order, and only tolerates missing blob files when they are older than the minimum needed blob file or linked only to the missing L0 suffix.
- **Source:** `db/version_builder.cc`, `Rep::ValidVersionAvailable`, `OnlyMissingL0Suffix`, `MissingL0FilesAreL0Suffix`, `RemainingSstFilesNotMissingBlobFiles`; `db/version_set_test.cc`, `BestEffortsRecoverIncompleteVersionTest`, `AtomicGroupBestEffortRecoveryTest`
- **Fix:** Expand the section to document all admissibility rules, especially the atomic-group and blob-linkage restrictions.

### [WRONG] LSM ordering and overlap checks are assigned to `Apply()`, but they happen in `SaveTo()`
- **File:** `03_version_storage_info.md`, section `Consistency Checks`; `05_version_builder.md`, section `Consistency Validation`
- **Claim:** "When `force_consistency_checks` is true (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), `VersionBuilder` validates file ordering and overlap constraints during `Apply()` and `SaveTo()`." Also: "During `Apply()`, the builder validates: ... File ordering constraints for the level (non-overlapping for L1+)"
- **Reality:** `Apply()` validates add/delete consistency, duplicate/add-at-different-level errors, and missing-file bookkeeping. The full level ordering and overlap validation comes from `CheckConsistency()` calls inside `SaveTo()`. In debug builds those checks always run; in release builds they depend on `force_consistency_checks`, which now defaults to `true`.
- **Source:** `db/version_builder.cc`, `Rep::Apply`, `ApplyFileAddition`, `ApplyFileDeletion`, `Rep::SaveTo`, `CheckConsistency`; `include/rocksdb/advanced_options.h`, field `force_consistency_checks`
- **Fix:** Move the ordering and overlap discussion to `SaveTo()` and note the debug-build override plus the current default of `force_consistency_checks = true`.

### [WRONG] Manifest tailing catch-up mode is described as discovering new column families
- **File:** `08_recovery.md`, section `ManifestTailer`
- **Claim:** "`MustOpenAllColumnFamilies()` returns false, allowing new CFs to be discovered"
- **Reality:** in catch-up mode, `ManifestTailer::OnColumnFamilyAdd()` ignores add-CF edits when the CF does not already exist locally. The tailer only continues tracking CFs that are already present in the local `ColumnFamilySet`.
- **Source:** `db/version_edit_handler.cc`, `ManifestTailer::OnColumnFamilyAdd`
- **Fix:** Say that catch-up mode does not auto-create newly added CFs; it ignores them unless the follower already has that CF.

### [WRONG] `kSVObsolete` sentinel value is wrong
- **File:** `10_super_version.md`, section `Sentinel Values`
- **Claim:** "| `kSVObsolete` | `(void*)1` | Background thread has installed a new SuperVersion |"
- **Reality:** `kSVObsolete` is `nullptr`.
- **Source:** `db/column_family.cc`, definition of `SuperVersion::kSVObsolete`
- **Fix:** Replace `(void*)1` with `nullptr`.

### [WRONG] The TLS acquire path does not have a re-entrant `kSVInUse` fallback
- **File:** `10_super_version.md`, section `Thread-Local Caching`
- **Claim:** "Step 4: If the previous value was `kSVInUse` (reentrant access), fall back to `GetReferencedSuperVersion()` which acquires the mutex."
- **Reality:** `ColumnFamilyData::GetThreadLocalSuperVersion()` asserts that the swapped-out value is never `kSVInUse`. The code only handles the fast path with a cached `SuperVersion*` and the slow path with `kSVObsolete`.
- **Source:** `db/column_family.cc`, `ColumnFamilyData::GetThreadLocalSuperVersion`
- **Fix:** Remove the re-entrant fallback step and state that seeing `kSVInUse` is an invariant violation.

### [MISLEADING] The recovery chapter compresses final version creation and epoch recovery into the wrong layer
- **File:** `08_recovery.md`, section `Normal Recovery Flow`
- **Claim:** "Step 4: For each column family: `VersionBuilder::SaveTo()` produces a new `VersionStorageInfo`; `RecoverEpochNumbers()` infers missing epoch numbers; The new Version is installed as current"
- **Reality:** handler finalization and epoch recovery are separate phases. `VersionEditHandler::CheckIterationResult()` loads tables and forces final version creation via `MaybeCreateVersionBeforeApplyEdit(..., true)`, which calls `SaveTo()` and `AppendVersion()`. `RecoverEpochNumbers()` is called later by `VersionSet::Recover()` or `VersionSet::TryRecoverFromOneManifest()` after handler iteration succeeds.
- **Source:** `db/version_edit_handler.cc`, `VersionEditHandler::CheckIterationResult`; `db/version_set.cc`, `VersionSet::Recover`, `VersionSet::TryRecoverFromOneManifest`, `VersionSet::RecoverEpochNumbers`
- **Fix:** Split the writeup into handler finalization versus post-replay epoch-number repair.

### [MISLEADING] File-boundary handling for UDT recovery is documented as "pad minimum timestamps"
- **File:** `08_recovery.md`, section `VersionEditHandler (Normal Recovery)`
- **Claim:** "`MaybeHandleFileBoundariesForNewFiles()` pads minimum timestamps to file boundaries when `user_defined_timestamps_persisted` is false"
- **Reality:** the helper pads the smallest boundary with the minimum timestamp, but the largest boundary is padded with the maximum timestamp when it is a range-tombstone sentinel. It can also force `user_defined_timestamps_persisted = false` for legacy files when UDT persistence is being enabled.
- **Source:** `db/version_edit_handler.cc`, `VersionEditHandler::MaybeHandleFileBoundariesForNewFiles`
- **Fix:** Document the smallest and largest boundary rules separately and mention the UDT-upgrade backfill behavior.

### [STALE] Skip-manifest-write mode is documented as a generic durability bypass for arbitrary in-memory updates
- **File:** `07_log_and_apply.md`, section `Skip-Manifest-Write Mode`
- **Claim:** "This is used for in-memory state changes that don't need durability (e.g., some SuperVersion installations)."
- **Reality:** the in-tree call site is the special `DBImpl::SetOptions()` path. It uses a dummy edit to append a new `Version` without writing MANIFEST and without releasing the DB mutex, so mutable option changes take effect atomically with respect to other version appenders.
- **Source:** `db/db_impl/db_impl.cc`, `DBImpl::SetOptions`; `db/version_set.cc`, `VersionSet::ProcessManifestWrites`; `db/version_edit.h`, `VersionEdit::MarkNoManifestWriteDummy`
- **Fix:** Replace the generic example with the actual `SetOptions()` use case and explain why it exists.

### [MISLEADING] `PrepareAppend()` is not always called without the DB mutex
- **File:** `04_version.md`, section `PrepareAppend`
- **Claim:** "This is called without the DB mutex held to avoid blocking."
- **Reality:** that is true on the normal MANIFEST-write path, but the skip-manifest-write `SetOptions()` path deliberately calls `PrepareAppend()` while still holding the DB mutex.
- **Source:** `db/version_set.cc`, `VersionSet::ProcessManifestWrites`, `Version::PrepareAppend`; `db/db_impl/db_impl.cc`, `DBImpl::SetOptions`
- **Fix:** Qualify the statement as the normal path and call out the skip-manifest-write exception.

### [MISLEADING] The compaction-score section uses the wrong option name and oversimplifies what `l0_delay_trigger_count_` counts
- **File:** `03_version_storage_info.md`, section `Compaction Scoring`
- **Claim:** "`L0`: Score considers both file count vs `level0_compaction_trigger` and total size. The `l0_delay_trigger_count_` tracks the count used for write stall decisions."
- **Reality:** the option is `level0_file_num_compaction_trigger`, not `level0_compaction_trigger`. Also, `l0_delay_trigger_count_` is not always the raw number of L0 files: for universal compaction `CalculateBaseBytes()` folds non-empty lower levels into a total sorted-run count, and that count is what later feeds the L0-style score and stall logic.
- **Source:** `include/rocksdb/options.h`, field `level0_file_num_compaction_trigger`; `db/version_set.cc`, `VersionStorageInfo::CalculateBaseBytes`, `VersionStorageInfo::ComputeCompactionScore`; `db/column_family.cc`, `GetL0FileCountForCompactionSpeedup`
- **Fix:** Rename the option, explain the universal-compaction sorted-run behavior, and avoid describing `l0_delay_trigger_count_` as just "L0 file count".

## Completeness Gaps

### `verify_manifest_content_on_close` is completely missing
- **Why it matters:** this is a new DB option in the VersionSet close path. Anyone diagnosing MANIFEST corruption or close-time failures needs to know that RocksDB can now re-read and, if needed, rewrite MANIFEST on close.
- **Where to look:** `include/rocksdb/options.h`, field `verify_manifest_content_on_close`; `db/version_set.cc`, `VersionSet::Close`; `db/version_set_test.cc`, `ManifestContentValidationOnClose*`
- **Suggested scope:** add a short subsection to `06_manifest.md` describing the validation and rewrite loop and that the option is mutable.

### MANIFEST snapshot docs miss the `write_dbid_to_manifest` and `write_identity_file` semantics
- **Why it matters:** DB identity persistence now depends on options, and MANIFEST snapshots can start with a standalone DB ID record before any per-CF state. This affects compatibility and provenance debugging.
- **Where to look:** `include/rocksdb/options.h`, fields `write_dbid_to_manifest` and `write_identity_file`; `db/version_set.cc`, `VersionSet::WriteCurrentStateToManifest`; `db/db_impl/db_impl_open.cc`
- **Suggested scope:** expand `06_manifest.md` with a brief note on DB ID record ordering and the requirement that `write_dbid_to_manifest` and `write_identity_file` cannot both be false.

### MANIFEST rewrite docs omit the WAL-deletion carry-forward record
- **Why it matters:** without this detail, the snapshot description misses how a rewritten MANIFEST preserves obsolete-WAL state and avoids accidentally "resurrecting" deleted WALs on later additions.
- **Where to look:** `db/version_set.cc`, `VersionSet::WriteCurrentStateToManifest`
- **Suggested scope:** add one paragraph to `06_manifest.md` near the WAL-tracking section.

### Recovery docs do not explain the option-dependent strictness of table loading
- **Why it matters:** the text describes normal recovery as strictly failing on missing files, but `VersionEditHandler::LoadTables()` also relaxes failures when `paranoid_checks` is false, and separately has `no_error_if_files_missing`.
- **Where to look:** `db/version_edit_handler.cc`, `VersionEditHandler::LoadTables`; `include/rocksdb/options.h`, `paranoid_checks`
- **Suggested scope:** extend `08_recovery.md` with a short option-semantics note.

### Best-effort recovery behavior is under-documented relative to the tests
- **Why it matters:** the tests encode the real contract for acceptable incomplete versions, especially around atomic groups and blob-file linkage. A maintainer cannot safely change recovery behavior from the current docs.
- **Where to look:** `db/version_set_test.cc`, `AtomicGroupBestEffortRecoveryTest`, `BestEffortsRecoverIncompleteVersionTest`; `db/version_builder.cc`
- **Suggested scope:** expand `05_version_builder.md` and `08_recovery.md` rather than adding a new chapter.

### Secondary catch-up cleanup behavior is barely covered
- **Why it matters:** `ManifestTailer` collects intermediate files that were added and then deleted during tailing so the caller can clean them up. That is an operational detail people need when debugging follower lag or leaked files.
- **Where to look:** `db/version_edit_handler.cc`, `ManifestTailer::GetAndClearIntermediateFiles`; `db/version_builder.cc`, intermediate file tracking
- **Suggested scope:** add a small note to `08_recovery.md`.

## Depth Issues

### `06_manifest.md` needs the sharp-edge semantics of `max_manifest_space_amp_pct`
- **Current:** the chapter gives the auto-tuning formula and says the threshold adapts after rolling.
- **Missing:** `max_manifest_space_amp_pct = 0` intentionally approximates the old rewrite-on-every-manifest-write behavior once the compacted manifest is larger than the minimum cap; negative values are clamped to zero in `UpdatedMutableDbOptions()`; and changes take effect on the next manifest write, not immediately.
- **Source:** `include/rocksdb/options.h`, `max_manifest_space_amp_pct`; `db/version_set.cc`, `VersionSet::UpdatedMutableDbOptions`; `db/db_etc3_test.cc`, `AutoTuneManifestSize`

### `03_version_storage_info.md` needs a more precise L0 scoring explanation
- **Current:** "Score considers both file count vs `level0_compaction_trigger` and total size."
- **Missing:** the option sanitizer forces `level0_file_num_compaction_trigger` away from zero, the level-style score is effectively the max of file-count pressure and size pressure, and universal compaction reuses the L0 slot to represent total sorted runs across the DB.
- **Source:** `db/column_family.cc`, `SanitizeOptions`; `db/version_set.cc`, `VersionStorageInfo::ComputeCompactionScore`, `VersionStorageInfo::CalculateBaseBytes`

### `10_super_version.md` skips the most subtle part of the TLS ref handoff
- **Current:** the chapter explains the swap and CAS fast path and invalidation at a high level.
- **Missing:** `GetReferencedSuperVersion()` takes an extra ref before attempting to return the thread-local pointer, and may immediately drop one ref if `ReturnThreadLocalSuperVersion()` fails. Separately, `ResetThreadLocalSuperVersions()` scrapes TLS slots to `kSVObsolete` before the old SuperVersion is unreffed so TLS can never hold the last reference.
- **Source:** `db/column_family.cc`, `ColumnFamilyData::GetReferencedSuperVersion`, `ResetThreadLocalSuperVersions`, `InstallSuperVersion`

## Structure and Style Violations

### Inline code quotes are used throughout the doc set
- **File:** all files under `docs/components/version_management/`
- **Details:** the docs rely heavily on inline backticked identifiers and option names even though the style rules for this doc set explicitly say not to use inline code quotes.

### Several chapters exceed the requested 40-80 line range
- **File:** `01_version_edit.md`, `07_log_and_apply.md`, `08_recovery.md`, `09_column_families.md`, `10_super_version.md`
- **Details:** these chapters are 86, 82, 106, 98, and 87 lines respectively.

### `Key Invariants` mixes real invariants with implementation details
- **File:** `index.md`
- **Details:** "Checksum on every MANIFEST record (reuses WAL log format); verified before replay" is a mechanism, not a correctness invariant in the "corruption or crash if violated" sense requested by the style guide.

## Undocumented Complexity

### Close-time MANIFEST self-healing
- **What it is:** `VersionSet::Close()` first size-checks the current MANIFEST, may rewrite it through `LogAndApply()` if the size check fails, and when `verify_manifest_content_on_close_` is enabled it performs up to two full rereads with at most one rewrite in between.
- **Why it matters:** this explains why `Close()` can do real I/O, rotate MANIFEST, or return corruption and I/O errors even when no user writes are in flight.
- **Key source:** `db/version_set.cc`, `VersionSet::Close`; `db/version_set_test.cc`, `ManifestContentValidationOnClose*`
- **Suggested placement:** `06_manifest.md`

### Snapshot record ordering on MANIFEST rewrite
- **What it is:** `WriteCurrentStateToManifest()` can emit DB ID first, then WAL additions, then a WAL deletion watermark, then per-CF comparator and UDT-persistence metadata, and finally the file records.
- **Why it matters:** this is useful for MANIFEST dump analysis, downgrade debugging, DB identity provenance, and WAL-state reasoning.
- **Key source:** `db/version_set.cc`, `VersionSet::WriteCurrentStateToManifest`
- **Suggested placement:** `06_manifest.md`

### Best-effort recovery validity is a multi-factor policy, not just "missing suffix of L0"
- **What it is:** allowed incomplete versions depend on missing-file location, L0 ordering, blob-file linkage, and whether atomic groups touched the state.
- **Why it matters:** otherwise a maintainer will assume best-effort recovery is broader than it is and can make unsafe changes to missing-file handling.
- **Key source:** `db/version_builder.cc`, `Rep::ValidVersionAvailable`; `db/version_set_test.cc`, `BestEffortsRecoverIncompleteVersionTest`, `AtomicGroupBestEffortRecoveryTest`
- **Suggested placement:** `05_version_builder.md` and `08_recovery.md`

### SuperVersion invalidation is coordinated to avoid TLS owning the last ref
- **What it is:** installing a new SuperVersion first scrapes all TLS slots to `kSVObsolete`, then unreferences the old SuperVersion. `GetReferencedSuperVersion()` additionally uses a temporary extra ref to make the TLS-return race safe.
- **Why it matters:** this is the core lifetime guarantee behind lock-free reads and an easy place to introduce use-after-free bugs.
- **Key source:** `db/column_family.cc`, `InstallSuperVersion`, `ResetThreadLocalSuperVersions`, `GetReferencedSuperVersion`
- **Suggested placement:** `10_super_version.md`

### `max_manifest_space_amp_pct = 0` is a real operational cliff
- **What it is:** after the compacted MANIFEST grows beyond the minimum cap, every later manifest write can force a rewrite; the tests use this to emulate the old behavior.
- **Why it matters:** a small configuration change can dramatically increase MANIFEST write amplification and commit stall time.
- **Key source:** `include/rocksdb/options.h`, option comment; `db/db_etc3_test.cc`, `AutoTuneManifestSize`
- **Suggested placement:** `06_manifest.md`

### Catch-up tailing accumulates deletable intermediate files
- **What it is:** while tailing MANIFEST, RocksDB tracks files that were added and then deleted before they ever became part of an installed Version, and exposes them through `GetAndClearIntermediateFiles()`.
- **Why it matters:** this is part of keeping followers and secondary instances from leaking files while they catch up.
- **Key source:** `db/version_edit_handler.cc`, `ManifestTailer::GetAndClearIntermediateFiles`; `db/version_builder.cc`, intermediate file tracking
- **Suggested placement:** `08_recovery.md`

## Positive Notes

- The index file follows the requested structure well: overview, key source files, chapter table, key characteristics, and key invariants.
- The chapter decomposition mirrors the real subsystem shape, which makes the material easier to navigate than a single monolithic writeup.
- `01_version_edit.md` and `11_version_compatibility.md` do a good job on MANIFEST tag format, safe-ignore behavior, and forward-compatibility concepts.
- The atomic-group material in `01_version_edit.md` is directionally strong and points readers at one of the most important cross-CF correctness mechanisms in the subsystem.
