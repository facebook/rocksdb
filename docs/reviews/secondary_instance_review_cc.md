# Review: secondary_instance -- Claude Code

## Summary
**Overall quality: good**

The documentation provides solid coverage of secondary instance internals including the catch-up workflow, read operations, file deletion challenges, column family handling, remote compaction, and comparison with other read modes. The code-level details are mostly accurate and the chapter organization is logical.

The main concerns are: (1) incorrect constructor parameter description claiming a non-existent "is_secondary" flag, (2) outdated API signatures using `DB**` instead of `unique_ptr<DB>*`, (3) incomplete listing of `CompactionServiceOptionsOverride` fields applied during remote compaction, and (4) index.md is slightly under the 40-line minimum. All are fixable without restructuring.

## Correctness Issues

### [WRONG] Constructor parameter explanation
- **File:** 01_opening_and_recovery.md, "Opening Workflow" Step 1
- **Claim:** "The constructor calls `DBImpl(db_options, dbname, false, true, true)` -- the second `true` marks this as a read-only instance, and the third `true` marks it as a secondary instance."
- **Reality:** `DBImpl` constructor signature is `DBImpl(const DBOptions& options, const std::string& dbname, const bool seq_per_batch = false, const bool batch_per_txn = true, bool read_only = false)`. The five-argument call `DBImpl(db_options, dbname, false, true, true)` means `seq_per_batch=false`, `batch_per_txn=true`, `read_only=true`. There is no "is_secondary" parameter in `DBImpl`.
- **Source:** `db/db_impl/db_impl.h` (DBImpl constructor, around line 197), `db/db_impl/db_impl_secondary.cc` (line 30)
- **Fix:** Replace with: "The constructor calls `DBImpl(db_options, dbname, false, true, true)` with `seq_per_batch=false`, `batch_per_txn=true`, and `read_only=true`. The secondary-specific behavior is determined by the `DBImplSecondary` subclass, not by a flag in `DBImpl`."

### [STALE] API signature uses raw pointer instead of unique_ptr
- **File:** 01_opening_and_recovery.md, "Public API" table
- **Claim:** Single CF overload takes `DB**`; multi CF overload also takes `DB**` (implied by table structure)
- **Reality:** Both overloads take `std::unique_ptr<DB>*` since D13311 (`Deprecate raw DB pointer in public APIs`). The raw-pointer variants were removed in D14335.
- **Source:** `include/rocksdb/db.h` lines 221-223 and 256-260
- **Fix:** Change `DB**` to `unique_ptr<DB>*` in both overload rows. Also update the single-CF row parameters to: `Options`, primary `name`, `secondary_path`, `unique_ptr<DB>*`

### [MISLEADING] WAL sequence number comparison description
- **File:** 02_catching_up.md, "WAL Replay Details" Step 2
- **Claim:** "The batch's sequence number is compared against the largest sequence number in L0 files for the relevant column family."
- **Reality:** The code checks `l0_files.back()->fd.largest_seqno`, which is specifically the largest seqno of the *last* L0 file (by sorted order). This is a conservative heuristic -- not a comparison against "the largest sequence number in L0 files" which could be interpreted as scanning all L0 files. It's actually the last file's largest seqno.
- **Source:** `db/db_impl/db_impl_secondary.cc` lines 242-251
- **Fix:** Replace with: "The batch's sequence number is compared against the `largest_seqno` of the newest L0 SST file for the relevant column family. If the batch's sequence is at or below this threshold, it is assumed the data is already persisted in SSTs and is skipped."

### [MISLEADING] ReadOnly instance "Does not create any files" claim
- **File:** 07_comparison.md, "ReadOnly Instance" section
- **Claim:** "Does not create any files (no info log, no metadata)"
- **Reality:** `DBImplReadOnly` inherits from `DBImpl` which may create info log files depending on `DBOptions::info_log` configuration. The `ReadOnly` instance does not own tables and logs (`OwnTablesAndLogs()` returns false) but whether it creates info log files depends on options, not a hard prohibition.
- **Source:** `db/db_impl/db_impl_readonly.h`, `db/db_impl/db_impl.cc` (constructor)
- **Fix:** Soften to: "Does not create database files. May create info log files depending on options configuration."

## Completeness Gaps

### Missing: `options_map` override in CompactionServiceOptionsOverride
- **Why it matters:** Since commit 0be3abf7b ("Arbitrary string map in CompactionServiceOptionsOverride"), the `options_map` field allows overriding arbitrary serializable DB and CF options in remote compaction. The doc in 06_remote_compaction.md Step 2 lists individual override fields but omits `options_map` and also omits `file_checksum_gen_factory`, `statistics`, `listeners`, `env`, and `info_log`.
- **Where to look:** `include/rocksdb/options.h` (struct CompactionServiceOptionsOverride), `db/db_impl/db_impl_secondary.cc` lines 1496-1516
- **Suggested scope:** Update Step 2 in 06_remote_compaction.md to mention `options_map` for arbitrary serializable option overrides, and list the additional directly-set fields (`file_checksum_gen_factory`, `statistics`, `listeners`, `env`, `info_log`).

### Missing: `compaction_filter_factory` in override list
- **Why it matters:** The doc lists override fields but omits `compaction_filter_factory`, which is applied at line 1541-1542 of `db_impl_secondary.cc`. Users configuring remote compaction need to know all overridable fields.
- **Where to look:** `db/db_impl/db_impl_secondary.cc` line 1541
- **Suggested scope:** Add `compaction_filter_factory` to the list of overridable fields in 06_remote_compaction.md Step 2.

### Missing: Second `OpenAndCompact` overload (simple version)
- **Why it matters:** The doc in 06_remote_compaction.md only describes the overload with `OpenAndCompactOptions`. There's also a simpler overload that takes just `name`, `output_directory`, `input`, `output`, `override_options` and delegates with default `OpenAndCompactOptions()`.
- **Where to look:** `include/rocksdb/db.h` lines 285-288, `db/db_impl/db_impl_secondary.cc` lines 1605-1611
- **Suggested scope:** Brief mention in 06_remote_compaction.md API section.

### Missing: `allow_resumption` option details
- **Why it matters:** The `OpenAndCompactOptions::allow_resumption` field (default `false`) controls whether compaction can resume from progress files. The doc describes the resumption mechanism well but doesn't mention that `allow_resumption` defaults to `false` and that the `paranoid_file_checks` limitation also applies to `verify_output_flags` containing `kVerifyIteration`. Wait -- it does mention `kVerifyIteration`. But it says `allow_resumption=false` requires empty output directory, which is correct. The default value is not mentioned.
- **Where to look:** `include/rocksdb/options.h` lines 2911-2950
- **Suggested scope:** Mention the default value (`false`) in 06_remote_compaction.md.

### Missing: `GetMergeOperands` support
- **Why it matters:** `DBImplSecondary::GetImpl()` handles `GetMergeOperands` (code lines 427-446). This was fixed in PRs #13340 and #13396. Not mentioned in the supported operations table in 03_read_operations.md.
- **Where to look:** `db/db_impl/db_impl_secondary.cc` lines 427-446
- **Suggested scope:** Add row to the supported operations table in 03_read_operations.md.

### Missing: `MultiGet` potential inconsistency with `Get` snapshot behavior
- **Why it matters:** The doc says `MultiGet()` is "Inherited from DBImpl" which is true, but the base class `MultiGet` batch implementation does not go through `DBImplSecondary::GetImpl()`. The base class may handle snapshots differently. Developers may assume `MultiGet` and `Get` have identical snapshot semantics on secondary.
- **Where to look:** `db/db_impl/db_impl.cc` (DBImpl::MultiGet)
- **Suggested scope:** Add a note in 03_read_operations.md that `MultiGet` is inherited and uses the base class implementation path.

## Depth Issues

### RemoveOldMemTables eviction logic could be clearer
- **Current:** "RemoveOldMemTables() frees memtables whose data has been fully flushed to SST (as indicated by the column family's log number from the MANIFEST)."
- **Missing:** The mechanism is specifically: `cfd->imm()->RemoveOldMemTables(cfd->GetLogNumber(), ...)` where `GetLogNumber()` returns the WAL log number associated with the most recent flushed memtable as recorded in the MANIFEST. Immutable memtables created from WAL files with log numbers below this are removed. This is important because it explains why there can be temporary memory growth if the primary has a large unflushed backlog.
- **Source:** `db/db_impl/db_impl_secondary.cc` line 681

### SuperVersion installation in TryCatchUpWithPrimary
- **Current:** "For each changed column family, a new `SuperVersion` is installed."
- **Missing:** The code at lines 683-685 does `job_context.superversion_contexts.back()` which reuses a single `SuperVersionContext`. It calls `cfd->InstallSuperVersion(&sv_context, &mutex_)` then `sv_context.NewSuperVersion()`. The fact that it uses `back()` means it's accessing the last element of the superversion_contexts vector, and `NewSuperVersion()` prepares for the next CF. This is a subtle detail but important for understanding the code flow.
- **Source:** `db/db_impl/db_impl_secondary.cc` lines 680-686

## Structure and Style Violations

### index.md line count below minimum
- **File:** index.md
- **Details:** 36 lines. Spec requires 40-80 lines. Consider expanding with one or two more key characteristics or invariants to reach 40 lines.

## Undocumented Complexity

### WAL recovery mode interaction
- **What it is:** `RecoverLogFiles()` passes `immutable_db_options_.wal_recovery_mode` to the log reader's `ReadRecord()` (line 211). The recovery mode affects how incomplete/corrupted WAL records at the tail are handled. Different modes (kTolerateCorruptedTailRecords, kAbsoluteConsistency, etc.) can produce different results on the secondary.
- **Why it matters:** Users choosing a WAL recovery mode for the secondary need to understand that overly strict modes may cause the secondary to fail to tail WALs that the primary is still writing to (since the tail of an active WAL may appear "incomplete").
- **Key source:** `db/db_impl/db_impl_secondary.cc` lines 210-212
- **Suggested placement:** Add to existing chapter 02 (WAL Replay Details section)

### FindNewLogNumbers filtering by existing readers
- **What it is:** `FindNewLogNumbers()` (lines 109-112) uses a min-log-number optimization: if `log_readers_` is non-empty, it only considers WAL files with log number >= the smallest existing reader's log number. This prevents re-processing old WALs but means the secondary can never "go back" if `log_readers_` gets out of sync.
- **Why it matters:** Understanding this optimization explains why the secondary can't recover from certain error states without reopening.
- **Key source:** `db/db_impl/db_impl_secondary.cc` lines 106-120
- **Suggested placement:** Add to existing chapter 02 (WAL Replay Details section)

### Concurrent read safety during TryCatchUpWithPrimary
- **What it is:** During `TryCatchUpWithPrimary()`, the MANIFEST tailing, WAL replay, and SuperVersion installation all happen under `mutex_`. Read operations (`Get`, `NewIterator`) acquire SuperVersions via `GetAndRefSuperVersion()` / `GetReferencedSuperVersion()` which are lock-free (using atomic operations). This means reads are never blocked by `TryCatchUpWithPrimary()` -- they simply see the old or new SuperVersion depending on timing.
- **Why it matters:** This is a key architectural guarantee for secondary instance performance -- reads don't block on catch-up.
- **Key source:** `db/db_impl/db_impl_secondary.cc` lines 646-688 (catch-up under mutex), `db/db_impl/db_impl.h` (GetAndRefSuperVersion)
- **Suggested placement:** Add to existing chapter 02 or chapter 03

## Positive Notes

- **Accurate file ownership model**: The description of `OwnTablesAndLogs()` and file lifecycle is precise and matches the code.
- **Well-structured catch-up workflow**: Chapter 02 accurately traces the MANIFEST -> WAL -> memtable management -> SuperVersion installation pipeline with correct ordering.
- **Good WAL deletion tolerance coverage**: Chapter 04 correctly explains the `IsPathNotFound` tolerance pattern and the `max_open_files=-1` workaround with its limitations.
- **Remote compaction resumability**: Chapter 06 provides thorough coverage of the compaction progress file management, including the `CompactionProgressFilesScan` structure and the initialization/cleanup workflow, which matches the code well.
- **LogReaderContainer checksumming**: The doc correctly identifies that WAL checksumming is always enabled regardless of `paranoid_checks` and explains the rationale (matching the code comment at line 32-34 of `db_impl_secondary.h`).
- **Comparison table**: Chapter 07 provides a useful side-by-side comparison with accurate class hierarchy and feature differences.
