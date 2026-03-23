# Review: compaction -- Claude Code

## Summary
(Overall quality rating: good)

The compaction documentation is comprehensive and well-structured, covering 15 chapters across the full breadth of compaction subsystems. The vast majority of technical claims -- option defaults, algorithm descriptions, formula details, struct/field names, and code flow descriptions -- are verified correct against the current codebase. Structure and style compliance is excellent (no box-drawing characters, no line number references, no inline code quotes, proper INVARIANT usage, correct index.md format).

The primary concerns are: (1) two phantom options that do not exist in the current codebase (`level_compaction_dynamic_file_size` was removed in RocksDB 9.0; `change_temperature_compaction_only` never existed), and (2) a significant number of recent features and options (2024-2025) that are not covered, including `target_file_size_is_upper_bound`, `cf_allow_ingest_behind`, FIFO trivial copy options, new temperature values, and several new APIs.

## Correctness Issues

### [STALE] `level_compaction_dynamic_file_size` option was removed
- **File:** 02_leveled_compaction.md, "Output File Splitting" section; also referenced in 08_compaction_job.md "Grandparent Overlap Tracking" section
- **Claim:** "controlled by `level_compaction_dynamic_file_size` in `ColumnFamilyOptions`, see `include/rocksdb/advanced_options.h`, enabled by default"
- **Reality:** This option was deprecated in RocksDB 8.11.0 and removed in RocksDB 9.0.0. The feature (grandparent pre-cut and skippable boundary heuristics) is now always active for `kCompactionStyleLevel`. There is no option gate in the current `ShouldStopBefore()` implementation.
- **Source:** `HISTORY.md` (8.11.0 deprecation, 9.0.0 removal); `db/compaction/compaction_outputs.cc` ShouldStopBefore() -- no option check
- **Fix:** Remove all references to `level_compaction_dynamic_file_size`. State that the grandparent boundary alignment optimization is always enabled for leveled compaction (since RocksDB 9.0).

### [WRONG] `change_temperature_compaction_only` option does not exist
- **File:** 07_fifo_compaction.md, "Strategy 4: Temperature Change Compaction" section (line 149) and Configuration Reference table (line 168)
- **Claim:** "The `change_temperature_compaction_only` option (see `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h`) controls whether the file data is rewritten or only metadata is updated."
- **Reality:** No such option exists in `CompactionOptionsFIFO` or anywhere in `include/rocksdb/advanced_options.h`. The temperature change behavior is controlled by `file_temperature_age_thresholds` and the existing compaction infrastructure.
- **Source:** `include/rocksdb/advanced_options.h` -- `CompactionOptionsFIFO` struct has no `change_temperature_compaction_only` field
- **Fix:** Remove the option from the Limitations subsection and the Configuration Reference table. If temperature change compaction behavior needs clarification, describe it based on the actual code in `compaction_picker_fifo.cc`.

### [MISLEADING] CompactionService definition location
- **File:** 10_remote_compaction.md, Files line and "Four-Phase Protocol" section
- **Claim:** "The primary calls `CompactionService::Schedule()` (see `include/rocksdb/options.h`)"
- **Reality:** `include/rocksdb/options.h` only contains a `std::shared_ptr<CompactionService>` field in `DBOptions`. The `CompactionService` class with its virtual `Schedule()`, `Wait()`, and `OnInstallation()` methods is defined in `include/rocksdb/compaction_service.h` (or similar). The actual method signatures are: `Schedule` returns `CompactionServiceScheduleResponse` and `Wait` returns `CompactionServiceJobStatus`.
- **Source:** `include/rocksdb/options.h` line 1666 (forward reference only); `db/compaction/compaction_service_job.cc` lines 86, 133 (actual usage)
- **Fix:** Update the Files line and in-text references to point to the correct header where `CompactionService` is defined.

### [MISLEADING] ShouldFormSubcompactions conditions incomplete
- **File:** 09_subcompaction.md, "When Subcompactions Are Used" section
- **Claim:** Table shows "Leveled: L0 to Lo (where o > 0), or manual compaction"
- **Reality:** The actual conditions in `Compaction::ShouldFormSubcompactions()` are more nuanced: (a) PlainTable is always excluded, (b) when `compaction_pri == kRoundRobin` with leveled style, subcompactions are formed for any compaction with `output_level_ > 0` even when `max_subcompactions_ <= 1`, (c) the precise leveled condition is `(start_level_ == 0 || is_manual_compaction_) && output_level_ > 0`, (d) universal condition is `number_levels_ > 1 && output_level_ > 0`.
- **Source:** `db/compaction/compaction.cc` ShouldFormSubcompactions() lines 915-943
- **Fix:** Expand the table to include the RoundRobin special case (bypasses max_subcompactions check) and the PlainTable exclusion. Clarify the universal condition includes the `number_levels_ > 1` guard.

### [MISLEADING] Size amplification uses compensated sizes
- **File:** 06_universal_compaction.md, "Strategy 2: Size Amplification Reduction" section
- **Claim:** "Size amplification is computed as: `candidate_size * 100 / base_sr_size`"
- **Reality:** The computation uses `compensated_file_size` (which includes tombstone compensation), not raw file sizes. The formula structure is correct but may give readers the wrong impression about what "size" means here.
- **Source:** `db/compaction/compaction_picker_universal.cc` -- uses `compensated_file_size` from SortedRun
- **Fix:** Clarify that `candidate_size` and `base_sr_size` use compensated file sizes (which add a bonus for files with many deletions).

### [MISLEADING] SST partitioner L0 guard location
- **File:** 13_sst_partitioner.md, "Partitioner Creation" section
- **Claim:** "CompactionOutputs creates a partitioner via Compaction::CreateSstPartitioner()... The partitioner is only created for non-L0 output levels"
- **Reality:** `Compaction::CreateSstPartitioner()` creates the partitioner unconditionally. The L0 guard is in the `CompactionOutputs` constructor, which sets `partitioner_ = nullptr` when `output_level() == 0`.
- **Source:** `db/compaction/compaction.cc` CreateSstPartitioner() (no L0 check); `db/compaction/compaction_outputs.cc` CompactionOutputs constructor lines 807-809
- **Fix:** Attribute the L0 guard to the `CompactionOutputs` constructor, not `CreateSstPartitioner()`.

## Completeness Gaps

### Missing options: `target_file_size_is_upper_bound`
- **Why it matters:** Directly affects compaction output file splitting -- enables tail estimation to prevent oversized output files. Added in 2024.
- **Where to look:** `include/rocksdb/advanced_options.h` (AdvancedColumnFamilyOptions); `db/compaction/compaction_outputs.cc` ShouldStopBefore()
- **Suggested scope:** Add to chapter 08 (CompactionJob), Output File Splitting Criteria section

### Missing option: `cf_allow_ingest_behind`
- **Why it matters:** Column-family-level version of `allow_ingest_behind` that affects universal compaction tombstone preservation and last-level reservation. Added in 2024.
- **Where to look:** `include/rocksdb/advanced_options.h`; `db/compaction/compaction_picker_universal.cc`
- **Suggested scope:** Mention in chapter 06 (Universal Compaction), Interactions section

### Missing options: FIFO trivial copy for temperature change
- **Why it matters:** `allow_trivial_copy_when_change_temperature` and `trivial_copy_buffer_size` enable trivial copy (instead of rewrite) for temperature change compaction, reducing I/O.
- **Where to look:** `include/rocksdb/advanced_options.h` CompactionOptionsFIFO; `db/compaction/compaction_picker_fifo.cc`
- **Suggested scope:** Add to chapter 07 (FIFO Compaction), Strategy 4 and Configuration Reference

### Missing option: `bottommost_file_compaction_delay`
- **Why it matters:** Affects scheduling of bottommost-file compactions in leveled compaction. Not mentioned anywhere in the docs.
- **Where to look:** `include/rocksdb/advanced_options.h`
- **Suggested scope:** Mention in chapter 01 (Overview) triggers table or chapter 02 (Leveled Compaction)

### Missing: New temperature values `kCool` and `kIce`
- **Why it matters:** The temperature tier vocabulary has expanded beyond hot/warm/cold. Relevant to FIFO temperature change compaction.
- **Where to look:** `include/rocksdb/advanced_options.h` Temperature enum
- **Suggested scope:** Brief mention in chapter 07 (FIFO Compaction), temperature change section

### Missing API: `CancelAwaitingJobs()` in CompactionService
- **Why it matters:** Allows cancellation of awaiting remote compaction jobs. Omission from chapter 10 is notable.
- **Where to look:** CompactionService class definition
- **Suggested scope:** Add to chapter 10 (Remote Compaction), Limitations section or a new Cancellation subsection

### Missing: `CompactForTieringCollector`
- **Why it matters:** A built-in table property collector that marks files for tiered compaction. Interacts with compaction triggers.
- **Where to look:** `utilities/` or `db/compaction/`
- **Suggested scope:** Brief mention in chapter 01 (Overview) or a new section

### Missing option: `default_write_temperature`
- **Why it matters:** Affects temperature assignment for compaction output files.
- **Where to look:** `include/rocksdb/advanced_options.h`
- **Suggested scope:** Brief mention in relevant chapters (07, 08)

### Missing: Abort background compaction API
- **Why it matters:** New per-job abort API (`AbortBackgroundCompaction`) beyond the existing `AbortAllCompactions()`.
- **Where to look:** `include/rocksdb/db.h`
- **Suggested scope:** Add to chapter 14 (Manual Compaction), Cancellation section

### Missing: Universal compaction re-picking optimization
- **Why it matters:** Reduces input lock time by forwarding/re-picking in universal compaction. Behavioral change from 2024.
- **Where to look:** `db/compaction/compaction_picker_universal.cc`
- **Suggested scope:** Mention in chapter 06 (Universal Compaction)

## Depth Issues

### VerifyOutputFlags bitmask not explained
- **Current:** Chapters 08 and 10 mention `verify_output_flags` briefly
- **Missing:** The `VerifyOutputFlags` enum is a bitmask with flags `kVerifyNone`, `kVerifyBlockChecksum`, `kVerifyIteration`, `kVerifyFileChecksum`, `kEnableForLocalCompaction`, `kEnableForRemoteCompaction`. The semantics of combining flags are not explained.
- **Source:** `include/rocksdb/advanced_options.h` VerifyOutputFlags enum

### ProcessKeyValueCompaction refactored step sequence
- **Current:** Chapter 08 describes a 8-step sequence
- **Missing:** The actual code has been refactored into named helper methods: `SetupAndValidateCompactionFilter()`, `CreateInputIterator()`, `CreateCompactionIterator()`, `CreateFileHandlers()`, `ProcessKeyValue()`, `FinalizeProcessKeyValueStatus()`, `FinalizeSubcompaction()`, `NotifyOnSubcompactionBegin/Completed()`. The doc's step numbers don't precisely match the current code structure.
- **Source:** `db/compaction/compaction_job.cc` ProcessKeyValueCompaction()

## Structure and Style Violations

No structural or style violations found. All checks pass:
- index.md is 48 lines (within 40-80 target)
- Follows the standard pattern (overview, key source files, chapter table, key characteristics, key invariants)
- No box-drawing characters in any file
- No line number references in any file
- All 15 chapter files have a `**Files:**` line
- No inline code quotes (only function/struct name references)
- INVARIANT used only for true correctness invariants (2 occurrences, both valid)

## Undocumented Complexity

### Leveled compaction input file expansion limiting
- **What it is:** Recent change (2024) that limits how much input file expansion can occur during leveled compaction picking, preventing excessively large compactions.
- **Why it matters:** Affects compaction size predictability and write stall avoidance.
- **Key source:** `db/compaction/compaction_picker_level.cc`
- **Suggested placement:** Add to chapter 02 (Leveled Compaction), Step 3 "SetupOtherInputsIfNeeded"

### Universal compaction input lock reduction via forwarding/re-picking
- **What it is:** Optimization that reduces the time compaction inputs are locked by re-picking after execution if the original inputs are no longer optimal.
- **Why it matters:** Reduces write stall risk in universal compaction under heavy write load.
- **Key source:** `db/compaction/compaction_picker_universal.cc`
- **Suggested placement:** Add to chapter 06 (Universal Compaction), new subsection or Interactions section

### TimedPut (kTypeValuePreferredSeqno) compaction handling
- **What it is:** Chapter 11 mentions ValuePreferredSeqno briefly, but the broader implications for per-key placement, range deletion interaction (checking coverage before and after sequence number swap), and the `proximal_after_seqno_` threshold are under-documented.
- **Why it matters:** TimedPut is a key API for per-key-placement workloads. Incorrect handling causes data loss or incorrect reads.
- **Key source:** `db/compaction/compaction_iterator.cc` NextFromInput() kTypeValuePreferredSeqno handling
- **Suggested placement:** Expand the existing section in chapter 11

## Positive Notes

- **Exceptional accuracy on defaults and formulas**: Every option default value, score formula, and threshold constant was verified correct. This includes non-trivial formulas like the L0 score computation with 10x scaling, the dynamic level size calculation stages, the size-based intra-L0 condition, and the grandparent pre-cut 50-90% adaptive range.
- **Correct struct and field name references**: All struct names (`CompactionStatsFull`, `SubcompactionState`, `SortedRun`, `CompactionServiceInput`, `CompactionServiceResult`, etc.) and field names (`preserve_seqno_after_`, `proximal_after_seqno_`, `extra_num_subcompaction_threads_reserved_`, `cur_compactions_reserved_size_`, etc.) match the source code exactly.
- **Good coverage of the compaction filter evolution**: The FilterV3 -> FilterV2 -> Filter delegation chain and the `kRemoveAndSkipUntil` silent-to-kKeep conversion are both correctly documented.
- **Well-structured chapters**: Clean separation of concerns across 15 chapters with consistent formatting. The index.md provides an excellent entry point.
- **Correct invariant identification**: The two INVARIANT uses (clean cut, non-overlapping subcompaction ranges) are genuine correctness invariants, not style preferences.
- **Thorough SST partitioner and disk space management chapters**: Chapters 13 and 15 had zero correctness issues, with all field names, method names, and default values verified correct.
