# Review: compaction — Codex

## Summary
Overall quality rating: significant issues

The chapter breakdown and index structure are useful, and the docs cover a wide surface area. However, several of the most important behavioral claims are stale or wrong in the current tree: deprecated options are described as live controls, sanitized defaults are reported as raw struct defaults, and multiple execution paths that bypass CompactionJob are described as if they do not exist.

The biggest risk is that a maintainer will build the wrong mental model for debugging or feature work. The highest-value fixes are to correct option semantics, scope the CompactionJob pipeline to merge-style compactions, and document the newer per-key-placement, verification, and FIFO behaviors that were added or changed recently.

## Correctness Issues

### [WRONG] The overview and CompactionJob chapters claim every compaction runs through CompactionJob
- **File:** `docs/components/compaction/01_overview.md` "Compaction Pipeline", `docs/components/compaction/05_trivial_move.md` "Execution", `docs/components/compaction/08_compaction_job.md` "Three-Phase Lifecycle"
- **Claim:** "Every compaction, regardless of style, follows the same three-phase pipeline." "Every compaction (manual or automatic) is executed by a `CompactionJob` object." "When `IsTrivialMove()` returns true, `CompactionJob::Run()` ... takes a fast path."
- **Reality:** The current implementation has multiple non-CompactionJob paths in `DBImpl::BackgroundCompaction()`: FIFO deletion compactions, FIFO temperature-change trivial-copy compactions, and trivial moves are handled directly there. Those paths do not enter `CompactionJob::Prepare()`, `Run()`, or `Install()`.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::BackgroundCompaction()`, `DBImpl::PerformTrivialMove()`
- **Fix:** Re-scope the pipeline description to merge-style compactions, and add an explicit note that deletion compactions, temperature-change trivial-copy compactions, and trivial moves bypass CompactionJob.

### [STALE] `level_compaction_dynamic_file_size` is documented as an active tuning knob even though it is deprecated/no-op
- **File:** `docs/components/compaction/02_leveled_compaction.md` "Output File Splitting", `docs/components/compaction/08_compaction_job.md` "Grandparent Overlap Tracking"
- **Claim:** "The grandparent pre-cut and skippable boundary heuristics implement the compaction output file alignment optimization (controlled by `level_compaction_dynamic_file_size` ... enabled by default)." "The adaptive file-cutting heuristic (enabled by default since RocksDB 7.8 via `level_compaction_dynamic_file_size` ...)"
- **Reality:** `level_compaction_dynamic_file_size` is registered as deprecated in the option table. The grandparent-boundary heuristics are unconditional behavior inside `CompactionOutputs::ShouldStopBefore()`; they are not gated by a live option anymore.
- **Source:** `options/cf_options.cc` option entry for `level_compaction_dynamic_file_size`, `db/compaction/compaction_outputs.cc` `CompactionOutputs::ShouldStopBefore()`, `CompactionOutputs::UpdateGrandparentBoundaryInfo()`, `db/compaction/compaction_job_test.cc` grandparent-boundary tests
- **Fix:** Remove this option as a runtime control from the docs and describe the heuristics as built-in output-splitting behavior.

### [WRONG] The FIFO chapter reports the wrong default for `ttl`
- **File:** `docs/components/compaction/07_fifo_compaction.md` "Configuration Reference"
- **Claim:** "`ttl` ... Default 0"
- **Reality:** The raw option field uses the sentinel `UINT64_MAX - 1`, and sanitization turns that into an effective default of 30 days for block-based tables or 0 otherwise. The default is not uniformly 0.
- **Source:** `include/rocksdb/advanced_options.h` `ColumnFamilyOptions::ttl`, `db/column_family.cc` option sanitization logic
- **Fix:** Document the effective default, including the block-based-table requirement and the non-block-based fallback to 0.

### [WRONG] The universal chapter reports the wrong default for `periodic_compaction_seconds` and misses the `ttl` interaction
- **File:** `docs/components/compaction/06_universal_compaction.md` "Configuration Reference"
- **Claim:** "`periodic_compaction_seconds` ... 0 (disabled)"
- **Reality:** For block-based universal compaction, the effective default is 30 days. Universal compaction also folds `ttl` into `periodic_compaction_seconds`: if one is zero the other wins, otherwise the stricter non-zero value is used.
- **Source:** `include/rocksdb/advanced_options.h` `ColumnFamilyOptions::periodic_compaction_seconds`, `db/column_family.cc` option sanitization logic, `db/db_universal_compaction_test.cc` coverage for the effective default and min-of-two behavior
- **Fix:** Document the effective default for block-based universal compaction and explain the backward-compatibility rule that combines `ttl` and `periodic_compaction_seconds`.

### [WRONG] The dynamic-level chapter gives the wrong threshold for base-level movement and the wrong impression about unnecessary levels
- **File:** `docs/components/compaction/04_dynamic_levels.md` "Stage 3: Compute Level Targets" and "How Base Level Changes Over Time"
- **Claim:** "Levels below the base level ... means no compaction is triggered to those levels." "the base shifts from L6 to L5 when the last level exceeds approximately 2.56 GB"
- **Reality:** Levels below the base do get drained when they are non-empty: `ComputeCompactionScore()` explicitly boosts scores for unnecessary levels. The last-to-second-last base-level shift happens when the last level exceeds `max_bytes_for_level_base`, not when it exceeds `max_bytes_for_level_base * max_bytes_for_level_multiplier`.
- **Source:** `include/rocksdb/advanced_options.h` comments on `level_compaction_dynamic_level_bytes`, `db/version_set.cc` `VersionStorageInfo::CalculateBaseBytes()`, `VersionStorageInfo::ComputeCompactionScore()`
- **Fix:** Rework the example to match the actual `(base / multiplier, base]` target window, and explain that unnecessary levels have infinite targets but are still proactively drained when non-empty.

### [WRONG] The `kMinOverlappingRatio` tie-break rule is misstated
- **File:** `docs/components/compaction/03_file_selection_and_priority.md` "kMinOverlappingRatio"
- **Claim:** "When two files have the same overlap ratio, the file with the smaller key range is preferred."
- **Reality:** The tie-break is by smallest-key ordering, not by key-range width. Equal-score files are ordered by comparing `file->smallest`.
- **Source:** `db/version_set.cc` `SortFileByOverlappingRatio()`
- **Fix:** Replace the tie-break description with "the file whose smallest key sorts first is preferred."

### [WRONG] The FIFO temperature-change section documents a nonexistent option and the wrong execution model
- **File:** `docs/components/compaction/07_fifo_compaction.md` "Strategy 4: Temperature Change Compaction" and "Configuration Reference"
- **Claim:** "`change_temperature_compaction_only` controls whether the file data is rewritten or only metadata is updated."
- **Reality:** That option does not exist. The relevant FIFO options are `allow_trivial_copy_when_change_temperature` and `trivial_copy_buffer_size`. The optimized path is still a file copy into a new SST with a new file number and target temperature; it is not a metadata-only change.
- **Source:** `include/rocksdb/advanced_options.h` `CompactionOptionsFIFO`, `db/db_impl/db_impl_compaction_flush.cc` FIFO temperature-change trivial-copy branch
- **Fix:** Replace the nonexistent option with the real ones and explain that the optimization is copy-based, not metadata-only.

### [WRONG] The subcompaction chapter incorrectly says round-robin fan-out is initialized from the number of L0 input files
- **File:** `docs/components/compaction/09_subcompaction.md` "Boundary Generation Algorithm"
- **Claim:** "For round-robin priority with leveled compaction: initialized to the number of L0 input files"
- **Reality:** The code uses `c->num_input_files(0)`, which means the number of files in the first compaction input level. Under round-robin leveled compaction this applies to non-L0 start levels too, not just L0.
- **Source:** `db/compaction/compaction_job.cc` `CompactionJob::GenSubcompactionBoundaries()`, `db/compaction/compaction.cc` `Compaction::ShouldFormSubcompactions()`
- **Fix:** Describe it as the number of start-level input files, not the number of L0 files.

### [WRONG] The compaction-filter precedence rule is incorrect for flush filtering
- **File:** `docs/components/compaction/12_compaction_filter.md` "Configuration"
- **Claim:** "If both are set, `compaction_filter` takes precedence over `compaction_filter_factory`."
- **Reality:** That is only true for compaction-time filtering. Flush filtering is wired only through `CompactionFilterFactory::ShouldFilterTableFileCreation(kFlush)` and `CreateCompactionFilter()`. A singleton `compaction_filter` does not participate in flush filtering.
- **Source:** `db/compaction/compaction.cc` `Compaction::CreateCompactionFilter()`, `db/flush_job.cc` flush filter setup
- **Fix:** Scope the precedence rule to compaction-time filtering and add an explicit note that flush filtering requires the factory path.

### [MISLEADING] The L0-concurrency description is too absolute
- **File:** `docs/components/compaction/01_overview.md` "Tracking Running Compactions", `docs/components/compaction/02_leveled_compaction.md` "L0 Compaction Specifics"
- **Claim:** "For leveled compaction, only one L0 compaction runs at a time." "Leveled compaction allows at most one L0-to-base compaction concurrently."
- **Reality:** `level0_compactions_in_progress_` does block the normal L0 picker, but `PickFileToCompact()` can still fall back to `PickSizeBasedIntraL0Compaction()` while another start-level-0 compaction is already running. The restriction is more nuanced than "one L0 compaction total."
- **Source:** `db/compaction/compaction_picker_level.cc` `LevelCompactionBuilder::PickFileToCompact()`, `PickSizeBasedIntraL0Compaction()`, `db/compaction/compaction_picker.cc` `CompactionPicker::RegisterCompaction()`
- **Fix:** Rephrase this as a coarse start-level-0 scheduling guard and describe the size-based intra-L0 fallback explicitly.

## Completeness Gaps

### Verification mode semantics are still under-documented
- **Why it matters:** The current text says verification is controlled by `verify_output_flags`, but it does not explain the actual bitmask split between "what to verify" and "when to enable it." That makes it difficult to reason about why a corruption check did or did not run, especially for remote compaction.
- **Where to look:** `include/rocksdb/advanced_options.h` `VerifyOutputFlags`, `db/compaction/compaction_job.cc` `CompactionJob::VerifyOutputFiles()`, `db/compaction/compaction_service_test.cc`
- **Suggested scope:** Expand the verification subsection in chapter 08 and the remote-compaction verification subsection in chapter 10. Cover the local/remote enable bits, the block-checksum vs iteration vs file-checksum modes, the `paranoid_file_checks` compatibility path, and the prerequisites for file-checksum verification.

### Per-key placement needs a scheduler-level explanation, not just an output-routing explanation
- **Why it matters:** The docs mostly describe per-key placement as "newer keys go to the proximal level," but the picker and overlap-detection code treat the proximal level as a second output destination with its own conflict rules. That affects whether a compaction is legal, whether proximal output is enabled, and whether older last-level data is pinned.
- **Where to look:** `db/compaction/compaction.cc` `EvaluateProximalLevel()`, `PopulateProximalLevelOutputRange()`, `db/compaction/compaction_picker.cc` `FilesRangeOverlapWithCompaction()`, `db/compaction/compaction_job.cc` proximal-range checks, `db/compaction/compaction_picker_test.cc`, `db/compaction/tiered_compaction_test.cc`
- **Suggested scope:** Add a dedicated subsection spanning chapters 06, 08, and 09, or a short standalone chapter if the team expects more tiered/per-key-placement work.

### FIFO kv-ratio compaction should document that fallback happens at runtime, not option validation time
- **Why it matters:** Operators can configure `use_kv_ratio_compaction` in combinations that pass validation and only fall back at runtime. That is an important operational detail when debugging why the old cost-based intra-L0 path was used.
- **Where to look:** `include/rocksdb/advanced_options.h` `CompactionOptionsFIFO::use_kv_ratio_compaction`, `db/compaction/compaction_picker_fifo.cc` runtime prerequisite checks, `db/compaction/compaction_picker_test.cc` `FIFOOptionValidation` and `FIFORatioBasedFallbackOnInvalidConfig`
- **Suggested scope:** Add one concise note in chapter 07 near the ratio-based algorithm prerequisites and fallback description.

## Depth Issues

### Output verification is described too generically
- **Current:** Chapter 08 says output verification is "controlled by `verify_output_flags` and legacy `paranoid_file_checks`."
- **Missing:** The doc does not explain that verification has two orthogonal dimensions: the verification type bits and the local/remote enable bits. It also omits the fact that file-checksum verification is skipped unless a checksum generator is configured and the output file recorded a file checksum.
- **Source:** `include/rocksdb/advanced_options.h` `VerifyOutputFlags`, `db/compaction/compaction_job.cc` `CompactionJob::VerifyOutputFiles()`

### Per-key placement is reduced to a single sequence-number threshold
- **Current:** Chapter 08 says "keys with `ikey.sequence > proximal_after_seqno_` go to the proximal level."
- **Missing:** That is only the first cut. `PopulateProximalLevelOutputRange()` computes a safe user-key range for proximal output, may disable proximal output when the last/proximal level layout is incompatible, and sets `keep_in_last_level_through_seqno_` to pin older last-level data. The docs should show that routing is constrained by both sequence number and safe output range.
- **Source:** `db/compaction/compaction.cc` `PopulateProximalLevelOutputRange()`, `OverlapProximalLevelOutputRange()`, `db/compaction/compaction_job.cc` proximal-range assertions

## Structure and Style Violations

### Inline code quotes are used throughout the entire doc set
- **File:** `docs/components/compaction/index.md` and essentially every chapter file
- **Details:** The prompt explicitly forbids inline code quotes, but the generated docs use backticks in headings, tables, prose, and bullets everywhere. This is systemic, not a one-off cleanup item.

### Several `Files:` lines omit major implementation files that the chapter relies on
- **File:** `docs/components/compaction/05_trivial_move.md`, `docs/components/compaction/07_fifo_compaction.md`, `docs/components/compaction/12_compaction_filter.md`
- **Details:** The trivial-move chapter omits `db/db_impl/db_impl_compaction_flush.cc`, which now owns the execution path. The FIFO chapter omits `db/column_family.cc` and `db/db_impl/db_impl_compaction_flush.cc`, even though it documents sanitized defaults and the temperature-change copy path. The compaction-filter chapter omits `db/flush_job.cc` even though it discusses flush filtering.

## Undocumented Complexity

### Proximal output is guarded by a computed safe range and last-level pinning
- **What it is:** Per-key placement is not just "newer seqnos go to the proximal level." `PopulateProximalLevelOutputRange()` computes a safe user-key interval for proximal output, may downgrade universal compactions from full-range to non-last-range behavior, and uses `keep_in_last_level_through_seqno_` to keep older data in the last level when moving it back up would be unsafe.
- **Why it matters:** Anyone debugging tiered compaction, range-delete behavior, or proximal-level conflicts will misread the system if they only know about `proximal_after_seqno_`.
- **Key source:** `db/compaction/compaction.cc`
- **Suggested placement:** Expand chapters 06 and 08, with a short cross-reference from chapter 09.

### FIFO temperature-change "trivial copy" is a real I/O path with separate observability behavior
- **What it is:** The FIFO temperature-change optimization allocates a new file number, opens a destination file with the target temperature, copies the SST with a configurable buffer, and installs new metadata. It also skips some of the normal table-file-creation listener behavior called out in comments.
- **Why it matters:** Readers otherwise expect a metadata-only operation and will underestimate the I/O cost and the monitoring implications.
- **Key source:** `db/db_impl/db_impl_compaction_flush.cc`
- **Suggested placement:** Extend chapter 07's temperature-change section.

### Verification behavior depends on both mode bits and compaction locality
- **What it is:** `VerifyOutputFlags` combines verification modes and enable conditions in one bitmask. Local and remote compactions can be verified differently, `paranoid_file_checks` expands into iteration verification for both, and file-checksum verification silently requires supporting checksum configuration.
- **Why it matters:** This is exactly the kind of corruption-debugging detail that maintainers need, and the current docs flatten it into a single sentence.
- **Key source:** `include/rocksdb/advanced_options.h`, `db/compaction/compaction_job.cc`
- **Suggested placement:** Chapter 08, with a short remote-compaction-specific note in chapter 10.

## Positive Notes

- `docs/components/compaction/index.md` follows the requested index pattern well: short overview, key source files, chapter table, characteristics, and invariants, all within the target line budget.
- The manual-compaction chapter usefully calls out the `exclusive_manual_compaction` plus `canceled` limitation and the pre-flush optimization when both range bounds are present; both match the current implementation.
- The docs do a good job splitting the topic by compaction style and by helper component instead of trying to force everything into a single monolithic chapter.
