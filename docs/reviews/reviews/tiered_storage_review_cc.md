# Review: tiered_storage -- Claude Code

## Summary
Overall quality rating: **good**

The tiered storage documentation is well-structured and covers the major subsystems comprehensively: temperature concepts, configuration, seqno-to-time mapping, per-key placement, TimedPut, universal/FIFO integration, filesystem integration, monitoring, and best practices. The writing is clear and generally accurate. The most significant issue is a factual error about `output_temperature_override` availability (it exists in both `CompactFiles()` and `CompactRange()`, not just `CompactFiles()`). There are also inaccuracies about seqno-to-time mapping storage scope and FIFO file age estimation. The documentation misses several recent features (kv-ratio compaction, `cf_allow_ingest_behind` interaction, `trivial_copy_buffer_size`, `metadata_write_temperature`/`wal_write_temperature`).

## Correctness Issues

### [WRONG] output_temperature_override claimed to be CompactFiles-only
- **File:** 02_temperature_configuration.md, "output_temperature_override" section
- **Claim:** "Available only through `CompactFiles()` (not `CompactRange()`)."
- **Reality:** `CompactRangeOptions` also has `output_temperature_override` (with the same type and default). Both `CompactFiles()` and `CompactRange()` support temperature override.
- **Source:** `include/rocksdb/options.h`, `struct CompactRangeOptions`, line ~2481
- **Fix:** Change to "Available through both `CompactFiles()` and `CompactRange()`. Forces all output files to the specified temperature, overriding all other rules."

### [WRONG] Seqno-to-time mapping claimed to only be stored in non-last-level SSTs
- **File:** 03_seqno_to_time_mapping.md, "Sequence Number Zeroing" section
- **Claim:** "The seqno-to-time mapping is only stored in non-last-level SST files (since last-level data with zeroed seqnos has no useful mapping)."
- **Reality:** `CompactionOutputs::Finish()` writes the seqno-to-time mapping to ALL output SST files regardless of level. It calls `relevant_mapping.CopyFromSeqnoRange()` and `builder_->SetSeqnoTimeTableProperties()` unconditionally. The mapping may be less useful for last-level files when seqnos are zeroed, but it is still stored.
- **Source:** `db/compaction/compaction_outputs.cc`, `CompactionOutputs::Finish()`
- **Fix:** Change to "The seqno-to-time mapping is stored in all output SST files. For last-level files where sequence numbers have been zeroed, the mapping entries may reference seqno 0 and provide limited value, but the mapping is still persisted."

### [MISLEADING] FIFO file age estimation description
- **File:** 07_fifo_temperature.md, "File Age Estimation" section
- **Claim:** "This method uses the file's table properties (`oldest_ancester_time` or `file_creation_time`) and optionally the previous file's creation time to estimate when the newest key was written."
- **Reality:** `TryGetNewestKeyTime()` first tries `newest_key_time` from table properties (via pinned reader). If unavailable, it falls back to `prev_file->TryGetOldestAncesterTime()`. The method does not use `file_creation_time` at all. `TryGetOldestAncesterTime()` checks `oldest_ancester_time` directly then from table properties.
- **Source:** `db/version_edit.h`, `FileMetaData::TryGetNewestKeyTime()` and `FileMetaData::TryGetOldestAncesterTime()`
- **Fix:** Change to "File age is estimated using `FileMetaData::TryGetNewestKeyTime()`. This method first tries the `newest_key_time` table property (a recently added property recording the newest key's write time). If unavailable, it falls back to the previous file's `oldest_ancester_time` as a proxy. Files without a valid time estimate are skipped."

### [MISLEADING] Sampling cadence formula description
- **File:** 03_seqno_to_time_mapping.md, "Sampling Cadence" section
- **Claim:** "cadence = min_preserve_seconds / kMaxSeqnoTimePairsPerCF"
- **Reality:** The actual formula uses ceiling division: `(min_preserve_seconds + kMaxSeqnoTimePairsPerCF - 1) / kMaxSeqnoTimePairsPerCF`. This rounds up to at least 1.
- **Source:** `db/seqno_to_time_mapping.h`, `MinAndMaxPreserveSeconds::GetRecodingCadence()`
- **Fix:** Show the ceiling division formula or note "rounded up to at least 1 second".

### [MISLEADING] PrepareTimes Step 4 description
- **File:** 03_seqno_to_time_mapping.md, "Compaction Integration" section
- **Claim:** "Step 4 -- The final threshold `proximal_after_seqno_` is set to `max(preclude_last_level_min_seqno, keep_in_last_level_through_seqno)`."
- **Reality:** The `keep_in_last_level_through_seqno` comes from `Compaction::GetKeepInLastLevelThroughSeqno()`, not a field called `keep_in_last_level_through_seqno`. The naming in the doc matches close enough but the doc omits the intermediate step where `preclude_last_level_min_seqno` is first potentially lowered to `earliest_snapshot_` (Step 3). Step 3 and Step 4 descriptions should be clearer that the snapshot adjustment happens to `preclude_last_level_min_seqno` before it is combined with the keep-in-last-level threshold.
- **Source:** `db/compaction/compaction_job.cc`, lines 395-412
- **Fix:** Clarify that the snapshot adjustment modifies `preclude_last_level_min_seqno` itself before Step 4.

### [MISLEADING] Compaction reason exclusion described differently
- **File:** 04_per_key_placement.md, "Conditions for Per-Key Placement" section, condition 6
- **Claim:** "Compaction reason: Not `kExternalSstIngestion` or `kRefitLevel`"
- **Reality:** The exclusion of these reasons is not inside `EvaluateProximalLevel()` itself. It is in the `Compaction` constructor where `EvaluateProximalLevel()` is bypassed entirely and `proximal_level_` is set to `kInvalidLevel` directly. The doc implies this check is part of `EvaluateProximalLevel()`.
- **Source:** `db/compaction/compaction.cc`, Compaction constructor, lines 346-353
- **Fix:** Note that this exclusion happens in the Compaction constructor before `EvaluateProximalLevel()` is called, not within the method itself.

## Completeness Gaps

### Missing: output_temperature_override in CompactRangeOptions
- **Why it matters:** Users of `CompactRange()` need to know they can override temperature, especially for manual temperature migration workflows.
- **Where to look:** `include/rocksdb/options.h`, `struct CompactRangeOptions`
- **Suggested scope:** Update chapter 2 to cover both `CompactFiles` and `CompactRange` temperature override.

### Missing: metadata_write_temperature and wal_write_temperature DB options
- **Why it matters:** These DB-level options control temperature for non-SST files (MANIFEST, WAL). Users doing full tiered storage need to know about them for complete temperature management.
- **Where to look:** `include/rocksdb/options.h`, `DBOptions`
- **Suggested scope:** Brief mention in chapter 2 or chapter 8 (filesystem integration).

### Missing: trivial_copy_buffer_size option
- **Why it matters:** Controls buffer size for trivial copy temperature changes in FIFO. Users enabling trivial copy need to tune this for large files.
- **Where to look:** `include/rocksdb/advanced_options.h`, `CompactionOptionsFIFO::trivial_copy_buffer_size`
- **Suggested scope:** Add to chapter 7 under the trivial copy section.

### Missing: cf_allow_ingest_behind interaction with tiered storage
- **Why it matters:** Both `cf_allow_ingest_behind` and `preclude_last_level_data_seconds` reserve the last level. The options comment explicitly mentions their interaction: "If `cf_allow_ingest_behind=true` or `preclude_last_level_data_seconds > 0`, then the last level is reserved". Users need to understand they share similar level reservation semantics.
- **Where to look:** `include/rocksdb/advanced_options.h`, `cf_allow_ingest_behind` comments and `level_compaction_dynamic_level_bytes` comments
- **Suggested scope:** Mention in chapter 4 or chapter 10 (best practices).

### Missing: newest_key_time table property
- **Why it matters:** This is a recently added table property (commit `af2a36d2c`, 2024) that improves FIFO temperature change accuracy. It's the primary source for `TryGetNewestKeyTime()` and replaces the older heuristic of using `oldest_ancester_time` from neighboring files.
- **Where to look:** `include/rocksdb/table_properties.h`, `TableProperties::newest_key_time`
- **Suggested scope:** Mention in chapter 7 (FIFO temperature) under file age estimation.

### Missing: kv-ratio FIFO compaction interaction with temperature
- **Why it matters:** The new `use_kv_ratio_compaction` FIFO algorithm (commit `b040ab83e`, 2024) introduces a different file selection strategy that interacts with temperature migration. Users using this new algorithm need to understand when temperature changes apply.
- **Where to look:** `db/compaction/compaction_picker_fifo.cc`, `PickRatioBasedIntraL0Compaction()`
- **Suggested scope:** Brief mention in chapter 7.

### Missing: FIFO intra-L0 compaction interaction
- **Why it matters:** FIFO compaction has intra-L0 compaction modes (`allow_compaction`, `use_kv_ratio_compaction`) that can merge files and reset temperature. The ordering of picks (size-based drop, then intra-L0, then temperature change) means temperature changes are lowest priority.
- **Where to look:** `db/compaction/compaction_picker_fifo.cc`, `PickCompaction()` pick ordering
- **Suggested scope:** Mention in chapter 7.

## Depth Issues

### Per-key placement FIXME about overlapping ranges
- **Current:** Chapter 4 describes the proximal output range mechanics
- **Missing:** There is an important FIXME in `PopulateProximalLevelOutputRange()`: "when last level's input range does not overlap with proximal level, and proximal level input is empty, this call will not set proximal_level_smallest_ or proximal_level_largest_. No keys will be compacted up." This is a known limitation that can silently prevent upward migration in certain scenarios.
- **Source:** `db/compaction/compaction.cc`, `PopulateProximalLevelOutputRange()`, lines 447-453

### TimedPut swap logic when ikey_.sequence == 0
- **Current:** Chapter 5 describes the swap mechanics
- **Missing:** When `ikey_.sequence == 0`, the swap skips the counter increment and keeps `ikey_.sequence = 0` (line 1029-1032 in compaction_iterator.cc). The `preferred_seqno` is asserted to be `< ikey_.sequence || ikey_.sequence == 0`. This edge case around seqno 0 is worth documenting since there's a FIXME about it causing issues with `cf_ingest_behind`.
- **Source:** `db/compaction/compaction_iterator.cc`, lines 986-990, 1029-1032

### Temperature migration during compaction-to-non-last-level
- **Current:** Chapter 2 describes temperature assignment for compaction output
- **Missing:** What happens when data moves from the last level back up during a compaction (e.g., due to `level_compaction_dynamic_level_bytes` changes or shrinking `num_levels`). The temperature changes from `last_level_temperature` to `default_write_temperature`. This migration path is implicit but worth calling out.
- **Source:** `db/compaction/compaction.cc`, `GetOutputTemperature()`

## Structure and Style Violations

### index.md at minimum line count
- **File:** index.md
- **Details:** At exactly 40 lines (the lower bound of the 40-80 range). This is borderline but acceptable.

### INVARIANT usage in index.md
- **File:** index.md, "Key Invariants" section
- **Details:** The section is titled "Key Invariants" and some items are true invariants (e.g., `preserve_time_min_seqno <= preclude_last_level_min_seqno` is enforced by assertion). Others like "Temperature precedence" are behavioral guarantees, not invariants that cause corruption if violated. Consider renaming to "Key Guarantees" or splitting into invariants and behavioral properties.

## Undocumented Complexity

### kChangeTemperature pick ordering in FIFO
- **What it is:** Temperature change compactions are picked last in the FIFO picker: `PickSizeCompaction()` -> `PickIntraL0Compaction()` -> `PickTemperatureChangeCompaction()`. If any earlier pick succeeds, temperature migration is skipped for that cycle.
- **Why it matters:** Users may observe delayed temperature migration under heavy write loads or when intra-L0 compaction is active. Understanding the priority helps debug why files aren't changing temperature as expected.
- **Key source:** `db/compaction/compaction_picker_fifo.cc`, `FIFOCompactionPicker::PickCompaction()`
- **Suggested placement:** Add to chapter 7, after the temperature change compaction flow description.

### FIFO concurrent compaction limitation nuance
- **What it is:** The doc correctly says FIFO doesn't support parallel compactions. However, the actual check is `!level0_compactions_in_progress_.empty()`, which means it's specifically checking for in-progress L0 compactions (not all compactions). For multi-level FIFO, this distinction could matter since compactions on other levels are tracked differently.
- **Why it matters:** Precision about the check helps understand edge cases in FIFO compaction scheduling.
- **Key source:** `db/compaction/compaction_picker_fifo.cc`, line 87 and 376
- **Suggested placement:** Minor clarification in chapter 7.

### Dynamic level bytes interaction with tiered storage
- **What it is:** When `level_compaction_dynamic_level_bytes = true` (the default), the base level is computed dynamically, which affects what "proximal level" means. The proximal level is always `num_levels - 2` regardless of dynamic level computation. If the base level is higher than the proximal level, there may be empty intermediate levels, and data flows from base level directly to the last level during compaction, skipping the proximal level entirely until enough data accumulates.
- **Why it matters:** Users with small databases may see data go directly to the last level without per-key placement splitting, because compactions don't reach the last level until enough data accumulates.
- **Key source:** `db/compaction/compaction.cc`, `EvaluateProximalLevel()`, and `db/version_set.cc` level target computation
- **Suggested placement:** Add to chapter 10 (best practices) as a pitfall.

### CompactRange temperature override added recently
- **What it is:** Support for `output_temperature_override` in `CompactFiles` was expanded to also work in `CompactRange` (via `CompactRangeOptions`). This allows manual temperature migration using `CompactRange()`.
- **Why it matters:** This significantly improves the migration story for existing databases - users can use `CompactRange()` with temperature override to force-migrate files.
- **Key source:** `include/rocksdb/options.h`, `CompactRangeOptions::output_temperature_override`
- **Suggested placement:** Update chapter 2 and chapter 10 (migration strategy).

## Positive Notes

- **Excellent structure:** The 10-chapter breakdown follows data flow logically: concept -> config -> mapping -> placement -> API -> compaction styles -> filesystem -> monitoring -> best practices. This matches how a developer would learn the system.
- **Per-key placement chapter (04) is strong:** Accurately describes `EvaluateProximalLevel()`, `PopulateProximalLevelOutputRange()`, dual output management, and the threshold computation. The proximal output range type table is particularly helpful.
- **Best practices chapter (10) is practical:** Migration strategy, common pitfalls, and the `TimedPut` bulk loading example are genuinely useful for operators.
- **Temperature precedence is correctly documented:** The three-tier precedence in `GetOutputTemperature()` (override > last_level > default_write) matches the code exactly.
- **Snapshot interaction in PrepareTimes is correctly captured:** The heuristic of treating snapshot data as hot is accurately described.
- **Files lines are present in every chapter** with correct paths.
- **No box-drawing characters, no line number references, no inline code quotes** -- style guidelines are well followed.
