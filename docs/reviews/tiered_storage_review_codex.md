# Review: tiered_storage -- Codex

## Summary
Overall quality rating: **significant issues**

The chapter layout is good and the docs cover the right major topics: temperature concepts, option semantics, seqno-to-time mapping, per-key placement, TimedPut, FIFO/universal behavior, FileSystem integration, and monitoring. The material is also readable. The problem is that several of the most operationally important statements are wrong or too simplified to trust, especially at subsystem boundaries.

The highest-value fixes are in four areas: read-path temperature propagation, per-temperature accounting semantics, TimedPut's actual on-disk/on-compaction lifecycle, and the seqno-to-time mapping lifecycle. The docs also omit important option sanitization and newer cross-component behavior around ingestion, DB-level temperature knobs, and dynamic enablement of time tracking.

## Correctness Issues

### [WRONG] Read-path temperature is documented as flowing through `IOOptions`
- **File:** `01_temperature_concept.md`, "Temperature as a Hint" section; `08_filesystem_integration.md`, "File Reading" section
- **Claim:** "The FileSystem receives temperature through `FileOptions::temperature` at file creation time, and through `IOOptions` during read operations." Also: "Temperature is available during read operations through `IOOptions` and through the `FileMetaData::temperature` field when opening table readers."
- **Reality:** `IOOptions` has no temperature field. For SST reads, RocksDB passes temperature through `FileOptions::temperature` when opening the file and then carries it in `RandomAccessFileReader` / `FileOperationInfo`. `TableCache` also applies `default_temperature` there when manifest metadata is `kUnknown`.
- **Source:** `include/rocksdb/file_system.h` `struct IOOptions`; `include/rocksdb/file_system.h` `struct FileOptions`; `db/table_cache.cc`; `file/random_access_file_reader.cc`
- **Fix:** Rewrite the read-path explanation to say temperature reaches the FileSystem through `FileOptions::temperature` when opening table files, is surfaced to listeners via `FileOperationInfo::temperature`, and can be queried from open file objects with `GetTemperature()` when supported.

### [WRONG] Per-temperature IOStats are documented as tracking writes
- **File:** `08_filesystem_integration.md`, "I/O Statistics by Temperature" section; `09_monitoring.md`, "I/O Statistics by Temperature" section
- **Claim:** "`IOStatsContext` ... records bytes read and written per temperature tier." Also: "`IOStatsContext` ... tracks bytes read and written broken down by file temperature."
- **Reality:** `IOStatsContext` only defines per-temperature read counters and read counts. I did not find any write-by-temperature counters in `IOStatsContext`, and the recording in `RandomAccessFileReader` is read-only.
- **Source:** `include/rocksdb/iostats_context.h` `struct FileIOByTemperature`; `file/random_access_file_reader.cc` `RecordIOStats()`
- **Fix:** Change these sections to say RocksDB tracks per-temperature reads, not writes, and point readers to the exact counters/tickers that exist.

### [MISLEADING] `default_temperature` is documented as affecting perf context
- **File:** `02_temperature_configuration.md`, "default_temperature" section
- **Claim:** "This option only affects read-path I/O accounting (statistics, perf context)."
- **Reality:** The fallback temperature is applied when opening SST readers for files whose manifest temperature is `kUnknown`, and it feeds read-by-temperature IOStats/tickers. I did not find a temperature-specific field in `PerfContext`.
- **Source:** `db/table_cache.cc`; `file/random_access_file_reader.cc` `RecordIOStats()`; `include/rocksdb/perf_context.h`
- **Fix:** Say that `default_temperature` affects read-path temperature attribution for statistics/tickers/IOStats only; remove the perf-context claim unless a real `PerfContext` field is added.

### [WRONG] TimedPut internal representation is described as storing preferred seqno at write time
- **File:** `05_timed_put.md`, "Internal Representation: kTypeValuePreferredSeqno" section
- **Claim:** "The value is packed as: `[original_value | preferred_seqno (varint)]`" and "Where `preferred_seqno` is derived from the provided `write_unix_time` using the seqno-to-time mapping."
- **Reality:** A `TimedPut` in the write batch is first encoded as value plus a trailing fixed-width `write_unix_time`, via `PackValueAndWriteTime()`. During flush, RocksDB may rewrite it to value plus preferred seqno, or may downgrade it to plain `kTypeValue` if no useful preferred seqno can be derived. Compaction then works with the preferred-seqno form.
- **Source:** `db/write_batch.cc` `WriteBatchInternal::TimedPut()`; `db/builder.cc`; `db/seqno_to_time_mapping.h`
- **Fix:** Document the two-stage lifecycle explicitly: write batch stores unix write time, flush may translate that to preferred seqno, and compaction may then swap sequence numbers under safety checks.

### [WRONG] Seqno-to-time mapping storage format is described incorrectly
- **File:** `03_seqno_to_time_mapping.md`, "Storage Format" section
- **Claim:** "1. First pair stored as raw (seqno, time) using varint encoding 2. Subsequent pairs stored as deltas from the previous pair"
- **Reality:** The encoding is `[count][delta pair 1][delta pair 2]...`, where every stored pair is delta-encoded relative to a running base that starts at zero. There is no separately encoded raw first pair.
- **Source:** `db/seqno_to_time_mapping.cc` `EncodeTo()` and `DecodeImpl()`
- **Fix:** Replace the format description with the actual count-plus-delta-stream encoding.

### [WRONG] Sampling cadence formula omits the code's round-up behavior
- **File:** `03_seqno_to_time_mapping.md`, "Sampling Cadence" section
- **Claim:** "cadence = min_preserve_seconds / kMaxSeqnoTimePairsPerCF"
- **Reality:** The code uses ceiling division: `(min_preserve_seconds + kMaxSeqnoTimePairsPerCF - 1) / kMaxSeqnoTimePairsPerCF`. This matters whenever the preserve window is not an exact multiple of 100, and it guarantees at least one-second cadence when enabled.
- **Source:** `db/seqno_to_time_mapping.h` `MinAndMaxPreserveSeconds::GetRecodingCadence()`
- **Fix:** Show the exact ceiling-division formula and note the minimum-one-second behavior.

### [WRONG] Seqno-to-time mappings are documented as non-last-level-only
- **File:** `03_seqno_to_time_mapping.md`, "Sequence Number Zeroing" section
- **Claim:** "The seqno-to-time mapping is only stored in non-last-level SST files (since last-level data with zeroed seqnos has no useful mapping)."
- **Reality:** Flush and compaction write seqno-to-time properties to output SSTs regardless of level. Last-level files can retain non-empty mappings until later compactions zero enough sequence numbers that the mapping loses value.
- **Source:** `db/builder.cc`; `db/compaction/compaction_outputs.cc` `CompactionOutputs::Finish()`; `db/seqno_time_test.cc`
- **Fix:** Explain that mappings can still be persisted in last-level SSTs, but seqno zeroing progressively reduces their usefulness.

### [MISLEADING] FIFO file age estimation describes the wrong fallback data
- **File:** `07_fifo_temperature.md`, "File Age Estimation" section
- **Claim:** "This method uses the file's table properties (`oldest_ancester_time` or `file_creation_time`) and optionally the previous file's creation time to estimate when the newest key was written."
- **Reality:** `TryGetNewestKeyTime()` first uses the `newest_key_time` table property if available. If that is unavailable, it falls back to the previous newer file's oldest ancestor time via `prev_file->TryGetOldestAncesterTime()`. This is not a `file_creation_time` fallback.
- **Source:** `db/version_edit.h` `FileMetaData::TryGetNewestKeyTime()` and `TryGetOldestAncesterTime()`; `db/compaction/compaction_picker_fifo.cc` `PickTemperatureChangeCompaction()`
- **Fix:** Reframe this section around `newest_key_time` as the primary source and previous-file oldest-ancestor time as the fallback heuristic.

### [WRONG] FIFO threshold misordering is documented as undefined behavior
- **File:** `10_best_practices.md`, "FIFO threshold ordering" subsection
- **Claim:** "Violating this causes undefined behavior in temperature assignment."
- **Reality:** RocksDB validates this configuration and returns `Status::NotSupported` during DB open or `SetOptions()`. This is a checked configuration error, not undefined behavior.
- **Source:** `db/column_family.cc`; `db/db_options_test.cc`
- **Fix:** Replace the warning with the actual behavior: the DB rejects unsorted thresholds with `NotSupported`.

### [MISLEADING] Temperature immutability is stated too absolutely
- **File:** `01_temperature_concept.md`, "Temperature Persistence" section; `index.md`, "Key Characteristics" section
- **Claim:** "Important: A file's temperature is immutable after creation. Changing a file's temperature requires rewriting the file..." Also: "**Temperature immutability**: A file's temperature is set at creation and persisted in MANIFEST; changing requires rewriting the file"
- **Reality:** Rewriting is the normal RocksDB-managed migration path, but it is not the only way RocksDB can change its recorded temperature metadata. `experimental::UpdateManifestForFilesState()` can repair manifest temperature metadata to match external FileSystem state for existing files.
- **Source:** `include/rocksdb/experimental.h` `UpdateManifestForFilesState()`; `db/experimental.cc` `UpdateManifestForFilesState()`
- **Fix:** Qualify the statement: temperature is normally assigned at file creation and changed by rewrite, but manifest metadata can also be repaired to match externally moved files.

### [MISLEADING] Universal size-amp exclusion is presented as unconditional once preclude is enabled
- **File:** `10_best_practices.md`, "Size amplification with tiered storage" subsection; `index.md`, "Key Invariants" section
- **Claim:** "When `preclude_last_level_data_seconds > 0` with universal compaction, size amplification calculation automatically excludes the last level." Also: "When `preclude_last_level_data_seconds > 0`, universal compaction size amplification calculation excludes the last level"
- **Reality:** The last sorted run is skipped only when all of the code's predicate holds: preclude is enabled, `num_levels > 2`, the oldest sorted run is actually at the last level, and there is more than one sorted run.
- **Source:** `db/compaction/compaction_picker_universal.cc` `ShouldSkipLastSortedRunForSizeAmpCompaction()`
- **Fix:** Replace the unconditional wording with the full predicate or explicitly say this optimization applies only when the last sorted run is present and there are other sorted runs above it.

### [MISLEADING] Tombstone placement is oversimplified and partly wrong
- **File:** `04_per_key_placement.md`, "Tombstone Placement" section
- **Claim:** "Tombstones (deletion markers) are placed based on the same sequence number threshold as any other key. Since tombstones are always recently written ... they naturally end up in the proximal (hot) level."
- **Reality:** Point tombstones follow the same seqno-threshold routing as other point entries, but range tombstones are split later with per-output sequence-range filtering. The blanket statement that tombstones naturally end up in the proximal level is not true for older tombstones, and it hides the separate range-delete path.
- **Source:** `db/compaction/compaction_job.cc` `ProcessKeyValueCompaction()` and range-delete handling in `FinishCompactionOutputFile()`; `db/compaction/tiered_compaction_test.cc`
- **Fix:** Split this section into point tombstones vs. range tombstones, and document that range deletions are filtered separately for proximal and last-level outputs.

### [UNVERIFIABLE] `kDisabled` is documented as a normal proximal-output-range state
- **File:** `04_per_key_placement.md`, "Proximal Output Range" table
- **Claim:** "`kDisabled | No keys can go to proximal level | When conditions are not met`"
- **Reality:** I found the enum value, stringification, and logging for `kDisabled`, but I did not find code assigning that state. "Conditions are not met" currently maps to `kNotSupported`, not `kDisabled`.
- **Source:** `db/compaction/compaction.h` `enum class ProximalOutputRangeType`; `db/compaction/compaction.cc` `PopulateProximalLevelOutputRange()`; `db/compaction/compaction_job.cc`; repo-wide search for `ProximalOutputRangeType::kDisabled`
- **Fix:** Either document `kDisabled` as reserved/unimplemented today or remove it from the behavioral table until code actually produces it.

## Completeness Gaps

### Missing: External SST ingestion temperature rules
- **Why it matters:** Ingestion is a common migration path into tiered storage, and the temperature outcome is not obvious. Copied files use `last_level_temperature` only for `ingest_behind` or `fail_if_not_bottommost_level`; otherwise they use `default_write_temperature` even if they land in the last level. Moved files inherit the source file temperature.
- **Where to look:** `include/rocksdb/db.h`; `db/external_sst_file_ingestion_job.cc`; `db/external_sst_file_basic_test.cc`
- **Suggested scope:** Add a short subsection to chapter 2 or chapter 8, with a cross-reference from best practices.

### Missing: DB-level temperature options for non-SST files
- **Why it matters:** The docs talk about temperature as if it is only an SST concern, but `metadata_write_temperature` and `wal_write_temperature` are part of the operational story for full tiered deployments.
- **Where to look:** `include/rocksdb/options.h`; `db/db_impl/db_impl_open.cc`
- **Suggested scope:** Mention in chapter 2 and chapter 8.

### Missing: Option sanitization and validation rules
- **Why it matters:** The current docs describe the knobs but not the code's guardrails. Important behavior is missing: `last_level_temperature` is sanitized away for FIFO, `preserve_internal_time_seconds` / `preclude_last_level_data_seconds` are ignored in read-only DBs, and `file_temperature_age_thresholds` is rejected unless FIFO is single-level and the ages are strictly increasing.
- **Where to look:** `db/column_family.cc`; `db/db_options_test.cc`
- **Suggested scope:** Add a validation matrix to chapter 2 and chapter 7, and mirror the operational pitfalls in chapter 10.

### Missing: TimedPut limitations in WriteBatchWithIndex and transaction-style paths
- **Why it matters:** The current chapter reads like `TimedPut` is a general write API. In practice, `WriteBatchWithIndex` rejects it up front, and WBWI rebuild/indexing paths treat TimedPut tags as corruption because transaction APIs do not support them.
- **Where to look:** `include/rocksdb/utilities/write_batch_with_index.h`; `utilities/write_batch_with_index/write_batch_with_index.cc`
- **Suggested scope:** Expand chapter 5's limitations subsection.

### Missing: Readback and observability APIs for age/temperature
- **Why it matters:** The docs stop short of the APIs operators and test authors actually use to confirm tiering behavior. Important omissions include `rocksdb.iterator.write-time`, `DB::Properties::kLiveSstFilesSizeAtTemperature`, and the FIFO change-temperature ticker.
- **Where to look:** `include/rocksdb/iterator.h`; `include/rocksdb/db.h`; `include/rocksdb/statistics.h`; `monitoring/statistics.cc`; `db/compaction/tiered_compaction_test.cc`; `db/db_test2.cc`
- **Suggested scope:** Expand chapter 9, with brief cross-references from chapters 3 and 5.

### Missing: `CompactForTieringCollector`
- **Why it matters:** RocksDB has a built-in table-properties collector specifically for tiering workloads. It can count entries already eligible for last-level placement and mark files as needing compaction. That is exactly the kind of helper a tiered-storage guide should mention.
- **Where to look:** `include/rocksdb/utilities/table_properties_collectors.h`; `utilities/table_properties_collectors/compact_for_tiering_collector.cc`; `db/compaction/tiered_compaction_test.cc`
- **Suggested scope:** Add a brief note in chapter 10 or a small new subsection near chapters 4 and 5.

## Depth Issues

### Seqno-to-time mapping lifecycle is more complex than the current binary story
- **Current:** Chapter 3 jumps from "stored in SST property" to "not stored in last level once seqnos are zeroed."
- **Missing:** The real lifecycle is staged: mappings are propagated into output SSTs, can survive compaction into the last level, and only become less useful as seqno zeroing progresses over later bottommost compactions. The docs need that middle state, not just the endpoints.
- **Source:** `db/builder.cc`; `db/compaction/compaction_outputs.cc`; `db/seqno_time_test.cc`

### Tombstone, snapshot, and TimedPut interactions need a real edge-case section
- **Current:** Chapter 4 has a short snapshot paragraph and a simplistic tombstone paragraph.
- **Missing:** The subtle behavior is in the interactions: snapshot heuristics lower the last-level cutoff, range tombstones are split separately per output level, and TimedPut preferred-seqno swapping has an extra range-tombstone safety check before it is allowed.
- **Source:** `db/compaction/compaction_job.cc`; `db/compaction/compaction_iterator.cc`; `db/compaction/tiered_compaction_test.cc`

### FIFO temperature migration chapter omits compaction pick ordering
- **Current:** Chapter 7 explains how a file is chosen for temperature change once the picker is in that mode.
- **Missing:** Temperature-change compactions are not first-class or immediate. FIFO first considers size-based deletion and intra-L0 compaction, and only then attempts temperature change; it also changes at most one file per cycle. That priority order affects how quickly files age through tiers in practice.
- **Source:** `db/compaction/compaction_picker_fifo.cc`

## Structure and Style Violations

### Inline code quoting is pervasive across the corpus
- **File:** `index.md` and every chapter file
- **Details:** The docs rely heavily on backticked identifiers in prose, tables, and bullet lists. The review prompt's house style explicitly forbids inline code quotes in these component docs.

### "Key Invariants" in `index.md` mixes invariants with ordinary behavior
- **File:** `index.md`
- **Details:** Items like temperature precedence and universal size-amp exclusion are behavioral rules or conditional behavior, not correctness invariants of the "corruption/crash if violated" kind. The title should be narrowed or the list split.

### Several `Files:` lines omit primary implementation files used by the chapter
- **File:** `05_timed_put.md`; `08_filesystem_integration.md`; `01_temperature_concept.md`
- **Details:** `05_timed_put.md` does not list `db/write_batch.cc` or `db/builder.cc` even though the chapter depends on both. `08_filesystem_integration.md` omits `db/table_cache.cc` and `file/random_access_file_reader.cc`, which are central to the read-path discussion. `01_temperature_concept.md` discusses manifest persistence via `FileMetaData::temperature` but does not list `db/version_edit.h`.

## Undocumented Complexity

### Seqno-to-time bootstrap and publication model
- **What it is:** New DBs opened with preserve/preclude enabled pre-allocate sequence numbers and prepopulate historical mappings during `DB::Open()`. Later enablement does not do that; it only ensures a recent sample exists and then publishes immutable mapping snapshots through SuperVersions to enabled column families.
- **Why it matters:** This is the real explanation behind rollout behavior, dynamic enablement behavior, and the thread-safety story for concurrent readers using SuperVersions.
- **Key source:** `db/db_impl/db_impl_open.cc`; `db/db_impl/db_impl.cc` `EnsureSeqnoToTimeMapping()`, `PrepopulateSeqnoToTimeMapping()`, `InstallSuperVersionForConfigChange()`, `RecordSeqnoToTimeMapping()`
- **Suggested placement:** Expand chapter 3 and chapter 10.

### `rocksdb.iterator.write-time` semantics
- **What it is:** Iterators can expose an approximate write time per entry, but the property has sentinel semantics: `max uint64` means unknown and `0` is returned once sequence numbers have been zeroed out in last-level data.
- **Why it matters:** This is one of the best debugging tools for tiered placement, but only if readers understand what the sentinel values mean and why the value is a lower bound rather than an exact write timestamp.
- **Key source:** `include/rocksdb/iterator.h`; `db/db_iter.cc`; `db/compaction/tiered_compaction_test.cc`
- **Suggested placement:** Chapter 9, with a short mention in chapters 3 and 5.

### FIFO temperature migration can lag far behind thresholds
- **What it is:** The picker considers temperature change only after other FIFO compaction work, and then changes just one file per pass.
- **Why it matters:** Users can set thresholds correctly and still observe slow migration under sustained write load or active intra-L0 compaction. Without this detail, the docs over-promise the responsiveness of age-based movement.
- **Key source:** `db/compaction/compaction_picker_fifo.cc`
- **Suggested placement:** Chapter 7.

### `CompactForTieringCollector` is TimedPut-aware
- **What it is:** The collector does not just count raw sequence numbers. For TimedPut entries it extracts the packed preferred sequence number so that eligibility for last-level placement is computed using tiering semantics rather than the raw batch seqno.
- **Why it matters:** This makes the collector substantially more useful for historical backfill workloads, and it is exactly the sort of subtle integration a tiered-storage guide should surface.
- **Key source:** `utilities/table_properties_collectors/compact_for_tiering_collector.cc`
- **Suggested placement:** Add a short subsection near chapters 4 and 5, and reference it from chapter 10.

## Positive Notes

- The chapter breakdown is sensible and easy to navigate. Concept, configuration, placement, API, compaction-style integrations, FileSystem integration, monitoring, and best practices are the right slices.
- The docs correctly call out that temperature is advisory and FileSystem-dependent. That is the right conceptual starting point.
- The temperature precedence description for normal compaction output is mostly aligned with `Compaction::GetOutputTemperature()`, including the proximal-output exception in chapter 2.
- The current best-practices section is directionally useful, especially the migration advice and the operational framing around `kUnknown` vs. explicit temperatures.
