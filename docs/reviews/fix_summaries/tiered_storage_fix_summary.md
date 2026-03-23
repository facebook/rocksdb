# Fix Summary: tiered_storage

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 10 |
| Completeness | 7 |
| Structure/style | 3 |
| Total | 20 |

## Disagreements Found

3 disagreements documented in `tiered_storage_debates.md`:

1. **output_temperature_override in CompactRangeOptions** -- CC incorrectly claimed it exists in `CompactRangeOptions`. Code shows it only exists in `CompactionOptions` (for `CompactFiles()`). Original doc was correct; CC's suggestion was rejected. (high risk)
2. **FIFO threshold misordering** -- Codex correctly identified that RocksDB validates and returns `Status::NotSupported`, not undefined behavior as the doc claimed. (medium risk)
3. **Seqno-to-time encoding format** -- Codex mostly right about delta-from-zero encoding but incorrect about a count prefix. Updated to describe the actual algorithm accurately. (low risk)

## Changes Made

| File | Changes |
|------|---------|
| `index.md` | Renamed "Key Invariants" to "Key Invariants and Guarantees"; separated true invariant (INVARIANT tag) from behavioral guarantees; softened temperature immutability language; qualified universal size-amp exclusion; removed IOOptions from ch8 summary |
| `01_temperature_concept.md` | Added `db/version_edit.h` to Files line; removed IOOptions claim from read path; softened temperature immutability to mention `UpdateManifestForFilesState()` |
| `02_temperature_configuration.md` | Replaced "perf context" with "IOStatsContext" for `default_temperature`; added `metadata_write_temperature` and `wal_write_temperature` section |
| `03_seqno_to_time_mapping.md` | Fixed sampling cadence to show ceiling division formula; fixed storage format to describe delta-from-zero encoding; clarified PrepareTimes Step 3/4 snapshot adjustment order; fixed seqno-to-time mapping to say it is stored in ALL SSTs (not just non-last-level) |
| `04_per_key_placement.md` | Clarified compaction reason exclusion happens in Compaction constructor (not EvaluateProximalLevel); marked kDisabled as defined but not currently assigned; split tombstone placement into point vs. range tombstones |
| `05_timed_put.md` | Added `db/write_batch.cc` and `db/builder.cc` to Files line; documented two-stage lifecycle (write_unix_time at write batch, preferred_seqno at flush); added WriteBatchWithIndex rejection limitation |
| `06_universal_compaction.md` | Qualified size-amp exclusion as conditional (all predicates must hold) |
| `07_fifo_temperature.md` | Added `db/version_edit.h` to Files line; fixed file age estimation to describe `newest_key_time` primary and prev-file `oldest_ancester_time` fallback; added compaction pick ordering section; added `trivial_copy_buffer_size` mention |
| `08_filesystem_integration.md` | Added `db/table_cache.cc` and `file/random_access_file_reader.cc` to Files line; rewrote read-path temperature to use `FileOptions` (not `IOOptions`); fixed IOStatsContext to say reads only (not writes) |
| `09_monitoring.md` | Fixed IOStatsContext to say reads only (not writes); added `CompactForTieringCollector` section |
| `10_best_practices.md` | Fixed FIFO threshold ordering to say `Status::NotSupported` (not undefined behavior); qualified universal size-amp exclusion conditions |
