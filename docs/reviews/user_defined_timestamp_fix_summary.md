# Fix Summary: user_defined_timestamp

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 14 |
| Completeness | 2 |
| Structure/style | 2 |
| Total | 18 |

## Disagreements Found

3 disagreements documented in `user_defined_timestamp_debates.md`:
1. Seq/ts ordering: invariant vs application requirement (Codex correct)
2. IncreaseFullHistoryTsLow error: both reviewers partially right, code has two error paths
3. WriteBatch::Put(cf, key, value) semantics: Codex more precise about placeholder mechanism

## Changes Made

| File | Changes |
|------|---------|
| `01_key_encoding_and_comparator.md` | Reworded seq/ts ordering from "engine-maintained invariant" to "application-level requirement not enforced by the engine" |
| `02_write_path.md` | (1) Rewrote WriteBatch section to show both explicit-timestamp and deferred-timestamp forms for all operations. (2) Fixed DeleteRange value type from `kTypeDeletionWithTimestamp` to `kTypeRangeDeletion`. (3) Updated WBWI to document Put/Delete/SingleDelete support with timestamps, Merge unsupported. |
| `03_read_path.md` | (1) Fixed Seek() to use read timestamp (`ReadOptions::timestamp`), not max timestamp. (2) Fixed SeekForPrev() to document conditional behavior based on `iter_start_ts` and `iterate_upper_bound`. (3) Fixed option name from `auto_readahead_size` to `auto_refresh_iterator_with_snapshot`. |
| `04_compaction_and_gc.md` | (1) Fixed IncreaseFullHistoryTsLow to document both `InvalidArgument` and `TryAgain` error paths. (2) Changed "at bottommost level" to "no older versions beyond output level". (3) Clarified GetNewestUDT does not scan per-file timestamp metadata. |
| `05_flush_and_persistence.md` | Clarified `newest_udt_` concurrency: updated to describe the `allow_concurrent` parameter (not the DB option) and that the incompatibility is enforced by open-time option validation. |
| `06_recovery_and_wal_replay.md` | Fixed dropped-CF entries: they are only copied if a rebuilt batch is otherwise required, not always. |
| `07_migration_and_compatibility.md` | (1) Added row for allowed persist-flag toggle when ts_sz=0. (2) Replaced `has_no_udt` bit description with actual mechanism: boundary rewriting and `user_defined_timestamps_persisted=false`. (3) Removed false claim that SST timestamp metadata is used by `GetNewestUserDefinedTimestamp()`. |
| `08_transaction_integration.md` | (1) Fixed GetForUpdate to document validate/substitute behavior instead of "ignores". (2) Fixed SetCommitTimestamp ordering — no enforcement after Prepare. (3) Fixed `write_batch_track_timestamp_size` from `TransactionDBOptions` to `TransactionOptions`. (4) Scoped `enable_udt_validation` description to sanity checks and conflict detection. (5) Added missing Files entries. |
| `09_tools_and_testing.md` | (1) Documented db_bench 8-byte-only timestamp support. (2) Documented TimestampEmulator::GetTimestampForRead advancing the counter. (3) Replaced generic stress test bullet list with disabled-features tables. (4) Added missing Files entries. |
| `10_best_practices.md` | Changed BlobDB from "Yes" to "Partial" with note about unsupported timestamp-returning `Get`/`MultiGet`. |
| `index.md` | (1) Reworded seq/ts ordering as application-level requirement. (2) Qualified persist-flag toggle restriction to ts_sz > 0 only. |
