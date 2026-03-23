# Fix Summary: db_impl

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 19 |
| Structure/Style | 6 |
| Total | 25 |

## Disagreements Found

3 disagreements documented in `db_impl_debates.md`:
1. **Inline code quotes** — style interpretation difference (CC correct, no change)
2. **SuperVersion thread-local mechanism** — CC missed this, Codex correct (fixed)
3. **Close/snapshot status handling** — CC missed this, Codex correct (fixed)

## Changes Made

### `index.md`
- Removed non-invariant "Checksum on compressed data" from Key Invariants
- Fixed Key Characteristics: "version-number staleness detection" -> "sentinel-swap staleness detection"

### `01_overview.md`
- Fixed CompactedDBImpl description: "no L0 files" -> "all data in a single level"

### `02_db_open.md`
- Fixed WAL recovery default: `kPointInTimeRecovery` is the default, not `kTolerateCorruptedTailRecords`
- Added option header path references to SanitizeOptions and Speed-Up tables

### `03_db_close.md`
- Rewrote Snapshot Safety Check section: described as part of `DBImpl::Close()` and destructor entry path, not wrapper-only
- Separated Aborted-to-Incomplete conversion: applies to `CloseHelper()` errors, not snapshot check

### `04_column_families.md`
- Fixed CF drop removal timing: `SetDropped()` removes from `ColumnFamilySet` immediately, destructor handles linked-list cleanup
- Fixed atomic flush: not all CFs participate in every flush, only eligible ones selected by `SelectColumnFamiliesForAtomicFlush()`
- Removed unverifiable `disallow_memtable_writes` restriction note
- Added option header path references to CF config and write stall tables

### `05_version_management.md`
- Fixed LogAndApply: mutex is released during MANIFEST I/O, not held
- Rewrote thread-local caching: removed version_number comparison, described actual sentinel-swap protocol (`kSVInUse`/`kSVObsolete`)

### `06_write_path.md`
- Fixed process-crash durability claim: qualified with `manual_wal_flush` caveat
- Fixed unordered_write: affects snapshot-based `Get`, `MultiGet`, and iterators (not just iterators)
- Added option header path reference to write stall section

### `07_read_path.md`
- Removed hazard pointer claim, replaced with thread-local SuperVersion caching + ref counting
- Rewrote SuperVersion acquisition step: described actual sentinel-swap protocol
- Fixed GetSnapshot: uses `GetLastPublishedSequence()`, not `LastSequence()`
- Fixed iterator reference mechanism: holds SuperVersion -> Version chain, not direct SST file ref counts
- Fixed row cache key format: `(cache_id, file_number, seq_no, user_key)`, not `(file_number, user_key)`
- Fixed DeleteRange + row_cache: behavioral limitation, not write-time validation
- Fixed secondary instance GetImpl: uses `versions_->LastSequence()`, does not honor `ReadOptions::snapshot`

### `08_flush_compaction_scheduling.md`
- Fixed WBM flush CF selection: oldest mutable memtable (smallest creation sequence), not largest
- Fixed compaction scheduling limit: includes `bg_bottom_compaction_scheduled_` in the sum
- Fixed `bg_work_paused_`: means "background work is paused", not "DB is being deleted"

### `09_background_error_handling.md`
- Fixed `recovery_thread_` type: `unique_ptr<port::Thread>`, not `unique_ptr<Thread>`
- Fixed listener callback: `OnBackgroundError()` only modifies status; `OnErrorRecoveryBegin()` controls auto-recovery

### `10_secondary_and_readonly.md`
- Fixed CompactedDBImpl eligibility: added actual level-layout constraints (single L0 file accepted)
- Fixed secondary read path: described snapshot restrictions (ignores `ReadOptions::snapshot`, iterators reject snapshot and tailing)
