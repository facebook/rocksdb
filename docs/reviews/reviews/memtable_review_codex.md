# Review: memtable — Codex

## Summary
Overall quality rating: needs work.

The doc set is well organized and mostly follows the expected component-doc pattern. The index is within the target size, the chapter split is sensible, and the InlineSkipList and arena sections provide useful orientation for someone entering the codebase.

The main problem is precision at subsystem boundaries. Several important behaviors are described more simply than the code actually implements, and some of those simplifications cross the line into being wrong: immutable-memtable flush ordering, concurrent-write validation, VectorRep read/sort behavior, generic MultiGet finger-search claims, and range-tombstone flush handling. The configuration coverage is also incomplete in the places where RocksDB sanitizes or rewrites user options, which makes the docs unreliable for debugging real-world configurations.

## Correctness Issues

### [WRONG] The docs overstate FIFO flush ordering
- **File:** `docs/components/memtable/index.md` / Key Invariants; `docs/components/memtable/08_immutable_list.md` / Flush Ordering
- **Claim:** "Immutable memtables flushed in FIFO order (oldest first) and committed to the manifest in that order" and "Immutable memtables are flushed in FIFO order (oldest first)."
- **Reality:** The code guarantees manifest commit/removal order, not strict FIFO execution of the flush work itself. `PickMemtablesToFlush()` picks oldest not-yet-in-progress memtables, but multiple flushes can run concurrently, and `RollbackMemtableFlush()` can reopen younger completed memtables if an older batch rolls back. `TryInstallMemtableFlushResults()` is the part that enforces creation-order commit from the oldest completed memtable forward.
- **Source:** `db/memtable_list.cc` `PickMemtablesToFlush`; `db/memtable_list.cc` `RollbackMemtableFlush`; `db/memtable_list.cc` `TryInstallMemtableFlushResults`
- **Fix:** Say immutable memtables are selected starting from the oldest eligible memtable and their results are committed/removed in creation order, while SST writing can overlap or be retried out of order.

### [WRONG] Unsupported concurrent-write memtable reps do not fall back to serialized insertion
- **File:** `docs/components/memtable/09_concurrent_writes.md` / Concurrent Insert Requirements
- **Claim:** "When the configured representation does not support concurrent insert (i.e., `MemTableRepFactory::IsInsertConcurrentlySupported()` returns false), the write path falls back to serialized memtable insertion even if `allow_concurrent_memtable_write` is true."
- **Reality:** The configuration is rejected during option validation. When `allow_concurrent_memtable_write` is true, `ColumnFamilyData::ValidateOptions()` calls `CheckConcurrentWritesSupported()`, which returns `InvalidArgument` if the memtable factory does not support concurrent insert.
- **Source:** `db/column_family.cc` `ColumnFamilyData::ValidateOptions`; `db/column_family.cc` `CheckConcurrentWritesSupported`
- **Fix:** Document that users must disable `allow_concurrent_memtable_write` for non-supporting memtable reps; RocksDB does not silently downgrade the mode.

### [WRONG] In-place updates cannot be combined with concurrent memtable writes
- **File:** `docs/components/memtable/11_inplace_updates.md` / Limitations
- **Claim:** "Concurrent memtable writes with in-place updates require careful lock ordering"
- **Reality:** This is not a supported combination. `CheckConcurrentWritesSupported()` rejects `inplace_update_support` together with `allow_concurrent_memtable_write`.
- **Source:** `db/column_family.cc` `CheckConcurrentWritesSupported`
- **Fix:** Replace the warning with a hard incompatibility statement.

### [WRONG] VectorRep is not sorted by `MarkReadOnly()` and still serves reads while mutable
- **File:** `docs/components/memtable/01_representations.md` / Vector
- **Claim:** "Unsorted `std::vector` that is sorted once on `MarkReadOnly()`.", "The main vector is sorted only when `MarkReadOnly()` is called (at flush time).", and "Cannot serve ordered reads during writes (unsorted until `MarkReadOnly()` )"
- **Reality:** `MarkReadOnly()` only flips `immutable_ = true`; it does not sort. Sorting is lazy in `Iterator::DoSort()`. While the memtable is still mutable, `Get()` and `GetIterator()` take a read lock, copy the vector, and sort the copy as needed, so mutable reads are still supported.
- **Source:** `memtable/vectorrep.cc` `VectorRep::MarkReadOnly`; `memtable/vectorrep.cc` `VectorRep::Iterator::DoSort`; `memtable/vectorrep.cc` `VectorRep::Get`; `memtable/vectorrep.cc` `VectorRep::GetIterator`
- **Fix:** Describe VectorRep as storing inserts unsorted, sorting lazily on the first iterator/seek path, and using copy-on-read behavior while mutable.

### [WRONG] Hash memtable full-order iteration does not merge buckets on the fly
- **File:** `docs/components/memtable/01_representations.md` / HashSkipList, HashLinkList, Representation Comparison
- **Claim:** "Full-order iteration requires merging across all buckets" and "Full-order iteration | Efficient | Merge across buckets | Merge across buckets | After sort only"
- **Reality:** Both hash reps materialize a new globally sorted structure for full-order iteration. `GetIterator()` allocates a new arena, walks every bucket, and inserts every entry into a new sorted container before iteration starts.
- **Source:** `memtable/hash_skiplist_rep.cc` `HashSkipListRep::GetIterator`; `memtable/hash_linklist_rep.cc` `HashLinkListRep::GetIterator`
- **Fix:** Say full-order iteration rebuilds a separate sorted view, which is expensive in CPU and temporary memory.

### [WRONG] `SkipListFactory` configurability is described incorrectly
- **File:** `docs/components/memtable/01_representations.md` / SkipList (Default)
- **Claim:** "| Max height | 32 (configurable default 12) |"
- **Reality:** `SkipListFactory` only exposes `lookahead`. In the default skiplist path, `SkipListRep` constructs `InlineSkipList(compare, allocator)` using the internal defaults of max height `12` and branching factor `4`; users cannot configure those through `SkipListFactory`. Height and branching-factor knobs are exposed by `NewHashSkipListRepFactory()`, not the default skiplist factory.
- **Source:** `include/rocksdb/memtablerep.h` `SkipListFactory`; `memtable/skiplistrep.cc` `SkipListRep`; `memtable/inlineskiplist.h` `InlineSkipList` constructor defaults; `include/rocksdb/memtablerep.h` `NewHashSkipListRepFactory`
- **Fix:** Remove the configurable-height claim from the default skiplist section and, if needed, mention that the internal defaults are 12/4.

### [WRONG] Batch `MultiGet` optimization is described as generic skiplist finger search
- **File:** `docs/components/memtable/05_lookup_path.md` / Batch Lookup Path; `docs/components/memtable/13_configuration.md` / Performance Optimization Options
- **Claim:** "When `memtable_batch_lookup_optimization` is enabled (see `ImmutableDBOptions` in `include/rocksdb/options.h`), `MultiGet()` uses a three-phase approach:" and "Call `table_->MultiGet(num_keys, keys, callback_args, SaveValue)` which uses finger search in the skip list, carrying the search position forward between consecutive sorted keys."
- **Reality:** The option belongs to `AdvancedColumnFamilyOptions`, not `ImmutableDBOptions`. Also, only the skiplist rep overrides `MemTableRep::MultiGet()` to do finger-search-style batched lookup. Other reps inherit the default `MemTableRep::MultiGet()`, which uses an iterator and seeks per key.
- **Source:** `include/rocksdb/advanced_options.h` `memtable_batch_lookup_optimization`; `db/memtable.cc` `MemTable::MultiGet`; `memtable/skiplistrep.cc` `SkipListRep::MultiGet`; `include/rocksdb/memtablerep.h` `MemTableRep::MultiGet`
- **Fix:** Scope the option correctly and say the finger-search speedup is skiplist-specific, while other reps still use the batched setup/result plumbing without the same lookup algorithm.

### [WRONG] Several option scopes and headers are assigned to the wrong option type
- **File:** `docs/components/memtable/08_immutable_list.md` / History Trimming; `docs/components/memtable/11_inplace_updates.md` / Overview; `docs/components/memtable/13_configuration.md` / Representation Options, Concurrency Options, Performance Optimization Options
- **Claim:** "When `max_write_buffer_size_to_maintain > 0` (see `DBOptions` in `include/rocksdb/options.h`)...", "both requiring `inplace_update_support = true` (see `DBOptions` in `include/rocksdb/options.h`)", and "| `memtable_insert_with_hint_prefix_extractor` | nullptr | `ImmutableDBOptions` | Prefix extractor for insert hint optimization |"
- **Reality:** These are column-family-side advanced options, not DB-wide options. `inplace_update_support`, `max_write_buffer_size_to_maintain`, and `memtable_insert_with_hint_prefix_extractor` are defined in `AdvancedColumnFamilyOptions` and projected into CF-specific immutable/mutable option structs.
- **Source:** `include/rocksdb/advanced_options.h` `inplace_update_support`, `max_write_buffer_size_to_maintain`, `memtable_insert_with_hint_prefix_extractor`; `options/cf_options.h`
- **Fix:** Correct the scope/header references throughout the doc set to `AdvancedColumnFamilyOptions` unless the option is actually in `ColumnFamilyOptions` from `include/rocksdb/options.h`.

### [WRONG] Arena sizing defaults and shard sizing are oversimplified into the wrong formulas
- **File:** `docs/components/memtable/03_arena_allocation.md` / Block Allocation, Shard Refill; `docs/components/memtable/13_configuration.md` / Core Sizing Options
- **Claim:** "`kBlockSize` is computed from the `arena_block_size` option ... typically `write_buffer_size / 8`", "The shard block size is computed from the arena block size divided by the hardware concurrency level.", and "| `arena_block_size` | `write_buffer_size / 8` |"
- **Reality:** When unset, `SanitizeCfOptions()` computes `arena_block_size` as `min(1MB, write_buffer_size / 8)` and then aligns it up to 4 KB. `ConcurrentArena` then sets `shard_block_size_` to `min(128KB, block_size / 8)`, which is independent of hardware concurrency.
- **Source:** `db/column_family.cc` `SanitizeCfOptions`; `memory/concurrent_arena.cc` `ConcurrentArena::ConcurrentArena`
- **Fix:** Document the cap, 4 KB alignment, and the separate `min(128KB, block_size / 8)` shard calculation.

### [MISLEADING] `MarkForFlush()` is presented as only a manual/error-recovery path
- **File:** `docs/components/memtable/07_flush_triggers.md` / Trigger 1: Manual Flush
- **Claim:** "If `MarkForFlush()` has been called (e.g., by user request or error recovery), flush immediately."
- **Reality:** Iterators can also mark the active memtable for flush when they scan too many hidden entries. The read path uses `memtable_op_scan_flush_trigger` and `memtable_avg_op_scan_flush_trigger` to call `active_mem_->MarkForFlush()`, so `MarkForFlush()` is also a read-path performance trigger.
- **Source:** `db/db_iter.h` `MarkMemtableForFlushForPerOpTrigger`; `db/db_iter.h` `MarkMemtableForFlushForAvgTrigger`; `include/rocksdb/advanced_options.h` `memtable_op_scan_flush_trigger`, `memtable_avg_op_scan_flush_trigger`
- **Fix:** Broaden the trigger description and add a dedicated subsection explaining iterator-driven flush marking.

### [WRONG] The range-tombstone flush path is documented at the wrong abstraction layer
- **File:** `docs/components/memtable/10_range_tombstones.md` / Flush Interaction
- **Claim:** "During flush, `MemTableIterator` with `kRangeDelEntries` kind creates an iterator over `range_del_table_` that is used to write range tombstone metadata into the output SST file. The fragmented range tombstone list constructed at immutable time is used for efficient access."
- **Reality:** `FlushJob::WriteLevel0Table()` asks each memtable for a `FragmentedRangeTombstoneIterator` via `NewRangeTombstoneIterator()` or `NewTimestampStrippingRangeTombstoneIterator()`. Those APIs decide whether to use the prebuilt fragmented list or build a timestamp-stripping fragmented view. Flush does not directly hand a raw `MemTableIterator(kRangeDelEntries)` to SST writing.
- **Source:** `db/flush_job.cc` `FlushJob::WriteLevel0Table`; `db/memtable.cc` `NewRangeTombstoneIterator`; `db/memtable.cc` `NewTimestampStrippingRangeTombstoneIterator`; `db/memtable.cc` `NewRangeTombstoneIteratorInternal`
- **Fix:** Describe flush in terms of the fragmented range-tombstone iterators returned by the memtable APIs.

### [MISLEADING] Seek-time integrity checks are described too narrowly
- **File:** `docs/components/memtable/12_data_integrity.md` / Paranoid Memory Checks, Per-Key Checksum on Seek
- **Claim:** "When `paranoid_memory_checks` is enabled ... additional validation is performed during memtable traversal:" and "The `memtable_veirfy_per_key_checksum_on_seek` option ... enables key-level checksum verification during memtable seeks without the full overhead of `paranoid_memory_checks`."
- **Reality:** Validation-aware seek paths are used when either `paranoid_memory_checks` or `memtable_veirfy_per_key_checksum_on_seek` is enabled. Actual checksum verification still depends on `memtable_protection_bytes_per_key > 0`; with protection bytes set to `0`, the seek-check option does not verify anything.
- **Source:** `db/memtable.cc` `MemTable::Get`; `db/memtable.cc` `MemTable::MultiGet`; `db/memtable.cc` `MemTableIterator` constructor and `VerifyEntryChecksum`; `include/rocksdb/advanced_options.h` `memtable_veirfy_per_key_checksum_on_seek`
- **Fix:** Separate "validation-aware traversal is enabled" from "checksums are actually present to verify," and call out the dependency on `memtable_protection_bytes_per_key`.

## Completeness Gaps

### Iterator-driven scan-triggered flushes need dedicated coverage
- **Why it matters:** The docs currently mention `MarkForFlush()` but not the feature that actually drives it from the read path. `db/db_iterator_test.cc` exercises both per-op and average hidden-entry triggers, yet the memtable docs never explain how iterator scans can force an active memtable to flush.
- **Where to look:** `db/db_iter.h`; `db/db_iter.cc`; `include/rocksdb/advanced_options.h`; `db/column_family.cc`; `db/db_iterator_test.cc`
- **Suggested scope:** Add a subsection to chapter 7 covering `memtable_op_scan_flush_trigger`, `memtable_avg_op_scan_flush_trigger`, the fact that the average trigger depends on the per-op trigger, tailing-iterator limitations, and read-only sanitization to `0`.

### Option sanitization and fallback behavior are largely undocumented
- **Why it matters:** Many memtable options are not literal. RocksDB clamps, rewrites, or disables them during sanitization. Without those rules, operators reading the docs will misdiagnose configuration behavior.
- **Where to look:** `db/column_family.cc` `SanitizeCfOptions`; `include/rocksdb/advanced_options.h`; `include/rocksdb/options.h`
- **Suggested scope:** Expand chapter 13 with the important rules: `memtable_prefix_bloom_size_ratio` clipped to `[0, 0.25]`, `min_write_buffer_number_to_merge` clamped and forced to `1` under `atomic_flush`, negative `max_write_buffer_size_to_maintain` rewritten to `max_write_buffer_number * write_buffer_size`, and hash memtable factories without `prefix_extractor` sanitized back to `SkipListFactory`.

### User-defined-timestamp-in-memtable-only mode is under-documented
- **Why it matters:** The docs only mention timestamp stripping for range tombstones, but the feature is broader. It changes option compatibility, flush iterators, memtable state, and public API behavior, and it is covered heavily by `db/db_with_timestamp_basic_test.cc`.
- **Where to look:** `db/column_family.cc` `ColumnFamilyData::ValidateOptions`; `db/memtable.h` `newest_udt_`, `MaybeUpdateNewestUDT`; `db/memtable.cc` `MaybeUpdateNewestUDT`; `db/flush_job.cc` `FlushJob::WriteLevel0Table`; `db/db_impl/db_impl.cc` `GetNewestUserDefinedTimestamp`; `db/db_with_timestamp_basic_test.cc`
- **Suggested scope:** Add a short chapter-4/chapter-7/chapter-10 thread through the docs explaining `persist_user_defined_timestamps = false`, its incompatibility with `atomic_flush` and concurrent memtable writes, timestamp-stripping flush iterators, and `GetNewestUserDefinedTimestamp()`.

### The immutable-memtable docs ignore `ReadOnlyMemTable` and direct-ingested immutable memtables
- **Why it matters:** `MemTableList` stores `ReadOnlyMemTable*`, not just `MemTable*`. That abstraction is the boundary that allows direct ingestion of a `WriteBatchWithIndex` as an immutable memtable, which is a meaningful architectural fact for anyone touching flush or immutable-list code.
- **Where to look:** `db/memtable.h` `ReadOnlyMemTable`; `memtable/wbwi_memtable.h`
- **Suggested scope:** Add a brief note in chapter 8 that immutable memtables are an interface, not always the standard mutable `MemTable` implementation.

### MemPurge is a real memtable lifecycle path and is not mentioned
- **Why it matters:** The lifecycle chapters currently read as if every full memtable becomes an SST. In practice, `FlushJob` can choose MemPurge instead, and that choice depends on memtable content and memtable-rep capabilities.
- **Where to look:** `include/rocksdb/advanced_options.h` `experimental_mempurge_threshold`; `db/flush_job.cc` `MemPurgeDecider`, `MemPurge`; `db/memtable.h` `UniqueRandomSample`
- **Suggested scope:** Add a short note to chapter 7 or chapter 8 describing MemPurge as an alternative post-immutable path.

## Depth Issues

### The immutable-list chapter does not explain the in-progress-gap/rollback hazard
- **Current:** The chapter says memtables are flushed FIFO and briefly mentions rollback.
- **Missing:** It does not explain why `PickMemtablesToFlush()` stops when it sees an in-progress gap, or why `RollbackMemtableFlush()` may also roll back younger completed memtables. That is the key concurrency reason the manifest-commit path is so careful about order.
- **Source:** `db/memtable_list.cc` `PickMemtablesToFlush`; `db/memtable_list.cc` `RollbackMemtableFlush`

### The lookup chapter oversimplifies `SaveValue()`'s state machine
- **Current:** The dispatch table lists value types and actions at a high level.
- **Missing:** It does not explain that `kTypeRangeDeletion` is often a synthetic remap after comparing against `max_covering_tombstone_seq`, or that merge, blob-index, and timestamp-handling behavior depends on state accumulated before the switch. That detail matters when debugging point lookups that are affected by range tombstones or merge chains.
- **Source:** `db/memtable.cc` `SaveValue`

## Structure and Style Violations

### Inline code quoting is pervasive across the whole doc set
- **File:** `docs/components/memtable/index.md` and all chapter files
- **Details:** The review brief for this component family says "NO inline code quotes." These docs use inline code formatting for function names, option names, file names, enum values, and ordinary prose throughout. This is a systematic style violation, not a one-off edit.

## Undocumented Complexity

### `WBWIMemTable` is not just another memtable implementation
- **What it is:** `WBWIMemTable` is a `ReadOnlyMemTable` backed by `WriteBatchWithIndex`. It assigns sequence numbers during ingestion, can emit overwritten `SingleDelete` entries during flush, and intentionally does not implement every mutable-memtable capability.
- **Why it matters:** Anyone reading the current chapter 8 text would assume immutable memtables are always ordinary flushed `MemTable`s. That is no longer true, and the distinction affects flush, ingestion, and feature support.
- **Key source:** `memtable/wbwi_memtable.h`
- **Suggested placement:** Chapter 8

### MemPurge depends on memtable-rep sampling support
- **What it is:** `FlushJob::MemPurgeDecider()` samples memtable entries via `UniqueRandomSample()`. The base immutable-memtable interface requires that method, but support is representation-specific; for example, `WBWIMemTable` asserts false there.
- **Why it matters:** MemPurge is not a generic post-processing trick that automatically works for every immutable-memtable implementation.
- **Key source:** `db/flush_job.cc` `MemPurgeDecider`; `db/memtable.h` `ReadOnlyMemTable::UniqueRandomSample`; `memtable/wbwi_memtable.h`
- **Suggested placement:** Chapter 7 or chapter 8

### Memtable-only UDT mode tracks extra state in memory
- **What it is:** When `persist_user_defined_timestamps` is false, the memtable tracks `newest_udt_`, flush uses timestamp-stripping point and range-tombstone iterators, and `DB::GetNewestUserDefinedTimestamp()` reads back that memtable-derived state.
- **Why it matters:** This is a cross-cutting mode with real correctness constraints, not a small flush-time tweak.
- **Key source:** `db/memtable.h` `newest_udt_`, `MaybeUpdateNewestUDT`; `db/memtable.cc` `MaybeUpdateNewestUDT`; `db/flush_job.cc` `FlushJob::WriteLevel0Table`; `db/db_impl/db_impl.cc` `GetNewestUserDefinedTimestamp`
- **Suggested placement:** Chapter 4, chapter 7, and chapter 10

### Insert-hint optimization has narrower applicability than the option name suggests
- **What it is:** `memtable_insert_with_hint_prefix_extractor` drives a per-prefix hint map stored in the memtable, but only on the non-concurrent insert path and only for the point-entry table, not the range-delete table.
- **Why it matters:** Without that detail, a reader can easily assume the hint optimization helps concurrent writes or range tombstones too.
- **Key source:** `db/memtable.cc` `MemTable::Add`; `db/memtable.h` `insert_hints_`; `include/rocksdb/advanced_options.h` `memtable_insert_with_hint_prefix_extractor`
- **Suggested placement:** Chapter 4 or chapter 13

## Positive Notes

- `docs/components/memtable/index.md` is within the requested size budget and follows the expected overview/source-files/chapter-table/characteristics/invariants pattern.
- `docs/components/memtable/02_inlineskiplist.md` is strong. The splice/finger-search explanation and memory-ordering discussion match the current implementation well.
- `docs/components/memtable/03_arena_allocation.md` correctly explains the two-direction allocation strategy in `Arena` and the high-level reason `ConcurrentArena` exists.
- `docs/components/memtable/10_range_tombstones.md` does a good job explaining why range tombstones live in a separate skiplist and how mutable-side cache invalidation works.
