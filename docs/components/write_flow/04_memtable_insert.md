# MemTable Insertion

**Files:** `db/memtable.h`, `db/memtable.cc`, `memtable/skiplistrep.cc`, `util/arena.h`, `util/concurrent_arena.h`

## Overview

`MemTable` is an in-memory sorted data structure (default: lock-free skiplist) that receives writes before flush to SST files. It supports concurrent reads during writes and uses arena-based memory allocation for efficient bulk deallocation on flush.

## Internal Key Encoding

Every memtable entry is encoded as a contiguous byte sequence:

| Field | Encoding | Description |
|-------|----------|-------------|
| internal_key_size | varint32 | `user_key.size() + 8` (includes packed tag) |
| user_key | bytes | Raw user key (may include timestamp suffix) |
| packed_tag | fixed64 | `(sequence_number << 8) | value_type` |
| value_size | varint32 | Length of value |
| value | bytes | Raw value data |
| checksum | 0 or 8 bytes | Present when `protection_bytes_per_key == 8` |

**Important:** Within the same user key, entries are sorted by descending sequence number (newest first). This ensures point lookups find the latest version immediately without scanning older versions.

## Data Structures

The `MemTable` class (see `db/memtable.h`) contains:

| Field | Type | Purpose |
|-------|------|---------|
| `arena_` | `ConcurrentArena` | All memtable memory (shared by point and range-del tables) |
| `table_` | `unique_ptr<MemTableRep>` | Point key storage (skiplist, hash-skiplist, or vector) |
| `range_del_table_` | `unique_ptr<MemTableRep>` | Always skiplist; stores range tombstones |
| `bloom_filter_` | `unique_ptr<DynamicBloom>` | Prefix/whole-key bloom filter (null if disabled) |

**Tracking fields:**

| Field | Type | Purpose |
|-------|------|---------|
| `first_seqno_` | `atomic<SequenceNumber>` | Sequence number of first inserted key (0 if empty) |
| `earliest_seqno_` | `atomic<SequenceNumber>` | Lower bound on all sequence numbers |
| `data_size_` | `RelaxedAtomic<uint64_t>` | Total encoded bytes |
| `num_entries_` | `RelaxedAtomic<uint64_t>` | Entry count |
| `flush_state_` | `atomic<FlushStateEnum>` | `FLUSH_NOT_REQUESTED` / `FLUSH_REQUESTED` / `FLUSH_SCHEDULED` |

## Insertion Flow

`MemTable::Add()` (see `db/memtable.cc`) inserts a single key-value pair:

Step 1 - **Compute encoded size**: `varint(ikey_size) + ikey_size + varint(val_size) + val_size + protection_bytes`.

Step 2 - **Route to table**: `kTypeRangeDeletion` entries go to `range_del_table_`; all other types go to `table_`.

Step 3 - **Allocate**: `table->Allocate(encoded_len, &buf)` allocates from the arena.

Step 4 - **Encode**: Write length-prefixed internal key (user_key + packed tag), length-prefixed value, and optional checksum.

Step 5 - **Bloom update**: Add prefix and/or whole key (with timestamp stripped) to the bloom filter.

Step 6 - **Insert**: Call `table->InsertKey(handle)` or `InsertKeyWithHint(handle, &hint)` for skiplist insertion (O(log n) expected).

## Serial vs Concurrent Insertion

**Serial mode**: Direct `InsertKey()` plus atomic counter updates for `data_size_`, `num_entries_`, etc.

**Concurrent mode** (when `allow_concurrent_memtable_write` is enabled): Uses `InsertKeyConcurrently()` (lock-free skiplist CAS insertion) plus thread-local `MemTablePostProcessInfo` to batch counter updates. After the entire write batch completes, `BatchPostProcess()` drains accumulated counts into the atomic counters, reducing contention.

**Important:** Entries are never removed from a memtable. The entire memtable is discarded atomically on flush.

## Point Lookup

`MemTable::Get()` (see `db/memtable.cc`) searches for a key:

Step 1 - If `IsEmpty()` (i.e., `first_seqno_ == 0`), return `false` immediately.

Step 2 - Check range tombstones via `MaxCoveringTombstoneSeqnum(user_key)` from `range_del_table_`.

Step 3 - Check bloom filter (prefix or whole-key). On miss, return `false` immediately.

Step 4 - Search the skiplist via `table_->Get(key, &saver, SaveValue)`. The `SaveValue` callback is invoked for each entry with matching user_key, newest first:
- `kTypeValue`: copy value, return found
- `kTypeDeletion` / `kTypeSingleDeletion`: return not-found
- `kTypeMerge`: accumulate operand via `MergeHelper`
- `kTypeWideColumnEntity`: parse wide columns, return found

## Flush Trigger

`MemTable::ShouldFlushNow()` (see `db/memtable.cc`) returns `true` if:

- An external signal has marked this memtable for flush (`IsMarkedForFlush()`), or
- Range deletion count reaches `memtable_max_range_deletions_` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), or
- Arena allocation exceeds `write_buffer_size` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) using a three-stage check:
  1. If total allocation plus one more arena block fits within `write_buffer_size + kArenaBlockSize * kAllowOverAllocationRatio` (i.e., room for over-allocation by 60% of one arena block): return `false`
  2. If over the limit plus headroom: return `true`
  3. In the boundary zone: return `true` only if the current arena block is more than 75% used

This three-stage check avoids wasting memory in partially-filled arena blocks while preventing excessive overshooting of the configured buffer size.

After returning `true`, the memtable transitions `flush_state_` from `FLUSH_NOT_REQUESTED` to `FLUSH_REQUESTED`, which is picked up by the write leader's `PreprocessWrite()` call.

## SwitchMemtable

When a flush is triggered, `DBImpl::SwitchMemtable()` (see `db/db_impl/db_impl_write.cc`):

Step 1 - Writes any recoverable state to the current memtable.

Step 2 - Creates a new WAL file (unless the current WAL is empty) with the next file number from `VersionSet::NewFileNumber()`.

Step 3 - Constructs a new `MemTable` with `earliest_seq` set to the current last sequence.

Step 4 - Constructs `FragmentedRangeTombstones` on the old memtable for read efficiency.

Step 5 - Under `wal_write_mutex_`, flushes the current WAL writer's buffer and switches to the new WAL.

Step 6 - Moves the old memtable to the immutable list and installs a new `SuperVersion`.

If the WAL creation or buffer flush fails, the error is recorded as a background error and propagated to the caller.
