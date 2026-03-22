# Read Flow

## Overview

This document provides a comprehensive end-to-end trace of read operations in RocksDB, from application API calls through all internal layers to disk I/O. The read path is designed for high performance with multiple caching layers, snapshot isolation guarantees, and zero-copy optimizations where possible.

### High-Level Read Flow

```
┌───────────────────────────────────────────────────────────────┐
│                    Application Layer                          │
│  DB::Get() / DB::MultiGet() / DB::NewIterator()               │
└────────────────┬──────────────────────────────────────────────┘
                 │
                 v
┌───────────────────────────────────────────────────────────────┐
│               SuperVersion Acquisition                        │
│  GetAndRefSuperVersion() → consistent snapshot                │
│  (mem + imm + Version, ref-counted)                          │
└────────────────┬──────────────────────────────────────────────┘
                 │
                 v
┌───────────────────────────────────────────────────────────────┐
│                  Search Layers (ordered)                      │
│  1. Mutable MemTable     → skiplist + bloom                  │
│  2. Immutable MemTables  → newest to oldest                  │
│  3. SST Files (Version)  → L0 all files, L1+ binary search   │
└────────────────┬──────────────────────────────────────────────┘
                 │
                 v
┌───────────────────────────────────────────────────────────────┐
│              Block Cache / Disk I/O                           │
│  Cache lookup → compressed cache → disk read → decompress    │
│  Block types: data, index, filter                            │
└────────────────┬──────────────────────────────────────────────┘
                 │
                 v
┌───────────────────────────────────────────────────────────────┐
│            Resolution (during search above)                   │
│  - Range deletion: max_covering_tombstone_seq check          │
│  - Merge operator: operand collection across layers          │
│  - Blob value: retrieved in Version::Get after kFound        │
│  - Snapshot visibility: seq <= snapshot_seq filtering         │
└────────────────┬──────────────────────────────────────────────┘
                 │
                 v
┌───────────────────────────────────────────────────────────────┐
│            SuperVersion Release                               │
│  ReturnAndCleanupSuperVersion() → cleanup if last ref        │
└───────────────────────────────────────────────────────────────┘
```

---

## 1. Point Lookup APIs (Get / MultiGet)

### Get() Entry Point

**Location:** `db/db_impl/db_impl.cc:2482-2788` (`DBImpl::GetImpl`)

**Core Flow:**
```cpp
Status DBImpl::GetImpl(const ReadOptions& read_options,
                       const Slice& key,
                       GetImplOptions& get_impl_options);
```

**Step-by-Step:**

1. **Acquire SuperVersion** (line 2538):
   ```cpp
   SuperVersion* sv = GetAndRefSuperVersion(cfd);
   ```
   ⚠️ **INVARIANT:** Must acquire SuperVersion before accessing memtables/SST files to prevent use-after-free

2. **Determine snapshot sequence** (lines 2552-2586):
   - If `read_options.snapshot != nullptr`: Use snapshot's sequence number
   - Otherwise: Use `GetLastPublishedSequence()`
   - **Critical ordering**: Snapshot assigned AFTER SuperVersion acquisition to prevent data loss during concurrent flushes

3. **Construct LookupKey** (line 2612):
   ```cpp
   LookupKey lkey(key, snapshot, read_options.timestamp);
   ```
   Format: `klength (varint32) | userkey | [timestamp] | tag (sequence + type, 8 bytes)`
   Note: When `ReadOptions::timestamp` is supplied, timestamp bytes are inserted between user key and tag

4. **Search mutable memtable** (lines 2623-2631):
   ```cpp
   if (sv->mem->Get(lkey, ...)) {
     done = true;  // MEMTABLE_HIT
   }
   ```

5. **Search immutable memtables** (lines 2638-2654):
   ```cpp
   if (!done && sv->imm->Get(lkey, ...)) {
     done = true;  // MEMTABLE_HIT
   }
   ```
   Searches newest to oldest

6. **Search SST files** (lines 2684-2694):
   ```cpp
   if (!done) {
     sv->current->Get(...);  // MEMTABLE_MISS
   }
   ```

7. **Post-processing** (lines 2696-2788):
   - Merge operand threshold check and `GetMergeOperands` assembly
   - Statistics recording (`RecordTick`, `RecordInHistogram`)
   - Note: Blob retrieval happens earlier inside `Version::Get()`, not here

8. **Release SuperVersion** (line 2785):
   ```cpp
   ReturnAndCleanupSuperVersion(cfd, sv);
   ```
   ⚠️ **INVARIANT:** Must release after read completes to prevent resource leaks

### MultiGet() Optimizations

**Location:** `db/db_impl/db_impl.cc:3337-3466` (`DBImpl::MultiGetImpl`)

**Key Optimizations:**

| Optimization | Benefit | Implementation |
|--------------|---------|----------------|
| **Pre-sorted keys** | Sequential SST access, block sharing | `PrepareMultiGetKeys()` sorts by CF + user key (line 3184) |
| **Shared SuperVersion** | Single snapshot for entire batch | Acquired once in caller (line 3276) |
| **Bitmap tracking** | O(1) key completion checks | `value_mask_` in `MultiGetContext` |
| **Batched bloom filters** | Single filter lookup | `MultiGetFilter()` processes entire range |
| **Block reuse** | Multiple keys from same block | Tracked with `reused_mask` (sync_and_async.h:426) |
| **Adjacent block coalescing** | Fewer I/O syscalls | Combine reads in `RetrieveMultipleBlocks()` (sync_and_async.h:89) |
| **Async cache lookups** | Parallel cache queries | `StartAsyncLookupFull()` (sync_and_async.h:450) |

**Flow:**
```
MultiGet(keys[])
  → PrepareMultiGetKeys() [sort by CF + key]
  → MultiGetImpl() [batch of MAX_BATCH_SIZE=32]
      → mem->MultiGet()
      → imm->MultiGet()
      → Version::MultiGet()
          → MultiGetFilter() [batch bloom check]
          → RetrieveMultipleBlocks() [coalesce adjacent, async I/O]
```

---

## 2. SuperVersion Acquisition

**Location:** `db/column_family.h:206-275` (structure), `db/column_family.cc:1353-1485` (acquisition logic)

### Structure

```cpp
struct SuperVersion {
  ColumnFamilyData* cfd;
  ReadOnlyMemTable* mem;              // Current mutable memtable
  MemTableListVersion* imm;           // Immutable memtables
  Version* current;                   // SST files (Version)
  MutableCFOptions mutable_cf_options;
  uint64_t version_number;            // Monotonically increasing version
  WriteStallCondition write_stall_condition;
  std::string full_history_ts_low;    // Oldest readable timestamp
  std::shared_ptr<const SeqnoToTimeMapping> seqno_to_time_mapping;

 private:
  std::atomic<uint32_t> refs;         // Reference count
  autovector<ReadOnlyMemTable*> to_delete;  // Deferred cleanup
};
```

### Refcounting Rules

**Ref() implementation** (`column_family.cc:511-514`):
```cpp
SuperVersion* SuperVersion::Ref() {
  refs.fetch_add(1, std::memory_order_relaxed);
  return this;
}
```

**Unref() implementation** (`column_family.cc:516-521`):
```cpp
bool SuperVersion::Unref() {
  uint32_t previous_refs = refs.fetch_sub(1);
  assert(previous_refs > 0);
  return previous_refs == 1;  // true if last reference
}
```

⚠️ **INVARIANT:** Acquire SuperVersion before read, release after read completes. This prevents memtables/SST files from being deleted mid-read.

### Thread-Local Caching

**Location:** `column_family.cc:1366-1394` (`GetThreadLocalSuperVersion`)

**Mechanism:**
- Per-thread slot cached in `local_sv_` (ThreadLocalPtr)
- Sentinel values: `kSVInUse` (thread using), `kSVObsolete` (invalidated by InstallSuperVersion)
- Atomic swap operations prevent races

**Fast path (thread-local hit):**
```cpp
void* ptr = local_sv_->Swap(SuperVersion::kSVInUse);
if (ptr != SuperVersion::kSVObsolete) {
  return static_cast<SuperVersion*>(ptr);  // Cached SV, already ref'd
}
```

**Slow path (cache miss):**
```cpp
db->mutex()->Lock();
sv = super_version_->Ref();  // Increment refcount
db->mutex()->Unlock();
```

### Consistency Guarantee

**How SuperVersion provides consistent snapshots:**
1. `InstallSuperVersion()` updates `super_version_` pointer while holding the DB mutex (line 1443)
2. Marks all thread-local copies as `kSVObsolete` via `ResetThreadLocalSuperVersions()` (line 1469)
3. Old SuperVersion kept alive by existing references (refcount)
4. Cleanup deferred until last reference released
5. Safety comes from mutex + refcount + thread-local invalidation, not a lock-free atomic swap

⚠️ **INVARIANT:** Readers see a consistent point-in-time snapshot of mem + imm + Version without holding DB mutex.

---

## 3. MemTable Lookup

**Location:** `db/memtable.cc:1405-1531` (Get), `db/memtable_list.cc:105-191` (MemTableListVersion::Get)

### MemTable::Get() Flow

**Entry point:** `memtable.cc:1405-1483`

```
MemTable::Get(key, value, ...)
  |
  v
  IsEmpty()? → return false
  |
  v
  Range deletion check (lines 1420-1436)
    - Create range tombstone iterator
    - Check if key covered: update max_covering_tombstone_seq
  |
  v
  Bloom filter check (if enabled, lines 1441-1457)
    - memtable_whole_key_filtering: check whole key
    - Prefix mode: check prefix only
    - Bloom says "not found"? → skip table lookup
  |
  v
  Skiplist search: GetFromTable() (lines 1467-1469)
    - Calls table_->Get() → MemTableRep::Get()
    - Iterates skiplist, invokes SaveValue() callback per entry
  |
  v
  Result handling (lines 1474-1479)
```

### SaveValue() Callback - Sequence Number Filtering

**Location:** `memtable.cc:1169-1403`

**Per-entry processing:**
```cpp
// 1. Parse entry (lines 1184-1218)
key_length, user_key, seq, type, value = parse_entry(entry)

// 2. User key match? (lines 1195-1196)
if (user_key != lookup_key) return true;  // Continue iteration

// 3. Snapshot visibility (lines 1220-1222)
if (!CheckCallback(seq)) return true;  // Not visible, continue

// 4. Range tombstone integration (lines 1253-1259)
if (max_covering_tombstone_seq > seq) {
  // Key deleted by range tombstone
  treat as kTypeDeletion;
}

// 5. Type dispatch (lines 1260-1403)
switch (type) {
  case kTypeValue:              return value, stop;
  case kTypeValuePreferredSeqno: return value (with preferred seqno), stop;
  case kTypeDeletion:           return NotFound, stop;
  case kTypeDeletionWithTimestamp: return NotFound, stop;
  case kTypeSingleDeletion:     return NotFound, stop;
  case kTypeMerge:              collect operand, continue if not done;
  case kTypeBlobIndex:          return blob reference, stop;
  case kTypeWideColumnEntity:   return wide-column entity, stop;
}
```

⚠️ **INVARIANT:** Snapshot visibility enforced via `ReadCallback::IsVisible(seq)`. Only keys with `seq <= snapshot_seq` are visible.

### MemTableListVersion::Get() - Immutable Memtable Search

**Location:** `memtable_list.cc:105-191`

**Strategy: Newest to Oldest**
```cpp
for (auto& memtable : memlist_) {  // memlist_ ordered newest→oldest
  SequenceNumber current_seq = kMaxSequenceNumber;

  bool done = memtable->Get(key, value, ..., &current_seq, ...);

  if (*seq == kMaxSequenceNumber) {
    *seq = current_seq;  // Record first sequence found
  }

  if (done) return true;  // Found final value

  if (merge_in_progress) continue;  // Accumulate merge operands
}
```

⚠️ **INVARIANT:** Immutable memtables searched newest first to find most recent value quickly.

---

## 4. Block Cache Integration

**Location:** `table/block_based/block_based_table_reader.cc:2080-2151` (RetrieveBlock), `1824-1982` (MaybeReadBlockAndLoadToCache)

### Multi-Tier Cache Strategy

```
RetrieveBlock(block_handle)
  |
  v
  ┌─────────────────────────────────┐
  │ Uncompressed Block Cache        │
  │ (LRUCache / HyperClockCache)   │
  └───────┬─────────────────────────┘
          │ miss
          v
  ┌─────────────────────────────────┐
  │ Compressed Cache / Secondary    │
  │ (CompressedSecondaryCache)     │
  └───────┬─────────────────────────┘
          │ miss
          v
  ┌─────────────────────────────────┐
  │ Disk I/O                        │
  │ ReadBlockContents() → decompress│
  └───────┬─────────────────────────┘
          │
          v
  ┌─────────────────────────────────┐
  │ Insert into Cache(s)            │
  │ Uncompressed → primary cache    │
  │ Compressed → secondary cache    │
  └─────────────────────────────────┘
```

### Cache Key Construction

**Location:** `block_based_table_reader.cc:727-732`, `cache/cache_key.h:125-128`

**Per-file base key:**
```cpp
// SetupBaseCacheKey (lines 678-725)
OffsetableCacheKey base_cache_key(db_id, db_session_id, file_number);
```
- Globally unique with high probability
- Stable across DB open/close, backup/restore **only when SST has `db_session_id` and `orig_file_number`** (newer SSTs)
- Old SSTs lacking these properties use a fallback key that is **not stable** across DB close/re-open or across different DBs

**Per-block key derivation:**
```cpp
// GetCacheKey (line 727-732)
CacheKey GetCacheKey(const OffsetableCacheKey& base, const BlockHandle& handle) {
  return base.WithOffset(handle.offset() >> 2);  // XOR operation
}
```
- Fast: single XOR on hot path
- 16-byte fixed size (2× uint64_t)
- 8-byte common prefix per file

### Block Cache Miss Handling

**Location:** `block_based_table_reader.cc:1871-1970`

**Detailed flow:**
```cpp
// 1. Detect cache miss — read and cache path (lines 1873-1875)
if (block not in cache && !no_io && fill_cache) {

  // 2. Read from disk (lines 1888-1916)
  BlockFetcher block_fetcher(file, handle, ...);
  if (async_read && prefetch_buffer) {
    s = block_fetcher.ReadAsyncBlockContents();  // Async I/O
  } else {
    s = block_fetcher.ReadBlockContents();       // Sync I/O
  }
  // Checksum verified during fetch

  // 3. Separate compressed/uncompressed (lines 1932-1942)
  if (do_uncompress && compression_type != kNoCompression) {
    compressed_contents = block_fetcher.GetCompressedBlock();
    uncompressed_contents = std::move(tmp_contents);  // Already decompressed
  }

  // 4. Insert into cache (lines 1948-1951)
  PutDataBlockToCache(key, block_cache,
                      std::move(uncompressed_contents),
                      std::move(compressed_contents), ...);
}

// 5. Fallback: read without caching (RetrieveBlock lines 2100-2147)
//    When fill_cache=false or block absent from cache, disk reads still
//    happen via ReadAndParseBlockFromFile() — block returned directly
//    without cache insertion.
```

### Cache Admission Policy

**Location:** `block_based_table_reader.cc:1606-1626`

**Conditions for insertion:**
- `ReadOptions::fill_cache == true`
- `ReadOptions::read_tier != kBlockCacheTier` (no I/O restriction)
- Block has owned memory (`block_holder->own_bytes()`)

**Priority levels:**
| Priority | Eviction Order | Use Case |
|----------|---------------|----------|
| `Cache::Priority::HIGH` | Last | Index/filter blocks (if `cache_index_and_filter_blocks_with_high_priority`) |
| `Cache::Priority::LOW` | Default | Data blocks, table properties |
| `Cache::Priority::BOTTOM` | First | Speculative prefetch |

### CacheItemHelper Lifecycle

**Location:** `table/block_based/block_cache.cc:108-115`, `cache/typed_cache.h:261-270`

**Callbacks for block lifecycle:**
| Callback | Purpose | Implementation |
|----------|---------|----------------|
| `Delete` | Destructor | `std::default_delete<TValue>{}(value)` |
| `Size` | Serialization size | `value->ContentSlice().size()` |
| `SaveTo` | Serialize to SecondaryCache | `std::copy_n(ContentSlice())` |
| `Create` | Deserialize from SecondaryCache | `BlockCreateContext::Create()` (includes decompression) |

⚠️ **INVARIANT:** Cache keys are globally unique. Collisions would cause incorrect data returns.

---

## 5. SST File Lookup (Version::Get)

**Location:** `db/version_set.cc:2710-2915` (Version::Get), `148-353` (FilePicker)

### FilePicker - Level-by-Level File Selection

**Architecture:**
```cpp
class FilePicker {
  int32_t curr_level_;                // Current level being searched
  int32_t curr_index_in_curr_level_;  // File index within level
  LevelFilesBrief* curr_file_level_;  // Current level's file array
  // ... search bounds for L1+ binary search
};
```

### L0 Search Strategy (All Files, Newest First)

**Location:** `version_set.cc:306-309`

```cpp
if (curr_level_ == 0) {
  start_index = 0;  // Check ALL L0 files
}
```

**L0 file ordering:**
- Files sorted by `epoch_number` descending (newest first)
- `epoch_number` assigned at flush time, tracks creation order
- Must check all files because key ranges overlap

**Loop logic** (`FilePicker::GetNextFile`, lines 184-250):
```cpp
while (curr_index_in_curr_level_ < curr_file_level_->num_files) {
  FdWithKeyRange* f = &curr_file_level_->files[curr_index_in_curr_level_];

  // Range filtering for optimization (lines 201-233)
  if (user_key < f->smallest_user_key) { ++curr_index_in_curr_level_; continue; }
  if (user_key > f->largest_user_key) { ++curr_index_in_curr_level_; continue; }

  if (curr_level_ == 0) {
    ++curr_index_in_curr_level_;  // Advance for next call
    return f;  // Return this L0 file
  }
  // ... L1+ logic ...
}
```

⚠️ **INVARIANT:** L0 files checked in epoch_number order (newest→oldest) to find most recent value first.

### L1+ Search Strategy (Binary Search to First Candidate File)

**Location:** `version_set.cc:310-334`

**Non-overlapping guarantee:** Files in L1+ have disjoint key ranges, maintained by compaction.

**Binary search implementation:**
```cpp
// Find earliest file where file.largest_key >= lookup_key
start_index = FindFileInRange(
    *internal_comparator_, *curr_file_level_, ikey_,
    static_cast<uint32_t>(search_left_bound_),
    static_cast<uint32_t>(search_right_bound_) + 1);

if (start_index == search_right_bound_ + 1) {
  // ikey_ comes after all files in search range
  curr_level_++;  // Skip this level
  continue;
}
```

**FindFileInRange** (`version_set.cc:103-111`):
```cpp
int FindFileInRange(..., uint32_t left, uint32_t right) {
  auto cmp = [&](const FdWithKeyRange& f, const Slice& k) -> bool {
    return icmp.Compare(f.largest_key, k) < 0;
  };
  return static_cast<int>(std::lower_bound(b + left, b + right, key, cmp) - b);
}
```

⚠️ **INVARIANT:** L1+ files are sorted and non-overlapping in internal-key order, so RocksDB can binary-search to the first candidate file. However, adjacent L1+ files may share the same user key at file boundaries (e.g., due to merge operands or snapshots preventing compaction from combining them), so a point lookup may need to check subsequent files in the same level.

### TableCache::Get() - Bloom Filter + Block Read

**Location:** `version_set.cc:2776-2782`, `db/table_cache.h:115-121`

**Invocation:**
```cpp
*status = table_cache_->Get(
    read_options, *internal_comparator(), *f->file_metadata, ikey,
    &get_context, mutable_cf_options_,
    file_read_hist,
    IsFilterSkipped(...),  // Skip bloom at bottom level if optimize_filters_for_hits
    fp.GetHitFileLevel(), ...);
```

**TableCache::Get() flow:**
```
TableCache::Get()
  |
  v
  FindTable() → check pinned reader → check cache → open file
  |
  v
  TableReader::Get() [BlockBasedTable::Get()]
    |
    v
    Check bloom filter (if !skip_filters)
      - false? → return NotFound (bloom negative)
      - true? → continue (might be false positive)
    |
    v
    Index block seek → find data block containing key
    |
    v
    RetrieveBlock() → cache lookup → disk read
    |
    v
    Data block binary search → GetContext::SaveValue()
```

### InternalKey Ordering

**Location:** `db/dbformat.h:381-428` (InternalKeyComparator::Compare)

**Format:**
```
InternalKey = user_key | timestamp | (sequence << 8) | type
```

**Comparison rules:**
```cpp
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // 1. Compare user_key (ASC) — includes timestamp if UDT enabled
  int r = user_comparator_.Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r != 0) return r;

  // 2. Compare packed footer (sequence << 8 | type) as uint64_t (DESC)
  //    This orders by sequence DESC, then type DESC as tie-breaker
  uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
  uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
  if (anum > bnum) return -1;  // Newer (larger seq or higher type) first
  else if (anum < bnum) return +1;
  else return 0;
}
```

⚠️ **INVARIANT:** Keys ordered by `user_key` ASC, then `sequence` DESC, then `type` DESC. Ensures newest version found first during forward iteration.

---

## 6. Iterator / Scan Path

**Location:** `db/db_iter.cc` (DBIter), `table/merging_iterator.cc` (MergingIterator), `db/arena_wrapped_db_iter.cc:253-274` (construction)

### Iterator Stack Architecture

```
┌──────────────────────────────────────┐
│         DBIter (user-facing)         │
│  Resolves internal keys → user keys │
│  Handles merges, deletions, visibility│
└─────────────┬────────────────────────┘
              │
              v
┌──────────────────────────────────────┐
│       MergingIterator                │
│  Min/max heap merge, range tombstone │
│  integration, multi-source iteration │
└─────────────┬────────────────────────┘
              │
         ┌────┴─────────────────────┐
         │                          │
         v                          v
┌────────────────┐        ┌────────────────┐
│ MemTable iter  │        │ SST file iters │
│ + range del    │        │ (L0...Ln)      │
└────────────────┘        │ + range del    │
                          └────────────────┘
```

### NewIterator() → MergingIterator Creation

**Location:** `db/arena_wrapped_db_iter.cc:253-274`, `db/db_impl/db_impl.cc:2239-2301`

**Construction flow:**
```cpp
MergeIteratorBuilder merge_iter_builder(&internal_comparator, arena, ...);

// 1. Mutable memtable (lines 2267-2273)
auto mem_iter = super_version->mem->NewIterator(...);
auto mem_tombstone_iter = super_version->mem->NewRangeTombstoneIterator(...);
merge_iter_builder.AddPointAndTombstoneIterator(mem_iter, mem_tombstone_iter);

// 2. Immutable memtables (lines 2275-2277)
super_version->imm->AddIterators(..., &merge_iter_builder, ...);

// 3. SST files L0...Ln (lines 2283-2284)
super_version->current->AddIterators(..., &merge_iter_builder, ...);

// 4. Build MergingIterator (line 2293)
internal_iter = merge_iter_builder.Finish(db_iter);
```

⚠️ **INVARIANT:** Children ordered by LSM tree recency (mem → imm → L0 → L1...Ln), ensuring newest data wins during merge.

### DBIter Direction Model

**Location:** `db/db_iter.h:98`, `db/db_iter.cc:813-861`

```cpp
enum Direction : uint8_t { kForward, kReverse };
```

**Positioning invariants:**
| Direction | Internal Iterator Position |
|-----------|---------------------------|
| **kForward** | At the exact entry yielding `key()/value()` (if not merged) |
| **kForward** (merged) | Immediately after last merge operand |
| **kReverse** | Just BEFORE all entries with `user_key == this->key()` |

**Direction changes:**
- `ReverseToForward()`: Re-seeks to position correctly for forward iteration (lines 813-833)
- `ReverseToBackward()`: Transitions from forward → reverse (lines 835-861)

### FindNextUserEntry() - Core Resolution Loop

**Location:** `db/db_iter.cc:356-622` (`FindNextUserEntryInternal`)

**Algorithm:**
```cpp
while (iter_.Valid()) {
  ParsedInternalKey ikey;
  ParseKey(&ikey);

  // 1. Check visibility
  if (!IsVisible(ikey.sequence, timestamp)) {
    iter_.Next();
    continue;
  }

  // 2. Skip if same user_key already processed
  if (skipping_saved_key_ &&
      user_comparator_.Equal(ikey.user_key, saved_key_.GetUserKey())) {
    num_skipped++;
    iter_.Next();
    continue;
  }

  // 3. Process by type
  switch (ikey.type) {
    case kTypeDeletion:
    case kTypeSingleDeletion:
      if (timestamp_lb_) {
        // Return deletion tombstone for timestamp queries
        return true;
      } else {
        // Hide all older versions
        skipping_saved_key_ = true;
        saved_key_.SetUserKey(ikey.user_key);
        iter_.Next();
        continue;
      }

    case kTypeValue:
    case kTypeBlobIndex:
    case kTypeWideColumnEntity:
      // Save key/value, return to user
      SetSavedKeyToSeekTarget(ikey.user_key);
      value_ = iter_.value();
      return true;

    case kTypeMerge:
      // Collect merge operands
      MergeValuesNewToOld();  // Resolves merge chain
      return true;
  }

  // 4. Seek optimization (lines 565-609)
  if (num_skipped > max_skip_ && !reseek_done) {
    // Too many versions of same key - seek past them
    IterKey last_key;
    last_key.SetInternalKey(saved_key_.GetUserKey(), 0, kTypeDeletion);
    iter_.Seek(last_key.GetInternalKey());
    RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
    reseek_done = true;
  }
}
```

### MergeValuesNewToOld() - Merge Resolution

**Location:** `db/db_iter.cc:631-748`

**Merge operand collection:**
```cpp
// 1. Push first merge operand
merge_context_.PushOperand(iter_.value(),
                          iter_.iter()->IsValuePinned());

// 2. Advance forward via iter_.Next() (newer → older sequence numbers
//    due to InternalKey ordering: same user_key sorted by descending seq)
iter_.Next();
while (iter_.Valid()) {
  ParsedInternalKey ikey;
  ParseKey(&ikey);

  if (!user_comparator_.Equal(ikey.user_key, saved_key_.GetUserKey())) {
    break;  // Different key, stop
  }

  if (!IsVisible(ikey.sequence)) {
    iter_.Next();
    continue;
  }

  if (ikey.type == kTypeMerge) {
    // Collect another merge operand
    merge_context_.PushOperand(iter_.value(), ...);
    iter_.Next();
  } else if (ikey.type == kTypeValue || ikey.type == kTypeBlobIndex) {
    // Found base value - perform full merge
    return MergeWithPlainBaseValue(value, ikey.user_key);
  } else {
    // Deletion - merge without base
    break;
  }
}

// 3. Apply merge operator
s = MergeHelper::TimedFullMerge(
    merge_operator_, saved_key_.GetUserKey(),
    nullptr,  // No base value
    merge_context_.GetOperands(),
    &value_, ...);
```

⚠️ **INVARIANT:** Merge operands collected newest→oldest, presented to merge operator in chronological order (oldest→newest).

### Iterator Stability Guarantees

**Pinned Blocks (Zero-Copy Iteration):**

**Location:** `db/pinned_iterators_manager.h:19-92`, `db/db_iter.h:316-327`

**Mechanism:**
- `PinnedIteratorsManager pinned_iters_mgr_` in DBIter
- `ReadOptions::pin_data` enables lifetime pinning (`pin_thru_lifetime_ = true`)
- `TempPinData()` / `ReleaseTempPinnedData()` for temporary pinning

**Benefits:**
- Avoids memcpy for large values
- Value remains valid until iterator moves or is destroyed

**Snapshot Holds (Consistent Reads):**

**Location:** `db/db_impl/db_impl.cc:2295-2299`

**SuperVersion reference held until iterator destruction:**
```cpp
SuperVersionHandle* cleanup = new SuperVersionHandle(
    this, &mutex_, super_version, ...);
internal_iter->RegisterCleanup(CleanupSuperVersionHandle, cleanup, nullptr);
```

**Guarantees:**
- SuperVersion refs Version (SST file set) + MemTable + MemTableList
- Prevents file deletion during iteration
- `CleanupSuperVersionHandle()` unrefs SuperVersion, schedules obsolete file deletion

⚠️ **INVARIANT:** Iterator holds SuperVersion reference, ensuring consistent view and preventing file deletion.

### Seek Optimization

**Skip optimization in FindNextUserEntry** (lines 565-609):
- **Trigger:** `num_skipped > max_skip_` (default from `max_sequential_skip_in_iterations`)
- **Strategy:** Seek to `(user_key, 0, kTypeDeletion)` to fast-forward past all versions of current key
- **Benefit:** Avoids comparing same user key hundreds of times

**Cascading seek in MergingIterator:**
- **Location:** `table/merging_iterator.cc:773-905` (`SeekImpl`)
- **Range tombstone optimization:** When point key covered by range tombstone, seek past tombstone end key
- **Cascades through levels** to avoid iterating over deleted ranges

---

## 7. MultiGet Optimization

See **Section 1 (Point Lookup APIs)** for comprehensive MultiGet optimization details, including:
- Pre-sorted keys for sequential access
- Bitmap tracking for O(1) completion checks
- Block reuse for multiple keys in same block
- Adjacent block coalescing for fewer I/O syscalls
- Async cache lookups and async file I/O
- Batched bloom filter checks

---

## 8. Range Deletion Handling

**Location:** `db/range_del_aggregator.h:287-479`, `db/range_del_aggregator.cc:23-553`

### RangeDelAggregator Architecture

```
RangeDelAggregator (abstract)
   |
   +-- ReadRangeDelAggregator      (reads/iterators)
   |     - Single StripeRep [0, snapshot_seqno]
   |     - Simple ShouldDelete check
   |
   +-- CompactionRangeDelAggregator (compaction)
         - Multiple StripeReps partitioned by snapshots
         - Timestamp filtering
```

### Tombstone Collection Flow

**AddTombstones** (`range_del_aggregator.cc:336-344`):
```cpp
void ReadRangeDelAggregator::AddTombstones(
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter,
    const InternalKey* smallest, const InternalKey* largest) {
  if (input_iter == nullptr || input_iter->empty()) return;

  // Wrap in TruncatedRangeDelIterator with file boundaries
  rep_.AddTombstones(std::make_unique<TruncatedRangeDelIterator>(
      std::move(input_iter), icmp_, smallest, largest));
}
```

**TruncatedRangeDelIterator** (`range_del_aggregator.cc:23-80`):
- **Purpose:** Ensures tombstones don't leak beyond SST file boundaries
- **Boundary truncation:** Clamps to `[smallest_, largest_)` (half-open interval) of source file
- **Sequence adjustment:** `largest.sequence -= 1` (line 73) in the general case, but skipped when:
  - The file boundary was artificially extended by a range tombstone (`kTypeRangeDeletion` with `kMaxSequenceNumber`)
  - `largest.sequence == 0` (no identical user key can exist in the next file)

⚠️ **INVARIANT:** Range tombstones truncated at file boundaries. Prevents deletion leakage across files.

### ShouldDelete Algorithm

**Location:** `range_del_aggregator.cc:186-212` (`ForwardRangeDelIterator::ShouldDelete`)

**Data structures:**
- `active_seqnums_`: multiset ordered by descending sequence (max seq on top)
- `active_iters_`: BinaryHeap ordered by end key (tracks when tombstones stop covering)
- `inactive_iters_`: BinaryHeap ordered by start key (tracks when tombstones start covering)

**Algorithm:**
```cpp
bool ForwardRangeDelIterator::ShouldDelete(const ParsedInternalKey& parsed) {
  // 1. Expire finished tombstones (lines 188-196)
  while (!active_iters_.empty() &&
         icmp_->Compare((*active_iters_.top())->end_key(), parsed) <= 0) {
    TruncatedRangeDelIterator* iter = PopActiveIter();
    iter->Next();  // Advance to next tombstone
    PushIter(iter, parsed);  // Re-insert if still valid
  }

  // 2. Activate starting tombstones (lines 198-207)
  while (!inactive_iters_.empty() &&
         icmp_->Compare(inactive_iters_.top()->start_key(), parsed) <= 0) {
    TruncatedRangeDelIterator* iter = PopInactiveIter();
    while (iter->Valid() && icmp_->Compare(iter->end_key(), parsed) <= 0) {
      iter->Next();
    }
    PushIter(iter, parsed);
  }

  // 3. Check highest sequence (lines 209-211)
  return active_seqnums_.empty()
             ? false
             : (*active_seqnums_.begin())->seq() > parsed.sequence;
}
```

**Complexity:** O(log k) per query amortized, where k = number of range tombstones

⚠️ **INVARIANT:** If tombstone with highest seqno in active set has `seq > key.sequence`, key is deleted.

### Tombstone Fragmentation

**FragmentedRangeTombstoneList** (`db/range_tombstone_fragmenter.h:101-104`):
- **Input:** Overlapping tombstones `[a,e)@10, [c,g)@15, [f,z)@5`
- **Output:** Non-overlapping fragments:
  ```
  [a,c)@10
  [c,e)@10,15    (two tombstones overlap)
  [e,f)@15
  [f,g)@15,5
  [g,z)@5
  ```
- **Benefits:** No overlaps, efficient seq lookup, compact storage

### Integration with Point Lookups

**Current mechanism:** Point lookups use `max_covering_tombstone_seq` rather than `RangeDelAggregator::ShouldDelete()`:
```cpp
// In TableCache::Get() (table_cache.cc:511-528):
// For each SST file, compute max covering tombstone sequence
SequenceNumber max_tomb_seq = tombstone_iter->MaxCoveringTombstoneSeqnum(user_key);
if (max_tomb_seq > *max_covering_tombstone_seq) {
  *max_covering_tombstone_seq = max_tomb_seq;
}

// In GetContext::SaveValue() (get_context.cc:281-291):
// If a point entry's sequence is below the covering tombstone, treat as deleted
if (*max_covering_tombstone_seq_ > parsed_key.sequence) {
  type = kTypeRangeDeletion;  // Convert to deletion semantics
}
```
This avoids building a full `RangeDelAggregator` for point lookups, using a simple sequence number comparison instead.

### Integration with Iterators

**MergingIterator construction** (using `AddPointAndTombstoneIterator`):
```cpp
// Add memtable point + range tombstone iterators together
auto mem_iter = super_version->mem->NewIterator(...);
auto mem_tombstone_iter = super_version->mem->NewRangeTombstoneIterator(...);
merge_iter_builder.AddPointAndTombstoneIterator(mem_iter, std::move(mem_tombstone_iter));

// Immutable memtables and SST files added via AddIterators()
super_version->imm->AddIterators(..., &merge_iter_builder, ...);
super_version->current->AddIterators(..., &merge_iter_builder, ...);
```

**MergingIterator::SkipNextDeleted():** Filters out point keys covered by range tombstones before `DBIter` sees them. `DBIter::FindNextUserEntry()` consumes the already-filtered stream from the internal iterator.

---

## 9. Merge Operator Resolution

**Location:** `db/merge_helper.h:35-295`, `db/merge_helper.cc`, `db/merge_context.h:23-149`

### MergeContext - Operand Collection

**Structure:** `db/merge_context.h:23-149`
```cpp
class MergeContext {
  std::unique_ptr<std::vector<Slice>> operand_list_;
  bool operands_reversed_ = true;  // Newest first

  void PushOperand(const Slice& operand, bool pinned);
  const std::vector<Slice>& GetOperands();  // Returns oldest→newest
};
```

**Collection order:** Newest→oldest (reversed internally)
**Presentation order:** Oldest→newest (for merge operator)

### Resolution Stages

**1. In Memtable During Get()** (`memtable.cc:1376-1384`):
```cpp
case kTypeMerge: {
  Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
  merge_context_->PushOperand(v, ...);

  *(s->merge_in_progress) = true;
  *(s->found_final_value) = HandleTypeMerge(...);

  return !*(s->found_final_value);  // Continue if not done
}
```
When base value (Put) found later:
```cpp
if (merge_in_progress) {
  TimedFullMerge(merge_operator, user_key, &value,
                 merge_context->GetOperands(), ...);
}
```

**2. During Iteration in DBIter** (`db_iter.cc:631-748`):
```cpp
// MergeValuesNewToOld()
merge_context_.PushOperand(iter_.value(), ...);

iter_.Next();
while (iter_.Valid() && user_key_matches) {
  if (ikey.type == kTypeMerge) {
    merge_context_.PushOperand(iter_.value(), ...);
    iter_.Next();
  } else if (ikey.type == kTypeValue) {
    // Full merge with base value
    return MergeWithPlainBaseValue(value, ikey.user_key);
  } else {
    break;  // Deletion or different key
  }
}

// No base value - full merge without base
s = MergeHelper::TimedFullMerge(
    merge_operator_, user_key, nullptr,
    merge_context_.GetOperands(), &value_, ...);
```

**3. During Compaction in MergeHelper** (`merge_helper.cc:256-648`):
```cpp
// MergeUntil() - collect operands until Put/Delete
merge_context_.Clear();

while (iter->Valid()) {
  if (ikey.type == kTypeMerge) {
    merge_context_.PushOperand(value_slice, ...);
    iter->Next();
  } else if (ikey.type == kTypeValue) {
    // Full merge
    s = TimedFullMerge(user_merge_operator_, ikey.user_key, &value_slice,
                       merge_context_.GetOperands(), ...);
    break;
  } else {
    // Deletion or snapshot boundary
    break;
  }
}
```

### Full Merge vs Partial Merge

**Full Merge** (base value + operands → result):
```cpp
// merge_helper.cc:109-245
success = merge_operator->FullMergeV3(merge_in, &merge_out);
```
Returns kTypeValue or kTypeWideColumnEntity

**Partial Merge** (operands → fewer operands):
```cpp
// merge_helper.cc:622-643
if (merge_context_.GetNumOperands() >= 2 ||
    (allow_single_operand_ && merge_context_.GetNumOperands() == 1)) {
  merge_success = user_merge_operator_->PartialMergeMulti(
      orig_ikey.user_key,
      std::deque<Slice>(merge_context_.GetOperands().begin(),
                        merge_context_.GetOperands().end()),
      &merge_result, logger_);

  if (merge_success) {
    // Replace N operands with 1 merged result
    merge_context_.Clear();
    merge_context_.PushOperand(merge_result);
  }
}
```

**Threshold:** Partial merge is attempted when operand count >= 2 (or 1 if `MergeOperator::AllowSingleOperand()` returns true)
- Triggered during compaction in `MergeHelper::MergeUntil()` when no base value is found
- Note: `max_successive_merges` is a separate write-path optimization (in `write_batch.cc`) that eagerly merges during memtable insertion — it does not control compaction-side partial merges

### Snapshot Integration

**Location:** `merge_helper.cc:347-355`

**Snapshot boundaries prevent merge:**
```cpp
if (stop_before > 0 && ikey.sequence <= stop_before &&
    snapshot_checker_->CheckInSnapshot(ikey.sequence, stop_before) !=
        SnapshotCheckerResult::kNotInSnapshot) {
  // Can't merge across snapshot boundary
  break;
}
```

⚠️ **INVARIANT:** Cannot merge operands across snapshot boundaries. Must preserve individual operands visible to snapshots.

---

## 10. Snapshot Isolation

**Mechanism:** Sequence numbers provide snapshot isolation

### Sequence Number Assignment

**Location:** See [write_flow.md](write_flow.md) for sequence number assignment during writes

**Key properties:**
- Monotonically increasing (assigned by WriteThread leader)
- Every write (Put/Delete/Merge) gets unique sequence number
- Snapshots capture current `last_sequence_` value

### Visibility Check

**In memtable SaveValue** (`memtable.cc:1220-1222`):
```cpp
if (!s->CheckCallback(seq)) {
  return true;  // Not visible to snapshot, continue iteration
}
```

**In DBIter** (`db_iter.cc:311-312`):
```cpp
bool IsVisible(SequenceNumber sequence, const Slice& ts, bool* more_recent);
```
Only yields keys with `seq <= sequence_` (snapshot seqno)

**In Version::Get:**
- `GetContext` initialized with snapshot sequence
- Only values with `seq <= snapshot_seq` considered visible

⚠️ **INVARIANT:** Only keys with `sequence <= snapshot_sequence` are visible. Ensures consistent point-in-time reads.

---

## Key Invariants Summary

| Invariant | Enforcement | Impact |
|-----------|-------------|--------|
| **SuperVersion refcounting** | Acquire before read, release after | Prevents use-after-free of memtables/SST files |
| **Snapshot visibility** | `seq <= snapshot_seq` | Ensures consistent reads across all layers |
| **Lookup order: mem → imm → SST** | Enforced in GetImpl/MultiGetImpl | Newest data found first |
| **L0 epoch ordering** | Newest first by epoch_number | Correct read ordering despite overlapping ranges |
| **L1+ non-overlapping** | Compaction maintains disjoint internal-key ranges | Binary search works; adjacent files may share a user key at boundaries |
| **InternalKey ordering** | user_key ASC, sequence DESC, type DESC | Newest version found first during iteration |
| **Range tombstone truncation** | Clamp to `[smallest, largest)` | Prevents deletion leakage across files |
| **Merge operand order** | Collected newest→oldest | Correct merge operator semantics |
| **Iterator stability** | SuperVersion ref held | Consistent view, files not deleted during iteration |
| **Block pinning** | PinnedIteratorsManager | Zero-copy value access |
| **Cache key uniqueness** | Global uniqueness via db_id + session_id + file_number + offset | Prevents incorrect data returns |
| **Snapshot isolation** | Sequence number filtering | Point-in-time consistent reads |

---

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| **Get() memtable** | O(log N) | Skiplist seek + bloom filter check |
| **Get() SST (L1+)** | O(log L × log F) | L levels, F files per level, binary search |
| **Get() SST (L0)** | O(F) | F L0 files, check all |
| **Block cache lookup** | O(1) expected | Hash table (LRU/ClockCache) |
| **MultiGet (batch)** | O(B log N) amortized | B keys, shared blocks, coalesced I/O |
| **Iterator Seek** | O(log N) | Binary search in blocks, index |
| **Iterator Next** | O(1) amortized | Sequential scan, seek optimization when skipping versions |
| **RangeDelAggregator ShouldDelete** | O(log K) amortized | K range tombstones, heap operations |
| **Bloom filter check** | O(k) | k hash functions, typically 10 |

---

## Interactions With Other Components

- **Write Path** ([write_flow.md](write_flow.md)): Writes assign sequence numbers, populate memtables, trigger flushes when memtable full
- **Version Management** ([version_management.md](version_management.md)): SuperVersion refs Version (SST file set), LogAndApply atomically updates Version
- **SST Table Format** ([sst_table_format.md](sst_table_format.md)): BlockBasedTable provides Get/MultiGet/NewIterator interfaces, block cache integration
- **Compaction** ([compaction.md](compaction.md)): Maintains L1+ non-overlapping invariant, resolves merges permanently
- **Cache** ([cache.md](cache.md)): LRUCache/HyperClockCache cache blocks, SecondaryCache provides compressed tier
- **File I/O** ([file_io.md](file_io.md)): ReadBlockContents reads from disk, FilePrefetchBuffer optimizes sequential access
- **Blob DB** ([blob_db.md](blob_db.md)): BlobDB value retrieval for blob-separated storage

---

## Common Read Patterns

### Hot Key (Cached)
```
Get(key) → mem (miss) → imm (miss) → Version::Get()
  → TableCache (hit on pinned reader)
  → BlockCache (hit on data block)
  → return value
```
**Latency:** ~1-2μs (all in-memory)

### Cold Key (Uncached)
```
Get(key) → mem (miss) → imm (miss) → Version::Get()
  → L0 bloom (negative) → L1 bloom (negative) → ... → Ln bloom (positive)
  → BlockCache miss on index → disk read index
  → BlockCache miss on data → disk read data → decompress
  → insert into cache → return value
```
**Latency:** ~100μs - 1ms (disk I/O latency)

### Range Scan (Iterator)
```
NewIterator() → SuperVersion → MergingIterator
  → Seek(start_key) → Next() × N
  → Block pinning avoids repeated cache lookups
  → Prefetching optimizes sequential I/O
```
**Throughput:** ~100K - 1M keys/sec (depending on value size, cache hit rate)

---

## Debugging and Observability

**Statistics:**
- `MEMTABLE_HIT` / `MEMTABLE_MISS`: Memtable hit rate
- `BLOCK_CACHE_HIT` / `BLOCK_CACHE_MISS`: Block cache hit rate
- `BLOOM_FILTER_USEFUL`: Bloom filter saved disk reads
- `NUMBER_OF_RESEEKS_IN_ITERATION`: Iterator seek optimizations triggered
- `GET_HIT_L0` / `GET_HIT_L1` / ...: Per-level hit rates

**PerfContext:** Thread-local detailed performance counters
- `block_cache_hit_count` / `block_cache_miss_count`
- `bloom_memtable_hit_count` / `bloom_sst_hit_count`
- `internal_key_skipped_count` / `internal_delete_skipped_count`

**Logging:**
- `info_log_level = DEBUG_LEVEL` enables verbose logging
- `Statistics::ToString()` dumps all counters

---

## References

- **Architecture Overview:** [ARCHITECTURE.md](../../ARCHITECTURE.md)
- **Write Path:** [write_flow.md](write_flow.md)
- **Version Management:** [version_management.md](version_management.md)
- **SST Table Format:** [sst_table_format.md](sst_table_format.md)
- **Compaction:** [compaction.md](compaction.md)
- **Cache:** [cache.md](cache.md)
- **File I/O:** [file_io.md](file_io.md)
- **Blob DB:** [blob_db.md](blob_db.md)
- **DB Impl:** [db_impl.md](db_impl.md)
