# Iterator Architecture

## Overview

RocksDB's iterator architecture provides a unified abstraction for traversing data across multiple sources (memtables, SST files, levels) while handling deletions, merges, snapshots, and range tombstones. The design uses a two-tier hierarchy:

1. **InternalIterator**: Used internally for LSM operations, works with internal keys (user_key + sequence + type)
2. **Iterator**: User-facing API that presents deduplicated user keys with snapshot isolation

Iterators form a tree structure where leaf iterators read actual data (memtables, SST blocks) and composite iterators merge results from multiple children (MergingIterator, LevelIterator, DBIter).

```
                    DBIter (user-facing)
                        |
                  MergingIterator (merge across sources)
                    /    |    \
                   /     |     \
             MemTable  L0 Files  LevelIterators (L1-L6)
                                    |
                              BlockBasedTableIterator
                                 /        \
                            IndexIter   DataBlockIter
```

## Iterator Hierarchy

### InternalIteratorBase<TValue>

`table/internal_iterator.h`

The template base class for all internal iterators. Provides core navigation methods and optional optimizations for performance.

```cpp
template <class TValue = Slice>
class InternalIteratorBase : public Cleanupable {
 public:
  // Position methods
  virtual void SeekToFirst() = 0;
  virtual void SeekToLast() = 0;
  virtual void Seek(const Slice& target) = 0;
  virtual void SeekForPrev(const Slice& target) = 0;
  virtual void Next() = 0;
  virtual void Prev() = 0;

  // State methods
  virtual bool Valid() const = 0;
  virtual Slice key() const = 0;
  virtual TValue value() const = 0;
  virtual Status status() const = 0;

  // Optional optimizations
  virtual bool PrepareValue() { return true; }
  virtual bool NextAndGetResult(IterateResult* result);

  // Pinning support (zero-copy when ReadOptions::pin_data = true)
  virtual bool IsKeyPinned() const { return false; }
  virtual bool IsValuePinned() const { return false; }
  virtual void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {}

  // Write time support
  virtual uint64_t write_unix_time() const {
    return std::numeric_limits<uint64_t>::max();
  }
};

using InternalIterator = InternalIteratorBase<Slice>;
```

**Key Methods:**

- **Seek(target)**: Position at first key >= target (forward scan)
- **SeekForPrev(target)**: Position at last key <= target (reverse scan)
- **PrepareValue()**: Deferred value loading optimization. When `ReadOptions::allow_unprepared_value = true`, iterator can defer expensive value reads until explicitly needed. Returns false on error.
- **NextAndGetResult()**: Combined Next() + bounds check optimization. Returns `IterateResult` with key, whether upper bound was reached, and value preparation status. Avoids repeated virtual calls.

⚠️ **INVARIANT**: If `Valid() == true`, then `status().ok() == true`. If `Valid() == false`, check `status()` to distinguish end-of-iteration from error.

⚠️ **INVARIANT**: Keys must be strictly ordered according to the comparator. Violating this will cause undefined behavior in MergingIterator.

⚠️ **INVARIANT**: After calling `Next()`, `Prev()`, `Seek()`, or `SeekForPrev()`, the iterator must be in a consistent state. `Valid()` and `status()` must reflect the current position.

### Iterator (User-Facing)

`include/rocksdb/iterator.h`

The public API exposed to RocksDB users. Operates on user keys (without sequence numbers) with snapshot isolation.

```cpp
class Iterator : public IteratorBase {
 public:
  // Inherited: Seek, SeekToFirst, SeekToLast, Next, Prev, Valid, key, value, status

  // Wide-column support
  virtual const WideColumns& columns() const;

  // Timestamp support
  virtual Slice timestamp() const {
    assert(false);
    return Slice();
  }

  // Iterator properties
  virtual Status GetProperty(std::string property_name, std::string* prop);
  // Properties: "rocksdb.iterator.is-key-pinned"
  //             "rocksdb.iterator.is-value-pinned"
  //             "rocksdb.iterator.write-time"

  // Prepare support for multi-scan
  virtual void Prepare(const MultiScanArgs& scan_opts);
};
```

**Thread Safety:**
- Multiple threads can call const methods (e.g., `key()`, `value()`) concurrently if no thread calls non-const methods
- Non-const methods (`Seek()`, `Next()`, etc.) require external synchronization

**Typical Usage:**
```cpp
std::unique_ptr<Iterator> it(db->NewIterator(read_options));
for (it->Seek(target); it->Valid(); it->Next()) {
  Slice key = it->key();
  Slice value = it->value();
  // Process key-value pair
}
if (!it->status().ok()) {
  // Handle error
}
```

### IteratorWrapper

`table/iterator_wrapper.h`

A critical performance optimization layer that wraps InternalIterator to reduce virtual call overhead.

```cpp
template <class TValue = Slice>
class IteratorWrapperBase {
 private:
  InternalIteratorBase<TValue>* iter_;
  IterateResult result_;  // Cached key and status
  bool valid_;            // Cached validity

 public:
  // Inline methods avoid virtual calls on hot path
  bool Valid() const { return valid_; }
  Slice key() const { return result_.key; }

  void Next() {
    iter_->Next();
    Update();  // Update cached state
  }

  void Seek(const Slice& target) {
    iter_->Seek(target);
    Update();
  }
};
```

**Optimization**: By caching `valid_` and `key`, typical iteration loops avoid two virtual calls per iteration:

```cpp
// Without IteratorWrapper: 3 virtual calls per iteration
while (iter->Valid()) {           // Virtual call
  Slice key = iter->key();        // Virtual call
  // ...
  iter->Next();                   // Virtual call
}

// With IteratorWrapper: 1 virtual call per iteration
while (wrapper.Valid()) {         // Inline (cached)
  Slice key = wrapper.key();      // Inline (cached)
  // ...
  wrapper.Next();                 // Virtual call + Update()
}
```

This improves CPU cache locality and reduces instruction cache pressure, providing 5-10% performance improvement in scan-heavy workloads.

## MemTable Iterators

`db/memtable.h`, `memtable/skiplist.h`

MemTable iterators provide in-memory data access with no I/O overhead.

```cpp
class MemTable {
 public:
  InternalIterator* NewIterator(
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping,
      Arena* arena,
      bool use_range_del_table = false);
};
```

**Implementation**: Typically wraps `MemTableRep::Iterator`, which for the default SkipList implementation is a `SkipList<...>::Iterator`.

**Key Characteristics:**
- **No I/O**: All data is in memory
- **Lock-free reads**: SkipList uses lock-free algorithms for concurrent reads
- **Prefix bloom**: When `prefix_extractor` is configured, can skip entire memtable if prefix doesn't exist
- **Snapshot support**: Returns keys visible to the specified snapshot sequence number

**SkipList Iterator**:
```cpp
// Seek implementation (simplified)
template <class Comparator>
void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target);
}

// Next is a simple pointer hop
void Next() {
  assert(Valid());
  node_ = node_->Next(0);  // Follow level-0 pointer
}
```

⚠️ **INVARIANT**: MemTable iterators become invalid after the MemTable is deleted. Users must ensure MemTable lifetime exceeds iterator lifetime (typically via Version pinning).

## Block Iterators

### DataBlockIter

`table/block_based/data_block_hash_index.h`, `table/block_based/block.h`

Iterates within a single data block using restart points for efficient binary search.

**Data Block Format:**
```
[Record 0]
[Record 1]
[Record 2]
...
[Record N-1]
[Restart Point 0: uint32] <- Offset of Record 0
[Restart Point 1: uint32] <- Offset of Record 16
...
[Num Restart Points: uint32]
```

Each record uses delta encoding:
```
[shared_key_len: varint32]
[unshared_key_len: varint32]
[value_len: varint32]
[unshared_key_bytes: char[unshared_key_len]]
[value_bytes: char[value_len]]
```

**Restart Points**: Every N keys (default 16) is stored as a full key (shared_len = 0), creating a restart point. This enables binary search.

**Seek Algorithm:**
```cpp
// 1. Binary search restart points to find the restart point <= target
uint32_t left = 0, right = num_restarts_ - 1;
while (left < right) {
  uint32_t mid = (left + right + 1) / 2;
  SeekToRestartPoint(mid);
  if (CompareCurrentKey(target) < 0) {
    left = mid;
  } else {
    right = mid - 1;
  }
}

// 2. Linear scan from restart point to target
SeekToRestartPoint(left);
while (ParseNextKey() && CompareCurrentKey(target) < 0) {}
```

**Hash Index Optimization**: For prefix-seekable tables with `BlockBasedTableOptions::data_block_hash_index_enable = true`, the block includes a hash map for prefix seeks:

```cpp
// Hash-based seek (when prefix matches)
uint32_t* hash_map = GetHashMap();
uint32_t hash = Hash(ExtractPrefix(target));
uint32_t restart_idx = hash_map[hash % num_buckets];
if (restart_idx != kNoEntry) {
  SeekToRestartPoint(restart_idx);
  // Linear scan within prefix
}
```

⚠️ **INVARIANT**: Block data must remain valid (pinned) for iterator lifetime. Managed by BlockBasedTableIterator via BlockContents pinning.

### BlockBasedTableIterator

`table/block_based/block_based_table_iterator.h`

Implements a two-level iterator: index block → data blocks.

```cpp
class BlockBasedTableIterator : public InternalIteratorBase<Slice> {
 private:
  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter_;  // Points to index entries
  DataBlockIter block_iter_;       // Iterates current data block
  BlockPrefetcher block_prefetcher_;  // Readahead
  bool is_at_first_key_from_index_;  // Lazy block loading flag
};
```

**State Machine:**

```
[Initial State]
    |
    v
Seek(target) ──> index_iter_.Seek(target)
                 is_at_first_key_from_index_ = true
                 block_iter_ NOT loaded yet
    |
    v
PrepareValue() ──> if (is_at_first_key_from_index_):
                      InitDataBlock()  // Load block into cache/memory
                      block_iter_.SeekToFirst()
                      is_at_first_key_from_index_ = false
    |
    v
value() ──> block_iter_.value()
```

**Lazy Loading**: To avoid loading blocks when only checking keys or when upper bound check will skip the block, data block loading is deferred until `PrepareValue()`.

**Seek Implementation (Simplified):**
```cpp
void BlockBasedTableIterator::Seek(const Slice& target) {
  // 1. Seek index to find block containing target
  index_iter_.Seek(target);
  if (!index_iter_.Valid()) {
    ResetDataIter();
    return;
  }

  // 2. Mark that we're at first key from index (lazy loading)
  is_at_first_key_from_index_ = true;

  // 3. Check prefix bloom filter if enabled
  if (check_filter_ && !CheckPrefixMayMatch(target)) {
    ResetDataIter();
    return;
  }

  // 4. Data block will be loaded on first PrepareValue() call
}

bool BlockBasedTableIterator::PrepareValue() {
  if (is_at_first_key_from_index_) {
    InitDataBlock();  // Load block, seek to first key
    if (!block_iter_.Valid()) return false;
    is_at_first_key_from_index_ = false;
  }
  return block_iter_.PrepareValue();
}
```

**Prefix Bloom Filter Check:**
```cpp
bool CheckPrefixMayMatch(const Slice& target) {
  Slice prefix = prefix_extractor_->Transform(target);
  BlockHandle filter_handle = index_iter_.GetCurrentFilterHandle();
  FilterBlockReader* filter = GetFilterReader();
  return filter->PrefixMayMatch(prefix, filter_handle);
}
```

**Upper Bound Optimization**: When `ReadOptions::iterate_upper_bound` is set, the iterator can skip entire data blocks:

```cpp
// In NextImpl()
if (block_upper_bound_check_ &&
    user_comparator_.Compare(index_iter_.key(), *upper_bound_) >= 0) {
  // Current block's smallest key >= upper bound, we're done
  ResetDataIter();
  return;
}
```

⚠️ **INVARIANT**: `index_iter_` and `block_iter_` must be kept in sync. When `index_iter_` advances, `block_iter_` must be invalidated or updated.

⚠️ **INVARIANT**: `is_at_first_key_from_index_` is true only when positioned at first key of a block that hasn't been loaded yet. Must be cleared by InitDataBlock().

## Two-Level Iterator

`table/two_level_iterator.h`

A general pattern where a first-level iterator yields handles/keys, and second-level iterators are created on-demand to iterate the referenced data.

```cpp
class TwoLevelIterator : public InternalIterator {
 private:
  std::unique_ptr<InternalIteratorBase<IndexValue>> first_level_iter_;
  std::unique_ptr<InternalIterator> second_level_iter_;
  TwoLevelIteratorState* state_;  // Factory for second-level iterators
};

class TwoLevelIteratorState {
 public:
  virtual InternalIteratorBase<IndexValue>* NewSecondaryIterator(const BlockHandle& handle) = 0;
};
```

**Usage Examples:**
1. **Partitioned Indexes**: First level iterates index partitions, second level reads each partition's index blocks
2. **Partitioned Filters**: First level selects filter partition, second level reads filter blocks
3. **BlockBasedTableIterator**: Conceptually a two-level iterator (index → data blocks), but optimized with custom logic

**Seek Flow:**
```cpp
void TwoLevelIterator::Seek(const Slice& target) {
  // 1. Seek first level
  first_level_iter_->Seek(target);
  InitDataBlock();  // Create second-level iterator
  if (!second_level_iter_) return;

  // 2. Seek within second level
  second_level_iter_->Seek(target);
  SkipEmptyDataBlocksForward();
}

void InitDataBlock() {
  if (!first_level_iter_->Valid()) {
    SetSecondLevelIterator(nullptr);
    return;
  }

  BlockHandle handle = first_level_iter_->value().handle;

  // Create second-level iterator
  InternalIteratorBase<IndexValue>* iter = state_->NewSecondaryIterator(handle);
  SetSecondLevelIterator(iter);
}
```

## Level Iterator

`table/block_based/block_based_table_reader.cc` (LevelIterator pattern)

Concatenates multiple SST file iterators within a single LSM level. Maintains current file iterator and switches files as iteration progresses.

```cpp
// Conceptual interface (actual implementation inlined in various places)
class LevelIterator : public InternalIterator {
 private:
  std::vector<FileMetaData*> files_;  // Sorted files in level
  size_t file_index_;                 // Current file
  InternalIterator* file_iter_;       // Current file's iterator
  TableCache* table_cache_;           // For opening file iterators
};
```

**Seek Algorithm:**
```cpp
void LevelIterator::Seek(const Slice& target) {
  // 1. Binary search files to find file possibly containing target
  file_index_ = FindFile(files_, target);

  // 2. Open file iterator
  file_iter_ = table_cache_->NewIterator(
      read_options_, files_[file_index_]);

  // 3. Seek within file
  file_iter_->Seek(target);

  // 4. Skip to next file if current exhausted
  while (file_iter_->Valid() == false && file_index_ + 1 < files_.size()) {
    file_index_++;
    file_iter_ = table_cache_->NewIterator(
        read_options_, files_[file_index_]);
    file_iter_->SeekToFirst();
  }
}
```

**Next() Implementation:**
```cpp
void LevelIterator::Next() {
  file_iter_->Next();

  // If current file exhausted, move to next file
  if (!file_iter_->Valid() && file_index_ + 1 < files_.size()) {
    file_index_++;
    delete file_iter_;
    file_iter_ = table_cache_->NewIterator(
        read_options_, files_[file_index_]);
    file_iter_->SeekToFirst();
  }
}
```

**Sentinel Keys for Range Tombstones**: To prevent premature file switching when range tombstones extend beyond file boundaries, special sentinel keys mark tombstone start/end:

```cpp
bool IsDeleteRangeSentinelKey(const Slice& ikey) {
  // Check if key is a range tombstone boundary marker
  ParsedInternalKey parsed;
  ParseInternalKey(ikey, &parsed);
  return parsed.type == kTypeRangeDeletion && IsMaxSequenceNumber(parsed.sequence);
}

// In Next(): Don't switch files on sentinel keys
if (!file_iter_->Valid() && !IsDeleteRangeSentinelKey(file_iter_->key())) {
  AdvanceToNextFile();
}
```

⚠️ **INVARIANT**: Files within a level are non-overlapping (for L1+) or sorted by smallest key (for L0 in level-based compaction). This ensures correct iteration order.

## Merging Iterator

`table/merging_iterator.h`, `table/merging_iterator.cc`

The most complex iterator, merging outputs from multiple child iterators (memtables, levels) using min/max heaps while handling range tombstones.

```cpp
class MergingIterator : public InternalIterator {
 private:
  struct HeapItem {
    size_t level;
    IteratorWrapper iter;
    enum Type { ITERATOR, DELETE_RANGE_START, DELETE_RANGE_END };
    Type type;
    ParsedInternalKey tombstone_pik;  // For range tombstone events
  };

  std::vector<HeapItem> children_;
  BinaryHeap<HeapItem*, MinHeapItemComparator> minHeap_;  // Forward iteration
  BinaryHeap<HeapItem*, MaxHeapItemComparator> maxHeap_;  // Reverse iteration

  // Range tombstone management
  std::vector<TruncatedRangeDelIterator*> range_tombstone_iters_;
  std::set<size_t> active_;  // Levels with active range tombstones
};
```

### Heap Structure

**MinHeapItemComparator** (Forward Iteration):
```cpp
struct MinHeapItemComparator {
  bool operator()(const HeapItem* a, const HeapItem* b) const {
    // First compare keys
    int cmp = comparator_->Compare(a->iter.key(), b->iter.key());
    if (cmp != 0) return cmp > 0;  // Min-heap: larger values at bottom

    // Tie-break: prioritize point keys over range tombstone events
    if (a->type != b->type) {
      return a->type > b->type;  // ITERATOR < DELETE_RANGE_START < DELETE_RANGE_END
    }

    // Tie-break by level (lower levels win for same key)
    return a->level > b->level;
  }
};
```

### Range Tombstone Handling

Range tombstones are integrated into the heap as special events:

```cpp
// When a range tombstone starts
HeapItem delete_range_start;
delete_range_start.type = HeapItem::DELETE_RANGE_START;
delete_range_start.tombstone_pik = ParseInternalKey(range_del->start_key());
minHeap_.push(&delete_range_start);

// When a range tombstone ends
HeapItem delete_range_end;
delete_range_end.type = HeapItem::DELETE_RANGE_END;
delete_range_end.tombstone_pik = ParseInternalKey(range_del->end_key());
minHeap_.push(&delete_range_end);
```

**Active Range Tombstones**:
```cpp
void PopDeleteRangeStart() {
  while (!minHeap_.empty() && minHeap_.top()->type == HeapItem::DELETE_RANGE_START) {
    HeapItem* item = minHeap_.top();
    active_.insert(item->level);  // Mark level as having active tombstone
    minHeap_.pop();
  }
}

void PopDeleteRangeEnd() {
  while (!minHeap_.empty() && minHeap_.top()->type == HeapItem::DELETE_RANGE_END) {
    HeapItem* item = minHeap_.top();
    active_.erase(item->level);  // Tombstone no longer active
    minHeap_.pop();
    // Advance range tombstone iterator for this level
    range_tombstone_iters_[item->level]->Next();
    InsertRangeTombstoneToMinHeap(item->level);
  }
}
```

### Finding Visible Keys

The core algorithm skips keys covered by range tombstones:

```cpp
void MergingIterator::FindNextVisibleKey() {
  // Process delete range start events
  PopDeleteRangeStart();

  while (!minHeap_.empty()) {
    HeapItem* item = minHeap_.top();

    // If top is a tombstone event, process it
    if (item->type != HeapItem::ITERATOR) {
      if (item->type == HeapItem::DELETE_RANGE_START) {
        PopDeleteRangeStart();
      } else {
        PopDeleteRangeEnd();
      }
      continue;
    }

    // Check if current key is deleted by any active range tombstone
    ParsedInternalKey pik;
    ParseInternalKey(item->iter.key(), &pik);

    bool deleted = false;
    for (size_t level : active_) {
      TruncatedRangeDelIterator* tombstone_iter = range_tombstone_iters_[level];
      if (tombstone_iter->Valid() &&
          PointKeyCoveredByRangeTombstone(pik, tombstone_iter)) {
        deleted = true;
        break;
      }
    }

    if (deleted) {
      // Skip this key
      item->iter.Next();
      if (item->iter.Valid()) {
        minHeap_.replace_top(item);  // Re-heapify
      } else {
        minHeap_.pop();
      }
      continue;
    }

    // Found a visible key
    current_key_ = item->iter.key();
    return;
  }

  // Heap empty, iteration complete
  valid_ = false;
}
```

### Cascading Seeks Optimization

Inspired by Pebble's optimization: when a range tombstone at level L covers the seek target, seek levels > L to the tombstone's end key instead of the target.

```cpp
void MergingIterator::SeekImpl(const Slice& target, bool cascading_seek) {
  // Seek all children
  for (auto& child : children_) {
    child.iter.Seek(target);
    minHeap_.push(&child);
  }

  // Seek range tombstone iterators
  Slice cascading_target = target;
  for (size_t level = 0; level < range_tombstone_iters_.size(); ++level) {
    range_tombstone_iters_[level]->Seek(cascading_target);

    if (range_tombstone_iters_[level]->Valid()) {
      InsertRangeTombstoneToMinHeap(level);

      // If this tombstone covers the target, cascade the seek
      if (cascading_seek &&
          PointKeyCoveredByRangeTombstone(target, range_tombstone_iters_[level])) {
        // Seek lower levels to tombstone end key
        cascading_target = range_tombstone_iters_[level]->end_key().user_key;
      }
    }
  }

  FindNextVisibleKey();
}
```

### Direction Switching

MergingIterator supports both forward and reverse iteration, but switching directions is expensive:

```cpp
void MergingIterator::SwitchToForward() {
  ClearHeaps();

  // Rebuild minHeap from maxHeap state
  for (auto& child : children_) {
    if (child.iter.Valid()) {
      // Move past the current key (which was already returned)
      child.iter.Next();
      if (child.iter.Valid()) {
        minHeap_.push(&child);
      }
    }
  }

  // Rebuild range tombstone state
  RebuildRangeTombstones();

  direction_ = kForward;
  FindNextVisibleKey();
}
```

⚠️ **INVARIANT** (Forward Iteration):
1. `minHeap_.top()->type == ITERATOR` (after FindNextVisibleKey)
2. `minHeap_.top()->iter.key()` is not covered by any active range tombstone
3. For all levels `i`, and levels `j <= i`: `range_tombstone_iters_[j].end_key() <= children_[i].key()`
4. `active_` contains exactly the levels with range tombstones covering the current position

⚠️ **INVARIANT** (Heap Property):
After each operation (Seek, Next, FindNextVisibleKey), the heap property must hold: parent <= all children (MinHeap) or parent >= all children (MaxHeap).

## DBIter (User-Facing Iterator)

`db/db_iter.h`, `db/db_iter.cc`

DBIter wraps InternalIterator to provide the user-facing Iterator API with snapshot isolation, merge resolution, and deletion handling.

```cpp
class DBIter : public Iterator {
 private:
  IteratorWrapper iter_;         // Underlying merging iterator
  SequenceNumber sequence_;      // Snapshot sequence number
  Direction direction_;          // kForward or kReverse

  IterKey saved_key_;            // Current user key being returned
  Slice value_;                  // Points to iter_ value or blob
  bool valid_;
  bool current_entry_is_merged_;

  MergeContext merge_context_;   // For collecting merge operands
  BlobReader blob_reader_;       // For reading blob values
  PinnedIteratorsManager pinned_iters_mgr_;  // When pin_data = true
};
```

### Core Responsibilities

1. **Snapshot Isolation**: Filter keys by sequence number
2. **User Key Deduplication**: Show only the latest version per user key
3. **Merge Operator**: Combine merge operands into final value
4. **Deletion Handling**: Skip tombstones (Delete, SingleDelete)
5. **Timestamp Filtering**: Apply timestamp bounds when user-defined timestamps enabled
6. **Blob Resolution**: Read blob values from blob files when blob_db enabled
7. **Wide Column Support**: Convert entities to WideColumns format

### Direction and Positioning

**Forward Iteration**:
- `iter_` points AT the current entry (or just after if merged)
- `saved_key_` contains the user key
- `value_` points to the value

**Reverse Iteration**:
- `iter_` positioned BEFORE all versions of `saved_key_`
- Must call `FindValueForCurrentKey()` to reconstruct value by scanning forward

```cpp
void DBIter::Prev() {
  assert(Valid());

  if (direction_ == kForward) {
    // Switching from forward to reverse
    SwitchIteratorDirection();
  }

  PrevInternal();
}

void DBIter::SwitchIteratorDirection() {
  // Currently at saved_key_ in forward direction
  // Need to position iter_ BEFORE all versions of saved_key_

  direction_ = kReverse;
  iter_->Prev();  // Move to previous user key's latest version

  while (iter_->Valid() &&
         user_comparator_.Equal(ExtractUserKey(iter_->key()), saved_key_.GetUserKey())) {
    iter_->Prev();  // Skip all versions of current key
  }

  // Now iter_ is before all versions of saved_key_
}
```

### Finding Current Value

When positioned at a user key, DBIter scans all internal keys for that user key to resolve the value:

```cpp
bool DBIter::FindValueForCurrentKey() {
  assert(iter_->Valid());

  merge_context_.Clear();
  has_current_user_key_ = false;
  current_entry_is_merged_ = false;

  // Scan all internal keys for this user key
  while (iter_->Valid()) {
    ParsedInternalKey ikey;
    ParseInternalKey(iter_->key(), &ikey);

    // Sequence number check
    if (ikey.sequence > sequence_) {
      iter_->Next();  // Skip invisible versions
      continue;
    }

    // Different user key? Stop.
    if (!user_comparator_.Equal(ikey.user_key, saved_key_.GetUserKey())) {
      break;
    }

    // Process based on type
    if (ikey.type == kTypeValue || ikey.type == kTypeWideColumnEntity) {
      if (timestamp_checker_.IsVisible(ikey.timestamp)) {
        value_ = iter_->value();
        has_current_user_key_ = true;
        return true;
      }
    } else if (ikey.type == kTypeMerge) {
      if (timestamp_checker_.IsVisible(ikey.timestamp)) {
        merge_context_.PushOperand(iter_->value());
      }
    } else if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion) {
      // Key deleted, stop
      has_current_user_key_ = false;
      return false;
    } else if (ikey.type == kTypeBlobIndex) {
      // Resolve blob value
      if (!ResolveBlobValue(iter_->value())) {
        return false;
      }
      has_current_user_key_ = true;
      return true;
    }

    iter_->Next();
  }

  // Apply accumulated merges
  if (merge_context_.GetNumOperands() > 0) {
    std::string merged_value;
    Status s = MergeHelper::TimedFullMerge(
        merge_operator_, saved_key_.GetUserKey(), nullptr,
        merge_context_.GetOperands(), &merged_value, logger_, statistics_,
        clock_, /* update_num_ops_stats */ true);
    if (!s.ok()) {
      status_ = s;
      return false;
    }
    value_ = merged_value;
    current_entry_is_merged_ = true;
    has_current_user_key_ = true;
    return true;
  }

  return false;
}
```

### Next() Implementation

```cpp
void DBIter::Next() {
  assert(Valid());

  if (direction_ == kReverse) {
    SwitchIteratorDirection();
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    }
  }

  // Save the current key to detect user key changes
  Slice prev_key = saved_key_.GetUserKey();

  // Skip all remaining versions of current user key
  do {
    iter_->Next();
  } while (iter_->Valid() &&
           user_comparator_.Equal(ExtractUserKey(iter_->key()), prev_key));

  // Find next valid entry
  FindNextUserEntry(false /* not skipping saved key */);
}

void DBIter::FindNextUserEntry(bool skipping_saved_key) {
  while (iter_->Valid()) {
    ParsedInternalKey ikey;
    ParseInternalKey(iter_->key(), &ikey);

    // Snapshot check
    if (ikey.sequence > sequence_) {
      iter_->Next();
      continue;
    }

    // Save user key
    saved_key_.SetUserKey(ikey.user_key);

    // Try to find value for this key
    if (FindValueForCurrentKey()) {
      valid_ = true;
      return;
    }

    // Key deleted or invisible, continue
    // (iter_ already advanced by FindValueForCurrentKey)
  }

  valid_ = false;
}
```

### Blob Resolution

When `enable_blob_files = true`, values may be stored externally:

```cpp
bool DBIter::ResolveBlobValue(const Slice& blob_index) {
  BlobIndex index;
  Status s = index.DecodeFrom(blob_index);
  if (!s.ok()) {
    status_ = s;
    return false;
  }

  PinnableSlice blob_value;
  s = blob_reader_.GetBlob(read_options_, index, &blob_value);
  if (!s.ok()) {
    status_ = s;
    return false;
  }

  value_ = blob_value.data();
  if (pinned_iters_mgr_) {
    pinned_iters_mgr_->PinSlice(blob_value);
  }

  return true;
}
```

⚠️ **INVARIANT** (Forward Direction): `iter_` is positioned at the current entry being returned, or just after if `current_entry_is_merged_ == true`.

⚠️ **INVARIANT** (Reverse Direction): `iter_` is positioned BEFORE all internal keys with user key equal to `saved_key_`. Must call `FindValueForCurrentKey()` to reconstruct value.

⚠️ **INVARIANT**: `valid_ == true` implies `has_current_user_key_ == true` and `status_.ok() == true`.

## Range Deletion Iterators

`db/range_del_aggregator.h`, `db/range_del_aggregator.cc`

Range tombstones (created by `DeleteRange()`) are stored separately and processed by specialized iterators.

### FragmentedRangeTombstoneIterator

Base iterator for non-overlapping range tombstone fragments.

```cpp
class FragmentedRangeTombstoneIterator : public InternalIterator {
 public:
  const RangeTombstone& Tombstone() const;

  ParsedInternalKey start_key() const;
  ParsedInternalKey end_key() const;
  SequenceNumber seq() const;
};
```

**Fragmentation**: Overlapping range tombstones are fragmented into non-overlapping pieces:

```
Original:
  [A-------D) seq=10
    [B---------E) seq=15
        [C---E) seq=20

Fragmented:
  [A--B) seq=10
      [B--C) seq=15
          [C--D) seq=20
              [D--E) seq=20
```

### TruncatedRangeDelIterator

Wraps FragmentedRangeTombstoneIterator with truncation to SST file bounds:

```cpp
class TruncatedRangeDelIterator {
 private:
  std::unique_ptr<FragmentedRangeTombstoneIterator> iter_;
  Slice smallest_;  // SST file's smallest key
  Slice largest_;   // SST file's largest key

 public:
  void Seek(const Slice& target);
  void SeekInternalKey(const Slice& internal_key);

  ParsedInternalKey start_key() const;
  ParsedInternalKey end_key() const;
  SequenceNumber seq() const;
};
```

**Truncation Example**:
```
SST File: [apple, grape]
Range Tombstone: [banana, mango) seq=100

Truncated: [banana, grape] seq=100
  (Truncated to [smallest, largest] intersection)
```

### ForwardRangeDelIterator

State machine for checking if point keys are deleted during forward iteration:

```cpp
class ForwardRangeDelIterator {
 private:
  struct ActiveSeqnoRange {
    SequenceNumber seq;
    Slice end_key;
  };

  BinaryHeap<ActiveSeqnoRange> active_seqnos_;  // Active tombstones by seqno
  BinaryHeap<RangeTombstone> inactive_by_end_;  // Tombstones to activate

 public:
  bool ShouldDelete(const ParsedInternalKey& pik);
};
```

**Algorithm**:
```cpp
bool ForwardRangeDelIterator::ShouldDelete(const ParsedInternalKey& pik) {
  // 1. Pop expired tombstones (end_key <= pik.user_key)
  while (!active_seqnos_.empty() &&
         comparator_.Compare(active_seqnos_.top().end_key, pik.user_key) <= 0) {
    active_seqnos_.pop();
  }

  // 2. Activate tombstones that now cover pik
  while (!inactive_by_end_.empty() &&
         comparator_.Compare(inactive_by_end_.top().start_key, pik.user_key) <= 0) {
    RangeTombstone rt = inactive_by_end_.top();
    inactive_by_end_.pop();
    active_seqnos_.push({rt.seq, rt.end_key});
  }

  // 3. Check if covered by highest-seqno active tombstone
  if (!active_seqnos_.empty() && active_seqnos_.top().seq > pik.sequence) {
    return true;  // Deleted
  }

  return false;
}
```

⚠️ **INVARIANT**: Range tombstone fragments within a file are non-overlapping and sorted by start key.

⚠️ **INVARIANT**: `ShouldDelete()` must be called with monotonically increasing keys (within a scan direction).

## Compaction Iterator

`db/compaction/compaction_iterator.h`, `db/compaction/compaction_iterator.cc`

CompactionIterator is NOT an InternalIterator—it's a special-purpose stateful processor for compaction.

```cpp
class CompactionIterator {
 private:
  SequenceIterWrapper input_;  // Wraps InternalIterator, counts entries
  const Compaction* compaction_;
  const CompactionFilter* compaction_filter_;
  MergeHelper merge_helper_;
  RangeDelAggregator range_del_agg_;
  BlobGarbageCollectionPolicy blob_gc_policy_;

  // State
  enum class State {
    kNotInitialized,
    kAtNonMergeEntry,
    kAtMerge,
    kOutputtingMerge,
  };
  State state_;

 public:
  void SeekToFirst();
  void Next();
  bool Valid() const;
  const Slice& key() const;
  const Slice& value() const;

  CompactionIteratorStats stats_;
};
```

### Compaction Processing

CompactionIterator performs these transformations:

1. **Drop keys invisible to snapshots**: If key's sequence number is below all snapshots and not the latest version, drop it
2. **Merge consecutive merge operands**: Combine merge operands into a single value
3. **Apply compaction filter**: User-defined filtering (e.g., TTL)
4. **Handle SingleDelete**: Match SingleDelete with corresponding Put
5. **Zero sequence numbers at bottommost level**: Keys at bottom level get sequence = 0 for better compression
6. **Blob GC**: Rewrite blob references if blob file is being GCed
7. **Timestamp GC**: Drop history older than `full_history_ts_low`

```cpp
void CompactionIterator::Next() {
  while (input_.Valid()) {
    ParsedInternalKey ikey;
    ParseInternalKey(input_.key(), &ikey);

    // 1. Check if deleted by range tombstone
    if (range_del_agg_.ShouldDelete(ikey, bottommost_level_)) {
      input_.Next();
      stats_.num_deleted_by_range_tombstone++;
      continue;
    }

    // 2. Snapshot visibility check
    SequenceNumber latest_snapshot = GetLatestSnapshot();
    if (ikey.sequence <= latest_snapshot &&
        !IsLatestVersionForUserKey(ikey.user_key)) {
      // Not visible to any snapshot and not latest version, drop
      input_.Next();
      stats_.num_dropped_internal_keys++;
      continue;
    }

    // 3. Compaction filter
    if (compaction_filter_ &&
        compaction_filter_->Filter(level_, ikey, input_.value()) == kRemove) {
      input_.Next();
      stats_.num_dropped_by_compaction_filter++;
      continue;
    }

    // 4. Process merge operands
    if (ikey.type == kTypeMerge) {
      ProcessMerge(ikey);
      continue;
    }

    // 5. Zero seqno at bottommost level
    if (bottommost_level_ && ikey.sequence < latest_snapshot) {
      ikey.sequence = 0;
      ikey.type = kTypeValue;  // Normalize
      UpdateKey(ikey);
    }

    // Output this entry
    current_key_ = input_.key();
    current_value_ = input_.value();
    input_.Next();
    return;
  }

  valid_ = false;
}
```

### Blob Garbage Collection

When a blob file is being GCed (e.g., too much garbage), CompactionIterator rewrites blob indexes:

```cpp
void CompactionIterator::ProcessBlobIndex() {
  BlobIndex blob_index;
  blob_index.DecodeFrom(input_.value());

  if (blob_gc_policy_.ShouldGC(blob_index.file_number())) {
    // Read blob value
    PinnableSlice blob_value;
    blob_file_reader_->Get(blob_index, &blob_value);

    // Write to new blob file
    BlobIndex new_index;
    blob_file_builder_->Add(key(), blob_value, &new_index);

    // Update value
    current_value_ = new_index.Encode();
    stats_.num_blobs_relocated++;
  }
}
```

⚠️ **INVARIANT**: CompactionIterator must be called with a sorted InternalIterator input (enforced by compaction's MergingIterator).

⚠️ **INVARIANT**: SingleDelete must be matched with exactly one Put/Delete. Violating this causes undefined behavior.

## Seek Optimizations

### Prefix Bloom Filters

When `Options::prefix_extractor` is set, RocksDB can skip seeks to non-existent prefixes:

```cpp
// In BlockBasedTableIterator::Seek()
if (prefix_extractor_) {
  Slice prefix = prefix_extractor_->Transform(target);

  // Check whole-file prefix filter
  if (filter_policy_ && !filter_policy_->KeyMayMatch(prefix)) {
    // Prefix definitely doesn't exist in this file
    ResetDataIter();
    return;
  }

  // Check partition filter (if partitioned)
  BlockHandle filter_handle = index_iter_.value().filter_handle;
  if (!filter_->PrefixMayMatch(prefix, filter_handle)) {
    // Prefix doesn't exist in this index partition
    ResetDataIter();
    return;
  }
}
```

### Monotonic Seeks (Tailing Iterator)

When `ReadOptions::tailing = true`, DBIter can reuse its position for subsequent seeks:

```cpp
void DBIter::Seek(const Slice& target) {
  if (tailing_ && valid_ &&
      user_comparator_.Compare(target, saved_key_.GetUserKey()) >= 0) {
    // Target is after current position, just continue from here
    while (valid_ && user_comparator_.Compare(key(), target) < 0) {
      Next();
    }
    return;
  }

  // Normal seek
  iter_->Seek(EncodeInternalKey(target, sequence_, kValueTypeForSeek));
  FindNextUserEntry(false);
}
```

### Adaptive Readahead

`table/block_based/block_prefetcher.h`

BlockPrefetcher dynamically adjusts readahead size based on access patterns:

```cpp
class BlockPrefetcher {
 private:
  size_t readahead_size_;       // Current readahead size
  size_t initial_readahead_size_;
  size_t max_readahead_size_;
  uint64_t num_file_reads_;
  uint64_t num_sequential_reads_;

 public:
  void Prefetch(uint64_t offset, size_t length);
};

void BlockPrefetcher::Prefetch(uint64_t offset, size_t length) {
  num_file_reads_++;

  // Detect sequential access
  if (offset == prev_offset_ + prev_length_) {
    num_sequential_reads_++;

    // Increase readahead size if sequential
    if (num_sequential_reads_ > 2) {
      readahead_size_ = std::min(readahead_size_ * 2, max_readahead_size_);
    }
  } else {
    // Random access, reset
    num_sequential_reads_ = 0;
    readahead_size_ = initial_readahead_size_;
  }

  // Issue prefetch
  if (readahead_size_ > 0) {
    file_->Prefetch(offset, length + readahead_size_);
  }
}
```

### Block Cache Lookup for Readahead Sizing

`BlockCacheLookupForReadAheadSize()` probes the block cache to estimate optimal readahead:

```cpp
size_t BlockCacheLookupForReadAheadSize(
    uint64_t offset, size_t max_size, size_t block_size) {
  size_t readahead_size = 0;

  // Probe cache for next N blocks
  for (size_t i = 0; i < max_size / block_size; ++i) {
    uint64_t block_offset = offset + i * block_size;
    Cache::Handle* handle = block_cache_->Lookup(GetCacheKey(block_offset));

    if (handle) {
      // Block in cache, no need to prefetch further
      block_cache_->Release(handle);
      break;
    } else {
      // Block not in cache, include in readahead
      readahead_size += block_size;
    }
  }

  return readahead_size;
}
```

## Iterator Pinning (PinData)

When `ReadOptions::pin_data = true`, iterators hold references to data blocks until deleted, enabling zero-copy access.

### PinnedIteratorsManager

`table/pinned_iterators_manager.h`

```cpp
class PinnedIteratorsManager : public Cleanupable {
 public:
  void PinSlice(const Slice& slice, Cleanupable* cleanable);
  void PinIterator(InternalIterator* iter, bool is_arena = false);

 private:
  std::vector<std::unique_ptr<Cleanupable>> pinned_objects_;
};

void PinnedIteratorsManager::PinSlice(const Slice& slice, Cleanupable* cleanable) {
  // Transfer cleanup functions from cleanable to this manager
  cleanable->DelegateCleanupsTo(this);
  // slice memory now kept alive until this manager is destroyed
}
```

### Usage in BlockBasedTableIterator

```cpp
void BlockBasedTableIterator::InitDataBlock() {
  BlockHandle handle = index_iter_.value().handle;

  BlockCacheLookupContext lookup_context;
  BlockContents contents;
  Status s = block_cache_->GetDataBlock(handle, &contents, &lookup_context);

  if (read_options_.pin_data && contents.allocation == Allocation::kCacheAllocation) {
    // Block from cache, register cleanup to release cache handle
    RegisterCleanup(&ReleaseCacheHandle, cache_handle_, block_cache_);
  }

  block_iter_.Initialize(..., pinned_iters_mgr_);
}
```

**Requirements for Full Pinning**:
1. `BlockBasedTableOptions::use_delta_encoding = false` (delta encoding requires reconstruction)
2. Block cache or direct I/O with persistent buffers
3. All child iterators support pinning

⚠️ **INVARIANT**: When `IsKeyPinned() == true`, the memory pointed to by `key()` remains valid until iterator destruction or explicit unpin.

⚠️ **INVARIANT**: When `IsValuePinned() == true`, the memory pointed to by `value()` remains valid until iterator destruction or explicit unpin.

## Forward vs Reverse Iteration

### Performance Asymmetry

**Forward iteration** (Next):
- Natural LSM order (memtable → L0 → L1 → ...)
- Sequential block reads
- Prefetching effective
- Typical: 1-2 million keys/sec

**Reverse iteration** (Prev):
- Against natural order
- Random block access pattern
- Prefetching ineffective
- Typical: 200-500k keys/sec (2-5x slower)

### MergingIterator Direction Switch

```cpp
void MergingIterator::SwitchToForward() {
  // Clear reverse heap
  maxHeap_.clear();

  // Rebuild forward heap
  for (auto& child : children_) {
    if (child.iter.Valid()) {
      // Skip the key that was just returned in reverse direction
      child.iter.Next();
      if (child.iter.Valid()) {
        minHeap_.push(&child);
      }
    } else {
      // Was at beginning, now seek to first
      child.iter.SeekToFirst();
      if (child.iter.Valid()) {
        minHeap_.push(&child);
      }
    }
  }

  // Rebuild range tombstone state
  for (size_t level = 0; level < range_tombstone_iters_.size(); ++level) {
    // Position tombstone iterators for forward scan
    InsertRangeTombstoneToMinHeap(level);
  }

  direction_ = kForward;
  FindNextVisibleKey();
}

void MergingIterator::SwitchToBackward() {
  // Clear forward heap
  minHeap_.clear();
  active_.clear();

  // Rebuild reverse heap
  for (auto& child : children_) {
    if (child.iter.Valid()) {
      // Skip the key that was just returned in forward direction
      child.iter.Prev();
      if (child.iter.Valid()) {
        maxHeap_.push(&child);
      }
    } else {
      // Was at end, now seek to last
      child.iter.SeekToLast();
      if (child.iter.Valid()) {
        maxHeap_.push(&child);
      }
    }
  }

  // Rebuild range tombstone state for reverse
  RebuildRangeTombstonesReverse();

  direction_ = kReverse;
  FindPrevVisibleKey();
}
```

**Cost**: Direction switches rebuild heaps and range tombstone state, requiring O(N log N) where N = number of child iterators. Avoid frequent direction switches.

### DBIter Reverse Positioning

In reverse iteration, DBIter maintains `iter_` BEFORE the current key:

```cpp
void DBIter::PrevInternal() {
  // iter_ is positioned before all versions of saved_key_
  // Need to move to previous user key

  while (iter_->Valid()) {
    ParsedInternalKey ikey;
    ParseInternalKey(iter_->key(), &ikey);

    if (user_comparator_.Compare(ikey.user_key, saved_key_.GetUserKey()) < 0) {
      // Found previous user key
      saved_key_.SetUserKey(ikey.user_key);

      // Scan forward from here to find value
      if (FindValueForCurrentKeyReverse()) {
        valid_ = true;
        return;
      }
      // else: key deleted, continue
    }

    iter_->Prev();
  }

  valid_ = false;
}

bool DBIter::FindValueForCurrentKeyReverse() {
  // Current position: at latest version of saved_key_
  // Scan forward to find visible value

  SequenceNumber last_visible_seq = 0;
  Slice last_visible_value;
  bool found = false;

  while (iter_->Valid()) {
    ParsedInternalKey ikey;
    ParseInternalKey(iter_->key(), &ikey);

    if (!user_comparator_.Equal(ikey.user_key, saved_key_.GetUserKey())) {
      break;  // Moved to next user key
    }

    if (ikey.sequence <= sequence_ && ikey.sequence > last_visible_seq) {
      // More recent visible version
      if (ikey.type == kTypeValue) {
        last_visible_value = iter_->value();
        last_visible_seq = ikey.sequence;
        found = true;
      } else if (ikey.type == kTypeDeletion) {
        // Key was deleted
        found = false;
        break;
      }
      // Handle merges...
    }

    iter_->Next();
  }

  // Restore position: move back before saved_key_
  while (iter_->Valid() &&
         user_comparator_.Compare(ExtractUserKey(iter_->key()),
                                   saved_key_.GetUserKey()) >= 0) {
    iter_->Prev();
  }

  if (found) {
    value_ = last_visible_value;
    return true;
  }
  return false;
}
```

This double-scan (forward to find value, backward to restore position) explains much of the reverse iteration overhead.

## Iterator Lifecycle and Memory Management

### Allocation Patterns

**Heap Allocation** (common):
```cpp
Iterator* iter = db->NewIterator(ReadOptions());
// ... use iter ...
delete iter;
```

**Arena Allocation** (internal, compaction):
```cpp
Arena arena;
InternalIterator* iter = new (arena.AllocateAligned(sizeof(DBIter)))
    DBIter(...);
// No explicit delete needed, arena cleanup handles it
```

### Cleanup Mechanism

Iterators inherit from `Cleanupable`, which manages cleanup callbacks:

```cpp
class Cleanupable {
 private:
  struct Cleanup {
    void (*function)(void* arg1, void* arg2);
    void* arg1;
    void* arg2;
    Cleanup* next;
  };
  Cleanup* cleanup_ = nullptr;

 public:
  void RegisterCleanup(void (*func)(void*, void*), void* arg1, void* arg2);
  void DelegateCleanupsTo(Cleanupable* other);
  ~Cleanupable();
};
```

**Example**: Releasing cache handle on iterator destruction:
```cpp
static void ReleaseCacheHandle(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(handle);
}

// In BlockBasedTableIterator::InitDataBlock()
RegisterCleanup(&ReleaseCacheHandle, block_cache_, cache_handle_);
```

### IteratorWrapper Deletion

```cpp
void IteratorWrapperBase::DeleteIter(bool is_arena_mode) {
  if (iter_) {
    if (!is_arena_mode) {
      delete iter_;  // Heap allocated
    } else {
      iter_->~InternalIteratorBase();  // Arena allocated: manual destructor
    }
    iter_ = nullptr;
  }
}
```

⚠️ **INVARIANT**: Iterators must not outlive the DB, ColumnFamily, or Snapshot they reference. Violating this causes use-after-free.

⚠️ **INVARIANT**: When using arena allocation, arena must outlive all iterators allocated from it.

## Key Source Files

| Component | Primary Files |
|-----------|---------------|
| **InternalIterator base** | `table/internal_iterator.h` |
| **User Iterator** | `include/rocksdb/iterator.h` |
| **IteratorWrapper** | `table/iterator_wrapper.h` |
| **MergingIterator** | `table/merging_iterator.h`, `table/merging_iterator.cc` |
| **DBIter** | `db/db_iter.h`, `db/db_iter.cc` |
| **BlockBasedTableIterator** | `table/block_based/block_based_table_iterator.h`, `table/block_based/block_based_table_iterator.cc` |
| **Block Iterator** | `table/block_based/block.h`, `table/block_based/block.cc`, `table/block_based/data_block_hash_index.h` |
| **TwoLevelIterator** | `table/two_level_iterator.h`, `table/two_level_iterator.cc` |
| **CompactionIterator** | `db/compaction/compaction_iterator.h`, `db/compaction/compaction_iterator.cc` |
| **Range Tombstones** | `db/range_del_aggregator.h`, `db/range_del_aggregator.cc` |
| **Pinning** | `table/pinned_iterators_manager.h` |
| **Prefetching** | `table/block_based/block_prefetcher.h`, `table/block_based/block_prefetcher.cc` |
| **IterateResult** | `include/rocksdb/advanced_iterator.h` |

## Summary

RocksDB's iterator architecture provides a flexible, composable framework for traversing LSM data:

1. **InternalIterator** forms the internal abstraction working with internal keys
2. **Leaf iterators** (MemTable, DataBlockIter) read actual data with minimal overhead
3. **Composite iterators** (MergingIterator, LevelIterator, TwoLevelIterator) combine multiple sources
4. **DBIter** presents the user-facing API with snapshot isolation and merge resolution
5. **Special iterators** (CompactionIterator, Range tombstone) handle specific use cases

**Performance characteristics**:
- Forward iteration: optimized, sequential I/O, effective prefetching
- Reverse iteration: 2-5x slower, random I/O, direction switches expensive
- Iterator pinning: zero-copy when `pin_data = true`
- Caching: IteratorWrapper reduces virtual call overhead by ~5-10%

**Key invariants**:
- Keys must be strictly ordered according to comparator
- Valid() == true implies status().ok() == true
- Range tombstone fragments are non-overlapping within a file
- ShouldDelete() requires monotonic key access
- Iterator lifetime must not exceed DB/Snapshot lifetime
