# RocksDB MemTable

## Overview

MemTable is RocksDB's in-memory write buffer that accepts all incoming writes (Put/Delete/Merge) before they are flushed to persistent SST files. It provides fast concurrent insertion and point/range lookups while maintaining sorted order by internal key `(user_key, sequence_number_descending)`.

### Key Characteristics

- **Sorted in-memory buffer**: Keys sorted by `(user_key, seq_desc)` for efficient iteration
- **Write-optimized**: Lock-free concurrent inserts via CAS operations (when `allow_concurrent_memtable_write=true`)
- **Pluggable representations**: SkipList (default), HashSkipList, HashLinkList, Vector
- **Bloom filter acceleration**: Optional prefix/whole-key bloom filter for negative lookups
- **Arena allocation**: All memory allocated from arena/concurrent_arena for cache locality
- **Reference counted**: Transitions from mutable -> immutable -> flushed with refcount management

### High-Level Architecture

```
+------------------------------------------------------------------+
|  WRITE PATH (DBImpl::WriteImpl)                                  |
|  WriteBatch -> MemTable::Add                                     |
+-----------------------------+------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
|  MEMTABLE (db/memtable.h)                                        |
|  +--------------------------------------------------------+      |
|  | Active MemTable (mutable, accepts writes)              |      |
|  |  +- table_: MemTableRep (point key/value pairs)        |      |
|  |  +- range_del_table_: MemTableRep (range deletes)      |      |
|  |  +- bloom_filter_: DynamicBloom (optional)             |      |
|  |  +- arena_: ConcurrentArena (memory allocation)        |      |
|  |  +- flush_state_: FLUSH_NOT_REQUESTED/REQUESTED/       |      |
|  |  |                 FLUSH_SCHEDULED                      |      |
|  +--------------------------------------------------------+      |
|                                                                    |
|  Flush Triggers:                                                  |
|   - MarkForFlush() (manual/error recovery)                        |
|   - num_range_deletes >= memtable_max_range_deletions             |
|   - Memory heuristic exceeds write_buffer_size                    |
+-----------------------------+------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
|  MEMTABLE REPRESENTATION (include/rocksdb/memtablerep.h)         |
|  +--------------------------------------------------------+      |
|  | SkipList (default) - InlineSkipList                    |      |
|  |  - Lock-free concurrent insert via CAS                 |      |
|  |  - 12 levels by default, max 32 levels                 |      |
|  |  - O(log N) insert/lookup, O(log D) sequential insert  |      |
|  |  - Nodes stored inline with key data                   |      |
|  +--------------------------------------------------------+      |
|  +--------------------------------------------------------+      |
|  | HashSkipList - prefix hash + skiplist per bucket       |      |
|  |  - Optimized for prefix seek                           |      |
|  |  - Requires prefix extractor                           |      |
|  +--------------------------------------------------------+      |
|  +--------------------------------------------------------+      |
|  | HashLinkList - prefix hash + sorted linked list        |      |
|  |  - Memory efficient for small datasets                 |      |
|  +--------------------------------------------------------+      |
|  +--------------------------------------------------------+      |
|  | Vector - unsorted vector, sorted on MarkReadOnly()     |      |
|  |  - Bulk load optimization                              |      |
|  +--------------------------------------------------------+      |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
|  IMMUTABLE MEMTABLE LIST (db/memtable_list.h)                    |
|  +--------------------------------------------------------+      |
|  | MemTableListVersion                                    |      |
|  |  memlist_ = [imm3, imm2, imm1]  (newest first)        |      |
|  |  -> Get() searches newest -> oldest                    |      |
|  |  -> FlushJob flushes oldest first                      |      |
|  +--------------------------------------------------------+      |
+-----------------------------+------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
|  FLUSH TO SST (db/flush_job.cc)                                  |
|  FlushJob iterates immutable memtable -> writes L0 SST           |
+------------------------------------------------------------------+
```

---

## 1. MemTable Interface and Representations

**Files:** `include/rocksdb/memtablerep.h`, `db/memtable.h`, `db/memtable.cc`

### Core Abstraction: MemTableRep

`MemTableRep` is the abstract interface that all memtable representations must implement. It defines a sorted, concurrent-readable, single-writer (or lock-free multi-writer) container.

**Interface contract** (`include/rocksdb/memtablerep.h`):

| Property | Description |
|----------|-------------|
| No duplicates | Does not store duplicate `(key, seq)` pairs |
| Sorted iteration | Uses `KeyComparator` for ordering |
| Concurrent reads | Multiple readers without locking |
| No deletions | Items never deleted until MemTable destroyed |

**Key methods:**

```cpp
class MemTableRep {
  virtual KeyHandle Allocate(size_t len, char** buf);
  virtual void Insert(KeyHandle handle) = 0;        // Pure virtual
  virtual bool InsertKey(KeyHandle handle);           // Returns false on dup
  virtual void InsertConcurrently(KeyHandle handle);  // Lock-free (virtual, not pure)
  virtual bool InsertKeyConcurrently(KeyHandle handle); // Lock-free, dup detection
  virtual void InsertWithHint(KeyHandle handle, void** hint);
  virtual Iterator* GetIterator(Arena* arena = nullptr) = 0;
  // ...
};
```

Note: `InsertConcurrently()` is a virtual method with a default implementation (not pure virtual). The `InsertKey*` variants return `false` when `CanHandleDuplicatedKey()` is true and the `<key, seq>` already exists.

### 1.1 SkipList (Default) - InlineSkipList

**Files:** `memtable/inlineskiplist.h`, `memtable/skiplistrep.cc`

InlineSkipList is the default memtable representation, optimized for concurrent reads and lock-free concurrent writes via CAS.

**Properties** (`memtable/inlineskiplist.h`):

- **Max height**: `kMaxPossibleHeight = 32` (configurable default 12 via constructor)
- **Branching factor**: 4 (1/4 probability of growing each level)
- **Complexity**: O(log N) insert/lookup, O(log D) for sequential inserts with finger search
- **Memory layout**: Node header + key inlined, next pointers stored **below** Node struct

**Thread safety** (`memtable/inlineskiplist.h:20-27`):

```
INVARIANT: Writes via Insert() require external synchronization (write thread).
INVARIANT: InsertConcurrently() is lock-free, safe with concurrent reads/inserts.
INVARIANT: Allocated nodes never deleted until InlineSkipList destroyed.
INVARIANT: Node contents (except next/prev pointers) immutable after linking.
```

**Advantages:**
- Proven data structure with predictable O(log N) performance
- Lock-free concurrent insert via CAS
- Cache-friendly sequential access with hints
- Well-suited for random write workloads

**Disadvantages:**
- Higher memory overhead per node
- Poor prefix locality (keys with same prefix scattered across skiplist)

### 1.2 HashSkipList

**Files:** `memtable/hash_skiplist_rep.cc`

Hash table where each bucket is a skiplist. Optimized for workloads with prefix structure (e.g., `"user123:tweet456"`).

**When to use:**
- Workload has clear prefix structure (requires `prefix_extractor`)
- Iteration within a prefix is common
- Cross-prefix iteration is rare

**Advantages:**
- O(1) prefix lookup to find bucket
- Better cache locality for same-prefix keys

**Disadvantages:**
- Requires prefix extractor configuration
- Full iteration requires traversing all buckets
- Memory overhead for hash table

### 1.3 HashLinkList

**Files:** `memtable/hash_linklist_rep.cc`

Hash table where each bucket is a sorted linked list.

**When to use:**
- Small datasets where skiplist overhead is too high
- Prefix-based workload
- Memory constrained

**Advantages:**
- Lower per-node memory overhead than HashSkipList

**Disadvantages:**
- O(N_bucket) insert/lookup within bucket (no log factor)
- Slower for large buckets

### 1.4 Vector

**Files:** `memtable/vector_rep.cc`

Unsorted `std::vector`, sorted once on `MarkReadOnly()`.

**When to use:**
- Bulk load scenarios
- MemTable will be flushed immediately after loading
- Iteration only needed after all writes complete

**Advantages:**
- Fastest inserts (append-only)
- Minimal memory overhead
- Supports concurrent inserts (via thread-local buffering)

**Disadvantages:**
- Cannot serve reads during writes (until sorted)
- O(N log N) sort penalty on `MarkReadOnly()`

---

## 2. InlineSkipList Internals

**Files:** `memtable/inlineskiplist.h`

### 2.1 Node Layout

InlineSkipList uses a custom memory layout to save space. Instead of storing the key as a pointer, the key is stored **inline** immediately after the Node header.

**Memory layout** (`memtable/inlineskiplist.h`):

```
For a node with height H storing key K:

Memory layout (addresses grow downward -> upward):
+------------------------------------------+ <-- AllocateNode return address
|  next_[H-1] (AtomicPointer)              |
|  next_[H-2]                              |
|  ...                                      |
|  next_[1]                                |
+------------------------------------------+ <-- Node* (struct base)
|  next_[0]   (AtomicPointer)              | <-- Node::next_[0]
+------------------------------------------+ <-- Node::Key()
|  Key data:                               |
|    varint32(internal_key_size)            |
|    user_key bytes                        |
|    8-byte packed(seq, type)              |
|    varint32(value_size)                  |
|    value bytes                           |
|    checksum (if enabled)                 |
+------------------------------------------+

Accessing next_[n]:  (&next_[0] - n)  (pointer arithmetic)
Accessing key:       &next_[1]        (immediately after next_[0])
```

**Key insight**: `next_[1..H-1]` are stored **before** the Node struct. This avoids wasting a pointer in the Node struct for the key, saving `sizeof(void*)` bytes per node.

**Node methods** (`memtable/inlineskiplist.h`):

```cpp
struct Node {
  const char* Key() const { return reinterpret_cast<const char*>(&next_[1]); }

  Node* Next(int n) {
    return (&next_[0] - n)->Load();  // Acquire load
  }

  void SetNext(int n, Node* x) {
    (&next_[0] - n)->Store(x);       // Release store
  }

  bool CASNext(int n, Node* expected, Node* x) {
    return (&next_[0] - n)->CasStrong(expected, x);  // CAS for concurrent insert
  }

  // "Stash" height in next_[0] before node is linked
  void StashHeight(const int height) {
    memcpy(static_cast<void*>(&next_[0]), &height, sizeof(int));
  }
  int UnstashHeight() const { /* inverse */ }
};
```

### 2.2 Concurrent Insert via CAS

**Files:** `memtable/inlineskiplist.h`

When `allow_concurrent_memtable_write=true`, RocksDB uses `InsertConcurrently()`, which performs lock-free insertion using Compare-And-Swap (CAS) operations.

**Algorithm** (`Insert<UseCAS=true>` template):

1. **Allocate node** (thread-safe via allocator)
2. **Find splice** (prev/next pointers at each level)
3. **Validate and link** using CAS in bottom-up order:

```cpp
template <bool UseCAS>
bool Insert(const char* key, Splice* splice, bool allow_partial_splice_fix) {
  // 1. Find insertion point for all levels
  Node* x = reinterpret_cast<Node*>(const_cast<char*>(key)) - 1;
  int height = x->UnstashHeight();

  // 2. Recompute splice (search structure) as needed
  RecomputeSpliceLevels(key, splice, recompute_level);

  // 3. Link node level-by-level (bottom to top)
  for (int i = 0; i < height; i++) {
    while (true) {
      Node* prev = splice->prev_[i];
      Node* next = splice->next_[i];
      x->NoBarrier_SetNext(i, next);

      if (UseCAS) {
        // CAS: atomically link if prev still points to next
        if (prev->CASNext(i, next, x)) break;  // Success
        // Retry: concurrent insert modified prev->next[i]
        FindSpliceForLevel(key, prev, next, i, &splice->prev_[i], &splice->next_[i]);
      } else {
        prev->SetNext(i, x);  // Single-writer: direct write
        break;
      }
    }
  }
  return true;
}
```

**Memory ordering** (`memtable/inlineskiplist.h`):

```
INVARIANT: SetNext() uses release-store so readers see fully initialized node.
INVARIANT: Next() uses acquire-load to observe initialization.
INVARIANT: CASNext() provides acquire-release semantics for linearizability.
```

### 2.3 Finger Search Optimization

**Files:** `memtable/inlineskiplist.h`

For sequential insertions (common pattern in WriteBatch), InlineSkipList uses **finger search** to reduce cost from O(log N) to **O(log D)** where D = distance from last insert position.

**Splice structure** (`memtable/inlineskiplist.h`):

```cpp
struct Splice {
  int height_ = 0;
  Node** prev_;  // prev_[i] < key for all levels i
  Node** next_;  // next_[i] >= key for all levels i
  // INVARIANT: prev_[i+1].key <= prev_[i].key < next_[i].key <= next_[i+1].key
};
```

**Usage:**

```cpp
void* hint = nullptr;  // Splice cached across InsertKeyWithHint calls
table_->InsertKeyWithHint(handle1, &hint);  // O(log N) first insert
table_->InsertKeyWithHint(handle2, &hint);  // O(log D) if handle2 near handle1
table_->InsertKeyWithHint(handle3, &hint);  // O(log D) sequential pattern
```

**When finger search helps:**
- Sequential inserts (monotonically increasing keys)
- Batched inserts of keys with same prefix
- Workloads where recent insert position predicts next insert

---

## 3. Arena and ConcurrentArena Allocation

**Files:** `memory/arena.h`, `memory/arena.cc`, `memory/concurrent_arena.h`, `memory/concurrent_arena.cc`

### 3.1 Why Arena Allocation?

MemTable uses arena allocation to:
1. **Avoid malloc overhead**: Single block allocation instead of per-key malloc
2. **Improve cache locality**: Related keys stored in same memory region
3. **Simplify memory management**: Entire arena freed at once when MemTable destroyed
4. **Enable lock-free allocation**: Per-core shards eliminate contention

### 3.2 Arena

**Files:** `memory/arena.h`, `memory/arena.cc`

Arena is a bump-pointer allocator that carves out memory from fixed-size blocks. It has an inline block (`kInlineSize = 2048` bytes) that avoids heap allocation for small/empty memtables, plus dynamically allocated blocks.

**Allocation strategy:**

```cpp
char* Arena::Allocate(size_t bytes) {
  if (bytes <= alloc_bytes_remaining_) {
    unaligned_alloc_ptr_ -= bytes;
    alloc_bytes_remaining_ -= bytes;
    return unaligned_alloc_ptr_;
  }
  return AllocateFallback(bytes, false);
}

char* AllocateFallback(size_t bytes, bool aligned) {
  if (bytes > kBlockSize / 4) {
    // Large allocation: dedicated block (irregular)
    return AllocateNewBlock(bytes);
  }
  // Small allocation: waste remaining space, allocate new block
  block_head = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize - bytes;
  // ...
}
```

**Memory accounting:**

```cpp
size_t Arena::ApproximateMemoryUsage() const {
  return blocks_memory_ + blocks_.size() * sizeof(char*) - alloc_bytes_remaining_;
}

size_t Arena::MemoryAllocatedBytes() const { return blocks_memory_; }
```

**Key constants:**
- `kMinBlockSize` = 4096 (minimum, also the default if no `arena_block_size` specified)
- `kMaxBlockSize` = 2 GB
- `kInlineSize` = 2048 (inline block avoids heap allocation for empty memtables)
- Actual `kBlockSize` is set from `arena_block_size` option (typically `write_buffer_size / 8`)

### 3.3 ConcurrentArena

**Files:** `memory/concurrent_arena.h`, `memory/concurrent_arena.cc`

ConcurrentArena wraps Arena with:
1. **Spinlock protection** on main arena
2. **Per-core shards** for allocations that fit within `shard_block_size_ / 4` to reduce contention

**Shard structure:**

```cpp
struct Shard {
  char padding[40];
  mutable SpinMutex mutex;
  char* free_begin_;
  std::atomic<size_t> allocated_and_unused_;
};
```

**Allocation path** (`memory/concurrent_arena.h`):

```cpp
template <typename Func>
char* AllocateImpl(size_t bytes, bool force_arena, const Func& func) {
  // Go directly to arena if allocation is too large for shard
  if (bytes > shard_block_size_ / 4 || force_arena || ...) {
    arena_lock.lock();
    return func();  // Allocate from main arena
  }

  // Pick a shard (per-core)
  Shard* s = shards_.AccessAtCore(cpu & (shards_.Size() - 1));
  // ... lock shard, allocate from shard, refill from arena if needed
}
```

**Shard refill** (`memory/concurrent_arena.h`):

When a shard runs out of space, it allocates a new chunk (approximately `shard_block_size_` bytes) from the main arena. The shard block size is computed as a fraction of the arena block size based on hardware concurrency.

```
INVARIANT: Per-core shards never freed until MemTable destroyed.
INVARIANT: Shard block size adjusts to match arena block boundaries to avoid fragmentation.
```

---

## 4. Insert Path: Put/Delete/Merge to MemTable

**Files:** `db/memtable.cc`

### 4.1 Entry Encoding

Every key-value pair is encoded into a self-contained binary format before insertion.

**Format** (`db/memtable.cc`):

```
+-------------------------------------------------------------+
| varint32(internal_key_size)                                  |  internal_key_size = user_key.size() + 8
| user_key bytes                                               |  Raw user key
| uint64 packed(sequence_number, ValueType)                    |  seq (56 bits) | type (8 bits)
| varint32(value_size)                                         |
| value bytes                                                  |  Raw value (empty for Delete)
| [optional] checksum                                          |  protection_bytes_per_key (0, 1, 2, 4, or 8)
+-------------------------------------------------------------+
```

**ValueType encoding** (`db/dbformat.h`):

```cpp
enum ValueType : unsigned char {
  kTypeDeletion              = 0x0,   // Delete
  kTypeValue                 = 0x1,   // Put
  kTypeMerge                 = 0x2,   // Merge
  kTypeSingleDeletion        = 0x7,   // SingleDelete
  kTypeRangeDeletion         = 0xF,   // DeleteRange
  kTypeDeletionWithTimestamp = 0x14,  // Delete with UDT
  kTypeWideColumnEntity      = 0x16,  // PutEntity (wide columns)
  kTypeValuePreferredSeqno   = 0x18,  // Value with write time
  // ...
};
```

### 4.2 MemTable::Add() Flow

**Files:** `db/memtable.cc` (line ~950)

```cpp
Status MemTable::Add(SequenceNumber s, ValueType type,
                     const Slice& key,      // User key
                     const Slice& value,
                     const ProtectionInfoKVOS64* kv_prot_info,
                     bool allow_concurrent,
                     MemTablePostProcessInfo* post_process_info,
                     void** hint) {
  // 1. Encode entry
  uint32_t key_size = static_cast<uint32_t>(key.size());
  uint32_t val_size = static_cast<uint32_t>(value.size());
  uint32_t internal_key_size = key_size + 8;
  uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size
                       + VarintLength(val_size) + val_size
                       + moptions_.protection_bytes_per_key;

  // 2. Select table: range deletions go to range_del_table_
  std::unique_ptr<MemTableRep>& table =
      type == kTypeRangeDeletion ? range_del_table_ : table_;
  char* buf = nullptr;
  KeyHandle handle = table->Allocate(encoded_len, &buf);  // Arena allocation

  // 3. Write encoded entry to buf
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, PackSequenceAndType(s, type));
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);

  // 4. Optional checksum
  UpdateEntryChecksum(kv_prot_info, key, value, type, s,
                      buf + encoded_len - moptions_.protection_bytes_per_key);

  // 5. Insert into memtable representation
  if (!allow_concurrent) {
    // Sequential path: use InsertKey or InsertKeyWithHint
    if (table == table_ && insert_with_hint_prefix_extractor_ != nullptr &&
        insert_with_hint_prefix_extractor_->InDomain(key_slice)) {
      table->InsertKeyWithHint(handle, &insert_hints_[prefix]);
    } else {
      table->InsertKey(handle);
    }

    // Update metadata immediately (relaxed store, no atomics needed)
    num_entries_.StoreRelaxed(num_entries_.LoadRelaxed() + 1);
    data_size_.StoreRelaxed(data_size_.LoadRelaxed() + encoded_len);
    if (type == kTypeDeletion || type == kTypeSingleDeletion ||
        type == kTypeDeletionWithTimestamp) {
      num_deletes_.StoreRelaxed(num_deletes_.LoadRelaxed() + 1);
    } else if (type == kTypeRangeDeletion) {
      num_range_deletes_.StoreRelaxed(num_range_deletes_.LoadRelaxed() + 1);
    }

    // Update bloom filter (prefix and/or whole key separately)
    if (bloom_filter_ && prefix_extractor_ &&
        prefix_extractor_->InDomain(key_without_ts)) {
      bloom_filter_->Add(prefix_extractor_->Transform(key_without_ts));
    }
    if (bloom_filter_ && moptions_.memtable_whole_key_filtering) {
      bloom_filter_->Add(key_without_ts);
    }

    UpdateFlushState();
  } else {
    // Concurrent path: use InsertKeyConcurrently (or with hint)
    bool res = (hint == nullptr)
                   ? table->InsertKeyConcurrently(handle)
                   : table->InsertKeyWithHintConcurrently(handle, hint);

    // Defer metadata to post_process_info (batched later via BatchPostProcess)
    post_process_info->num_entries++;
    post_process_info->data_size += encoded_len;
    if (type == kTypeDeletion) {
      post_process_info->num_deletes++;
    }
    // Range deletions tracked separately after the if/else block

    // Use AddConcurrently for bloom filter (thread-safe variant)
    if (bloom_filter_ && prefix_extractor_ &&
        prefix_extractor_->InDomain(key_without_ts)) {
      bloom_filter_->AddConcurrently(
          prefix_extractor_->Transform(key_without_ts));
    }
    if (bloom_filter_ && moptions_.memtable_whole_key_filtering) {
      bloom_filter_->AddConcurrently(key_without_ts);
    }
  }

  return Status::OK();
}
```

**Concurrent vs. sequential insert:**

| Mode | When | Insert method | Metadata update |
|------|------|---------------|-----------------|
| Sequential | `allow_concurrent_memtable_write=false` | `InsertKey()` or `InsertKeyWithHint()` | Immediate relaxed store |
| Concurrent | `allow_concurrent_memtable_write=true` | `InsertKeyConcurrently()` or `InsertKeyWithHintConcurrently()` | Deferred via `MemTablePostProcessInfo`, then `BatchPostProcess` |

```
INVARIANT: Sequential inserts acquire write thread lock (single writer).
INVARIANT: Concurrent inserts use CAS, no external lock required.
INVARIANT: Metadata (data_size_, num_entries_) updated via BatchPostProcess for concurrent writes.
INVARIANT: Range deletions always go to range_del_table_, not table_.
```

---

## 5. Lookup Path: Get and MultiGet

**Files:** `db/memtable.cc` (line ~1405)

### 5.1 MemTable::Get() Flow

```cpp
bool MemTable::Get(const LookupKey& key, std::string* value,
                   PinnableWideColumns* columns, std::string* timestamp,
                   Status* s, MergeContext* merge_context,
                   SequenceNumber* max_covering_tombstone_seq,
                   SequenceNumber* seq, const ReadOptions& read_opts,
                   bool immutable_memtable, ReadCallback* callback,
                   bool* is_blob_index, bool do_merge) {
  if (IsEmpty()) return false;  // Early exit

  // 1. Check range tombstones first
  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
      NewRangeTombstoneIterator(read_opts,
                                GetInternalKeySeqno(key.internal_key()),
                                immutable_memtable));
  if (range_del_iter) {
    SequenceNumber covering_seq =
        range_del_iter->MaxCoveringTombstoneSeqnum(key.user_key());
    if (covering_seq > *max_covering_tombstone_seq) {
      *max_covering_tombstone_seq = covering_seq;
    }
  }

  // 2. Bloom filter check (negative lookup optimization)
  bool may_contain = true;
  if (bloom_filter_) {
    if (moptions_.memtable_whole_key_filtering) {
      // Whole key filtering takes priority when enabled
      may_contain = bloom_filter_->MayContain(user_key_without_ts);
    } else {
      // Prefix filtering only
      assert(prefix_extractor_);
      if (prefix_extractor_->InDomain(user_key_without_ts)) {
        may_contain = bloom_filter_->MayContain(
            prefix_extractor_->Transform(user_key_without_ts));
      }
    }
  }

  if (bloom_filter_ && !may_contain) {
    PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
    *seq = kMaxSequenceNumber;
    return false;  // Definitely not present
  }

  // 3. Search memtable via GetFromTable
  //    Calls table_->Get(key, &saver, SaveValue), or
  //    table_->GetAndValidate() when paranoid_memory_checks enabled
  GetFromTable(key, *max_covering_tombstone_seq, do_merge, callback,
               is_blob_index, value, columns, timestamp, s, merge_context,
               seq, &found_final_value, &merge_in_progress);

  return found_final_value;
}
```

### 5.2 Bloom Filter (DynamicBloom)

**Files:** `util/dynamic_bloom.h`, `util/dynamic_bloom.cc`

MemTable optionally uses a prefix bloom filter to accelerate negative lookups (key definitely not present).

**Configuration** (`db/memtable.cc`):

```cpp
if ((prefix_extractor_ || moptions_.memtable_whole_key_filtering) &&
    moptions_.memtable_prefix_bloom_bits > 0) {
  bloom_filter_.reset(
      new DynamicBloom(&arena_, moptions_.memtable_prefix_bloom_bits,
                       6 /* hard coded 6 probes */,
                       moptions_.memtable_huge_page_size, ioptions.logger));
}
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `memtable_prefix_bloom_size_ratio` | Bloom filter bits as fraction of `write_buffer_size` | 0 (disabled) |
| `memtable_whole_key_filtering` | Add whole user key to bloom (not just prefix) | false |

**Calculation:**

```
memtable_prefix_bloom_bits = write_buffer_size * memtable_prefix_bloom_size_ratio * 8
```

**Example:**
- `write_buffer_size = 64 MB`
- `memtable_prefix_bloom_size_ratio = 0.125`
- -> Bloom filter size = `64 MB * 0.125 = 8 MB = 67M bits`

**False positive rate** (6 hash functions):

```
FPR ~ (1 - e^(-6*N/M))^6  where N = num_keys, M = num_bits
```

For 1M keys with 67M bits: `FPR ~ 0.4%` (very low)

```
INVARIANT: Bloom filter has false positives but never false negatives.
INVARIANT: Bloom filter memory allocated from arena, freed with MemTable.
```

### 5.3 MultiGet Optimization

**Files:** `db/memtable.cc`, `memtable/inlineskiplist.h`

For batched lookups of sorted keys, MemTable uses **finger search** to reduce per-key cost from O(log N) to **O(log D)**.

**Batch lookup interface:**

```cpp
void MemTable::MultiGet(const ReadOptions& read_options,
                        MultiGetRange* range,
                        ReadCallback* callback,
                        bool immutable_memtable);
```

**InlineSkipList::MultiGet** (`memtable/inlineskiplist.h`):

```cpp
Status MultiGet(size_t num_keys, const char* const* keys,
                void** callback_args,
                bool (*callback_func)(void* arg, const char* entry),
                bool allow_data_in_errors = false,
                bool detect_key_out_of_order = false,
                const std::function<Status(const char*, bool)>&
                    key_validation_callback = nullptr) const;
```

Uses a cached search path ("finger") that carries forward between consecutive key lookups, reducing the search from the top of the skiplist each time.

**Complexity:**
- Without finger: `O(K * log N)` for K keys
- With finger: `O(K * log D)` where D = average distance between consecutive keys
- Best case (sequential): `O(K)` if keys are evenly spaced

---

## 6. MemTable Flush Triggers

**Files:** `db/memtable.cc`

MemTable transitions from mutable -> immutable when flush is triggered. Flush triggers are checked in `UpdateFlushState()` after each write.

### 6.1 Flush Trigger Conditions

**Files:** `db/memtable.cc` (line ~197)

```cpp
bool MemTable::ShouldFlushNow() {
  // 1. Manual flush requested
  if (IsMarkedForFlush()) {
    return true;
  }

  // 2. Too many range deletions
  if (memtable_max_range_deletions_ > 0 &&
      num_range_deletes_.LoadRelaxed() >=
          static_cast<uint64_t>(memtable_max_range_deletions_)) {
    return true;
  }

  // 3. Memory usage heuristic
  size_t write_buffer_size = write_buffer_size_.LoadRelaxed();
  const double kAllowOverAllocationRatio = 0.6;

  auto allocated_memory =
      table_->ApproximateMemoryUsage() + arena_.MemoryAllocatedBytes();

  // Allow one more block if within over-allocation ratio
  if (allocated_memory + kArenaBlockSize <
      write_buffer_size + kArenaBlockSize * kAllowOverAllocationRatio) {
    return false;
  }

  // Additional heuristic: if in last block, stop at 75% full
  // to avoid excessive over-allocation
  // ...
  return arena_.AllocatedAndUnused() < kArenaBlockSize / 4;
}
```

Note: The memory check is **not** a simple `ApproximateMemoryUsage() >= write_buffer_size` comparison. It uses a heuristic that considers arena block sizes and an over-allocation ratio (0.6) to avoid unnecessary fragmentation while staying close to the target size.

**Trigger summary:**

| Trigger | Condition | Rationale |
|---------|-----------|-----------|
| **Manual flush** | `MarkForFlush()` called | User request or error recovery |
| **Range delete limit** | `num_range_deletes >= memtable_max_range_deletions` | Prevent excessive range tombstones in memory |
| **Memory heuristic** | allocated_memory approaches write_buffer_size | Primary trigger: MemTable full |

### 6.2 write_buffer_size

**Option:** `write_buffer_size` (default: 64 MB)

Defines the target size of a single MemTable. When the memory heuristic determines the memtable is approximately full, it is marked for flush.

**ApproximateMemoryUsage** (`db/memtable.cc`):

```cpp
size_t MemTable::ApproximateMemoryUsage() {
  autovector<size_t> usages = {
      arena_.ApproximateMemoryUsage(),
      table_->ApproximateMemoryUsage(),
      range_del_table_->ApproximateMemoryUsage(),
      ROCKSDB_NAMESPACE::ApproximateMemoryUsage(insert_hints_)};
  size_t total_usage = 0;
  for (size_t usage : usages) {
    if (usage >= std::numeric_limits<size_t>::max() - total_usage) {
      return std::numeric_limits<size_t>::max();
    }
    total_usage += usage;
  }
  return total_usage;
}
```

Note: `ShouldFlushNow()` uses `table_->ApproximateMemoryUsage() + arena_.MemoryAllocatedBytes()` (not `ApproximateMemoryUsage()`) for its memory check, which is a different calculation.

**Arena memory accounting** (`memory/arena.h`):

```cpp
size_t Arena::ApproximateMemoryUsage() const {
  return blocks_memory_ + blocks_.size() * sizeof(char*) - alloc_bytes_remaining_;
}
```

### 6.3 max_write_buffer_number

**Option:** `max_write_buffer_number` (default: 2)

Limits the total number of memtables (1 mutable + N immutable) in memory before writes are blocked.

**Flush state transition:**

```
[Mutable MemTable]
   | memory heuristic triggers flush
[Immutable MemTable #1] + [New Mutable MemTable]
   | Another write_buffer_size exceeded
[Immutable #2] + [Immutable #1] + [New Mutable]
   | num_unflushed >= max_write_buffer_number -> WRITE STALL
   | FlushJob completes for Immutable #1
[Immutable #2] + [Mutable] -> Writes unblocked
```

```
INVARIANT: At most 1 mutable MemTable per column family at any time.
INVARIANT: Immutable MemTables are read-only, never modified.
INVARIANT: Flush processes immutable MemTables in FIFO order (oldest first).
```

### 6.4 memtable_max_range_deletions

**Option:** `memtable_max_range_deletions` (default: 0 = disabled)

Triggers flush when MemTable accumulates too many range deletions (`DeleteRange`).

**Rationale:**
- Range deletes stored separately in `range_del_table_`
- Each range delete can cover millions of keys
- Too many range deletes -> expensive iteration and memory overhead
- Flushing converts range deletes to more efficient SST format

---

## 7. MemTableList: Immutable MemTable Management

**Files:** `db/memtable_list.h`, `db/memtable_list.cc`

### 7.1 MemTableListVersion

`MemTableListVersion` is a snapshot of the list of immutable memtables. It's reference counted and used by reads/iterators to ensure a consistent view.

**Structure** (`db/memtable_list.h`):

```cpp
class MemTableListVersion {
  std::list<ReadOnlyMemTable*> memlist_;           // Immutable MemTables (newest -> oldest)
  std::list<ReadOnlyMemTable*> memlist_history_;   // Flushed MemTables (for transaction validation)
  int refs_ = 0;                                   // Reference count
  size_t* parent_memtable_list_memory_usage_;      // Memory tracking
  const int64_t max_write_buffer_size_to_maintain_; // History retention limit
};
```

**Ordering:**

```
memlist_ = [imm_newest, imm_2, imm_1, imm_oldest]
            ^ newest                    ^ oldest (next to flush)
```

### 7.2 MemTableList Operations

**Files:** `db/memtable_list.cc`

**Add new immutable MemTable:**

```cpp
void MemTableList::Add(ReadOnlyMemTable* m,
                       autovector<ReadOnlyMemTable*>* to_delete) {
  InstallNewVersion();  // Creates new MemTableListVersion
  current_->Add(m, to_delete);  // Calls AddMemTable -> memlist_.push_front(m)
  m->MarkImmutable();
  num_flush_not_started_++;
}

void MemTableListVersion::AddMemTable(ReadOnlyMemTable* m) {
  memlist_.push_front(m);  // Add to front (newest)
  *parent_memtable_list_memory_usage_ += m->ApproximateMemoryUsage();
}
```

**Remove flushed MemTable:**

```cpp
void MemTableListVersion::Remove(ReadOnlyMemTable* m,
                                 autovector<ReadOnlyMemTable*>* to_delete) {
  assert(refs_ == 1);  // Only mutable when refs_ == 1
  memlist_.remove(m);
  m->MarkFlushed();
  if (max_write_buffer_size_to_maintain_ > 0) {
    memlist_history_.push_front(m);  // Keep for transaction validation
    TrimHistory(to_delete, 0);
  } else {
    UnrefMemTable(to_delete, m);    // Schedule destruction
  }
}
```

### 7.3 Read Path (Get)

**Files:** `db/memtable_list.cc`

```cpp
bool MemTableListVersion::Get(const LookupKey& key, ...) {
  return GetFromList(&memlist_, key, ...);
}

bool MemTableListVersion::GetFromList(
    std::list<ReadOnlyMemTable*>* list, const LookupKey& key, ...) {
  // Search newest -> oldest
  for (auto& memtable : *list) {
    bool done = memtable->Get(key, value, ..., true /* immutable_memtable */);
    if (done) {
      return true;  // Stop at first definitive result
    }
  }
  return false;
}
```

**Ordering guarantee:**

```
INVARIANT: memlist_ ordered newest -> oldest ensures most recent value found first.
INVARIANT: MemTableListVersion is immutable once created (new versions for updates).
```

### 7.4 FlushJob Interaction

**Files:** `db/flush_job.cc`

FlushJob flushes **oldest** immutable MemTable first (FIFO order):

1. Pick oldest memtable(s) from `memlist_`
2. Build iterator over the memtable
3. Write sorted entries to L0 SST file
4. Call `RemoveMemTablesOrRestoreFlags()` to update the list

**FIFO rationale:**
- Oldest MemTable has highest probability of containing obsolete keys (later versions in newer MemTables)
- Flushing oldest first maximizes space reclamation
- Maintains write order in LSM tree (older writes flushed to older SST files)

---

## 8. Concurrent MemTable Writes

**Files:** `db/memtable.cc`, `db/db_impl/db_impl_write.cc`

### 8.1 allow_concurrent_memtable_write

**Option:** `allow_concurrent_memtable_write` (default: true)

When enabled, multiple threads can insert into MemTable concurrently without holding the DB mutex.

**Architecture:**

```
+--------------------------------------------------------------+
|  WriteThread (db/write_thread.h)                             |
|  +------------------------------------------------------+    |
|  | Leader election: one thread becomes leader           |    |
|  |  +- Leader performs WAL write (serialized)           |    |
|  |  +- Leader assigns sequence numbers to batch         |    |
|  |  +- Leader signals followers to proceed              |    |
|  +------------------------------------------------------+    |
+----------------------------+---------------------------------+
                             |
            +----------------+------------------+
            |                                   |
            v                                   v
+-----------------------+            +-----------------------+
| Leader Thread         |            | Follower Thread       |
|   MemTable::Add       |            |   MemTable::Add       |
|     |                 |            |     |                 |
|   InsertKeyConcurrently|            |   InsertKeyConcurrently|
|     (CAS-based)       |            |     (CAS-based)       |
+-----------------------+            +-----------------------+
       Concurrent insertion into InlineSkipList
```

**Write flow with concurrent memtable write:**

1. **WriteThread leader election**: One thread becomes leader
2. **Leader writes WAL**: Serialized (only leader writes)
3. **Leader assigns sequence numbers**: Atomically increments and assigns to batch
4. **Parallel MemTable insertion**: Leader + followers insert concurrently via CAS
5. **Publish sequence**: Leader publishes last sequence number when all done

### 8.2 MemTablePostProcessInfo and BatchPostProcess

**Files:** `db/memtable.h`

For concurrent writes, metadata updates (data_size, num_entries) are deferred and batched to avoid contention.

```cpp
struct MemTablePostProcessInfo {
  uint64_t data_size = 0;
  uint64_t num_entries = 0;
  uint64_t num_deletes = 0;
  uint64_t num_range_deletes = 0;
};

// Each thread accumulates locally
MemTablePostProcessInfo post_process_info;
memtable->Add(..., allow_concurrent=true, &post_process_info, ...);

// After all inserts, batch update MemTable metadata
memtable->BatchPostProcess(post_process_info);
```

**BatchPostProcess** (`db/memtable.h`):

```cpp
void MemTable::BatchPostProcess(const MemTablePostProcessInfo& update_counters) {
  table_->BatchPostProcess();  // Notify memtable rep
  num_entries_.FetchAddRelaxed(update_counters.num_entries);
  data_size_.FetchAddRelaxed(update_counters.data_size);
  if (update_counters.num_deletes != 0) {
    num_deletes_.FetchAddRelaxed(update_counters.num_deletes);
  }
  if (update_counters.num_range_deletes > 0) {
    num_range_deletes_.FetchAddRelaxed(update_counters.num_range_deletes);
  }
  UpdateFlushState();
}
```

### 8.3 Performance Impact

**Concurrent writes** (`allow_concurrent_memtable_write=true`):
- **Pros**: Higher write throughput (parallel MemTable insertion)
- **Cons**: CAS contention on skiplist pointers, bloom filter uses `AddConcurrently()`

**Sequential writes** (`allow_concurrent_memtable_write=false`):
- **Pros**: Simpler, no CAS overhead, better for single-threaded workloads
- **Cons**: Lower write throughput (serialized MemTable insertion)

```
INVARIANT: Concurrent writes require MemTableRepFactory::IsInsertConcurrentlySupported() == true.
INVARIANT: SkipListFactory and VectorRepFactory support concurrent insert.
INVARIANT: HashSkipList and HashLinkList do NOT support concurrent insert by default.
```

---

## Summary

MemTable is RocksDB's in-memory write buffer with these key characteristics:

1. **Pluggable representations**: SkipList (default lock-free), HashSkipList, HashLinkList, Vector
2. **InlineSkipList**: CAS-based lock-free concurrent insert, O(log D) finger search for sequential inserts
3. **Arena allocation**: Bump-pointer allocator with inline block and per-core shards for low contention
4. **Insert path**: Encode `(user_key, seq, type, value)` -> arena allocate -> InsertKey/InsertKeyConcurrently -> bloom filter add
5. **Lookup path**: Bloom filter negative check -> table_->Get()/GetAndValidate() -> SaveValue callback
6. **Flush triggers**: Memory heuristic based on write_buffer_size, range delete limit, manual flush
7. **MemTableList**: List of ReadOnlyMemTable objects, flushed oldest-first, versioned for consistent reads
8. **Concurrent writes**: Lock-free CAS insertion when `allow_concurrent_memtable_write=true`, batched metadata via `BatchPostProcess`

**Key files:**
- `db/memtable.h`, `db/memtable.cc` -- MemTable core (inherits ReadOnlyMemTable)
- `memtable/inlineskiplist.h` -- Lock-free skiplist
- `memtable/skiplistrep.cc`, `memtable/hash_skiplist_rep.cc` -- MemTableRep implementations
- `memory/arena.h`, `memory/arena.cc` -- Arena allocator (bump-pointer with inline block)
- `memory/concurrent_arena.h`, `memory/concurrent_arena.cc` -- Thread-safe arena with per-core shards
- `db/memtable_list.h`, `db/memtable_list.cc` -- Immutable MemTable management
- `include/rocksdb/memtablerep.h` -- MemTableRep interface
