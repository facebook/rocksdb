# RocksDB MemTable

## Overview

MemTable is RocksDB's in-memory write buffer that accepts all incoming writes (Put/Delete/Merge) before they are flushed to persistent SST files. It provides fast concurrent insertion and point/range lookups while maintaining sorted order by internal key `(user_key, sequence_number_descending)`.

### Key Characteristics

- **Sorted in-memory buffer**: Keys sorted by `(user_key, seq_desc)` for efficient iteration
- **Write-optimized**: Lock-free concurrent inserts via CAS operations (when `allow_concurrent_memtable_write=true`)
- **Pluggable representations**: SkipList (default), HashSkipList, HashLinkList, Vector
- **Bloom filter acceleration**: Optional prefix/whole-key bloom filter for negative lookups
- **Arena allocation**: All memory allocated from arena/concurrent_arena for cache locality
- **Reference counted**: Transitions from mutable → immutable → flushed with refcount management

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  WRITE PATH (DBImpl::WriteImpl)                                │
│  WriteBatch → MemTable::Add                                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         v
┌─────────────────────────────────────────────────────────────────┐
│  MEMTABLE (db/memtable.h)                                      │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ Active MemTable (mutable, accepts writes)            │     │
│  │  ├─ table_: MemTableRep (point key/value pairs)     │     │
│  │  ├─ range_del_table_: MemTableRep (range deletes)   │     │
│  │  ├─ bloom_filter_: DynamicBloom (optional)          │     │
│  │  ├─ arena_: Arena (memory allocation)               │     │
│  │  └─ flush_state_: FLUSH_NOT_REQUESTED/REQUESTED     │     │
│  └──────────────────────────────────────────────────────┘     │
│                                                                 │
│  Flush Triggers:                                               │
│   • ApproximateMemoryUsage() ≥ write_buffer_size              │
│   • NumRangeDeletion() ≥ memtable_max_range_deletions         │
│   • Marked for flush (manual/error recovery)                  │
└────────────────────────┬───────────────────────────────────────┘
                         │
                         v
┌─────────────────────────────────────────────────────────────────┐
│  MEMTABLE REPRESENTATION (include/rocksdb/memtablerep.h)      │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ SkipList (default) - InlineSkipList                  │     │
│  │  • Lock-free concurrent insert via CAS               │     │
│  │  • 12 levels by default, max 32 levels               │     │
│  │  • O(log N) insert/lookup, O(log D) sequential insert│     │
│  │  • Nodes stored inline with key data                 │     │
│  └──────────────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ HashSkipList - prefix hash + skiplist per bucket     │     │
│  │  • Optimized for prefix seek                         │     │
│  │  • Requires prefix extractor                         │     │
│  └──────────────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ HashLinkList - prefix hash + sorted linked list      │     │
│  │  • Memory efficient for small datasets               │     │
│  └──────────────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ Vector - unsorted vector, sorted on MarkReadOnly()   │     │
│  │  • Bulk load optimization                            │     │
│  └──────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
                         │
                         v
┌─────────────────────────────────────────────────────────────────┐
│  IMMUTABLE MEMTABLE LIST (db/memtable_list.h)                 │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ MemTableListVersion                                  │     │
│  │  memlist_ = [imm3, imm2, imm1]  (FIFO order)        │     │
│  │  ↓ Get() searches newest → oldest                   │     │
│  │  ↓ FlushJob flushes oldest first                    │     │
│  └──────────────────────────────────────────────────────┘     │
└────────────────────────┬───────────────────────────────────────┘
                         │
                         v
┌─────────────────────────────────────────────────────────────────┐
│  FLUSH TO SST (db/flush_job.cc)                                │
│  FlushJob iterates immutable memtable → writes L0 SST         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. MemTable Interface and Representations

**Files:** `include/rocksdb/memtablerep.h`, `db/memtable.h`, `db/memtable.cc`

### Core Abstraction: MemTableRep

`MemTableRep` is the abstract interface that all memtable representations must implement. It defines a sorted, concurrent-readable, single-writer (or lock-free multi-writer) container.

**Interface contract** (`include/rocksdb/memtablerep.h:62`):

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
  virtual void Insert(KeyHandle handle) = 0;
  virtual void InsertConcurrently(KeyHandle handle) = 0;  // Lock-free
  virtual void InsertWithHint(KeyHandle handle, void** hint);
  virtual Iterator* GetIterator(Arena* arena = nullptr) = 0;
  // ...
};
```

### 1.1 SkipList (Default) - InlineSkipList

**Files:** `memtable/inlineskiplist.h`, `memtable/skiplistrep.cc`

InlineSkipList is the default memtable representation, optimized for concurrent reads and lock-free concurrent writes via CAS.

**Properties** (`memtable/inlineskiplist.h:70-78`):

- **Max height**: 32 levels (configurable, default 12)
- **Branching factor**: 4 (1/4 probability of growing each level)
- **Complexity**: O(log N) insert/lookup, O(log D) for sequential inserts with finger search
- **Memory layout**: Node header + key inlined, next pointers stored **below** Node struct

**Thread safety** (`memtable/inlineskiplist.h:20-27`):

```
⚠️ INVARIANT: Writes via Insert() require external synchronization (write thread).
⚠️ INVARIANT: InsertConcurrently() is lock-free, safe with concurrent reads/inserts.
⚠️ INVARIANT: Allocated nodes never deleted until InlineSkipList destroyed.
⚠️ INVARIANT: Node contents (except next pointers) immutable after linking.
```

**Advantages:**
- Proven data structure with predictable O(log N) performance
- Lock-free concurrent insert via CAS
- Cache-friendly sequential access with hints
- Well-suited for random write workloads

**Disadvantages:**
- Higher memory overhead per node (~24 bytes overhead for 12-level average)
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

**Disadvantages:**
- Cannot serve reads during writes
- O(N log N) sort penalty on `MarkReadOnly()`

---

## 2. InlineSkipList Internals

**Files:** `memtable/inlineskiplist.h`

### 2.1 Node Layout

InlineSkipList uses a custom memory layout to save space. Instead of storing the key as a pointer, the key is stored **inline** immediately after the Node header.

**Memory layout** (`memtable/inlineskiplist.h:352-374`):

```
For a node with height H storing key K:

Memory layout (addresses grow downward → upward):
┌─────────────────────────────────────────┐ ← AllocateNode return address
│  next_[H-1] (AtomicPointer)             │
│  next_[H-2]                             │
│  ...                                     │
│  next_[1]                               │
├─────────────────────────────────────────┤ ← Node* (struct base)
│  next_[0]   (AtomicPointer)             │ ← Node::next_[0]
├─────────────────────────────────────────┤ ← Node::Key()
│  Key data:                              │
│    varint32(internal_key_size)          │
│    user_key bytes                       │
│    8-byte packed(seq, type)             │
│    varint32(value_size)                 │
│    value bytes                          │
│    checksum (if enabled)                │
└─────────────────────────────────────────┘

Accessing next_[n]:  (&next_[0] - n)  (pointer arithmetic)
Accessing key:       &next_[1]        (immediately after next_[0])
```

**Key insight**: `next_[1..H-1]` are stored **before** the Node struct. This avoids wasting a pointer in the Node struct for the key, saving `sizeof(void*)` bytes per node.

**Node methods** (`memtable/inlineskiplist.h:358-405`):

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
  void StashHeight(int height) {
    memcpy(&next_[0], &height, sizeof(int));
  }
  int UnstashHeight() const { /* inverse */ }
};
```

### 2.2 Concurrent Insert via CAS

**Files:** `memtable/inlineskiplist.h:119-138`

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

**Memory ordering** (`memtable/inlineskiplist.h:376-395`):

```
⚠️ INVARIANT: SetNext() uses release-store so readers see fully initialized node.
⚠️ INVARIANT: Next() uses acquire-load to observe initialization.
⚠️ INVARIANT: CASNext() provides acquire-release semantics for linearizability.
```

### 2.3 Finger Search Optimization

**Files:** `memtable/inlineskiplist.h:143-150`

For sequential insertions (common pattern in WriteBatch), InlineSkipList uses **finger search** to reduce cost from O(log N) to **O(log D)** where D = distance from last insert position.

**Splice structure** (`memtable/inlineskiplist.h:340-350`):

```cpp
struct Splice {
  int height_;
  Node** prev_;  // prev_[i] < key for all levels i
  Node** next_;  // next_[i] >= key for all levels i
  // INVARIANT: prev_[i].key < prev_[i-1].key < ... < key <= ... < next_[i-1].key < next_[i].key
};
```

**Usage:**

```cpp
void* hint = nullptr;  // Splice cached across InsertWithHint calls
table_->InsertWithHint(handle1, &hint);  // O(log N) first insert
table_->InsertWithHint(handle2, &hint);  // O(log D) if handle2 near handle1
table_->InsertWithHint(handle3, &hint);  // O(log D) sequential pattern
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

**Files:** `memory/arena.h:31-100`

Arena is a simple bump-pointer allocator that carves out memory from fixed-size blocks.

**Allocation strategy:**

```cpp
char* Arena::Allocate(size_t bytes) {
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);  // New block needed
}

char* AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Large allocation: dedicated block
    return AllocateNewBlock(bytes);
  }
  // Small allocation: waste remaining space, allocate new block
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;
  return Allocate(bytes);
}
```

**Memory overhead:**
- Fragmentation: Up to `kBlockSize` wasted per arena (last block may be partially full)
- Default `kBlockSize` = 4 KB (configurable via `arena_block_size`)

### 3.3 ConcurrentArena

**Files:** `memory/concurrent_arena.h:35-100`

ConcurrentArena wraps Arena with:
1. **Spinlock protection** on main arena
2. **Per-core shards** for small allocations (<= 2KB default) to reduce contention

**Allocation path** (`memory/concurrent_arena.h:52-68`):

```cpp
char* ConcurrentArena::Allocate(size_t bytes) {
  return AllocateImpl(bytes, false,
    [this, bytes] { return arena_.Allocate(bytes); });
}

template <typename Func>
char* AllocateImpl(size_t bytes, bool force_arena, Func allocate_from_arena) {
  size_t cpu = port::PhysicalCoreID();  // Current CPU core

  // Try per-core shard first (lock-free fast path)
  if (!force_arena && bytes <= shard_block_size_) {
    Shard* shard = &shards_[cpu % num_cpus];
    SpinMutexLock lock(&shard->mutex);
    if (bytes <= shard->allocated_and_unused_) {
      shard->allocated_and_unused_ -= bytes;
      char* result = shard->free_begin_;
      shard->free_begin_ += bytes;
      return result;  // Fast path: no main arena lock
    }
  }

  // Fallback to main arena (spinlock required)
  SpinMutexLock lock(&arena_mutex_);
  return allocate_from_arena();
}
```

**Shard refill** (`memory/concurrent_arena.cc`):

When a shard runs out of space, it allocates a new `shard_block_size_` chunk from the main arena.

```
⚠️ INVARIANT: Per-core shards never freed until MemTable destroyed.
⚠️ INVARIANT: Shard block size chosen to evenly divide arena block size (no cross-block fragments).
```

---

## 4. Insert Path: Put/Delete/Merge to MemTable

**Files:** `db/memtable.cc:950-1060`

### 4.1 Entry Encoding

Every key-value pair is encoded into a self-contained binary format before insertion.

**Format** (`db/memtable.cc:956-961`):

```
┌────────────────────────────────────────────────────────────┐
│ varint32(internal_key_size)                                │  internal_key_size = user_key.size() + 8
│ user_key bytes                                             │  Raw user key
│ uint64 packed(sequence_number, ValueType)                 │  seq (56 bits) | type (8 bits)
│ varint32(value_size)                                       │
│ value bytes                                                │  Raw value (empty for Delete)
│ [optional] checksum                                        │  protection_bytes_per_key (0, 1, 2, 4, or 8)
└────────────────────────────────────────────────────────────┘
```

**ValueType encoding** (`db/dbformat.h`):

```cpp
enum ValueType : uint8_t {
  kTypeDeletion         = 0x0,  // Delete
  kTypeValue            = 0x1,  // Put
  kTypeMerge            = 0x2,  // Merge
  kTypeRangeDeletion    = 0xF,  // DeleteRange
  kTypeSingleDeletion   = 0x7,  // SingleDelete
  kTypeWideColumnEntity = 0x12, // PutEntity (wide columns)
  // ...
};
```

### 4.2 MemTable::Add() Flow

**Files:** `db/memtable.cc:950-1060`

```cpp
Status MemTable::Add(SequenceNumber s, ValueType type,
                     const Slice& key,      // User key
                     const Slice& value,
                     const ProtectionInfoKVOS64* kv_prot_info,
                     bool allow_concurrent,
                     MemTablePostProcessInfo* post_process_info,
                     void** hint) {
  // 1. Encode entry
  uint32_t internal_key_size = key.size() + 8;
  uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size
                       + VarintLength(value.size()) + value.size()
                       + moptions_.protection_bytes_per_key;

  char* buf = nullptr;
  KeyHandle handle = table_->Allocate(encoded_len, &buf);  // Arena allocation

  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key.size());
  p += key.size();
  EncodeFixed64(p, PackSequenceAndType(s, type));  // Pack seq + type
  p += 8;
  p = EncodeVarint32(p, value.size());
  memcpy(p, value.data(), value.size());

  // 2. Optional checksum
  UpdateEntryChecksum(kv_prot_info, key, value, type, s,
                      buf + encoded_len - moptions_.protection_bytes_per_key);

  // 3. Insert into memtable
  if (allow_concurrent) {
    table_->InsertConcurrently(handle);  // Lock-free CAS insert
  } else {
    if (hint && insert_with_hint_prefix_extractor_) {
      table_->InsertWithHint(handle, hint);  // Finger search optimization
    } else {
      table_->Insert(handle);  // Single-writer insert
    }
  }

  // 4. Update bloom filter
  if (bloom_filter_) {
    bloom_filter_->Add(ExtractUserKey(Slice(buf, encoded_len)));
  }

  // 5. Update metadata
  if (!allow_concurrent) {
    data_size_ += encoded_len;
    num_entries_++;
    if (type == kTypeDeletion || type == kTypeSingleDeletion) {
      num_deletes_++;
    }
  } else {
    // Batch update via post_process_info for concurrent case
    post_process_info->data_size += encoded_len;
    post_process_info->num_entries++;
    // ...
  }

  // 6. Update flush state
  UpdateFlushState();

  return Status::OK();
}
```

**Concurrent vs. sequential insert:**

| Mode | When | Insert method | Metadata update |
|------|------|---------------|-----------------|
| Sequential | `allow_concurrent_memtable_write=false` | `Insert()` or `InsertWithHint()` | Immediate `data_size_++`, `num_entries_++` |
| Concurrent | `allow_concurrent_memtable_write=true` | `InsertConcurrently()` | Deferred via `MemTablePostProcessInfo` batch update |

```
⚠️ INVARIANT: Sequential inserts acquire write thread lock (single writer).
⚠️ INVARIANT: Concurrent inserts use CAS, no external lock required.
⚠️ INVARIANT: Metadata (data_size_, num_entries_) updated atomically in batches for concurrent writes.
```

---

## 5. Lookup Path: Get and MultiGet

**Files:** `db/memtable.cc:1405-1550`

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
      NewRangeTombstoneIterator(read_opts, key.sequence(), immutable_memtable));
  if (range_del_iter) {
    SequenceNumber covering_seq =
        range_del_iter->MaxCoveringTombstoneSeqnum(key.user_key());
    if (covering_seq > *max_covering_tombstone_seq) {
      *max_covering_tombstone_seq = covering_seq;
    }
  }

  // 2. Bloom filter check (negative lookup optimization)
  if (bloom_filter_) {
    bool may_contain = false;
    if (prefix_extractor_ && prefix_extractor_->InDomain(key.user_key())) {
      may_contain = bloom_filter_->MayContain(
          prefix_extractor_->Transform(key.user_key()));
    } else if (moptions_.memtable_whole_key_filtering) {
      may_contain = bloom_filter_->MayContain(key.user_key());
    } else {
      may_contain = true;  // Bloom filter disabled for this key
    }

    if (!may_contain) {
      PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
      return false;  // Definitely not present
    }
    PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
  }

  // 3. Search memtable representation
  MemTableRep::Iterator* iter = table_->GetDynamicPrefixIterator();
  iter->Seek(key.internal_key().data());

  // 4. Process result (call SaveValue to handle merges, deletes, etc.)
  bool found = SaveValue(...);  // Handles ValueType logic

  return found;
}
```

### 5.2 Bloom Filter (DynamicBloom)

**Files:** `util/dynamic_bloom.h`, `util/dynamic_bloom.cc`

MemTable optionally uses a prefix bloom filter to accelerate negative lookups (key definitely not present).

**Configuration** (`db/memtable.cc:133-140`):

```cpp
if ((prefix_extractor_ || moptions_.memtable_whole_key_filtering) &&
    moptions_.memtable_prefix_bloom_bits > 0) {
  bloom_filter_.reset(
      new DynamicBloom(&arena_, moptions_.memtable_prefix_bloom_bits,
                       6 /* hard coded 6 hash probes */,
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
- → Bloom filter size = `64 MB * 0.125 = 8 MB = 67M bits`

**False positive rate** (6 hash functions):

```
FPR ≈ (1 - e^(-6*N/M))^6  where N = num_keys, M = num_bits
```

For 1M keys with 67M bits: `FPR ≈ 0.4%` (very low)

```
⚠️ INVARIANT: Bloom filter has false positives but never false negatives.
⚠️ INVARIANT: Bloom filter memory allocated from arena, freed with MemTable.
```

### 5.3 MultiGet Optimization

**Files:** `db/memtable.cc:1550-1650`, `memtable/inlineskiplist.h:143-160`

For batched lookups of sorted keys, MemTable uses **finger search** to reduce per-key cost from O(log N) to **O(log D)**.

**Batch lookup interface:**

```cpp
void MemTable::MultiGet(const ReadOptions& read_options,
                        MultiGetRange* range,
                        ReadCallback* callback,
                        bool immutable_memtable) {
  // Sort keys in range
  range->Sort();

  // Use InlineSkipList::MultiGet with finger search
  table_->MultiGet(..., callback_func);
}
```

**InlineSkipList::MultiGet** (`memtable/inlineskiplist.h:155-160`):

```cpp
Status MultiGet(size_t num_keys, const char* const* keys,
                void** callback_args,
                bool (*callback_func)(void* arg, const char* entry), ...) {
  Splice finger;  // Cached search path
  finger.height_ = 0;

  for (size_t i = 0; i < num_keys; i++) {
    Node* node = nullptr;
    // Finger search: O(log D) where D = distance from previous key
    FindGreaterOrEqualWithFinger(keys[i], &node, &finger, ...);

    // Invoke callback for all matching entries
    while (node && callback_func(callback_args[i], node->Key())) {
      node = node->Next(0);
    }
  }
  return Status::OK();
}
```

**Complexity:**
- Without finger: `O(K * log N)` for K keys
- With finger: `O(K * log D)` where D = average distance between consecutive keys
- Best case (sequential): `O(K)` if keys are evenly spaced

---

## 6. MemTable Flush Triggers

**Files:** `db/memtable.cc:197-280`

MemTable transitions from mutable → immutable when flush is triggered. Flush triggers are checked in `UpdateFlushState()` after each write.

### 6.1 Flush Trigger Conditions

**Files:** `db/memtable.cc:197-230`

```cpp
bool MemTable::ShouldFlushNow() {
  // 1. Manual flush requested
  if (IsMarkedForFlush()) {
    return true;
  }

  // 2. Too many range deletions
  if (memtable_max_range_deletions_ > 0 &&
      num_range_deletes_.LoadRelaxed() >= memtable_max_range_deletions_) {
    return true;
  }

  // 3. Memory usage exceeds write_buffer_size
  size_t write_buffer_size = write_buffer_size_.LoadRelaxed();
  if (arena_.ApproximateMemoryUsage() >= write_buffer_size) {
    return true;
  }

  return false;
}
```

**Trigger summary:**

| Trigger | Condition | Rationale |
|---------|-----------|-----------|
| **Manual flush** | `MarkForFlush()` called | User request or error recovery |
| **Range delete limit** | `num_range_deletes >= memtable_max_range_deletions` | Prevent excessive range tombstones in memory |
| **Memory limit** | `ApproximateMemoryUsage() >= write_buffer_size` | Primary trigger: MemTable full |

### 6.2 write_buffer_size

**Option:** `write_buffer_size` (default: 64 MB)

Defines the target size of a single MemTable. When exceeded, the MemTable is marked for flush.

**Calculation:**

```cpp
size_t ApproximateMemoryUsage() {
  return arena_.ApproximateMemoryUsage()        // Key-value data + skiplist nodes
       + table_->ApproximateMemoryUsage()       // Skiplist overhead
       + range_del_table_->ApproximateMemoryUsage()
       + bloom_filter_->ApproximateMemoryUsage();  // If enabled
}
```

**Arena memory accounting** (`memory/arena.cc`):

```cpp
size_t Arena::ApproximateMemoryUsage() const {
  return blocks_memory_ + blocks_.capacity() * sizeof(char*);
  // blocks_memory_ = sum of all block allocations
}
```

### 6.3 max_write_buffer_number

**Option:** `max_write_buffer_number` (default: 2)

Limits the total number of memtables (1 mutable + N immutable) in memory before writes are blocked.

**Write stall logic** (`db/db_impl/db_impl_write.cc`):

```cpp
// PreprocessWrite checks:
if (num_unflushed_memtables >= max_write_buffer_number) {
  // Block writes until flush completes
  write_stall_condition = true;
}
```

**Flush state transition:**

```
[Mutable MemTable]
   ↓ ApproximateMemoryUsage() >= write_buffer_size
[Immutable MemTable #1] + [New Mutable MemTable]
   ↓ Another write_buffer_size exceeded
[Immutable #2] + [Immutable #1] + [New Mutable]
   ↓ num_unflushed >= max_write_buffer_number → WRITE STALL
   ↓ FlushJob completes for Immutable #1
[Immutable #2] + [Mutable] → Writes unblocked
```

```
⚠️ INVARIANT: At most 1 mutable MemTable per column family at any time.
⚠️ INVARIANT: Immutable MemTables are read-only, never modified.
⚠️ INVARIANT: Flush processes immutable MemTables in FIFO order (oldest first).
```

### 6.4 memtable_max_range_deletions

**Option:** `memtable_max_range_deletions` (default: 0 = disabled)

Triggers flush when MemTable accumulates too many range deletions (`DeleteRange`).

**Rationale:**
- Range deletes stored separately in `range_del_table_`
- Each range delete can cover millions of keys
- Too many range deletes → expensive iteration and memory overhead
- Flushing converts range deletes to more efficient SST format

---

## 7. MemTableList: Immutable MemTable Management

**Files:** `db/memtable_list.h`, `db/memtable_list.cc`

### 7.1 MemTableListVersion

`MemTableListVersion` is a snapshot of the list of immutable memtables. It's reference counted and used by reads/iterators to ensure consistent view.

**Structure** (`db/memtable_list.h:43-150`):

```cpp
class MemTableListVersion {
  std::list<ReadOnlyMemTable*> memlist_;           // Immutable MemTables (newest → oldest)
  std::list<ReadOnlyMemTable*> memlist_history_;  // Flushed MemTables (history)
  int refs_;                                       // Reference count
  size_t* parent_memtable_list_memory_usage_;     // Memory tracking
};
```

**Ordering:**

```
memlist_ = [imm_newest, imm_2, imm_1, imm_oldest]
           ↑ newest                     ↑ oldest (next to flush)
```

### 7.2 MemTableList Operations

**Files:** `db/memtable_list.cc`

**Add new immutable MemTable:**

```cpp
void MemTableList::Add(MemTable* m, autovector<MemTable*>* to_delete) {
  // Create new version with m prepended to memlist_
  MemTableListVersion* new_version = new MemTableListVersion(*current_);
  new_version->memlist_.push_front(m);  // Add to front (newest)
  current_->Unref(to_delete);
  current_ = new_version;
}
```

**Remove flushed MemTable:**

```cpp
void MemTableList::RemoveOldMemTable(MemTable* m, autovector<MemTable*>* to_delete) {
  // Create new version without m
  MemTableListVersion* new_version = new MemTableListVersion(*current_);
  new_version->memlist_.remove(m);  // Remove from list
  if (m->Unref()) {
    to_delete->push_back(m);  // Schedule destruction
  }
  current_->Unref(to_delete);
  current_ = new_version;
}
```

### 7.3 Read Path (Get)

**Files:** `db/memtable_list.cc:120-200`

```cpp
bool MemTableListVersion::Get(const LookupKey& key, ...) {
  // Search newest → oldest
  for (auto it = memlist_.begin(); it != memlist_.end(); ++it) {
    ReadOnlyMemTable* mem = *it;
    bool found = mem->Get(key, value, ...);
    if (found) {
      return true;  // Stop at first match (newest version)
    }
  }
  return false;  // Not found in any memtable
}
```

**Ordering guarantee:**

```
⚠️ INVARIANT: memlist_ ordered newest → oldest ensures most recent value found first.
⚠️ INVARIANT: MemTableListVersion is immutable once created (new versions for updates).
```

### 7.4 FlushJob Interaction

**Files:** `db/flush_job.cc`

FlushJob flushes **oldest** immutable MemTable first (FIFO order):

```cpp
void FlushJob::Run() {
  // Pick oldest memtable from memlist_
  MemTable* m = memlist_->PickMemtableToFlush();  // Returns oldest

  // Build iterator over m
  InternalIterator* iter = m->NewIterator(...);

  // Write to SST
  TableBuilder* builder = ...;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    builder->Add(iter->key(), iter->value());
  }
  builder->Finish();

  // Remove m from MemTableList
  memlist_->RemoveOldMemTable(m, &to_delete);
}
```

**FIFO rationale:**
- Oldest MemTable has highest probability of containing obsolete keys (later versions in newer MemTables)
- Flushing oldest first maximizes space reclamation
- Maintains write order in LSM tree (older writes flushed to older SST files)

---

## 8. Concurrent MemTable Writes

**Files:** `db/memtable.cc:950-1060`, `db/db_impl/db_impl_write.cc`

### 8.1 allow_concurrent_memtable_write

**Option:** `allow_concurrent_memtable_write` (default: true)

When enabled, multiple threads can insert into MemTable concurrently without holding the DB mutex.

**Architecture:**

```
┌─────────────────────────────────────────────────────────────┐
│  WriteThread (db/write_thread.h)                           │
│  ┌────────────────────────────────────────────────┐        │
│  │ Leader election: one thread becomes leader     │        │
│  │  ├─ Leader performs WAL write (serialized)     │        │
│  │  ├─ Leader assigns sequence numbers to batch   │        │
│  │  └─ Leader signals followers to proceed        │        │
│  └────────────────────────────────────────────────┘        │
└───────────────────────┬─────────────────────────────────────┘
                        │
       ┌────────────────┴─────────────────┐
       │                                  │
       v                                  v
┌──────────────────┐            ┌──────────────────┐
│ Leader Thread    │            │ Follower Thread  │
│ InsertInto()     │            │ InsertInto()     │
│   ↓              │            │   ↓              │
│ MemTable::Add    │            │ MemTable::Add    │
│   ↓              │            │   ↓              │
│ InsertConcurrently│            │ InsertConcurrently│
│   (CAS-based)    │            │   (CAS-based)    │
└──────────────────┘            └──────────────────┘
      Concurrent insertion into InlineSkipList
```

**Write flow with concurrent memtable write:**

1. **WriteThread leader election**: One thread becomes leader
2. **Leader writes WAL**: Serialized (only leader writes)
3. **Leader assigns sequence numbers**: Atomically increments and assigns to batch
4. **Parallel MemTable insertion**: Leader + followers insert concurrently via CAS
5. **Publish sequence**: Leader publishes last sequence number when all done

### 8.2 MemTablePostProcessInfo

**Files:** `db/memtable.h:74-79`

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
memtable->BatchedUpdateMetadata(&post_process_info);
```

**Atomic batch update** (`db/memtable.cc`):

```cpp
void MemTable::BatchedUpdateMetadata(MemTablePostProcessInfo* info) {
  data_size_.FetchAdd(info->data_size);
  num_entries_.FetchAdd(info->num_entries);
  num_deletes_.FetchAdd(info->num_deletes);
  num_range_deletes_.FetchAdd(info->num_range_deletes);
}
```

### 8.3 Performance Impact

**Concurrent writes** (`allow_concurrent_memtable_write=true`):
- **Pros**: Higher write throughput (parallel MemTable insertion)
- **Cons**: CAS contention on skiplist pointers, bloom filter contention

**Sequential writes** (`allow_concurrent_memtable_write=false`):
- **Pros**: Simpler, no CAS overhead, better for single-threaded workloads
- **Cons**: Lower write throughput (serialized MemTable insertion)

**Benchmark results** (example):

| Workload | Sequential | Concurrent | Speedup |
|----------|------------|------------|---------|
| Single thread | 100K ops/sec | 95K ops/sec | 0.95x (CAS overhead) |
| 4 threads | 110K ops/sec | 320K ops/sec | 2.9x |
| 16 threads | 120K ops/sec | 800K ops/sec | 6.7x |

```
⚠️ INVARIANT: Concurrent writes require MemTableRep that supports InsertConcurrently().
⚠️ INVARIANT: Only InlineSkipList and its derivatives support concurrent insert.
```

---

## Summary

MemTable is RocksDB's in-memory write buffer with these key characteristics:

1. **Pluggable representations**: SkipList (default lock-free), HashSkipList, HashLinkList, Vector
2. **InlineSkipList**: CAS-based lock-free concurrent insert, O(log D) finger search for sequential inserts
3. **Arena allocation**: Bump-pointer allocator with per-core shards for low contention
4. **Insert path**: Encode `(user_key, seq, type, value)` → arena allocate → insert → bloom filter add
5. **Lookup path**: Bloom filter negative check → skiplist seek → process result (merge, delete, etc.)
6. **Flush triggers**: `write_buffer_size` exceeded, range delete limit, manual flush
7. **MemTableList**: FIFO list of immutable MemTables, flushed oldest-first, versioned for consistent reads
8. **Concurrent writes**: Lock-free CAS insertion when `allow_concurrent_memtable_write=true`, batched metadata updates

**Key files:**
- `db/memtable.h`, `db/memtable.cc` — MemTable core
- `memtable/inlineskiplist.h` — Lock-free skiplist
- `memtable/skiplistrep.cc`, `memtable/hash_skiplist_rep.cc` — MemTableRep implementations
- `memory/arena.cc`, `memory/concurrent_arena.cc` — Arena allocators
- `db/memtable_list.h`, `db/memtable_list.cc` — Immutable MemTable management
- `include/rocksdb/memtablerep.h` — MemTableRep interface
