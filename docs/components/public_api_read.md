# Public Read APIs

This document provides a deep dive into RocksDB's public read APIs, covering point lookups (Get, MultiGet), range scans (Iterators), merge operand retrieval, and async I/O optimizations.

**Key Source Files:**
- `include/rocksdb/db.h` - Main DB interface with all read APIs
- `include/rocksdb/options.h` - ReadOptions configuration
- `include/rocksdb/slice.h` - Slice and PinnableSlice
- `include/rocksdb/iterator.h`, `include/rocksdb/iterator_base.h` - Iterator interface
- `db/db_impl/db_impl.cc` - Get/MultiGet implementations
- `file/random_access_file_reader.cc` - Async I/O support

---

## Table of Contents

1. [Overview](#overview)
2. [DB::Get - Point Lookups](#dbget---point-lookups)
3. [DB::MultiGet - Batched Point Lookups](#dbmultiget---batched-point-lookups)
4. [DB::NewIterator - Range Scans](#dbnewiterator---range-scans)
5. [Iterator Operations](#iterator-operations)
6. [DB::GetMergeOperands](#dbgetmergeoperands)
7. [DB::GetEntity / DB::MultiGetEntity](#dbgetentity--dbmultigetentity---wide-column-reads)
8. [DB::KeyMayExist](#dbkeymayexist---lightweight-existence-check)
9. [Async I/O](#async-io)
10. [Scan Patterns](#scan-patterns)
11. [ReadOptions Reference](#readoptions-reference)
12. [PinnableSlice - Zero-Copy Values](#pinnableslice---zero-copy-values)
13. [Column Family Reads](#column-family-reads)

---

## Overview

RocksDB provides two primary read patterns:

1. **Point Lookups**: Retrieve specific key-value pairs using `Get()` or `MultiGet()`
2. **Range Scans**: Iterate over key ranges using `Iterator`

Both patterns share the same `ReadOptions` configuration and can operate on specific column families or the default column family.

```
Read Path Decision Tree:

Single key?
  yes -> Need wide columns?
           yes -> GetEntity()
           no  -> Need existence only?
                    yes -> KeyMayExist()
                    no  -> Get()
  no  -> Multiple specific keys?
           yes -> MultiGet() / MultiGetEntity()
           no  -> Range/prefix scan?
                    yes -> NewIterator() + Seek/Next/Prev
                    no  -> Multiple ranges?
                             yes -> NewMultiScan()
```

⚠️ **INVARIANT**: All reads see a consistent snapshot of the database. If `ReadOptions::snapshot` is nullptr, an implicit snapshot is created at the start of the read operation.

---

## DB::Get - Point Lookups

### API Signature

```cpp
// include/rocksdb/db.h:599-601
virtual Status Get(const ReadOptions& options,
                   ColumnFamilyHandle* column_family,
                   const Slice& key,
                   PinnableSlice* value,
                   std::string* timestamp) = 0;
```

**Overloads** (defined in `include/rocksdb/db.h:606-656`):
- `Get(options, column_family, key, PinnableSlice* value)` - No timestamp
- `Get(options, column_family, key, std::string* value, timestamp)` - std::string with timestamp
- `Get(options, column_family, key, std::string* value)` - std::string, no timestamp
- `Get(options, key, std::string* value)` - Default column family
- `Get(options, key, std::string* value, timestamp)` - Default column family with timestamp

Note: There is no default-column-family convenience overload for `PinnableSlice*`. Use `Get(options, db->DefaultColumnFamily(), key, &pinnable_val)` explicitly.

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `options` | `const ReadOptions&` | Read configuration (snapshot, caching, checksums) |
| `column_family` | `ColumnFamilyHandle*` | Target column family (or `DefaultColumnFamily()`) |
| `key` | `const Slice&` | Key to lookup (without timestamp suffix) |
| `value` | `PinnableSlice*` | Output value (may avoid copy if pinned) |
| `timestamp` | `std::string*` | Optional: returns key's timestamp (user-defined timestamp feature) |

### Return Values

| Status | Meaning |
|--------|---------|
| `OK` | Key found, value populated |
| `OK` (subcode `kMergeOperandThresholdExceeded`) | Key found, but `merge_operand_count_threshold` was exceeded |
| `NotFound` | Key does not exist |
| `Corruption` | Data corruption detected (if `verify_checksums=true`) |
| `IOError` | I/O failure reading SST file |
| `Incomplete` | Deadline/timeout exceeded or `read_tier` restriction |

### Behavior

1. **Snapshot isolation**: Read as of `ReadOptions::snapshot` (or implicit snapshot if null)
2. **Search order**: Active memtable → Immutable memtables → L0 SSTs (newest first) → L1+ SSTs (level-by-level)
3. **Short-circuit**: Returns immediately on finding the key (or tombstone)
4. **Merge handling**: Applies merge operands up to base value (or until `merge_operand_count_threshold`)

⚠️ **INVARIANT**: `Get()` returns the value as of the snapshot, even if newer writes occurred after the snapshot was taken.

### Example

```cpp
ReadOptions read_opts;
read_opts.verify_checksums = true;
read_opts.fill_cache = true;

PinnableSlice value;
Status s = db->Get(read_opts, "user:12345", &value);

if (s.ok()) {
  // Value is available in value.data() / value.size()
  ProcessValue(value);
} else if (s.IsNotFound()) {
  // Key does not exist
} else {
  // Handle error
  LOG(ERROR) << "Get failed: " << s.ToString();
}
```

---

## DB::MultiGet - Batched Point Lookups

### API Signature

```cpp
// include/rocksdb/db.h:822-825
virtual void MultiGet(const ReadOptions& options,
                      const size_t num_keys,
                      ColumnFamilyHandle** column_families,
                      const Slice* keys,
                      PinnableSlice* values,
                      std::string* timestamps,
                      Status* statuses,
                      const bool sorted_input = false) = 0;
```

**Overloads**:
- Single column family: `MultiGet(options, column_family, num_keys, keys, values, statuses, sorted_input)`
- Default column family with vectors: `MultiGet(options, keys_vector)` → returns `vector<Status>`

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `num_keys` | `size_t` | Number of keys to retrieve |
| `column_families` | `ColumnFamilyHandle**` | Array of column family handles (one per key) |
| `keys` | `const Slice*` | Array of keys to lookup |
| `values` | `PinnableSlice*` | Output array for values |
| `timestamps` | `std::string*` | Optional: output array for timestamps |
| `statuses` | `Status*` | Per-key status results |
| `sorted_input` | `bool` | Hint: keys are sorted (enables optimizations) |

⚠️ **INVARIANT**: `num_keys` must match the size of all arrays (`column_families`, `keys`, `values`, `statuses`).

### Optimizations

1. **Sorted key optimization**: If `sorted_input=true`, MultiGet can:
   - Merge-scan memtables more efficiently
   - Batch read blocks from SST files in sequential order
   - Skip duplicate keys early

2. **Parallel I/O**: With `ReadOptions::async_io=true` and `optimize_multiget_for_io=true`:
   - Reads SST files across multiple levels concurrently
   - Uses io_uring (Linux) for async I/O when available
   - Reduces tail latency by maximizing I/O parallelism

3. **Batched block reads**: Groups keys targeting the same SST file to batch block cache lookups and disk reads

### Flow Diagram

```
MultiGet(keys[])
    ↓
Sort keys by (CF ID, user key) if sorted_input=false
    ↓
For each level (memtable → L0 → L1 → ...):
    ↓
    Batch keys by SST file
    ↓
    If async_io:
      yes -> Issue async reads for all files in parallel
               -> Poll completion (io_uring or thread pool)
               -> Merge results
      no  -> (continue below)
    ↓
Sequential read per file
    ↓
Short-circuit keys as found
    ↓
Return statuses[]
```

### Example

```cpp
ReadOptions read_opts;
read_opts.async_io = true;
read_opts.optimize_multiget_for_io = true;

const size_t num_keys = 100;
std::vector<Slice> keys(num_keys);
std::vector<PinnableSlice> values(num_keys);
std::vector<Status> statuses(num_keys);

// Populate keys...
for (size_t i = 0; i < num_keys; ++i) {
  keys[i] = EncodeUserKey(i);
}

db->MultiGet(read_opts, db->DefaultColumnFamily(), num_keys,
             keys.data(), values.data(), statuses.data(),
             /*sorted_input=*/false);

for (size_t i = 0; i < num_keys; ++i) {
  if (statuses[i].ok()) {
    ProcessValue(keys[i], values[i]);
  } else if (!statuses[i].IsNotFound()) {
    LOG(ERROR) << "Key " << i << " error: " << statuses[i].ToString();
  }
}
```

⚠️ **INVARIANT**: MultiGet uses a single snapshot for all keys, ensuring cross-key consistency.

### Value Size Limits

`ReadOptions::value_size_soft_limit` caps cumulative value size:

```cpp
read_opts.value_size_soft_limit = 10 * 1024 * 1024; // 10MB

// If cumulative values exceed 10MB, remaining keys return Status::Aborted
db->MultiGet(read_opts, num_keys, ...);
```

---

## DB::NewIterator - Range Scans

### API Signature

```cpp
// include/rocksdb/db.h:990-991
virtual Iterator* NewIterator(const ReadOptions& options,
                              ColumnFamilyHandle* column_family) = 0;
```

**Overload**: `NewIterator(options)` uses `DefaultColumnFamily()`.

### Lifetime Management

⚠️ **INVARIANT**: Caller owns the returned `Iterator*` and must delete it before closing the DB.

```cpp
Iterator* it = db->NewIterator(read_opts);
// Use iterator...
delete it; // REQUIRED before db.reset() or DB::Close()
```

### Iterator Properties

Iterators support querying runtime properties via `GetProperty()`:

| Property | Description |
|----------|-------------|
| `rocksdb.iterator.is-key-pinned` | "1" if `key()` remains valid until iterator deletion |
| `rocksdb.iterator.is-value-pinned` | "1" if `value()` remains valid until iterator deletion |
| `rocksdb.iterator.super-version-number` | LSM version used by iterator |
| `rocksdb.iterator.internal-key` | User-key portion of the internal key at which iteration stopped |
| `rocksdb.iterator.write-time` | Best estimate of write time as 64-bit raw value (8 bytes, decode with `DecodeU64Ts`); returns `std::numeric_limits<uint64_t>::max()` if unknown, 0 if seqno zeroed out |

**Pinning**: Keys/values are pinned when `ReadOptions::pin_data=true` and `BlockBasedTableOptions::use_delta_encoding=false`.

---

## Iterator Operations

All iterator operations are defined in `include/rocksdb/iterator_base.h` and `include/rocksdb/iterator.h`.

### Core Operations

```cpp
// include/rocksdb/iterator_base.h:26-103

virtual bool Valid() const = 0;   // Iterator positioned at valid entry?

virtual void SeekToFirst() = 0;  // Position at first key
virtual void SeekToLast() = 0;   // Position at last key
virtual void Seek(const Slice& target) = 0;        // Seek >= target
virtual void SeekForPrev(const Slice& target) = 0; // Seek <= target
virtual void Next() = 0;  // Advance forward  (REQUIRES: Valid())
virtual void Prev() = 0;  // Advance backward (REQUIRES: Valid())

virtual Slice key() const = 0;    // Current key   (REQUIRES: Valid())
virtual Status status() const = 0; // Error status
```

Note: `value()` is defined in `include/rocksdb/iterator.h:45` (inherits from `IteratorBase`).

### Seek Semantics

| Operation | Target Behavior | Boundary Behavior |
|-----------|-----------------|-------------------|
| `Seek(k)` | Position at first key `>= k` | If no key `>= k`, `Valid()=false` |
| `SeekForPrev(k)` | Position at last key `<= k` | If no key `<= k`, `Valid()=false` |
| `SeekToFirst()` | Position at smallest key | Empty DB: `Valid()=false` |
| `SeekToLast()` | Position at largest key | Empty DB: `Valid()=false` |

⚠️ **INVARIANT**: Seek operations clear previous error status. After seek, `status()` reflects only seek-time errors.

### Navigation Pattern

```cpp
// Forward iteration
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  ProcessEntry(it->key(), it->value());
}
if (!it->status().ok()) {
  // Handle error
}

// Reverse iteration
for (it->SeekToLast(); it->Valid(); it->Prev()) {
  ProcessEntry(it->key(), it->value());
}

// Seek to specific key
it->Seek("user:1000");
if (it->Valid() && it->key().starts_with("user:1000")) {
  // Found exact key or next key
}
```

### Iterator Bounds

`ReadOptions::iterate_lower_bound` and `iterate_upper_bound` constrain iteration:

```cpp
ReadOptions opts;
Slice lower = "user:1000";
Slice upper = "user:2000";
opts.iterate_lower_bound = &lower;
opts.iterate_upper_bound = &upper;  // Exclusive

Iterator* it = db->NewIterator(opts);
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  // Only sees keys in [user:1000, user:2000)
}
delete it;
```

⚠️ **INVARIANT**: `iterate_upper_bound` is **exclusive**. Iterator will never return a key `>= upper_bound`.

⚠️ **INVARIANT**: With `prefix_extractor`, bounds must share the same prefix as the seek key (unless `auto_prefix_mode=true`).

### Wide Columns

For wide-column entities (see `docs/components/wide_column.md`):

```cpp
virtual const WideColumns& columns() const;  // include/rocksdb/iterator.h:54

// Returns all columns for current entry
for (it->Seek(key); it->Valid(); it->Next()) {
  const WideColumns& cols = it->columns();
  for (const auto& col : cols) {
    ProcessColumn(col.name(), col.value());
  }
}
```

### PrepareValue() - Lazy Value Loading

When `ReadOptions::allow_unprepared_value=true`, iterators may defer loading values until explicitly requested:

```cpp
ReadOptions opts;
opts.allow_unprepared_value = true;

Iterator* it = db->NewIterator(opts);
for (it->Seek(start_key); it->Valid(); it->Next()) {
  if (ShouldProcessKey(it->key())) {
    if (!it->PrepareValue()) {
      // Value loading failed
      LOG(ERROR) << it->status().ToString();
      break;
    }
    ProcessValue(it->value());
  }
  // Else skip expensive value load
}
delete it;
```

**Use cases**: BlobDB with large values, multi-CF iterators, filtering by key before loading value.

### Refresh() - Update Iterator View

`Refresh()` updates the iterator to see the latest DB state without creating a new iterator:

```cpp
// include/rocksdb/iterator_base.h:69-75
virtual Status Refresh();                       // Latest DB state
virtual Status Refresh(const class Snapshot*);  // Latest state under snapshot
```

After `Refresh()`, the iterator is invalidated and must be repositioned with a `Seek*()` call. This is more efficient than deleting and recreating the iterator.

---

## DB::GetMergeOperands

Retrieve individual merge operands without applying the merge operator.

### API Signature

```cpp
// include/rocksdb/db.h:707-711
virtual Status GetMergeOperands(
    const ReadOptions& options,
    ColumnFamilyHandle* column_family,
    const Slice& key,
    PinnableSlice* merge_operands,
    GetMergeOperandsOptions* get_merge_operands_options,
    int* number_of_operands) = 0;
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `merge_operands` | `PinnableSlice*` | Output array (sized by `expected_max_number_of_operands`) |
| `get_merge_operands_options` | `GetMergeOperandsOptions*` | Limits and callbacks |
| `number_of_operands` | `int*` | Output: actual number of operands found |

### GetMergeOperandsOptions

```cpp
// include/rocksdb/db.h:97-118
struct GetMergeOperandsOptions {
  // Hard limit on operand count (returns Incomplete if exceeded)
  int expected_max_number_of_operands = 0;

  // Callback invoked per operand (newest to oldest)
  // Return false to stop fetching early
  std::function<bool(Slice)> continue_cb;
};
```

### Operand Order

⚠️ **INVARIANT**: Merge operands are returned **oldest to newest** (chronological order, in order of insertion).

### Example

```cpp
GetMergeOperandsOptions opts;
opts.expected_max_number_of_operands = 10;
opts.continue_cb = [](Slice operand) {
  // Return false to stop early
  return operand.size() < 1024; // Stop if operand > 1KB
};

PinnableSlice operands[10];
int num_operands = 0;

Status s = db->GetMergeOperands(read_opts, cf, "counter:user123",
                                 operands, &opts, &num_operands);

if (s.ok()) {
  for (int i = 0; i < num_operands; ++i) {
    LOG(INFO) << "Operand " << i << ": " << operands[i].ToString();
  }
} else if (s.IsIncomplete()) {
  LOG(WARN) << "More than 10 operands exist";
}
```

---

## DB::GetEntity / DB::MultiGetEntity - Wide Column Reads

For wide-column entities, `GetEntity()` and `MultiGetEntity()` return all columns without requiring the caller to deserialize from a plain value.

### GetEntity API

```cpp
// include/rocksdb/db.h:667-672
virtual Status GetEntity(const ReadOptions& options,
                         ColumnFamilyHandle* column_family,
                         const Slice& key,
                         PinnableWideColumns* columns);

// include/rocksdb/db.h:678-682 - Multi-CF for single key
virtual Status GetEntity(const ReadOptions& options,
                         const Slice& key,
                         PinnableAttributeGroups* result);
```

### MultiGetEntity API

```cpp
// include/rocksdb/db.h:876-885 - Single CF
virtual void MultiGetEntity(const ReadOptions& options,
                            ColumnFamilyHandle* column_family,
                            size_t num_keys, const Slice* keys,
                            PinnableWideColumns* results,
                            Status* statuses,
                            bool sorted_input = false);

// include/rocksdb/db.h:908-917 - Multi-CF
virtual void MultiGetEntity(const ReadOptions& options,
                            size_t num_keys,
                            ColumnFamilyHandle** column_families,
                            const Slice* keys,
                            PinnableWideColumns* results,
                            Status* statuses,
                            bool sorted_input = false);
```

Both have default implementations returning `Status::NotSupported`.

See `docs/components/wide_column.md` for full wide-column documentation.

---

## DB::KeyMayExist - Lightweight Existence Check

A bloom-filter-based existence check that can avoid I/O. Returns `false` if the key definitely does not exist; returns `true` if it may exist (can be a false positive).

### API Signature

```cpp
// include/rocksdb/db.h:954-958
virtual bool KeyMayExist(const ReadOptions& options,
                         ColumnFamilyHandle* column_family,
                         const Slice& key, std::string* value,
                         std::string* timestamp,
                         bool* value_found = nullptr);
```

**Overloads**:
- `KeyMayExist(options, column_family, key, value, value_found)` - No timestamp
- `KeyMayExist(options, key, value, value_found)` - Default column family
- `KeyMayExist(options, key, value, timestamp, value_found)` - Default CF with timestamp

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `value` | `std::string*` | Output value (populated if found in memory) |
| `value_found` | `bool*` | If non-null, set to true when value was found in memory without I/O |

### Return Values

| Return | Meaning |
|--------|---------|
| `false` | Key definitely does not exist (bloom filter miss) |
| `true` | Key may exist (could be false positive); check `*value_found` for value availability |

### Use Case

Use when you need a fast "likely not found" check before committing to a full `Get()`:

```cpp
std::string value;
bool value_found = false;
if (db->KeyMayExist(read_opts, cf, key, &value, &value_found)) {
  if (value_found) {
    // Value was found in memtable, no I/O needed
    ProcessValue(value);
  } else {
    // Key might exist -- do a full Get() to confirm
    PinnableSlice pval;
    Status s = db->Get(read_opts, cf, key, &pval);
    // ...
  }
} else {
  // Key definitely does not exist
}
```

---

## Async I/O

RocksDB supports asynchronous I/O for reads to reduce latency via I/O parallelism.

### Configuration

```cpp
ReadOptions opts;
opts.async_io = true;  // Enable async reads

// MultiGet-specific: read multiple levels in parallel
opts.optimize_multiget_for_io = true;
```

### Async I/O Path

```
Sequential read detected (Iterator auto-readahead)
    ↓
  async_io=true?
    ↓ yes
Create async read request
    ↓
Submit to FileSystem (io_uring or thread pool)
    ↓
Continue processing (overlap I/O with CPU)
    ↓
Poll/wait for I/O completion
    ↓
Consume data
```

### io_uring Support (Linux)

When available, RocksDB uses io_uring for efficient async I/O:

- **Zero-copy**: Direct I/O to user buffers
- **Batched submission**: Multiple reads in one syscall
- **Polling**: Optional busy-polling for lowest latency

**Requirements**:
- Linux kernel 5.1+ with io_uring support
- RocksDB built with `USE_URING=1`
- FileSystem implementation supports async reads

### Async I/O in MultiGet

With `async_io=true` and `optimize_multiget_for_io=true`, MultiGet issues reads for SST files across all levels simultaneously:

```
Level 0: [SST1, SST2] --async read--> io_uring queue
Level 1: [SST3]       --async read--> io_uring queue
Level 2: [SST4, SST5] --async read--> io_uring queue
                          ↓
                    Parallel I/O
                          ↓
                 Merge results by key
```

**Benefit**: Reduces P99 latency by hiding I/O latency for keys spread across levels.

### Prefetching and Readahead

`ReadOptions::readahead_size` controls prefetch size for iterators:

```cpp
ReadOptions opts;
opts.readahead_size = 256 * 1024; // 256KB

// Iterator will prefetch 256KB chunks when sequential access detected
Iterator* it = db->NewIterator(opts);
```

RocksDB auto-readahead (see `include/rocksdb/options.h:2102-2104`):
- Starts at 8KB after detecting more than two sequential reads
- Doubles on each additional read up to `BlockBasedTableOptions::max_auto_readahead_size` (default 256KB)
- Resets back to 8KB at each level when the iterator moves to a new file
- `readahead_size` overrides auto-readahead with a fixed size

---

## Scan Patterns

### Prefix Scan

Scan all keys with a common prefix. Requires `prefix_extractor` configured.

```cpp
ReadOptions opts;
opts.total_order_seek = false;  // Enable prefix bloom filter
opts.auto_prefix_mode = false;

// Seek to prefix boundary
it->Seek("user:12345:");
while (it->Valid() && it->key().starts_with("user:12345:")) {
  ProcessEntry(it->key(), it->value());
  it->Next();
}
```

**Optimization**: Prefix bloom filters skip SST files without matching prefix.

⚠️ **INVARIANT**: With `prefix_extractor`, iterator behavior is undefined if you seek outside the prefix or cross prefix boundaries during iteration (unless `total_order_seek=true` or `auto_prefix_mode=true`).

### Range Scan with Bounds

```cpp
ReadOptions opts;
Slice lower_bound = "user:1000";
Slice upper_bound = "user:2000";
opts.iterate_lower_bound = &lower_bound;
opts.iterate_upper_bound = &upper_bound;

Iterator* it = db->NewIterator(opts);
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  // Automatically stops at upper_bound
  ProcessEntry(it->key(), it->value());
}
delete it;
```

**Benefits**:
- Upper bound enables SST file filtering via bloom/index
- `SeekToLast()` positions at first key `< upper_bound`

### Reverse Scan

```cpp
Iterator* it = db->NewIterator(read_opts);
for (it->SeekToLast(); it->Valid(); it->Prev()) {
  ProcessEntry(it->key(), it->value());
}
delete it;
```

Or seek to end of range and iterate backward:

```cpp
Slice upper_bound = "user:2000";
it->SeekForPrev(upper_bound);
while (it->Valid() && it->key() >= lower_bound) {
  ProcessEntry(it->key(), it->value());
  it->Prev();
}
```

### Tailing Iterator

A tailing iterator sees writes that occur after iterator creation (experimental):

```cpp
ReadOptions opts;
opts.tailing = true;  // See new data as it's written

Iterator* it = db->NewIterator(opts);
// Iterator will reflect records inserted after iterator creation
```

⚠️ **CAUTION**: Tailing iterators have performance overhead and limited snapshot consistency guarantees. They provide a view of the complete database and can read newly added data, making them suitable for sequential reads of a growing dataset.

---

## ReadOptions Reference

Complete reference for `ReadOptions` (defined in `include/rocksdb/options.h:1994`).

### Snapshot and Consistency

```cpp
const Snapshot* snapshot = nullptr;
```
- `nullptr`: Implicit snapshot at read start (default)
- Non-null: Read as of specific snapshot

```cpp
const Snapshot* snap = db->GetSnapshot();
ReadOptions opts;
opts.snapshot = snap;
// All reads see DB state at snapshot time
db->ReleaseSnapshot(snap);
```

### User-Defined Timestamps (Experimental)

```cpp
const Slice* timestamp = nullptr;      // Upper bound (inclusive)
const Slice* iter_start_ts = nullptr;  // Lower bound (for iterators)
```

Returns data visible up to `timestamp`. For iterators, returns all versions in `[iter_start_ts, timestamp]`.

### Deadlines and Timeouts

```cpp
std::chrono::microseconds deadline = std::chrono::microseconds::zero();
std::chrono::microseconds io_timeout = std::chrono::microseconds::zero();
```

- `deadline`: Absolute deadline for entire operation (best-effort)
- `io_timeout`: Per-file-read timeout

```cpp
opts.deadline = std::chrono::microseconds(env->NowMicros() + 100'000); // 100ms total deadline
opts.io_timeout = std::chrono::milliseconds(50); // 50ms per I/O
```

Returns `Status::Incomplete` on timeout.

### Tiering and Caching

```cpp
ReadTier read_tier = kReadAllTier;
```

- `kReadAllTier`: Read from all storage tiers (default)
- `kBlockCacheTier`: Only read data already in memtable or block cache (return `Incomplete` otherwise)
- `kPersistedTier`: Read persisted data only; skips memtable when WAL is disabled (Get/MultiGet only, not iterators)
- `kMemtableTier`: Read from memtable only; used for memtable-only iterators

```cpp
bool fill_cache = true;
```

If `false`, read blocks are not added to block cache (useful for bulk scans).

### Checksums and Integrity

```cpp
bool verify_checksums = true;
```

Verify block checksums during read (slight CPU overhead, detects corruption).

### Async I/O

```cpp
bool async_io = false;
bool optimize_multiget_for_io = true; // MultiGet-specific
```

See [Async I/O](#async-io) section.

### Iterator-Specific Options

```cpp
size_t readahead_size = 0;
const Slice* iterate_lower_bound = nullptr;
const Slice* iterate_upper_bound = nullptr;
bool tailing = false;
bool total_order_seek = false;
bool auto_prefix_mode = false;
bool prefix_same_as_start = false;
bool pin_data = false;
```

See [Iterator Operations](#iterator-operations) and [Scan Patterns](#scan-patterns).

### Rate Limiting

```cpp
Env::IOPriority rate_limiter_priority = Env::IO_TOTAL;
```

Charge read I/O to rate limiter at specified priority (`IO_LOW`, `IO_HIGH`, etc.). `IO_TOTAL` disables rate limiting.

### Limits

```cpp
uint64_t value_size_soft_limit = std::numeric_limits<uint64_t>::max();
std::optional<size_t> merge_operand_count_threshold;
uint64_t max_skippable_internal_keys = 0;
```

- `value_size_soft_limit`: MultiGet cumulative value cap
- `merge_operand_count_threshold`: Returns `kMergeOperandThresholdExceeded` subcode if exceeded
- `max_skippable_internal_keys`: Fail seek as `Incomplete` if too many internal keys skipped (0 = unlimited)

---

## PinnableSlice - Zero-Copy Values

`PinnableSlice` (defined in `include/rocksdb/slice.h:179`) avoids copying values by pinning the underlying storage.

### Usage

```cpp
PinnableSlice value;
Status s = db->Get(read_opts, key, &value);

if (s.ok()) {
  // Access value without copy
  DoSomething(value.data(), value.size());

  // PinnableSlice holds reference to block cache entry or internal buffer
}
// Cleanup happens when value goes out of scope
```

### Pinning Mechanics

```cpp
inline bool IsPinned() const { return pinned_; }
```

- **Pinned (`true`)**: Value data points to block cache entry (held via `Cleanable` cleanup functions)
- **Not pinned (`false`)**: Value copied to internal `self_space_` buffer

⚠️ **INVARIANT**: A pinned `PinnableSlice` keeps the block cache entry referenced until `Reset()` or destruction.

### Lifetime

```cpp
PinnableSlice value;
db->Get(read_opts, key, &value);

// value.data() remains valid until:
// 1. value.Reset() is called, OR
// 2. value goes out of scope (destructor), OR
// 3. value is reused in another Get/MultiGet call
```

### Move Semantics

```cpp
PinnableSlice value1;
db->Get(opts, key1, &value1);

PinnableSlice value2 = std::move(value1);
// value1 is now empty, value2 owns the pinned data
```

⚠️ **INVARIANT**: Copy constructor/assignment are **deleted**. Use move semantics only.

---

## Column Family Reads

All read APIs accept an optional `ColumnFamilyHandle*` parameter.

### Default Column Family

```cpp
db->Get(opts, key, &value); // Uses DefaultColumnFamily()
```

Equivalent to:

```cpp
db->Get(opts, db->DefaultColumnFamily(), key, &value);
```

### Reading from Specific Column Families

```cpp
ColumnFamilyHandle* cf = /* from DB::Open() */;

PinnableSlice value;
db->Get(opts, cf, key, &value);

Iterator* it = db->NewIterator(opts, cf);
```

### MultiGet Across Column Families

```cpp
const size_t num_keys = 3;
ColumnFamilyHandle* cfs[3] = {cf1, cf2, cf1};
Slice keys[3] = {"key1", "key2", "key3"};
PinnableSlice values[3];
Status statuses[3];

db->MultiGet(opts, num_keys, cfs, keys, values, statuses);

// statuses[0]: result for key1 in cf1
// statuses[1]: result for key2 in cf2
// statuses[2]: result for key3 in cf1
```

⚠️ **INVARIANT**: All reads within a single Get/MultiGet/Iterator use the same snapshot, ensuring consistency across column families.

### Consistent Multi-CF Iteration

```cpp
// include/rocksdb/db.h:998-1001
virtual Status NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) = 0;
```

Creates iterators across multiple column families with a consistent snapshot:

```cpp
std::vector<ColumnFamilyHandle*> cfs = {cf1, cf2, cf3};
std::vector<Iterator*> iters;

db->NewIterators(opts, cfs, &iters);

// All iterators see consistent snapshot across CFs
for (auto* it : iters) {
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    // Process...
  }
  delete it;
}
```

### Cross-CF Iterators

**NewCoalescingIterator** merges wide columns from multiple CFs. When a key exists in multiple CFs, columns are coalesced with later CFs shadowing earlier ones:

```cpp
// include/rocksdb/db.h:1016-1018
virtual std::unique_ptr<Iterator> NewCoalescingIterator(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families) = 0;
```

**NewAttributeGroupIterator** returns per-CF wide columns separately (as `IteratorAttributeGroups`):

```cpp
// include/rocksdb/db.h:1022-1024
virtual std::unique_ptr<AttributeGroupIterator> NewAttributeGroupIterator(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families) = 0;
```

### NewMultiScan - Multi-Range Scan

Scans multiple disjoint key ranges in a single pass. Ranges should be in increasing order of start key:

```cpp
// include/rocksdb/db.h:1055-1057
virtual std::unique_ptr<MultiScan> NewMultiScan(
    const ReadOptions& options, ColumnFamilyHandle* column_family,
    const MultiScanArgs& scan_opts);
```

See `include/rocksdb/multi_scan_iterator.h` for `MultiScanArgs` and usage details.

---

## Best Practices

### Point Lookups

1. **Use PinnableSlice for Get()**: Avoids memcpy for values in block cache
2. **Set `verify_checksums=true`**: Detect corruption at minimal CPU cost
3. **Avoid repeated snapshots**: Reuse snapshot for consistent multi-key reads

### MultiGet

1. **Batch keys**: Use MultiGet instead of multiple Get calls (10-100x faster for many keys)
2. **Enable async I/O**: `async_io=true` + `optimize_multiget_for_io=true` for P99 latency
3. **Sort keys**: Set `sorted_input=true` if keys are already sorted
4. **Limit value size**: Set `value_size_soft_limit` to prevent unbounded memory growth

### Iterators

1. **Set bounds**: Always specify `iterate_upper_bound` for range scans (enables bloom filtering)
2. **Disable cache population**: `fill_cache=false` for large scans
3. **Readahead for sequential scans**: `readahead_size=256KB` for spinning disks
4. **Check status**: Always check `it->status()` after iteration loop
5. **Lazy value loading**: Use `allow_unprepared_value=true` with BlobDB for large values

### General

1. **Snapshot management**: Release snapshots promptly to allow compaction
2. **Deadline for latency control**: Set `deadline` for user-facing queries
3. **Prefix scans**: Configure `prefix_extractor` for workload-specific optimizations
4. **Monitor metrics**: Track `rocksdb.block.cache.miss`, `rocksdb.bloom.filter.useful` via Statistics

---

## Invariants Summary

⚠️ **All reads see a consistent snapshot** (explicit or implicit).

⚠️ **Get() returns immediately on finding key** (no further level scanning).

⚠️ **MultiGet uses single snapshot for all keys** (cross-key consistency).

⚠️ **Caller owns Iterator*** (must delete before DB close).

⚠️ **Seek operations clear previous error status**.

⚠️ **iterate_upper_bound is exclusive** (iterator never returns key >= bound).

⚠️ **With prefix_extractor, stay within prefix** (unless total_order_seek or auto_prefix_mode).

⚠️ **PinnableSlice copy is deleted** (use move semantics).

⚠️ **Pinned PinnableSlice holds block cache reference** (until Reset/destruction).

⚠️ **GetMergeOperands returns oldest-to-newest operands** (chronological order).

⚠️ **MultiGet array sizes must match num_keys** (keys, values, statuses).

---

## Related Documentation

- `ARCHITECTURE.md` - High-level RocksDB architecture and data flow
- `docs/components/write_flow.md` - Write path (Put, Delete, Merge)
- `docs/components/memtable.md` - Memtable internals (read path first stop)
- `docs/components/sst_table_format.md` - SST file format and block-based table
- `docs/components/compaction.md` - Compaction and LSM structure
- `docs/components/cache.md` - Block cache (critical for read performance)
- `docs/components/wide_column.md` - Wide-column entity support
- `docs/components/user_defined_timestamp.md` - User-defined timestamp feature

