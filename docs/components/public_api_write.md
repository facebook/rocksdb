# Public Write APIs

This document covers RocksDB's public write APIs, providing detailed information about write operations, batching, options, and flow control mechanisms.

**Key Source Files:**
- `include/rocksdb/db.h` - All write API declarations
- `include/rocksdb/options.h` - WriteOptions definition
- `include/rocksdb/write_batch.h` - WriteBatch API
- `db/write_batch.cc` - WriteBatch implementation
- `db/db_impl/db_impl_write.cc` - Write path implementation
- `include/rocksdb/merge_operator.h` - Merge operator interface
- `db/external_sst_file_ingestion_job.cc` - External SST ingestion

---

## 1. DB::Put - Single Key-Value Writes

**API Signature:**
```cpp
// include/rocksdb/db.h:429-442
Status Put(const WriteOptions& options,
           ColumnFamilyHandle* column_family,
           const Slice& key,
           const Slice& value) = 0;

Status Put(const WriteOptions& options,
           ColumnFamilyHandle* column_family,
           const Slice& key,
           const Slice& ts,
           const Slice& value) = 0;
```

**Behavior:**
- Sets the database entry for `key` to `value`
- If `key` already exists, it will be **overwritten**
- Returns `OK` on success, non-OK status on error

**⚠️ INVARIANT:** If user-defined timestamp is enabled, `Put` with timestamp must be used; otherwise timestamp-less version must be used. Mixing these will return `Status::InvalidArgument`.

**User-Defined Timestamp Support:**
```cpp
// db/db_impl/db_impl_write.cc:23-39
Status DBImpl::Put(const WriteOptions& o, ColumnFamilyHandle* column_family,
                   const Slice& key, const Slice& val) {
  const Status s = FailIfCfHasTs(column_family);
  if (!s.ok()) {
    return s;
  }
  return DB::Put(o, column_family, key, val);
}
```

The implementation validates timestamp requirements before delegating to the base `DB::Put` which internally creates a single-entry WriteBatch.

**Default Column Family Convenience:**
```cpp
Status Put(const WriteOptions& options, const Slice& key, const Slice& value) {
  return Put(options, DefaultColumnFamily(), key, value);
}
```

---

## 2. DB::Delete and DB::SingleDelete

### 2.1 DB::Delete - Standard Deletion

**API Signature:**
```cpp
// include/rocksdb/db.h:461-473
Status Delete(const WriteOptions& options,
              ColumnFamilyHandle* column_family,
              const Slice& key) = 0;
```

**Behavior:**
- Removes the database entry for `key` if it exists
- Returns `OK` on success
- **Not an error** if `key` did not exist

**⚠️ INVARIANT:** Delete is idempotent - deleting a non-existent key succeeds.

### 2.2 DB::SingleDelete - Optimized Deletion

**API Signature:**
```cpp
// include/rocksdb/db.h:491-503
Status SingleDelete(const WriteOptions& options,
                    ColumnFamilyHandle* column_family,
                    const Slice& key) = 0;
```

**Behavior:**
- Optimized deletion for keys that were written **exactly once** with Put
- Undefined behavior if key was overwritten (Put called multiple times) or merged

**⚠️ INVARIANT:** SingleDelete only behaves correctly if there has been **exactly one Put** for this key since the previous SingleDelete. Violating this contract leads to undefined behavior including data inconsistency.

**Restrictions (include/rocksdb/db.h:485-488):**
- Must not mix SingleDelete with Delete for the same key
- Must not mix SingleDelete with Merge for the same key
- Experimental performance optimization for very specific workloads

**Comparison:**

| Feature | Delete | SingleDelete |
|---------|--------|--------------|
| Use case | General-purpose deletion | Single-write keys only |
| Overwrites allowed | Yes | No (undefined behavior) |
| Mix with Merge | Yes | No |
| Performance | Standard | Optimized for specific pattern |
| Safety | Always safe | Requires strict contract |

---

## 3. DB::DeleteRange - Range Deletion

**API Signature:**
```cpp
// include/rocksdb/db.h:521-536
Status DeleteRange(const WriteOptions& options,
                   ColumnFamilyHandle* column_family,
                   const Slice& begin_key,
                   const Slice& end_key);
```

**Behavior:**
- Removes database entries in range `[begin_key, end_key)` - **inclusive** of begin_key, **exclusive** of end_key
- Returns `Status::InvalidArgument` if `end_key` comes before `begin_key` according to comparator
- Not an error if range contains no existing data

**⚠️ INVARIANT:** Range is half-open: `[begin_key, end_key)`. The end_key itself is NOT deleted.

**Production Caveats (include/rocksdb/db.h:513-520):**

1. **Read Performance Degradation:** Accumulating too many range tombstones in memtable degrades read performance. Mitigate by manually flushing occasionally.

2. **max_open_files Impact:** Limiting max_open_files in presence of range tombstones degrades read performance. Set `max_open_files = -1` when possible.

3. **Incompatibility:** Returns `Status::NotSupported()` if `row_cache` is configured.

**Implementation Detail:**
Range deletions are stored as "range tombstones" using `WriteBatchInternal::DeleteRange`. During compaction, range tombstones are used by `RangeDelAggregator` to efficiently drop keys.

---

## 4. DB::Merge - Merge Operator Writes

**API Signature:**
```cpp
// include/rocksdb/db.h:542-552
Status Merge(const WriteOptions& options,
             ColumnFamilyHandle* column_family,
             const Slice& key,
             const Slice& value) = 0;
```

**Behavior:**
- Merges `value` with existing value for `key` using user-provided `merge_operator`
- Deferred computation - merge not executed until read or compaction
- Returns `Status::NotSupported` if no merge_operator configured

**⚠️ INVARIANT:** Column family MUST have a merge_operator configured. Otherwise all Merge calls return `Status::NotSupported`.

**Validation (db/db_impl/db_impl_write.cc:63-75):**
```cpp
Status DBImpl::Merge(const WriteOptions& o, ColumnFamilyHandle* column_family,
                     const Slice& key, const Slice& val) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  if (!cfh->cfd()->ioptions().merge_operator) {
    return Status::NotSupported("Provide a merge_operator when opening DB");
  }
  return DB::Merge(o, column_family, key, val);
}
```

**Merge Operator Interface:**
```cpp
// include/rocksdb/merge_operator.h:161-162
virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                         MergeOperationOutput* merge_out) const;
```

**Example Use Cases:**
- Counters: increment operations without read-modify-write
- Lists: append operations
- JSON documents: field updates
- Complex data structures with incremental updates

**Deferred Execution:**
```
User calls:    Merge(k, +1)  Merge(k, +5)  Merge(k, +3)
Storage:       [+1]          [+1][+5]      [+1][+5][+3]
Get(k):        Executes: base_value + 1 + 5 + 3
Compaction:    Collapses to single value
```

---

## 5. WriteBatch - Atomic Batching

**API:**
```cpp
// include/rocksdb/write_batch.h:64-74
class WriteBatch : public WriteBatchBase {
 public:
  explicit WriteBatch(size_t reserved_bytes = 0, size_t max_bytes = 0);

  Status Put(ColumnFamilyHandle* column_family, const Slice& key, const Slice& value);
  Status Delete(ColumnFamilyHandle* column_family, const Slice& key);
  Status SingleDelete(ColumnFamilyHandle* column_family, const Slice& key);
  Status DeleteRange(ColumnFamilyHandle* column_family,
                     const Slice& begin_key, const Slice& end_key);
  Status Merge(ColumnFamilyHandle* column_family, const Slice& key, const Slice& value);
  Status PutEntity(ColumnFamilyHandle* column_family, const Slice& key,
                   const WideColumns& columns);
};
```

**Atomicity Guarantee:**
Updates are applied in the order added to WriteBatch. All updates succeed atomically or all fail.

**Example (include/rocksdb/write_batch.h:12-18):**
```cpp
batch.Put("key", "v1");
batch.Delete("key");
batch.Put("key", "v2");
batch.Put("key", "v3");
// After Write(): key -> "v3"
```

**⚠️ INVARIANT:** Operations in WriteBatch are applied in insertion order. Final value is determined by last operation on each key.

### 5.1 WriteBatch Internal Format

**Binary Format (db/write_batch.cc:10-37):**
```
WriteBatch::rep_ :=
   sequence: fixed64
   count: fixed32
   data: record[count]

record :=
   kTypeValue varstring varstring                     // Put
   kTypeDeletion varstring                            // Delete
   kTypeSingleDeletion varstring                      // SingleDelete
   kTypeRangeDeletion varstring varstring             // DeleteRange
   kTypeMerge varstring varstring                     // Merge
   kTypeColumnFamilyValue varint32 varstring varstring
   kTypeColumnFamilyDeletion varint32 varstring
   kTypeWideColumnEntity varstring varstring          // PutEntity
   ...
```

**⚠️ INVARIANT:** WriteBatch header is 12 bytes (8-byte sequence + 4-byte count). This is defined as `WriteBatchInternal::kHeader`.

### 5.2 Save Points

**API:**
```cpp
// include/rocksdb/write_batch.h:218-231
void SetSavePoint();
Status RollbackToSavePoint();
Status PopSavePoint();
```

**Behavior:**
- `SetSavePoint()`: Record current batch state
- `RollbackToSavePoint()`: Remove all entries since last save point
- `PopSavePoint()`: Remove save point without rollback

**Use Case:** Transaction-like partial rollback within a batch.

### 5.3 Thread Safety

**⚠️ INVARIANT (include/rocksdb/write_batch.h:20-23):** Multiple threads can invoke const methods without synchronization. Non-const methods require external synchronization.

---

## 6. DB::Write - Explicit Batch Submission

**API Signature:**
```cpp
// include/rocksdb/db.h:559
Status Write(const WriteOptions& options, WriteBatch* updates) = 0;
```

**Behavior:**
- Applies updates in `WriteBatch` atomically
- Even empty WriteBatch will sync WAL if `options.sync = true`
- Returns OK on success

**Implementation Entry (db/db_impl/db_impl_write.cc:151-163):**
```cpp
Status DBImpl::Write(const WriteOptions& write_options, WriteBatch* my_batch) {
  Status s;
  if (write_options.protection_bytes_per_key > 0) {
    s = WriteBatchInternal::UpdateProtectionInfo(
        my_batch, write_options.protection_bytes_per_key);
  }
  if (s.ok()) {
    s = WriteImpl(write_options, my_batch, /*callback=*/nullptr,
                  /*user_write_cb=*/nullptr, /*wal_used=*/nullptr);
  }
  return s;
}
```

**Write Flow:**
```
DB::Write()
    ↓
DBImpl::Write()
    ↓
DBImpl::WriteImpl()
    ↓
┌─────────────────────┐
│ WriteThread Batching│ (Group commit)
└─────────────────────┘
    ↓
┌─────────────────────┐
│ WAL Write           │
└─────────────────────┘
    ↓
┌─────────────────────┐
│ Memtable Insert     │
└─────────────────────┘
    ↓
Return Status
```

**⚠️ INVARIANT:** Protection bytes per key (if enabled) must be 0 or 8. Other values return `Status::InvalidArgument`.

---

## 7. PutEntity - Wide Column Writes

**API Signature:**
```cpp
// include/rocksdb/db.h:449-455
Status PutEntity(const WriteOptions& options,
                 ColumnFamilyHandle* column_family,
                 const Slice& key,
                 const WideColumns& columns);

Status PutEntity(const WriteOptions& options,
                 const Slice& key,
                 const AttributeGroups& attribute_groups);
```

**Behavior:**
- Stores wide-column entity: key → {column1:value1, column2:value2, ...}
- Overwrites existing entry if key exists
- Second variant splits entity across multiple column families (AttributeGroups)

**Wide Column Format:**
```cpp
// include/rocksdb/wide_columns.h
using WideColumns = std::vector<std::pair<std::string, std::string>>;
// Each pair is (column_name, column_value)
```

**Default Column:**
Wide-column entities have a special default anonymous column `kDefaultWideColumnName`. When reading with `Get()`, returns this column's value.

**Example:**
```cpp
WideColumns columns = {
  {"", "default_value"},      // kDefaultWideColumnName
  {"field1", "value1"},
  {"field2", "value2"}
};
db->PutEntity(write_opts, key, columns);

// Later:
std::string val;
db->Get(read_opts, key, &val);  // Returns "default_value"
```

**⚠️ INVARIANT:** Wide-column entities must not be used with column families that enable user-defined timestamp (not yet supported).

---

## 8. WriteOptions - Write Configuration

**Structure (include/rocksdb/options.h:2319-2407):**

```cpp
struct WriteOptions {
  bool sync = false;
  bool disableWAL = false;
  bool ignore_missing_column_families = false;
  bool no_slowdown = false;
  bool low_pri = false;
  bool memtable_insert_hint_per_batch = false;
  Env::IOPriority rate_limiter_priority = Env::IO_TOTAL;
  size_t protection_bytes_per_key = 0;
};
```

### 8.1 sync

**Default:** `false`

**Behavior:**
- `true`: Call `fsync()` before returning (durability guarantee)
- `false`: Write may be in OS buffer cache (faster but less durable)

**Crash Semantics:**
- `sync=false`: Similar to `write()` system call (may lose recent writes on machine crash)
- `sync=true`: Similar to `write()` + `fdatasync()` (survives machine crash)

**⚠️ INVARIANT:** `sync=true` and `disableWAL=true` together return `Status::InvalidArgument` - cannot sync without WAL.

### 8.2 disableWAL

**Default:** `false`

**Behavior:**
- `true`: Skip write-ahead log (faster but loses durability)
- `false`: Write to WAL before memtable

**⚠️ INVARIANT:** With `disableWAL=true`, writes may be lost after crash. Backup engine requires `disableWAL=false` OR `flush_before_backup=true`.

**Validation (db/db_impl/db_impl_write.cc:436-438):**
```cpp
if (write_options.sync && write_options.disableWAL) {
  return Status::InvalidArgument("Sync writes has to enable WAL.");
}
```

### 8.3 no_slowdown

**Default:** `false`

**Behavior:**
- `true`: Fail immediately with `Status::Incomplete()` if write would block
- `false`: Wait/sleep as needed to apply write

**Use Case:** Latency-sensitive applications that prefer failure over blocking.

### 8.4 low_pri

**Default:** `false`

**Behavior:**
- `true`: Treat as low-priority write; may be slowed down or canceled if compaction behind
- If `low_pri=true` AND `no_slowdown=true`: Canceled immediately with `Status::Incomplete()`
- If `low_pri=true` AND `no_slowdown=false`: Slowed down to minimize impact on high-priority writes

**Implementation (db/db_impl/db_impl_write.cc:495-500):**
```cpp
if (write_options.low_pri) {
  Status s = ThrottleLowPriWritesIfNeeded(write_options, my_batch);
  if (!s.ok()) {
    return s;
  }
}
```

### 8.5 memtable_insert_hint_per_batch

**Default:** `false`

**Behavior:**
- `true`: Maintain last insert position per memtable as hint for concurrent writes
- Improves performance if keys in WriteBatch are **sequential**
- Ignored if `concurrent_memtable_writes=false`

**⚠️ INVARIANT:** Only effective with `concurrent_memtable_writes=true`.

### 8.6 rate_limiter_priority

**Default:** `Env::IO_TOTAL` (disabled)

**Behavior:**
- `Env::IO_USER`: Charge internal rate limiter at user priority
- `Env::IO_TOTAL`: Disable rate limiter charging

**Restrictions (db/db_impl/db_impl_write.cc:398-409):**
- Only `IO_USER` and `IO_TOTAL` allowed
- Only works with automatic WAL flush (`disableWAL=false` AND `manual_wal_flush=false`)

### 8.7 protection_bytes_per_key

**Default:** `0` (disabled)

**Behavior:**
- `0`: No protection
- `8`: 8 bytes of protection info per key (checksums)

**⚠️ INVARIANT:** Only values 0 and 8 are supported. Other values return `Status::InvalidArgument`.

**Purpose:** Detect in-memory corruption during write path.

---

## 9. IngestExternalFile - Bulk Loading

**API Signature:**
```cpp
// include/rocksdb/db.h:2015-2023
Status IngestExternalFile(
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& external_files,
    const IngestExternalFileOptions& options) = 0;

Status IngestExternalFiles(
    const std::vector<IngestExternalFileArg>& args) = 0;
```

**Purpose:**
Efficiently load externally-generated SST files into the database without going through memtable.

**Steps (include/rocksdb/db.h:1972-2014):**

1. **Preparation:** Validate files, check overlaps, copy/move/link files into DB directory
2. **Assignment:** Assign global sequence numbers to keys in files
3. **Insertion:** Add files to appropriate LSM level without compaction

**Options (include/rocksdb/options.h:2623+):**

```cpp
struct IngestExternalFileOptions {
  // Move files instead of copying
  bool move_files = false;

  // Allow global seqno assignment
  bool allow_global_seqno = true;

  // Allow blocking flush if memtable overlaps
  bool allow_blocking_flush = true;

  // Ingest to bottommost level (oldest data)
  bool ingest_behind = false;

  // Verify file checksums
  bool verify_checksums_before_ingest = false;

  // Write global seqno to file footer
  bool write_global_seqno = true;
};
```

**⚠️ INVARIANT:** Files with overlapping ranges cannot be ingested with `ingest_behind=true`. Returns `Status::NotSupported`.

**⚠️ INVARIANT:** If `allow_global_seqno=false` and files overlap, ingestion fails because distinct sequence numbers are required.

**File Validation (db/external_sst_file_ingestion_job.cc:62-70):**
```cpp
if (file_to_ingest.num_entries == 0 &&
    file_to_ingest.num_range_deletions == 0) {
  return Status::InvalidArgument("File contain no entries");
}

if (!file_to_ingest.smallest_internal_key.Valid() ||
    !file_to_ingest.largest_internal_key.Valid()) {
  return Status::Corruption("Generated table have corrupted keys");
}
```

**Performance:**
- **Much faster** than writing through Put/Write (bypasses memtable, WAL)
- Ideal for bulk imports, database migration, data lake ingestion

**Restrictions:**
- Incompatible with `unordered_write`
- Incompatible with `enable_pipelined_write`

---

## 10. Column Family Writes

All write APIs support targeting specific column families:

```cpp
ColumnFamilyHandle* cf = ...;

db->Put(write_opts, cf, key, value);
db->Delete(write_opts, cf, key);
db->Merge(write_opts, cf, key, value);

WriteBatch batch;
batch.Put(cf, key1, value1);
batch.Put(cf, key2, value2);
db->Write(write_opts, &batch);
```

**Default Column Family:**
If no `ColumnFamilyHandle` specified, APIs use `DefaultColumnFamily()`.

**⚠️ INVARIANT (include/rocksdb/options.h:2346-2350):** If `ignore_missing_column_families=true` and writing to dropped column families, the write is **silently ignored** (no error). Other writes in batch still succeed.

**Cross-CF Atomicity:**
WriteBatch can contain writes to multiple column families. All writes succeed or fail atomically:

```cpp
WriteBatch batch;
batch.Put(cf1, "key", "value1");
batch.Put(cf2, "key", "value2");
batch.Delete(cf3, "key");
db->Write(write_opts, &batch);  // All CFs updated atomically
```

**⚠️ INVARIANT:** If `atomic_flush=false`, flushes are per-CF. If `atomic_flush=true`, all CFs flush together atomically.

---

## 11. Write Flow Control

RocksDB implements write flow control to prevent memtable/L0 explosion. Writes may stall or slow down under certain conditions.

### 11.1 Stall Conditions

**Hard Stops (writes blocked until resolved):**

1. **Memtable Full:** All memtables full and flush can't keep up
2. **L0 File Count:** L0 file count exceeds `level0_stop_writes_trigger`
3. **Pending Compaction Bytes:** Pending compaction bytes exceed hard limit

**Soft Delays (writes artificially slowed):**

1. **L0 Slowdown:** L0 file count exceeds `level0_slowdown_writes_trigger`
2. **Compaction Pending:** Pending compaction bytes exceed soft limit

**Options Controlling Stalls:**

```cpp
// ColumnFamilyOptions
int level0_slowdown_writes_trigger = 20;   // Slow down writes
int level0_stop_writes_trigger = 36;        // Stop writes
uint64_t soft_pending_compaction_bytes_limit = 64GB;  // Slow down
uint64_t hard_pending_compaction_bytes_limit = 256GB; // Stop
```

### 11.2 Write Controller

**Component:** `WriteController` tracks stall/delay state.

**Delay Calculation:**
```cpp
// WriteController calculates delay based on:
// 1. How far over threshold we are
// 2. Write rate
// 3. Configured delay parameters
```

**User Detection:**

Application can detect write delays/stops:

```cpp
WriteOptions opts;
opts.no_slowdown = true;  // Fail instead of waiting

Status s = db->Put(opts, key, value);
if (s.IsIncomplete()) {
  // Write would have been delayed/stopped
  // Handle by: retry later, drop write, backpressure upstream, etc.
}
```

**Monitoring:**

```cpp
// DB statistics track stalls
db->GetProperty("rocksdb.is-write-stopped", &value);
db->GetProperty("rocksdb.actual-delayed-write-rate", &value);

// Or via Statistics
options.statistics->getTickerCount(STALL_MICROS);
options.statistics->getHistogramData(WRITE_STALL, &data);
```

### 11.3 Write Stall Reasons

**Query Current State:**
```cpp
uint64_t cf_stats_value;
db->GetIntProperty(cf_handle,
                   "rocksdb.num-running-flushes",
                   &cf_stats_value);

std::string stall_reason;
db->GetProperty("rocksdb.is-write-stopped", &stall_reason);
```

**Possible Reasons:**
- `level0_slowdown`: L0 file count too high
- `level0_stop`: L0 file count critical
- `pending_compaction_bytes`: Too much compaction backlog
- `memtable_compaction`: Memtable count too high

### 11.4 Avoiding Stalls

**Best Practices:**

1. **Tune L0 Settings:**
   ```cpp
   cf_options.level0_slowdown_writes_trigger = 20;
   cf_options.level0_stop_writes_trigger = 36;
   ```

2. **Increase Write Buffer Size:**
   ```cpp
   cf_options.write_buffer_size = 128 << 20;  // 128MB
   cf_options.max_write_buffer_number = 6;
   ```

3. **More Compaction Threads:**
   ```cpp
   db_options.max_background_compactions = 8;
   db_options.max_background_flushes = 4;
   ```

4. **Rate Limit Writes at Application Level:** Better to backpressure upstream than stall in RocksDB

5. **Monitor and Alert:** Set up monitoring for write stalls before they become critical

**⚠️ INVARIANT:** Write stalls are **necessary for correctness** - they prevent unbounded memory growth. Don't disable them; tune configuration instead.

---

## Write API Comparison

| API | Atomicity | Durability (default) | Performance | Use Case |
|-----|-----------|---------------------|-------------|----------|
| Put | Single key | WAL | Baseline | Single key update |
| Delete | Single key | WAL | Baseline | Remove key |
| SingleDelete | Single key | WAL | Optimized | Delete single-Put key |
| DeleteRange | Range (logical) | WAL | Fast | Bulk delete range |
| Merge | Single key | WAL | Deferred | Incremental updates |
| Write(WriteBatch) | All keys in batch | WAL | Group commit | Atomic multi-key |
| PutEntity | Single entity | WAL | Baseline | Wide-column data |
| IngestExternalFile | All files | No WAL | Very fast | Bulk import |

---

## Performance Considerations

### Hot Path Optimizations

**WriteBatch is Critical Path:**
All write APIs convert to WriteBatch internally. Optimizations:

1. **Pre-allocate:** `WriteBatch batch(reserved_bytes)`
2. **Reuse:** Clear and reuse batch instead of creating new ones
3. **Sequential Keys:** Enable `memtable_insert_hint_per_batch=true`

**Group Commit:**
Multiple concurrent writes are batched together automatically. This amortizes WAL sync cost:

```
Thread 1: Write(batch1)  ─┐
Thread 2: Write(batch2)  ─┼─> Single WAL sync
Thread 3: Write(batch3)  ─┘
```

### Write Amplification

**Minimizing WA:**
- Use `IngestExternalFile` for bulk loads (bypasses WAL+memtable)
- Use `DeleteRange` instead of many individual Deletes
- Use Merge instead of Get-modify-Put
- Tune compaction to reduce file rewrites

---

## Error Handling

**Common Error Status:**

| Status | Meaning | Recovery |
|--------|---------|----------|
| `OK` | Success | N/A |
| `NotSupported` | Feature not available (e.g., Merge without operator) | Configure properly and retry |
| `InvalidArgument` | Bad parameters (e.g., sync+disableWAL) | Fix parameters |
| `Incomplete` | Would block with no_slowdown=true | Retry later or backpressure |
| `IOError` | I/O failure (disk full, etc.) | Check disk, retry with backoff |
| `Corruption` | Data corruption detected | Investigate and potentially restore from backup |
| `Aborted` | Write aborted (e.g., by callback) | Application-specific |

**Example Error Handling:**
```cpp
Status s = db->Write(write_opts, &batch);
if (!s.ok()) {
  if (s.IsIncomplete()) {
    // Write stalled, retry with backoff
    sleep(delay);
    return RetryWrite(batch);
  } else if (s.IsIOError()) {
    // Disk issue, check disk space
    LogError("Write failed: " + s.ToString());
    AlertOps();
  } else {
    // Other error
    LogError("Unexpected write error: " + s.ToString());
  }
}
```

---

## Testing Write APIs

**Unit Test Patterns:**

1. **Verify Overwrite:**
   ```cpp
   db->Put(opts, "key", "v1");
   db->Put(opts, "key", "v2");
   std::string val;
   db->Get(read_opts, "key", &val);
   ASSERT_EQ("v2", val);
   ```

2. **Verify Batch Atomicity:**
   ```cpp
   WriteBatch batch;
   batch.Put("k1", "v1");
   batch.Put("k2", "v2");
   db->Write(opts, &batch);
   // Either both present or both absent
   ```

3. **Verify Delete Idempotence:**
   ```cpp
   db->Delete(opts, "nonexistent");
   ASSERT_TRUE(status.ok());
   ```

4. **Verify Merge Semantics:**
   ```cpp
   db->Merge(opts, "counter", "1");
   db->Merge(opts, "counter", "5");
   std::string val;
   db->Get(read_opts, "counter", &val);
   // Value depends on merge operator
   ```

---

## Summary

RocksDB provides a rich set of write APIs optimized for different use cases:

- **Put/Delete**: Standard key-value operations
- **SingleDelete**: Optimized deletion for single-Put keys
- **DeleteRange**: Efficient range deletion
- **Merge**: Deferred computation for incremental updates
- **WriteBatch**: Atomic multi-key updates with group commit
- **PutEntity**: Wide-column entity storage
- **IngestExternalFile**: Bulk loading bypassing memtable

**WriteOptions** control durability, performance, and flow control trade-offs. Understanding write flow control is critical for maintaining stable write performance under heavy load.

For implementation details of the write path (WAL, memtable, write thread), see `docs/components/write_flow.md`.
