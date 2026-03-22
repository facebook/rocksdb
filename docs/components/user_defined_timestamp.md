# RocksDB User-Defined Timestamps (UDT)

## Overview

User-Defined Timestamps (UDT) is a feature that allows applications to embed custom timestamps directly into keys, enabling multi-version concurrency control (MVCC), time-travel queries, and time-based garbage collection. Unlike RocksDB's internal sequence numbers (which are opaque to users), UDT provides application-visible versioning where each key-value pair carries a user-supplied timestamp.

**Primary use cases:**
- **Multi-version data**: Maintain multiple timestamped versions of the same key
- **Time-travel queries**: Read data as it existed at a specific point in time
- **TTL and garbage collection**: Expire old versions based on timestamp thresholds
- **Snapshot isolation**: Provide consistent point-in-time views of data

**Key files:**
- `include/rocksdb/comparator.h` - Comparator interface with timestamp support
- `include/rocksdb/advanced_options.h` - `persist_user_defined_timestamps` option
- `include/rocksdb/options.h` - `ReadOptions::timestamp`, `ReadOptions::iter_start_ts`
- `db/dbformat.h` - Internal key format with timestamp encoding
- `util/udt_util.h` - Timestamp recovery and migration utilities
- `util/comparator.cc` - `ComparatorWithU64TsImpl` implementation

---

## 1. UDT Concept

In RocksDB without UDT, keys are ordered by user key, then by descending sequence number. With UDT enabled:
1. **Timestamp is part of the user key** (appended as a suffix)
2. **Ordering**: user key (without timestamp) → timestamp (descending) → sequence number (descending)
3. **Application controls versioning** via timestamps, not just RocksDB's internal sequence numbers

**⚠️ INVARIANT:** For the same user key (without timestamp), **larger (newer) timestamps come first** in iteration order.

### Why UDT?

| Without UDT | With UDT |
|-------------|----------|
| Only latest version visible (after compaction) | Multiple versions retained until explicit GC |
| No time-travel queries | Read historical data via timestamp bounds |
| Opaque sequence numbers | Application-controlled versioning |
| Retention controlled by snapshots/TTL compaction filter | Native timestamp-based retention |

---

## 2. Key Encoding

UDT modifies the internal key format by inserting the timestamp **between the user key and the internal footer**:

```
┌─────────────────────┬──────────────┬─────────────────────┐
│  User-provided key  │  Timestamp   │  Seqno + ValueType  │
│     (variable)      │   (ts_sz)    │      (8 bytes)      │
└─────────────────────┴──────────────┴─────────────────────┘
                           ↑
                    timestamp_size() bytes
                    (e.g., 8 bytes for uint64_t)
```

**Without UDT:**
```
Internal key = user_key || (seqno << 8 | type)
                          ^^^^^^^^^^^^^^^^^^^^
                          8-byte footer
```

**With UDT:**
```
Internal key = user_key_without_ts || timestamp || (seqno << 8 | type)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
               Full user key (what users see)
```

### Key Extraction Functions

From `db/dbformat.h`:

```cpp
// Extract timestamp from user key (includes timestamp)
inline Slice ExtractTimestampFromUserKey(const Slice& user_key, size_t ts_sz) {
  assert(user_key.size() >= ts_sz);
  return Slice(user_key.data() + user_key.size() - ts_sz, ts_sz);
}

// Strip timestamp from user key
inline Slice StripTimestampFromUserKey(const Slice& user_key, size_t ts_sz) {
  assert(user_key.size() >= ts_sz);
  return Slice(user_key.data(), user_key.size() - ts_sz);
}

// Extract timestamp from internal key
inline Slice ExtractTimestampFromKey(const Slice& internal_key, size_t ts_sz) {
  const size_t key_size = internal_key.size();
  assert(key_size >= kNumInternalBytes + ts_sz);
  return Slice(internal_key.data() + key_size - ts_sz - kNumInternalBytes, ts_sz);
}
```

**⚠️ INVARIANT:** All keys in a column family with UDT enabled MUST have the same timestamp size. This is enforced by the comparator's `timestamp_size()`.

---

## 3. Comparator Integration

The `Comparator` base class provides timestamp awareness via the `timestamp_size_` field:

```cpp
class Comparator : public Customizable, public CompareInterface {
 public:
  Comparator(size_t ts_sz) : timestamp_size_(ts_sz) {}

  inline size_t timestamp_size() const { return timestamp_size_; }

  // Compare with timestamps (default implementation)
  virtual int Compare(const Slice& a, const Slice& b) const = 0;

  // Compare without timestamps
  virtual int CompareWithoutTimestamp(const Slice& a, bool a_has_ts,
                                      const Slice& b, bool b_has_ts) const;

  // Compare only timestamps
  virtual int CompareTimestamp(const Slice& ts1, const Slice& ts2) const;

  virtual Slice GetMaxTimestamp() const;
  virtual Slice GetMinTimestamp() const;

 private:
  size_t timestamp_size_;
};
```

### ComparatorWithU64TsImpl

The built-in U64 timestamp comparator (`util/comparator.cc:238-318`) implements the UDT ordering logic:

```cpp
template <typename TComparator>
class ComparatorWithU64TsImpl : public Comparator {
 public:
  explicit ComparatorWithU64TsImpl() : Comparator(/*ts_sz=*/sizeof(uint64_t)) {}

  int Compare(const Slice& a, const Slice& b) const override {
    // 1. Compare user keys without timestamp
    int ret = CompareWithoutTimestamp(a, b);
    if (ret != 0) {
      return ret;
    }

    // 2. For same user key, compare timestamps in DESCENDING order
    // (larger/newer timestamp comes first)
    return -CompareTimestamp(
        ExtractTimestampFromUserKey(a, timestamp_size()),
        ExtractTimestampFromUserKey(b, timestamp_size()));
  }

  int CompareTimestamp(const Slice& ts1, const Slice& ts2) const override {
    uint64_t lhs = DecodeFixed64(ts1.data());
    uint64_t rhs = DecodeFixed64(ts2.data());
    return (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
  }

  Slice GetMaxTimestamp() const override { return MaxU64Ts(); }  // 0xFFFFFFFFFFFFFFFF
  Slice GetMinTimestamp() const override { return MinU64Ts(); }  // 0x0000000000000000
};
```

**Usage:**
```cpp
// Enable UDT with uint64_t timestamps
options.comparator = BytewiseComparatorWithU64Ts();

// Helper functions from include/rocksdb/comparator.h
Slice EncodeU64Ts(uint64_t ts, std::string* ts_buf);  // Encode timestamp
Status DecodeU64Ts(const Slice& ts, uint64_t* int_ts); // Decode timestamp
```

**⚠️ INVARIANT:** Timestamp comparison must be **consistent across restarts**. For custom timestamp formats, users must provide a `Comparator` subclass that:
1. Sets `timestamp_size()` to the timestamp byte length
2. Implements `CompareTimestamp()` to compare timestamps
3. Implements `GetMaxTimestamp()` and `GetMinTimestamp()`
4. Implements `TimestampToString()` for debugging

---

## 4. Write Path with UDT

### Put with Timestamp

When writing with UDT enabled, use the timestamp-aware `Put` overload:

```cpp
// User provides key WITHOUT timestamp, and timestamp separately
Slice user_key = "mykey";
uint64_t timestamp = 1234567890;

// Encode timestamp
std::string ts_buf;
Slice ts = EncodeU64Ts(timestamp, &ts_buf);

// Write using the timestamp-aware Put overload:
//   DB::Put(WriteOptions, key, ts, value)
// The plain Put(options, key, value) will reject UDT-enabled CFs
// with Status::InvalidArgument via FailIfCfHasTs().
db->Put(WriteOptions(), user_key, ts, value);
```

### WAL Encoding with Timestamps

**Challenge:** During recovery, RocksDB needs to know each column family's timestamp size to parse keys correctly.

**Solution:** `UserDefinedTimestampSizeRecord` (`util/udt_util.h:24-83`)

When a column family with non-zero timestamp size has not yet been recorded in the current WAL file, a `UserDefinedTimestampSizeRecord` is emitted via `MaybeAddUserDefinedTimestampSizeRecord()`. This record is written **once per column family per WAL file** (not before every `WriteBatch`), and applies to all subsequent records in that WAL.

```cpp
class UserDefinedTimestampSizeRecord {
 public:
  // Maps column family ID -> timestamp size
  std::vector<std::pair<uint32_t, size_t>> cf_to_ts_sz_;

  void EncodeTo(std::string* dst) const {
    for (const auto& [cf_id, ts_sz] : cf_to_ts_sz_) {
      PutFixed32(dst, cf_id);      // 4 bytes: CF ID
      PutFixed16(dst, ts_sz);      // 2 bytes: timestamp size
    }
  }

  Status DecodeFrom(Slice* src);
};
```

**⚠️ INVARIANT:** WAL guarantees that timestamp size info is logged **before** any `WriteBatch` that needs it (emitted once per CF per WAL file). Zero timestamp sizes are omitted (implicit).

### WriteBatch Encoding

From `db/write_batch.cc`, each entry in a `WriteBatch` is encoded as:

```
┌──────┬───────────┬─────────────────────┬──────────┬────────┐
│ Tag  │  CF ID    │  Key (with ts)      │ Val Len  │ Value  │
│ (1B) │ (varint)  │   (varint + data)   │(varint)  │ (data) │
└──────┴───────────┴─────────────────────┴──────────┴────────┘
```

The key includes the timestamp suffix. During recovery, RocksDB uses the earlier `UserDefinedTimestampSizeRecord` to correctly parse key boundaries.

---

## 5. Read Path with UDT

### Point Lookups (Get/MultiGet)

**ReadOptions::timestamp** specifies the **read timestamp** (upper bound):

```cpp
ReadOptions read_opts;
uint64_t read_ts = 1000;
std::string ts_buf;
Slice ts = EncodeU64Ts(read_ts, &ts_buf);
read_opts.timestamp = &ts;

std::string value;
db->Get(read_opts, "key_without_ts", &value);
// Returns the latest version of "key" with timestamp <= 1000
```

**Behavior:**
- For a given user key (without timestamp), returns the **most recent version** with `timestamp <= read_timestamp`
- Internally, `DBImpl::GetImpl()` constructs `LookupKey(key, snapshot, read_options.timestamp)`, which appends the caller-provided read timestamp to the key (not `max_timestamp`). Visibility is determined by comparing the read timestamp against each version's timestamp.

**⚠️ INVARIANT:** `ReadOptions::timestamp` must be provided when reading from a UDT-enabled column family. Omitting it returns `Status::InvalidArgument`.

### Iterator Range Scans

**ReadOptions::iter_start_ts** (lower bound) and **ReadOptions::timestamp** (upper bound) define a timestamp range:

```cpp
ReadOptions read_opts;
uint64_t start_ts = 500;
uint64_t end_ts = 1000;

std::string start_ts_buf, end_ts_buf;
Slice start_ts_slice = EncodeU64Ts(start_ts, &start_ts_buf);
Slice end_ts_slice = EncodeU64Ts(end_ts, &end_ts_buf);
read_opts.iter_start_ts = &start_ts_slice;
read_opts.timestamp = &end_ts_slice;

auto iter = db->NewIterator(read_opts);
for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
  // When iter_start_ts is set, key() returns the full internal key
  // (user_key + timestamp + seqno/type). Use timestamp() for the timestamp.
  // When iter_start_ts is NOT set, key() returns user key WITHOUT timestamp.
  Slice key = iter->key();
  Slice value = iter->value();
  Slice ts = iter->timestamp();     // Extracted timestamp
}
```

**Timestamp extraction:**
```cpp
// From include/rocksdb/iterator.h
virtual Slice timestamp() const;
// Base class provides a virtual method; concrete iterators implement
// timestamp extraction from the current key
```

**⚠️ INVARIANT:** If `iter_start_ts == nullptr`, only the **most recent version** of each key (with `timestamp <= ReadOptions::timestamp`) is returned. When `iter_start_ts != nullptr`, `key()` returns the full internal key (user key + timestamp + seqno/type); use `timestamp()` to extract the timestamp separately.

### Key-Shape Invariants for UDT APIs

When UDT is enabled, different APIs have different expectations for whether keys include timestamps:

| API | Key format | Notes |
|-----|-----------|-------|
| `Put(opts, key, ts, value)` | Key **without** timestamp | Timestamp passed separately |
| `Delete(opts, key, ts)` | Key **without** timestamp | Timestamp passed separately |
| `WriteBatch::DeleteRange(cf, begin, end, ts)` | Begin/end keys **without** timestamp | Timestamp passed separately |
| `Get(opts, key, &value)` | Key **without** timestamp | Read timestamp via `ReadOptions::timestamp` |
| `Iterator::Seek(target)` | Target **without** timestamp | "Target does not contain timestamp" |
| `Iterator::SeekForPrev(target)` | Target **without** timestamp | "Target does not contain timestamp" |
| `ReadOptions::iterate_lower_bound` | Key **without** timestamp | |
| `ReadOptions::iterate_upper_bound` | Key **without** timestamp | |
| `Range` / `RangeOpt` endpoints | Keys **without** timestamp | |
| `Iterator::key()` (no `iter_start_ts`) | Returns key **without** timestamp | Timestamp stripped |
| `Iterator::key()` (with `iter_start_ts`) | Returns **internal key** (key + ts + seqno/type) | |
| `Iterator::timestamp()` | Returns the timestamp | Always available |

### Timestamp Mismatch Validation

From `db/db_impl/db_impl.cc:2491`:

```cpp
Status DBImpl::Get(..., const ReadOptions& read_options, ...) {
  if (read_options.timestamp) {
    // Validate provided timestamp matches CF's timestamp size
    const Status s = FailIfTsMismatchCf(column_family, *(read_options.timestamp));
    if (!s.ok()) {
      return s;  // InvalidArgument if mismatch
    }
  } else {
    // Ensure CF doesn't require timestamps
    const Status s = FailIfCfHasTs(column_family);
    if (!s.ok()) {
      return s;
    }
  }
  // ...
}
```

---

## 6. Compaction with UDT

### Timestamp-Based Garbage Collection

**CompactRangeOptions::full_history_ts_low** specifies the **cutoff timestamp** for garbage collection:

```cpp
CompactRangeOptions compact_opts;
uint64_t gc_ts = 500;  // Garbage collect versions older than 500
std::string ts_buf;
Slice cutoff_ts = EncodeU64Ts(gc_ts, &ts_buf);
compact_opts.full_history_ts_low = &cutoff_ts;

db->CompactRange(compact_opts, nullptr, nullptr);
// After compaction, versions with timestamp < 500 are removed
```

**Compaction logic** (`db/compaction/compaction_iterator.cc`):

```cpp
// Pseudocode from CompactionIterator
bool CompactionIterator::NextFromInput() {
  ParsedInternalKey ikey;
  ParseInternalKey(input_->key(), &ikey, allow_data_in_errors);

  if (timestamp_size_ > 0 && full_history_ts_low_) {
    // Extract timestamp from user key
    Slice ts = ExtractTimestampFromUserKey(ikey.user_key, timestamp_size_);

    // GC if timestamp < full_history_ts_low
    if (cmp_->CompareTimestamp(ts, *full_history_ts_low_) < 0) {
      if (/* this is not the newest version of this key */) {
        // Drop this version (garbage collect)
        return true;  // Skip to next key
      } else {
        // Keep this version (newest for this user key)
      }
    }
  }
  // ...
}
```

**⚠️ INVARIANT:** Compaction with `full_history_ts_low` **may GC** older versions of each user key (the option comment says "maybe GCed"). For Put/Merge entries, the newest version of a user key is generally retained even if older than `full_history_ts_low`. However, when the newest version is a `kTypeDeletionWithTimestamp` with timestamp below `full_history_ts_low`, compaction can drop the deletion marker **and all older versions**, removing all history for that key entirely.

### Dynamic full_history_ts_low

From `db/column_family.h`:
- Each `ColumnFamilyData` tracks `full_history_ts_low_` (can be updated via `DB::IncreaseFullHistoryTsLow()`, monotonically increasing)
- `SuperVersion::full_history_ts_low` snapshots the effective value at SuperVersion creation time (immutable per SuperVersion, used for read-side sanity checks)
- `full_history_ts_low` is persisted to MANIFEST via `VersionEdit::SetFullHistoryTsLow()` and survives restarts
- Compaction reads the `full_history_ts_low` value to determine which versions to garbage collect

### Key Public APIs for Timestamp GC

```cpp
// Increase the full_history_ts_low cutoff (can only increase, never decrease)
virtual Status IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                        std::string ts_low) = 0;

// Get current full_history_ts_low value
virtual Status GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                   std::string* ts_low) = 0;

// Get the newest user-defined timestamp written to this column family
virtual Status GetNewestUserDefinedTimestamp(
    ColumnFamilyHandle* column_family, std::string* newest_timestamp) = 0;
```

---

## 7. Flush with UDT: persist_user_defined_timestamps

**Option:** `AdvancedColumnFamilyOptions::persist_user_defined_timestamps` (`include/rocksdb/advanced_options.h:1200`)

```cpp
// Default: true (persist timestamps to SST files)
bool persist_user_defined_timestamps = true;
```

### When persist_user_defined_timestamps = true (default)

- **SST files store keys WITH timestamps**
- **WAL stores keys WITH timestamps**
- **Use case:** Full timestamp-based versioning and time-travel queries
- **Storage overhead:** `timestamp_size` bytes per key

### When persist_user_defined_timestamps = false

- **SST files store keys WITHOUT timestamps** (timestamps are stripped during flush)
- **WAL still stores keys WITH timestamps** (enables recovery)
- **MemTable stores keys WITH timestamps** (for in-memory queries)
- **Use case:** "UDT in memtable only" — timestamps used for in-memory conflict resolution but not persisted
- **Benefit:** Reduces storage overhead in SST files

**Flush behavior** (from `db/flush_job.cc:868`):

```cpp
// Determine whether to strip timestamps during flush
const bool logical_strip_timestamp =
    ts_sz > 0 && !cfd_->ioptions().persist_user_defined_timestamps;

// If logical_strip_timestamp is true, timestamps are stripped when
// building the SST file; otherwise keys retain their timestamps
```

**⚠️ INVARIANT:** Once `persist_user_defined_timestamps = false`, reads with `ReadOptions::timestamp < full_history_ts_low` will return `Status::InvalidArgument`. This is because older versions are collapsed during flush.

**Migration path** (`util/udt_util.h:265`):
```cpp
// ValidateUserDefinedTimestampsOptions enforces:
// 1. Can enable UDT if persist_user_defined_timestamps = false initially
// 2. Can disable UDT if persist_user_defined_timestamps was already false
// 3. Cannot change persist_user_defined_timestamps from true -> false
//    or false -> true without full compaction
```

**Restrictions:**
- **NOT compatible with atomic flush** (`options.atomic_flush = true`)
- **NOT compatible with concurrent memtable writes** (`options.allow_concurrent_memtable_write = true`)
- **Only supports builtin `.u64ts` comparators** (e.g., `BytewiseComparatorWithU64Ts()`, `ReverseBytewiseComparatorWithU64Ts()`). Custom comparators with arbitrary timestamp formats are not supported when `persist_user_defined_timestamps = false`.
- **Recommended:** Set `options.avoid_flush_during_shutdown = true` and `options.avoid_flush_during_recovery = true` to preserve WAL-based timestamp recovery (e.g., for downgrade scenarios). These are not hard requirements but are strongly recommended when relying on WAL timestamps.

---

## 8. Recovery: WAL Replay with Timestamps

### Timestamp Size Mismatch Problem

During crash recovery, RocksDB replays WAL entries. If the column family's timestamp size has changed (e.g., UDT was enabled or disabled), the recorded timestamp size in the WAL may differ from the current running timestamp size.

**Example scenarios:**
1. **WAL recorded ts_sz = 0, running ts_sz = 8**: UDT was just enabled
2. **WAL recorded ts_sz = 8, running ts_sz = 0**: UDT was just disabled
3. **WAL recorded ts_sz = 8, running ts_sz = 16**: Timestamp format changed (NOT supported)

### TimestampRecoveryHandler

From `util/udt_util.h:105-192`:

```cpp
class TimestampRecoveryHandler : public WriteBatch::Handler {
 public:
  TimestampRecoveryHandler(
      const UnorderedMap<uint32_t, size_t>& running_ts_sz,  // Current CF timestamp sizes
      const UnorderedMap<uint32_t, size_t>& record_ts_sz,   // WAL recorded timestamp sizes
      bool seq_per_batch, bool batch_per_txn);

  // Reconcile timestamp discrepancies during replay
  Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override;
  // ... other WriteBatch operations

  std::unique_ptr<WriteBatch>&& TransferNewBatch();  // Returns corrected WriteBatch

 private:
  // Best-effort reconciliation rules:
  // 1. recorded_ts_sz = 0, running_ts_sz > 0 => Append min timestamp
  // 2. recorded_ts_sz > 0, running_ts_sz = 0 => Strip timestamp
  // 3. recorded_ts_sz == running_ts_sz => No-op
  // 4. Both non-zero but different => InvalidArgument (unsupported)
  Status ReconcileTimestampDiscrepancy(uint32_t cf, const Slice& key,
                                       std::string* new_key_buf, Slice* new_key);
};
```

### Recovery Modes

From `util/udt_util.h:196-208`:

```cpp
enum class TimestampSizeConsistencyMode {
  // Fail if any mismatch
  kVerifyConsistency,

  // Attempt best-effort reconciliation, create new WriteBatch if needed
  kReconcileInconsistency,
};

Status HandleWriteBatchTimestampSizeDifference(
    const WriteBatch* batch,
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz,
    TimestampSizeConsistencyMode check_mode,
    bool seq_per_batch, bool batch_per_txn,
    std::unique_ptr<WriteBatch>* new_batch = nullptr);
```

**⚠️ INVARIANT:** Changing timestamp size from N to M (both non-zero, N ≠ M) is **NOT supported** and will fail recovery. Only transitions between 0 ↔ N are allowed (enable/disable UDT).

---

## 9. Column Family Timestamp Configuration

### Per-CF Timestamp Size

Each column family has its own timestamp configuration via its comparator:

```cpp
ColumnFamilyOptions cf_opts;
cf_opts.comparator = BytewiseComparatorWithU64Ts();  // ts_sz = 8

DBOptions db_opts;
std::vector<ColumnFamilyDescriptor> cf_descriptors = {
  {kDefaultColumnFamilyName, cf_opts_default},      // No UDT
  {"cf_with_udt", cf_opts},                         // UDT enabled
};

DB* db;
DB::Open(db_opts, dbname, cf_descriptors, &handles, &db);
```

**Timestamp size sources:**
1. **Comparator:** `Comparator::timestamp_size()` determines the byte length
2. **ImmutableCFOptions:** Stores `comparator` and derived `user_comparator->timestamp_size()`
3. **VersionEdit:** Records `persist_user_defined_timestamps` flag per SST file (via table properties)

### Timestamp Size Enforcement

From `db/db_impl/db_impl.cc:2491`:

```cpp
Status DBImpl::FailIfTsMismatchCf(ColumnFamilyHandle* cf, const Slice& ts) {
  auto cfd = static_cast<ColumnFamilyHandleImpl*>(cf)->cfd();
  size_t expected_ts_sz = cfd->user_comparator()->timestamp_size();

  if (ts.size() != expected_ts_sz) {
    return Status::InvalidArgument(
        "Timestamp size mismatch: expected " + std::to_string(expected_ts_sz) +
        " bytes, got " + std::to_string(ts.size()) + " bytes");
  }
  return Status::OK();
}
```

**⚠️ INVARIANT:** All operations on a column family must use timestamps of **exactly** `comparator->timestamp_size()` bytes. Mixed timestamp sizes are rejected.

---

## 10. Compatibility and Migration

### Enabling UDT on Existing Data

From `util/udt_util.h:239-268`:

```cpp
Status ValidateUserDefinedTimestampsOptions(
    const Comparator* new_comparator,
    const std::string& old_comparator_name,
    bool new_persist_udt,
    bool old_persist_udt,
    bool* mark_sst_files_has_no_udt);
```

**Allowed migrations:**
1. **No comparator change, no persist flag change**: ✅ Safe
2. **Enable UDT with `persist_user_defined_timestamps = false`**: ✅ Safe
   - Existing SST files are marked as "no UDT"
   - New writes include timestamps (memtable only)
   - Old data remains readable without timestamps
3. **Disable UDT if `persist_user_defined_timestamps` was already `false`**: ✅ Safe
   - No timestamps in SST files to clean up
4. **Enable UDT with `persist_user_defined_timestamps = true`**: ❌ **Rejected**
   - Returns `Status::InvalidArgument`; this transition is not supported
5. **Toggle `persist_user_defined_timestamps` while UDT remains enabled**: ❌ **Rejected**
   - The option is immutable (`persist_user_defined_timestamps` cannot be changed via `SetOptions()`)
   - Returns `Status::InvalidArgument` if the flag differs between old and new config
6. **Change timestamp size**: ❌ **NOT supported**
   - Cannot change from 8-byte to 16-byte timestamps

**Migration procedure** (enable UDT on existing DB):

```cpp
// Enable UDT with persist = false (the only supported path for existing data)
Options options;
options.comparator = BytewiseComparatorWithU64Ts();
options.persist_user_defined_timestamps = false;

DB* db;
Status s = DB::Open(options, dbname, &db);

// Write with timestamps (timestamps kept in memtable, stripped on flush)
std::string ts_buf;
Slice ts = EncodeU64Ts(1000, &ts_buf);
db->Put(WriteOptions(), "mykey", ts, "value");
```

### VersionEdit Tracking

**FileMetaData** (`db/version_edit.h:323`):

```cpp
struct FileMetaData {
  // Value of the `AdvancedColumnFamilyOptions.persist_user_defined_timestamps`
  // flag when the file is created. Default to true, only when this flag is
  // false, it's explicitly written to Manifest.
  bool user_defined_timestamps_persisted = true;

  // Minimum user-defined timestamp in the file. Empty if no UDT or unknown.
  std::string min_timestamp;

  // Maximum user-defined timestamp in the file. Empty if no UDT or unknown.
  std::string max_timestamp;
};
```

**VersionEdit** (`db/version_edit.h:724-733`) provides methods to track the persist flag:

```cpp
void SetPersistUserDefinedTimestamps(bool persist_user_defined_timestamps) {
  has_persist_user_defined_timestamps_ = true;
  persist_user_defined_timestamps_ = persist_user_defined_timestamps;
}
bool HasPersistUserDefinedTimestamps() const;
bool GetPersistUserDefinedTimestamps() const;
```

**SST file table properties** store the `persist_user_defined_timestamps` flag (via `user_defined_timestamps_persisted` field) used during file creation. When opening an SST file, RocksDB reads this property to determine whether keys have timestamps.

**⚠️ INVARIANT:** Once an SST file is written with `persist_user_defined_timestamps = false`, it CANNOT be read as if it has timestamps (and vice versa). Changing this flag requires rewriting SST files via compaction.

---

## Diagrams

### Key Format with UDT

```
┌───────────────────────────────────────────────────────────┐
│                    INTERNAL KEY                           │
├───────────────────────────────┬──────────┬────────────────┤
│     USER KEY (with ts)        │   TS     │  Seqno + Type  │
│ ┌─────────────────┬─────────┐ │  (8B)    │     (8B)       │
│ │  User Key w/o TS│   TS    │ │          │                │
│ │   (variable)    │  (8B)   │ │          │                │
│ └─────────────────┴─────────┘ │          │                │
│  <─── Seen by user ───────>   │          │                │
└───────────────────────────────┴──────────┴────────────────┘
                                  ↑                ↑
                          Duplicate for           Internal
                          clarity (same           footer
                          timestamp)
```

**Note:** The timestamp appears to be duplicated but is only stored once. The diagram shows the logical structure where the user key includes the timestamp suffix.

### Compaction with full_history_ts_low

```
Before Compaction (3 versions of "key1"):
┌──────────────────────────────────────┐
│ key1|ts=1000  →  value_v3  (seqno=3) │  ← Newest
│ key1|ts=600   →  value_v2  (seqno=2) │
│ key1|ts=200   →  value_v1  (seqno=1) │  ← Oldest
└──────────────────────────────────────┘

After Compaction (full_history_ts_low = 500):
┌──────────────────────────────────────┐
│ key1|ts=1000  →  value_v3  (seqno=3) │  ← Kept (ts >= 500)
│ key1|ts=600   →  value_v2  (seqno=2) │  ← Kept (ts >= 500)
└──────────────────────────────────────┘
  key1|ts=200 is DELETED (ts < 500)

Special case: If key1|ts=200 was the ONLY Put version:
┌──────────────────────────────────────┐
│ key1|ts=200   →  value_v1  (seqno=1) │  ← Kept (newest Put for this key)
└──────────────────────────────────────┘

Special case: If a Delete(key1, ts=200) was the only entry and
ts < full_history_ts_low, it may be dropped entirely (no versions remain).
```

### Read Path with Timestamp Bounds

```
Iterator with iter_start_ts=400, timestamp=900:

LSM State:
┌──────────────────────────────────────┐
│ key1|ts=1000  →  value_v4  (seqno=4) │  ← Filtered (ts > 900)
│ key1|ts=800   →  value_v3  (seqno=3) │  ← RETURNED
│ key1|ts=600   →  value_v2  (seqno=2) │  ← RETURNED
│ key1|ts=300   →  value_v1  (seqno=1) │  ← Filtered (ts < 400)
│ key2|ts=700   →  value_k2  (seqno=5) │  ← RETURNED
└──────────────────────────────────────┘

Iterator output (in order):
  key1|ts=800 → value_v3
  key1|ts=600 → value_v2
  key2|ts=700 → value_k2
```

### persist_user_defined_timestamps = false

```
┌─────────────────┐
│    MemTable     │  Keys stored WITH timestamps
│  key1|ts=100    │
│  key1|ts=200    │
└────────┬────────┘
         │ Flush
         ↓
┌─────────────────┐
│   SST File      │  Keys stored WITHOUT timestamps
│     key1        │  (timestamps stripped)
│   (seqno=2)     │
└─────────────────┘

Read with timestamp=150 (after flush, full_history_ts_low not set):
  → Returns value from SST (key treated as having minimum timestamp)

Read with timestamp=150 (after full_history_ts_low=200 set):
  → InvalidArgument (read timestamp < full_history_ts_low)
```

---

## Usage Examples

### Basic UDT Workflow

```cpp
// 1. Open DB with UDT-enabled comparator
Options options;
options.create_if_missing = true;
options.comparator = BytewiseComparatorWithU64Ts();

DB* db;
Status s = DB::Open(options, dbname, &db);

// 2. Write with timestamps (using the timestamp-aware Put overload)
for (uint64_t ts = 100; ts <= 300; ts += 100) {
  std::string ts_buf;
  Slice timestamp = EncodeU64Ts(ts, &ts_buf);

  s = db->Put(WriteOptions(), "mykey", timestamp,
              "value_at_ts_" + std::to_string(ts));
}

// 3. Read latest version as of ts=250
ReadOptions read_opts;
uint64_t read_ts = 250;
std::string read_ts_buf;
Slice read_ts_slice = EncodeU64Ts(read_ts, &read_ts_buf);
read_opts.timestamp = &read_ts_slice;

std::string value;
db->Get(read_opts, "mykey", &value);  // Returns "value_at_ts_200"

// 4. Scan all versions between ts=150 and ts=300
// Each Slice needs its own backing buffer to avoid aliasing
std::string start_ts_buf, end_ts_buf;
Slice start_ts_slice = EncodeU64Ts(150, &start_ts_buf);
Slice end_ts_slice = EncodeU64Ts(300, &end_ts_buf);
read_opts.iter_start_ts = &start_ts_slice;
read_opts.timestamp = &end_ts_slice;

auto iter = db->NewIterator(read_opts);
for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
  uint64_t ts;
  DecodeU64Ts(iter->timestamp(), &ts);
  std::cout << "ts=" << ts << " value=" << iter->value().ToString() << "\n";
}
// Output (newer timestamps first for same user key):
//   ts=300 value=value_at_ts_300
//   ts=200 value=value_at_ts_200

delete iter;
delete db;
```

### Garbage Collection

```cpp
// Compact and garbage collect versions older than ts=200
CompactRangeOptions compact_opts;
std::string gc_ts_buf;
Slice gc_ts = EncodeU64Ts(200, &gc_ts_buf);
compact_opts.full_history_ts_low = &gc_ts;

db->CompactRange(compact_opts, nullptr, nullptr);
// After compaction, version at ts=100 is deleted
```

---

## Summary Table

| Aspect | Details |
|--------|---------|
| **Key encoding** | `user_key_without_ts \|\| timestamp \|\| (seqno + type)` |
| **Ordering** | user key → timestamp (desc) → seqno (desc) |
| **Comparator** | `BytewiseComparatorWithU64Ts()` for uint64_t timestamps |
| **Write** | Use `Put(options, key, ts, value)` overload; plain `Put` rejects UDT CFs |
| **Read (point)** | `ReadOptions::timestamp` = upper bound |
| **Read (scan)** | `iter_start_ts` = lower bound, `timestamp` = upper bound |
| **GC** | `CompactRangeOptions::full_history_ts_low` triggers version removal |
| **Persistence** | `persist_user_defined_timestamps` controls SST storage |
| **Recovery** | `TimestampRecoveryHandler` reconciles WAL mismatches |
| **Migration** | Enable with `persist = false` only; enabling with `persist = true` is rejected |

**⚠️ CRITICAL INVARIANTS:**
1. **Timestamp size must be uniform** across all keys in a column family
2. **Larger (newer) timestamps come first** in iteration order
3. **Older versions may be GCed** by compaction when older than `full_history_ts_low`; deletion markers can cause complete removal of a key's history
4. **Changing timestamp size** (N → M, both non-zero) is **NOT supported**
5. **`persist_user_defined_timestamps` flag** must match the SST file's creation-time setting when reading

---

**References:**
- Original design: https://github.com/facebook/rocksdb/wiki/User-defined-Timestamp
- API documentation: `include/rocksdb/options.h`, `include/rocksdb/comparator.h`
- Test examples: `db/db_with_timestamp_basic_test.cc`, `db/db_with_timestamp_compaction_test.cc`
