# RocksDB Write-Ahead Log (WAL)

## Overview

The Write-Ahead Log (WAL) is RocksDB's **durability mechanism** that ensures committed writes survive process crashes and power failures. By default, each write operation is first appended to the WAL before being inserted into the MemTable. Writes with `WriteOptions::disableWAL=true` skip the WAL entirely. In 2PC transactions, the prepare phase writes only to the WAL (`disable_memtable=true`), deferring MemTable insertion to the commit phase. During crash recovery, RocksDB replays WAL records to reconstruct in-memory state.

### Purpose

1. **Crash Recovery:** Reconstruct MemTables from durable WAL records after a crash
2. **Durability Guarantees:** Provide configurable durability via sync modes
3. **Replication:** Enable logical replication by tailing WAL files
4. **2PC Support:** Store prepare/commit markers for two-phase commit transactions

### WAL in the Write Path

```
┌──────────────────────────────────────────────────────────────┐
│  DBImpl::WriteImpl (db/db_impl/db_impl_write.cc)            │
│  ┌──────────────────────────────────────────────┐           │
│  │ 1. WriteThread (batch, elect leader)         │           │
│  │ 2. WAL Write (log::Writer::AddRecord)  ◄──────────┐      │
│  │ 3. MemTable insertion                         │   │      │
│  │ 4. Publish sequence number                    │   │      │
│  └──────────────────────────────────────────────┘   │      │
└──────────────────────────────────────────────────────┼──────┘
                                                       │
                                                       v
                              ┌────────────────────────────────┐
                              │  WAL FILE (32KB blocks)        │
                              │  ┌──────────────────────────┐  │
                              │  │ Block 0 (32KB)           │  │
                              │  │  ├─ Record (Full/First)  │  │
                              │  │  ├─ Record (Middle)      │  │
                              │  │  └─ Record (Last)        │  │
                              │  ├──────────────────────────┤  │
                              │  │ Block 1 (32KB)           │  │
                              │  │  └─ ...                  │  │
                              │  └──────────────────────────┘  │
                              └────────────────────────────────┘
                                           │
                         Memtable full? ───┘
                                           │
                                           v
                              ┌────────────────────────────────┐
                              │  FLUSH → L0 SST                │
                              │  WAL can be archived/deleted   │
                              └────────────────────────────────┘
```

---

## 1. Record Format

**Files:** `db/log_format.h`, `db/log_writer.h:51-74`

### Physical Record Structure

Each WAL record has a **7-byte legacy header** or **11-byte recyclable header** followed by payload:

#### Legacy Record (default)

```
┌─────────┬───────────┬───────────┬─────────────┐
│ CRC (4B)│ Size (2B) │ Type (1B) │ Payload     │
└─────────┴───────────┴───────────┴─────────────┘
         └──────── 7 bytes header ────────┘
```

| Field | Size | Description |
|-------|------|-------------|
| CRC | 4B | CRC32C checksum over `Type + Payload` (legacy) or `Type + Log Number + Payload` (recyclable) |
| Size | 2B | Payload length (≤ 65535 bytes) |
| Type | 1B | Record type (see below) |
| Payload | variable | Record data (see below) |

**Note:** While most records contain serialized `WriteBatch` data, the WAL also contains non-WriteBatch metadata records: `kSetCompressionType` (WAL compression marker), `kUserDefinedTimestampSizeType` / `kRecyclableUserDefinedTimestampSizeType` (UDT size records), and `kPredecessorWALInfoType` / `kRecyclePredecessorWALInfoType` (WAL verification records). These are emitted by `Writer::AddCompressionTypeRecord()`, `Writer::MaybeAddUserDefinedTimestampSizeRecord()`, and `Writer::MaybeAddPredecessorWALInfo()` (`db/log_writer.cc:193-306`) and handled by the reader (`db/log_reader.cc:176-238`).

**Files:** `db/log_format.h:56-57`, `db/log_writer.cc:311-328`

#### Recyclable Record (when `recycle_log_files=true`)

```
┌─────────┬───────────┬───────────┬────────────────┬─────────────┐
│ CRC (4B)│ Size (2B) │ Type (1B) │ Log Number (4B)│ Payload     │
└─────────┴───────────┴───────────┴────────────────┴─────────────┘
         └────────────── 11 bytes header ──────────────┘
```

The **log number** field distinguishes records from the current writer vs. stale data from a recycled file.

**Files:** `db/log_format.h:59-61`

### Record Types

**Files:** `db/log_format.h:22-49`

```cpp
enum RecordType : uint8_t {
  // Reserved for preallocated files
  kZeroType = 0,

  // Legacy types
  kFullType = 1,        // Record fits in single block
  kFirstType = 2,       // First fragment of multi-block record
  kMiddleType = 3,      // Middle fragment
  kLastType = 4,        // Last fragment

  // Recyclable types (log number included)
  kRecyclableFullType = 5,
  kRecyclableFirstType = 6,
  kRecyclableMiddleType = 7,
  kRecyclableLastType = 8,

  // Metadata types
  kSetCompressionType = 9,                                // WAL compression marker
  kUserDefinedTimestampSizeType = 10,                     // UDT size record
  kRecyclableUserDefinedTimestampSizeType = 11,           // Recyclable UDT size record

  // WAL verification (recyclable variants at 130/131)
  kPredecessorWALInfoType = 130,                          // WAL verification
  kRecyclePredecessorWALInfoType = 131,                   // Recyclable WAL verification
};
```

⚠️ **INVARIANT:** Records with `type >= 10` and bit 7 set (`kRecordTypeSafeIgnoreMask = 0x80`) can be safely ignored by older readers.

### WriteBatch Payload

The **Payload** field contains a serialized `WriteBatch` with the following structure:

**Files:** `db/write_batch.cc:432-503`, `db/dbformat.h:41-78`

```
WriteBatch::rep_ :=
  sequence: fixed64         // Starting sequence number
  count: fixed32            // Number of operations
  data: record[count]       // Variable-length records

record :=
  kTypeValue varstring varstring                          // Put
  kTypeDeletion varstring                                 // Delete
  kTypeSingleDeletion varstring                           // SingleDelete
  kTypeRangeDeletion varstring varstring                  // Delete range
  kTypeMerge varstring varstring                          // Merge
  kTypeColumnFamilyValue varint32 varstring varstring     // CF Put
  kTypeColumnFamilyDeletion varint32 varstring            // CF Delete
  kTypeColumnFamilySingleDeletion varint32 varstring      // CF SingleDelete
  kTypeColumnFamilyRangeDeletion varint32 varstring varstring  // CF DeleteRange
  kTypeColumnFamilyMerge varint32 varstring varstring     // CF Merge
  kTypeLogData varstring                                  // Arbitrary log data blob
  kTypeBlobIndex varstring varstring                      // Blob DB index
  kTypeColumnFamilyBlobIndex varint32 varstring varstring // CF Blob DB index
  kTypeDeletionWithTimestamp varstring                    // Delete with timestamp
  kTypeBeginPrepareXID                                    // 2PC prepare start
  kTypeEndPrepareXID varstring                            // 2PC prepare end (XID)
  kTypeCommitXID varstring                                // 2PC commit
  kTypeCommitXIDAndTimestamp varstring varstring          // 2PC commit with timestamp
  kTypeRollbackXID varstring                              // 2PC rollback
  kTypeBeginPersistedPrepareXID                           // 2PC persisted prepare start
  kTypeBeginUnprepareXID                                  // 2PC unprepare start
  kTypeWideColumnEntity varstring varstring               // Wide column entity
  kTypeColumnFamilyWideColumnEntity varint32 varstring varstring  // CF wide column
  kTypeValuePreferredSeqno varstring varstring            // TimedPut (value + write time)
  kTypeColumnFamilyValuePreferredSeqno varint32 varstring varstring  // CF TimedPut
  kTypeNoop                                               // No-op marker

varstring :=
  len: varint32
  data: uint8[len]
```

---

## 2. Block Structure

**Files:** `db/log_format.h:54`

WAL files are divided into **fixed 32KB blocks**:

```cpp
constexpr unsigned int kBlockSize = 32768;  // 32KB
```

### Block Layout

```
┌─────────────────────── WAL File ────────────────────────┐
│                                                          │
│  Block 0 (32768 bytes)                                  │
│  ┌────────────────────────────────────────────────────┐ │
│  │ Record 1 (Full)                     │ Record 2 ... │ │
│  │ [7B header][payload]                │              │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  Block 1 (32768 bytes)                                  │
│  ┌────────────────────────────────────────────────────┐ │
│  │ ... Record 2 (Middle) ...                          │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  Block 2 (32768 bytes)                                  │
│  ┌────────────────────────────────────────────────────┐ │
│  │ ... Record 2 (Last) │ Padding (zeros)              │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  Block 3 ...                                            │
└──────────────────────────────────────────────────────────┘
```

⚠️ **INVARIANT:** Writer never leaves `< header_size` bytes in a block. If a record header doesn't fit, the remaining space is zero-padded and a new block starts.

**Files:** `db/log_writer.cc:113-133`

### Spanning Records Across Blocks

When a WriteBatch exceeds `32KB - header_size`, it's fragmented:

| Scenario | Record Types |
|----------|--------------|
| Fits in one block | `kFullType` |
| Spans 2 blocks | `kFirstType` → `kLastType` |
| Spans 3+ blocks | `kFirstType` → `kMiddleType`... → `kLastType` |

**Example:** 70KB WriteBatch (assuming 7-byte header)

```
Block 0: [kFirstType][32761 bytes payload]
Block 1: [kMiddleType][32761 bytes payload]
Block 2: [kLastType][4478 bytes payload][padding]
```

**Files:** `db/log_writer.cc:162-172`

---

## 3. WAL Writer

**Files:** `db/log_writer.h`, `db/log_writer.cc`

### Writer Initialization

```cpp
Writer::Writer(std::unique_ptr<WritableFileWriter>&& dest,
               uint64_t log_number,
               bool recycle_log_files,
               bool manual_flush = false,
               CompressionType compression_type = kNoCompression,
               bool track_and_verify_wals = false);
```

**Files:** `db/log_writer.cc:23-41`

| Parameter | Purpose |
|-----------|---------|
| `dest` | Writable file handle for the WAL |
| `log_number` | Unique WAL file number (monotonically increasing) |
| `recycle_log_files` | Use recyclable record format + log number |
| `manual_flush` | If true, caller must call `WriteBuffer()` to flush |
| `compression_type` | WAL compression (experimental) |
| `track_and_verify_wals` | Write predecessor WAL info for verification |

### Writing Records: `AddRecord()`

**Files:** `db/log_writer.cc:89-191`

```cpp
IOStatus Writer::AddRecord(const WriteOptions& write_options,
                           const Slice& slice,
                           const SequenceNumber& seqno = 0);
```

#### Algorithm

1. **Fragment the record** if it exceeds available block space
2. For each fragment:
   - Determine record type (`kFullType`, `kFirstType`, `kMiddleType`, `kLastType`)
   - Call `EmitPhysicalRecord()` to write header + payload
3. **Flush** (unless `manual_flush=true`)
4. Update `last_seqno_recorded_`

**Files:** `db/log_writer.cc:112-191`

### Emitting Physical Records: `EmitPhysicalRecord()`

**Files:** `db/log_writer.cc:311-363`

```cpp
IOStatus Writer::EmitPhysicalRecord(const WriteOptions& write_options,
                                    RecordType type,
                                    const char* ptr,
                                    size_t n);
```

#### Steps

1. **Format header:**
   - Bytes 4-5: Payload length (little-endian 16-bit)
   - Byte 6: Record type
   - Bytes 0-3: CRC32C checksum (over type + payload for legacy; type + log number + payload for recyclable)
   - Bytes 7-10 (recyclable only): Log number

2. **Compute CRC:**
   ```cpp
   uint32_t crc = type_crc_[type];  // Pre-computed type CRC
   // For recyclable records, extend with 4-byte log number:
   // crc = crc32c::Extend(crc, log_number_buf, 4);
   uint32_t payload_crc = crc32c::Value(ptr, n);
   crc = crc32c::Crc32cCombine(crc, payload_crc, n);
   crc = crc32c::Mask(crc);  // Mask for storage
   ```

3. **Write header + payload** via `WritableFileWriter::Append()`

4. **Update `block_offset_`**

⚠️ **INVARIANT:** `block_offset_ + header_size + n <= kBlockSize` when entering `EmitPhysicalRecord()`.

---

## 4. WAL Reader

**Files:** `db/log_reader.h`, `db/log_reader.cc`

### Reader Initialization

```cpp
Reader::Reader(std::shared_ptr<Logger> info_log,
               std::unique_ptr<SequentialFileReader>&& file,
               Reporter* reporter,
               bool checksum,
               uint64_t log_num,
               bool track_and_verify_wals = false,
               bool stop_replay_for_corruption = false,
               uint64_t min_wal_number_to_keep = std::numeric_limits<uint64_t>::max(),
               const PredecessorWALInfo& observed_predecessor_wal_info = {});
```

**Files:** `db/log_reader.h:64-70`

| Parameter | Purpose |
|-----------|---------|
| `file` | Sequential file reader for WAL |
| `reporter` | Callback for corruption reports |
| `checksum` | Verify CRC checksums |
| `log_num` | Expected log number (for recycled files) |
| `stop_replay_for_corruption` | Fail immediately on corruption vs. skip |

### Reading Records: `ReadRecord()`

**Files:** `db/log_reader.h:86-89`, `db/log_reader.cc`

```cpp
bool ReadRecord(Slice* record,
                std::string* scratch,
                WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords,
                uint64_t* record_checksum = nullptr);
```

#### Algorithm

1. **Read physical records** via `ReadPhysicalRecord()` until a complete logical record is assembled
2. **Handle fragmentation:**
   - `kFullType`: Return immediately
   - `kFirstType`: Start accumulating fragments
   - `kMiddleType`: Append to accumulator
   - `kLastType`: Finalize and return
3. **Validate checksums** (if `checksum=true`)
4. **Report corruption** via `Reporter::Corruption()` based on `WALRecoveryMode`

### Corruption Handling

**Files:** `db/log_reader.cc`, `db/log_reader.h:193-208`

The reader distinguishes several error types:

```cpp
enum : uint8_t {
  kEof = kMaxRecordType + 1,              // End of file
  kBadRecord = kMaxRecordType + 2,        // Invalid CRC or zero-length
  kBadHeader = kMaxRecordType + 3,        // Invalid header
  kOldRecord = kMaxRecordType + 4,        // Recycled log with old log number
  kBadRecordLen = kMaxRecordType + 5,     // Invalid length
  kBadRecordChecksum = kMaxRecordType + 6,// Checksum mismatch
};
```

⚠️ **INVARIANT:** When `recycle_log_files=true`, reader skips records with `log_number != expected_log_number` (treats as `kOldRecord`).

---

## 5. Sync Modes and Durability

### WriteOptions::sync

**Files:** `include/rocksdb/options.h:2330-2344`, `include/rocksdb/write_batch.h`

```cpp
struct WriteOptions {
  bool sync = false;  // fdatasync() after write (or fsync() if use_fsync=true)
  bool disableWAL = false;  // Skip WAL entirely (lose durability)
  ...
};
```

| Mode | Behavior | Durability | Performance |
|------|----------|------------|-------------|
| `sync=false` | Write to OS page cache, periodic background sync | Weak (survives process crash, not power loss) | Fast |
| `sync=true` | `fdatasync()` after each write (or `fsync()` if `DBOptions::use_fsync=true`) | Strong (survives power loss) | Slow (~1ms per write) |
| `disableWAL=true` | Skip WAL, only MemTable | None (data lost on crash before flush) | Fastest |

**Note:** `sync=true` combined with `disableWAL=true` is rejected with `Status::InvalidArgument("Sync writes has to enable WAL.")` (`db/db_impl/db_impl_write.cc:436-437`).

⚠️ **INVARIANT:** `disableWAL=true` is **incompatible** with `recycle_log_file_num > 0`, **except** when using two write queues with `disable_memtable=true` (internal 2PC split writes). Returns `Status::InvalidArgument()` otherwise.

**Files:** `HISTORY.md:526`

### manual_wal_flush

**Files:** `include/rocksdb/options.h:1474`

```cpp
struct DBOptions {
  bool manual_wal_flush = false;
};
```

When `manual_wal_flush=true`:
- Writes do **not** automatically flush the WAL buffer
- Application must call `DB::FlushWAL()` explicitly
- Reduces `write()` syscall overhead when batching many small writes

**Files:** `db/log_writer.h:149-150`, `db/log_writer.cc:181-183`

---

## 6. WAL Lifecycle

### Creation

WALs are created during:
1. **DB::Open:** Create initial WAL for the database
2. **MemTable switch:** Rotate to a new WAL when the active MemTable becomes immutable

**Files:** `db/db_impl/db_impl_open.cc`, `db/db_impl/db_impl_write.cc`

WAL files are named: `<wal_dir>/<log_number>.log`

- `log_number` is a monotonically increasing `uint64_t` from `VersionSet::next_file_number_`
- Example: `000123.log`

### Rotation

A new WAL is created when:
- **MemTable is full** and switches to immutable
- **Manual memtable switch** via `DB::Flush()`
- **`max_total_wal_size` exceeded** in multi-CF databases (forces `SwitchWAL()` to flush column families backed by the oldest live WAL)

**Files:** `db/db_impl/db_impl_write.cc` (SwitchMemtable logic)

### Archival

After a MemTable is flushed to L0 SST, its backing WAL can be archived:

**Files:** `db/wal_manager.cc:287-297`, `db/db_impl/db_impl_files.cc:657`

```cpp
void WalManager::ArchiveWALFile(const std::string& fname, uint64_t number);
```

- Archived WALs are moved to `<wal_dir>/archive/` directory
- Useful for backup tools, replication, debugging

⚠️ **INVARIANT:** A WAL can be archived only if `log_number < MinLogNumberToKeep()`, which is the minimum WAL number across all live MemTables and 2PC transactions.

**Files:** `db/db_impl/db_impl_open.cc:1177-1182`

### Deletion (Purging Obsolete WALs)

Archived WALs are deleted when:

**Files:** `db/wal_manager.cc:140`, `db/db_impl/db_impl_files.cc:723`

```cpp
void WalManager::PurgeObsoleteWALFiles();
```

#### Deletion Policy

Controlled by two options:

**Files:** `include/rocksdb/options.h:1058-1067`

```cpp
struct DBOptions {
  uint64_t WAL_ttl_seconds = 0;      // Delete archived WALs older than TTL
  uint64_t WAL_size_limit_MB = 0;    // Delete oldest WALs exceeding total size
};
```

| Condition | Action |
|-----------|--------|
| `WAL_ttl_seconds > 0` | Delete WALs older than TTL (checked every `TTL/2` seconds) |
| `WAL_size_limit_MB > 0` | Delete oldest WALs when approximate total archive size exceeds limit (approximated using max non-empty WAL size × file count; checked every 10 minutes) |
| Both set | Delete if **either** condition is met |
| Both unset (default) | Obsolete WALs are not archived; they are deleted directly from the live WAL directory |

**Files:** `db/wal_manager.cc:140-286`

---

## 7. WAL Recycling

**Files:** `include/rocksdb/options.h:976`

### Purpose

Avoid filesystem allocation overhead by **reusing deleted WAL files** instead of creating new ones.

```cpp
struct DBOptions {
  size_t recycle_log_file_num = 0;  // Max WALs to keep for recycling
};
```

### How It Works

1. When a WAL is no longer needed, instead of deleting it, it is kept for recycling
2. `DBImpl::CreateWAL()` calls `FileSystem::ReuseWritableFile(new_log_fname, old_log_fname, ...)` to reuse the file (`db/db_impl/db_impl_open.cc:2359-2367`)
3. **Recyclable record types** include a `log_number` field to distinguish old vs. new data

**Files:** `db/log_format.h:33-36`, `db/log_writer.cc:29-32`

⚠️ **INVARIANT:** Reader **must** skip records with `log_number != current_log_number` when `recycle_log_files=true`.

### Trade-offs

| Benefit | Cost |
|---------|------|
| Reduces file allocation latency | 4 extra bytes per record (log number) |
| Reduces filesystem metadata churn | Complexity in reader (ignore stale data) |

### Compatibility

⚠️ **WARNING:** `recycle_log_file_num > 0` is **incompatible** with `WriteOptions::disableWAL`, **except** when using two write queues with `disable_memtable=true` (internal 2PC split writes). Returns `Status::InvalidArgument()` otherwise.

**Files:** `HISTORY.md:526`

---

## 8. Crash Recovery

**Files:** `db/db_impl/db_impl_open.cc:1128-1800`

### Recovery Flow

During `DB::Open()`, if existing WAL files are found:

```
DB::Open()
  └─> DBImpl::Recover()
       └─> DBImpl::RecoverLogFiles()
            ├─ For each WAL file (in order):
            │   ├─ Open log::Reader
            │   ├─ ReadRecord() → WriteBatch
            │   ├─ WriteBatchInternal::InsertInto(memtable)
            │   └─ Update sequence number
            └─ Flush final memtable (if needed)
```

**Files:** `db/db_impl/db_impl_open.cc:1128-1800`

### Recovery Modes

**Files:** `include/rocksdb/options.h:414-451`

```cpp
enum class WALRecoveryMode : char {
  kTolerateCorruptedTailRecords = 0x00,  // Ignore incomplete tail
  kAbsoluteConsistency = 0x01,           // Fail on any corruption
  kPointInTimeRecovery = 0x02,           // Stop before corruption (default)
  kSkipAnyCorruptedRecords = 0x03,       // Skip all corrupted records
};
```

| Mode | Behavior | Use Case |
|------|----------|----------|
| `kTolerateCorruptedTailRecords` | Allow incomplete last record (crash mid-write), refuse to open DB if corruption detected | Applications requiring no rollback |
| `kAbsoluteConsistency` | Fail on **any** WAL corruption | Unit tests, high-consistency apps |
| `kPointInTimeRecovery` (default) | Stop replay at first corruption (valid point-in-time) | Production (disk controller cache) |
| `kSkipAnyCorruptedRecords` | Salvage as much data as possible, skip corrupted records | Disaster recovery |

**Files:** `include/rocksdb/options.h:414-451`

### Recovery and 2PC

**Files:** `include/rocksdb/options.h:1393-1395`

```cpp
struct DBOptions {
  bool allow_2pc = false;  // Enable two-phase commit recovery
};
```

When `allow_2pc=true`:
- Recovery retains **prepared transactions** (not yet committed) in memory
- Application must call `TransactionDB::GetTransactionByName()` to commit or rollback
- WALs backing prepared transactions **cannot be deleted** until commit/rollback

⚠️ **INVARIANT:** `MinLogNumberToKeep()` considers the minimum WAL number of **prepared transactions** when `allow_2pc=true`.

**Files:** `db/db_impl/db_impl_open.cc:1178-1182`

### WAL Verification

**Files:** `db/log_writer.h:163-164`, `db/log_reader.h:158-167`

When `Options::track_and_verify_wals=true`:
- Writer emits a `kPredecessorWALInfoType` record with the previous WAL's metadata
- Reader verifies the chain to detect missing WALs (WAL hole)

**Files:** `db/log_writer.cc:238-274`

---

## 9. WAL in 2PC Transactions

**Files:** `db/write_batch.cc:24-30`

### 2PC WAL Records

Two-phase commit transactions write special markers to the WAL:

```
WriteBatch record types for 2PC:
  kTypeBeginPrepareXID         // Start of prepare phase
  kTypeBeginPersistedPrepareXID // Start of persisted prepare (WritePrepared)
  kTypeBeginUnprepareXID       // Start of unprepare (WriteUnprepared)
  kTypeEndPrepareXID varstring // End of prepare (XID = transaction ID)
  kTypeCommitXID varstring     // Commit phase
  kTypeCommitXIDAndTimestamp varstring varstring // Commit with timestamp
  kTypeRollbackXID varstring   // Rollback phase
```

### Write Flow

1. **Prepare Phase:**
   ```
   Transaction::Prepare()
     └─> PrepareInternal()
          // WriteBatch was initialized with kTypeNoop at position 0.
          // MarkEndPrepare() rewrites the initial Noop into
          // kTypeBeginPrepareXID, kTypeBeginPersistedPrepareXID,
          // or kTypeBeginUnprepareXID (depending on write policy),
          // then appends kTypeEndPrepareXID(xid).
          WriteBatchInternal::MarkEndPrepare(batch, xid)
     └─> WriteImpl(batch, disableWAL=false, disable_memtable=true)
   ```

2. **Commit Phase:**
   ```
   Transaction::Commit()
     └─> CommitInternal()
          WriteBatchInternal::MarkCommit(batch, xid)
     └─> WriteImpl(batch, write_options_)  // Uses caller's write_options
   ```

**Note:** Neither prepare nor commit forces `sync=true`. The prepare phase forces `disableWAL=false` but uses the caller's `write_options_` for sync behavior. The commit phase also uses the caller's `write_options_` as-is.

### Recovery with 2PC

**Files:** `db/db_impl/db_impl_open.cc:1178-1182`

During recovery:
1. Replay `kTypeBeginPrepareXID` → store in `PreparedTransactions` map
2. Replay `kTypeCommitXID` → remove from `PreparedTransactions`
3. Replay `kTypeRollbackXID` → remove from `PreparedTransactions`
4. After recovery, remaining `PreparedTransactions` are **uncommitted** → application must resolve

⚠️ **INVARIANT:** WALs containing prepared but uncommitted transactions **cannot be deleted**. `MinLogNumberToKeep()` is the minimum across all prepared transaction WALs.

**Files:** `db/db_impl/db_impl_open.cc:1178-1182`

---

## 10. WAL Options Summary

**Files:** `include/rocksdb/options.h`

### DBOptions (WAL Lifecycle)

| Option | Default | Description |
|--------|---------|-------------|
| `wal_dir` | `""` (use `db_name`) | Directory for WAL files |
| `WAL_ttl_seconds` | `0` (disabled) | Delete archived WALs older than TTL |
| `WAL_size_limit_MB` | `0` (disabled) | Delete oldest archived WALs when approximate total size exceeds limit |
| `max_total_wal_size` | `0` (auto) | Force WAL rotation when total WAL size exceeds limit (multi-CF) |
| `recycle_log_file_num` | `0` (disabled) | Number of WALs to recycle instead of delete |
| `manual_wal_flush` | `false` | Manual WAL buffer flush via `FlushWAL()` |
| `wal_recovery_mode` | `kPointInTimeRecovery` | WAL corruption handling during recovery |
| `wal_compression` | `kNoCompression` | WAL record compression (only ZSTD supported) |
| `allow_2pc` | `false` | Enable two-phase commit recovery |
| `track_and_verify_wals_in_manifest` | `false` | Track synced WAL sizes in MANIFEST for verification |
| `avoid_flush_during_recovery` | `false` | Try to avoid flushing during WAL recovery |

### WriteOptions (Per-Write Durability)

| Option | Default | Description |
|--------|---------|-------------|
| `sync` | `false` | Call `fdatasync()` after write (or `fsync()` if `use_fsync=true`) |
| `disableWAL` | `false` | Skip WAL entirely (no durability) |

---

## 11. Key Invariants

⚠️ **INVARIANT:** Writer never leaves `< header_size` bytes in a block. Remaining space is zero-padded.
- **Files:** `db/log_writer.cc:113-133`

⚠️ **INVARIANT:** CRC checksum covers `RecordType + Payload` for legacy records, and `RecordType + LogNumber + Payload` for recyclable records (not the CRC field itself).
- **Files:** `db/log_writer.cc:323-340`

⚠️ **INVARIANT:** When `recycle_log_files=true`, reader skips records with `log_number != expected_log_number`.
- **Files:** `db/log_reader.cc` (ReadPhysicalRecord)

⚠️ **INVARIANT:** A WAL can be archived only if its log number is less than `MinLogNumberToKeep()`.
- **Files:** `db/db_impl/db_impl_open.cc:1177-1182`

⚠️ **INVARIANT:** `MinLogNumberToKeep()` considers:
  - Minimum WAL number of all live MemTables
  - Minimum WAL number of prepared 2PC transactions (if `allow_2pc=true`)
- **Files:** `db/db_impl/db_impl_open.cc:1177-1182`

⚠️ **INVARIANT:** `recycle_log_file_num > 0` is incompatible with `WriteOptions::disableWAL`, except when using two write queues with `disable_memtable=true` (internal 2PC split writes). Returns `Status::InvalidArgument()` otherwise.
- **Files:** `HISTORY.md:526`

⚠️ **INVARIANT:** Sequence numbers in a single WriteBatch form a contiguous range `[seq, seq + count)`.
- **Files:** `db/write_batch.cc:9-12`

⚠️ **INVARIANT:** Records with `type >= 10` and bit 7 set (`kRecordTypeSafeIgnoreMask = 0x80`) can be safely ignored by older readers for forward compatibility.
- **Files:** `db/log_format.h:50-51`

---

## 12. Code References

### Core Files

| File | Description |
|------|-------------|
| `db/log_format.h` | Record types, block size, header sizes |
| `db/log_writer.h` | Writer interface |
| `db/log_writer.cc` | Writer implementation (`AddRecord`, `EmitPhysicalRecord`) |
| `db/log_reader.h` | Reader interface, corruption reporting |
| `db/log_reader.cc` | Reader implementation (`ReadRecord`, `ReadPhysicalRecord`) |
| `db/write_batch.cc:432-503` | WriteBatch parser tag handling |
| `db/wal_manager.h` | WAL archival and purging |
| `db/wal_manager.cc` | `ArchiveWALFile`, `PurgeObsoleteWALFiles` |
| `db/db_impl/db_impl_open.cc:1128-1800` | Crash recovery (`RecoverLogFiles`) |
| `db/db_impl/db_impl_write.cc` | Write path integration |
| `include/rocksdb/options.h:414-451` | `WALRecoveryMode` enum |
| `include/rocksdb/options.h:976-1067` | WAL options (`recycle_log_file_num`, `WAL_ttl_seconds`, etc.) |

---

## 13. Diagram: Complete WAL Lifecycle

```
┌─────────────────────────────────────────────────────────────────┐
│  WRITE PATH                                                     │
│  DBImpl::WriteImpl                                              │
│    ├─ log::Writer::AddRecord(WriteBatch) ───────┐              │
│    └─ MemTable::Insert(WriteBatch)              │              │
└──────────────────────────────────────────────────┼──────────────┘
                                                   │
                                                   v
                                    ┌──────────────────────────┐
                                    │  ACTIVE WAL (000123.log) │
                                    │  - Receives new writes   │
                                    │  - Synced per WriteOpts  │
                                    └──────────┬───────────────┘
                                               │
                             MemTable full? ───┘
                                               │
                                               v
                                    ┌──────────────────────────┐
                                    │  ROTATION                │
                                    │  - MemTable → Immutable  │
                                    │  - Create new WAL        │
                                    │    (000124.log)          │
                                    └──────────┬───────────────┘
                                               │
                                               v
                                    ┌──────────────────────────┐
                                    │  OLD WAL (000123.log)    │
                                    │  - Backs immutable mem   │
                                    │  - Cannot delete yet     │
                                    └──────────┬───────────────┘
                                               │
                            Flush complete ────┘
                                               │
                                               v
                                    ┌──────────────────────────┐
                                    │  ARCHIVAL                │
                                    │  mv 000123.log           │
                                    │     archive/000123.log   │
                                    └──────────┬───────────────┘
                                               │
                  WAL_ttl_seconds elapsed OR ──┘
                  WAL_size_limit_MB exceeded
                                               │
                                               v
                                    ┌──────────────────────────┐
                                    │  DELETION / RECYCLING    │
                                    │  - Delete (default)      │
                                    │  - Or recycle if enabled │
                                    └──────────────────────────┘
```

---

## 14. Performance Considerations

### Write Amplification

Each write incurs:
- **1× WAL write** (append-only, sequential)
- **1× MemTable write** (in-memory insert)

WAL compression (`compression_type`) can reduce WAL size but adds CPU overhead.

### Sync Overhead

- `WriteOptions::sync=false`: ~10-100μs latency (page cache write)
- `WriteOptions::sync=true`: ~1-10ms latency (fsync to disk)

For high throughput, use **group commit** (default in RocksDB): multiple threads' writes are batched into a single WAL write + fsync.

**Files:** `db/db_impl/db_impl_write.cc` (WriteThread)

### Recycling vs. Allocation

Recycling WALs avoids:
- `open()` syscalls for new files
- Filesystem metadata updates
- Disk space allocation

The reuse is delegated to `FileSystem::ReuseWritableFile()`, so the exact mechanism depends on the filesystem implementation.

Trade-off: **4 bytes per record** overhead (log number field).

---

## 15. Common Pitfalls

1. **Forgetting `sync=true` for critical writes**
   - Without `sync`, data may be lost on power failure (only in OS page cache)
   - Use `sync=true` for transactional systems requiring durability

2. **Deleting WALs prematurely**
   - Ensure `WAL_ttl_seconds` and `WAL_size_limit_MB` account for backup windows
   - Replication tools may tail WALs in `archive/` directory

3. **Recycling with `disableWAL`**
   - `recycle_log_file_num > 0` and `WriteOptions::disableWAL=true` are generally incompatible (exception: internal 2PC split writes with two write queues)
   - Returns `Status::InvalidArgument()`

4. **Ignoring 2PC prepared transactions**
   - If `allow_2pc=true`, application **must** resolve prepared transactions after recovery
   - Failure to commit/rollback blocks WAL deletion indefinitely

5. **WAL directory on slow storage**
   - WAL is in the critical write path
   - Use fast SSD for `wal_dir` (separate from SST storage if needed)

---

## 16. Additional Features

### WAL Compression

WAL compression is controlled via `DBOptions::wal_compression` (`include/rocksdb/options.h:1476-1481`). Currently only ZSTD (`kZSTD`) is supported; unsupported types are sanitized to `kNoCompression` at open time (`db/db_impl/db_impl_open.cc:188-193`). When enabled, `CreateWAL()` writes a `kSetCompressionType` record before user data (`db/db_impl/db_impl_open.cc:2391`, `db/log_writer.cc:193-235`). The Writer constructor parameter is `CompressionType compressionType` (`db/log_writer.h:82-86`). Compressed WAL records are readable in RocksDB >= 7.4.0 regardless of the writer's setting.

### Option Sanitization at Open Time

During `DB::Open()`, several sanitizations affect WAL recycling (`db/db_impl/db_impl_open.cc:101-121`):
- WAL recycling is disabled when WAL archival retention is enabled (`WAL_ttl_seconds > 0` or `WAL_size_limit_MB > 0`)
- WAL recycling is disabled for `kTolerateCorruptedTailRecords` and `kAbsoluteConsistency` recovery modes

### track_and_verify_wals_in_manifest

When `DBOptions::track_and_verify_wals_in_manifest=true` (`include/rocksdb/options.h:660-674`), the log numbers and sizes of synced closed WALs are recorded in MANIFEST (`db/db_impl/db_impl.cc:1952-1957`). During `DB::Open()`, these are verified against actual WAL files on disk (`db/db_impl/db_impl_open.cc:761-779`). This is an additional protection against WAL corruption beyond the per-WAL-entry checksum.

### User-Defined Timestamp Size Records

The WAL format includes sideband records for user-defined timestamp sizes. Writers emit `kUserDefinedTimestampSizeType` / `kRecyclableUserDefinedTimestampSizeType` records (`db/log_writer.cc:276-306`) when column families use non-zero timestamp sizes. Readers track and validate these (`db/log_reader.cc:215-237`), and recovery can rebuild a `WriteBatch` to reconcile recorded timestamp sizes with the running CF configuration (`db/db_impl/db_impl_open.cc`, `util/udt_util.h`).

### Replication via GetUpdatesSince

Replication is supported through `DB::GetUpdatesSince(seq_number)` (`include/rocksdb/db.h:1880-1887`), which returns a `TransactionLogIterator` (`include/rocksdb/transaction_log.h`). This is implemented through `WalManager::GetUpdatesSince()` (`db/wal_manager.cc:103-129`). **Important:** `WAL_ttl_seconds` or `WAL_size_limit_MB` must be set to large values to retain WALs long enough for the iterator to read them.

### avoid_flush_during_recovery

When `DBOptions::avoid_flush_during_recovery=true` (`include/rocksdb/options.h:1415-1429`), RocksDB tries to avoid flushing during recovery, keeping recovered state backed by live WALs instead of flushing final memtables (`db/db_impl/db_impl_open.cc`). Note that `allow_2pc=true` forces this option off during sanitization (`db/db_impl/db_impl_open.cc:148-153`).

### Other Notes

- **Remote WAL:** Write WAL to remote storage (e.g., S3) for disaster recovery
- **Asynchronous WAL writes:** Decouple WAL write latency from user-facing latency (research)
- **WAL pipelining:** Overlap WAL write with MemTable insertion (requires careful ordering)
