# EventListener — Observability and Event Notification System

This document describes RocksDB's EventListener interface, which enables applications to observe database events such as flushes, compactions, file operations, and error conditions. EventListener is the primary mechanism for external monitoring, statistics collection, and custom automation logic.

## Table of Contents
1. [Overview](#overview)
2. [EventListener Interface](#eventlistener-interface)
3. [Event Types](#event-types)
4. [Multiple Listeners](#multiple-listeners)
5. [Threading Model](#threading-model)
6. [Implementation Patterns](#implementation-patterns)
7. [Code References](#code-references)

---

## Overview

EventListener provides callbacks for major database operations and state changes. Applications implement EventListener methods to:
- Monitor database health (background errors, stall conditions)
- Collect statistics (compaction metrics, file lifecycle)
- Trigger custom actions (backup on flush, alerting on errors)
- Integrate with external systems (logging services, monitoring dashboards)

**Key source file:** `include/rocksdb/listener.h`

Listeners are registered via `DBOptions::listeners`:
```cpp
Options options;
options.listeners.emplace_back(std::make_shared<MyListener>());
DB* db;
Status s = DB::Open(options, "/path/to/db", &db);
```

### ⚠️ INVARIANT: Critical Usage Rules

**1. No Blocking DB Operations from Callbacks**
- DO NOT call blocking writes (`DB::Put`, `DB::Write` with `no_slowdown=false`) from compaction-related callbacks (`OnCompactionCompleted`, `OnCompactionBegin`)
- Reason: Compaction is needed to resolve "writes stopped" conditions. Blocking writes from compaction callbacks can deadlock RocksDB
- Safe alternative: Use `WriteBatch` with `no_slowdown=true`, which can accumulate writes and return `Status::Incomplete` if the DB is stopped

**2. No Extended Execution**
- Callbacks must return quickly (sub-millisecond) to avoid blocking RocksDB background threads
- Long-running operations (network I/O, heavy computation) should be offloaded to separate threads

**3. No Exception Propagation**
- Exceptions MUST NOT escape callback implementations into RocksDB code
- RocksDB is not exception-safe; propagated exceptions cause undefined behavior including data loss and deadlocks

**4. No Reentrancy**
- DO NOT call `CompactRange` or similar operations from callbacks that wait for background workers
- Reason: Background workers may be occupied executing the callback

---

## EventListener Interface

EventListener is an abstract base class with virtual callback methods. All methods have no-op default implementations, so applications only override callbacks they need.

```cpp
class EventListener : public Customizable {
 public:
  // Flush events
  virtual void OnFlushBegin(DB* db, const FlushJobInfo& info);
  virtual void OnFlushCompleted(DB* db, const FlushJobInfo& info);
  virtual void OnManualFlushScheduled(DB* db, const std::vector<ManualFlushInfo>& info);

  // Compaction events
  virtual void OnCompactionBegin(DB* db, const CompactionJobInfo& info);
  virtual void OnCompactionCompleted(DB* db, const CompactionJobInfo& info);
  virtual void OnSubcompactionBegin(const SubcompactionJobInfo& info);
  virtual void OnSubcompactionCompleted(const SubcompactionJobInfo& info);

  // Table file lifecycle events
  virtual void OnTableFileCreationStarted(const TableFileCreationBriefInfo& info);
  virtual void OnTableFileCreated(const TableFileCreationInfo& info);
  virtual void OnTableFileDeleted(const TableFileDeletionInfo& info);

  // Blob file lifecycle events
  virtual void OnBlobFileCreationStarted(const BlobFileCreationBriefInfo& info);
  virtual void OnBlobFileCreated(const BlobFileCreationInfo& info);
  virtual void OnBlobFileDeleted(const BlobFileDeletionInfo& info);

  // MemTable events
  virtual void OnMemTableSealed(const MemTableInfo& info);

  // Column family events
  virtual void OnColumnFamilyHandleDeletionStarted(ColumnFamilyHandle* handle);

  // Error and stall events
  virtual void OnBackgroundError(BackgroundErrorReason reason, Status* bg_error);
  virtual void OnStallConditionsChanged(const WriteStallInfo& info);

  // External file ingestion
  virtual void OnExternalFileIngested(DB* db, const ExternalFileIngestionInfo& info);

  // Error recovery
  virtual void OnErrorRecoveryBegin(BackgroundErrorReason reason, Status bg_error, bool* auto_recovery);
  virtual void OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo& info);

  // File I/O events (opt-in via ShouldBeNotifiedOnFileIO)
  virtual void OnFileReadFinish(const FileOperationInfo& info);
  virtual void OnFileWriteFinish(const FileOperationInfo& info);
  virtual void OnFileFlushFinish(const FileOperationInfo& info);
  virtual void OnFileSyncFinish(const FileOperationInfo& info);
  virtual void OnFileRangeSyncFinish(const FileOperationInfo& info);
  virtual void OnFileTruncateFinish(const FileOperationInfo& info);
  virtual void OnFileCloseFinish(const FileOperationInfo& info);
  virtual bool ShouldBeNotifiedOnFileIO() { return false; }

  // I/O error events
  virtual void OnIOError(const IOErrorInfo& info);

  // Background job pressure monitoring (EXPERIMENTAL)
  virtual void OnBackgroundJobPressureChanged(DB* db, const BackgroundJobPressure& pressure);
};
```

**Implementation note:** Subclass `EventListener` and override only the methods you need. Use `override` keyword to catch API changes at compile time.

---

## Event Types

### OnFlushCompleted

**Triggered when:** A memtable flush completes successfully

**Thread context:** RocksDB background flush thread

**Locking:** Called WITHOUT `db_mutex_` held

**Info structure:** `FlushJobInfo`
```cpp
struct FlushJobInfo {
  uint32_t cf_id;                      // Column family ID
  std::string cf_name;                 // Column family name
  std::string file_path;               // Path to the newly created SST file
  uint64_t file_number;                // File number of the new SST
  uint64_t oldest_blob_file_number;    // Oldest blob file referenced (Integrated BlobDB)
  uint64_t thread_id;                  // Thread ID that completed the flush
  int job_id;                          // Unique job ID (within thread)
  bool triggered_writes_slowdown;      // True if L0 file count >= slowdown trigger
  bool triggered_writes_stop;          // True if L0 file count >= stop trigger
  SequenceNumber smallest_seqno;       // Smallest sequence number in flushed file
  SequenceNumber largest_seqno;        // Largest sequence number in flushed file
  TableProperties table_properties;    // Table properties of the flushed SST
  FlushReason flush_reason;            // Why flush was triggered
  CompressionType blob_compression_type; // Compression for blob files (if any)
  std::vector<BlobFileAdditionInfo> blob_file_addition_infos; // Blob files created
};
```

**Common use cases:**
- Trigger external backup after flush completes
- Monitor write stall conditions (`triggered_writes_slowdown`, `triggered_writes_stop`)
- Track SST file creation for external file management
- Collect statistics on flush frequency and reasons

**Implementation location:** `db/db_impl/db_impl_compaction_flush.cc:914` (`DBImpl::NotifyOnFlushCompleted`)

**Notification sequence:**
1. Flush job completes, writes to MANIFEST
2. DBImpl computes write stall flags from current state
3. `mutex_` is **unlocked**
4. All listeners called sequentially: `listener->OnFlushCompleted(this, *info)`
5. `mutex_` is **locked** again

### OnFlushBegin

**Triggered when:** RocksDB is about to start a flush job (after memtables are picked)

**Thread context:** RocksDB background flush thread

**Locking:** Called WITHOUT `db_mutex_` held

**Info structure:** `FlushJobInfo` (populated with job metadata, but no file info yet)

**Use cases:**
- Pre-flush logging or state preparation
- Coordinating with external systems before flush starts

**⚠️ INVARIANT:** This callback executes AFTER `FlushJob::PickMemTable()` to prevent snapshot races. The code explicitly orders `NotifyOnFlushBegin()` after memtable picking so that no new snapshot can be taken between the two operations.

### OnManualFlushScheduled

**Triggered when:** A manual flush is scheduled via `DB::Flush()`

**Thread context:** The thread calling the flush API (foreground thread)

**Info structure:** `std::vector<ManualFlushInfo>`
```cpp
struct ManualFlushInfo {
  uint32_t cf_id;                      // Column family ID
  std::string cf_name;                 // Column family name
  FlushReason flush_reason;            // Reason that triggered this manual flush
};
```

**Use cases:**
- Track manual flush requests from application
- Monitor flush API usage patterns
- Coordinate with external systems before manual flush begins

**⚠️ NOTE:** This callback is triggered by `DB::Flush()` (memtable flush), NOT by `DB::FlushWAL()` (WAL buffer flush). `FlushWAL()` has no listener notification. The vector contains multiple entries only when atomic flush is enabled with more than one column family. Otherwise, it always contains exactly one entry.

### OnCompactionCompleted

**Triggered when:** A compaction job completes (success or failure)

**Thread context:** RocksDB background compaction thread

**Locking:** Called WITHOUT `db_mutex_` held

**Info structure:** `CompactionJobInfo`
```cpp
struct CompactionJobInfo {
  uint32_t cf_id;                      // Column family ID
  std::string cf_name;                 // Column family name
  Status status;                       // Success or failure status
  uint64_t thread_id;                  // Thread ID
  int job_id;                          // Job ID (unique within thread)
  int num_l0_files;                    // Number of L0 files (see note below)
  int base_input_level;                // Input level (e.g., L0 or L1)
  int output_level;                    // Output level
  std::vector<std::string> input_files;      // Input SST file paths
  std::vector<CompactionFileInfo> input_file_infos;  // Input file metadata
  std::vector<std::string> output_files;     // Output SST file paths
  std::vector<CompactionFileInfo> output_file_infos; // Output file metadata
  TablePropertiesCollection table_properties; // Properties keyed by file path
  CompactionReason compaction_reason;  // Why compaction ran
  CompressionType compression;         // Compression used for output files
  CompactionJobStats stats;            // Detailed compaction statistics
  CompressionType blob_compression_type; // Blob file compression
  std::vector<BlobFileAdditionInfo> blob_file_addition_infos; // Blob files created
  std::vector<BlobFileGarbageInfo> blob_file_garbage_infos;   // Blob files marked for GC
  bool aborted;                        // True if compaction was aborted via AbortAllCompactions()
};
```

**⚠️ NOTE on `num_l0_files`:** This field contains different values depending on the callback:
- In `OnCompactionBegin`: number of L0 files BEFORE compaction starts
- In `OnCompactionCompleted`: number of L0 files AFTER compaction completes

**Common use cases:**
- Collect write amplification statistics (`stats.num_input_records`, `stats.num_output_records`)
- Monitor compaction backlog and effectiveness
- Track file lifecycle (which files were compacted away)
- Trigger external actions after compaction (e.g., tiered storage migration)

**Implementation location:** `db/db_impl/db_impl_compaction_flush.cc:1827` (`DBImpl::NotifyOnCompactionCompleted`)

**⚠️ INVARIANT:** `NotifyOnCompactionCompleted` is only called if `Compaction::ShouldNotifyOnCompactionCompleted()` returns true. This flag is a latch set by `NotifyOnCompactionBegin()` to ensure begin/completed counts stay matched. Since `NotifyOnCompactionBegin()` is called for all compaction types (FIFO deletion, FIFO trivial copy, trivial moves, and normal compactions), `OnCompactionCompleted` fires for all of them as well.

### OnCompactionBegin

**Triggered when:** Compaction is about to start (before inputs are read)

**Use cases:**
- Pre-compaction logging
- Reserving resources for compaction
- Coordinating with external systems

### OnSubcompactionBegin / OnSubcompactionCompleted

**Triggered when:** A compaction is divided into subcompactions (for parallelism)

**Context:**
- If compaction is split into 2 subcompactions: one `OnCompactionBegin`, then two `OnSubcompactionBegin` calls
- If compaction is NOT split: one `OnCompactionBegin`, then one `OnSubcompactionBegin`
- **Always** at least one `OnSubcompactionBegin` per compaction

**Info structure:** `SubcompactionJobInfo`
```cpp
struct SubcompactionJobInfo {
  uint32_t cf_id;
  std::string cf_name;
  Status status;
  uint64_t thread_id;
  int job_id;                          // Identifies the parent compaction
  int subcompaction_job_id;            // -1 if not a subcompaction, otherwise unique within job_id
  int base_input_level;
  int output_level;
  CompactionReason compaction_reason;
  CompressionType compression;
  CompactionJobStats stats;
  CompressionType blob_compression_type;
};
```

**⚠️ NOTE:** `table_properties` is NOT populated for subcompactions. Get it from the parent `OnCompactionBegin` or `OnCompactionCompleted` callback.

### OnTableFileCreationStarted / OnTableFileCreated

**Triggered when:** An SST file creation begins or completes

**Purpose:** External logging service integration (no DB pointer provided)

**Thread context:** Background flush or compaction thread, or recovery/repair thread (during `DB::Open()` or `RepairDB()`)

**Info structures:**
```cpp
struct TableFileCreationBriefInfo {
  std::string db_name;                 // Database name
  std::string cf_name;                 // Column family name
  std::string file_path;               // Path to the file being created
  int job_id;                          // Job ID (flush or compaction)
  TableFileCreationReason reason;      // kFlush, kCompaction, kRecovery, kMisc
};

struct TableFileCreationInfo : public TableFileCreationBriefInfo {
  uint64_t file_size;                  // Size of the created file
  TableProperties table_properties;    // Detailed table properties
  Status status;                       // Success or failure
  std::string file_checksum;           // Checksum of the file
  std::string file_checksum_func_name; // Checksum function name
};
```

**Use cases:**
- Log all SST file creations to external service
- Track file creation success/failure separately from flush/compaction events
- Monitor file checksums for integrity verification

**⚠️ NOTE:** Historically `OnTableFileCreated` was called only on success. Now it's called on both success and failure. Check `info.status` to determine outcome.

### OnTableFileDeleted

**Triggered when:** An SST file is deleted (after compaction or deletion)

**Info structure:** `TableFileDeletionInfo`
```cpp
struct TableFileDeletionInfo {
  std::string db_name;
  std::string file_path;               // Path to deleted file
  int job_id;                          // Job that deleted the file
  Status status;                       // Whether deletion succeeded
};
```

**Implementation location:** `db/event_helpers.cc:201` (`EventHelpers::LogAndNotifyTableFileDeletion`)

### OnBlobFileCreationStarted

**Triggered when:** A blob file creation is about to start (before actual file creation)

**Purpose:** External logging service integration

**Thread context:** Background flush or compaction thread, or recovery/repair thread (during `DB::Open()` or `RepairDB()`)

**Info structure:** `BlobFileCreationBriefInfo`
```cpp
struct BlobFileCreationBriefInfo : public FileCreationBriefInfo {
  std::string db_name;                 // Database name
  std::string cf_name;                 // Column family name
  std::string file_path;               // Path to the blob file being created
  int job_id;                          // Job ID (flush or compaction)
  BlobFileCreationReason reason;       // kFlush, kCompaction, kRecovery
};
```

**Use cases:**
- Pre-creation logging for blob files
- Track blob file creation lifecycle
- External monitoring integration

### OnBlobFileCreated

**Triggered when:** A blob file creation completes (success or failure)

**Thread context:** Background flush or compaction thread, or recovery/repair thread (during `DB::Open()` or `RepairDB()`)

**Info structure:** `BlobFileCreationInfo`
```cpp
struct BlobFileCreationInfo : public BlobFileCreationBriefInfo {
  uint64_t total_blob_count;           // Number of blobs in the file
  uint64_t total_blob_bytes;           // Total bytes in the file
  Status status;                       // Whether creation succeeded
  std::string file_checksum;           // Checksum of the blob file
  std::string file_checksum_func_name; // Checksum function name
};
```

**Use cases:**
- Monitor blob file creation success/failure
- Track blob file sizes and counts
- Verify blob file checksums

**⚠️ NOTE:** This callback is invoked on both success and failure. Check `info.status` to determine outcome.

### OnBlobFileDeleted

**Triggered when:** A blob file is deleted (after garbage collection or database cleanup)

**Thread context:** Background purge thread or user thread (e.g., during `ColumnFamilyHandleImpl` destruction when `job_id == 0`)

**Info structure:** `BlobFileDeletionInfo`
```cpp
struct BlobFileDeletionInfo : public FileDeletionInfo {
  std::string db_name;                 // Database name
  std::string file_path;               // Path to deleted blob file
  int job_id;                          // Job that deleted the file
  Status status;                       // Whether deletion succeeded
};
```

**Use cases:**
- Track blob file lifecycle
- Monitor garbage collection effectiveness
- External cleanup coordination

### OnMemTableSealed

**Triggered when:** A memtable is made immutable (before flush begins)

**Thread context:** Foreground write thread or background thread (depending on what triggered the seal)

**Locking:** Called WITHOUT `db_mutex_` held

**Info structure:** `MemTableInfo`
```cpp
struct MemTableInfo {
  std::string cf_name;                 // Column family name
  SequenceNumber first_seqno;          // First sequence number inserted
  SequenceNumber earliest_seqno;       // Smallest seqno that could be in this memtable
  uint64_t num_entries;                // Total entries
  uint64_t num_deletes;                // Number of deletions
  std::string newest_udt;              // Newest user-defined timestamp (if persist_user_defined_timestamps=false)
};
```

**Use cases:**
- Monitor memtable churn rate
- Track sequence number progression
- Trigger actions when memtable is sealed but not yet flushed

**⚠️ INVARIANT:** `earliest_seqno` is the smallest sequence number guaranteed to be in this memtable or later memtables. Any write with seqno >= `earliest_seqno` will be present in this memtable or a later one.

### OnColumnFamilyHandleDeletionStarted

**Triggered when:** A column family handle is about to be deleted via `DB::DestroyColumnFamilyHandle()`

**Thread context:** The thread calling `DestroyColumnFamilyHandle` (foreground thread)

**Signature:**
```cpp
virtual void OnColumnFamilyHandleDeletionStarted(ColumnFamilyHandle* handle);
```

**Use cases:**
- Cleanup external resources associated with a column family
- Log column family deletions
- Invalidate cached references to the column family

**⚠️ WARNING:** The `handle` pointer becomes a dangling pointer after the deletion completes. Do NOT store or dereference it outside this callback.

### OnStallConditionsChanged

**Triggered when:** Write stall conditions change (stalled, stopped, or resumed)

**Thread context:** Thread that triggered the superversion change (could be flush, compaction, or write thread)

**Locking:** Called WITHOUT `db_mutex_` held

**Info structure:** `WriteStallInfo`
```cpp
struct WriteStallInfo {
  std::string cf_name;
  struct {
    WriteStallCondition cur;           // Current stall condition
    WriteStallCondition prev;          // Previous stall condition
  } condition;
};

enum class WriteStallCondition {
  kNormal,                             // No write stall
  kDelayed,                            // Writes are being slowed down
  kStopped                             // Writes are completely stopped
};
```

**Common use cases:**
- Alert when database enters write-stall mode
- Monitor write backpressure in production
- Track how often and why stalls occur

**Trigger location:** `db/job_context.h:91` — notifications are queued in `JobContext` and dispatched during cleanup

### OnBackgroundError

**Triggered when:** A background operation (flush, compaction, MANIFEST write) or user-write path (write callback failure, memtable insertion failure) encounters an error

**Thread context:** The thread that encountered the error (flush, compaction, WAL sync, user write, or DB open thread)

**Locking:** Called WITHOUT `db_mutex_` held (mutex is explicitly unlocked before callback)

**Signature:**
```cpp
virtual void OnBackgroundError(BackgroundErrorReason reason, Status* bg_error);

enum class BackgroundErrorReason {
  kFlush,                              // Flush failed
  kCompaction,                         // Compaction failed
  kWriteCallback,                      // Write callback failed
  kMemTable,                           // MemTable operation failed
  kManifestWrite,                      // MANIFEST write failed
  kFlushNoWAL,                         // Flush failed (WAL was OK)
  kManifestWriteNoWAL,                 // MANIFEST write failed (WAL was OK)
  kAsyncFileOpen                       // Async file open failed
};
```

**Special behavior:** Listeners can attempt to **suppress** background errors by resetting `*bg_error` to `Status::OK()`. However, this is NOT a blanket guarantee. In some code paths (unrecoverable data-loss errors, retryable I/O errors), `ErrorHandler::SetBGError()` records the error in `bg_error_` and may set `is_db_stopped_` BEFORE listeners are notified. In those cases, clearing the callback argument does not undo the already-recorded background error. Suppression is only effective in the `HandleKnownErrors` path where listeners are consulted before the error is committed. Use with extreme caution.

**Use cases:**
- Alert on background errors
- Implement custom error recovery logic
- Decide whether to allow automatic recovery

**Implementation location:** `db/event_helpers.cc:51` (`EventHelpers::NotifyOnBackgroundError`)

**⚠️ INVARIANT:** The mutex is explicitly unlocked before calling listeners (lines 59-68):
```cpp
db_mutex->AssertHeld();
db_mutex->Unlock();  // Release lock while notifying
for (auto& listener : listeners) {
  listener->OnBackgroundError(reason, bg_error);
  bg_error->PermitUncheckedError();
  if (*auto_recovery) {
    listener->OnErrorRecoveryBegin(reason, *bg_error, auto_recovery);
  }
}
db_mutex->Lock();  // Re-acquire lock
```

### OnExternalFileIngested

**Triggered when:** An external SST file is ingested via `IngestExternalFile`

**Thread context:** The thread calling `IngestExternalFile` (foreground thread)

**Locking:** Runs on same thread as `IngestExternalFile`, so blocking this callback will block the ingestion call

**Info structure:** `ExternalFileIngestionInfo`
```cpp
struct ExternalFileIngestionInfo {
  std::string cf_name;                 // Column family name
  std::string external_file_path;      // Path to the file OUTSIDE the DB
  std::string internal_file_path;      // Path to the file INSIDE the DB (after ingestion)
  SequenceNumber global_seqno;         // Global sequence number assigned to keys
  TableProperties table_properties;    // Properties of the ingested file
};
```

**Use cases:**
- Log external file ingestions
- Track bulk load operations
- Monitor ingestion frequency and volume

### OnErrorRecoveryBegin

**Triggered when:** Automatic error recovery is about to start (for recoverable errors like `NoSpace`)

**Signature:**
```cpp
virtual void OnErrorRecoveryBegin(BackgroundErrorReason reason, Status bg_error, bool* auto_recovery);
```

**Special behavior:** Set `*auto_recovery = false` to suppress automatic recovery. The database will remain in read-only mode until `DB::Resume()` is called manually.

**Use cases:**
- Implement custom recovery logic
- Decide whether to allow auto-recovery based on error type
- Alert operators before recovery starts

### OnErrorRecoveryEnd

**Triggered when:** Error recovery completes (successfully or not)

**Info structure:** `BackgroundErrorRecoveryInfo`
```cpp
struct BackgroundErrorRecoveryInfo {
  Status old_bg_error;                 // The original error that triggered recovery
  Status new_bg_error;                 // Final status (Status::OK() if recovery succeeded)
};
```

**Use cases:**
- Alert on recovery success/failure
- Log recovery outcomes
- Trigger actions based on recovery result

**⚠️ NOTE:** `OnErrorRecoveryCompleted(Status old_bg_error)` is DEPRECATED. Use `OnErrorRecoveryEnd` instead.

### File I/O Events (Opt-in)

**Event methods:**
- `OnFileReadFinish(const FileOperationInfo& info)`
- `OnFileWriteFinish(const FileOperationInfo& info)`
- `OnFileSyncFinish(const FileOperationInfo& info)`
- `OnFileFlushFinish(const FileOperationInfo& info)`
- `OnFileRangeSyncFinish(const FileOperationInfo& info)`
- `OnFileTruncateFinish(const FileOperationInfo& info)`
- `OnFileCloseFinish(const FileOperationInfo& info)`

**Opt-in requirement:** Override `ShouldBeNotifiedOnFileIO()` to return `true`

**Info structure:** `FileOperationInfo`
```cpp
struct FileOperationInfo {
  FileOperationType type;              // kRead, kWrite, kSync, kFlush, kClose, etc.
  const std::string& path;             // File path
  Temperature temperature;             // File temperature hint (if available)
  uint64_t offset;                     // Offset (for read/write)
  size_t length;                       // Length (for read/write)
  Duration duration;                   // Operation duration (nanoseconds)
  const SystemTimePoint& start_ts;     // Start timestamp
  Status status;                       // Operation status
};
```

**Use cases:**
- Monitor I/O latency at the file operation level
- Track slow I/O operations
- Collect detailed I/O statistics

**⚠️ WARNING:** These callbacks are invoked on file read, write, flush, sync, range-sync, truncate, and close operations. Not all `FileOperationType` values (e.g., `kOpen`, `kVerify`) have corresponding finish callbacks. Additionally, some read error paths (e.g., `SequentialFileReader`) do not emit `OnIOError`. These callbacks can impact performance if not implemented carefully. Only enable if you need fine-grained I/O monitoring.

### OnIOError

**Triggered when:** An I/O error occurs during a file operation that has `OnIOError` instrumentation (not all operations emit this callback; see warning above)

**Opt-in requirement:** Override `ShouldBeNotifiedOnFileIO()` to return `true` for most I/O error paths. However, some internal paths (e.g., `VersionSet::Close()` manifest verification) invoke `OnIOError` on ALL registered listeners without checking `ShouldBeNotifiedOnFileIO()`.

**Thread context:** The thread that encountered the I/O error

**Info structure:** `IOErrorInfo`
```cpp
struct IOErrorInfo {
  IOStatus io_status;                  // The I/O error status
  FileOperationType operation;         // kRead, kWrite, kSync, kOpen, etc.
  std::string file_path;               // Path to the file where error occurred
  size_t length;                       // Length of the operation (for read/write)
  uint64_t offset;                     // Offset of the operation (for read/write)
};
```

**Use cases:**
- Monitor and alert on I/O errors
- Track I/O error patterns across different file types
- Collect detailed error context for debugging
- Implement custom I/O error handling logic

**⚠️ NOTE:** This requires `ShouldBeNotifiedOnFileIO()` to return `true`, which enables file I/O event notifications and may impact performance.

### OnBackgroundJobPressure (EXPERIMENTAL)

**Triggered when:** A flush or compaction background job completes

**Purpose:** Monitor background job scheduling pressure and write-stall proximity

**Thread context:** Background thread that completed the job, WITHOUT `db_mutex_` held

**Info structure:** `BackgroundJobPressure`
```cpp
struct BackgroundJobPressure {
  int compaction_scheduled;            // Pending compactions (LOW + BOTTOM)
  int compaction_running;              // Running compactions (LOW + BOTTOM)
  int compaction_low_scheduled;        // Pending LOW priority compactions
  int compaction_low_running;          // Running LOW priority compactions
  int compaction_bottom_scheduled;     // Pending BOTTOM priority compactions
  int compaction_bottom_running;       // Running BOTTOM priority compactions
  int flush_scheduled;                 // Pending flushes
  int flush_running;                   // Running flushes
  int write_stall_proximity_pct;       // How close to write stall (0=healthy, 100=at threshold, >100=stalling)
  bool compaction_speedup_active;      // Whether compaction speedup is active
};
```

**Use cases:**
- Monitor background job queue depth
- Predict write stalls before they occur (`write_stall_proximity_pct`)
- Adjust workload based on DB pressure

**⚠️ NOTE:** This API is EXPERIMENTAL and subject to change. Fires on EVERY job completion, even if values haven't changed from previous call.

---

## Multiple Listeners

RocksDB supports registering multiple listeners:
```cpp
Options options;
options.listeners.emplace_back(std::make_shared<ListenerA>());
options.listeners.emplace_back(std::make_shared<ListenerB>());
options.listeners.emplace_back(std::make_shared<ListenerC>());
```

### ⚠️ INVARIANT: Sequential Notification Order

Listeners are called **sequentially** in the order they were registered:
```cpp
for (const auto& listener : immutable_db_options_.listeners) {
  listener->OnFlushCompleted(this, *info);
}
```

**Implications:**
1. **No parallelism:** Listener A must complete before Listener B is called
2. **Blocking cascade:** If Listener A is slow, all subsequent listeners are delayed
3. **Deterministic order:** Notification order is always the same (registration order)

**Best practice:** If a listener needs to perform slow operations (network I/O, disk writes), offload the work to a separate thread and return quickly from the callback.

### Example: Multiple Listeners
```cpp
class MetricsListener : public EventListener {
 public:
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    metrics_collector_->RecordFlush(info.cf_name, info.table_properties.data_size);
    // Returns quickly — just updates in-memory counters
  }
};

class BackupListener : public EventListener {
 public:
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    // Offload to background thread
    backup_queue_->Enqueue([=]() {
      CreateBackup(info.file_path);  // Slow operation happens asynchronously
    });
    // Returns quickly — work is queued
  }
};

Options options;
options.listeners.emplace_back(std::make_shared<MetricsListener>());
options.listeners.emplace_back(std::make_shared<BackupListener>());
// MetricsListener is always called first, BackupListener second
```

---

## Threading Model

### ⚠️ INVARIANT: Thread Context Rules

**1. Listeners are called from the ACTUAL thread performing the operation:**
- `OnFlushCompleted` → RocksDB background flush thread
- `OnCompactionCompleted` → RocksDB background compaction thread
- `OnExternalFileIngested` → User's foreground thread (the one calling `IngestExternalFile`)
- `OnBackgroundError` → The thread that encountered the error

**2. Mutex is RELEASED before calling listeners:**
```cpp
mutex_.AssertHeld();
mutex_.Unlock();
for (const auto& listener : immutable_db_options_.listeners) {
  listener->OnFlushCompleted(this, *info);
}
mutex_.Lock();
```
This prevents deadlocks but means:
- **DB state can change during callback:** Another thread could start a new flush, change write stall state, etc.
- **Info struct may become stale:** Don't rely on `info` being accurate after callback returns
- **Copy data if needed:** If you need to use info outside the callback, make a copy

**3. No reentrancy guarantees:**
- Listeners can be called from multiple threads simultaneously
- Example: `OnFlushCompleted` from flush thread while `OnCompactionCompleted` runs on compaction thread
- **Thread safety requirement:** Listener implementations must be thread-safe if they modify shared state

### Thread Safety Example
```cpp
class StatisticsListener : public EventListener {
 public:
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);  // REQUIRED: protect shared state
    flush_count_++;
    total_flushed_bytes_ += info.table_properties.data_size;
  }

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);  // REQUIRED: same mutex
    compaction_count_++;
  }

 private:
  std::mutex mutex_;
  uint64_t flush_count_ = 0;
  uint64_t compaction_count_ = 0;
  uint64_t total_flushed_bytes_ = 0;
};
```

### Deadlock Prevention

**DO NOT** call blocking write operations or operations that wait for background workers from within listener callbacks. The `db_mutex_` is intentionally released before calling listeners, so simple read-only operations like `db->GetProperty()` are safe (they will acquire and release `db_mutex_` normally). The real deadlock risks are:
- Blocking writes (`DB::Put`, `DB::Write` with `no_slowdown=false`) from compaction callbacks — compaction is needed to resolve write stalls
- `CompactRange()` or similar operations from callbacks — they wait for background workers that may be occupied executing the callback

```cpp
// WRONG — DEADLOCK RISK
class BadListener : public EventListener {
  void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override {
    db->CompactRange(CompactRangeOptions(), nullptr, nullptr);  // Waits for background workers
    // Deadlock: background workers are occupied executing this callback
  }
};
```

**Correct approach:** Offload blocking operations to a separate thread.

---

## Implementation Patterns

### Pattern 1: Simple Logging Listener
```cpp
class LoggingListener : public EventListener {
 public:
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    LOG(INFO) << "Flush completed: cf=" << info.cf_name
              << " file=" << info.file_path
              << " size=" << info.table_properties.data_size;
  }

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override {
    LOG(INFO) << "Compaction completed: cf=" << info.cf_name
              << " reason=" << GetCompactionReasonString(info.compaction_reason)
              << " input_files=" << info.input_files.size()
              << " output_files=" << info.output_files.size();
  }
};
```

### Pattern 2: Metrics Collection Listener
```cpp
class MetricsListener : public EventListener {
 public:
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.flush_count++;
    stats_.total_flushed_bytes += info.table_properties.data_size;
    stats_.flush_reasons[info.flush_reason]++;
  }

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!info.status.ok()) {
      stats_.failed_compactions++;
      return;
    }
    stats_.compaction_count++;
    stats_.total_compacted_bytes += ComputeTotalSize(info.input_files, info.table_properties);
    stats_.write_amplification = ComputeWriteAmp(info.input_files, info.output_files);
  }

  Stats GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;  // Return by value to avoid race
  }

 private:
  mutable std::mutex mutex_;
  struct Stats {
    uint64_t flush_count = 0;
    uint64_t compaction_count = 0;
    uint64_t failed_compactions = 0;
    uint64_t total_flushed_bytes = 0;
    uint64_t total_compacted_bytes = 0;
    double write_amplification = 0.0;
    std::map<FlushReason, uint64_t> flush_reasons;
  } stats_;
};
```

### Pattern 3: Async Work Dispatcher
```cpp
class AsyncListener : public EventListener {
 public:
  AsyncListener() : shutdown_(false) {
    worker_thread_ = std::thread(&AsyncListener::ProcessQueue, this);
  }

  ~AsyncListener() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      shutdown_ = true;
    }
    cv_.notify_one();
    worker_thread_.join();
  }

  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    // Copy info — original may be destroyed after callback returns
    FlushJobInfo info_copy = info;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      work_queue_.push([this, info_copy]() {
        ProcessFlush(info_copy);  // Slow operation
      });
    }
    cv_.notify_one();
  }

 private:
  void ProcessQueue() {
    while (true) {
      std::function<void()> work;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !work_queue_.empty() || shutdown_; });
        if (shutdown_ && work_queue_.empty()) break;
        work = std::move(work_queue_.front());
        work_queue_.pop();
      }
      work();  // Execute outside lock
    }
  }

  void ProcessFlush(const FlushJobInfo& info) {
    // Slow operation: network I/O, disk writes, etc.
    BackupFile(info.file_path);
  }

  std::mutex mutex_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> work_queue_;
  std::thread worker_thread_;
  bool shutdown_;
};
```

### Pattern 4: Error Handling and Recovery
```cpp
class ErrorHandlingListener : public EventListener {
 public:
  void OnBackgroundError(BackgroundErrorReason reason, Status* bg_error) override {
    LOG(ERROR) << "Background error: reason=" << static_cast<int>(reason)
               << " status=" << bg_error->ToString();

    // Example: suppress NoSpace errors, allowing DB to continue in degraded mode
    if (bg_error->IsNoSpace() && reason == BackgroundErrorReason::kFlush) {
      LOG(WARNING) << "Suppressing NoSpace error for flush";
      *bg_error = Status::OK();  // Suppress error
      // NOTE: This is DANGEROUS. Only suppress if you have a recovery plan.
    }
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason reason, Status bg_error,
                            bool* auto_recovery) override {
    LOG(INFO) << "Error recovery starting: reason=" << static_cast<int>(reason);

    // Example: disable auto-recovery for certain errors
    if (reason == BackgroundErrorReason::kManifestWrite) {
      LOG(WARNING) << "Disabling auto-recovery for MANIFEST write error";
      *auto_recovery = false;  // Force manual recovery via DB::Resume()
    }
  }

  void OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo& info) override {
    if (info.new_bg_error.ok()) {
      LOG(INFO) << "Error recovery succeeded";
      alert_system_->SendAlert("RocksDB recovered from error");
    } else {
      LOG(ERROR) << "Error recovery failed: " << info.new_bg_error.ToString();
      alert_system_->SendCriticalAlert("RocksDB recovery failed!");
    }
  }

 private:
  AlertSystem* alert_system_;
};
```

### Pattern 5: Write Stall Monitoring
```cpp
class StallMonitor : public EventListener {
 public:
  void OnStallConditionsChanged(const WriteStallInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);

    LOG(INFO) << "Write stall changed: cf=" << info.cf_name
              << " prev=" << ConditionToString(info.condition.prev)
              << " cur=" << ConditionToString(info.condition.cur);

    if (info.condition.cur == WriteStallCondition::kStopped) {
      // CRITICAL: writes are completely stopped
      alert_system_->SendCriticalAlert("RocksDB writes stopped for " + info.cf_name);
      stall_start_time_ = std::chrono::steady_clock::now();
    } else if (info.condition.cur == WriteStallCondition::kDelayed) {
      // WARNING: writes are being slowed down
      alert_system_->SendWarning("RocksDB writes slowed for " + info.cf_name);
    } else if (info.condition.prev != WriteStallCondition::kNormal) {
      // Recovery: stall condition cleared
      auto duration = std::chrono::steady_clock::now() - stall_start_time_;
      LOG(INFO) << "Stall cleared after "
                << std::chrono::duration_cast<std::chrono::seconds>(duration).count()
                << " seconds";
    }
  }

 private:
  static std::string ConditionToString(WriteStallCondition cond) {
    switch (cond) {
      case WriteStallCondition::kNormal: return "Normal";
      case WriteStallCondition::kDelayed: return "Delayed";
      case WriteStallCondition::kStopped: return "Stopped";
    }
    return "Unknown";
  }

  std::mutex mutex_;
  AlertSystem* alert_system_;
  std::chrono::steady_clock::time_point stall_start_time_;
};
```

---

## Code References

| Component | File |
|-----------|------|
| EventListener interface | `include/rocksdb/listener.h` |
| Event helper utilities | `db/event_helpers.h`, `db/event_helpers.cc` |
| Flush notifications | `db/db_impl/db_impl_compaction_flush.cc:914` (`NotifyOnFlushCompleted`) |
| Compaction notifications | `db/db_impl/db_impl_compaction_flush.cc:1827` (`NotifyOnCompactionCompleted`) |
| Background error handling | `db/event_helpers.cc:51` (`NotifyOnBackgroundError`) |
| Stall condition notifications | `db/job_context.h:91` (queued in JobContext) |
| Listener test examples | `db/listener_test.cc` |
| Info structures | `include/rocksdb/listener.h:34-556` |
| CompactionJobStats | `include/rocksdb/compaction_job_stats.h` |

---

## Summary

EventListener provides a powerful mechanism for observing RocksDB internals without modifying the core codebase. Key principles:

1. **Thread awareness:** Callbacks run on RocksDB background threads; implement thread-safe listeners
2. **Non-blocking:** Return quickly from callbacks; offload slow work to separate threads
3. **No reentrancy:** Don't call blocking DB operations from callbacks
4. **Sequential notification:** Multiple listeners are called in registration order
5. **No exception propagation:** RocksDB is not exception-safe; catch all exceptions in listener code

Use EventListener for:
- External monitoring and alerting
- Statistics collection and analysis
- Custom automation (backups, tiered storage, etc.)
- Error recovery and resilience

For more context on background operations, see:
- `docs/components/flush.md` — Flush process and FlushJob
- `docs/components/compaction.md` — Compaction process and CompactionJob
- `ARCHITECTURE.md` — High-level database architecture
