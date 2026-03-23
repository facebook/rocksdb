# RocksDB Transaction Subsystem

This document provides a deep dive into RocksDB's transaction subsystem, which enables ACID-compliant multi-operation atomic updates with snapshot isolation.

## Overview

RocksDB supports two transaction models with three write policies:

**Transaction Models:**
- **Pessimistic Transactions** (`TransactionDB`): Lock-based concurrency control
- **Optimistic Transactions** (`OptimisticTransactionDB`): Validation-based concurrency control

**Write Policies (Pessimistic only):**
- **WriteCommitted**: Data written to memtable at commit time (default)
- **WritePrepared**: Data written at prepare time, commit writes marker only
- **WriteUnprepared**: Data written before prepare for large transactions

```
Transaction Subsystem
  |
  +-- TransactionDB (Pessimistic)
  |     +-- WriteCommitted
  |     +-- WritePrepared
  |     +-- WriteUnprepared
  |
  +-- OptimisticTxnDB (No locking)
        +-- OptimisticTxn
```

**Key Files:**
- `include/rocksdb/utilities/transaction.h` - Transaction API
- `include/rocksdb/utilities/transaction_db.h` - TransactionDB API and TransactionOptions
- `include/rocksdb/utilities/optimistic_transaction_db.h` - OptimisticTransactionDB API and OptimisticTransactionOptions
- `utilities/transactions/pessimistic_transaction_db.{h,cc}` - Pessimistic TransactionDB implementation
- `utilities/transactions/optimistic_transaction_db_impl.{h,cc}` - Optimistic TransactionDB implementation
- `utilities/transactions/pessimistic_transaction.{h,cc}` - Lock-based transactions
- `utilities/transactions/optimistic_transaction.{h,cc}` - Validation-based transactions
- `utilities/transactions/write_prepared_txn.{h,cc}` - WritePrepared implementation
- `utilities/transactions/write_unprepared_txn.{h,cc}` - WriteUnprepared implementation
- `utilities/transactions/lock/point/point_lock_manager.{h,cc}` - Point lock manager
- `utilities/transactions/lock/range/range_tree/` - Range lock manager

---

## Transaction Types

### Pessimistic Transactions

**File:** `utilities/transactions/pessimistic_transaction.h`

Pessimistic transactions acquire locks during execution to prevent conflicts. When a transaction performs `Put()`, `Delete()`, or `GetForUpdate()`, locks are acquired immediately.

**Design:**
- Locks acquired before commit prevent conflicting operations
- Blocking: threads wait if lock unavailable
- Deadlock detection available
- Three write policies supported

**Lock Types:**
- **Shared locks**: Multiple readers can hold simultaneously
- **Exclusive locks**: Single writer, no concurrent readers/writers

**Example Usage:**
```cpp
TransactionDB* txn_db;
TransactionDBOptions txn_db_options;
Options options;
Status s = TransactionDB::Open(options, txn_db_options, path, &txn_db);

Transaction* txn = txn_db->BeginTransaction(write_options);
txn->Put("key1", "value1");  // Acquires exclusive lock on "key1"
txn->Get(read_options, "key2", &value);  // Reads without locking
txn->GetForUpdate(read_options, "key3", &value);  // Acquires lock for later write
s = txn->Commit();  // Releases all locks
delete txn;
```

⚠️ **INVARIANT:** Transactions are NOT thread-safe. Each transaction must be accessed by a single thread at a time.

⚠️ **INVARIANT:** Locks must be acquired in a consistent order across transactions to avoid deadlocks (unless deadlock detection is enabled).

### Optimistic Transactions

**File:** `utilities/transactions/optimistic_transaction.h`

Optimistic transactions do not acquire locks during execution. Instead, they validate at commit time that no conflicts occurred.

**Design:**
- No locks during execution → lower overhead
- Validation at commit checks for write-write conflicts
- Returns `Status::Busy()` on conflict (caller must retry)
- Best for workloads with infrequent conflicts

**Conflict Detection:**
At commit time, the transaction checks if any key it wrote was modified by another transaction since the snapshot was taken. Implementation uses `tracked_locks_` (a `LockTracker`) and sequence number comparison via `TransactionUtil::CheckKeysForConflicts()`.

**Example Usage:**
```cpp
OptimisticTransactionDB* txn_db;
Status s = OptimisticTransactionDB::Open(options, path, &txn_db);

Transaction* txn = txn_db->BeginTransaction(write_options);
txn->Put("key1", "value1");  // No lock acquired
s = txn->Commit();  // Validates no conflicts occurred
if (s.IsBusy()) {
  // Conflict detected, retry transaction
  delete txn;
  txn = txn_db->BeginTransaction(write_options);
  // ... retry logic
}
delete txn;
```

Note: `OptimisticTransactionDB::BeginTransaction()` takes `OptimisticTransactionOptions` (not `TransactionOptions`). This struct has `set_snapshot` and `cmp` fields. The validation policy is configured via `OptimisticTransactionDBOptions::validate_policy` (`kValidateSerial` or `kValidateParallel`).

⚠️ **INVARIANT:** Optimistic transactions require that all writes go through the transaction API. Direct writes to the underlying DB can bypass conflict detection.

---

## Write Policies (Pessimistic Transactions)

### WriteCommitted (Default)

**File:** `utilities/transactions/pessimistic_transaction_db.h` (class `WriteCommittedTxnDB`)

In WriteCommitted mode, transaction data is buffered in memory and written to the memtable only at `Commit()` time.

**Properties:**
- Uncommitted data never visible to other transactions
- Simplest and most mature policy
- Supports all features including user-defined timestamps
- Higher memory usage for large transactions

**Commit Flow:**
```
Put("k1", "v1") → WriteBatch (in memory)
Put("k2", "v2") → WriteBatch (in memory)
Commit()        → Write entire batch to WAL + Memtable atomically
```

⚠️ **INVARIANT:** The transaction's WriteBatch is only written to the memtable during `Commit()`. Rollback is trivial (discard the batch).

### WritePrepared

**File:** `utilities/transactions/write_prepared_txn.{h,cc}`

WritePrepared writes data to the memtable at `Prepare()` time, while `Commit()` only writes a commit marker to the WAL.

**Design Motivation:**
- Reduces commit latency (no data write during commit)
- Enables parallel writes in prepare phase
- Requires additional bookkeeping for visibility

**Key Data Structures:**

1. **PreparedHeap** (`utilities/transactions/write_prepared_txn_db.h`, class `PreparedHeap`):
   - Deque-based structure with an auxiliary min-heap for amortized O(1) erase
   - Tracks prepared but not-yet-committed sequence numbers
   - `top()` returns the smallest prepared sequence number
   - Stored in `prepared_txns_` member of `WritePreparedTxnDB`

2. **CommitCache** (`utilities/transactions/write_prepared_txn_db.h`, member `commit_cache_`):
   - Array of `std::atomic<CommitEntry64b>` indexed by `prep_seq % COMMIT_CACHE_SIZE`
   - Each `CommitEntry` stores `{prep_seq, commit_seq}` pair
   - Fast lookup to determine if a prepared entry is committed
   - Evicts old entries when slot is reused by a newer transaction

3. **old_commit_map_** (`utilities/transactions/write_prepared_txn_db.h`):
   - Maps `snapshot_seq -> vector<prep_seq>`: for each snapshot, stores prep_seqs that committed between `max_evicted_seq_` and `snapshot_seq`
   - Fallback for commit entries evicted from CommitCache that overlap with live snapshots
   - If snapshot is no longer found in old_commit_map_, the snapshot is considered released (sets `snap_released=true`)

**Write Flow:**
```
Prepare():
  1. Write data to WAL with kTypeBeginPersistedPrepareXID marker
     (distinct from WriteCommitted's kTypeBeginPrepareXID)
  2. Apply data to memtable
  3. Add prepare_seq to PreparedHeap (prepared_txns_)
  4. Write kTypeEndPrepareXID marker

Commit():
  1. Write kTypeCommitXID marker to WAL
  2. Add (prepare_seq → commit_seq) to CommitCache
  3. Remove prepare_seq from PreparedHeap
```

**Visibility Rules:**
```cpp
// Simplified logic of WritePreparedTxnDB::IsInSnapshot()
// Returns true if the transaction that wrote prep_seq is committed
// and visible at snapshot_seq.
bool IsInSnapshot(uint64_t prep_seq, uint64_t snapshot_seq,
                  uint64_t min_uncommitted, bool* snap_released) {
  if (snapshot_seq < prep_seq) return false;  // Written after snapshot
  if (prep_seq < min_uncommitted) return true;  // Known committed

  // Check commit cache (array indexed by prep_seq % COMMIT_CACHE_SIZE)
  CommitEntry cached;
  if (GetCommitEntry(prep_seq % COMMIT_CACHE_SIZE, &cached) &&
      cached.prep_seq == prep_seq) {
    return cached.commit_seq <= snapshot_seq;
  }

  // If prep_seq > max_evicted_seq_, it's not in cache and not evicted,
  // so it must still be prepared (not yet committed)
  if (max_evicted_seq_ < prep_seq) return false;

  // Check delayed_prepared_ set (for txns moved from PreparedHeap)
  // If found there and not in delayed_prepared_commits_, still uncommitted

  // Finally check old_commit_map_ for evicted entries overlapping snapshots
  // If snapshot not found in old_commit_map_, snapshot was released:
  //   sets *snap_released = true, returns true
  // If found, checks if prep_seq is in the snapshot's prep set
}
```

⚠️ **INVARIANT:** `prepare_seq < commit_seq` always. The commit sequence number is always strictly greater than the prepare sequence number.

⚠️ **INVARIANT:** If a snapshot has been released (evicted from old_commit_map_), `IsInSnapshot` sets `*snap_released = true` and returns `true`. The caller (e.g., compaction) must handle this case by re-checking with a newer snapshot.

**Optimization: Dual Write Queues**
- Main queue: processes Prepare() writes
- Commit queue: processes Commit() markers in parallel
- Allows prepare and commit to overlap

### WriteUnprepared

**File:** `utilities/transactions/write_unprepared_txn.{h,cc}`

WriteUnprepared allows transactions to write data to the database before `Prepare()` is called. This is critical for large transactions that exceed memory limits.

**Design Motivation:**
- Large transactions can flush data to DB incrementally
- Avoids buffering entire transaction in memory
- Requires complex visibility tracking

**Key Data Structures:**

**`unprep_seqs_`** (`std::map<SequenceNumber, size_t>`):
- Maps each unprepared batch's sequence number to its sub-batch count
- Updated each time transaction data is flushed to DB via `FlushWriteBatchToDB()`
- Used during recovery to identify uncommitted data and for visibility tracking

**Write Flow:**
```
Put("k1", "v1"):
  if (write_batch_size > threshold):
    FlushWriteBatchToDBImpl():
      1. Write batch to WAL with kTypeBeginUnprepareXID marker
         (marks it as unprepared data, distinct from prepared batches)
      2. Apply to memtable
      3. Record sequence number and batch count in unprep_seqs_
      4. Create new WriteBatch for subsequent writes

Prepare():
  1. Flush remaining batch (if any) with kTypeBeginPersistedPrepareXID
  2. Write kTypeEndPrepareXID marker

Commit():
  1. Write kTypeCommitXID marker (contains only the transaction name/XID)
  2. Pre-release callback updates CommitCache with all unprep_seqs_ entries
  3. Release locks
```

**Visibility:**
WriteUnprepared data is visible to the transaction itself but hidden from other transactions using a special snapshot mechanism:
- Transaction's own read snapshot includes its unprepared writes
- External readers skip unprepared entries using `unprep_seqs_` tracking

**Recovery:**
During recovery (`utilities/transactions/pessimistic_transaction_db.cc:124-178`):
1. Scan WAL for unprepared writes and prepare markers
2. Recreate transaction object with `unprep_seqs_` populated
3. Application responsible for committing or rolling back recovered transactions

⚠️ **INVARIANT:** All unprepared sequence numbers must be tracked in `unprep_seqs_` until commit/rollback. Loss of this tracking would make data permanently visible or lost.

**SavePoint Complexity:**
WriteUnprepared has the most complex savepoint handling because savepoints may span both flushed (in DB) and unflushed (in batch) data.

Structure from `write_unprepared_txn.h:258`:
```cpp
struct SavePoint {
  // Record of unprep_seqs_ at this savepoint
  std::map<SequenceNumber, size_t> unprep_seqs_;

  // Snapshot used to read keys at this savepoint during RollbackToSavePoint
  std::unique_ptr<ManagedSnapshot> snapshot_;
};
```

WriteUnpreparedTxn maintains three separate data structures for savepoints:
1. `TransactionBaseImpl::save_points_` - tracks which keys modified in each savepoint
2. `WriteUnpreparedTxn::flushed_save_points_` - savepoints on already-flushed batches (with snapshot and unprep_seqs)
3. `WriteUnpreparedTxn::unflushed_save_points_` - savepoints on current in-memory write_batch_

Rollback to savepoint must:
1. Rollback unflushed data (discard WriteBatch entries)
2. Write rollback keys for flushed data using `WriteRollbackKeys()`
3. Restore snapshot and `unprep_seqs_` state

---

## Lock Management

### Point Lock Manager

**File:** `utilities/transactions/lock/point/point_lock_manager.{h,cc}`

The Point Lock Manager handles locks on individual keys using a striped hash table design.

**Architecture:**
```
PointLockManager
  +-- lock_maps_cache_ (thread-local cache of lock_maps_)
  +-- lock_maps_ : UnorderedMap<ColumnFamilyId, shared_ptr<LockMap>>
        +-- LockMap (per column family)
              +-- stripes[num_stripes]  (key hashed to stripe)
                    +-- LockMapStripe
                          +-- map<key, LockInfo>
```

**Striping:**
- `num_stripes` (default 16): number of lock map stripes
- Key hashed to determine stripe: `stripe = Hash(key) % num_stripes`
- Reduces contention by partitioning lock table

**LockInfo Structure:**
From `utilities/transactions/lock/point/point_lock_manager.cc`:
```cpp
struct LockInfo {
  bool exclusive;  // True if exclusive lock, false if shared
  autovector<TransactionID> txn_ids;  // Transactions holding lock
  uint64_t expiration_time;  // Lock invalid after this time (us)
  std::unique_ptr<std::list<KeyLockWaiter*>> waiter_queue;  // Waiting transactions
};
```

`KeyLockWaiter` (same file) contains a `TransactionDBCondVar` for blocking, the waiter's `TransactionID`, and lock type (`exclusive`).

**Lock Acquisition:**
```
AcquireLocked():
  1. Hash key to determine stripe
  2. Acquire stripe mutex
  3. Check if lock is available:
     - SHARED: granted if no exclusive lock held
     - EXCLUSIVE: granted if no locks held
  4. If available:
     - Add txn_id to granted set
     - Return OK
  5. If not available:
     - Add to waiting_queue
     - Release mutex and wait on condition variable
     - When woken, recheck availability (may loop)
```

⚠️ **INVARIANT:** Lock upgrades (shared to exclusive) and downgrades (exclusive to shared) are supported in-place. The lock manager handles upgrade/downgrade without requiring the caller to release and reacquire. However, lock upgrade attempts may still result in deadlock if two transactions both hold shared locks and try to upgrade simultaneously.

**Lock Release:**
```
UnLock():
  1. Acquire stripe mutex
  2. Remove txn_id from granted set
  3. If waiting_queue not empty:
     - Check if next waiter can be granted
     - Wake waiting transaction
  4. Release mutex
```

**Thread-Local Cache Optimization:**
Lock map lookups are cached in thread-local storage (`lock_maps_cache_`) to avoid repeated hash computations and stripe lookups.

### Range Lock Manager

**File:** `utilities/transactions/lock/range/range_tree/*`

Range locks allow locking key ranges `[start_key, end_key)` rather than individual points. Useful for range queries and preventing phantom reads.

**Design:**
- Locks represented using interval tree data structures in `range_tree/`
- Endpoints tracked to detect overlaps
- Lock escalation support to prevent memory exhaustion

**Lock Escalation:**
When too many range locks are held, escalate to coarser-grained locks:
```
[a, b), [b, c), [c, d) → [a, d)
```

Range lock escalation is triggered by memory limits (configured via `LockManager::SetMaxLockMemory()`), not by `max_num_locks`. The escalation count is tracked in `TransactionDB::LockManager::Counters::escalation_count`.

Note: `TransactionDBOptions::max_num_locks` applies to the **point lock manager** only. If the number of point-locked keys exceeds `max_num_locks`, lock acquisition returns `Status::Busy()`. Default is -1 (no limit).

⚠️ **INVARIANT:** Range locks must maintain non-overlapping invariant for exclusive locks. Overlapping exclusive ranges cause conflict.

### Deadlock Detection

**File:** `utilities/transactions/lock/point/point_lock_manager.h:31-101, 191-198`

Deadlock detection identifies circular wait dependencies among transactions.

**Data Structures:**

1. **DeadlockInfoBuffer** (circular buffer):
   - Stores recent deadlock incidents
   - Limited size (default 5 entries)
   - Used for debugging and monitoring

2. **wait_txn_map_** (`HashMap<TransactionID, TrackedTrxInfo>`):
   - Maps each waiting transaction to a `TrackedTrxInfo` containing the blocking transaction IDs (`m_neighbors`), column family, exclusivity flag, and waiting key

3. **rev_wait_txn_map_** (`HashMap<TransactionID, int>`):
   - Maps each blocking transaction to the count of transactions waiting on it
   - Used for efficient cleanup when a transaction releases locks

**Cycle Detection Algorithm:**
```
IncrementWaiters(waiting_txn, wait_ids):
  1. Add wait_ids to wait_txn_map_[waiting_txn]
  2. Increment rev_wait_txn_map_ count for each blocking txn
  3. Perform DFS from waiting_txn:
     - Follow edges in wait_txn_map_
     - If cycle detected (revisit waiting_txn), deadlock found
     - Bounded by deadlock_detect_depth (default 50)
  4. If deadlock:
     - Record in DeadlockInfoBuffer
     - Call DecrementWaiters() to clean up
     - Return true (caller returns Status::Busy)
```

**Configuration (in `TransactionOptions`):**
- `deadlock_detect`: enable/disable detection (default `false`)
- `deadlock_detect_depth`: max DFS depth (default 50, prevents expensive searches)
- `deadlock_timeout_us`: microseconds to wait before performing deadlock detection (default 500)

⚠️ **INVARIANT:** Lock ordering must be respected to avoid internal deadlocks: `lock_map_mutex_` > stripe mutexes (ascending CF ID, ascending stripe order) > `wait_txn_map_mutex_`. Deadlock detection acquires `wait_txn_map_mutex_` after releasing stripe mutexes.

**Lock Timeout vs. Deadlock Detection:**
- Lock timeout: wait for `lock_timeout` ms, then return `Status::TimedOut()`
- Deadlock detection: immediate detection, return `Status::Busy()`
- Both can be enabled simultaneously (deadlock detection is faster)

---

## Two-Phase Commit (2PC)

**File:** `utilities/transactions/pessimistic_transaction.cc:590-678`

Two-Phase Commit enables distributed transaction coordination across multiple RocksDB instances or with external systems.

**Protocol:**

**Phase 1: Prepare**
```cpp
Status Transaction::Prepare() {
  1. Validate transaction is in STARTED state
  2. Write data to WAL with markers:
     - kTypeBeginPrepareXID (WriteCommitted) or
       kTypeBeginPersistedPrepareXID (WritePrepared/WriteUnprepared)
     - <transaction writes>
     - kTypeEndPrepareXID
  3. Mark log file containing prepare
  4. Transition state to PREPARED
  5. Return OK
}
```

**Phase 2: Commit**
```cpp
Status Transaction::Commit() {
  1. If not prepared, call Prepare() first
  2. Write commit marker to WAL:
     - kTypeCommitXID
  3. Apply to memtable (if WriteCommitted)
  4. Update commit cache (if WritePrepared)
  5. Release locks
  6. Transition state to COMMITTED
  7. Return OK
}
```

**WAL Structure for 2PC (WriteCommitted):**
```
WAL record layout:
  kTypeBeginPrepareXID("txn_name")
  kTypePut(cf, "key1", "value1")
  kTypeDelete(cf, "key2")
  kTypeEndPrepareXID("txn_name")
  ... other transactions ...
  kTypeCommitXID("txn_name")
```

⚠️ **INVARIANT:** Named transactions required for 2PC. Transaction name (XID) must be unique and set via `Transaction::SetName()` after `BeginTransaction()`.

⚠️ **INVARIANT:** Once `Prepare()` succeeds, the transaction MUST be either committed or rolled back. The WAL entry persists across restarts.

**Recovery:**
During DB open (`utilities/transactions/pessimistic_transaction_db.cc:124-178`):
1. Iterate recovered transactions from WAL replay
2. For each prepared transaction without corresponding commit:
   - Recreate `Transaction` object via `BeginTransaction()`
   - Rebuild from write batch and set state to PREPARED
   - Add to `transactions_` map (accessible via `GetTransactionByName()`)
3. Application calls `Commit()` or `Rollback()` on recovered transactions

**Example 2PC Usage:**
```cpp
// Coordinator
Transaction* txn = txn_db->BeginTransaction(write_options);
txn->SetName("dist_txn_123");  // Unique XID (required for 2PC)

txn->Put("key", "value");
Status s = txn->Prepare();  // Phase 1
if (s.ok()) {
  // Coordinate with other participants...
  s = txn->Commit();  // Phase 2
}

// Recovery after crash
TransactionDB* txn_db = ...;  // Open DB
Transaction* recovered = txn_db->GetTransactionByName("dist_txn_123");
if (recovered) {
  // Decision from coordinator log
  if (should_commit) {
    recovered->Commit();
  } else {
    recovered->Rollback();
  }
  delete recovered;
}
```

---

## Transaction API

**File:** `include/rocksdb/utilities/transaction.h`

### Lifecycle

**TransactionState enum** (`transaction.h:725-735`):
```
STARTED           → Initial state after BeginTransaction()
AWAITING_PREPARE  → Prepare() called, waiting for completion
PREPARED          → Prepare() succeeded, ready for commit
AWAITING_COMMIT   → Commit() called, waiting for completion
COMMITTED         → Transaction committed successfully
AWAITING_ROLLBACK → Rollback() called, waiting for completion
ROLLEDBACK        → Transaction rolled back
LOCKS_STOLEN      → Transaction expired, locks released
```

### Core Operations

**BeginTransaction:**
```cpp
Transaction* txn_db->BeginTransaction(
  const WriteOptions& write_options,
  const TransactionOptions& txn_options = TransactionOptions(),
  Transaction* old_txn = nullptr
);
```
- `old_txn`: reuse transaction object (clears state)
- `txn_options.set_snapshot`: equivalent to calling `SetSnapshot()` at creation
- `txn_options.expiration`: auto-expire timeout (milliseconds, default -1 = no expiration)
- `txn_options.deadlock_detect`: enable deadlock detection for this txn (default `false`)
- `txn_options.lock_timeout`: per-transaction lock wait timeout in ms (default -1, uses `TransactionDBOptions::transaction_lock_timeout`)
- `txn_options.skip_prepare`: if true, can commit without calling Prepare (default `true`)
- Transaction name for 2PC is set via `txn->SetName("xid")` after creation

**Put / Delete:**
```cpp
Status Put(ColumnFamilyHandle* cf, const Slice& key, const Slice& value);
Status Delete(ColumnFamilyHandle* cf, const Slice& key);
```
- Acquires exclusive lock (pessimistic) or tracks write (optimistic)
- Buffered in transaction's WriteBatch

**Get:**
```cpp
Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
           const Slice& key, std::string* value);
```
- Reads from transaction's WriteBatch first
- Falls back to DB snapshot
- No locks acquired

**GetForUpdate:**
```cpp
Status GetForUpdate(const ReadOptions& options, ColumnFamilyHandle* cf,
                    const Slice& key, std::string* value,
                    bool exclusive = true,
                    const bool do_validate = true);
```
- Acquires lock (shared or exclusive) before reading
- Prevents other transactions from modifying key
- `do_validate`: if true (default), validates snapshot to detect conflicts
- Use when read-modify-write atomicity required

⚠️ **INVARIANT:** `GetForUpdate()` must be used instead of `Get()` if the application will later write the key based on the read value. Otherwise, lost update anomaly can occur.

**Commit:**
```cpp
Status Commit();
```
- WriteCommitted: writes data to WAL and memtable
- WritePrepared/WriteUnprepared: writes commit marker
- Releases all locks
- Validates conflicts (optimistic transactions)

**Rollback:**
```cpp
Status Rollback();
```
- Discards uncommitted writes
- Releases all locks
- WriteUnprepared: marks flushed data as rolled back using tombstones

### SavePoints

**File:** `include/rocksdb/utilities/transaction.h:265-280`

SavePoints enable nested transaction semantics with partial rollback.

```cpp
txn->SetSavePoint();                    // Create savepoint
txn->Put("key1", "value1");
txn->Put("key2", "value2");
txn->RollbackToSavePoint();             // Undo key1, key2
txn->Put("key3", "value3");
txn->Commit();                          // Only key3 committed
```

**Implementation:**
SavePoints capture transaction state:
- WriteBatch size and content
- Locks held
- Snapshot (for WriteUnprepared)
- `unprep_seqs_` (for WriteUnprepared)

Rollback restores captured state and releases locks acquired after savepoint.

⚠️ **INVARIANT:** SavePoints must be rolled back in LIFO order. Rolling back an earlier savepoint implicitly rolls back all later savepoints.

---

## Snapshot Isolation

**File:** `utilities/transactions/transaction_base.cc:125` (SetSnapshot), `utilities/transactions/pessimistic_transaction.cc:1277` (ValidateSnapshot)

Snapshot isolation ensures transactions read a consistent view of the database as of a specific point in time.

### Setting Snapshots

**SetSnapshot:**
```cpp
txn->SetSnapshot();
```
- Captures current sequence number as read snapshot
- All reads see database state at this sequence
- Explicit control over snapshot timing

**SetSnapshotOnNextOperation:**
```cpp
txn->SetSnapshotOnNextOperation();
// Snapshot will be taken at the next write or GetForUpdate operation
// (regular Get() does NOT trigger snapshot creation)
```
- Delays snapshot until next write or `GetForUpdate()` operation
- `TransactionOptions::set_snapshot = true` is equivalent to calling `SetSnapshot()` at transaction creation (not `SetSnapshotOnNextOperation`)
- Reduces snapshot holding time
- Useful when transaction start time is unpredictable

### Snapshot Validation

**File:** `utilities/transactions/pessimistic_transaction.cc:1277`

Validation ensures no conflicting writes occurred since snapshot:

```cpp
// Simplified logic of ValidateSnapshot (pessimistic_transaction.cc:1277)
Status PessimisticTransaction::ValidateSnapshot(
    ColumnFamilyHandle* column_family, const Slice& key,
    SequenceNumber* tracked_at_seq) {
  SequenceNumber snap_seq = snapshot_->GetSequenceNumber();

  if (*tracked_at_seq <= snap_seq) {
    // Already validated at or before snapshot - no conflict possible
    return Status::OK();
  }

  *tracked_at_seq = snap_seq;

  // Delegates to TransactionUtil::CheckKeyForConflicts() which uses
  // GetLatestSequenceForKey() to check if the key was modified
  // after snap_seq. Returns Status::Busy() on conflict.
  return TransactionUtil::CheckKeyForConflicts(
      db_impl_, column_family, key.ToString(), snap_seq, ...);
}
```

⚠️ **INVARIANT:** WritePrepared transactions use `GetSnapshotForWriteConflictBoundary()` (which may differ from the latest memtable sequence) for snapshot consistency. When `last_seq_same_as_publish_seq_` is false, this accounts for out-of-order sequence number publishing.

---

## Conflict Detection

### Write-Write Conflicts

**Pessimistic:**
- Detected via lock acquisition
- Transaction blocks until lock available or timeout
- `Status::TimedOut()` or `Status::Busy()` (deadlock)

**Optimistic:**
- Detected at commit time
- Sequence number comparison for all written keys
- `Status::Busy()` on conflict (no retry loop built-in)

### Read-Write Conflicts

Transactions using snapshots can encounter read-write conflicts:

```
T1: SetSnapshot()                     [seq=100]
T2: Put("key", "new_value")           [seq=101, committed]
T1: Get("key") → "old_value"          [reads snapshot seq=100]
T1: Put("key", "derived_from_old")
T1: Commit()                          [conflict if validated]
```

**Prevention:**
Use `GetForUpdate()` to acquire lock and prevent concurrent modifications:
```cpp
txn->GetForUpdate(options, cf, "key", &value);  // Locks key
// ... compute new value based on old ...
txn->Put("key", new_value);
txn->Commit();
```

---

## Error Handling and Failure Modes

### Common Error Codes

**`Status::Busy()`:**
- Optimistic transaction conflict detected
- Deadlock detected (pessimistic)
- Application should retry transaction

**`Status::TimedOut()`:**
- Lock acquisition exceeded `lock_timeout`
- Indicates high contention or potential deadlock
- Retry or increase timeout

**`Status::Expired()`:**
- Transaction exceeded `expiration` time
- Locks have been stolen by the system
- Transaction state is `LOCKS_STOLEN`, cannot commit

**`Status::TryAgain()`:**
- WritePrepared: commit status unknown (cache evicted)
- Application must reseek/reread to retry

**`Status::TxnNotPrepared()`** (subcode of `InvalidArgument`):
- Attempted `Commit()` on a transaction with `skip_prepare=false` without calling `Prepare()` first
- Named transactions (those with a name set via `SetName()`) require `Prepare()` before `Commit()` unless `skip_prepare` is true (default)

### Recovery Guarantees

**WriteCommitted:**
- Atomic: transaction either fully committed or fully absent
- Uncommitted transactions lost on crash (not in WAL)

**WritePrepared/WriteUnprepared:**
- Prepared transactions survive crash
- Recovered transactions accessible via `GetTransactionByName()`
- Application responsible for commit/rollback decision

⚠️ **INVARIANT:** After `Prepare()` returns OK, the transaction MUST be committed or rolled back even after crash/restart. Abandoning prepared transactions can lead to permanent lock leaks or data inconsistency.

### Concurrency Bugs to Avoid

**1. Lost Update:**
```cpp
// WRONG: Read without lock, write based on read
txn->Get(options, cf, "counter", &value);
int new_value = atoi(value.c_str()) + 1;
txn->Put("counter", std::to_string(new_value));

// CORRECT: Lock during read
txn->GetForUpdate(options, cf, "counter", &value);
int new_value = atoi(value.c_str()) + 1;
txn->Put("counter", std::to_string(new_value));
```

**2. Deadlock (A-B-B-A):**
```cpp
// Transaction 1
txn1->Put("A", ...);  // Locks A
txn1->Put("B", ...);  // Waits for B (locked by txn2)

// Transaction 2
txn2->Put("B", ...);  // Locks B
txn2->Put("A", ...);  // Waits for A (locked by txn1)
// → Deadlock
```
**Solution:** Always acquire locks in consistent order (e.g., lexicographic) or enable deadlock detection.

**3. Snapshot Isolation Violation:**
```cpp
// WRONG: Forget to set snapshot
txn->Put("key1", "value1");
// ... time passes, other transactions commit ...
txn->Get(options, cf, "key2", &value);  // Reads latest, not snapshot!

// CORRECT: Explicit snapshot
txn->SetSnapshot();
txn->Put("key1", "value1");
txn->Get(options, cf, "key2", &value);  // Consistent with snapshot
```

---

## Performance Considerations

### Lock Granularity

**Point Locks:**
- Fine-grained: high concurrency
- High overhead: many lock objects

**Range Locks:**
- Coarse-grained: lower concurrency
- Lower overhead: fewer lock objects
- Essential for range queries (prevent phantom reads)

**Trade-off:**
Use point locks for key-value workloads, range locks for range queries.

### Lock Striping

Increasing `num_stripes` reduces contention but increases memory usage:
- Default: 16 stripes
- High concurrency workload: 64+ stripes
- Memory-constrained: 4-8 stripes

### WritePrepared vs. WriteCommitted

**WritePrepared Advantages:**
- Lower commit latency (no data write)
- Parallel prepare/commit

**WritePrepared Disadvantages:**
- Additional memory (PreparedHeap, CommitCache)
- Potential `Status::TryAgain()` on reads
- More complex implementation

**Recommendation:**
- Use WriteCommitted for simplicity (default)
- Use WritePrepared for low-latency commits in high-throughput scenarios
- Benchmark your workload to validate improvement

### WriteUnprepared Trade-offs

**Advantages:**
- Supports very large transactions (GBs of writes)
- No memory limit on transaction size

**Disadvantages:**
- Most complex implementation
- Higher overhead for tracking `unprep_seqs_`
- Rollback requires writing tombstones

**Recommendation:**
Only use WriteUnprepared if transaction size exceeds available memory.

### Optimistic vs. Pessimistic

**Optimistic:**
- Best for low-conflict workloads
- No lock overhead during execution
- Wasted work on conflicts (full retry)

**Pessimistic:**
- Best for high-conflict workloads
- Lock overhead on every write
- No wasted work (blocking prevents conflicts)

**Rule of Thumb:**
- Conflict rate < 10%: Optimistic likely faster
- Conflict rate > 50%: Pessimistic likely faster
- 10-50%: Benchmark both

---

## Advanced Topics

### Transaction Expiration

Transactions can be configured to auto-rollback after a timeout:

```cpp
TransactionOptions txn_options;
txn_options.expiration = 5000;  // 5 seconds
Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);
```

**Behavior:**
- After `expiration` ms, locks are released ("stolen")
- Transaction state transitions to `LOCKS_STOLEN`
- Subsequent operations return `Status::Expired()`
- Prevents long-running transactions from blocking others

⚠️ **INVARIANT:** Expired transactions cannot be committed. Application must detect `Status::Expired()` and retry from the beginning.

### Skip Concurrency Control

**File:** `utilities/transactions/pessimistic_transaction_db.cc:124-178`

During recovery, prepared transactions are recreated with `skip_concurrency_control=true`:

```cpp
TransactionOptions txn_options;
txn_options.skip_concurrency_control = true;
```

**Purpose:**
- Avoid deadlocks during recovery (locks already held from before crash)
- Allow recovery logic to commit/rollback without lock acquisition

⚠️ **INVARIANT:** `skip_concurrency_control` is ONLY safe during recovery when no other transactions are active. Using in normal operation breaks isolation guarantees.

### Custom Comparators

Transactions must be aware of custom comparators for range operations:

```cpp
// Custom reverse comparator
class ReverseComparator : public Comparator { ... };

TransactionDB* txn_db;
Options options;
options.comparator = new ReverseComparator();
TransactionDB::Open(options, txn_db_options, path, &txn_db);
```

⚠️ **INVARIANT:** Lock manager uses the comparator for key ordering. Changing the comparator invalidates lock ordering and can cause incorrect conflict detection.

### User-Defined Timestamps

**File:** `include/rocksdb/utilities/transaction.h` (methods), `utilities/transactions/pessimistic_transaction.cc` (validation)

WriteCommitted transactions support user-defined timestamps. Timestamps are set on the transaction object (not in `TransactionOptions`):

```cpp
Transaction* txn = txn_db->BeginTransaction(write_options);
// Set read timestamp for conflict validation
txn->SetReadTimestampForValidation(read_ts);
// Set commit timestamp before committing
txn->SetCommitTimestamp(commit_ts);
txn->Commit();
```

⚠️ **LIMITATION:** WritePrepared and WriteUnprepared do NOT support user-defined timestamps. Attempting to use them returns `Status::NotSupported()`.

---

## Testing Recommendations

### Unit Tests

Cover the following scenarios:
- Basic transaction lifecycle (begin, put, commit)
- Conflict detection (write-write, read-write)
- Lock acquisition and release
- Deadlock detection
- SavePoint rollback
- 2PC prepare/commit/recovery
- Snapshot isolation
- Transaction expiration

### Stress Tests

Use RocksDB's transaction stress test:
```bash
./transaction_test --gtest_filter=*StressTest*
```

Tests concurrent transactions with random operations to detect race conditions.

### Custom Workloads

Benchmark your specific workload:
- Measure transaction throughput (txn/sec)
- Measure latency (p50, p99, p999)
- Compare pessimistic vs. optimistic
- Compare WriteCommitted vs. WritePrepared

Use `db_bench` with transaction options:
```bash
./db_bench \
  --benchmarks=randomtransaction \
  --transaction_db  # Use TransactionDB (pessimistic)
  # OR --optimistic_transaction_db  # Use OptimisticTransactionDB
```

---

## Summary

RocksDB's transaction subsystem provides flexible ACID guarantees with multiple concurrency control strategies. Key design decisions:

| Aspect | Options | Use Case |
|--------|---------|----------|
| **Concurrency Control** | Pessimistic, Optimistic | High conflict → Pessimistic<br>Low conflict → Optimistic |
| **Write Policy** | WriteCommitted, WritePrepared, WriteUnprepared | Large txns → WriteUnprepared<br>Low latency → WritePrepared<br>Simplicity → WriteCommitted |
| **Lock Granularity** | Point, Range | KV workload → Point<br>Range queries → Range |
| **Deadlock Handling** | Detection, Timeout | High contention → Detection<br>Simple → Timeout |
| **2PC** | Enabled, Disabled | Distributed → Enabled<br>Single DB → Disabled |

**Critical Invariants:**
- Transactions are single-threaded (not thread-safe)
- Prepared transactions MUST be committed or rolled back
- `GetForUpdate()` required for read-modify-write atomicity
- WritePrepared requires `prepare_seq < commit_seq`
- Expired transactions cannot commit

**Performance Best Practices:**
- Set appropriate `num_stripes` for lock manager
- Use optimistic transactions for low-conflict workloads
- Enable deadlock detection for complex lock patterns
- Benchmark write policies for your workload
- Use WritePrepared for low-latency commits
- Use WriteUnprepared only for very large transactions

Refer to `include/rocksdb/utilities/transaction.h` for complete API documentation.
