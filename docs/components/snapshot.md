# Snapshot: Point-in-Time Database Views

**Owner:** RocksDB Team
**Code:** `db/snapshot_impl.{h,cc}`, `include/rocksdb/snapshot.h`

## Overview

A Snapshot represents a **point-in-time, immutable view** of the database. Snapshots enable consistent reads across multiple operations without blocking concurrent writes. They are implemented using **sequence numbers** and a **doubly-linked list** data structure.

**Key Concepts:**
- Snapshots are identified by a `SequenceNumber` (a monotonically increasing counter)
- Multiple snapshots can coexist, each preserving a different point in database history
- Snapshots are **immutable** and **thread-safe** for concurrent reads
- Snapshots **pin data in memory/disk** by preventing compaction from deleting old versions

---

## Public API

### Creating and Releasing Snapshots

**include/rocksdb/db.h:**
```cpp
// Create a snapshot of current database state
const Snapshot* DB::GetSnapshot();

// Release a previously acquired snapshot
void DB::ReleaseSnapshot(const Snapshot* snapshot);
```

**Usage Example:**
```cpp
// Take a snapshot
const Snapshot* snapshot = db->GetSnapshot();

// Use snapshot for consistent reads
ReadOptions ro;
ro.snapshot = snapshot;
std::string value;
db->Get(ro, "key", &value);

// Release when done
db->ReleaseSnapshot(snapshot);
```

**⚠️ INVARIANT:** The caller must **not** use a snapshot after calling `ReleaseSnapshot()`.

**⚠️ INVARIANT:** Returns `nullptr` if snapshots are not supported (e.g., when `inplace_update_support` is enabled, which modifies data in-place without versioning).

### RAII Helper: ManagedSnapshot

**include/rocksdb/snapshot.h:**
```cpp
class ManagedSnapshot {
 public:
  explicit ManagedSnapshot(DB* db);  // Acquires snapshot
  ManagedSnapshot(DB* db, const Snapshot* _snapshot);  // Takes ownership
  ~ManagedSnapshot();                 // Releases snapshot automatically
  const Snapshot* snapshot();
};
```

**Usage:**
```cpp
{
  ManagedSnapshot ms(db);  // Snapshot acquired
  ReadOptions ro;
  ro.snapshot = ms.snapshot();
  db->Get(ro, "key", &value);
}  // Snapshot automatically released
```

---

## Implementation: SnapshotImpl and SnapshotList

### SnapshotImpl Structure

**db/snapshot_impl.h:**
```cpp
class SnapshotImpl : public Snapshot {
 public:
  SequenceNumber number_;               // Snapshot's sequence number
  SequenceNumber min_uncommitted_;      // For WritePrepared transactions

  SequenceNumber GetSequenceNumber() const override { return number_; }
  int64_t GetUnixTime() const override { return unix_time_; }
  uint64_t GetTimestamp() const override { return timestamp_; }

 private:
  SnapshotImpl* prev_;                  // Previous in doubly-linked list
  SnapshotImpl* next_;                  // Next in doubly-linked list
  SnapshotList* list_;                  // Owning list (for sanity checks)
  int64_t unix_time_;                   // Wall-clock time when created
  uint64_t timestamp_;                  // User-defined timestamp
  bool is_write_conflict_boundary_;     // For transaction isolation
};
```

**Key Fields:**
- `number_`: The sequence number identifying this snapshot
- `min_uncommitted_`: Used by WritePrepared transactions to limit query scope
- `prev_/next_`: Pointers for doubly-linked list (ordered by sequence number)
- `is_write_conflict_boundary_`: Marks snapshots used for write-conflict checking in transactions

### SnapshotList: Doubly-Linked List Management

**Data Structure:**
```
list_ (dummy head, seqno=0xFFFFFFFFL)

  dummy <-> 10 <-> 15 <-> 20 <-> 30 <-> dummy
  (oldest)                        (newest)

  - Circular doubly-linked list
  - list_.next_ points to oldest snapshot
  - list_.prev_ points to newest snapshot
```

**Operations:**

**Creating a Snapshot (db/snapshot_impl.h:84):**
```cpp
SnapshotImpl* SnapshotList::New(SnapshotImpl* s, SequenceNumber seq, ...) {
  s->number_ = seq;
  s->unix_time_ = unix_time;
  s->timestamp_ = ts;
  s->is_write_conflict_boundary_ = is_write_conflict_boundary;
  s->list_ = this;

  // Insert at end (new snapshots have highest sequence numbers)
  s->next_ = &list_;
  s->prev_ = list_.prev_;
  s->prev_->next_ = s;
  s->next_->prev_ = s;

  count_++;
  return s;
}
```

**Deleting a Snapshot (db/snapshot_impl.h:101):**
```cpp
void SnapshotList::Delete(const SnapshotImpl* s) {
  assert(s->list_ == this);

  // Unlink from doubly-linked list
  s->prev_->next_ = s->next_;
  s->next_->prev_ = s->prev_;

  count_--;
  // Note: Does NOT free the object (caller's responsibility)
}
```

**⚠️ INVARIANT:** Snapshots in the list are ordered by **ascending sequence number**.

**⚠️ INVARIANT:** `list_.next_` always points to the **oldest** snapshot, `list_.prev_` to the **newest**.

**⚠️ INVARIANT:** All operations on `SnapshotList` must be protected by **DB mutex** (enforced in `GetSnapshotImpl` and `ReleaseSnapshot` in db/db_impl/db_impl.cc).

### Retrieving All Snapshots

**db/snapshot_impl.h:118-153:**
```cpp
void SnapshotList::GetAll(
    std::vector<SequenceNumber>* snap_vector,
    SequenceNumber* oldest_write_conflict_snapshot,
    const SequenceNumber& max_seq) const {
  std::vector<SequenceNumber>& ret = *snap_vector;

  if (oldest_write_conflict_snapshot != nullptr) {
    *oldest_write_conflict_snapshot = kMaxSequenceNumber;
  }

  if (empty()) return;

  const SnapshotImpl* s = &list_;
  while (s->next_ != &list_) {
    if (s->next_->number_ > max_seq) break;

    // Avoid duplicates (multiple snapshots may have same sequence number)
    if (ret.empty() || ret.back() != s->next_->number_) {
      ret.push_back(s->next_->number_);
    }

    // Track oldest write-conflict boundary for transactions
    if (oldest_write_conflict_snapshot != nullptr &&
        *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
        s->next_->is_write_conflict_boundary_) {
      // If this is the first write-conflict boundary snapshot in the list,
      // it is the oldest
      *oldest_write_conflict_snapshot = s->next_->number_;
    }

    s = s->next_;
  }
}
```

---

## How Snapshots Affect Reads

### Sequence Number Visibility Filter

When a snapshot is used in a read operation, only data with **sequence numbers ≤ snapshot's sequence number** is visible.

**DBIter Constructor (db/db_iter.cc:40-46):**
```cpp
DBIter::DBIter(Env* _env, const ReadOptions& read_options,
               const ImmutableOptions& ioptions,
               const MutableCFOptions& mutable_cf_options,
               const Comparator* cmp, InternalIterator* iter,
               const Version* version, SequenceNumber s, bool arena_mode,
               ReadCallback* read_callback, ColumnFamilyHandleImpl* cfh,
               bool expose_blob_index, ReadOnlyMemTable* active_mem)
    : ...
      sequence_(s),  // Snapshot's sequence number
      ... {
  // Reads will only see keys with sequence <= sequence_
}
```

**Key Visibility Logic:**
```
Key written at seqno 100
Snapshot at seqno 150
-------------------------------------
Read with snapshot 150: VISIBLE   ✓ (100 <= 150)
Read with snapshot 90:  INVISIBLE ✗ (100 > 90)
```

**Read Example:**
```cpp
// Initially: key="value1" at seqno 100
const Snapshot* s1 = db->GetSnapshot();  // seqno = 100

db->Put(WriteOptions(), "key", "value2");  // seqno = 101

ReadOptions ro;
ro.snapshot = s1;
std::string value;
db->Get(ro, "key", &value);  // Returns "value1" (sees seqno 100, not 101)

db->ReleaseSnapshot(s1);
```

---

## How Snapshots Affect Compaction

### Preventing Premature Key Deletion

Compaction uses the **oldest snapshot's sequence number** to determine which old key versions can be safely deleted.

**CompactionIterator (db/compaction/compaction_iterator.cc:52-127):**
```cpp
CompactionIterator::CompactionIterator(
    InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
    SequenceNumber /*last_sequence*/, std::vector<SequenceNumber>* snapshots,
    SequenceNumber earliest_snapshot,
    SequenceNumber earliest_write_conflict_snapshot,
    SequenceNumber job_snapshot, const SnapshotChecker* snapshot_checker,
    ...,
    std::optional<SequenceNumber> preserve_seqno_min)
    : ...
      snapshots_(snapshots),
      earliest_snapshot_(earliest_snapshot),
      preserve_seqno_after_(preserve_seqno_min.value_or(earliest_snapshot)) {

  assert(snapshots_ != nullptr);
  assert(preserve_seqno_after_ <= earliest_snapshot_);

  // Compaction can only delete keys older than earliest_snapshot_
}
```

**Garbage Collection Rules:**

**findEarliestVisibleSnapshot (db/compaction/compaction_iterator.cc:1341-1390):**
```cpp
// Given a sequence number, return the sequence number of the
// earliest snapshot that this sequence number is visible in.
// Uses binary search (std::lower_bound) to find the snapshot.
SequenceNumber CompactionIterator::findEarliestVisibleSnapshot(
    SequenceNumber in, SequenceNumber* prev_snapshot) {
  assert(snapshots_->size());
  auto snapshots_iter =
      std::lower_bound(snapshots_->begin(), snapshots_->end(), in);

  if (snapshot_checker_ == nullptr) {
    return snapshots_iter != snapshots_->end() ? *snapshots_iter
                                               : kMaxSequenceNumber;
  }
  // ... WritePrepared transaction logic ...
}

// Used during compaction to determine if this key version is visible to any snapshot
SequenceNumber current_user_key_snapshot_ =
    visible_at_tip_
        ? earliest_snapshot_
        : findEarliestVisibleSnapshot(ikey_.sequence, &prev_snapshot);

// If key's seqno >= earliest snapshot's seqno, it MUST be kept
```

**Example:**
```
Snapshots: [100, 150, 200]
Compaction processing key "foo":
  - foo@seqno=250 (newest)
  - foo@seqno=180
  - foo@seqno=120
  - foo@seqno=90
  - foo@seqno=80

Outcome:
  - foo@250: KEEP (latest version, visible at tip)
  - foo@180: KEEP (latest version visible to snapshot 200)
  - foo@120: KEEP (latest version visible to snapshot 150)
  - foo@90:  KEEP (latest version visible to snapshot 100)
  - foo@80:  DELETE (same snapshot boundary as foo@90; shadowed by newer version)
```

**⚠️ INVARIANT:** For each user key, compaction keeps the **latest version visible at each snapshot boundary**, plus the latest version overall. A version is dropped only if a newer version exists in the same snapshot boundary (see `last_snapshot == current_user_key_snapshot_` check in compaction_iterator.cc).

**⚠️ INVARIANT:** Without snapshots, compaction only keeps the **most recent** version of each key.

### oldest_snapshot_sequence

**How Compaction Determines GC Boundary:**

**db/db_impl/db_impl_open.cc:2065-2068:**
```cpp
std::vector<SequenceNumber> snapshot_seqs =
    snapshots_.GetAll(&earliest_write_conflict_snapshot);

SequenceNumber earliest_snapshot =
    (snapshot_seqs.empty() ? kMaxSequenceNumber : snapshot_seqs.at(0));

// Pass to compaction to prevent premature deletion
```

**If `snapshot_seqs` is empty:**
- `earliest_snapshot = kMaxSequenceNumber`
- Compaction can delete **all but the latest** version of each key

**If snapshots exist:**
- `earliest_snapshot = min(all_snapshot_seqnos)`
- Compaction preserves all versions with `seqno >= earliest_snapshot`

---

## Snapshots and Iterators

### Implicit Snapshot for Iterator Lifetime

**When you create an iterator without specifying a snapshot, RocksDB creates an implicit snapshot:**

**db/db_impl/db_impl.cc (NewIteratorImpl:4072-4092):**
```cpp
ArenaWrappedDBIter* DBImpl::NewIteratorImpl(
    const ReadOptions& read_options, ColumnFamilyHandleImpl* cfh,
    SuperVersion* sv, SequenceNumber snapshot, ReadCallback* read_callback, ...) {

  if (snapshot == kMaxSequenceNumber) {
    // Note that the snapshot is assigned AFTER referencing the super
    // version because otherwise a flush happening in between may compact away
    // data for the snapshot, so the reader would see neither data that was be
    // visible to the snapshot before compaction nor the newer data inserted
    // afterwards.
    snapshot = versions_->LastSequence();
  }

  // Creates DBIter with the snapshot sequence number
  return NewDBIter(..., snapshot, ...);
}
```

**Key Behavior:**
- **Explicit snapshot:** Iterator sees database as of snapshot creation time
- **Implicit snapshot:** Iterator sees database as of iterator creation time
- Iterator remains consistent even if concurrent writes occur

**Example:**
```cpp
// Initially: key="v1" at seqno 100
Iterator* it = db->NewIterator(ReadOptions());  // Implicit snapshot at 100
it->SeekToFirst();

db->Put(WriteOptions(), "key", "v2");  // seqno = 101

// Iterator still sees "v1" (frozen at seqno 100)
```

**⚠️ INVARIANT:** An iterator's view **never changes** during its lifetime, regardless of concurrent writes.

---

## Snapshots and Transactions

### Snapshot Isolation in TransactionDB

Transactions use snapshots to implement **snapshot isolation**, ensuring consistent reads even with concurrent transactions.

**Public methods (utilities/transactions/transaction_base.h:255-272):**
```cpp
class TransactionBaseImpl : public Transaction {
 public:
  const Snapshot* GetSnapshot() const override {
    // will return nullptr when there is no snapshot
    return snapshot_.get();
  }

  std::shared_ptr<const Snapshot> GetTimestampedSnapshot() const override {
    return snapshot_;
  }

  void SetSnapshot() override;
  void SetSnapshotOnNextOperation(
      std::shared_ptr<TransactionNotifier> notifier = nullptr) override;

  void ClearSnapshot() override {
    snapshot_.reset();
    snapshot_needed_ = false;
    snapshot_notifier_ = nullptr;
  }
  // ...
};
```

**Protected/private members (utilities/transactions/transaction_base.h:392-394, 463, 467):**
```cpp
  // Stores the current snapshot that was set by SetSnapshot or null if
  // no snapshot is currently set.
  std::shared_ptr<const Snapshot> snapshot_;

  // SetSnapshotOnNextOperation() has been called and the snapshot has not yet
  // been reset.
  bool snapshot_needed_ = false;

  // SetSnapshotOnNextOperation() has been called and the caller would like
  // a notification through the TransactionNotifier interface
  std::shared_ptr<TransactionNotifier> snapshot_notifier_ = nullptr;
```

**Write-Conflict Detection:**
- `is_write_conflict_boundary_`: Marks snapshots used for conflict detection (db/snapshot_impl.h:50-51)
- Transaction reads use snapshot to check if keys have been modified since transaction start
- If a key was updated after transaction's snapshot, write-conflict occurs

**GetAll with Write-Conflict Tracking (db/snapshot_impl.h:142-148):**
```cpp
if (oldest_write_conflict_snapshot != nullptr &&
    *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
    s->next_->is_write_conflict_boundary_) {
  // First write-conflict boundary snapshot in the list
  *oldest_write_conflict_snapshot = s->next_->number_;
}
```

**Transaction Example:**
```cpp
TransactionDB* txn_db = ...;
Transaction* txn = txn_db->BeginTransaction(...);

txn->SetSnapshot();  // Lock transaction's view of database

std::string value;
txn->GetForUpdate(ReadOptions(), cf, "key", &value);  // Read with snapshot
txn->Put(cf, "key", "new_value");                     // Buffered write

Status s = txn->Commit();  // Fails if "key" modified after snapshot
```

**⚠️ INVARIANT:** WritePrepared transactions use `min_uncommitted_` to limit the scope of `IsInSnapshot()` queries (db/snapshot_impl.h:26-29).

---

## Snapshot Pinning: Space Implications

### How Snapshots Keep Old Versions Alive

Snapshots **pin data** by preventing compaction from deleting old key versions. This can lead to **space amplification** if snapshots are held for long periods.

**Space Amplification Example:**
```
Time 0: Create snapshot S1 at seqno 100
Time 1: Update key "foo" 10,000 times (seqno 101-10100)
Time 2: Compaction runs

WITHOUT snapshot S1:
  - Compaction keeps only foo@10100 (latest version)
  - Space: 1 version

WITH snapshot S1:
  - Compaction keeps foo@10100 AND foo@100 (visible to S1)
  - Intermediate versions 101-10099 can be deleted
  - Space: 2 versions

WITH snapshots S1@100, S2@5000:
  - Compaction keeps foo@10100, foo@5000, foo@100
  - Space: 3 versions (one per snapshot + tip)
```

**Best Practices:**
1. **Release snapshots promptly** after use
2. **Avoid long-lived snapshots** in production (hours/days)
3. **Monitor snapshot count** via `DB::GetProperty("rocksdb.num-snapshots")`
4. **Consider snapshot age** when diagnosing space amplification

**⚠️ INVARIANT:** Each snapshot potentially adds **one extra version** of every key to disk.

---

## Snapshot List Management: Thread Safety

### Concurrent Access and Locking

**⚠️ INVARIANT:** All `SnapshotList` operations **require holding DB mutex** (enforced in `GetSnapshotImpl` via `mutex_.Lock()`/`mutex_.AssertHeld()` and in `ReleaseSnapshot` via `InstrumentedMutexLock`).

**GetSnapshot Implementation (db/db_impl/db_impl.cc:4324-4351):**
```cpp
SnapshotImpl* DBImpl::GetSnapshotImpl(bool is_write_conflict_boundary, bool lock) {
  int64_t unix_time = 0;
  immutable_db_options_.clock->GetCurrentTime(&unix_time)
      .PermitUncheckedError();  // Ignore error
  SnapshotImpl* s = new SnapshotImpl;

  if (lock) {
    mutex_.Lock();  // Acquire DB mutex
  } else {
    mutex_.AssertHeld();  // Caller already holds mutex
  }

  // returns null if the underlying memtable does not support snapshot.
  if (!is_snapshot_supported_) {
    if (lock) mutex_.Unlock();
    delete s;
    return nullptr;
  }

  auto snapshot_seq = GetLastPublishedSequence();

  // Insert into snapshot list (protected by mutex_)
  snapshots_.New(s, snapshot_seq, unix_time,
                 is_write_conflict_boundary);

  if (lock) mutex_.Unlock();
  return s;
}
```

**ReleaseSnapshot (db/db_impl/db_impl.cc:4474-4546):**
```cpp
void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  if (s == nullptr) {
    // DBImpl::GetSnapshot() can return nullptr when snapshot
    // not supported by specifying the condition:
    // inplace_update_support enabled.
    return;
  }

  const SnapshotImpl* casted_s = static_cast<const SnapshotImpl*>(s);

  {
    InstrumentedMutexLock l(&mutex_);  // Acquire DB mutex
    snapshots_.Delete(casted_s);       // Remove from list

    // After removing a snapshot, update the oldest snapshot for each
    // column family and potentially schedule compaction/flush to
    // reclaim space that was pinned by the released snapshot.
    // ... (iterates column families, calls UpdateOldestSnapshot,
    //      EnqueuePendingCompaction, MaybeScheduleFlushOrCompaction) ...
  }

  delete casted_s;  // Free memory (outside mutex)
}
```

### Snapshot Object Immutability

**Once created, `SnapshotImpl` fields are immutable:**
- `number_` is `const` after creation (db/snapshot_impl.h:25 comment)
- Safe to read from multiple threads without synchronization
- Only **list management** (insert/delete) requires mutex

**include/rocksdb/snapshot.h:14-16:**
```cpp
// A Snapshot is an immutable object and can therefore be safely
// accessed from multiple threads without any external synchronization.
class Snapshot { ... };
```

**⚠️ INVARIANT:** Snapshot objects are **immutable** after creation and **thread-safe for reads**.

---

## Timestamped Snapshots

For user-defined timestamp support, RocksDB provides `TimestampedSnapshotList`:

**db/snapshot_impl.h:188-237:**
```cpp
class TimestampedSnapshotList {
 public:
  std::shared_ptr<const SnapshotImpl> GetSnapshot(uint64_t ts) const;

  void GetSnapshots(uint64_t ts_lb, uint64_t ts_ub,
                    std::vector<std::shared_ptr<const Snapshot>>& snapshots);

  void AddSnapshot(const std::shared_ptr<const SnapshotImpl>& snapshot);

  void ReleaseSnapshotsOlderThan(
      uint64_t ts,
      autovector<std::shared_ptr<const SnapshotImpl>>& snapshots_to_release);

 private:
  std::map<uint64_t, std::shared_ptr<const SnapshotImpl>> snapshots_;
};
```

**Usage (db/db_impl/db_impl.cc:4283-4322):**
```cpp
// Create timestamped snapshot
auto [status, snapshot] = db->CreateTimestampedSnapshot(snapshot_seq, ts);

// Retrieve by timestamp
std::shared_ptr<const SnapshotImpl> s = db->GetTimestampedSnapshot(ts);

// Retrieve range
std::vector<std::shared_ptr<const Snapshot>> snapshots;
db->GetTimestampedSnapshots(ts_lb, ts_ub, snapshots);

// Bulk release
db->ReleaseTimestampedSnapshotsOlderThan(ts);
```

**⚠️ INVARIANT:** All operations on `TimestampedSnapshotList` **must be protected by DB mutex** (db/snapshot_impl.h:187).

---

## Snapshot Lifecycle Diagram

```
DB Mutex Protected Zone:

  1. GetSnapshot()
       |
       v
     new SnapshotImpl()
     number_ = GetLastPublishedSequence()
     snapshots_.New(s, ...)  // Insert into doubly-linked list
       |
       v

  2. Snapshot In Use (mutex NOT held)
     - Reads use snapshot->GetSequenceNumber()
     - Compaction checks earliest_snapshot
     - Iterator uses snapshot for visibility

  3. ReleaseSnapshot(snapshot)
       |
       v
     mutex_.Lock()
     snapshots_.Delete(s)  // Remove from doubly-linked list
     mutex_.Unlock()
     delete s              // Free memory
```

---

## Common Pitfalls and Best Practices

### ❌ Pitfall 1: Forgetting to Release Snapshots
```cpp
void BadFunction(DB* db) {
  const Snapshot* s = db->GetSnapshot();
  // ... do work ...
  // MISSING: db->ReleaseSnapshot(s);
}  // Memory leak + pinned data
```

**✅ Solution:** Use `ManagedSnapshot` for RAII:
```cpp
void GoodFunction(DB* db) {
  ManagedSnapshot ms(db);  // Automatic cleanup
  // ... do work ...
}
```

### ❌ Pitfall 2: Long-Lived Snapshots
```cpp
const Snapshot* s = db->GetSnapshot();
// ... hours later, still holding snapshot
// Compaction cannot delete ANY old versions!
```

**✅ Solution:** Release snapshots as soon as possible:
```cpp
const Snapshot* s = db->GetSnapshot();
// Do work requiring snapshot
db->ReleaseSnapshot(s);  // Release immediately
```

### ❌ Pitfall 3: Using Snapshot After Release
```cpp
const Snapshot* s = db->GetSnapshot();
db->ReleaseSnapshot(s);

ReadOptions ro;
ro.snapshot = s;  // DANGLING POINTER!
db->Get(ro, "key", &value);  // Undefined behavior
```

**✅ Solution:** Clear snapshot pointer after release:
```cpp
const Snapshot* s = db->GetSnapshot();
db->ReleaseSnapshot(s);
s = nullptr;  // Prevent accidental reuse
```

---

## Performance Characteristics

| Operation | Time Complexity | Lock Held | Notes |
|-----------|----------------|-----------|-------|
| `GetSnapshot()` | O(1) | DB mutex | Inserts at end of list |
| `ReleaseSnapshot()` | O(CF) worst | DB mutex | Unlinks from list; conditionally iterates column families to trigger bottommost compaction |
| `snapshots_.GetAll()` | O(n) | DB mutex | n = number of snapshots |
| Reading with snapshot | O(1) | None | Just seqno comparison in iterator |
| Compaction GC decision | O(log n) | None | Binary search via `findEarliestVisibleSnapshot()` |

**Memory Overhead:**
- Per snapshot: ~80 bytes (`SnapshotImpl` object)
- Negligible for <1000 snapshots

**Space Amplification:**
- Worst case: O(k × n) extra versions
  - k = number of snapshots
  - n = number of keys updated between snapshots

---

## Summary

**Snapshots enable consistent, point-in-time reads in RocksDB by:**
1. **Sequence Number Tagging:** Each write gets a monotonically increasing seqno
2. **Immutable Views:** Snapshots freeze the database state at a specific seqno
3. **Visibility Filtering:** Reads see only data with seqno ≤ snapshot's seqno
4. **GC Prevention:** Compaction preserves all versions visible to any snapshot
5. **Thread-Safe Design:** Snapshots are immutable and safe for concurrent reads

**Key Implementation Details:**
- **Data Structure:** Doubly-linked list ordered by sequence number
- **Locking:** List modifications require DB mutex; reads are lock-free
- **Space Trade-off:** Snapshots pin old data, preventing garbage collection
- **Transaction Support:** Snapshots enable snapshot isolation and write-conflict detection

**Best Practices:**
- Use `ManagedSnapshot` for automatic cleanup
- Release snapshots promptly to allow compaction
- Monitor snapshot count and age to diagnose space amplification
- Understand implicit snapshots in iterators
