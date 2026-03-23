A snapshot captures a point-in-time view of the DB at the time it's created. Snapshots do not persist across DB restarts.

A snapshot is associated with an internal sequence number assigned by RocksDB when taking the snapshot without application intervention. By taking a snapshot, application registers an object with the DB so that the DB will preserve the point-in-time view at the specified sequence number. In most cases, if a key's sequence number is greater than the sequence number of the snapshot, then we know the key is invisible to the snapshot. If a key's sequence number is smaller than or equal to that of a snapshot, then the key is visible to the snapshot. Things can be more complex for `WritePreparedTxnDB` and `WriteUnpreparedTxnDB` which rely on `SnapshotCheckers` to perform more sophisticated visibility checking.

We can associate rocksdb snapshots with user-specified timestamps, e.g. HLC. Since a snapshot is a volatile object representing a sequence number, timestamped snapshots establish an explicit bi-directional mapping between sequence numbers and timestamps. This is helpful because it allows applications to perform consistent reads on multiple rocksdb snapshots each of which is taken from a different RocksDB instance as long as the snapshots have the same timestamp. This is not possible in the past, because original snapshots/sequence numbers from different RocksDB instances are not comparable/related. Currently, this functionality is exposed to the layer of rocksdb transactions.

Applications can take a timestamped snapshot with T when all writes that correspond to timestamps <= T have been acknowledged by RocksDB. Applications also need to ensure that any two timestamped snapshots should satisfy the following:
```
snapshot1.seq <= snapshot2.seq iff. snapshot1.ts <= snapshot2.ts
```

## API Usage

Snapshot allows applications to perform point-in-time read. Applications can read from a snapshot by setting `ReadOptions::snapshot`.

```cpp
// ==============
// General APIs
// ==============
//
// Create a snapshot. Caller is responsible for releasing the returned snapshot.
const Snapshot* DB::GetSnapshot();

// When finished, release resources associated with the snapshot.
void DB::ReleaseSnapshot(const Snapshot* snapshot);

// ==========================
// Timestamped snapshot APIs
// ==========================
//
// Commit a transaction and create a snapshot with timestamp ts. The
// created snapshot includes the updates made by the transaction.
Status Transaction::CommitAndTryCreateSnapshot(
    std::shared_ptr<TransactionNotifier> notifier,
    TxnTimestamp ts,
    std::shared_ptr<const Snapshot>* ret);

std::pair<Status, std::shared_ptr<const Snapshot>>
TransactionDB::CreateTimestampedSnapshot(TxnTimestamp ts);

// Return the timestamped snapshot correponding to given timestamp. If ts is
// kMaxTxnTimestamp, then we return the latest timestamped snapshot if present.
// Othersise, we return the snapshot whose timestamp is equal to `ts`. If no
// such snapshot exists, then we return null.
std::shared_ptr<const Snapshot> TransactionDB::GetTimestampedSnapshot(TxnTimestamp ts) const;

// Return the latest timestamped snapshot if present.
std::shared_ptr<const Snapshot> 
TransactionDB::GetLatestTimestampedSnapshot() const;

Status TransactionDB::GetAllTimestampedSnapshots(
    std::vector<std::shared_ptr<const Snapshot>>& snapshots) const;

// Return timestamped snapshots whose timestamps fall in [ts_lb, ts_ub) and store them in `snapshots`.
Status TransactionDB::GetTimestampedSnapshots(
    TxnTimestamp ts_lb,
    TxnTimestamp ts_ub,
    std::vector<std::shared_ptr<const Snapshot>>& snapshots) const;

// Release timestamped snapshots whose timestamps < ts
void TransactionDB::ReleaseTimestampedSnapshotsOlderThan(TxnTimestamp ts);
```

## Implementation

### Flush/compaction
Both flush and compaction use `CompactionIterator` to process key-value pairs and determine if each key-value pair should be dropped or output to resulting file. `CompactionIterator` is aware of all the snapshots and ensures that the data visible to each snapshot is preserved.

### Representation

A snapshot is represented by a small object of `SnapshotImpl` class. It holds only a few primitive fields, like the seqnum at which the snapshot was taken, user-specified timestamp, etc.

All snapshots (with and without timestamps) are stored in a linked list owned by `DBImpl`. One benefit is we can allocate the list node before acquiring the DB mutex. Then while holding the mutex, we only need to update list pointers. Additionally, `ReleaseSnapshot()` can be called on the snapshots in an arbitrary order. With linked list, we can remove a node from the middle without shifting.

### Scalability

The main downside of linked list is it cannot be binary searched despite its ordering. During flush/compaction, we have to scan the snapshot list when we need to find out the earliest snapshot to which a key is visible. When there are many snapshots, this scan can significantly slow down flush/compaction to the point of causing write stalls. We've noticed problems when snapshot count is in the hundreds of thousands. To address this limitation, we allocate a binary-searchable data structure, e.g. a vector and copy the sequence numbers of all alive snapshots for each flush/compaction job. This is a trade-off between space and time.