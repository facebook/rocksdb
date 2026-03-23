RocksDB supports Transactions when using a (Pessimistic)`TransactionDB` or `OptimisticTransactionDB`.  Transactions have a simple BEGIN/COMMIT/ROLLBACK API and allows applications to modify their data concurrently while letting RocksDB handle the conflict checking.  RocksDB supports both pessimistic and optimistic concurrency control.  

Note that RocksDB provides Atomicity by default when writing multiple keys via `WriteBatch`. Transactions provide a way to guarantee that a batch of writes will only be written if there are no conflicts. Similar to a `WriteBatch`, no other threads can see the changes in a transaction until it has been written (committed).

### TransactionDB
When using a `TransactionDB`, all keys that are written are locked internally by RocksDB to perform conflict detection.  If a key cannot be locked, the operation will return an error.  When the transaction is committed, it is guaranteed to succeed as long as the database is able to be written to.

A `TransactionDB` can be better for workloads with heavy concurrency compared to an `OptimisticTransactionDB`.  However, there is a small locking overhead when `TransactionDB` is used.  A `TransactionDB` will do conflict checking for all write operations (`Put`, `Delete` and `Merge`), including writes performed outside a Transaction.

Locking timeouts and limits can be tuned in the `TransactionDBOptions`.

	TransactionDB* txn_db;
	Status s = TransactionDB::Open(options, txndb_options, path, &txn_db);

	Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);
	s = txn->Put(“key”, “value”);
	s = txn->Delete(“key2”);
	s = txn->Merge(“key3”, “value”);
	s = txn->Commit();
	delete txn;

The default write policy is `WriteCommitted`. The alternatives are `WritePrepared` and `WriteUnprepared`. Read more about them [[here | WritePrepared-Transactions]].

### OptimisticTransactionDB
Optimistic Transactions provide light-weight optimistic concurrency control for workloads that do not expect high contention/interference between multiple transactions.

Optimistic Transactions do not take any locks when preparing writes. Instead, they rely on doing conflict-detection at commit time to validate that no other writers have modified the keys being written by the current transaction. If there is a conflict with another write (or it cannot be determined), the commit will return an error and no keys will be written.

Optimistic concurrency control is useful for many workloads that need to protect against occasional write conflicts. However, this may not be a good solution for workloads where write-conflicts occur frequently due to many transactions constantly attempting to update the same keys. For these workloads, using a `TransactionDB` may be a better fit. An `OptimisticTransactionDB` may be more performant than a `TransactionDB` for workloads that have many non-transactional writes and few transactions. 

	DB* db;
	OptimisticTransactionDB* txn_db;

	Status s = OptimisticTransactionDB::Open(options, path, &txn_db);
	db = txn_db->GetBaseDB();

	OptimisticTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
	txn->Put(“key”, “value”);
	txn->Delete(“key2”);
	txn->Merge(“key3”, “value”);
	s = txn->Commit();
	delete txn;


### Reading from a Transaction            
Transactions also support easily reading the state of keys that are currently batched in a given transaction but not yet committed:

	db->Put(write_options, “a”, “old”);
	db->Put(write_options, “b”, “old”);
	txn->Put(“a”, “new”);

	vector<string> values;
	vector<Status> results = txn->MultiGet(read_options, {“a”, “b”}, &values);
	//  The value returned for key “a” will be “new” since it was written by this transaction.
	//  The value returned for key “b” will be “old” since it is unchanged in this transaction.

You can also iterate through keys that exist in both the db and the current transaction by using `Transaction::GetIterator()`.

RocksDB transactions support [Read-your-own-writes](https://rocksdb.org/blog/2015/02/27/write-batch-with-index.html), and more information
can be found at [[Write-Batch-With-Index | Write Batch With Index]].

### Guarding against Read-Write Conflicts:
Call GetForUpdate() to read a key and make the read value a precondition for transaction commit.

	// Start a transaction 
	txn = txn_db->BeginTransaction(write_options);

	// Read key1 in this transaction
	Status s = txn->GetForUpdate(read_options, “key1”, &value);

	// Write to key1 OUTSIDE of the transaction
	s = db->Put(write_options, “key1”, “value0”);

If this transaction was created by a `TransactionDB`, the `Put` would either timeout or block until the transaction commits or aborts.  If this transaction were created by an `OptimisticTransactionDB`, then the `Put` would succeed, but the transaction would fail when `txn->Commit()` is called.

	// Repeat the previous example but just do a Get() instead of a GetForUpdate()
	txn = txn_db->BeginTransaction(write_options);

	// Read key1 in this transaction
	Status s = txn->Get(read_options, “key1”, &value);

	// Write to key1 OUTSIDE of the transaction
	s = db->Put(write_options, “key1”, “value0”);

	// No conflict since transactions only do conflict checking for keys read using GetForUpdate().
	s = txn->Commit();

Currently, `GetForUpdate()` is the only way to establish Read-Write conflicts, so it can be used in combination with iterators, for example.

### Setting a Snapshot

By default, Transaction conflict checking validates that no one else has written a key *after* the time the key was first written in this transaction. This isolation guarantee is sufficient for many use-cases. However, you may want to guarantee that no one else has written a key since the start of the transaction. This can be accomplished by calling `SetSnapshot()` after creating the transaction.

Default behavior:

	// Create a txn using either a TransactionDB or OptimisticTransactionDB
	txn = txn_db->BeginTransaction(write_options);

	// Write to key1 OUTSIDE of the transaction
	db->Put(write_options, “key1”, “value0”);

	// Write to key1 IN transaction
	s = txn->Put(“key1”, “value1”);
	s = txn->Commit();
	// There is no conflict since the write to key1 outside of the transaction
	// happened before it was written in this transaction.

Using `SetSnapshot()`:

	txn = txn_db->BeginTransaction(write_options);
	txn->SetSnapshot();

	// Write to key1 OUTSIDE of the transaction
	db->Put(write_options, “key1”, “value0”);

	// Write to key1 IN transaction
	s = txn->Put(“key1”, “value1”);
	s = txn->Commit();
	// Transaction will NOT commit since key1 was written outside of this transaction 
	// after SetSnapshot() was called (even though this write
	// occurred before this key was written in this transaction).

Note that in the `SetSnapshot()` case of the previous example, if this were a `TransactionDB`, the `Put()` would have failed. If this were an `OptimisticTransactionDB`, the `Commit()` would fail.	

### Repeatable Read
Similar to normal RocksDB reads, you can achieve repeatable reads when reading through a transaction by specifying a `Snapshot` in the `ReadOptions`.

	read_options.snapshot = db->GetSnapshot();
	s = txn->GetForUpdate(read_options, “key1”, &value);
	…
	s = txn->GetForUpdate(read_options, “key1”, &value);
	db->ReleaseSnapshot(read_options.snapshot);

Note that Setting a `Snapshot` in the `ReadOptions` only affects the version of the data that is read.  This does not have any effect on whether the transaction will be able to be committed.

If you have called `SetSnapshot()`, you can read using the same snapshot that was set in the transaction:

	read_options.snapshot = txn->GetSnapshot();
	Status s = txn->GetForUpdate(read_options, “key1”, &value);

### Tuning / Memory Usage

Internally, Transactions need to keep track of which keys have been written recently.  The existing in-memory write buffers are re-used for this purpose.  Transactions will still obey the existing `max_write_buffer_number` option when deciding how many write buffers to keep in memory.  In addition, using transactions will not affect flushes or compactions.

It is possible that switching to using a `[Optimistic]TransactionDB` will use more memory than was used previously.  If you have set a very large value for `max_write_buffer_number`, a typical RocksDB instance will never come close to this maximum memory limit.  However, an `[Optimistic]TransactionDB` will try to use as many write buffers as allowed.  But, this can be tuned by either reducing `max_write_buffer_number` or by setting `max_write_buffer_size_to_maintain`. See [[memtable|memtable]] for more details about `max_write_buffer_size_to_maintain`.

OptimisticTransactionDBs:
At commit time, optimistic transactions will use the in-memory write buffers for conflict detection.  For this to be successful, the data buffered must be older than the changes in the transaction.  If not, Commit will fail.  To decrease the likelihood of `Commit` failures due to insufficient buffer history, increase `max_write_buffer_size_to_maintain`.

TransactionDBs:
If `SetSnapshot()` is used, `Put/Delete/Merge/GetForUpdate` operations will first check the in-memory buffers to do conflict detection.  If there isn’t sufficient data history in the in-memory buffers, the SST files will then be checked.  Increasing `max_write_buffer_size_to_maintain` will reduce the chance that SST files must be read during conflict detection.


### Save Points

In addition to `Rollback()`, Transactions can also be partially rolled back if `SavePoints` are used.

	s = txn->Put("A", "a");
	txn->SetSavePoint();
	s = txn->Put("B", "b");
	txn->RollbackToSavePoint()
	s = txn->Commit()
	// Since RollbackToSavePoint() was called, this transaction will 
	// only write key A and not write key B.

## Under the hood

A high-level, succinct overview of how transactions work under the hood.

### Read Snapshot

Each update in RocksDB is done by inserting an entry tagged with a monotonically increasing sequence number. Assigning a seq to `read_options.snapshot` will be used by (transactional or non-transactional) DB to read only values with seq smaller than or equal to that, i.e., read snapshots → `DBImpl::GetImpl`

That aside, transactions can call `TransactionBaseImpl::SetSnapshot` which will invoke `DBImpl::GetSnapshot`. It achieves two goals:

1. Return the current seq: transactions will use the seq (instead of the seq of its written value) to check for write-write conflicts → `TransactionImpl::TryLock` → `TransactionImpl::ValidateSnapshot` → `TransactionUtil::CheckKeyForConflicts`
2. Makes sure that values with smaller seq will not be erased by compaction jobs, etc. ([snapshots_.GetAll](https://github.com/facebook/rocksdb/search?utf8=%E2%9C%93&q=snapshots_.GetAll)). Such marked snapshots must be released by the callee (`DBImpl::ReleaseSnapshot`)

### Read-Write conflict detection

Read-Write conflicts can be prevented by escalating them to write-write conflict: doing reads via `GetForUpdate` (instead of `Get`).

### Write-Write conflict detection: pessimistic approach

Write-write conflicts are detected at the write time using a lock table.

Non-transactional updates (`Put`, `Merge` and `Delete`) are internally run under a transaction. So, every update is through transactions → `TransactionDBImpl::Put`

Every update acquires a lock beforehand →  `TransactionImpl::TryLock`
`PointLockManager::TryLock` has only 16 locks per column family → `size_t num_stripes = 16`

Commit simply writes the write batch to WAL as well as to Memtable by calling `DBImpl::Write` → `TransactionImpl::Commit`

To support distributed transactions, the clients can call Prepare after performing the writes. It writes the value to WAL but not to the MemTable, which allows recovery in the case of machine crash → `TransactionImpl::Prepare`
If `Prepare` is invoked, `Commit` writes a commit marker to the WAL and writes values to MemTable. This is done by calling `MarkWalTerminationPoint()` before adding value to the write batch.


### Write-Write conflict detection: optimistic approach

Write-write conflicts are detected using seq of the latest values at the commit time.

Each update adds the key to an in-memory vector →  `TransactionDBImpl::Put` and `OptimisticTransactionImpl::TryLock`

Commit links `OptimisticTransactionImpl::CheckTransactionForConflicts` as a callback to the write batch → `OptimisticTransactionImpl::Commit`
which will be invoked in `DBImpl::WriteImpl` via `write->CheckCallback`

The conflict detection logic is implemented at `TransactionUtil::CheckKeysForConflicts`

* only checks for conflicts against keys present in memory and fails otherwise.
* conflict detection is done by checking the latest seq of each key (`DBImpl::GetLatestSequenceForKey`) against the seq used for writing it.
