This document outlines the implementation of Two Phase Commit in rocksdb.

##### This project can be decomposed into five areas of focus:

1. Modification of the WAL format
2. Extension of the existing transaction API
3. Modification of the write path
4. Modification of the recovery path
5. Integration with MyRocks

### Modification of WAL Format

The WAL consists of one or more logs. Each log is one or more serialized WriteBatches. During recovery WriteBatches are reconstructed from the logs. To modify the WAL format or extend its functionality we must only concern ourselves with the WriteBatch.

A WriteBatch is an ordered set of records (Put(k,v), Merge(k,v), Delete(k), SingleDelete(k)) that represent RocksDB write operations. Each of these records has a binary string representation. As records are added to a WriteBatch their binary representation is appended to the WriteBatch's binary string representation. This binary string is prefixed with the starting sequence number of the batch followed by the number of records contained in the batch.  Each record may be prefixed with a column family modifier record if the operation does not apply to the default column family.

A WriteBatch can be iterated over by extending the WriteBatch::Handler.  MemTableInserter is an extension of WriteBatch::Handler which inserts the operations contained in a WriteBatch into the appropriate column family MemTable.

An existing WriteBatch may have the logical representation:
`Sequence(0);NumRecords(3);Put(a,1);Merge(a,1);Delete(a);`

Modification of the WriteBatch format for 2PC includes the addition of four new records. 

* Prepare(xid)
* EndPrepare()
* Commit(xid)
* Rollback(xid)

A 2PC capable WriteBatch may have the logical representation:
`Sequence(0);NumRecords(6);Prepare(foo);Put(a,b);Put(x,y);EndPrepare();Put(j,k);Commit(foo);`

It can be seen that Prepare(xid) and EndPrepare() are analogous to mating brackets which contain the operations belonging to transaction with ID 'foo'. Commit(xid) and Rollback(xid) mark that operations belonging to transaction with ID xid should be committed or rolled-back.

##### Sequence ID Distribution

When a WriteBatch is inserted into a memtable (via MemTableInserter) the sequence ID of each operation is equal to the sequence ID of the WriteBatch plus the number of sequence ID consuming records previous to this operation in the WriteBatch. This implicit mapping of sequence ids within a WriteBatch will no longer hold with the addition of 2PC. Operations contained within a Prepare() enclosure will consume sequence IDs as if they were inserted starting at the location of their relative Commit() marker. This Commit() marker may be in a different WriteBatch or log from the prepared operations to which it applies.

##### Backwards Compatibility

WAL formats are not versioned so we need to take note of backwards compatibility. A current version of RocksDB would not be able to recover itself from a WAL file containing 2PC markers. In fact it would fatal on the unrecognized record ids. It would be trivial, however, to patch a current version of RocksDB to be able to recover itself from this new WAL format just skipping over the prepared sections and unknown markers.

##### Existing Progress

Progress had been made on this front and relevant discussion can be found at https://reviews.facebook.net/D54093

## Extension of Transaction API

For the time being we will only focus on 2PC for pessimistic transactions. The client must specify ahead of time if they intend to employ two phase commit semantics.  For example, the client code could be imagined as:

```c++
TransactionDB* db;
TransactionDB::Open(Options(), TransactionDBOptions(), "foodb", &db);

TransactionOptions txn_options;
txn_options.two_phase_commit = true
txn_options.xid = "12345";
Transaction* txn = db->BeginTransaction(write_options, txn_options);
    
txn->Put(...);
txn->Prepare();
txn->Commit();
```
A transaction object now has more states that it can occupy so our enum of states now becomes:

```c++
enum ExecutionStatus {
  STARTED = 0,
  AWAITING_PREPARE = 1,
  PREPARED = 2,
  AWAITING_COMMIT = 3,
  COMMITED = 4,
  AWAITING_ROLLBACK = 5,
  ROLLEDBACK = 6,
  LOCKS_STOLEN = 7,
};
```

The transaction API will gain a new member function, Prepare().  Prepare() will call into WriteImpl with a context of it self giving WriteImpl and the WriteThread access to the ExecutionStatus, XID, and WriteBatch. WriteImpl will insert the Prepare(xid) marker followed by the contents of the WriteBatch followed by EndPrepare() marker. No memtable insertion will be issued. When the same transaction instance issued its commit, again, it calls into WriteImpl(). This time only a Commit() marker is inserted into the WAL on its behalf and the contents of the WriteBatch are inserted into the appropriate memtables. When Rollback() on the transactions is called the contents of the transactions are cleared and a call into WriteImpl to insert a Rollback(xid) marker is made if the transaction is in a prepared state.

These so-called 'meta markers' (Prepare(xid), EndPrepare(), Commit(xid), Rollback(xid)) will never be inserted directly into a write batch. The write path (WriteImpl()) will have the context of the transactions it is writing. It uses this context to insert the relevant markers directly into the WAL (So they are inserted into the aggregate WriteBatch right before being inserted into the WAL, but no other WriteBatch). During recovery these markers will be encountered by the MemTableInserter which he will use to reconstruct previously prepared transactions.

##### Transaction Wallclock Expiration

Currently at the time of a transaction commit there is a callback which will fail the write if the transaction has expired. Similarly, if a transaction has expired then it is now eligible to have its locks stolen by other transactions. These mechanisms should still be in place for 2PC - the difference being that the expiration callback will be called at the time of preparation. If the transaction did not expire at the time of preparation then it cannot expire at the time of commit.

##### TransactionDB Modification

To use transactions the client must open a TransactionDB. This TransactionDB instance is then used to create Transactions. This TransactionDB now keeps track of a mapping from XID to all two phase transactions which has been created. When a transactions is Deleted or Rolled-back it is removed from this mapping. There is also an API to query all outstanding prepared transactions. This is used during MyRocks recovery.

The TransactionDB also keeps track of a min heap of all log numbers containing a prepared section. When a transaction is 'prepared' its WriteBatch is written to a log, this log number is then stored in the transaction object and subsequently the min heap. When a transaction is committed its log number is deleted from the min heap, but it is not forgotten! It is now the duty of each memtable to keep track of the oldest log it needs to keep around until his is successfully flushed to L0.

### Modification of the Write Path

The write path can be decomposed into two main areas of focus. DBImpl::WriteImpl(...) and the MemTableInserter. Multiple client threads will call into WriteImpl. The first thread will be designated as the 'leader' while a number of following threads will be designated as 'followers'. Both the leader and set of followers will be batched together into a logical group referred to as a 'write group'. The leader will take all WriteBatches of the write group, concatenate them together and write this blob out to the WAL. Depending on the size of the write group and the current memtables's willingness to support parallel writes the leader may insert all WriteBatches into the memtable or each thread may be left to insert his own WriteBatch into the memtable.

All memtable inserts are handled by MemTableInserter. This is an implementation of WriteBatch::Handler - a WriteBatch iterator handler. This handler iterates over all elements in a WriteBatch (Put, Delete, Merge, etc) and makes the appropriate call into the current MemTable. MemTableInserter will also handle in-place merges, deletes and updates.

Modification of the write path will include adding an optional parameter to DBImpl::WriteImpl. This optional parameter will be a pointer to the two phase transaction instance that is having his data written. This object will give the write path insight into the current state of two phase transaction. A 2PC transaction will call into WriteImpl once for preparation, once for commit, and once for roll-back - though commit and rollback are obviously exclusive operations.

```c++
Status DBImpl::WriteImpl(
  const WriteOptions& write_options, 
  WriteBatch* my_batch,
  WriteCallback* callback,
  Transaction* txn
) {
  WriteThread::Writer w;
  //...
  w.txn = txn; // writethreads also have txn context for memtable insert

  // we are now the group leader
  int total_count = 0;
  uint64_t total_byte_size = 0;
  for (auto writer : write_group) {
    if (writer->CheckCallback(this)) {
      if (writer->ShouldWriteToMem())
        total_count += WriteBatchInternal::Count(writer->batch)
       }
  }
  const SequenceNumber current_sequence = last_sequence + 1;
  last_sequence += total_count;

  // now we produce the WAL entry from our write group
  for (auto writer : write_group) {
    // currently only optimistic transactions use callbacks
    // and optimistic transaction do not support 2pc
   if (writer->CallbackFailed()) {
      continue;
    } else if (writer->IsCommitPhase()) {
      WriteBatchInternal::MarkCommit(merged_batch, writer->txn->XID_);
    } else if (writer->IsRollbackPhase()) {
      WriteBatchInternal::MarkRollback(merged_batch, writer->txn->XID_);
    } else if (writer->IsPreparePhase()) {
      WriteBatchInternal::MarkBeginPrepare(merged_batch, writer->txn->XID_);
      WriteBatchInternal::Append(merged_batch, writer->batch);
      WriteBatchInternal::MarkEndPrepare(merged_batch);
      writer->txn->log_number_ = logfile_number_;
    } else {
      assert(writer->ShouldWriteToMem());
      WriteBatchInternal::Append(merged_batch, writer->batch);
    }
  }
  //now do MemTable Inserts for WriteGroup
}
```

`WriteBatchInternal::InsertInto` could then be modified to only iterate over writers having no Transaction associated or Transactions in the COMMIT state.

##### Modification of MemTableInserter for WritePath

As you can see above when a transactions is prepared the transaction takes note of what log number its prepared section resided in. At the time of insertion each MemTable must keep track of the minimum log number containing prepared data which has been inserted into him. This modification will take place in the MemTableInserter. We will discuss how this value is used in the log lifespan section.

### Modification of Recovery Path

The current recovery path is already pretty well suited for two phase commit. It iterates over all batches in all the logs in chronological order and feeds them, along the the log number, into the MemTableInserter. The MemTableInserter then iterates over each of these batches and inserts the values into the correct MemTable. Each MemTable knows what values it can ignore for insertion based on the current log number being recovered from.

To make recovery work for 2PC we must only modify the MemTableInserter to be aware of our four new 'meta markers'.

Keep this in mind: When a two phase transaction is committed it contains insertions that will act on multiple CFs (multiple memtables). These memtables will flush at different times. We still make use of the CF log number to avoid duplicate inserts for recovered, two phase, committed transaction.

Consider the following scenario:

1. Two Phase Transactions TXN inserts into CFA and CFB
2. TXN prepared to LOG 1
3. TXN marked as COMMITTED in LOG 2
4. TXN is inserted into MemTables
5. CFA is flushed to L0
6. CFA log_number is now LOG 3
7. CFB has not been flushed and it still referencing LOG 1 prep section
8. CRASH RECOVERY
9. LOG 1 is still around because CFB was referencing LOG 1 prep section
10. Iterate over logs starting at LOG 1
11. CFB has prepared values reinserted into mem, again referencing LOG 1 prep section
12. CFA skips insertion from commit marker in LOG 2 because it is consistent to LOG 3
13. CFB is flushed to L0 and is now consistent to LOG 3
14. LOG 1, LOG 2 can now be released

##### Rebuilding Transactions

As mentioned before, modification of the recovery path only required modification of MemTableInserter to handle the new meta-markers. Because at the time of recovery we can't have access to a full instance of a TransactionDB we must recreate hollow 'shill' transactions. This is essentially  mapping of XID → (WriteBatch, log_number) for all recovered prepared transactions. When we hit a Commit(xid) marker we attempt to look up the shill transaction for this xid and re-insert into Mem. If we hit a rollback(xid) marker we delete the shill transaction. At the end of recovery we are left with a set of all prepared transactions in shill form. We then recreate full transactions from these objects, acquiring the required locks, so that they are available to `GetAllPreparedTransactions()`. Rocks DB is now the the same state is it was before crash/shutdown.

##### Log Lifespan

To find the minimum log that must be kept around we first find the minimum log_number_ of each column family.

We must also consider the minimum value in the prepared sections heap in TransactionDB. This represents the earliest log containing a prep section that has not been committed.

We must also consider the minimum prep section log referenced by all MemTables and ImmutableMemTables that have not been flushed.

The minimum of these three values is the earliest log that still contains data not yet flushed to L0.
