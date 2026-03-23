This document presents the initial design for moving memtable writes from the prepare phase to unprepared phase when the write batch is still being written to.

# Goals

These are the goals of the project:

1. Reduce memory footprint, which would also enable handling very large transactions.
2. Avoid write stalls caused by large transactions writing to DB at once.

# Synopsis

Transactions are currently buffered in memory until the prepare phase of 2PC. When the transaction is large, so is the buffered data, and writing such large buffered data to DB at once negatively affects the concurrent transactions. Moreover, buffering the entire data of very large transactions can run the server out of memory. Top contributors to memory footprint are i) the buffered key/values, i.e., the write batch, ii) the per-key locks. In this design we divide the project to three stages reducing the memory usage of i) values, ii) keys, iii) locks in them respectively.

To eliminate the memory usage of buffered values in large transaction, unprepared data will be gradually batched into unprepared batches and written into the database while the write batch is still being built. The keys are still buffered though to simplify the rollback algorithm. When a transaction commits, the commit cache is updated with an entry per unprepared batch. The main challenges are handling the rollback and read-your-own-writes.

Transactions need to be able to read their own uncommitted data. Previously this was done by looking into the buffered data before looking into the DB. We address this challenge by keeping the track of the sequence numbers of the written unprepared batches to the disk, and augment ReadCallback to return true if the sequence number of the read data matches them. This applies only to large transactions that do not have the write batch buffered in memory.

The current rollback algorithm in WritePrepared can work only after recovery when there is no live snapshots. In WriteUnPrepared however transactions can rollback in presence of live snapshots. We redesign the rollback algorithm to i) append to the transaction the prior values of the keys that are modified by the transaction, hence essentially cancelling the writes, ii) commit the rolled back transaction. Treating a rollback transaction as committed greatly simplified the implementation as the existing mechanisms of handling live snapshots with committing transaction can seamless apply. WAL will still include a rollback marker to enable the recovery procedure to reattempt the rollback should the DB crash in the middle. To determine the set of prior values, the list of modified keys must be tracked in the Transaction object. In the absence of the buffered write batch, in the first stage of the project we still buffer the modified key set. In the 2nd stage, we retrieve the key set from the WAL when the transaction is large.

RocksDB tracks the list of keys locked in a transaction in TransactionLockMgr. For large transactions, the list of locked keys will not fit in memory. Automatic lock escalation into range locks can be used to approximate a set of point locks. When RocksDB detects that a transaction is locking a large number keys within a certain range, it will automatically upgrade it to a range lock. We will tackle this in the 3rd stage of the project. More details in Section “Key Locking” below.

# Stages

The plan is to execute this project in 3 stages:

1. Implement unprepared batches, but keys are still buffered in memory for rollback and for key-locking.
2. Use the WAL to obtain the key set during rollback instead of buffering in memory.
3. Implement range lock mechanism for large transactions.

# Implementation

## WritePrepared Overview

In the existing WritePrepared policy, the data structures are:

* PrepareHeap: A heap of in-progress prepare sequence numbers.
* CommitCache: a mapping between prepare sequence number to commit seq.
    * max_evicted_seq_: the largest evicted sequence number from the commit cache.
* OldCommitMap: list of evicted entries from the commit cache if they are still invisible to some live snapshots.
* DelayedPrepared: list of prepared sequence numbers from PrepareHeap that are less than max_evicted_seq_.

## Put

The writes into the database will need to be batched to avoid the overhead of going through write queues. To avoid confusion with “write batches”, these will be called “unprepared batches”. By batching, we also save on the number of unprepared sequence numbers that we have to generate and track in our data structures. Once a batch reaches a configurable threshold, it'll be written as if it were a prepare operation in WritePreparedTxn, except that a new WAL type called BeginUnprepareXID will be used as opposed to BeginPersistedPrepareXID used by the WritePreparedTxn policy. All the keys in the same unprepared batch will get the same sequence number (unless there is a duplicate key, which would divide the batch into multiple sub-batches).

Proposed WAL format:
[BeginUnprepareXID]...[EndPrepare(XID)]

This implies that we'll need to know the XID of the transaction (where XID is a name uniq during lifetime of the transaction) before final prepare (this is true for MyRocks, since we generate the XID of the transaction when it begins).

The Transaction object will need to track the list of unprepared batches that have written to the db. To do this, the Transaction object will contain a set of unprep_seq numbers, and when an unprepared batch is written, the unprep_seq is added to the set.

On unprepared batch write, the unprep_seq number is also added to the unprepared heap (similar to prepare heap in WritePreparedTxn).

## Prepare

On prepare we will write out the remaining entries in the current write batch to the WAL but with using the BeginPersistedPrepareXID marker instead to denote that the transaction is now prepared. This is needed so that on crash, we can return to the application the list of prepared transactions so that the application can perform the correct action. Unprepared transactions will be implicitly rolled backed on recovery.

Proposed WAL format:
[BeginUnprepareXID]...[EndPrepare(XID)]
...
[BeginUnprepareXID]...[EndPrepare(XID)]
...
[BeginPersistedPrepareXID]...[EndPrepare(XID)]
...
...[Commit(XID)]

In this case, the final prepare gets BeginPersistedPrepareXID instead of BeginUnprepareXID to denote that the transaction has been truly prepared.

Note that although the DB (sst files) are backward- and forward-compatible between WritePreparedTxn and WriteUnpreparedTxn, the WAL of WriteUnpreparedTxn is not forward-compatible with that of WritePreparedTxn: WritePreparedTxn will successfully fail on recovering from WAL files generated by WriteUnpreparedTxn because of the new marker types. However, WriteUnpreparedTxn is still fully backward compatible with WritePreparedTxn and will be able to read WritePreparedTxn WAL files, since they will look the same.

## Commit

On commit, the commit map and unprepared heap need to be updated. For WriteUnprepared, a commit will potentially have multiple prepare sequence numbers associated with it. All (unprep_seq, commit_seq) pairs must be added to the commit map and all unprep_seq must be removed from the unprepare_heap.

If commit is performed without a prepare and the transaction has not previously written unprepared batches, then the current unprepared batch of writes will be written out directly similar to CommitWithoutPrepareInternal in the WritePreparedTxn case. If the transaction has already written unprepared batches, then an implicit prepare phase is added.

## Rollback

In WritePreparedTxn, rollback implementation was limited to only rollbacks right after recovery. It was implemented like this:

1. prep_seq = seq at which the prepared data was written
2. For each modified key, read the original values at prep_seq - 1
3. Write the original values back but at a new sequence number, rollback_seq
4. rollback_seq is added to commit map
5. prep_seq is then removed from PrepareHeap.

This implementation would not work if there are live snapshots to which prep_seq is visible. This is because if max_evicted_seq advances beyond prep_seq, we could have prep_seq < max_evicted_seq < snaphot_seq < rollback_seq. Then, the transaction reading at snapshot_seq would assume data at prep_seq were committed since prep_seq < max_evicted_seq and yet it is not recorded in old_commit_map. 

This shortcoming in WritePreparedTxn was tolerated because MySQL would only rollback prepared transactions on recovery, where there would be no live snapshots to observe this inconsistency. In WriteUnpreparedTxn, however, this scenario occurs not just on recovery, but also on user-initiated rollbacks of unprepared transactions.

We fix the issue by writing a rollback marker, appending the rollback data to the aborted transaction, and then committing the transaction in the commit map. Since the transaction is appended with rolled back data, although committed, it does not change the state of db, and hence is effectively rolled back. If max_evicted_seq advances beyond prep_seq, since the <prep_seq, commit_seq> is added to the CommitCache, the existing procedure, i.e, adding evicted entry to old_commit_map, will take care of live snapshots with prep_seq < snapshot_seq < commit_seq. If it crashed in the middle of rollback, on recovery it reads the rollback marker and finishes up the rollback process.

If the DB crashes in the middle of rollback, the recovery will see some partially written rolled back data in the WAL. Since the recovery process will eventually reattempt the rollback, such partial data will be simply overwritten with the new rolled back batch containing the same prior values.

The rollback batch could be written either at once or divided into multiple sub-patches in the case of a very large transaction. We will explore this option further during the implementation.

The other issue with rollbacks in WriteUnpreparedTxn is knowing which keys to rollback. Previously, since the whole prepare batch was buffered in memory, it was possible to just iterate over the write batch to find the set of modified keys that need to be rolled back. In the first iteration of the project, we still keep the write key-set in the memory. In the next iteration, if the size of the key-set goes beyond a threshold, we purge the key set from memory and retrieve it from the WAL should the transaction aborts later. Every transaction already tracks a list of unprepared sequence numbers, and that can be used to seek into the correct position in the WAL.

## Get

The read path is largely the same as WritePreparedTxn. The only difference is for transactions being able to read its own writes. Currently, GetFromBatchAndDB handles this by first checking the write batch first before fetching from the DB where ReadCallback is called to determine visibility. In the absence of write batch we need another mechanism to handle this.

Recall that every transaction maintains a list of unprep_seq. Before entering the main visibility logic as described in WritePreparedTxn, check if the key has a sequence number that exists in the set of unprep_seq. If it is present, then the key is visible. This logic happens in ReadCallback which currently does not take a set of sequence numbers, but this can be extended so that the set of unprep_seq can be passed down.

Currently, Get and Seek will seek directly to the sequence number specified by the snapshot when reading from DB, so uncommitted data written by the same transaction is potentially skipped before visibility logic can even be applied. To resolve this issue, this optimization would have to be removed if the current transaction is large enough to have its buffered write batch removed and instead written to the DB as unprepared batches.

## Recovery

Recovery will work similarly to WritePreparedTxn except with a few changes to how we determine transaction state (unprepared, prepared, aborted, committed).

During recovery, unprepared batches with the same XID must be tracked until the EndPrepare marker is observed. If recovery finishes without EndPrepare, then the transaction is unprepared and the equivalent of an application rollback is implicitly done.

If recovery ends with EndPrepare, but there is no commit marker, then the transaction is prepared, and is presented to the application.

If a rollback marker is found after EndPrepare but there is no commit marker, then the transaction is aborted, and the recovery process must overwrite the aborted data with their prior values.

If a commit marker is found, then the transaction is committed.

## Delayed Prepared

Large transactions will probably have long duration as well. If a transaction is not committed after a long time (1 min for a 100 kTPS workload), its sequence number is moved to DelayedPrepared, which is currently a simple set protected by a lock. If it turns out that the current implementation is a bottleneck, we will change DelayedPrepared from a set into a sharded hash table, similar to how transaction key locking is done. If it works well enough for key locking (which occurs more frequently), it should work fine for tracking prepared transactions.

## Key Locking

Currently, RocksDB supports point locks via a sharded hashtable in TransactionLockMgr. Every time a lock is requested, the key is hashed and a lookup is done to see if an existing lock with the same key exists. If so, the thread blocks, otherwise, the lock is granted and inserted into the hashtable. This implies that all locked keys will reside in memory, which may be problematic for large transactions.

To mitigate this problem, range locks can be used to approximate the large set of point locks when a transaction is detected to have locked many keys within a range. Here we present a preliminary approach to tackle this problem to show its feasibility. When reaching this stage of the project we will reconsider alternative designs, and/or whether the parallel Gap Locking has already eliminated the problem.

To support range locks, the key space will need to be partitioned in N logical partitions, where every partition represents a contiguous range in the key space. A partition key will represent every partition and can be calculated from the key itself through a callback provided by the application. A partition key is automatically write-locked if the number of locked keys in that partition goes beyond a threshold; in that case the individual keys are released.

A set will be used to hold the set of all partitions. A partition will have the following struct:
struct Partition {
    map<TransactionID, int> txn_locks;
    enum { UNLOCKED, LOCKED, LOCK_REQUEST } status = UNLOCKED;
    std::shared_ptr<TransactionDBMutex> part_mutex;
    std::shared_ptr<TransactionDBCondVar> part_cv;
    int waiters = 0;
};

When a lock for a key is requested:

1. Lookup the corresponding partition structure. If it does not exist, then create and insert it. If status is UNLOCKED, then increment with txn_locks[id]++, otherwise increment waiters and block on part_cv. Decrement waiters and repeat this step when the thread is woken.
2. Request a point lock on the point lock hashtable.
    1. If the point lock is granted, then check if the threshold is exceeded by looking at txn_locks[id].
    2. If point lock times out, decrement with txn_locks[id]--.

To upgrade a lock:

1. Set partition status to LOCK_REQUEST, increment waiters, and block on part_cv until txn_locks only contains the current transaction. Decrement waiters and recheck txn_locks when woken.
2. Set status to LOCKED.
3. Remove all point locks in that partition from the point lock hashtable. The point locks to remove can be determined by tracked_keys_.
4. Update tracked_keys_ to remove all point locks in that partition and add the partition lock in tracked_keys_.


To unlock a point lock:

1. Remove the point lock from the point lock hashtable.
2. Decrement txn_locks in the corresponding partition.
3. Signal on part_cv. if waiters is nonzero. Otherwise remove the partition.


To unlock a partition lock (no user API to trigger this, but this happens when transactions end):

1. Signal on part_cv if waiters is nonzero. Otherwise, remove the partition.

Note that any time data from partition is being read, its mutex part_mutex must be held.