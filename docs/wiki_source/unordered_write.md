# Unordered Writes
This document summarizes the design of `DBOptions.unordered_write` feature, and explains how WritePrepared transactions still provides ordering in presence of `unordered_write=true`. The feature offers 40-130% higher write throughput compared to vanilla rocksdb. 

## Background

When RocksDB executes write requests coming from concurrent write threads, it groups the write threads, assigns order to them, optionally writes them to WAL, and then performs each of the writes to memtables, either serially or concurrently (when `DBOptions.allow_concurrent_memtable_write=true`). The leader of the write group then waits for the writes to finish, updates the `last_visible_seq` to the last sequence number in the write group, and then let individual write threads to resume. At this point the next write group could be formed.
When the user takes a snapshot of DB, it will be given the `last_visible_seq`. When the user reads from the snapshot, it only reads values with a sequence number <= the snapshot’s sequence number
This simple design offers powerful guarantees:

* **Atomic reads**: Either all of a write batch is visible to reads or none of it. This is thanks to the write group advancing the last visible sequence number to the end of the write batch in the group (in contrast to in the middle of a write batch).
* **Read-your-own writes**: When a write thread returns to the user, a subsequent read by the same thread will be able to see its own writes. This is because the write thread doesn’t return until the leader of the write group updates the last visible sequence number to be larger or equal to the sequence number that is assigned to the write batch of the write thread.
* **Immutable Snapshots**: Since `last_visible_seq` is not advanced until all the writes in the write group are finished, the reads visible to the snapshot are immutable in the sense that it will not be affected by any in-flight or future writes as they would be performed with a sequence number larger than that of the snapshot.

The downside of this approach is that the entire write group has to wait for the slowest memtable write to finish before (i) it can return to the user, or (ii) the next write group can be formed. This negatively impacts the rocksdb’s write throughput.

## Ordering in WritePrepared Txns

When configured with `DBoptions.two_write_queues=true`, *non-2pc* writes in WritePrepared Txns are performed in two steps:

1. The write batch is written to the underlying DB with the sequence number `prepare_seq` via the main write queue. The write group advances `last_visible_seq` as usual.
2. The `prepare_seq` → `commit_seq` pair is added to a commit table via a 2nd write queue. The `commit_seq` is a normal sequence number allocated within the write queue. The write group advances a new global variable called `last_published_seq` to be the last `commit_seq` in the write group.

The snapshots are fed with `last_published_seq` (in contrast with `last_visible_seq`). The reads will return that latest value with a `commit_seq` <= the snapshot sequence number. The `commit_seq` is obtained by doing a lookup in the commit table using the sequence number of each value.
Note that the order in this approach is specified by `commit_seq`, which is assigned in the 2nd write thread. The snapshot also depends on the `last_published_seq` that is updated in the 2nd write thread. Therefore the ordering that core rocksdb provides in the first write group is redundant, and without that the users of TransactionDB would still see ordered writes and enjoy reads from immutable snapshots.

## unordered_write: Design

With `unordered_write=true`, the writes to the main write queue in rocskdb goes through a different path:

1. Form a write group
2. The leader in the write group orders the writes, optionally persist them in the WAL, and updates `last_visible_seq` to the sequence number of the last write batch in the group.
3. The leader resumes all the individual write threads to perform their writes into memtable.
4. The next write group is formed while the memtable writes of the previous ones are still in flight.

Note that this approach still gives read-your-own-write properties but not atomic reads nor the immutable snapshot property. However as explained above, TransactionDB configured with WritePrepared transactions and two_write_queues is not affected by that as it uses a 2nd write queue to provide immutable snapshots to its reads.

* **read-your-own-write**: When a write thread returns to the user, its write batch is already placed in the memtables with sequence numbers lower than the snapshot sequence number. Therefore the writes a write thread is always visible to its subsequent reads.
* **immutable snapshots**: The reads can no longer benefit from immutable snapshots since a snapshot is fed with a sequence number larger than that of upcoming or in-flight writes. Therefore, reads from that snapshots will see different views of the db depending on the subset of those in-flight writes that are landed by the time the read is performed.
* **atomic writes**: is no longer provided since the `last_visible_seq` which is fed to snapshot is larger than the sequence numbers of keys in upcoming insertion of the write batch to the memtable. Therefore each prefix of the write batch that is landed on memtables is immediately visible to the readers who read from that snapshot.

If the user can tolerate the relaxed guarantee they can enjoy the higher throughput of `unordered_write` feature. Otherwise, they would need to implement their own mechanism to advance the snapshot sequence number to a value that is guaranteed to be larger than any in-flight write. One approach is to use TransactionDB configured with WritePrepared and `two_write_queues` which would still offer considerably higher throughput than vanilla rocksdb.

## unordered_write: Implementation

* If `unordered_write` is true, the writes are first redirected to `WriteImplWALOnly()` on the primary write queue where it:
    * groups the write threads
    * if a threshold is reached that requires memtable flush
        * wait on switch_cv_ until `pending_memtable_writes_.load() == 0`
        * Flush memtable
    * persists the writes in WAL if it is enabled
    * increases `pending_memtable_writes_` with number of write threads that will later write to memtable
    * updates `last_visible_seq` to the last sequence number in the write group
* If the the write thread needs to write to memtable, it calls `UnorderedWriteMemtable()` to
    * write to memtable (in concurrent with other in-flight writes)
    * decrease `pending_memtable_writes_`
    * If (`pending_memtable_writes_.load() == 0`) call `switch_cv_.notify_all()`

## WritePrepared Optimizations with unordered_write

WritePrepared transaction by default makes use of a lock table to prevent write-write conflicts. This feature is however extra when TransactionDB is used only to provide ordering for vanilla rocksdb users, and can be disabled with `TransactionDBOption::skip_concurrency_control=true`.
The only consequence of skipping concurrency control is the following anomaly after a restart, which does not seem to be problematic for vanilla rocksdb users:

* Thread t1 writes Value V1 to Key K and to K’
* Thread t2 writes Value V2 to Key K
* The two are are grouped together and written to the WAL with <t1, t2> order
* The two writes are however committed in opposite order: <Commit(t2), Commit(t1)>
* The readers might see {K=V2} or {K=V1,K’=V1} depending on their snapshot
* DB restarts
* During recovery the commit order is dictated by WAL write order: <Commit(t1) Commit(t2)>
* The reader see this as the db state: {K=V2,K’=V1} which is different than the DB state before the restart.

## Experimental Results

Benchmark:
```
TEST_TMPDIR=/dev/shm/ ~/db_bench --benchmarks=fillrandom --threads=32 --num=10000000 -max_write_buffer_number=16 --max_background_jobs=64 --batch_size=8 --writes=3000000 -level0_file_num_compaction_trigger=99999 --level0_slowdown_writes_trigger=99999 --level0_stop_writes_trigger=99999 -enable_pipelined_write=false -disable_auto_compactions --transaction_db=true --unordered_write=1 --disable_wal=1
```

Throughput with `unordered_write=true` and using WritePrepared transaction
- WAL: +42%
- No-WAL: +34%

Throughput with unordered_write=true
- WAL: +63%
- NoWAL: +131%

Note: this is an upper-bound on the improvement as it improves only the bottleneck of writing to memtable while the write throughput could also be bottlenecked by compaction speed and IO too.


## Future work

* Optimize WritePrepared
    * WritePrepared adds `prepare_seq` to a heap. The main reason is to provide correctness for an almost impossible scenario that the commit table flows over `prepare_seq` before updated with `prepare_seq` → `commit_seq`. The contention of the shared heap structure is currently a major bottleneck.
* Move WritePrepared engine to core rocksdb
    * To enjoy the ordering of WritePrepared, the users currently need to open their db with `TransactionDB::Open`. It would be more convenient if the feature if enabled with setting an option on the vanilla rocksdb engine.
* Alternative approaches to ordering
    * The gap between the write throughput of vanilla rocksdb with `unordered_write=true` and the throughput of WritePrepared with `unordered_write` is significant. We can explore simpler and more efficient ways of providing immutable snapshots in presence of unordered_write feature.

## FAQ

*Q*: Do we need to use 2PC with WritePrepared Txns?

*A*: No. All the user needs to do is to open the same db with TransactionDB, and use it with the same standard API of vanilla rocksdb. The 2PC’s feature of WritePrepared Txns is irrelevant here and can be ignored.

*Q*: 2nd write queue in WritePrepared transactions also does ordering between the commits. Why does not it suffer from the same performance problem of the main write queue in vanilla rocksdb?

*A*: The write to commit table is mostly as fast as updating an element in an array. Therefore, it is much less vulnerable to the slow writes problem that is hurting the throughput of ordering in vanilla rocksdb.

*Q:* Is memtable size accurately enforced in unordered_writes?

*A:* Not as much as before. From the moment that the threshold is reached until we wait for in-flight writes to finish, the memtable size could increase beyond the threshold. 