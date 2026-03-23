RocksDB supports both optimistic and pessimistic concurrency controls. The pessimistic transactions make use of locks to provide isolation between the transactions. The default write policy in pessimistic transactions is _WriteCommitted_, which means that the data is written to the DB, i.e., the memtable, only after the transaction is committed. This policy simplified the implementation but came with some limitations in throughput, transaction size, and variety in supported isolation levels. In the below, we explain these in detail and present the other write policies, _WritePrepared_ and _WriteUnprepared_. We then dive into the design of _WritePrepared_ transactions.

# _WriteCommitted_, Pros and Cons

With _WriteCommitted_ write policy, the data is written to the memtable only after the transaction commits. This greatly simplifies the read path as any data that is read by other transactions can be assumed to be committed. This write policy, however, implies that the writes are buffered in memory in the meanwhile. This makes memory a bottleneck for large transactions. The delay of the commit phase in 2PC (two-phase commit) also becomes noticeable since most of the work, i.e., writing to memtable, is done at the commit phase. When the commit of multiple transactions are done in a serial fashion, such as in 2PC implementation of MySQL, the lengthy commit latency becomes a major contributor to lower throughput. Moreover this write policy cannot provide weaker isolation levels, such as READ UNCOMMITTED, that could potentially provide higher throughput for some applications.

# Alternatives: _WritePrepared_ and _WriteUnprepared_

To tackle the lengthy commit issue, we should do memtable writes at earlier phases of 2PC so that the commit phase become lightweight and fast. 2PC is composed of Write stage, where the transaction `::Put` is invoked, the prepare phase, where `::Prepare` is invoked (upon which the DB promises to commit the transaction if later is requested), and commit phase, where `::Commit` is invoked and the transaction writes become visible to all readers. To make the commit phase lightweight, the memtable write could be done at either `::Prepare` or `::Put` stages, resulting into _WritePrepared_ and _WriteUnprepared_ write policies respectively. The downside is that when another transaction is reading data, it would need a way to tell apart which data is committed, and if they are, whether they are committed before the transaction's start, i.e., in the read snapshot of the transaction. _WritePrepared_ would still have the issue of buffering the data, which makes the memory the bottleneck for large transactions. It however provides a good milestone for transitioning from _WriteCommitted_ to _WriteUnprepared_ write policy. Here we explain the design of _WritePrepared_ policy. The changes that make the design to also support _WriteUnprepared_ can be found [here](https://github.com/facebook/rocksdb/wiki/WriteUnprepared-Transactions).

# _WritePrepared_ in a nutshell

These are the primary design questions that needs to be addressed:
1) How do we identify the key/values in the DB with transactions that wrote them?
2) How do we figure if a key/value written by transaction Txn_w is in the read snapshot of the reading transaction Txn_r?
3) How do we rollback the data written by aborted transactions?

With _WritePrepared_, a transaction still buffers the writes in a write batch object in memory. When 2PC `::Prepare` is called, it writes the in-memory write batch to the WAL (write-ahead log) as well as to the memtable(s) (one memtable per column family); We reuse the existing notion of sequence numbers in RocksDB to tag all the key/values in the same write batch with the same sequence number, `prepare_seq`, which is also used as the identifier for the transaction. At commit time, it writes a commit marker to the WAL, whose sequence number, `commit_seq`, will be used as the commit timestamp of the transaction. Before releasing the commit sequence number to the readers, it stores a mapping from `prepare_seq` to `commit_seq` in an in-memory data structure that we call _CommitCache_. When a transaction reading values from the DB (tagged with `prepare_seq`) it makes use of the _CommitCache_ to figure if `commit_seq` of the value is in its read snapshot. To rollback an aborted transaction, we apply the status before the transaction by making another write that cancels out the writes of the aborted transaction.

The _CommitCache_ is a lock-free data structure that caches the recent commit entries. Looking up the entries in the cache must be enough for almost all the transactions that commit in a timely manner. When evicting the older entries from the cache, it still maintains some other data structures to cover the corner cases for transactions that take abnormally too long to finish. We will cover them in the design details below.

# _WritePrepared_ Design

Here we present the design details for _WritePrepared_ transactions. We start by presenting the efficient design for _CommitCache_, and dive into other data structures as we see their importance to guarantee the correctness on top of _CommitCache_.

## _CommitCache_

The question of whether a data is committed or not is mainly about very recent transactions. In other words, given a proper rollback algorithm in place, we can assume any old data in the DB is committed and also is present in the snapshot of a reading transaction, which is mostly a recent snapshot. Leveraging this observation, maintaining a cache of recent commit entries must be sufficient for most of the cases. _CommitCache_ is an efficient data structure that we designed for this purpose.

_CommitCache_ is a fixed-size, in-memory array of commit entries. To update the cache with a commit entry, we first index the `prepare_seq` with the array size and then rewrite the corresponding entry in the array, i.e.: _CommitCache_[`prepare_seq` % `array_size`] = <`prepare_seq`, `commit_seq`>. Each insertion will result into eviction of the previous value, which results into updating `max_evicted_seq`, the maximum evicted sequence number from _CommitCache_. When looking up in the _CommitCache_, if a `prepare_seq` > `max_evicted_seq` and yet not in the cache, then it is considered as not committed. If the entry is otherwise found in the cache, then it is committed and will be read by the transaction with snapshot sequence number `snap_seq` if `commit_seq` <= `snap_seq`. If `prepare_seq` < `max_evicted_seq`, then we are reading an old data, which is most likely committed unless proven otherwise, which we explain below how.

Given 80K tps (transactions per second) of writes, 8M entries in the commit cache (hardcoded in TransactionDBOptions::wp_commit_cache_bits), and having sequence numbers increased by two per transaction, it would take roughly 50 seconds for an inserted entry to be evicted from the _CommitCache_. In practice however the delay between prepare and commit is a fraction of a millisecond and this limit is thus not likely to be met. For the sake of correctness however we need to cover the cases where a prepared transaction is not committed by the time `max_evicted_seq` advances its `prepare_seq`, as otherwise the reading transactions would assume it is committed. To do so, we maintain a heap of prepare sequence numbers called _PreparedHeap_: a `prepare_seq` is inserted upon `::Prepare` and removed upon `::Commit`. When `max_evicted_seq` advances, if it becomes larger than the minimum `prepare_seq` in the heap, we pop such entries and store them in a set called `delayed_prepared_`. Verifying that `delayed_prepared_` is empty is an efficient operation which needs to be done before calling an old `prepare_seq` as committed. Otherwise, the reading transactions should also look into `delayed_prepared_` to see if the `prepare_seq` of the values that they read is found there. Let us emphasis that such cases is not expected to happen in a reasonable setup and hence not negatively affecting the performance.

Although for read-write transactions, they are expected to commit in fractions of a millisecond after the `::Prepare` phase, it is still possible for a few read-only transactions to hang on some very old snapshots. This is the case for example when a transaction takes a backup of the DB, which could take hours to finish. Such read-only transactions cannot assume an old data to be in their reading snapshot, since their snapshot could also be quite old. More precisely, we still need to keep an evicted entry <`prepare_seq`, `commit_seq`> around if there is a live snapshot with sequence number `snap_seq` where `prepare_seq` <= `snap_seq` < `commit_seq`. In such cases we add the `prepare_seq` to `old_commit_map_`, a mapping from snapshot sequence number to a set of `prepare_seq`. The only transactions that would have to pay the cost of looking into this data structure are the ones that are reading from a very old snapshot. The size of this data structure is expected to be very small as i) there are only a few transactions doing backups and ii) there are limited number of concurrent transactions that overlap with their reading snapshot. `old_commit_map_` is garbage collected lazily when the DB is informed of the snapshot release via its periodic fetch of snapshot list as part of the procedure for advancing `max_evicted_seq_`. 

## _PreparedHeap_

As its name suggests, _PreparedHeap_ is a heap of prepare sequence numbers: a `prepare_seq` is inserted upon `::Prepare` and removed upon `::Commit`. The heap structure allows efficient check of the minimum `prepare_seq` against `max_evicted_seq` as was explained above. To allow efficient removal of entries from the heap, the actual removal is delayed until the entry reaches the top of the heap, i.e., becomes the minimum. To do so, the removed entry will be added to another heap if it is not already on top. Upon each change, the top of the two heaps are compared to see if the top of the main heap is tagged for removal.

## Rollback

To rollback an aborted transaction, for each written key/value we write another key/value to cancel out the previous write. Thanks to write-write conflict avoidance done via locks in pessimistic transactions, it is guaranteed that only one pending write will be on each key, meaning that we only need to look at the previous value to figure the state to which we should rollback. If the result of `::Get` from a snapshot with sequence number _kMaxSequenceNumber_, is a normal value (the to-be-rolled-back data will be skipped since they are not comitted) then we do a `::Put` on that key with that value, and if it is a non-existent value, we insert a ::Delete entry. All the values (written by aborted transaction as well as by the rollback step) then commit with the same `commit_seq`, and the `prepare_seq` of aborted transaction and of the rollback batch is removed from _PreparedHeap_.

## Atomic Commit

During a commit, a commit marker is written to the WAL and also a commit entry is added to the _CommitCache_. These two needs to be done atomically otherwise a reading transaction at one point might miss the update into the _CommitCache_ but later sees that. We achieve that by updating the _CommitCache_ before publishing the sequence number of the commit entry. In this way, if a reading snapshot can see the commit sequence number it is guaranteed that the _CommitCache_ is already updated as well. This is done via a `PreReleaseCallback` that is added to `::WriteImpl` logic for this purpose. `PreReleaseCallback` is also used to add `prepare_seq` to _PreparedHeap_ so that its top always represents the smallest uncommitted transaction. (Refer to _Smallest Uncommitted_ Section to see how this is used).

When we have two write queues (`two_write_queues`=`true`) then the primary write queue can write to both WAL and memtable and the 2nd one can write only to the WAL, which will be used for writing the commit marker in `WritePrepared` transactions. In this case the primary queue (and its `PreReleaseCallback` callback) is always used for prepare entires and the 2nd queue (and its `PreReleaseCallback` callback) is always used only for commits. This i) avoids race condition between the two queues, ii) maintains the in-order addition to _PreparedHeap_, and iii) simplifies the code by avoiding concurrent insertion to _CommitCache_ (and the code that is called upon each eviction from it).

Since the last sequence number could advance by either queue while the other is not done with the reserved lower sequence number, this could raise an atomicity issue. To address that we introduce the notion of last published sequence number, which will be used when taking a snapshot. When we have one write queue, this is the same as the last sequence number and when we have two write queues this is the last committed entry (performed by the 2nd queue). This restriction penalizes non-2PC transactions by splitting them to two steps: i) write to memtable via the primary queue, ii) commit and publish the sequence number via the 2nd queue.

## IsInSnapshot

`IsInSnapshot(prepare_seq, snapshot_seq)` implements the core algorithm of _WritePrepared_, which puts all the data structures together to determine if a value tagged with `prepare_seq` is in the reading snapshot `snapshot_seq`.

Here is the simplified version of `IsInSnapshot` algorithm:

    inline bool IsInSnapshot(uint64_t prep_seq, uint64_t snapshot_seq,
                             uint64_t min_uncommitted = 0,
                             bool *snap_released = nullptr) const {
      if (snapshot_seq < prep_seq) return false;
      if (prep_seq < min_uncommitted) return true;
      max_evicted_seq_ub = max_evicted_seq_.load();
      some_are_delayed = delayed_prepared_ not empty
      if (prep_seq in CommitCache) return CommitCache[prep_seq] <= snapshot_seq;
      if (max_evicted_seq_ub < prep_seq) return false; // still prepared
      if (some_are_delayed) {
         ...
      }
      if (max_evicted_seq_ub < snapshot_seq) return true; // old commit with no overlap with snapshot_seq
      // commit is old so is the snapshot, check if there was an overlap
      if (snaoshot_seq not in old_commit_map_) {
        *snap_released = true;
        return true;
      }
      bool overlapped = prepare_seq in old_commit_map_[snaoshot_seq];
      return !overlapped;
    }
It returns true if it can determine that `commit_seq` <= `snapshot_seq` and false otherwise.
- `snapshot_seq` < `prep_seq` => `commit_seq` > `snapshot_seq` because `prep_seq` <= `commit_seq`
- `prep_seq` < `min_uncommitted` => `commit_seq` <= `snapshot_seq`
- Checking emptiness of `delayed_prepared_` in `some_are_delayed` before _CommitCache_ lookup is an optimization to skip acquiring lock on it if there is no delayed transaction in the system, as it is the norm.
- If not in _CommitCache_ and none of the delayed prepared cases apply, then this is an old commit that is evicted from _CommitCache_.
   * `max_evicted_seq_` < `snapshot_seq` => `commit_seq` < `snapshot_seq` since `commit_seq` <= `max_evicted_seq_`
   * Otherwise, `old_commit_map_` includes all such old snapshots as well as any commit that overlaps with them.

In the below we see the full implementation of `IsInSnapshot` that also covers the corner cases.
`IsInSnapshot(prepare_seq, snapshot_seq)` implements the core algorithm of _WritePrepared_, which puts all the data structures together to determine if a value tagged with `prepare_seq` is in the reading snapshot `snapshot_seq`.

    inline bool IsInSnapshot(uint64_t prep_seq, uint64_t snapshot_seq,
                             uint64_t min_uncommitted = 0,
                             bool *snap_released = nullptr) const {
      if (snapshot_seq < prep_seq) return false;
      if (prep_seq < min_uncommitted) return true;
      do {
        max_evicted_seq_lb = max_evicted_seq_.load();
        some_are_delayed = delayed_prepared_ not empty
        if (prep_seq in CommitCache) return CommitCache[prep_seq] <= snapshot_seq;
        max_evicted_seq_ub = max_evicted_seq_.load();
        if (max_evicted_seq_lb != max_evicted_seq_ub) continue;
        if (max_evicted_seq_ub < prep_seq) return false; // still prepared
        if (some_are_delayed) {
          if (prep_seq in delayed_prepared_) {
            // might be committed but not added to commit cache yet
            if (prep_seq not in delayed_prepared_commits_) return false;
            return delayed_prepared_commits_[prep_seq] < snapshot_seq;
          } else {
            // 2nd probe due to non-atomic commit cache and delayed_prepared_
            if (prep_seq in CommitCache) return CommitCache[prep_seq] <= snapshot_seq;
            max_evicted_seq_ub = max_evicted_seq_.load();
          }
        }
      } while (UNLIKELY(max_evicted_seq_lb != max_evicted_seq_ub));
      if (max_evicted_seq_ub < snapshot_seq) return true; // old commit with no overlap with snapshot_seq
      // commit is old so is the snapshot, check if there was an overlap
      if (snaoshot_seq not in old_commit_map_) {
        *snap_released = true;
        return true;
      }
      bool overlapped = prepare_seq in old_commit_map_[snaoshot_seq];
      return !overlapped;
    }
- Since `max_evicted_seq_` and _CommitCache_ are updated separately, the while loop simplifies the algorithm by ensuring that `max_evicted_seq_` is not changed during _CommitCache_ lookup.
- The commit of a delayed prepared involves four non-atomic steps: i) update _CommitCache_ ii) add to `delayed_prepared_commits_`, iii) publish sequence, and iv) remove from `delayed_prepared_`.
   - If the reader simply follows _CommitCache_ lookup + `delayed_prepared_` lookup order, it might found a delayed prepared in neither and miss checking against its `commit_seq`. So address that if the sequence was not found in `delayed_prepared_`, it still does a 2nd lookup in _CommitCache_. The reverse order ensures that it will see the commit if there was any.
   * There are odd scenarios where the commit of a delayed prepared could be evicted from commit cache before the entry is removed from `delayed_prepared_` list. `delayed_prepared_commits_` which is updated every time a delayed prepared is evicted from commit cache helps not to miss such commits.


## Flush/Compaction

Flush/Compaction threads, similarly to reading transaction, make use of `IsInSnapshot` API to figure which versions can be safely garbage collected without affecting the live snapshots. The difference is that a snapshot might be already released by the time the compaction is calling `IsInSnapshot`. To address that, if `IsInSnapshot` is extended with `snap_released` argument so that if it could not confidently give a true/false response, it will signal the caller that the `snapshot_seq` is no longer valid.

## Duplicate keys

WritePrepared writes all data of the same write batch with the same sequence number. This is assuming that there is no duplicate key in the write batch. To be able to handle duplicate keys, we divide a write batch to multiple sub-batches, one after each duplicate key. The memtable returns false if it receives a key with the same sequence number. The mentable inserter then advances the sequence number and tries again.

The limitation with this approach is that the write process needs to know the number of sub-batches beforehand so that it could allocate the sequence numbers for each write accordingly. When using transaction API this is done cheaply via the index of WriteBatchWithIndex as it already has mechanisms to detect duplicate insertions. When calling `::CommitBatch` to write a batch directly to the DB, however, the DB has to pay the cost of iterating over the write batch and count the number of sub-batches. This would result into a non-negligible overhead if there are many of such writes.

Detecting duplicate keys requires knowing the comparator of the column family (cf). If a cf is dropped, we need to first make sure that the WAL does not have an entry belonging to that cf. Otherwise if the DB crashes afterwards the recovery process will see the entry but does not have the comparator to tell whether it is a duplicate.

# Optimizations

Here we cover the additional details in the design that were critical in achieving good performance.

## Lock-free _CommitCache_

Most reads from recent data result into a lookup into _CommitCache_. It is therefore vital to make _CommitCache_ efficient for reads. Using a fixed array was already a conscious design decision to serve this purpose. We however need to further avoid the overhead of synchronization for reading from this array. To achieve this, we make _CommitCache_ an array of std::atomic<uint64_t> and encode <`prepare_seq`, `commit_seq`> into the available 64 bits. The reads and writes from the array are done with std::memory_order_acquire and std::memory_order_release respectively. In an x86_64 architecture these operations are translated into simple reads and writes into memory thanks to the guarantees of the hardware cache coherency protocol. We have other designs for this data structure and will explore them in future.

To encode <`prepare_seq`, `commit_seq`> into 64 bits we use this algorithm: i) the higher bits of `prepare_seq` is already implied by the index of the entry in the _CommitCache_; ii) the lower bits of prepare seq are encoded in the higher bits of the 64-bit entry; iii) the difference between the `commit_seq` and `prepare_seq` is encoded into the lower bits of the 64-bit entry.

## Less-frequent updates to `max_evicted_seq`

Normally `max_evicted_seq` is expected to be updated upon each eviction from the _CommitCache_. Although updating `max_evicted_seq` is not necessarily expensive, the maintenance operations that come with it are. For example, it requires holding a mutex to verify the top in _PreparedHeap_ (although this can be optimized to be done without a mutex). More importantly, it involves holding the db mutex for fetching the list of live snapshots from the DB since maintaining `old_commit_map_` depends on the list of live snapshots up to `max_evicted_seq`. To reduce this overhead, upon each update to `max_evicted_seq` we increase its value further by 1% of the _CommitCache_ size (if it does not exceed last publish sequence number), so that the maintenance is done 100 times before _CommitCache_ array wraps around rather than once per eviction.

## Lock-free Snapshot List

In the above, we mentioned that a few read-only transactions doing backups are expected at each point of time. Each evicted entry, which is expected upon each insert, therefore needs to be checked against the list of live snapshots (which was taken when `max_evicted_seq` was last advanced). Since this is done frequently, it needs to be done efficiently, without holding any mutex. We therefore design a data structure that lets us perform this check in a lock-free manner. To do so, we store the first S snapshots (S hardcoded in TransactionDBOptions::wp_snaoshot_cache_bits to be 128) in an array of std::atomic<uint64_t>, which we know is efficient for reads and write on x86_64 architecture. The single writer updates the array with a list of snapshots sorted in ascending order by starting from index 0, and updates the size in an atomic variable.

We need to guarantee that the concurrent reader will be able to read all the snapshots that are still valid after the update. Both new and old lists are sorted and the new list is a subset of the previous list plus some new items. Thus if a snapshot repeats in both new and old lists, it will appear with a lower index in the new list. So if we simply insert the new snapshots in order, if an overwritten item is still valid in the new list, it is either written to the same place in the array or it is written in a place with a lower index before it gets overwritten by another item. This guarantees a reader that reads the array from the other side will eventually see a snapshot that repeats in the update, either before it gets overwritten by the writer or afterwards. 

If the number of snapshots exceed the array size, the remaining updates will be stored in a vector protected by a mutex. This is just to ensure correctness in the corner cases and is not expected to happen in a normal run.

## Smallest Uncommitted

We keep track of smallest uncommitted data and store it in the snapshot. When reading the data if its sequence number lower than the smallest uncommitted data, then we skip lookup into CommitCache to reduce cpu cache misses. The smallest entry in `delayed_prepared_` will represent the _Smallest UnCommitted_ if it is not empty. Otherwise, which is almost always the case, the _PreparedHeap_ top represents the smallest uncommitted thanks to the rule of adding entires to it in ascending order by doing so only via the primary write queue. _Smallest UnCommitted_ will also be used to limit _ValidateSnapshot_ search to memtables when it knows that _Smallest UnCommitted_ is already larger than smallest sequence in the memtables.

## rocksdb_commit_time_batch_for_recovery

As explained above, we have specified that commits are only done via the 2nd queue. When the commit is accompanied with `CommitTimeWriteBatch` however, then it would write to memtable too, which essentially would send the batch to the primary queue. In this case we do two separate writes to finish the commit: one writing `CommitTimeWriteBatch` via the main queue, and two committing that as well as the prepared batch via the 2nd queue. To avoid this overhead the users can set `rocksdb_commit_time_batch_for_recovery` configuration variable to true which tells RocksDB that such data will only be required during recovery. RocksDB benefits from that by writing the `CommitTimeWriteBatch` only to the WAL. It still keeps the last copy around in memory to write it to the SST file after each flush. When using this option `CommitTimeWriteBatch` cannot have duplicate entries since we do not want to pay the cost of counting sub-batches upon each commit request.

# Experimental Results

Here is a summary of improvements on some sysbench benchmarks as well as linkbench (done via MyRocks).
* benchmark...........tps.........p95 latency....cpu/query
* insert...................68%
* update-noindex...30%......38%
* update-index.......61%.......28%
* read-write............6%........3.5%
* read-only...........-1.2%.....-1.8%
* linkbench.............1.9%......+overall........0.6%

Here are also the detailed results for [In-Memory Sysbench](https://gist.github.com/maysamyabandeh/bdb868091b2929a6d938615fdcf58424) and [SSD Sysbench](https://gist.github.com/maysamyabandeh/ff94f378ab48925025c34c47eff99306) courtesy of [@mdcallag](https://github.com/mdcallag).

# Current Limitations

There is ~1% overhead for read workloads. This is due to the extra work that needs to be done to tell apart uncommitted data from committed ones.

The rollback of merge operands is currently disabled. This is to hack around a problem in MyRocks that does not lock the key before using the merge operand on it.

Currently `Iterator::Refresh` is not supported. There is no fundamental obstacle and it can be added upon request.

Although the DB generated by _WritePrepared_ policy is backward/forward compatible with the classic _WriteCommitted_ policy, the WAL format is not. Therefore to change the _WritePolicy_ the WAL has to be empties first by flushing the DB.

Non-2PC transactions will result into two writes if `two_write_queues` is enabled. If majority of transactions are non-2PC, the `two_write_queues` optimization should be disabled.

TransactionDB::Write incurs additional cost of detecting duplicate keys in the write batch.

If a column family is dropped, the WAL has to be cleaned up beforehand so that there would be no entry belonging to that CF in the WAL.

When using `rocksdb_commit_time_batch_for_recovery`, the data passed to `CommitTimeWriteBatch` cannot have duplicate keys and will be visible only after a memtable flush.