Many users implement a queue service using RocksDB. In these services, from each queue, new items are added with a higher sequence ID and removed from the smallest sequence ID. Users usually read from a queue in sequence ID increasing order.

### Key Encoding
You can simply encode it as `<queue_id, sequence_id>`, where queue_id is fixed length encoded and sequence_id is encoded as big endian.

While iterating keys, a user can create an iterator and seek to `<queue_id, target_sequence_id>` and iterate from there. 

### Old Item Deletion Problem
Since the oldest items are deleted, there can be a large amount of "tombstones" in the beginning of each `queue_id`. As a result, the two queries might be exceptionally slow:
* Seek(`<queue_id, 0>`)
* While you are in the last sequence ID of a `queue_id` and try to call Next()

To mitigate a problem, you can remember the first and last sequence ID of each queue_id, and never iterate over the range.

As another way to solve the second problem, you can set an end key of your iterate when you iterate inside a queue_id, by letting `ReadOptions.iterate_upper_bound` point to `<queue_id, max_int>` `<queue_id + 1>`. We encourage you always set it no matter whether you see the slowness problem caused by deletions.

### Checking new sequence IDs of a queue_id
If a user finishes processing the last `sequence_id` of a `queue_id`, and keep polling new item to be created, just Seek(`<queue_id, last_processed_id>`) and call Next() and see whether the next key is still for the same `<queue_id>`. Make sure `ReadOptions.iterate_upper_bound` points to `<queue_id + 1>` to avoid slowness for the item deletion problem.

If you want to further optimize this use case, to avoid binary search of the whole LSM tree each time, consider using `TailingIterator`(or `ForwardIterator` called in some parts of the codes) (https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h#L1235-L1241).

### Reclaiming space of deleted items faster
The queue service is a good use case of `CompactOnDeletionCollector`, which prioritizes ranges with more deletes when scheduling compactions. Set ImmutableCFOptions::table_properties_collector_factories to the factory defined here: https://github.com/facebook/rocksdb/blob/main/include/rocksdb/utilities/table_properties_collectors.h#L23-L27 