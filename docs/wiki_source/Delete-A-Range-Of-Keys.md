**Note: the techniques described in this page are obsolete. For users of RocksDB 5.18+, the native [[DeleteRange|DeleteRange]] function is a better alternative for all known use cases.**

In many cases, people want to drop a range of keys. For example, in MyRocks, we encoded rows from one table by prefixing with table ID, so that when we need to drop a table, all keys with the prefix needs to be dropped. For another example, if we store different attributes of a user with key with the format `[user_id][attribute_id]`, then if a user deletes the account, we need to delete all the keys prefixing `[user_id]`.

The standard way of deleting those keys is to iterate through all of them and issue `Delete()` to them one by one. This approach works when the number of keys to delete is not large. However, there are two potential drawbacks of this solution:

1. the space occupied by the data will not be reclaimed immediately. We'll wait for compaction to clean up the data. This is usually a concern when the range to delete takes a significant amount of space of the database.
2. the chunk of tombstones may slow down iterators.

There are two other ways you can do to delete keys from a range:

The first way is to issue a `DeleteFilesInRange()` to the range. The command will remove all SST files only containing keys in the range to delete. For a large chunk, it will immediately reclaim most space, so it is a good solution to problem 1. One thing needs to be taken care of is that after the operations, some keys in the range may still exist in the database. If you want to remove all of them, you should follow up with other operations, but it can be done in a slower pace. Also, be aware that, `DeleteFilesInRange()` will be removed despite of existing snapshots, so you shouldn't expect to be able to read data from the range using existing snapshots any more.

Another way is to apply compaction filter together with `CompactRange()`. You can write a compaction filter that can filter out keys from deleted range. When you want to delete keys from a range, call `CompactRange()` for the range to delete. While the compaction finishes, the keys will be dropped. We recommend you to turn `CompactionFilter::IgnoreSnapshots()` to true to make sure keys are dropped even if you have outstanding snapshots. Otherwise, you may not be able to fully remove all the keys in the range from the system. This approach can also solve the problem of reclaiming data, but it introduces more I/Os than the DeleteFilesInRange() approach. However, `DeleteFilesInRange()` cannot remove all the data in the range. So a better way is to first apply `DeleteFilesInRange()`, and then issue `CompactRange()` with compaction filter.

Problem 2 is a harder problem to solve. One way is to apply `DeleteFilesInRange()` + `CompactRange()` so that all keys and tombstones for the range are dropped. It works for large ranges, but it is too expensive if we frequently drop small ranges. The reason is that `DeleteFilesInRange()` is unlikely to delete any file and `CompactRange()` will delete far more data than needed because it needs to execute compactions against all SST files containing any key in the deletion rang. For the use cases where `CompactRange()` is too expensive, there are still two ways to reduce the harm:

1. if you never overwrite existing keys, you can try to use `DB::SingleDelete()` instead of `Delete()` to kill tombstones sooner. Tombstones will be dropped after it meets the original keys, rather than compacted to the last level.
2. use `NewCompactOnDeletionCollectorFactory()` to speed up compaction when there are chunks of tombstones.
