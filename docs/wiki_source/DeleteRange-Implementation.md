This document is aimed at engineers familiar with the RocksDB codebase and conventions.

# Tombstone Fragments

The DeleteRange API does not provide any restrictions on the ranges it can delete (though if `start >= end`, the deleted range will be considered empty). This means that ranges can overlap and cover wildly different numbers of keys. The lack of structure in the range tombstones created by a user make it impossible to binary search range tombstones. For example, suppose a DB contained the range tombstones `[c, d)@4`, `[g, h)@7`, and `[a, z)@10`, and we were looking up the key `e@1`. If we sorted the tombstones by start key, then we would select `[c, d)`, which doesn't cover `e`. If we sorted the tombstones by end key, then we would select `[g, h)`, which doesn't cover `e`. However, we see that `[a, z)` does cover `e`, so in both cases we've looked at the wrong tombstone. If we left the tombstones in this form, we would have to scan through all of them in order to find a potentially covering tombstone. This linear search overhead on the read path becomes very costly as the number of range tombstones grows.

However, it is possible to transform these tombstones using the following insight: one range tombstone is equivalent to several contiguous range tombstones at the same sequence number; for example, `[a, d)@4` is equivalent to `[a, b)@4; [b, d)@4`. Using this fact, we can always "fragment" a list of range tombstones into an equivalent set of tombstones that are non-overlapping. From the example earlier, we can convert `[c, d)@4`, `[g, h)@7`, and `[a, z)@10` into: `[a, c)@10`, `[c, d)@10`, `[c, d)@4`, `[d, g)@10`, `[g, h)@10`, `[g, h)@7`, `[h, z)@10`.

Now that the tombstone fragments do not overlap, we can safely perform a binary search. Going back to the `e@1` example, binary search would find `[d, g)@10`, which covers `e@1`, thereby giving the correct result.

## Fragmentation Algorithm

See [db/range_tombstone_fragmenter.cc](https://github.com/facebook/rocksdb/blob/main/db/range_tombstone_fragmenter.cc).

# Write Path

## Memtable

When `DeleteRange` is called, a kv is written to a dedicated memtable for range tombstones. The format is `start : end` (i.e., `start` is the key, `end` is the value). Range tombstones are not fragmented in the memtable directly, but are instead fragmented each time a read occurs.

See [Compaction](#compaction) for details on how range tombstones are flushed to SSTs.

## SST Files

Just like in memtables, range tombstones in SSTs are not stored inline with point keys. Instead, they are stored in a dedicated meta-block for range tombstones. Unlike in memtables, however, range tombstones are fragmented and cached along with the table reader when the table reader is first created. Each SST file contains all range tombstones at that level that cover user keys overlapping with the file's key range; this greatly simplifies iterator seeking and point lookups.

See [Compaction](#compaction) for details on how range tombstones are compacted down the LSM tree.

# Read Path

Due to the simplicity of the write path, the read path requires more work.

## Point Lookups

When a user calls `Get` and the point lookup progresses down the LSM tree, the range tombstones in the table being searched are first fragmented (if not fragmented already) and binary searched before checking the file's contents. This is done through `FragmentedRangeTombstoneIterator::MaxCoveringTombstoneSeqnum` (see [db/range_tombstone_fragmenter.cc](https://github.com/facebook/rocksdb/blob/main/db/range_tombstone_fragmenter.cc)); if a tombstone is found (i.e., the return value is non-zero), then we know that there is no need to search lower levels since their merge operands / point values are deleted. We check the current level for any keys potentially written after the tombstone fragment, process the merge operands (if any), and return. This is similar to how finding a point tombstone stops the progression of a point lookup.

## Range Scans

The key idea for reading range deletions during point scans is to create a structure resembling the merging iterator used by `DBIter` for point keys, but for the range tombstones in the set of tables being examined.

Here is an example to illustrate how this works for forward scans (reverse scans are similar):

Consider a DB with a memtable containing the tombstone fragment `[a, b)@40, [a, b)@35`, and 2 L0 files `1.sst` and `2.sst`. `1.sst` contains the tombstone fragments `[a, c)@15, [d, f)@20`, and `2.sst` contains the tombstone fragments `[b, e)@5, [e, x)@10`. Aside from the merging iterator of point keys in the memtable and SST files, we also keep track of the following 3 data structures:
1. a min-heap of fragmented tombstone iterators (one iterator per table) ordered by end key (*active heap*)
2. an ordered set of fragmented tombstone iterators ordered by sequence number (*active seqnum set*)
3. a min-heap of tombstones ordered by start key (*inactive heap*)

The active heap contains all iterators pointing at tombstone fragments that cover the most recent (internal) lookup key, the active seqnum set contains the the iterators that are in the active heap (though for brevity we will only write out the seqnums), and the inactive heap contains iterators pointing at tombstone fragments that start after the most recent lookup key. Note that an iterator is not allowed to be in both the active and inactive set.

Suppose the internal merging iterator in `DBIter` points to the internal key `a@4`. The active iterators would be the tombstone iterator for `1.sst` pointing at `[a, c)@15` and the tombstone iterator for the memtable pointing at `[a, b)@40`, and the only inactive iterator would be the tombstone iterator for `2.sst` pointing at `[b, e)@5`. The active seqnum set would contain `{40, 15}`. From these data structures, we know that the largest covering tombstone has a seqnum of 40, which is larger than 4; hence, `a@4` is deleted and we need to check another key.

Next, suppose we then check `b@50`. In response, the active heap now contains the iterators for `1.sst` pointing at `[a, c)@15` and `2.sst` pointing at `[b, e)@5`, the inactive heap contains nothing, and the active seqnum set contains `{15, 5}`. Note that the memtable iterator is now out of scope and is not tracked by these data structures. (Even though the memtable iterator had another tombstone fragment, the fragment was identical to the previous one except for its seqnum (which is smaller), so it was skipped.) Since the largest seqnum is 15, `b@50` is not covered.

For implementation details, see [db/range_del_aggregator.cc](https://github.com/facebook/rocksdb/blob/main/db/range_del_aggregator.cc).

# Compaction

During flushes and compactions, there are two primary operations that range tombstones need to support:
1. Identifying point keys in a "snapshot stripe" (the range of seqnums between two adjacent snapshots) that can be deleted.
2. Writing range tombstones to an output file.

For (1), we use a similar data structure to what was described for range scans, but create one for each snapshot stripe. Furthermore, during iteration we skip tombstones whose seqnums are out of the snapshot stripe's range.

For (2), we create a merging iterator out of all the fragmented tombstone iterators within the output file's range, create a new iterator by passing this to the fragmenter, and writing out each of the tombstone fragments in this iterator to the table builder.

For implementation details, see [db/range_del_aggregator.cc](https://github.com/facebook/rocksdb/blob/main/db/range_del_aggregator.cc).

## File Boundaries

In a database with only point keys, determining SST file boundaries is straightforward: simply find the smallest and largest user key in the file. However, with range tombstones, the story gets more complicated. A simple policy would be to allow the start and end keys of a range tombstone to act as file boundaries (where the start key's sequence number would be the tombstone's sequence number, and the end key's sequence number would be `kMaxSequenceNumber`). This works fine for L0 files, but for lower levels, we need to ensure that files cover disjoint ranges of internal keys. If we encounter particularly large range tombstones that cover many keys, we would be forced to create excessively large SST files. To prevent this, we instead restrict the largest (user) key of a file to be no greater than the smallest user key of the next file; if a range tombstone straddles the two files, this largest key's sequence number and type are set to `kMaxSequenceNumber` and `kTypeRangeDeletion`. In effect, we pretend that the tombstone does not extend past at the beginning of the next file.

A noteworthy edge case is when two adjacent files have a user key that "straddles" the two files; that is, the largest user key of one file is the smallest user key of the next file. (This case is uncommon, but can happen if a snapshot is preventing an old version of a key from being deleted.) In this case, even if there are range tombstones covering this gap in the level, we do not change the file boundary using the range tombstone convention described above, since this would make the largest (internal) key smaller than it should be.

# Range Tombstone Truncation

In the past, if a compaction input file contained a range tombstone, we would also add all other files containing the same range tombstone to the compaction. However, for particularly large range tombstones (commonly created from operations like dropping a large table in SQL), this created unexpectedly large compactions. To make these sorts of compactions smaller, we allowed a compaction "clean cut" to stop at a file whose largest key had a "range tombstone footer" (sequence number and type of `kMaxSequenceNumber` and `kTypeRangeDeletion`).

Unfortunately, this conflicted with another RocksDB optimization that set all key seqnums to 0 in the bottommost level if there was only one version of each key (in that level). Consider the following example:
1. A DB has three levels (Lmax == L2) and no snapshots, with a range tombstone `[a, f)@10` in L1, and two files `1.sst` (point internal key range `[a@5-c@3]`) and `2.sst` (point internal key range `[e@20-g@7]`) in L1.
2. If `2.sst` is compacted down to L2, its internal key `e@20` has its seqnum set to 0.
3. A user creates an iterator and reaches the `e@0` (formerly `e@20`) key. The tombstone `[a, f)@10` in `1.sst` is considered active, and since its seqnum is greater than 0, it is considered to have covered `e`; this is an error as the key should be exposed.

To prevent this, we needed to find a way to prevent the tombstone in `1.sst` from covering keys that were in `2.sst`; more generally, we needed to prevent range tombstones from being able to cover keys outside of their file range. This problem ended up having some very complex edge cases (see the tests in [db/db_range_del_test.cc](https://github.com/facebook/rocksdb/blob/main/db/db_range_del_test.cc) for some examples), so the solution has two parts: _internal key range tombstone boundaries_ and _atomic compaction units_.

## Internal Key Range Tombstone Boundaries

With the goal of preventing range tombstones from covering keys outside of their range, a simple idea would be to truncate range tombstones at the user key boundaries of its file. However, range tombstones are end-key-exclusive, so even if they should cover the largest key in a file, truncating them at that key will prevent them from doing so. Although the file boundary extension logic discussed earlier prevents this problem in many cases, when a user key straddles adjacent SST files, the largest key in the smaller of the two files is a real point key and therefore unsuitable as a user key truncation point.

A less intuitive idea is to use internal keys as tombstone boundaries. Under this extension, a key is covered if it's included in the internal key range and has a seqnum less than the tombstone's seqnum (not the boundary seqnums). To provide expected behaviour, untruncated tombstones will have seqnums of `kMaxSequenceNumber` on the start and end keys. In the aforementioned edge case where the largest key straddles two SST files, we simply use the internal key with seqnum one less than the actual largest key as the tombstone boundary. (If the seqnum is 0 or it's an artificially extended boundary (from range tombstones), then we ignore this step: see the comments on `TruncatedRangeDelIterator` in [db/range_del_aggregator.cc](https://github.com/facebook/rocksdb/blob/main/db/range_del_aggregator.cc) for an explanation.)

## Atomic Compaction Units

Even though we use internal keys in-memory as range tombstone boundaries, we still have to use user keys on disk, since that's what the original format requires. This means that we have to convert an internal key-truncated tombstone to a user key-truncated tombstone during compaction. However, with just user keys, we run the risk of a similar problem to the one described earlier, where a tombstone that actually covers the largest key in the file is truncated to not cover it in a compaction output file.

Broadly speaking, a solution to this problem would involve truncating the tombstone past the largest key of its file during compaction. One simple idea would be to use the compaction boundaries at the input level. However, because we can stop a compaction before the end of a tombstone (the primary motivation for these changes), we can run into a situation like the following:
1. A DB has three levels (Lmax == L2) with no snapshots, and a tombstone `[a, g)@10` is split between two L1 files: `1.sst` (point key range `[a@4-c@6]`) and `2.sst` (point key range `[e@12-k@7]`).
2. A L1-L2 compaction is issued to compact `2.sst` down to L2. The key `e@12` is retained in output file `3.sst` and has its seqnum set to 0.
3. Another L1-L2 compaction is issued later to compact `1.sst` down to L2, with key range `[a-f)`, which also pulls in `3.sst`. The range tombstone is truncated to `[a, f)@10`, so the `e@0` key (formerly `e@12`) is considered covered by the tombstone, which is incorrect.

The root of this problem is that a tombstone duplicated across files can be in different compactions with overlapping boundaries. To solve this, we must pick boundaries that are disjoint for _all_ compactions involving those files, which we call _atomic compaction unit boundaries_.  These boundaries exactly span one or more files which have overlapping boundaries (a file with a range tombstone end key as its largest key is not considered to overlap with the next file), and are computed at compaction time. Using atomic compaction unit boundaries, we allow the end key of a file to be included in a truncated range tombstone's boundaries, while also preventing those boundaries from including keys from previous compactions.

For implementation details, see [db/compaction.cc](https://github.com/facebook/rocksdb/blob/main/db/compaction.cc).