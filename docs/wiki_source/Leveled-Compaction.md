## Structure of the files
Files on disk are organized in multiple levels. We call them level-1, level-2, etc, or L1, L2, etc, for short. A special level-0 (or L0 for short) contains files just flushed from in-memory write buffer (memtable). Each level (except level 0) is one data sorted run:

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/level_structure.png)

Inside each level (except level 0), data is range partitioned into multiple SST files:

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/level_files.png)

The level is a sorted run because keys in each SST file are sorted (See [Block-based Table Format](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format) as an example). To identify a position for a key, we first binary search the start/end key of all files to identify which file possibly contains the key, and then binary search inside the file to locate the exact position. In all, it is a full binary search across all the keys in the level.

All non-0 levels have target sizes. Compaction's goal will be to restrict data size of those levels to be under the target. The size targets are usually exponentially increasing:
![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/level_targets.png)

## Compactions
Compaction triggers when number of L0 files reaches `level0_file_num_compaction_trigger`, files of L0 will be merged into L1. Normally we have to pick up all the L0 files because they usually are overlapping:

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/pre_l0_compaction.png)

After the compaction, it may push the size of L1 to exceed its target:

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/post_l0_compaction.png)

In this case, we will pick at least one file from L1 and merge it with the overlapping range of L2. The result files will be placed in L2:

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/pre_l1_compaction.png)

If the results push the next level's size exceeds the target, we do the same as previously -- pick up a file and merge it into the next level:

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/post_l1_compaction.png)

and then

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/pre_l2_compaction.png) 

and then

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/post_l2_compaction.png) 

Multiple compactions can be executed in parallel if needed:

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/multi_thread_compaction.png)

Maximum number of compactions allowed is controlled by `max_background_compactions`.

However, L0 to L1 compaction is not parallelized by default. In some cases, it may become a bottleneck that limit the total compaction speed. RocksDB supports subcompaction-based parallelization only for L0 to L1. To enable it, users can set `max_subcompactions` to more than 1. Then, we'll try to partition the range and use multiple threads to execute it:

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/subcompaction.png)

See more about subcompaction in https://github.com/facebook/rocksdb/wiki/Subcompaction.

## Compaction Picking
When multiple levels trigger the compaction condition, RocksDB needs to pick which level to compact first. A score is generated for each level:

* For non-zero levels, the score is total size of the level divided by the target size. If there are already files picked that are being compacted into the next level, the size of those files is not included into the total size, because they will soon go away.

* for level-0, the score is the total number of files, divided by `level0_file_num_compaction_trigger`, or total size over `max_bytes_for_level_base`, which ever is larger. (if the file size is smaller than `level0_file_num_compaction_trigger`, compaction won't trigger from level 0, no matter how big the score is.)

We compare the score of each level, and the level with highest score takes the priority to compact.

Which file(s) to compact from the level are explained in [[Choose Level Compaction Files]].

## Option `level_compaction_dynamic_level_bytes` and Levels' Target Size
There are two flavors of leveled compaction depending on the option `level_compaction_dynamic_level_bytes`:
### `level_compaction_dynamic_level_bytes` is `false`
If `level_compaction_dynamic_level_bytes` is false, then level targets are determined as following: L1's target will be `max_bytes_for_level_base`. And then `Target_Size(Ln+1) = Target_Size(Ln) * max_bytes_for_level_multiplier * max_bytes_for_level_multiplier_additional[n]`. `max_bytes_for_level_multiplier_additional` is by default all 1.

For example, if `max_bytes_for_level_base = 16384`, `max_bytes_for_level_multiplier = 10` and `max_bytes_for_level_multiplier_additional` is not set, then size of L1, L2, L3 and L4 will be 16384, 163840, 1638400, and 16384000, respectively.  

### `level_compaction_dynamic_level_bytes` is `true` (Recommended, default since version 8.4)

Note that `max_bytes_for_level_multiplier_additional` is ignored when this option is true.

#### Guaranteed Space Amp Upper Bound
Target size of the last level (`num_levels`-1) will be actual size of largest level (which is usually the last level). And then `Target_Size(Ln-1) = Target_Size(Ln) / max_bytes_for_level_multiplier`. We won't fill any level whose target will be lower than `max_bytes_for_level_base / max_bytes_for_level_multiplier `. These levels will be kept empty and all L0 compaction will skip those levels and directly go to the first level with valid target size.

For example, if `max_bytes_for_level_base` is 1GB, `num_levels=6` and the actual size of last level is 276GB, then the target size of L1-L6 will be 0, 0, 0.276GB, 2.76GB, 27.6GB and 276GB, respectively.

This is to guarantee a stable LSM-tree structure, where 90% of data is stored in the last level, which can't be guaranteed if `level_compaction_dynamic_level_bytes` is `false`. For example, in the previous example:

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/dynamic_level.png)

We can guarantee 90% of data is stored in the last level, 9% data in the second last level. There will be multiple benefits to it. 

#### More Adaptive Compaction To Write Traffic

To help leveled compaction handle temporary spike of writes more smoothly, we make the following adjustment to target size when determining compaction priority.
With `level_compaction_dynamic_level_bytes = true`, when computing compaction score, we include `total_downcompact_bytes` as part of target size, i.e., compaction score of Ln becomes `Ln size / (Ln target size + total_downcompact_bytes)`. The value of `total_downcompact_bytes` for Ln is the estimated total bytes to be compacted down from L0 ... Ln-1 to Ln. When write traffic is heavy, there is more compaction debt. `total_downcompact_bytes` will be larger for higher levels which makes lower level prioritized for compaction to handle the heavy write traffic.

Note that leveled compaction still cannot efficiently handle write rate that is too much higher than capacity based on the configuration. Works on going to further improve it.

#### Migrating From `level_compaction_dynamic_level_bytes=false` To `level_compaction_dynamic_level_bytes=true`

Before RocksDB version 8.2, users are expected to do a full manual compaction to compact all files into the last level of LSM. Then they can reopen the DB to turn on this option.

RocksDB version 8.2 adds support for automatic migration when this option is turned on. There are two steps for this migration:

1) During DB open, files in an LSM are trivially moved down to fill the LSM starting from the last level. For example, if before DB open, an LSM with 7 levels looks like:

```
L0:
L1: F1
L2:
L3: F2
L4: F3
L5: F4 F5 F6
L6:
```

After the trivial moves are done during DB open, the LSM becomes:

```
L0:
L1:
L2:
L3: F1
L4: F2
L5: F3
L6: F4 F5 F6
```


2) **Automatic Drain Of Unnecessary Levels:** From the above we know that any level with `target size < max_bytes_for_level_base / max_bytes_for_level_multiplier` are not needed and should be kept empty. It is possible that a DB is using more levels than needed (remember that `level_compaction_dynamic_level_bytes=false` does not guarantee space amp compared to `level_compaction_dynamic_level_bytes=true`). This can also happen when a user deletes a lot of DB data. Since RocksDB version 8.2, RocksDB will try to drain these unnecessary levels in the background through compaction. These draining compactions are assigned lower priorities and happen when there is no other background compaction needed. For example, suppose that in the above LSM, only L6, L5 and L4 are needed. Then L3 will be gradually compacted down to make a "stable LSM structure" that is expected when `level_compaction_dynamic_level_bytes=true`.

```
Support max_bytes_for_level_base = 10MB, max_bytes_for_level_multiplier = 2. Then L3 is not needed for the LSM below and will be compacted down during background compactions.

L0:
L1:
L2:
L3: F1 (target = 4MB < max_bytes_for_level_base / max_bytes_for_level_multiplier)
L4: F2 (target = 8MB)
L5: F3 (target = 16MB)
L6: F4 F5 F6 (actual size = 32MB, target size = 32MB)
```

Note that this migration automation also works when a user is switching from Universal to Leveled compaction as long as the number of levels is not smaller than number of non-empty levels in the LSM.

## Intra-L0 Compaction
Too many L0 files hurt read performance in most queries. To address the issue, RocksDB may choose to compact some L0 files together to a larger file. This sacrifices write amplification by one but may significantly improve read amplification in L0 and in turn increase the capability RocksDB can hold data in L0. This would generate other benefits which would be explained below. Additional write amplification of 1 is far smaller than the usual write amplification of leveled compaction, which is often larger than 10. So we believe it is a good trade-off.
Maximum size of Intra-L0 compaction is also bounded by `options.max_compaction_bytes`. If the option takes a reasonable value, total L0 size will still be bounded, even with Intra-L0 files.


## TTL
A file could exist in the LSM tree without going through the compaction process for a really long time if there are no updates to the data in the file's key range. For example, in certain use cases, the keys are "soft deleted" -- set the values to be empty instead of actually issuing a Delete. There might not be any more writes to this "deleted" key range, and if so, such data could remain in the LSM for a really long time resulting in wasted space.

A dynamic `ttl` column-family option has been introduced to solve this problem. Files (and, in turn, data) older than TTL will be scheduled for compaction when there is no other background work. This will make the data go through the regular compaction process, reach to the bottommost level and get rid of old unwanted data.
This also has the (good) side-effect of all the data in the non-bottommost level being newer than ttl, and all data in the bottommost level older than ttl. Note that it could lead to more writes as RocksDB would schedule more compactions.

## Periodic compaction
If compaction filter is present, RocksDB ensures that data go through compaction filter after a certain amount of time. This is achieved via `options.periodic_compaction_seconds`. Setting it to 0 disables this feature. Leaving it the default value, i.e. UINT64_MAX - 1, indicates that RocksDB controls the feature. At the moment, RocksDB will change the value to 30 days. Whenever RocksDB tries to pick a compaction, files older than 30 days will be eligible for compaction and be compacted to the same level when there is no other background work.
