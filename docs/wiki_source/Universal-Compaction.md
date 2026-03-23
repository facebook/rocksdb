Universal Compaction Style is a compaction style, targeting the use cases requiring lower write amplification, trading off read amplification and space amplification.

## Conceptual Basis
As introduced by multiple authors and systems, there are two main types of LSM-tree compaction strategies:
* leveled compaction, as the default compaction style in RocksDB
* an alternative compaction strategy, sometimes called "size tiered" [1] or "tiered" [2].

The key difference between the two strategies is that leveled compaction tends to aggressively merge a smaller sorted run into a larger one, while "tiered" waits for several sorted runs with similar size and merge them together.

It is generally regarded that the second strategy provides far better write amplification with worse read amplification [2][3]. An intuitive way to think about it: in tiered storage, every time an update is compacted, it tends to be moved from a smaller sorted run to a much larger one. Every compaction is likely to make the update exponentially closer to the final sorted run, which is the largest. In leveled compaction, however, an update is compacted more as a part of the larger sorted run where a smaller sorted run is merged into, than as a part of the smaller sorted run. As a result, in most of the times an update is compacted, it is not moved to a larger sorted run, so it doesn't make much progress towards the final largest run.

The benefit of "tiered" compaction is not without downside. The worse case number of sorted runs is far higher than leveled compaction. It may cause higher I/O costs and/or higher CPU costs during reads. The lazy nature of the compaction scheduling also makes the compaction traffic much more spiky, the number of sorted runs greatly varies over time, hence large variation of performance.

Nevertheless, RocksDB provides a Universal compaction in the "tiered" family. Users may try this compaction style if leveled compaction is not able to handle the required write rate.

[1] The term is used by Cassandra. See their [doc](https://docs.datastax.com/en/cassandra/3.0/cassandra/operations/opsConfigureCompaction.html).

[2] N. Dayan, M. Athanassoulis, and S. Idreos, “[Monkey: Optimal Navigable Key-Value Store](https://stratos.seas.harvard.edu/publications/monkey-optimal-navigable-key-value-store),” in ACM SIGMOD International Conference on Management of Data, 2017.

[3] [https://smalldatum.blogspot.com/2018/08/name-that-compaction-algorithm.html](https://smalldatum.blogspot.com/2018/08/name-that-compaction-algorithm.html)


## Overview And the Basic Idea

When using this compaction style, all the SST files are organized as sorted runs covering the whole key ranges. One sorted run covers data generated during a time range. Different sorted runs never overlap on their time ranges. Compaction can only happen among two or more sorted runs of adjacent time ranges. The output is a single sorted run whose time range is the combination of input sorted runs. After any compaction, the condition that sorted runs never overlap on their time ranges still holds. A sorted run can be implemented as an L0 file, or a "level" in which data is stored as key range partitioned files.

The basic idea of the compaction style: with a threshold of number of sorted runs N, we only start compaction when number of sorted runs reaches N. When it happens, we try to pick files to compact so that number of sorted runs is reduced in the most economic way: (1) it starts from the smallest file; (2) one more sorted run is included if its size is no larger than the existing compaction size. The strategy assumes and itself tries to maintain that the sorted run containing more recent data is smaller than ones containing older data.

## Limitations

### Double Size Issue
In universal style compaction, sometimes full compaction is needed. In this case, output data size is similar to input size. During compaction, both of input files and the output file need to be kept, so the DB will be temporarily double the disk space usage. Be sure to keep enough free space for full compaction.

### DB (Column Family) Size if num_levels=1
When using Universal Compaction, if num_levels = 1, all data of the DB (or Column Family to be precise) is sometimes compacted to one single SST file. There is a limitation of size of one single SST file. In RocksDB, a block cannot exceed 4GB (to allow size to be uint32). The index block can exceed the limit if the single SST file is too big. The size of index block depends on your data. In one of our use cases, we would observe the DB to reach the limitation when the DB grows to about 250GB, using 4K data block size. To mitigate this limitation you can use [partitioned indexes](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters).

This problem is mitigated if users set num_levels to be much larger than 1. In that case, larger "files" will be put in larger "levels" with files divided into smaller files (more explanation below). L0->L0 compaction can still happen for parallel compactions, but most likely files in L0 are much smaller.

## Data Layout and Placement
### Sorted Runs
As mentioned above, data is organized as sorted runs. Sorted runs are laid out by updated time of the data in it and stored as either files in L0 or a whole "level".

Here is an example of a typical file layout:
```
Level 0: File0_0, File0_1, File0_2
Level 1: (empty)
Level 2: (empty)
Level 3: (empty)
Level 4: File4_0, File4_1, File4_2, File4_3
Level 5: File5_0, File5_1, File5_2, File5_3, File5_4, File5_5, File5_6, File5_7
```
Levels with a larger number contain older sorted run than levels of smaller numbers. In this example, there are five sorted runs: three files in level 0, level 4 and 5. Level 5 is the oldest sorted run, level 4 is newer, and the level 0 files are the newest.

### Placement of Compaction Outputs
Compaction is always scheduled for sorted runs with consecutive time ranges and the outputs are always another sorted run. We always place compaction outputs to the highest possible level, following the rule of older data on levels with larger numbers.

Use the same example shown above. We have following sorted runs:
```
File0_0
File0_1
File0_2
Level 4: File4_0, File4_1, File4_2, File4_3
Level 5: File5_0, File5_1, File5_2, File5_3, File5_4, File5_5, File5_6, File5_7
```
If we compact all the data, the output sorted run will be placed in level 5. so it becomes:
```
Level 5: File5_0', File5_1', File5_2', File5_3', File5_4', File5_5', File5_6', File5_7'
```
Starting from this state, let's see how to place output sorted runs if we schedule different compactions:

If we compact File0_1, File0_2 and Level 4, the output sorted run will be placed in level 4.
```
Level 0: File0_0
Level 1: (empty)
Level 2: (empty)
Level 3: (empty)
Level 4: File4_0', File4_1', File4_2', File4_3'
Level 5: File5_0, File5_1, File5_2, File5_3, File5_4, File5_5, File5_6, File5_7
```

If we compact File0_0, File0_1 and File0_2, the output sorted run will be placed in level 3. 
```
Level 0: (empty)
Level 1: (empty)
Level 2: (empty)
Level 3: File3_0, File3_1, File3_2
Level 4: File4_0, File4_1, File4_2, File4_3
Level 5: File5_0, File5_1, File5_2, File5_3, File5_4, File5_5, File5_6, File5_7
```

If we compact File0_0 and File0_1, the output sorted run will still be placed in level 0.
```
Level 0: File0_0', File0_2
Level 1: (empty)
Level 2: (empty)
Level 3: (empty)
Level 4: File4_0, File4_1, File4_2, File4_3
Level 5: File5_0, File5_1, File5_2, File5_3, File5_4, File5_5, File5_6, File5_7
```

### Special case options.num_levels=1
If options.num_levels=1, we still follow the same placement rule. It means all the files will be placed under level 0 and each file is a sorted run. The behavior will be the same as initial universal compaction, so it can be used as a backward compatible mode.

## Compaction Picking Algorithm
Assuming we have sorted runs
```
    R1, R2, R3, ..., Rn
```
where R1 contains the most recent writes and Rn contains the oldest writes. Note that the sorted runs are ordered by the age of the writes in the sorted run. According to this sorting order, after compaction the output sorted run always takes the place of the input sorted runs to preserve the ordering by age of writes.

This explains how sorted runs are selected for compaction:

#### Precondition: n >= options.level0_file_num_compaction_trigger
Unless number of sorted runs reaches this threshold, no compaction will be triggered at all.

Note although the option name uses word _file_, the trigger is based on the number of sorted runs because the largest sorted runs can be range partitioned into many files. For the names of all options mentioned below, _file_ also means _sorted run_ for the same reason.

A _major compaction_ reads all sorted runs as input. A _minor compaction_ reads some, but not all, sorted runs as input. In both cases the output is one sorted run. A major compaction can take a long time and temporarily double the amount of disk space used for the column family.

If the pre-condition is satisfied, there are four conditions. Each of them can trigger a compaction. They are evaluated in order and the selection stops once a compaction has been scheduled. Before going into detail, the overview for picking a compaction is:
1. Try to schedule a compaction by age of data
2. Else if the space amplification constraint has been violated, try to schedule a major compaction
3. Else try to schedule a compaction (minor or major) while respecting [size_ratio](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/universal_compaction.h)
4. Else try to schedule a minor compaction without respecting [size_ratio](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/universal_compaction.h)

#### 1. Compaction triggered by age of data

For universal style compaction, the aging-based triggering criterion has the highest priority since it is a hard requirement. When trying to pick a compaction, this condition is checked first.

If the condition meets (there are files older than options.periodic_compaction_seconds), then RocksDB proceeds to pick sorted runs for compaction. RocksDB picks sorted runs from oldest to youngest until encountering a sorted run that is being compacted by another compaction. These files will be compacted to bottommost level unless bottommost level is reserved for ingestion behind. In this case, files will be compacted to second bottommost level.

#### 2. Compaction Triggered by Space Amplification
If the estimated _size amplification ratio_ is larger than options.compaction_options_universal.max_size_amplification_percent / 100, all files will be compacted to one sorted run.

Here is how _size amplification ratio_ is estimated:

```
    size amplification ratio = (size(R1) + size(R2) + ... size(Rn-1)) / size(Rn)
```
Please note, size of Rn is not included, which means 0 is the perfect size amplification and 100 means DB size is double the space of live data, and 200 means triple.

The reason we estimate size amplification in this way: in a stable sized DB, incoming rate of deletion should be similar to rate of insertion, which means for any of the sorted runs except Rn should include similar number of insertion and deletion. After applying R1, R2 ... Rn-1, to Rn, the size effects of them will cancel each other, so the output should also be size of Rn, which is the size of live data, which is used as the base of size amplification. This estimate is more likely to be accurate for workloads that do more overwrites than inserts. This estimate is likely to be incorrect for an insert-only workload.

If options.compaction_options_universal.max_size_amplification_percent = 25, which means we will keep total space of DB less than 125% of total size of live data. Let's use this value in an example. Assuming compaction is only triggered by space amplification, options.level0_file_num_compaction_trigger = 1, file size after each mem table flush is always 1, and compacted size always equals to total input sizes. After two flushes, we have two files size of 1, while 1/1 > 25% so we'll need to do a full compaction:

```
1 1  =>  2
```

After another memtable flush we have

```
1 2   =>  3
```

Which again triggers a full compaction because 1/2 > 25%. And in the same way:

```
1 3  =>  4
```

After another flush because 1/3 > 25%:

```
1 4
```

The compaction is not triggered, because 1/4 <= 25%. Another memtable flush will trigger another compaction:

```
1 1 4  =>  6
```

Because (1+1) / 4 > 25%.

And it keeps going like this:

```
1
1 1  =>  2
1 2  =>  3
1 3  =>  4
1 4
1 1 4  =>  6
1 6
1 1 6  =>  8
1 8
1 1 8
1 1 1 8  =>  11
1 11
1 1 11
1 1 1 11  =>  14
1 14
1 1 14
1 1 1 14
1 1 1 1 14  =>  18
```

#### 3. Compaction Triggered by number of sorted runs while respecting size_ratio

If a compaction is not triggered by the space amplification constraint as described above then a compaction can be triggered by the number of sorted runs. This step respects _size ratio trigger_ when selecting the sorted runs to compact.

We calculated a value of _size ratio trigger_ as

```
     size_ratio_trigger = (100 + options.compaction_options_universal.size_ratio) / 100
```
Usually options.compaction_options_universal.size_ratio is close to 0 so _size ratio trigger_ is close to 1.

The compaction picker considers the sorted runs in age order (R1 is the first candidate) to determine whether an adjacent sequence of sorted runs exists that satisfies the _size ratio trigger_, all sorted runs in the sequence are not being compacted and the sequence has at least [min_merge_width](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/universal_compaction.h) sorted runs. The sequence is limited to at most [max_merge_width](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/universal_compaction.h) sorted runs.

We start from R1, if size(R2) / size(R1) <= _size ratio trigger_, then (R1, R2) are qualified to be compacted together. We continue from here to determine whether R3 can be added too. If size(R3) / size(R1 + R2) <= _size ratio trigger_, we would include (R1, R2, R3). Then we do the same for R4. We keep comparing total existing size to the next sorted run until the _size ratio trigger_ condition doesn't hold any more.

Here is an example. Assuming options.compaction_options_universal.size_ratio = 0, total mem table flush size is always 1, compacted size always equals to total input sizes, compaction is only triggered by space amplification and options.level0_file_num_compaction_trigger = 5. Starting from an empty DB, after 5 mem table flushes, we have 5 files size of 1, which triggers a compaction of all files because 1/1 <= 1, 1/(1+1) <= 1, 1/(1+1+1) <=1 and 1/(1+1+1+1) <= 1:

```
1 1 1 1 1  =>  5
```

After 4 mem table flushes there are 5 files again and the first 4 files qualify: 1/1 <= 1, 1/(1+1) <= 1, 1/(1+1+1) <=1. The 5th one doesn't because: 5/(1+1+1+1) > 1:

```
1 1 1 1 5  => 4 5
```
They go on like that for several rounds:
```
1 1 1 1 1  =>  5
1 5  (no compaction triggered)
1 1 5  (no compaction triggered)
1 1 1 5  (no compaction triggered)
1 1 1 1 5  => 4 5
1 4 5  (no compaction triggered)
1 1 4 5  (no compaction triggered)
1 1 1 4 5 => 3 4 5
1 3 4 5  (no compaction triggered)
1 1 3 4 5 => 2 3 4 5
```

Another flush brings it to:

```
1 2 3 4 5
```
And no compaction is triggered, so we hold the compaction. Only when another flush comes, all files are qualified to compact together:

```
1 1 2 3 4 5 => 16
```

Because 1/1 <=1, 2/(1+1) <= 1, 3/(1+1+2) <= 1, 4/(1+1+2+3) <= 1 and 5/(1+1+2+3+4) <= 1. And we continue from there:
```
1 16  (no compaction triggered)
1 1 16  (no compaction triggered)
1 1 1 16  (no compaction triggered)
1 1 1 1 16  => 4 16
1 4 16  (no compaction triggered)
1 1 4 16  (no compaction triggered)
1 1 1 4 16 => 3 4 16
1 3 4 16  (no compaction triggered)
1 1 3 4 16 => 2 3 4 16
1 2 3 4 16  (no compaction triggered)
1 1 2 3 4 16 => 11 16
```

#### 4. Compaction Triggered by number of sorted runs without respecting size_ratio

If none of the previous conditions cause a compaction to be scheduled, then this condition might schedule one. This step does not respect _size ratio trigger_ but the logic is otherwise similar to what was described above for condition 3. If we need to compact more than options.compaction_options_universal.max_merge_width number of sorted runs, we cap it to options.compaction_options_universal.max_merge_width.

"Try to schedule" I mentioned below will happen when after flushing a memtable, finished a compaction. Sometimes duplicated attempts are scheduled.

See [Universal Style Compaction Example](https://github.com/facebook/rocksdb/wiki/Universal-Style-Compaction-Example) as an example of how output sorted run sizes look like for a common setting.

Parallel compactions are possible if options.max_background_compactions > 1. Same as all other compaction styles, parallel compactions will not work on the same sorted run.
 
## Subcompaction

[Subcompaction](https://github.com/facebook/rocksdb/wiki/Subcompaction) is supported in universal compaction. If the output level of a compaction is not level 0, we will try to range partition the inputs using `options.max_subcompaction` threads to compact them in parallel. It will help with the problem that full compaction of universal compaction takes too long. Note that as of RocksDB version 6, each thread that needs `options.max_subcompaction` threads will create them on demand which can ignore the limit set by `options.max_background_jobs` - see [issue 9291](https://github.com/facebook/rocksdb/issues/9291).

## Options to Tune
Following are options affecting universal compactions:
* options.compaction_options_universal: various options mentioned above
* options.level0_file_num_compaction_trigger the triggering condition of any compaction. It also means after all compactions' finishing, number of sorted runs will be under options.level0_file_num_compaction_trigger+1
* options.level0_slowdown_writes_trigger: if number of sorted runs exceeds this value, writes will be artificially slowed down.
* options.level0_stop_writes_trigger: if number of sorted runs exceeds this value, writes will stop until compaction finishes and number of sorted runs turns under this threshold.
* options.num_levels: if this value is 1, all sorted runs will be stored as level 0 files. Otherwise, we will try to fill non-zero levels as much as possible. The larger num_levels is, the less likely we will have large files on level 0.
* options.target_file_size_base: effective if options.num_levels > 1. Files of levels other than level 0 will be cut to file size not larger than this threshold.
* options.target_file_size_multiplier: it is effective, but we don't know a way to use this option in universal compaction that makes sense. So we don't recommend you to tune it.
* options.compaction_options_universal.compression_size_percent - set to -1 to compress all sorted runs including those produced by memtable flushes. Set to a value greater than 0 to compress the larger sorted runs.
* options.compaction_options_universal.allow_trivial_move - set to True to enable trivial moves during an initial bulk load that is done in key order. Trivial move will be triggered if input files are not overlapping and output level is not L0. If num_levels isn't large enough and a L0->L0 compaction is triggered, trivial move won't happen. Consider setting compression_size_percent to -1 if compression is desired.

Following options **DO NOT** affect universal compactions:
* options.max_bytes_for_level_base: only for level-based compaction
* options.level_compaction_dynamic_level_bytes: only for level-based compaction
* options.max_bytes_for_level_multiplier and options.max_bytes_for_level_multiplier_additional: only for level-based compaction
* options.expanded_compaction_factor: only for level-based compactions
* options.source_compaction_factor: only for level-based compactions
* options.max_grandparent_overlap_factor: only for level-based compactions
* options.soft_rate_limit and options.hard_rate_limit: deprecated
* options.hard_pending_compaction_bytes_limit: only for level-based compaction
* options.compaction_pri: only for level-based compaction

## Estimate Write Amplification
Estimating write amplification will be very helpful to users to tune universal compaction. This, however, is hard. Since universal compaction always makes locally optimized decision, the shape of the LSM-tree is hard to predict. You can see it from the [example](https://github.com/facebook/rocksdb/wiki/Universal-Style-Compaction-Example) mentioned above. We still don't have a good Math model to predict the write amplification.

Here is a not-so-good estimation.

The estimation based on the simplicity that every time an update is compacted, the size of output sorted run is doubled of the original one (this is a wild unproven estimation), with the exception of the first or last compaction it experienced, where sorted runs of similar sizes are compacted together.

Take an example, if _options.compaction_options_universal.max_size_amplification_percent = 25_, last sorted run's size is 256GB, and SST file size is 256MB after flushed from memtable, and _options.level0_file_num_compaction_trigger = 11_. Then in a stable stage, the file sizes are like this this:
```
256MB
256MB
256MB
256MB
2GB
4GB
8GB
16GB
16GB
16GB
256GB
```
Compaction stages are like following with its write amp:
```
256MB
256MB
256MB  (write amp 1)
256MB
--------
2GB    (write amp 1)
--------
4GB    (write amp 1)
--------
8GB    (write amp 1)
--------
16GB
16GB    (write amp 1)
16GB
--------
256GB   (write amp 4)
```
So the total write amp is estimated to be 9.

Here is how the write amplification is estimated:

_options.compaction_options_universal.max_size_amplification_percent_ always introduces write amplification by itself it is much lower than 100. This write amplification is estimated to be

_WA1 = 100 / options.compaction_options_universal.max_size_amplification_percent_.

If this is not lower than 100, estimate

_WA1 = 0_

Estimate total data size other than the last sorted run, S. If _options.compaction_options_universal.max_size_amplification_percent < 100_, estimate it using

_S = total_size * (options.compaction_options_universal.max_size_amplification_percent/100)_

Otherwise

_S = total_size_

Estimate SST file size flushed from memtable to be _M_. And we estimate maximum number of compactions for an update to reach maximum as:

_p = log(2, S/M)_

It is recommended that _options.level0_file_num_compaction_trigger > p_. And then we estimate the write amplification because of individual size ratio using:

_WA2 = p - log(2, options.level0_file_num_compaction_trigger - p)_

And then the total estimated write amplification would be _WA1 + WA2_.