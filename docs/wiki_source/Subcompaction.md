Here we explain the subcompaction, which is used in both Leveled and Universal compactions. 

![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/subcompaction.png)


## Goal
The goal of subcompaction is to speed up a compaction job by partitioning it among multiple threads.

## When
It is employed when one of the following conditions holds (See [`Compaction::ShouldFormSubcompactions()`](https://github.com/facebook/rocksdb/blob/bfdc91017c0667e05cd2f2b49337446e0fb5c1b1/db/compaction/compaction.cc#L773)):
* Leveled Compaction: L0 -> Lo where o > 0 or in a manual compaction
  * Why: L0->Lo cannot be run in parallel with another L0->Lo since L0s are usually overlapping. Hence, partitioning is the only way to speed it up. For manual compaction, it is invoked by the user and is usually not parallelized; therefore, it benefits from partitioning
* Universal Compaction: except L0 -> L0, i.e., any compaction with output level > 0.

## How
A compaction job is executed in three stages: Prepare(), Run(), and Install(). We describe how subcompaction works in each of these stages.

### Prepare
A compaction's input key range is partitioned into `n` ranges to allow for `n` subcompactions later. Ideally, we want to partition the input into subranges such that the workload is evenly distributed among each subcompaction job.

##### Since RocksDB V7.6 
The logic for partitioning (introduced in [#10393](https://github.com/facebook/rocksdb/pull/10393)) is mostly in [`CompactionJob::GenSubcompactionBoundaries()`](https://github.com/facebook/rocksdb/blob/bfdc91017c0667e05cd2f2b49337446e0fb5c1b1/db/compaction/compaction_job.cc#L447). It works roughly as follows:

1. For every compaction input file, we sample up to 128 anchor points that evenly partition the file into subranges. The sample anchor points also include the range sizes and may look something like this:
```
File1: (a1, 1000), (b1, 1200), (c1, 1100)
File2: (a2, 1100), (b2, 1000), (c2, 1000)
```

2. Combine, sort, and deduplicate the anchor points from all compaction input files. For the above example, we get:
```
(a1, 1000), (a2, 1100), (b1, 1200), (b2, 1000), (c1, 1100), (c2, 1000)
```

3. Determine `target_input_size` for each subcompaction. Say max_subcompactions = N, we may not need all N subcompactions if the input size is not big enough. We first determine `target_input_size` of a subcompaction. It is calculated as `max(input size / N, target file size of output level)`.

4. Iterate through the anchor points and use the accumulated sum of range sizes to determine boundaries for each subcompaction. The input size of each subcompaction is roughly `target_input_size` determined in step 3. 
```
Suppose target_level_size = 3200. Then based on the anchor points, we take "b1" as the partition key since the first three ranges would hit 3200.

(a1, 1000), (a2, 1100), (b1, 1200), (b2, 1000), (c1, 1100), (c2, 1000)

Two subcompactions:
subcompaction1: all keys < b1
subcompaction2: all keys >= b1
```

Note that since ranges defined by anchor points from compaction input files can overlap, there may be some inaccuracies in estimating the input size this way. This should be mitigated by having a large number(128) of anchor points from each input file.


##### Before RocksDB v7.6

It is based on a heuristic that worked out well so far. The heuristic could be improved in many ways.
1. Select boundaries based on the natural boundary of input levels/files.
   * first and last key of L0 files 
   * first and last key of non-0, non-last levels
   * first key of each SST file of the last level
1. Use Versions::ApproximateSize to estimate the data size in each boundary.
1. Merge boundaries to eliminate empty and smaller-than-average ranges.
   * find the average size in each range
   * starting from the beginning greedily merge adjacent ranges until their total size exceeds the average

### Run
A compaction job spawns n-1 threads, so there are in total n threads that execute subcompactions in parallel. Each subcompaction executes the function `CompactionJob::ProcessKeyValueCompaction()` and generates compaction output SST files. If `EventListener` is configured, each subcompaction job will invoke the callbacks `OnSubcompactionBegin()` and `OnSubcompactionCompleted()`. After all subcompactions are finished,  compaction-related statistics are aggregated together, recorded in RocksDB statistics and shared to `EventListener` via the  `OnCompactionCompleted()` callback.

### Install
Compaction output files from all subcompactions are sorted by their key ranges and installed in this step.


## Options
The option [`max_subcompactions`](https://github.com/facebook/rocksdb/blob/15053f3ab47839bc14dc31a8a62df1d23f6babbe/include/rocksdb/options.h#L719-L725) limits the max number of subcompactions for each compaction. By default, `max_subcompactions = 1`, which means it is disabled. Note that Round-Robin pri under leveled compaction allows subcompactions by default, and the number of subcompactions can be larger than max_subcompactions. See more in `CompactionJob::GenSubcompactionBoundaries()`. 

**Caveat** (Issue [#9291](https://github.com/facebook/rocksdb/issues/9291)): note that subcompaction ignores the limit `max_background_jobs` (or `max_background_compactions`). Each compaction is allowed to be split into `max_subcompactions` subcompactions. So, in total, there may be `max_background_jobs * max_subcompactions` background threads running compaction. 
