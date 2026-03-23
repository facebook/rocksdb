Trivial move feature can help reduce write amplification. Trivial move is a type of compaction that moves the SST file directly to the next level without compacting the file. It happens when there's no overlap with the key ranges in the next level. For example, in the following graph, SST file 2 on Level 1 doesn't have any overlap files on Level 2, so when compacting file 2 to Level 2, there's no need to re-write file 2:
![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/trivial_move.png)
SST file 1 on Level 1 cannot do trivial move, it has to be compacted with file 3 and 4 as there's overlap. 
 
This feature is available in both [Leveled](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction) (always enabled) and [Universal compaction](https://github.com/facebook/rocksdb/wiki/Universal-Compaction) (disabled on default; can be enable through `options.compaction_options_universal.allow_trivial_move`).
 
Trivial move is skipped in the following scenarios:

* When compression settings are different between the input and the output level.
    * This can happen when Hybrid compression is used (`options.bottommost_compression`) or per level compression is configured (`options.compression_per_level`)
* When Manual compaction is used, and compaction filter is configured
* (For Leveled Compaction alone) When the SST moved to level N+1 would overlap too many SSTs in level N+2 (would exceed [`max_compaction_bytes`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/advanced_options.h#L618)). The default setting for `max_compaction_bytes` is 25 * [`target_file_size_base`](https://github.com/facebook/rocksdb/blob/2a67d475f1a3fe24b4baae6e590cecabca099464/db/column_family.cc#L364) (So, any overlap spanning 25 SSTs in Level N+2). 

 
 
Two potential side effects of Trivial move are: 

* Many small/uncompacted SST files 
* Since, these files are not compacted, [Compaction filters](https://github.com/facebook/rocksdb/wiki/Compaction-Filter) are not run on these files. 

 
Workarounds possible are:

* Use [Manual Compaction](https://github.com/facebook/rocksdb/wiki/Manual-Compaction) to force the compaction by:
    * setting `BottommostLevelCompaction` to `kForce` or `kForceOptimized`. Or
    * Have compaction filter configured
* Setting a different bottommost compression - this will also force the compaction on the bottommost level.

 
