Compaction can be triggered manually by calling the <code>DB::CompactRange</code> or <code>DB::CompactFiles</code> method. This is meant to be used by advanced users to implement custom compaction strategies, including but not limited to the following use cases:

1. Optimize for read heavy workloads by compacting to lowest level after ingesting a large amount of data
2. Force the data to go through the compaction filter in order to consolidate it
3. Migrate to a new compaction configuration. For example, if changing number of levels, <code>CompactRange</code> can be called to compact to bottommost level and then move the files to a target level

The example code below shows how to use the APIs.

```cpp
Options dbOptions;

DB* db;
Status s = DB::Open(dbOptions, "/tmp/rocksdb",  &db);

// Write some data
...
Slice begin("key1");
Slice end("key100");
CompactRangeOptions options;

s = db->CompactRange(options, &begin, &end);
```
Or
```
CompactionOptions options;
std::vector<std::string> input_file_names;
int output_level;
...
Status s = db->CompactFiles(options, input_file_names, output_level);
```

## CompactRange

The <code>begin</code> and <code>end</code> arguments define the key range to be compacted. The behavior varies depending on the compaction style being used by the db. In case of universal and FIFO compaction styles, the <code>begin</code> and <code>end</code> arguments are ignored and all files are compacted. Also, files in each level are compacted and left in the same level. For leveled compaction style, all files containing keys in the given range are compacted to the last level containing files. If either <code>begin</code> or <code>end</code> are NULL, it is taken to mean the key before all keys in the db or the key after all keys respectively.

If more than one thread calls manual compaction, only one will actually schedule it while the other threads will simply wait for the scheduled manual compaction to complete. If <code>CompactRangeOptions::exclusive_manual_compaction</code> is set to true, the call will disable scheduling of automatic compaction jobs and wait for existing automatic compaction jobs to finish.

<code>DB::CompactRange</code> waits while compaction is performed on the background threads and thus is a blocking call.
 
The <code>CompactRangeOptions</code> supports the following options -
<ul>

<li><code>CompactRangeOptions::exclusive_manual_compaction</code> When set to true, no other compaction will run when this manual compaction is running. Default value is <code>true</code>

<li><code>CompactRangeOptions::change_level</code>,<code>CompactRangeOptions::target_level</code> Together, these options control the level where the compacted files will be placed. If <code>target_level</code> is -1, the compacted files will be moved to the minimum level whose computed max_bytes is still large enough to hold the files. Intermediate levels must be empty. For example, if the files were initially compacted to L5 and L2 is the minimum level large enough to hold the files, they will be placed in L2 if L3 and L4 are empty or in L4 if L3 is non-empty. If <code>target_level</code> is positive, the compacted files will be placed in that level provided intermediate levels are empty. If any any of the intermediate levels are not empty, the compacted files will be left where they are.

<li><code>CompactRangeOptions::target_path_id</code> Compaction outputs will be placed in options.db_paths[target_path_id] directory.

<li><code>CompactRangeOptions::bottommost_level_compaction</code> When set to <code>BottommostLevelCompaction::kSkip</code>, or when set to <code>BottommostLevelCompaction::kIfHaveCompactionFilter</code> (default) and a compaction filter is **not** defined for the column family, the bottommost level files are not compacted. <code>BottommostLevelCompaction::kForce</code> can force the compaction, which could also avoid trivial move to the bottommost level.

<li><code>CompactRangeOptions::allow_write_stall</code> When set to true, it will execute immediately even if doing so would cause the DB to enter write stall mode. Otherwise, it'll sleep until load is low enough. Default value is <code>false</code>

<li><code>CompactRangeOptions::max_subcompactions</code> If > 0, it will replace the option in the <code>DBOptions</code> for this compaction.

<li><code>CompactRangeOptions::full_history_ts_low</code> Set user-defined timestamp low bound, the data with older timestamp than low bound maybe GCed by compaction.

<li><code>CompactRangeOptions::canceled</code> Allows cancellation of an in-progress manual compaction. Cancellation can be delayed waiting on automatic compactions when used together with <code>exclusive_manual_compaction == true</code>.

<li><code>CompactRangeOptions::blob_garbage_collection_policy</code> If set to <code>kForce</code>, RocksDB will override <code>enable_blob_file_garbage_collection</code> to true; if set to <code>kDisable</code>, RocksDB will override it to false, and <code>kUseDefault</code> leaves the setting in effect. This enables customers to both force-enable and force-disable GC when calling <code>CompactRange</code>. Default value is <code>kUseDefault</code>.

<li><code>CompactRangeOptions::blob_garbage_collection_age_cutoff</code> If set to < 0 or > 1, RocksDB leaves <code>blob_file_garbage_collection_age_cutoff</code> from <code>ColumnFamilyOptions</code> in effect. Otherwise, it will override the user-provided setting. This enables customers to selectively override the age cutoff. Default value is <code>-1</code> (do not override).

</ul>

##  CompactFiles

This API compacts all the input files into a set of output files in the <code>output_level</code>. The number of output files is determined by the size of the data and the setting of <code>CompactionOptions::output_file_size_limit</code>. This API is not supported in ROCKSDB_LITE.


