Compaction algorithms constrain the LSM tree shape. They determine which sorted runs can be merged by it and which sorted runs need to be accessed for a read operation. You can read more on RocksDB Compactions here:
<a href="https://github.com/facebook/rocksdb/wiki/RocksDB-Overview#multi-threaded-compactions">Multi-threaded compactions</a>

## LSM terminology and metaphors
Let us first establish the different, sometimes mixed, metaphors and terminology used in describing LSM levels and structure.

- A level is **above** another level if its number is lower. For example, L1 is **above** L2
- The lowest-numbered level, L0, can be called the **top** level or **first** level.
  - A version of a key in L0 must be newer than versions of that same key in all levels below L0.
  - Thus, L0 is sometimes loosely referred to as the level containing the **newest** data.
- A level is **below** another level if its number is higher. For example, L2 is **below** L1.
- The highest-numbered level, Lmax, can be called the **bottom-most** or **last** level.
  - A version of a key in Lmax must be older than versions of that same key in all levels above Lmax.
  - Thus, Lmax is sometimes loosely referred to as the level containing the **oldest** data.
- When talking about a particular key or key-range, a level is considered **bottom-most** when that level contains data for that key or key-range and no below level contains data for it.

## Overview of Compaction algorithms
Source: https://smalldatum.blogspot.com/2018/08/name-that-compaction-algorithm.html

Here we present a taxonomy of compaction algorithms: Classic Leveled, Tiered, Tiered+Leveled, Leveled-N, FIFO. Out of them, Rocksdb implements Tiered+Leveled (termed Level Compaction in the code), Tiered (termed Universal in the code), and FIFO.

### Classic Leveled

Classic Leveled compaction, introduced by LSM-tree paper by O'Neil et al, minimizes space amplification at the cost of read and write amplification.

The LSM tree is a sequence of levels. Each level is one sorted run that can be range partitioned into many files. Each level is many times larger than the previous level. The size ratio of adjacent levels is sometimes called the fanout and write amplification is minimized when the same fanout is used between all levels. Compaction into level N (Ln) merges data from Ln-1 into Ln. Compaction into Ln rewrites data that was previously merged into Ln. The per-level write amplification is equal to the fanout in the worst case, but it tends to be less than the fanout in practice as explained in [this paper](https://hyeontaek.com/papers/msls-fast2016.pdf) by Hyeontaek Lim et al. Compaction in the original LSM paper was all-to-all -- all data from Ln-1 is merged with all data from Ln. It is some-to-some for LevelDB and RocksDB -- some data from Ln-1 is merged with some (the overlapping) data in Ln.

While write amplification is usually worse with leveled than with tiered, there are a few cases where leveled is competitive. The first is key-order inserts and a RocksDB optimization greatly reduces write-amp in that case. The second one is skewed writes where only a small fraction of the keys are likely to be updated. With the right value for compaction priority in RocksDB compaction should stop at the smallest level that is large enough to capture the write working set -- it won't go all the way to the max level. When leveled compaction is some-to-some then compaction is only done for the slices of the LSM tree that overlap the written keys, which can generate less write amplification than all-to-all compaction.

### Leveled-N

Leveled-N compaction is like leveled compaction but with less write and more read amplification. It allows more than one sorted run per level. Compaction merges all sorted runs from Ln-1 into one sorted run from Ln, which is leveled. And then "-N" is added to the name to indicate there can be n sorted runs per level. The [Dostoevsky paper](https://stratos.seas.harvard.edu/publications/dostoevsky-better-space-time-trade-offs-lsm-tree-based-key-value-stores) defined a compaction algorithm named Fluid LSM in which the max level has 1 sorted run but the non-max levels can have more than 1 sorted run. Leveled compaction is done into the max level.

### Tiered

Tiered compaction minimizes write amplification at the cost of read and space amplification.

The LSM tree can still be viewed as a sequence of levels as explained in the Dostoevsky paper by Niv Dayan and Stratos Idreos. Each level has N sorted runs. Each sorted run in Ln is ~N times larger than a sorted run in Ln-1. Compaction merges all sorted runs in one level to create a new sorted run in the next level. N in this case is similar to fanout for leveled compaction. Compaction does not read/rewrite sorted runs in Ln when merging into Ln. The per-level write amplification is 1 which is much less than for leveled where it was fanout.

A common approach for tiered is to merge sorted runs of similar size, without having the notion of levels (which imply a target for the number of sorted runs of specific sizes). Most include some notion of major compaction that includes the largest sorted run and conditions that trigger major and non-major compaction. Too many files and too many bytes are typical conditions.

There are a few challenges with tiered compaction:
- Transient space amplification is large when compaction includes a sorted run from the max level.
- The block index and bloom filter for large sorted runs will be large. Splitting them into smaller parts is a good idea.
- Compaction for large sorted runs takes a long time. Multi-threading would help.
- Compaction is all-to-all. When there is skew and most of the keys don't get updates, large sorted runs might get rewritten because compaction is all-to-all. In a traditional tiered algorithm there is no way to rewrite a subset of a large sorted run.

For tiered compaction the notion of levels are usually a concept to reason about the shape of the LSM tree and estimate write amplification. With RocksDB they are also an implementation detail. The levels of the LSM tree beyond L0 can be used to store the larger sorted runs. The benefit from this is to partition large sorted runs into smaller SSTs. This reduces the size of the largest bloom filter and block index chunks -- which is friendlier to the block cache -- and was a big deal before partitioned index/filter was supported. With subcompactions this enables multi-threaded compaction of the largest sorted runs. Note that RocksDB used the name universal rather than tiered.

Tiered compaction in RocksDB code base is termed Universal Compaction.

### Tiered+Leveled

Tiered+Leveled has less write amplification than leveled and less space amplification than tiered.

The tiered+leveled approach is a hybrid that uses tiered for the smaller levels and leveled for the larger levels. It is flexible about the level at which the LSM tree switches from tiered to leveled. For now I assume that if Ln is leveled then all levels that follow (Ln+1, Ln+2, ...) must be leveled.

SlimDB from VLDB 2018 is an example of tiered+leveled although it might allow Lk to be tiered when Ln is leveled for k > n. Fluid LSM is described as tiered+leveled but I think it is leveled-N.

Leveled compaction in RocksDB is also tiered+leveled. There can be N sorted runs at the memtable level courtesy of the max_write_buffer_number option -- only one is active for writes, the rest are read-only waiting to be flushed. A memtable flush is similar to tiered compaction -- the memtable output creates a new sorted run in L0 and doesn't read/rewrite existing sorted runs in L0. There can be N sorted runs in level 0 (L0) courtesy of level0_file_num_compaction_trigger. So the L0 is tiered. Compaction isn't done into the memtable level so it doesn't have to be labeled as tiered or leveled. Subcompactions in the RocksDB L0 makes this even more interesting, but that is a topic for another post.

### FIFO

The FIFOStyle Compaction drops oldest file when obsolete and can be used for cache-like data.

## Options
Here we give overview of the options that impact behavior of Compactions:
<ul>

<li><code>AdvancedColumnFamilyOptions::compaction_style</code> - RocksDB currently supports four compaction algorithms - <code>kCompactionStyleLevel</code>(default), <code>kCompactionStyleUniversal</code>, <code>kCompactionStyleFIFO</code> and <code>kCompactionStyleNone</code>. If <code>kCompactionStyleNone</code> is selected, compaction has to be triggered manually by calling <code>CompactRange() or CompactFiles()</code>). Level compaction options are available under <code>AdvancedColumnFamilyOptions</code>. Universal Compaction options are available in <code>AdvancedColumnFamilyOptions::compaction_options_universal</code> and FIFO compaction options available in <code>AdvancedColumnFamilyOptions::compaction_options_fifo</code>

<li><code>ColumnFamilyOptions::disable_auto_compactions</code> - This dynamically changeable setting can be used by the application to disable automatic compactions. Manual compactions can still be issued on this database.

<li><code>ColumnFamilyOptions::compaction_filter</code> - Allows an application to modify/delete a key-value during background compaction (single instance). The client must provide compaction_filter_factory if it requires a new compaction filter to be used for different compaction processes. Client should specify only one of filter or factory.

<li><code>ColumnFamilyOptions::compaction_filter_factory</code> - a factory that provides compaction filter objects which allow an application to modify/delete a key-value during background compaction. A new filter will be created for each compaction run.

<li> <code>DBOptions::max_subcompactions</code> (Default: 1) - Specify the max number of <a href="https://github.com/facebook/rocksdb/wiki/Subcompaction">subcompactions</a> each compaction is allowed to be split into. 
</ul>

Other options impacting performance of compactions and when they get triggered are:
<ul>

<li> <code>DBOptions::access_hint_on_compaction_start</code> (Default: NORMAL) - Specify the file access pattern once a compaction is started. It will be applied to all input files of a compaction. Other AccessHint settings - <code>NONE, SEQUENTIAL, WILLNEED </code>

<li> <code>ColumnFamilyOptions::level0_file_num_compaction_trigger</code> (Default: 4) - Number of files to trigger level-0 compaction. A negative value means that level-0 compaction will not be triggered by number of files at all. 

<li> <code>AdvancedColumnFamilyOptions::target_file_size_base</code> and <code>AdvancedColumnFamilyOptions::target_file_size_multiplier</code> - Target file size for compaction. target_file_size_base is per-file size for level-1. Target file size for level L can be calculated by target_file_size_base * (target_file_size_multiplier ^ (L-1)) For example, if target_file_size_base is 2MB and target_file_size_multiplier is 10, then each file on level-1 will be 2MB, and each file on level 2 will be 20MB, and each file on level-3 will be 200MB. Default <code>target_file_size_base</code> is 64MB and default <code>target_file_size_multiplier</code> is 1.

<li> <code>AdvancedColumnFamilyOptions::max_compaction_bytes (Default: target_file_size_base * 25)</code>
 - Maximum number of bytes in all compacted files. We avoid expanding the lower level file set of a compaction if it would make the total compaction cover more than this amount.

<li> <code>DBOptions::max_background_jobs </code> (Default: 2) - Maximum number of concurrent background jobs (compactions and flushes)

<li> <code>DBOptions::compaction_readahead_size</code> - If non-zero, we perform bigger reads when doing compaction. If you're running RocksDB on spinning disks, you should set this to at least 2MB. We enforce it to be 2MB if you don't set it with direct I/O. 
</ul>

Compaction can also be manually triggered. See [[Manual Compaction]]

See <code>include/rocksdb/options.h</code> and <code>include/rocksdb/advanced_options.h</code> for detailed explanation of these options

##  Leveled style compaction

See [[Leveled Compaction]].

##  Universal style compaction

For description about universal style compaction, see [Universal compaction style](https://github.com/facebook/rocksdb/wiki/Universal-Compaction)

If you're using Universal style compaction, there is an object <code>CompactionOptionsUniversal</code> that holds all the different options for that compaction. The exact definition is in <code>rocksdb/universal_compaction.h</code> and you can set it in <code>Options::compaction_options_universal</code>. Here we give a short overview of options in <code>CompactionOptionsUniversal</code>: <ul>

<li> <code>CompactionOptionsUniversal::size_ratio</code> - Percentage flexibility while comparing file size. If the candidate file(s) size is 1% smaller than the next file's size, then include next file into this candidate set. Default: 1

<li> <code>CompactionOptionsUniversal::min_merge_width</code> - The minimum number of files in a single compaction run. Default: 2

<li> <code>CompactionOptionsUniversal::max_merge_width</code> - The maximum number of files in a single compaction run. Default: UINT_MAX

<li> <code>CompactionOptionsUniversal::max_size_amplification_percent</code> - The size amplification is defined as the amount (in percentage) of additional storage needed to store a single byte of data in the database. For example, a size amplification of 2% means that a database that contains 100 bytes of user-data may occupy upto 102 bytes of physical storage. By this definition, a fully compacted database has a size amplification of 0%. Rocksdb uses the following heuristic to calculate size amplification: it assumes that all files excluding the earliest file contribute to the size amplification. Default: 200, which means that a 100 byte database could require upto 300 bytes of storage.

<li> <code>CompactionOptionsUniversal::compression_size_percent</code> - If this option is set to be -1 (the default value), all the output files will follow compression type specified. If this option is not negative, we will try to make sure compressed size is just above this value. In normal cases, at least this percentage of data will be compressed. When we are compacting to a new file, here is the criteria whether it needs to be compressed: assuming here are the list of files sorted by generation time: [ A1...An B1...Bm C1...Ct ], where A1 is the newest and Ct is the oldest, and we are going to compact B1...Bm, we calculate the total size of all the files as total_size, as well as the total size of C1...Ct as total_C, the compaction output file will be compressed iff total_C / total_size < this percentage

<li> <code>CompactionOptionsUniversal::stop_style</code> - The algorithm used to stop picking files into a single compaction run. Can be <code>kCompactionStopStyleSimilarSize</code> (pick files of similar size) or <code>kCompactionStopStyleTotalSize</code> (total size of picked files > next file). Default: <code>kCompactionStopStyleTotalSize</code>

<li> <code>CompactionOptionsUniversal::allow_trivial_move</code> - Option to optimize the universal multi level compaction by enabling trivial move for non overlapping files. Default: false.

</ul>

## FIFO Compaction Style

See [[FIFO compaction style]]

## Thread pools

Compactions are executed in thread pools. See [[Thread Pool]].