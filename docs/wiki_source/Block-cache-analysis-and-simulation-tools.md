RocksDB may configure a certain amount of main memory as a block cache to accelerate data access. Understanding the efficiency of block cache is very important. The block cache analysis and simulation tools help a user to collect block cache access traces, analyze its access pattern, and evaluate alternative caching policies. 

### Table of Contents
* **[Quick Start](#quick-start)**<br>
* **[Tracing Block Cache Accesses](#tracing-block-cache-accesses)**<br>
* **[Trace Format](#trace-format)**<br>
* **[Cache Simulations](#cache-simulations)**<br>
  * [RocksDB Cache Simulations](#rocksdb-cache-simulations)<br>
  * [Python Cache Simulations](#python-cache-simulations)<br>
    * [Supported Cache Simulators](#supported-cache-simulators)<br>
* **[Analyzing Block Cache Traces](#analyzing-block-cache-traces)**<br>

# Quick Start
db_bench supports tracing block cache accesses. This section demonstrates how to trace accesses when running db_bench. It also shows how to analyze and evaluate caching policies using the generated trace file. 

Create a database: 
```
./db_bench --benchmarks="fillseq" \
--key_size=20 --prefix_size=20 --keys_per_prefix=0 --value_size=100 \
--cache_index_and_filter_blocks --cache_size=1048576 \
--disable_auto_compactions=1 --disable_wal=1 --compression_type=none \
--min_level_to_compress=-1 --compression_ratio=1 --num=10000000
```
To trace block cache accesses when running `readrandom` benchmark:
```
./db_bench --benchmarks="readrandom" --use_existing_db --duration=60 \
--key_size=20 --prefix_size=20 --keys_per_prefix=0 --value_size=100 \
--cache_index_and_filter_blocks --cache_size=1048576 \
--disable_auto_compactions=1 --disable_wal=1 --compression_type=none \
--min_level_to_compress=-1 --compression_ratio=1 --num=10000000 \
--threads=16 \
-block_cache_trace_file="/tmp/binary_trace_test_example" \
-block_cache_trace_max_trace_file_size_in_bytes=1073741824 \
-block_cache_trace_sampling_frequency=1
```
Convert the trace file to human readable format:
```
./block_cache_trace_analyzer \
-block_cache_trace_path=/tmp/binary_trace_test_example \
-human_readable_trace_file_path=/tmp/human_readable_block_trace_test_example
```
Evaluate alternative caching policies: 
```
bash block_cache_pysim.sh /tmp/human_readable_block_trace_test_example /tmp/sim_results/bench 1 0 30
```
Plot graphs:
```
python block_cache_trace_analyzer_plot.py /tmp/sim_results /tmp/sim_results_graphs
```

# Tracing Block Cache Accesses
RocksDB supports block cache tracing APIs `StartBlockCacheTrace` and `EndBlockCacheTrace`. When tracing starts, RocksDB logs detailed information of block cache accesses into a trace file. A user must specify a trace option and trace file path when start tracing block cache accesses.

A trace option contains `max_trace_file_size` and `sampling_frequency`.
- `max_trace_file_size` specifies the maximum size of the trace file. The tracing stops when the trace file size exceeds the specified `max_trace_file_size`.
- `sampling_frequency` determines how frequent should RocksDB trace an access. RocksDB uses spatial downsampling such that it traces all accesses to sampled blocks. A `sampling_frequency` of 1 means tracing all block cache accesses. A `sampling_frequency` of 100 means tracing all accesses on ~1% blocks. 

An example to start tracing block cache accesses: 
```
Env* env = rocksdb::Env::Default();
EnvOptions env_options;
std::string trace_path = "/tmp/binary_trace_test_example"
std::unique_ptr<TraceWriter> trace_writer;
DB* db = nullptr;
std::string db_name = "/tmp/rocksdb"

/*Create the trace file writer*/
NewFileTraceWriter(env, env_options, trace_path, &trace_writer);
DB::Open(options, dbname, &db);

/*Start tracing*/
db->StartBlockCacheTrace(trace_opt, std::move(trace_writer));

/* Your call of RocksDB APIs */

/*End tracing*/
db->EndBlockCacheTrace()
```

# Trace Format
We can convert the generated binary trace file into human readable trace file in csv format. It contains the following columns: 

| Column Name   |  Values     | Comment |
| :------------- |:-------------|:-------------|
| Access timestamp in microseconds     | unsigned long | |
| Block ID      |  unsigned long     | A unique block ID. |
| Block type | 7: Index block <br> 8: Filter block <br> 9: Data block <br> 10: Uncompressed dictionary block <br> 11: Range deletion block   | |
| Block size | unsigned long     | Block size may be 0 when <br> 1) compaction observes cache misses and does not insert the missing blocks into the cache. <br> 2) IO error when fetching a block. <br> 3) prefetching filter blocks but the SST file does not have filter blocks.  |
| Column family ID | unsigned long | A unique column family ID. | 
| Column family name | string   | |
| Level | unsigned long  | The LSM tree level of this block. |
| SST file number | unsigned long     | The SST file this block belongs to. | 
| Caller |  See [Caller](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/table_reader_caller.h) | The caller that accesses this block, e.g., Get, Iterator, Compaction, etc. |
| No insert | 0: do not insert the block upon a miss <br> 1: insert the block upon a cache miss   |  | 
| Get ID | unsigned long | A unique ID associated with the Get request. |
| Get key ID | unsigned long | The referenced key of the Get request. |
| Get referenced data size | unsigned long | The referenced data (key+value) size of the Get request. |
| Is a cache hit |  0: A cache hit <br> 1: A cache miss    | The running RocksDB instance observes a cache hit/miss on this block. |
| Get Does get referenced key exist in this block | 0: Does not exist <br> 1: Exist      | Data block only: Whether the referenced key is found in this block. |
| Get Approximate number of keys in this block | unsigned long | Data block only. |
| Get table ID | unsigned long     | The table ID of the Get request. We treat the first four bytes of the Get request as table ID. |
| Get sequence number | unsigned long | The sequence number associated with the Get request. |
| Block key size | unsigned long     |  |
| Get referenced key size | unsigned long     | |
| Block offset in the SST file | unsigned long  | |

# Cache Simulations
We support running cache simulators using both RocksDB built-in caches and caching policies written in python. The cache simulator replays the trace and reports the miss ratio given a cache capacity and a caching policy.

## RocksDB Cache Simulations
To replay the trace and evaluate alternative policies, we first need to provide a cache configuration file. An example file contains the following content: 
```
lru,0,0,16M,256M,1G,2G,4G,8G,12G,16G,1T
```
Cache configuration file format:

| Column Name   |  Values     |
| :------------- |:-------------|
| Cache name     | lru: LRU <br> lru_priority: LRU with midpoint insertion <br> lru_hybrid: LRU that also caches row keys <br> ghost_*: A ghost cache for admission control. It admits an entry on its second access. <ul><li>Specifically,  the ghost cache only maintains keys and is managed by LRU.</li><li>Upon an access, the cache inserts the key into the ghost cache.</li><li>Upon a cache miss, the cache inserts the missing entry only if it observes a hit in the ghost cache.</li></ul> |
| Number of shard bits      |  unsigned long     |
| Ghost cache capacity      |  unsigned long     |
| Cache sizes      |  A list of comma separated cache sizes      |

Next, we can start simulating caches. 
```
./block_cache_trace_analyzer -mrc_only=true \
-block_cache_trace_downsample_ratio=100 \
-block_cache_trace_path=/tmp/binary_trace_test_example \
-block_cache_sim_config_path=/tmp/cache_config \
-block_cache_analysis_result_dir=/tmp/binary_trace_test_example_results \
-cache_sim_warmup_seconds=3600
```

It contains two important parameters: 

`block_cache_trace_downsample_ratio`: The sampling frequency used to collect the trace. The simulator scales down the given cache size by this factor. For example, with downsample_ratio of 100, the cache simulator creates a 1 GB cache to simulate a 100 GB cache. 

`cache_sim_warmup_seconds`: The number of seconds used for warmup. The reported miss ratio does NOT include the number of misses/accesses during the warmup.

The analyzer outputs a few files: 
- A miss ratio curve file: `{trace_duration_in_seconds}_{total_accesses}_mrc`. 
- Three miss ratio timeline files per second (1), per minute (60), and per hour (3600). 
- Three number of misses timeline files per second (1), per minute (60), and per hour (3600). 

## Python Cache Simulations
We also support a more diverse set of caching policies written in python. In addition to LRU, it provides replacement policies using reinforcement learning, cost class, and more. To use the python cache simulator, we need to first convert the binary trace file into human readable trace file. 
```
./block_cache_trace_analyzer \
-block_cache_trace_path=/tmp/binary_trace_test_example \
-human_readable_trace_file_path=/tmp/human_readable_block_trace_test_example
```

block_cache_pysim.py options:
```
1) Cache type (ts, linucb, arc, lru, opt, pylru, pymru, pylfu, pyhb, gdsize, trace). 
One may evaluate the hybrid row_block cache by appending '_hybrid' to a cache_type, e.g., ts_hybrid. 
Note that hybrid is not supported with opt and trace. 
2) Cache size (xM, xG, xT).
3) The sampling frequency used to collect the trace. 
(The simulation scales down the cache size by the sampling frequency).
4) Warmup seconds (The number of seconds used for warmup).
5) Trace file path.
6) Result directory (A directory that saves generated results)
7) Max number of accesses to process. (Replay the entire trace if set to -1.)
8) The target column family. (The simulation will only run accesses on the target column family. 
If it is set to all, it will run against all accesses.)
```
One example:
```
python block_cache_pysim.py lru 16M 100 3600 /tmp/human_readable_block_trace_test_example /tmp/results 10000000 0 all 
```
We also provide a bash script to simulate a batch of cache configurations: 
```
Usage: ./block_cache_pysim.sh trace_file_path result_dir downsample_size warmup_seconds max_jobs

-max_jobs: The maximum number of simulators to run at a time.
```
One example: 
```
bash block_cache_pysim.sh /tmp/human_readable_block_trace_test_example /tmp/sim_results/bench 1 0 30
```
block_cache_pysim.py output the following files: 
- A miss ratio curve file: `data-ml-mrc-{cache_type}-{cache_size}-{target_cf_name}`. 
- Two files on the timeline of miss ratios per minute (60), and per hour (3600). 
- Two files on the timeline of number of misses per second (1), per minute (60), and per hour (3600). 
For `ts` and `linucb`, it also outputs the following files: 
- Two files on the timeline of percentage of times a policy is selected:  per minute (60) and per hour (3600). 
- Two files on the timeline of number of times a policy is selected: per minute (60) and per hour (3600). 

block_cache_pysim.sh combines the outputs of block_cache_pysim.py into following files: 
- One miss ratio curve file per target column family: `ml_{target_cf_name}_mrc`
- One files on the timeline of number of misses per `{target_cf_name}{capacity}{time_unit}`: `ml_{target_cf_name}{capacity}{time_unit}miss_timeline`
- One files on the timeline of miss ratios per `{target_cf_name}{capacity}{time_unit}`: `ml_{target_cf_name}{capacity}{time_unit}miss_ratio_timeline`
- One files on the timeline of number of times a policy is selected per `{target_cf_name}{capacity}{time_unit}`: `ml_{target_cf_name}{capacity}{time_unit}policy_timeline`
- One files on the timeline of percentage of times a policy is selected per `{target_cf_name}{capacity}{time_unit}`: `ml_{target_cf_name}{capacity}{time_unit}policy_ratio_timeline`


### Supported Cache Simulators 

| Cache Name   |  Comment     |
| :------------- |:-------------|
| lru | Strict (Least recently used) LRU cache. The cache maintains an LRU queue. |
| gdsize | GreedyDual Size. <br> N. Young. The k-server dual and loose competitiveness for paging. Algorithmica, June 1994, vol. 11,(no.6):525-41. Rewritten version of ''On-line caching as cache size varies'', in The 2nd Annual ACM-SIAM Symposium on Discrete Algorithms, 241-250, 1991. |
| opt | The Belady MIN algorithm.<br> L. A. Belady. 1966. A Study of Replacement Algorithms for a Virtual-storage Computer. IBM Syst. J. 5, 2 (June 1966), 78-101. DOI=http://dx.doi.org/10.1147/sj.52.0078 |
| arc | Adaptive replacement cache.<br> Nimrod Megiddo and Dharmendra S. Modha. 2003. ARC: A Self-Tuning, Low Overhead Replacement Cache. In Proceedings of the 2nd USENIX Conference on File and Storage Technologies (FAST '03). USENIX Association, Berkeley, CA, USA, 115-130. |
| pylru | LRU cache with random sampling. |
| pymru | (Most recently used) MRU cache with random sampling. |
| pylfu | (Least frequently used) LFU cache with random sampling. |
| pyhb | Hyperbolic Caching.<br> Aaron Blankstein, Siddhartha Sen, and Michael J. Freedman. 2017. Hyperbolic caching: flexible caching for web applications. In Proceedings of the 2017 USENIX Conference on Usenix Annual Technical Conference (USENIX ATC '17). USENIX Association, Berkeley, CA, USA, 499-511. |
| pyccbt | Cost class: block type  |
| pycccfbt | Cost class: column family + block type |
| ts | Thompson sampling <br> Daniel J. Russo, Benjamin Van Roy, Abbas Kazerouni, Ian Osband, and Zheng Wen. 2018. A Tutorial on Thompson Sampling. Found. Trends Mach. Learn. 11, 1 (July 2018), 1-96. DOI: https://doi.org/10.1561/2200000070 |
| linucb | Linear UCB <br> Lihong Li, Wei Chu, John Langford, and Robert E. Schapire. 2010. A contextual-bandit approach to personalized news article recommendation. In Proceedings of the 19th international conference on World wide web (WWW '10). ACM, New York, NY, USA, 661-670. DOI=http://dx.doi.org/10.1145/1772690.1772758 |
| trace | Trace |
| *_hybrid | A hybrid cache that also caches row keys. |

`py*` caches use random sampling at eviction time. It samples 64 random entries in the cache, sorts these entries based on a priority function, e.g., LRU, and evicts from the lowest priority entry until the cache has enough capacity to insert the new entry.

`pycc*` caches group cached entries by a cost class. The cache maintains aggregated statistics for each cost class such as number of hits, total size. A cached entry is also tagged with one cost class. At eviction time, the cache samples 64 random entries and group them by their cost class. It then evicts entries based on their cost class's statistics. 

`ts` and `linucb` are two caches using reinforcement learning. The cache is configured with N policies, e.g., LRU, MRU, LFU, etc. The cache learns which policy is the best overtime and selects the best policy for eviction. The cache rewards the selected policy if the policy has not evicted the missing key before. 
`ts` does not use any feature of a block while `linucb` uses three features: a block's level, column family, and block type. 

`trace` reports the misses observed in the collected trace. 

# Analyzing Block Cache Traces
The `block_cache_trace_analyzer` analyzes a trace file and outputs useful statistics of the access pattern. It provides insights into how to tune and improve a caching policy. 

The `block_cache_trace_analyzer` may output statistics into multiple csv files saved in a result directory. We can plot graphs on these statistics using `block_cache_trace_analyzer_plot.py`. 

Analyzer options:
```
 -access_count_buckets (Group number of blocks by their access count given
  these buckets. If specified, the analyzer will output a detailed analysis
  on the number of blocks grouped by their access count break down by block
  type and column family.) type: string default: ""
-analyze_blocks_reuse_k_reuse_window (Analyze the percentage of blocks that
  are accessed in the [k, 2*k] seconds are accessed again in the next [2*k,
  3*k], [3*k, 4*k],...,[k*(n-1), k*n] seconds. ) type: int32 default: 0
-analyze_bottom_k_access_count_blocks (Print out detailed access
  information for blocks with their number of accesses are the bottom k
  among all blocks.) type: int32 default: 0
-analyze_callers (The list of callers to perform a detailed analysis on. If
  speicfied, the analyzer will output a detailed percentage of accesses for
  each caller break down by column family, level, and block type. A list of
  available callers are: Get, MultiGet, Iterator, ApproximateSize,
  VerifyChecksum, SSTDumpTool, ExternalSSTIngestion, Repair, Prefetch,
  Compaction, CompactionRefill, Flush, SSTFileReader, Uncategorized.)
  type: string default: ""
-analyze_correlation_coefficients_labels (Analyze the correlation
  coefficients of features such as number of past accesses with regard to
  the number of accesses till the next access.) type: string default: ""
-analyze_correlation_coefficients_max_number_of_values (The maximum number
  of values for a feature. If the number of values for a feature is larger
  than this max, it randomly selects 'max' number of values.) type: int32
  default: 1000000
-analyze_get_spatial_locality_buckets (Group data blocks by their
  statistics using these buckets.) type: string default: ""
-analyze_get_spatial_locality_labels (Group data blocks using these
  labels.) type: string default: ""
-analyze_top_k_access_count_blocks (Print out detailed access information
  for blocks with their number of accesses are the top k among all blocks.)
  type: int32 default: 0
-block_cache_analysis_result_dir (The directory that saves block cache
  analysis results.) type: string default: ""
-block_cache_sim_config_path (The config file path. One cache configuration
  per line. The format of a cache configuration is
  cache_name,num_shard_bits,ghost_capacity,cache_capacity_1,...,cache_capacity_N. 
  Supported cache names are lru, lru_priority, lru_hybrid. User may also add 
  a prefix 'ghost_' to a cache_name to add a ghost cache in front of the real 
  cache. ghost_capacity and cache_capacity can be xK, xM or xG where 
  x is a positive number.)
  type: string default: ""
-block_cache_trace_downsample_ratio (The trace collected accesses on one in
  every block_cache_trace_downsample_ratio blocks. We scale down the
  simulated cache size by this ratio.) type: int32 default: 1
-block_cache_trace_path (The trace file path.) type: string default: ""
-cache_sim_warmup_seconds (The number of seconds to warmup simulated
  caches. The hit/miss counters are reset after the warmup completes.)
  type: int32 default: 0
-human_readable_trace_file_path (The filt path that saves human readable
  access records.) type: string default: ""
-mrc_only (Evaluate alternative cache policies only. When this flag is
  true, the analyzer does NOT maintain states of each block in memory for
  analysis. It only feeds the accesses into the cache simulators.)
  type: bool default: false
-print_access_count_stats (Print access count distribution and the
  distribution break down by block type and column family.) type: bool
  default: false
-print_block_size_stats (Print block size distribution and the distribution
  break down by block type and column family.) type: bool default: false
-print_data_block_access_count_stats (Print data block accesses by user Get
  and Multi-Get.) type: bool default: false
-reuse_distance_buckets (Group blocks by their reuse distances given these
  buckets. For example, if 'reuse_distance_buckets' is '1K,1M,1G', we will
  create four buckets. The first three buckets contain the number of blocks
  with reuse distance less than 1KB, between 1K and 1M, between 1M and 1G,
  respectively. The last bucket contains the number of blocks with reuse
  distance larger than 1G. ) type: string default: ""
-reuse_distance_labels (Group the reuse distance of a block using these
  labels. Reuse distance is defined as the cumulated size of unique blocks
  read between two consecutive accesses on the same block.) type: string
  default: ""
-reuse_interval_buckets (Group blocks by their reuse interval given these
  buckets. For example, if 'reuse_distance_buckets' is '1,10,100', we will
  create four buckets. The first three buckets contain the number of blocks
  with reuse interval less than 1 second, between 1 second and 10 seconds,
  between 10 seconds and 100 seconds, respectively. The last bucket
  contains the number of blocks with reuse interval longer than 100
  seconds.) type: string default: ""
-reuse_interval_labels (Group the reuse interval of a block using these
  labels. Reuse interval is defined as the time between two consecutive
  accesses on the same block.) type: string default: ""
-reuse_lifetime_buckets (Group blocks by their reuse lifetime given these
  buckets. For example, if 'reuse_lifetime_buckets' is '1,10,100', we will
  create four buckets. The first three buckets contain the number of blocks
  with reuse lifetime less than 1 second, between 1 second and 10 seconds,
  between 10 seconds and 100 seconds, respectively. The last bucket
  contains the number of blocks with reuse lifetime longer than 100
  seconds.) type: string default: ""
-reuse_lifetime_labels (Group the reuse lifetime of a block using these
  labels. Reuse lifetime is defined as the time interval between the first
  access on a block and the last access on the same block. For blocks that
  are only accessed once, its lifetime is set to kMaxUint64.) type: string
  default: ""
-skew_buckets (Group the skew labels using these buckets.) type: string
  default: ""
-skew_labels (Group the access count of a block using these labels.)
  type: string default: ""
-timeline_labels (Group the number of accesses per block per second using
  these labels. Possible labels are a combination of the following: cf
  (column family), sst, level, bt (block type), caller, block. For example,
  label "cf_bt" means the number of acccess per second is grouped by unique
  pairs of "cf_bt". A label "all" contains the aggregated number of
  accesses per second across all possible labels.) type: string default: ""
```

An example that outputs a statistics summary of the access pattern: 
```
./block_cache_trace_analyzer -block_cache_trace_path=/tmp/test_trace_file_path
```

Another example: 
```
./block_cache_trace_analyzer \
-block_cache_trace_path=/tmp/test_trace_file_path \
-block_cache_analysis_result_dir=/tmp/sim_results/test_trace_results \
-print_block_size_stats \
-print_access_count_stats \
-print_data_block_access_count_stats \
-timeline_labels=cf,level,bt,caller \
-analyze_callers=Get,Iterator,Compaction \
-access_count_buckets=1,2,3,4,5,6,7,8,9,10,100,10000,100000,1000000 \
-analyze_bottom_k_access_count_blocks=10 \
-analyze_top_k_access_count_blocks=10 \
-reuse_lifetime_labels=cf,level,bt \
-reuse_lifetime_buckets=1,10,100,1000,10000,100000,1000000 \
-reuse_interval_labels=cf,level,bt,caller \
-reuse_interval_buckets=1,10,100,1000,10000,100000,1000000 \
-analyze_blocks_reuse_k_reuse_window=3600 \
-analyze_get_spatial_locality_labels=cf,level,all \
-analyze_get_spatial_locality_buckets=10,20,30,40,50,60,70,80,90,100,101 \
-analyze_correlation_coefficients_labels=all,cf,level,bt,caller \
-skew_labels=block,bt,table,sst,cf,level \
-skew_buckets=10,20,30,40,50,60,70,80,90,100
```

Next, we can plot graphs using the following command:
```
python block_cache_trace_analyzer_plot.py /tmp/sim_results /tmp/sim_results_graphs
```