# db_bench
`db_bench` is the main tool that is used to benchmark RocksDB's performance. RocksDB inherited db_bench from LevelDB, and enhanced it to support many additional options. db_bench supports many benchmarks to generate different types of workloads, and its various options can be used to control the tests. 

If you are just getting started with db_bench, here are a few things you can try:
1. Start with a simple benchmark like fillseq (or fillrandom) to create a database and fill it with some data
```bash
./db_bench --benchmarks="fillseq"
```
If you want more stats, add the meta operator "stats" and --statistics flag. 
```bash
./db_bench --benchmarks="fillseq,stats" --statistics
```
2. Read the data back
```bash
./db_bench --benchmarks="readrandom" --use_existing_db
```

You can also combine multiple benchmarks to the string that is passed to `--benchmarks` so that they run sequentially. Example:
```bash
./db_bench --benchmarks="fillseq,readrandom,readseq"
```
More in-depth example of db_bench usage can be found [here](https://github.com/facebook/rocksdb/wiki/performance-benchmarks) and [here](https://github.com/facebook/rocksdb/wiki/RocksDB-In-Memory-Workload-Performance-Benchmarks).

Benchmarks List:
```
      	fillseq       -- write N values in sequential key order in async mode
      	fillseqdeterministic       -- write N values in the specified key order and keep the shape of the LSM tree
      	fillrandom    -- write N values in random key order in async mode
      	filluniquerandomdeterministic       -- write N values in a random key order and keep the shape of the LSM tree
      	overwrite     -- overwrite N values in random key order in async mode
      	fillsync      -- write N/100 values in random key order in sync mode
      	fill100K      -- write N/1000 100K values in random order in async mode
      	deleteseq     -- delete N keys in sequential order
      	deleterandom  -- delete N keys in random order
      	readseq       -- read N times sequentially
      	readtocache   -- 1 thread reading database sequentially
      	readreverse   -- read N times in reverse order
      	readrandom    -- read N times in random order
      	readmissing   -- read N missing keys in random order
      	readwhilewriting      -- 1 writer, N threads doing random reads
      	readwhilemerging      -- 1 merger, N threads doing random reads
      	readrandomwriterandom -- N threads doing random-read, random-write
      	prefixscanrandom      -- prefix scan N times in random order
      	updaterandom  -- N threads doing read-modify-write for random keys
      	appendrandom  -- N threads doing read-modify-write with growing values
      	mergerandom   -- same as updaterandom/appendrandom using merge operator. Must be used with merge_operator
      	readrandommergerandom -- perform N random read-or-merge operations. Must be used with merge_operator
      	newiterator   -- repeated iterator creation
      	seekrandom    -- N random seeks, call Next seek_nexts times per seek
      	seekrandomwhilewriting -- seekrandom and 1 thread doing overwrite
      	seekrandomwhilemerging -- seekrandom and 1 thread doing merge
      	crc32c        -- repeated crc32c of 4K of data
      	xxhash        -- repeated xxHash of 4K of data
      	acquireload   -- load N*1000 times
      	fillseekseq   -- write N values in sequential key, then read them by seeking to each key
      	randomtransaction     -- execute N random transactions and verify correctness
      	randomreplacekeys     -- randomly replaces N keys by deleting the old version and putting the new version
        timeseries            -- 1 writer generates time series data and multiple readers doing random reads on id
```

For a list of all options:
```
$ ./db_bench -help
```

# persistent_cache_bench
```
$ ./persistent_cache_bench -help
persistent_cache_bench:
USAGE:
./persistent_cache_bench [OPTIONS]...
...
  Flags from utilities/persistent_cache/persistent_cache_bench.cc:
    -benchmark (Benchmark mode) type: bool default: false
    -cache_size (Cache size) type: uint64 default: 18446744073709551615
    -cache_type (Cache type. (block_cache, volatile, tiered)) type: string
      default: "block_cache"
    -enable_pipelined_writes (Enable async writes) type: bool default: false
    -iosize (Read IO size) type: int32 default: 4096
    -log_path (Path for the log file) type: string default: "/tmp/log"
    -nsec (nsec) type: int32 default: 10
    -nthread_read (Lookup threads) type: int32 default: 1
    -nthread_write (Insert threads) type: int32 default: 1
    -path (Path for cachefile) type: string default: "/tmp/microbench/blkcache"
    -volatile_cache_pct (Percentage of cache in memory tier.) type: int32
      default: 10
    -writer_iosize (File writer IO size) type: int32 default: 4096
    -writer_qdepth (File writer qdepth) type: int32 default: 1