**This is an archived page. Visit [[Performance Benchmarks]] for latest benchmark results.**

***

These benchmarks measure RocksDB performance when data resides on flash storage.
(The benchmarks on this page were generated in July 2018 with RocksDB 5.13.1)

# Setup

All of the benchmarks are run on the same AWS instance. Here are the details of the test setup:

* Instance type: i3.8xlarge
  * 32 vCPUs reported, Intel Xeo E5-2686 v4 (Broadwell) @ 2.3GHz
* Machine has 244 GB of RAM
* 4 x 1.9TB NVMe SSD storage (XFS filesystem)
* Kernel version: 4.4.0-1054-aws
* 1G rocksdb block cache
* 8 Billion keys; each key is of size 20 bytes, each value is of size 400 bytes
* [jemalloc](https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919) memory allocator

This is an IO bound workload where the data is 3.2TB while the machine has only 244GB of RAM. These results are obtained with a _release_ build of db_bench created via _make release_, using the `tools/benchmark.sh` with minor changes (`NUM_THREADS=64 NUM_KEYS=8000000000 CACHE_SIZE=17179869184`).

# Test 1. Bulk Load of keys in Random Order

Measure performance to load 8 billion keys into the database. The keys are inserted in random order. The database is empty at the beginning of this benchmark run and gradually fills up. No data is being read when the data load is in progress.

    rocksdb:   157.6 minutes, 353.6 MB/sec (3.2TB ingested, file size 1.5TB)

Rocksdb was configured to first load all the data in L0 with compactions switched off and using an unsorted vector memtable. Then it made a second pass over the data to merge-sort all the files in L0 into sorted files in L1.  Here are the commands we used for loading the data into rocksdb:

    export DB_DIR=/raid/db
    export WAL_DIR=/raid/wal
    export TEMP=/raid/tmp
    export OUTPUT_DIR=/raid/output
    tools/benchmark.sh bulkload
    du -s -k /raid/db
    1498565136      /raid/db


# Test 2. Bulk Load of keys in Sequential Order

Measure performance to load 8 billion keys into the database. The keys are inserted in sequential order. The database is empty at the beginning of this benchmark run and gradually fills up. No data is being read when the data load is in progress.

    rocksdb:   159 minutes, 335.7 MB/sec (3.2TB ingested, file size 1.45 TB)

Rocksdb was configured to use multi-threaded compactions so that multiple threads could be simultaneously compacting (via file-renames) non-overlapping key ranges in multiple levels. This was the primary reason why rocksdb is much much faster than leveldb for this workload. Here are the command(s) for loading the database into rocksdb.

    tools/benchmark.sh fillseq_disable_wal
    du -s -k /raid/db
    1448482656      /raid/db

# Test 3. Random Write

Measure performance to randomly overwrite 2 billion keys into the database. The database was first created by the previous benchmark, sequentially inserting 8 billion keys. The test was run with the Write-Ahead-Log (WAL) enabled but fsync on commit was not done to the WAL.  This test uses a single thread.

    rocksdb: 15 hours 38 min;  56.295 micros/op, 17K ops/sec,  13.8 MB/sec

Rocksdb was configured with 20 compaction threads. These threads can simultaneously compact non-overlapping key ranges in the same or different levels. Rocksdb was also configured for a 1TB database by setting the number of levels to 6 so that write amplification is reduced. L0-L1 compactions were given priority to reduce stalls. zlib compression was enabled only for levels 2 and higher so that L0 compactions can occur faster. Files were configured to be 64 MB in size so that frequent fsyncs after creation of newly compacted files are reduced. Here are the commands to overwrite 2 billion keys in rocksdb:

    NUM_KEYS=2000000000 NUM_THREADS=1 tools/benchmark.sh overwrite

# Test 4. Random Read

Measure random read performance of a database with 1 Billion keys, each key is 10 bytes and value is 800 bytes. Rocksdb and leveldb were both configured with a block size of 4 KB. Data compression is not enabled. There was a single thread in the benchmark application issuing random reads (1 Billion keys read) to the database. rocksdb is configured to verify checksums on every read while leveldb has checksum verification switched off.

    rocksdb:  18 hours,  64.7 micros/op, 15.4K ops/sec (checksum verification)

Data was first loaded into the database by sequentially writing all the 8 billion keys to the database. Once the load is complete, the benchmark randomly picks a key and issues a read request. The above measure measurement does not include the data loading part, it measures only the part that issues the random reads to database. The reason rocksdb is faster is because it does not use mmaped IO because mmaped IOs on some Linux platforms are known to be slow. Also, rocksdb shards the block cache into 64 parts to reduce lock contention. rocksdb is configured to avoid compactions triggered by seeks whereas leveldb does seek-compaction for this workload.

Here are the commands used to run the benchmark with rocksdb:

    NUM_KEYS=1000000000 NUM_THREADS=1 tools/benchmark.sh readrandom

# Test 5. Multi-threaded read and single-threaded write

Measure performance to randomly read 100M keys out of database with 8B keys and ongoing updates to existing keys. Number of read keys is configured to 100M to shorten the experiment time. Each key is 10 bytes and value is 800 bytes. Rocksdb and leveldb are both configured with a block size of 4 KB. Data compression is not enabled. There are 32 dedicated read threads in the benchmark issuing random reads to the database. A separate thread issues writes to the database at its best effort.

    rocksdb: 75 minutes, 1.42 micros/read, 713376 reads/sec

Data is first loaded into the database by sequentially writing all 8B keys to the database. Once the load is complete, the benchmark spawns 32 threads, which randomly pick keys and issue read requests. Another separate write thread randomly picks keys and issues write requests at the same time. The reported time does not include data loading phase. It measures duration to complete reading 100M keys. The test is done using rocksdb release 5.13.

Here are the commands used to run the benchmark with rocksdb:

    NUM_KEYS=100000000 NUM_THREADS=32 tools/benchmark.sh readwhilewriting