The goal of these benchmarks is to measure the performance of RocksDB when the data resides in RAM. The system is configured to store transaction logs on persistent storage so that data is not lost on machine reboots.

# TL;DR
4.5M - 7M read QPS for point lookups with sustained writes. 4M - 6M QPS prefix range scans with sustained writes.

# Setup

All of the benchmarks are run on the same machine. Here are common setup used for following benchmark tests. Some configs vary based on tests and will be mentioned in the corresponding subsection.

* 2 Intel Xeon E5-2660 @ 2.2GHz, total 16 cores (32 with HT)
* 20MB CPU cache, 144GB Ram
* CentOS release 6.3 (Kernel 3.2.51)
* Commit (c90d446ee7b87682b1e0ec7e0c778d25a90c6294) from main branch
* 2 max writer buffers of 128MB each
* Level style compaction
* PlainTable SST format, with BloomFilter enabled
* HashSkipList memtable format, with BloomFilter disabled
* WAL is enabled and kept on spinning disk. However, they are not archived and no DB backup is performed during the benchmark
* Each key is of size 20 bytes, each value is of size 100 bytes
* No compression
* Database is initially loaded with 500M unique keys by using db_bench's **filluniquerandom** mode, then read performance is measured for a 7200-second run in **readwhilewriting** mode with 32 reader threads. Each reader thread issues random key request, which is guaranteed to be found. Another dedicated writer thread issues write requests in the meantime.
* Performance are measured for light write rate (10K writes/sec => ~1.2MB/sec) and moderate write rate (80K writes/sec => ~9.6 MB/sec) respectively. 
* Total database size is ~73GB at steady state, data files stored in tmpfs
* Statistics is disabled during the benchmark and perf_context level is set to 0
* [jemalloc](https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919) memory allocator


# Test 1. Point Lookup

In this test, whole key is indexed for fast point lookup (i.e. prefix_extractor = NewFixedPrefixTransform(20)). When using this setup, range query is not supported. It provides maximum throughput for point lookup queries.

### 80K writes/sec

Below is the reported read performance when write rate is set to 80K writes/sec:

    readwhilewriting :       0.220 micros/op 4553529 ops/sec; (1026765999 of 1026765999 found)

The actual sustained write rate is found to be at ~52K writes/sec. This test is CPU-bound, with ~98.4% of CPU observed in user space and ~1.6% of CPU spent in kernel during the measurement.

Here is the command for filling up the DB:

    ./db_bench --db=/mnt/db/rocksdb --num_levels=6 --key_size=20 --prefix_size=20 --keys_per_prefix=0 --value_size=100 --cache_size=17179869184 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 --min_level_to_compress=-1 --disable_seek_compaction=1 --hard_rate_limit=2 --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --disable_wal=0 --wal_dir=/data/users/rocksdb/WAL_LOG --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=1048576 --histogram=0 --use_plain_table=1 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash --bloom_bits=10 --bloom_locality=1 --benchmarks=filluniquerandom --use_existing_db=0 --num=524288000 --threads=1 --allow_concurrent_memtable_write=false

Here is the command for running readwhilewriting benchmark:

    ./db_bench --db=/mnt/db/rocksdb --num_levels=6 --key_size=20 --prefix_size=20 --keys_per_prefix=0 --value_size=100 --cache_size=17179869184 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 --min_level_to_compress=-1 --disable_seek_compaction=1 --hard_rate_limit=2 --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --disable_wal=0 --wal_dir=/data/users/rocksdb/WAL_LOG --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=1048576 --histogram=0 --use_plain_table=1 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash --bloom_bits=10 --bloom_locality=1 --duration=7200 --benchmarks=readwhilewriting --use_existing_db=1 --num=524288000 --threads=32 --benchmark_write_rate_limit=81920 --allow_concurrent_memtable_write=false

### 10K writes/sec

Below is the reported read performance when write rate is set to 10K writes/sec:

    readwhilewriting :       0.142 micros/op 7054002 ops/sec; (1587250999 of 1587250999 found)

The actual sustained write rate is found to be at ~10K writes/sec. This test is CPU-bound, with ~99.5% of CPU observed in user space and ~0.5% of CPU spent in kernel during the measurement.

Here is the command for filling up the DB:

    ./db_bench --db=/mnt/db/rocksdb --num_levels=6 --key_size=20 --prefix_size=20 --keys_per_prefix=0 --value_size=100 --cache_size=17179869184 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 --min_level_to_compress=-1 --disable_seek_compaction=1 --hard_rate_limit=2 --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --disable_wal=0 --wal_dir=/data/users/rocksdb/0_WAL_LOG --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=1048576 --histogram=0 --use_plain_table=1 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash --bloom_bits=10 --bloom_locality=1 --benchmarks=filluniquerandom --use_existing_db=0 --num=524288000 --threads=1 --allow_concurrent_memtable_write=false

Here is the command for running readwhilewriting benchmark:

    ./db_bench --db=/mnt/db/rocksdb --num_levels=6 --key_size=20 --prefix_size=20 --keys_per_prefix=0 --value_size=100 --cache_size=17179869184 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 --min_level_to_compress=-1 --disable_seek_compaction=1 --hard_rate_limit=2 --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --disable_wal=0 --wal_dir=/data/users/rocksdb/0_WAL_LOG --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=1048576 --histogram=0 --use_plain_table=1 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash --bloom_bits=10 --bloom_locality=1 --duration=7200 --benchmarks=readwhilewriting --use_existing_db=1 --num=524288000 --threads=32 --benchmark_write_rate_limit=10240 --allow_concurrent_memtable_write=false


# Test 2. Prefix Range Query

In this test, a key prefix size of 12 bytes is indexed (i.e. prefix_extractor = NewFixedPrefixTransform(12)). db_bench is configured to generate approximately 10 keys per unique prefix. In this setting, **Seek()** can be performed within a given prefix as well as iteration on the returned iterator. However, behavior of scanning beyond the prefix boundary is not defined. This is a fairly common access pattern for graph data. Point lookup throughput is measured under this setting:

### 80K writes/sec

Below is the reported read performance when write rate is set to 80K writes/sec:

    readwhilewriting :       0.251 micros/op 3979207 ops/sec; (893448999 of 893448999 found)

The actual sustained write rate is found to be at ~67K writes/sec. This test is CPU-bound, with ~98.0% of CPU observed in user space and ~2.0% of CPU spent in kernel during the measurement. 

Here is the command for filling up the DB:

    ./db_bench --db=/mnt/db/rocksdb --num_levels=6 --key_size=20 --prefix_size=12 --keys_per_prefix=10 --value_size=100 --cache_size=17179869184 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 --min_level_to_compress=-1 --disable_seek_compaction=1 --hard_rate_limit=2 --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --disable_wal=0 --wal_dir=/data/users/rocksdb/WAL_LOG --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=1048576 --histogram=0 --use_plain_table=1 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash --bloom_bits=10 --bloom_locality=1 --benchmarks=filluniquerandom --use_existing_db=0 --num=524288000 --threads=1  --allow_concurrent_memtable_write=false

Here is the command for running readwhilewriting benchmark:

    ./db_bench --db=/mnt/db/rocksdb --num_levels=6 --key_size=20 --prefix_size=12 --keys_per_prefix=10 --value_size=100 --cache_size=17179869184 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 --min_level_to_compress=-1 --disable_seek_compaction=1 --hard_rate_limit=2 --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --disable_wal=0 --wal_dir=/data/users/rocksdb/WAL_LOG --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=1048576 --histogram=0 --use_plain_table=1 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash --bloom_bits=10 --bloom_locality=1 --duration=7200 --benchmarks=readwhilewriting --use_existing_db=1 --num=524288000 --threads=32 --benchmark_write_rate_limit=81920 --allow_concurrent_memtable_write=false

### 10K writes/sec

Below is the reported read performance when write rate is set to 10K writes/sec:

    readwhilewriting :       0.168 micros/op 5942413 ops/sec; (1334998999 of 1334998999 found)

The actual sustained write rate is found to be at ~10K writes/sec. This test is CPU-bound, with ~99.4% of CPU observed in user space and ~0.6% of CPU spent in kernel during the measurement. 

Here is the command for filling up the DB:

    ./db_bench --db=/mnt/db/rocksdb --num_levels=6 --key_size=20 --prefix_size=12 --keys_per_prefix=10 --value_size=100 --cache_size=17179869184 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 --min_level_to_compress=-1 --disable_seek_compaction=1 --hard_rate_limit=2 --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --disable_wal=0 --wal_dir=/data/users/rocksdb/0_WAL_LOG --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=1048576 --histogram=0 --use_plain_table=1 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash --bloom_bits=10 --bloom_locality=1 --benchmarks=filluniquerandom --use_existing_db=0 --num=524288000 --threads=1  --allow_concurrent_memtable_write=false

Here is the command for running readwhilewriting benchmark:

    ./db_bench --db=/mnt/db/rocksdb --num_levels=6 --key_size=20 --prefix_size=12 --keys_per_prefix=10 --value_size=100 --cache_size=17179869184 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 --min_level_to_compress=-1 --disable_seek_compaction=1 --hard_rate_limit=2 --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --disable_wal=0 --wal_dir=/data/users/rocksdb/0_WAL_LOG --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=1048576 --histogram=0 --use_plain_table=1 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash --bloom_bits=10 --bloom_locality=1 --duration=7200 --benchmarks=readwhilewriting --use_existing_db=1 --num=524288000 --threads=32 --benchmark_write_rate_limit=10240 --allow_concurrent_memtable_write=false