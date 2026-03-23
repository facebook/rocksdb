In April'14, we started building a [Java extension](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics) for RocksDB.  This page shows the benchmark results of RocksJava on flash storage.  The benchmark results of RocksDB C++ on flash storage can be found [here](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks).

# Setup
All of the benchmarks are run on the same machine. Here are the details of the test setup:

* Test with 1 billion key / value pairs. Each key is 16 bytes, and each value is 800 bytes. Total database raw size is ~1TB.
* Intel(R) Xeon(R) CPU E5-2660 v2 @ 2.20GHz, 40 cores.
* 25 MB CPU cache, 144 GB Ram
* CentOS release 5.2 (Final).
* Experiments were run under Funtoo chroot environment.
* g++ (Funtoo 4.8.1-r2) 4.8.1
* Java(TM) SE Runtime Environment (build 1.7.0_55-b13)
* Java HotSpot(TM) 64-Bit Server VM (build 24.55-b03, mixed mode)
* Commit [85f9bb4](https://github.com/facebook/rocksdb/commit/85f9bb4ef4845910f22d152b7c0d9c478da42fc7) was used in the experiment.
* 1G rocksdb block cache.
* Snappy 1.1.1 is used as the compression algorithm.
* JEMALLOC is not used.

# Bulk Load of keys in Sequential Order (Test 2)
This benchmark measures the performance of loading 1B keys into the database using RocksJava. The keys are inserted in sequential order.  The database is empty at the beginning of this benchmark run and gradually fills up.  No data is being read when the data load is in progress.  Below is the bulk-load performance of RocksJava:

    fillseq          :     2.48233 micros/op;  311.2 MB/s; 1000000000 ops done;  1 / 1 task(s) finished.

Similar to what we did in [RocksDB's C++ benchmark](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks), RocksJava was configured to use multi-threaded compactions so that multiple threads could be simultaneously compacting non-overlapping key ranges in multiple levels.  Our result shows that RocksJava is able to write 300+ MB/s, or ~400K writes per second. 

Here is the command for bulk-loading the database using RocksJava.

    bpl=10485760;overlap=10;mcz=0;del=300000000;levels=6;ctrig=4; delay=8; stop=12; wbn=3; mbc=20; mb=67108864;wbs=134217728; dds=0; sync=false; t=1; vs=800; bs=65536; cs=1048576; of=500000; si=1000000;
    ./jdb_bench.sh  --benchmarks=fillseq  --disable_seek_compaction=true  --mmap_read=false  --statistics=true  --histogram=true  --threads=$t  --key_size=10  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --compression_type=snappy  --cache_numshardbits=4  --open_files=$of  --verify_checksum=true  --db=/rocksdb-bench/java/b2  --sync=$sync  --disable_wal=true  --stats_interval=$si  --compression_ratio=0.50  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=false  --cache_remove_scan_count_limit=16 --num=1000000000

# Random Read (Test 4)
This benchmark measures the random read performance of RocksJava with 1 Billion keys, where each key is 16 bytes and value is 800 bytes respectively.  In this benchmark, RocksJava is configured with a block size of 4 KB, and Snappy compression is enabled.  There were 32 threads in the benchmark application issuing random reads to the database.  In addition, RocksJava is configured to verify checksums on every read.

The benchmark runs in two parts.  In the first part, data was first loaded into the database by sequentially writing all the 1B keys to the database.  Once the load is complete, it proceeds to the second part, in which 32 threads will be issuing random read requests concurrently.  We only measure the performance of the second part.

    readrandom       :     7.67180 micros/op;  101.4 MB/s; 1000000000 / 1000000000 found;  32 / 32 task(s) finished.

Our result shows that RocksJava is able to read 100+ MB/s, or processes ~130K reads per second.

Here are the commands used to run the benchmark with RocksJava:

    echo "Load 1B keys sequentially into database....."
    n=1000000000; r=1000000000; bpl=10485760;overlap=10;mcz=2;del=300000000;levels=6;ctrig=4; delay=8; stop=12; wbn=3; mbc=20; mb=67108864;wbs=134217728; dds=1; sync=false; t=1; vs=800; bs=4096; cs=1048576; of=500000; si=1000000;
    ./jdb_bench.sh  --benchmarks=fillseq  --disable_seek_compaction=true  --mmap_read=false  --statistics=true  --histogram=true  --num=$n  --threads=$t  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --cache_numshardbits=6  --open_files=$of  --verify_checksum=true  --db=/rocksdb-bench/java/b4  --sync=$sync  --disable_wal=true  --compression_type=snappy  --stats_interval=$si  --compression_ratio=0.50  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --min_level_to_compress=$mcz  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=false

    echo "Reading 1B keys in database in random order...."
    bpl=10485760;overlap=10;mcz=2;del=300000000;levels=6;ctrig=4; delay=8; stop=12; wbn=3; mbc=20; mb=67108864; wbs=134217728; dds=0; sync=false; t=32; vs=800; bs=4096; cs=1048576; of=500000; si=1000000;
    ./jdb_bench.sh  --benchmarks=readrandom  --disable_seek_compaction=true  --mmap_read=false  --statistics=true  --histogram=true  --num=$n  --reads=$r  --threads=$t  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --cache_numshardbits=6  --open_files=$of  --verify_checksum=true  --db=/rocksdb-bench/java/b4  --sync=$sync  --disable_wal=true  --compression_type=none  --stats_interval=$si  --compression_ratio=0.50  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --min_level_to_compress=$mcz  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=true

# Multi-Threaded Read and Single-Threaded Write (Test 5)
This benchmark measures performance of randomly reading 100M keys out of database with 1B keys while there are updates being issued concurrently.  Number of read keys in this benchmark is configured to 100M to shorten the experiment time.  Similar to the setting we had in Test 4, each key is again 16 bytes and value is 800 bytes respectively, and RocksJava is configured with a block size of 4 KB and with Snappy compression enabled.  In this benchmark, there are 32 dedicated read threads in the benchmark issuing random reads to the database while a separate thread issues random writes to the database at 10k writes per second.  Below is the random read while writing performance of RocksJava:

    readwhilewriting :     9.55882 micros/op;   81.4 MB/s; 100000000 / 100000000 found;  32 / 32 task(s) finished.

The result shows that RocksJava is able to read around 80 MB/s, or process ~100K reads per second, while updates are issued concurrently.

Here are the commands used to run the benchmark with RocksJava:

    echo "Load 1B keys sequentially into database....."
    dir="/rocksdb-bench/java/b5"
    num=1000000000; r=100000000;  bpl=536870912;  mb=67108864;  overlap=10;  mcz=2;  del=300000000;  levels=6;  ctrig=4; delay=8;  stop=12;  wbn=3;  mbc=20;  wbs=134217728;  dds=false;  sync=false;  vs=800;  bs=4096;  cs=17179869184; of=500000;  wps=0;  si=10000000;
    ./jdb_bench.sh  --benchmarks=fillseq  --disable_seek_compaction=true  --mmap_read=false  --statistics=true  --histogram=true  --num=$num  --threads=1  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --cache_numshardbits=6  --open_files=$of  --verify_checksum=true  --db=$dir  --sync=$sync  --disable_wal=true  --compression_type=snappy  --stats_interval=$si  --compression_ratio=0.5  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --min_level_to_compress=$mcz  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=false
    echo "Reading while writing 100M keys in database in random order...."
    bpl=536870912;mb=67108864;overlap=10;mcz=2;del=300000000;levels=6;ctrig=4;delay=8;stop=12;wbn=3;mbc=20;wbs=134217728;dds=false;sync=false;t=32;vs=800;bs=4096;cs=17179869184;of=500000;wps=10000;si=10000000;
    ./jdb_bench.sh  --benchmarks=readwhilewriting  --disable_seek_compaction=true  --mmap_read=false  --statistics=true  --histogram=true  --num=$num  --reads=$r  --writes_per_second=10000  --threads=$t  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --cache_numshardbits=6  --open_files=$of  --verify_checksum=true  --db=$dir  --sync=$sync  --disable_wal=false  --compression_type=snappy  --stats_interval=$si  --compression_ratio=0.5  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --min_level_to_compress=$mcz  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=true  --writes_per_second=$wps