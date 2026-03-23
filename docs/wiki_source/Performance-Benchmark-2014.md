**This is an archived page. Visit [[Performance Benchmarks]] for latest benchmark results.**

***

'These benchmark measures RocksDB performance when data resides on flash storage.
(The benchmarks on this page are from 2014 and could be out-dated.)

# Setup

All of the benchmarks are run on the same machine. Here are the details of the test setup:

* 12 CPUs, HT enabled -> 24 vCPUs reported, 2 sockets X 6 cores/socket with X5650 @ 2.67GHz
* 2 FusionIO devices in SW RAID 0 that can do ~200k 4kb read/second at peak
* Fusion IO devices were about 50% to 70% full for each of the benchmark runs
* Machine has 144 GB of RAM
* Operating System Linux 2.6.38.4
* 1G rocksdb block cache
* 1 Billion keys; each key is of size 10 bytes, each value is of size 800 bytes
* total database size is 800GB, stored on XFS filesystem with TRIM support
* [jemalloc](https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919) memory allocator

The following benchmark results compare the performance of rocksdb compared to [leveldb](https://code.google.com/p/leveldb/). This is an IO bound workload where the database is 800GB while the machine has only 144GB of RAM. These results are obtained with a _release_ build of db_bench created via _make release_.

# Test 1. Bulk Load of keys in Random Order

Measure performance to load 1B keys into the database. The keys are inserted in random order. The database is empty at the beginning of this benchmark run and gradually fills up. No data is being read when the data load is in progress.

    rocksdb:   103 minutes, 80 MB/sec (total data size 481 GB, 1 billion key-values)
    leveldb:   many many days (in 20 hours it inserted only 200 million key-values)

Rocksdb was configured to first load all the data in L0 with compactions switched off and using an unsorted vector memtable. Then it made a second pass over the data to merge-sort all the files in L0 into sorted files in L1. Leveldb is very slow because of high write amplification. Here are the command(s) for loading the data into rocksdb

    echo "Bulk load database into L0...."
    bpl=10485760;mcz=2;del=300000000;levels=2;ctrig=10000000; delay=10000000; stop=10000000; wbn=30; mbc=20; \
    mb=1073741824;wbs=268435456; sync=0; r=1000000000; t=1; vs=800; bs=65536; cs=1048576; of=500000; si=1000000; \
    ./db_bench \
      --benchmarks=fillrandom --disable_seek_compaction=1 --mmap_read=0 --statistics=1 --histogram=1 \
      --num=$r --threads=$t --value_size=$vs --block_size=$bs --cache_size=$cs --bloom_bits=10 \
      --cache_numshardbits=4 --open_files=$of --verify_checksum=1 \
      --sync=$sync --disable_wal=1 --compression_type=zlib --stats_interval=$si --compression_ratio=0.5 \
      --write_buffer_size=$wbs --target_file_size_base=$mb --max_write_buffer_number=$wbn \
      --max_background_compactions=$mbc --level0_file_num_compaction_trigger=$ctrig \
      --level0_slowdown_writes_trigger=$delay --level0_stop_writes_trigger=$stop --num_levels=$levels \
      --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz \
      --stats_per_interval=1 --max_bytes_for_level_base=$bpl --memtablerep=vector --use_existing_db=0 \
      --disable_auto_compactions=1 --allow_concurrent_memtable_write=false
    echo "Running manual compaction to do a global sort map-reduce style...."
    bpl=10485760;mcz=2;del=300000000;levels=2;ctrig=10000000; delay=10000000; stop=10000000; wbn=30; mbc=20; \
    mb=1073741824;wbs=268435456; sync=0; r=1000000000; t=1; vs=800; bs=65536; cs=1048576; of=500000; si=1000000; \
    ./db_bench \
      --benchmarks=compact --disable_seek_compaction=1 --mmap_read=0 --statistics=1 --histogram=1 \
      --num=$r --threads=$t --value_size=$vs --block_size=$bs --cache_size=$cs --bloom_bits=10 \
      --cache_numshardbits=4 --open_files=$of --verify_checksum=1 \
      --sync=$sync --disable_wal=1 --compression_type=zlib --stats_interval=$si --compression_ratio=0.5 \
      --write_buffer_size=$wbs --target_file_size_base=$mb --max_write_buffer_number=$wbn \
      --max_background_compactions=$mbc --level0_file_num_compaction_trigger=$ctrig \
      --level0_slowdown_writes_trigger=$delay --level0_stop_writes_trigger=$stop --num_levels=$levels \
      --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz \
      --stats_per_interval=1 --max_bytes_for_level_base=$bpl --memtablerep=vector --use_existing_db=1 \
      --disable_auto_compactions=1 --allow_concurrent_memtable_write=false
    du -s -k test
    504730832   test


Here are the command(s) for loading the data into leveldb:

    echo "Bulk load database ...."
    wbs=268435456; r=1000000000; t=1; vs=800; cs=1048576; of=500000; ./db_bench --benchmarks=fillrandom --num=$r --threads=$t --value_size=$vs --cache_size=$cs --bloom_bits=10 --open_files=$of --db=/data/mysql/leveldb/test --compression_ratio=0.5 --write_buffer_size=$wbs --use_existing_db=0

# Test 2. Bulk Load of keys in Sequential Order

Measure performance to load 1B keys into the database. The keys are inserted in sequential order. The database is empty at the beginning of this benchmark run and gradually fills up. No data is being read when the data load is in progress.

    rocksdb:   36 minutes, 370 MB/sec (total data size 760 GB)
    leveldb:   91 minutes, 146 MB/sec (total data size 760 GB)

Rocksdb was configured to use multi-threaded compactions so that multiple threads could be simultaneously compacting (via file-renames) non-overlapping key ranges in multiple levels. This was the primary reason why rocksdb is much much faster than leveldb for this workload. Here are the command(s) for loading the database into rocksdb.

    echo "Load 1B keys sequentially into database....."
    bpl=10485760;mcz=2;del=300000000;levels=6;ctrig=4; delay=8; stop=12; wbn=3; \
    mbc=20; mb=67108864;wbs=134217728; sync=0; r=1000000000; t=1; vs=800; \
    bs=65536; cs=1048576; of=500000; si=1000000; \
    ./db_bench \
      --benchmarks=fillseq --disable_seek_compaction=1 --mmap_read=0 --statistics=1 \
      --histogram=1 --num=$r --threads=$t --value_size=$vs --block_size=$bs \
      --cache_size=$cs --bloom_bits=10 --cache_numshardbits=4 --open_files=$of \
      --verify_checksum=1 --sync=$sync --disable_wal=1 \
      --compression_type=zlib --stats_interval=$si --compression_ratio=0.5 \
      --write_buffer_size=$wbs --target_file_size_base=$mb \
      --max_write_buffer_number=$wbn --max_background_compactions=$mbc \
      --level0_file_num_compaction_trigger=$ctrig --level0_slowdown_writes_trigger=$delay \
      --level0_stop_writes_trigger=$stop --num_levels=$levels \
      --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz \
      --stats_per_interval=1 --max_bytes_for_level_base=$bpl --use_existing_db=0

Here are the command(s) for loading the data into leveldb:

    echo "Load 1B keys sequentially into database....."
    wbs=134217728; r=1000000000; t=1; vs=800; cs=1048576; of=500000; ./db_bench --benchmarks=fillseq --num=$r --threads=$t --value_size=$vs --cache_size=$cs --bloom_bits=10 --open_files=$of --db=/data/mysql/leveldb/test --compression_ratio=0.5 --write_buffer_size=$wbs --use_existing_db=0

# Test 3. Random Write

Measure performance to randomly overwrite 1B keys into the database. The database was first created by sequentially inserting all the 1 B keys. The results here do not measure the sequential-insertion phase, it measures only second part of the test that overwrites 1 B keys in random order. The test was run with the Write-Ahead-Log (WAL) enabled but fsync on commit was not done to the WAL.

    rocksdb: 15 hours 38 min;  56.295 micros/op, 17K ops/sec,  13.8 MB/sec
    leveldb: many many days;  600 micros/op,     1.6K ops/sec, 1.3 MB/sec
              (in 5 days it overwrote only 662 million out of 1 billion keys, after which I killed the test)

Rocksdb was configured with 20 compaction threads. These threads can simultaneously compact non-overlapping key ranges in the same or different levels. Rocksdb was also configured for a 1TB database by setting the number of levels to 6 so that write amplification is reduced. L0-L1 compactions were given priority to reduce stalls. zlib compression was enabled only for levels 2 and higher so that L0 compactions can occur faster. Files were configured to be 64 MB in size so that frequent fsyncs after creation of newly compacted files are reduced. Here are the commands to overwrite 1 B keys in rocksdb:

    echo "Overwriting the 1B keys in database in random order...."
    bpl=10485760;mcz=2;del=300000000;levels=6;ctrig=4; delay=8; stop=12; wbn=3; \
    mbc=20; mb=67108864;wbs=134217728; sync=0; r=1000000000; t=1; vs=800; \
    bs=65536; cs=1048576; of=500000; si=1000000; \
    ./db_bench \
      --benchmarks=overwrite --disable_seek_compaction=1 --mmap_read=0 --statistics=1 \
      --histogram=1 --num=$r --threads=$t --value_size=$vs --block_size=$bs \
      --cache_size=$cs --bloom_bits=10 --cache_numshardbits=4 --open_files=$of \
      --verify_checksum=1 --sync=$sync --disable_wal=1 \
      --compression_type=zlib --stats_interval=$si --compression_ratio=0.5 \
      --write_buffer_size=$wbs --target_file_size_base=$mb --max_write_buffer_number=$wbn \
      --max_background_compactions=$mbc --level0_file_num_compaction_trigger=$ctrig \
      --level0_slowdown_writes_trigger=$delay --level0_stop_writes_trigger=$stop \
      --num_levels=$levels --delete_obsolete_files_period_micros=$del \
      --min_level_to_compress=$mcz  --stats_per_interval=1 \
      --max_bytes_for_level_base=$bpl --use_existing_db=1

Here are the commands to overwrite 1 B keys in leveldb:

    echo "Overwriting the 1B keys in database in random order...."
    wbs=268435456; r=1000000000; t=1; vs=800; cs=1048576; of=500000; ./db_bench --benchmarks=overwrite --num=$r --threads=$t --value_size=$vs --cache_size=$cs --bloom_bits=10 --open_files=$of --db=/data/mysql/leveldb/test --compression_ratio=0.5 --write_buffer_size=$wbs --use_existing_db=1

# Test 4. Random Read

Measure random read performance of a database with 1 Billion keys, each  key is 10 bytes and value is 800 bytes. Rocksdb and leveldb were both configured with a block size of 4 KB. Data compression is not enabled. There were 32 threads in the benchmark application issuing random reads to the database. rocksdb is configured to verify checksums on every read while leveldb has checksum verification switched off.

    rocksdb:  70 hours,  8 micros/op, 126K ops/sec (checksum verification)
    leveldb: 102 hours, 12 micros/op,  83K ops/sec (no checksum verification)

Data was first loaded into the database by sequentially writing all the 1B keys to the database. Once the load is complete, the benchmark randomly picks a key and issues a read request. The above measure measurement does not include the data loading part, it measures only the part that issues the random reads to database. The reason rocksdb is faster is because it does not use mmaped IO because mmaped IOs on some linux platforms are known to be slow. Also, rocksdb shards the block cache into 64 parts to reduce lock contention. rocksdb is configured to avoid compactions triggered by seeks whereas leveldb does seek-compaction for this workload.

Here are the commands used to run the benchmark with rocksdb:

    echo "Load 1B keys sequentially into database....."
    bpl=10485760;mcz=2;del=300000000;levels=6;ctrig=4; delay=8; stop=12; wbn=3; \
    mbc=20; mb=67108864;wbs=134217728; sync=0; r=1000000000; t=1; vs=800; \
    bs=4096; cs=1048576; of=500000; si=1000000; \
    ./db_bench \
      --benchmarks=fillseq --disable_seek_compaction=1 --mmap_read=0 \
      --statistics=1 --histogram=1 --num=$r --threads=$t --value_size=$vs \
      --block_size=$bs --cache_size=$cs --bloom_bits=10 --cache_numshardbits=6 \
      --open_files=$of --verify_checksum=1 --sync=$sync --disable_wal=1 \
      --compression_type=none --stats_interval=$si --compression_ratio=0.5 \
      --write_buffer_size=$wbs --target_file_size_base=$mb \
      --max_write_buffer_number=$wbn --max_background_compactions=$mbc \
      --level0_file_num_compaction_trigger=$ctrig \
      --level0_slowdown_writes_trigger=$delay \
      --level0_stop_writes_trigger=$stop --num_levels=$levels \
      --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz \
      --stats_per_interval=1 --max_bytes_for_level_base=$bpl \
      --use_existing_db=0
    echo "Reading 1B keys in database in random order...."
    bpl=10485760;overlap=10;mcz=2;del=300000000;levels=6;ctrig=4; delay=8; \
    stop=12; wbn=3; mbc=20; mb=67108864;wbs=134217728; sync=0; r=1000000000; \
    t=32; vs=800; bs=4096; cs=1048576; of=500000; si=1000000; \
    ./db_bench \
      --benchmarks=readrandom --disable_seek_compaction=1 --mmap_read=0 \
      --statistics=1 --histogram=1 --num=$r --threads=$t --value_size=$vs \
      --block_size=$bs --cache_size=$cs --bloom_bits=10 --cache_numshardbits=6 \
      --open_files=$of --verify_checksum=1 --sync=$sync --disable_wal=1 \
      --compression_type=none --stats_interval=$si --compression_ratio=0.5 \
      --write_buffer_size=$wbs --target_file_size_base=$mb \
      --max_write_buffer_number=$wbn --max_background_compactions=$mbc \
      --level0_file_num_compaction_trigger=$ctrig \
      --level0_slowdown_writes_trigger=$delay \
      --level0_stop_writes_trigger=$stop --num_levels=$levels \
      --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz \
      --stats_per_interval=1 --max_bytes_for_level_base=$bpl \
      --use_existing_db=1

Here are the commands used to run the test on leveldb:

    echo "Load 1B keys sequentially into database....."
    wbs=134217728; r=1000000000; t=1; vs=800; cs=1048576; of=500000; ./db_bench --benchmarks=fillseq --num=$r --threads=$t --value_size=$vs --cache_size=$cs --bloom_bits=10 --open_files=$of --db=/data/mysql/leveldb/test --compression_ratio=0.5 --write_buffer_size=$wbs --use_existing_db=0
    echo "Reading the 1B keys in database in random order...."
    wbs=268435456; r=1000000000; t=32; vs=800; cs=1048576; of=500000; ./db_bench --benchmarks=readrandom --num=$r --threads=$t --value_size=$vs --cache_size=$cs --bloom_bits=10 --open_files=$of --db=/data/mysql/leveldb/test --compression_ratio=0.5 --write_buffer_size=$wbs --use_existing_db=1

# Test 5. Multi-threaded read and single-threaded write

Measure performance to randomly read 100M keys out of database with 1B keys and ongoing updates to existing keys. Number of read keys is configured to 100M to shorten the experiment time. Each key is 10 bytes and value is 800 bytes. Rocksdb and leveldb are both configured with a block size of 4 KB. Data compression is not enabled. There are 32 dedicated read threads in the benchmark issuing random reads to the database. A separate thread issues writes to the database at its best effort.

    rocksdb: 11 hours 30 minutes, 9.640 micros/read, 103734 reads/sec
    leveldb: 20 hours 28 minutes

Data is first loaded into the database by sequentially writing all 1B keys to the database. Once the load is complete, the benchmark spawns 32 threads, which randomly pick keys and issue read requests. Another separate write thread randomly picks keys and issues write requests at the same time. The reported time does not include data loading phase. It measures duration to complete reading 100M keys. The test is done using rocksdb release 2.7 and leveldb 1.15

Here are the commands used to run the benchmark with rocksdb:

    echo "Load 1B keys sequentially into database....."
    num=1073741824;bpl=536870912;mb=67108864;mcz=2;del=300000000;levels=6; \
    ctrig=4;delay=8;stop=12;wbn=3;mbc=20;wbs=134217728;sync=0;vs=800;bs=4096; \
    cs=17179869184;of=500000;si=10000000; \
    ./db_bench \
      --benchmarks=fillseq --disable_seek_compaction=1 --mmap_read=0 \
      --statistics=1 --histogram=1 --num=$num --threads=1 --value_size=$vs \
      --block_size=$bs --cache_size=$cs --bloom_bits=10 --cache_numshardbits=6 \
      --open_files=$of --verify_checksum=1 --sync=$sync --disable_wal=1 \
      --compression_type=none --stats_interval=$si --compression_ratio=1 \
      --write_buffer_size=$wbs --target_file_size_base=$mb \
      --max_write_buffer_number=$wbn --max_background_compactions=$mbc \
      --level0_file_num_compaction_trigger=$ctrig \
      --level0_slowdown_writes_trigger=$delay \
      --level0_stop_writes_trigger=$stop --num_levels=$levels \
      --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz \
      --stats_per_interval=1 --max_bytes_for_level_base=$bpl \
      --use_existing_db=0
    echo "Reading while writing 100M keys in database in random order...."
    num=134217728;bpl=536870912;mb=67108864;mcz=2;del=300000000;levels=6; \
    ctrig=4;delay=8;stop=12;wbn=3;mbc=20;wbs=134217728;sync=0;t=32;vs=800; \
    bs=4096;cs=17179869184;of=500000;si=10000000; \
    ./db_bench \
      --benchmarks=readwhilewriting --disable_seek_compaction=1 --mmap_read=0 \
      --statistics=1 --histogram=1 --num=$num --threads=$t --value_size=$vs \
      --block_size=$bs --cache_size=$cs --bloom_bits=10 --cache_numshardbits=6 \
      --open_files=$of --verify_checksum=1 --sync=$sync --disable_wal=0 \
      --compression_type=none --stats_interval=$si --compression_ratio=1 \
      --write_buffer_size=$wbs --target_file_size_base=$mb \
      --max_write_buffer_number=$wbn --max_background_compactions=$mbc \
      --level0_file_num_compaction_trigger=$ctrig \
      --level0_slowdown_writes_trigger=$delay \
      --level0_stop_writes_trigger=$stop --num_levels=$levels \
      --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz \
      --stats_per_interval=1 --max_bytes_for_level_base=$bpl \
      --use_existing_db=1

Here are the commands used to run the benchmark with leveldb:

    echo "Load 1B keys sequentially into database....."
    num=1073741824;vs=800;cs=17179869184;of=500000;wbs=268435456; ./db_bench --benchmarks=fillseq --histogram=1 --num=$num --threads=1 --value_size=$vs --cache_size=$cs --bloom_bits=10 --open_files=$of --db=$dir --compression_ratio=1 --use_existing_db=0 --write_buffer_size=$wbs
    echo "Read while writing 100M keys in database in random order...."
    num=134217728;vs=800;cs=17179869184;of=500000;wbs=268435456; ./db_bench --benchmarks=readwhilewriting --histogram=1 --num=$num --threads=$t --value_size=$vs --cache_size=$cs --bloom_bits=10 --open_files=$of --db=$dir --compression_ratio=1 --use_existing_db=1 --write_buffer_size=$wbs