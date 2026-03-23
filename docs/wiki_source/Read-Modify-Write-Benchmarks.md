# Setup
We ran the benchmarks on the following machine:

* 16 CPUs, HT enabled -> 32 vCPUs, Intel Xeon E5-2660 @ 2.20GHz
* LSI 1.8TB Flash card (empty)
* 144GB RAM
* Linux 3.2.45

The goal of these benchmarks is to demonstrate the benefit of Merge operators on read-modify-write workloads, e.g. counters. Both benchmarks were executed using a single thread.

# Update random benchmark
We ran "update random" benchmark that executes 50.000.000 iterations of:

1. Read a random key
2. Write a new value to the random key

Each value was 8 bytes, simulating uint64_t counter. Write Ahead Log was turned off.

Here are the exact benchmark parameters we used:

    bpl=10485760;overlap=10;mcz=2;del=300000000;levels=6;ctrig=4; delay=8; stop=12; mbc=20; r=50000000; t=10; vs=8; bs=65536; si=1000000; time ./db_bench --benchmarks=updaterandom --disable_seek_compaction=1 --mmap_read=0 --statistics=1 --histogram=1 --num=$r --threads=$t --value_size=$vs --block_size=$bs --db=/data/sdb/ --disable_wal=1 --stats_interval=$si --max_background_compactions=$mbc --level0_file_num_compaction_trigger=$ctrig --level0_slowdown_writes_trigger=$delay --level0_stop_writes_trigger=$stop --num_levels=$levels --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz --max_grandparent_overlap_factor=$overlap --stats_per_interval=1 --max_bytes_for_level_base=$bpl --use_existing_db=0

Here is the result of the benchmark:

    29.852 micros/op 33498 ops/sec; ( updates:50000000 found:45003817)
    Total time 248 minutes and 46 seconds

# Merge operator update random
Using merge operator, we are able to perform read-modify-write using only one operator. In this benchmark, we performed 50.000.000 iterations of:

* Execute "uint64add" merge operator on a random key, which adds 1 to the value associated with the key

As in previous benchmark, each value was 8 bytes and Write Ahead Log was turned off.

Here are the exact benchmark parameters we used:

    bpl=10485760;overlap=10;mcz=2;del=300000000;levels=6;ctrig=4; delay=8; stop=12; mbc=20; r=50000000; t=10; vs=8; bs=65536; si=1000000; time ./db_bench --benchmarks=mergerandom --merge_operator=uint64add --disable_seek_compaction=1 --mmap_read=0 --statistics=1 --histogram=1 --num=$r --threads=$t --value_size=$vs --block_size=$bs --db=/data/sdb --disable_wal=1 --stats_interval=$si --max_background_compactions=$mbc --level0_file_num_compaction_trigger=$ctrig --level0_slowdown_writes_trigger=$delay --level0_stop_writes_trigger=$stop --num_levels=$levels --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz --max_grandparent_overlap_factor=$overlap --stats_per_interval=1 --max_bytes_for_level_base=$bpl --use_existing_db=0

Here is the result of the benchmark:

    9.444 micros/op 105892 ops/sec; ( updates:50000000)
    Total time 78 min and 53 sec