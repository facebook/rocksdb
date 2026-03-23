We introduce a new SST file format based on [Cuckoo Hashing](http://en.wikipedia.org/wiki/Cuckoo_hashing) which is optimized for very high point lookup rates. Applications which don't use range scan but require very fast point lookups can use this new table format. See [here](https://rocksdb.org/blog/2014/09/12/cuckoo.html) for a detailed description of algorithm.
 
Advantages:
* For most lookups, only one memory access required per lookup.
* The database is smaller by 8 bytes per key after compaction as we don't store sequence number and value type in the key.

Limitations:
* Because the file format is hashing based, range scan is very slow
* Key and value lengths are fixed
* Does not support Merge Operator
* Does not support Snapshots
* File must be mmaped
* The last SST file in the database may have a utilization of only 50% in worst case.
 
We have plans to reduce some of the limitations in future.

### Usage
A new table factory can be created by calling `NewCuckooTableFactory()` function in `table.h`. See comments in [`include/rocksdb/table.h`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/table.h) or [blogpost](https://rocksdb.org/blog/2014/09/12/cuckoo.html) for description of parameters.
 
Examples:
```cpp
options.table_factory.reset(NewCuckooTableFactory());
```
or
```cpp
options.table_factory.reset(NewCuckooTableFactory(
    0.9 /* hash_table_ratio */,
    100 /* max_search_depth */,
    5 /* cuckoo_block_size */);
```
 
### File Format
The Cuckoo SST file comprises of:
* Hash table containing key value pairs. Empty buckets are filled using a special key outside the key range in the table.
* Property Block containing Table properties
* Metadata and footer.
 
```cpp
    <beginning_of_file>
      <beginning_of_hash_table>
        [key1 value1]
        [key2 value2]
        [key3 value3]
        ...
        [More key-values including empty buckets]
        ...
        [keyN valueN]
      <end_of_hash_table>
      [Property Block]
      [Footer]                               (fixed size; starts at file_size - sizeof(Footer))
    <end_of_file>
```
 
It must be noted that for optimizing hash computation, we fixed the number of buckets in a hash table to be a power of two. So, file sizes can only be a fixed set of values and hence the size of files may be considerably smaller than the values determined by `options.target_file_size_base` and `options.target_file_size_multiplier` parameters.
 
 
### Performance Results
We filled the db with 100M records in 8 files with key length of 8 bytes and value length of 4 bytes. We chose the default Cuckoo hash parameter: `hash_table_ratio=0.9, max_search_depth=100, cuckoo_block_size=5`.
 
Command to create and compact DB:
 
    ./db_bench --disable_seek_compaction=1 --statistics=1 --histogram=1 --cache_size=1048576 --bloom_bits=10 --cache_numshardbits=4 --open_files=500000 --verify_checksum=1 --write_buffer_size=1073741824 --max_write_buffer_number=2 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --delete_obsolete_files_period_micros=300000000 --max_grandparent_overlap_factor=10 --stats_per_interval=1 --stats_interval=10000000 --compression_type=none --compression_ratio=1 --memtablerep=vector --sync=0 --disable_data_sync=1 --key_size=8 --value_size=4 --num_levels=7 --threads=1 --mmap_read=1 --mmap_write=0 --max_bytes_for_level_base=4294967296 --target_file_size_base=201327616 --level0_file_num_compaction_trigger=10 --max_background_compactions=20 --use_existing_db=0 --disable_wal=1  --db=/mnt/tmp/cuckoo/2M100 --use_cuckoo_table=1 --use_uint64_comparator --benchmarks=filluniquerandom,compact --cuckoo_hash_ratio=0.9 --num=100000000
 
Random Read:

    readrandom   :       0.371 micros/op 2698931 ops/sec; (809679999 of 809679999 found)
    ./db_bench --stats_interval=10000000 --open_files=-1  --key_size=8 --value_size=4 --num_levels=7 --threads=1 --mmap_read=1 --mmap_write=0 --use_existing_db=1 --disable_wal=1  --db=/mnt/tmp/cuckoo/2M100 --use_cuckoo_table=1 --benchmarks=readrandom --num=100000000 --readonly --use_uint64_comparator --duration=300
 
Multi Random Read:

    multireadrandom :       0.278 micros/op 3601345 ops/sec; (1080449950 of 1080449950 found)
    ./db_bench --stats_interval=10000000 --open_files=-1  --key_size=8 --value_size=4  --num_levels=7 --threads=1 --mmap_read=1 --mmap_write=0 --use_existing_db=1 --disable_wal=1 --db=/mnt/tmp/cuckoo/2M100 --use_cuckoo_table=1 --benchmarks=multireadrandom --num=10000000 --duration=300 --readonly --batch_size=50 --use_uint64_comparator
 
Note that the MultiGet experiment contains an implementation of MultiGet() in readonly mode which is not yet checked in. The performance is likely to improve further as we submit more optimizations that we have identified.

