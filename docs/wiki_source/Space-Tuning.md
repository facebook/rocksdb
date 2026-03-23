Sometimes users see high space utilization but hope to put more data to the same disk because other system metrics are still available. 

Block size, Compression algorithm, and Compaction affect a lot for space efficiency. Most configurations favoring space efficiency have side effects of degrading CPU, write amplification, or both. Optimal configurations heavily depend on workloads.

## Compaction
Compaction is the most effective way of reducing space. [[Leveled Compaction]] usually generates better space saving than [[Universal Compaction]]. Within leveled compaction, `options.max_bytes_for_level_multiplier` is the most effective option for space utilization, and for universal compaction, `options.CompactionOptionsUniversal.max_size_amplification_percent` is effective.

Sometimes compaction leaves behind garbage for normal scheduling. Check `options.ttl` and `options.periodic_compaction_seconds` for some finer control.

## Block Size
 Default RocksDB block size is 4KB before compression. We recommend to try 16KB for a good space saving. Bigger block size is more space efficient with compression, but needs more CPU time for decompression, especially when you need only part of the blocks, such as point lookups or short range scans. Check BLOCK_DECOMPRESS_TIME in Perf Context if you change block size and see how much extra CPU time is spent for decompression

## Index Block
 Smaller Block size has side effects of increasing index block size. 8KB data block will have 2x index block size compared to 16KB. If RocksDB block cache can not cache active index blocks in memory, it may cause significant slowdown for decompressing index blocks on cache miss.  It is generally a good practice to make bloom filter and index blocks smaller than block cache so that most of them fit in memory. The following three column family options are effective to tune index blocks.

```
format_version=5  (default is 4)
index_block_restart_interval=16  (default is 1)
enable_index_compression=false  (default is ON)
```

The first two options save index block size significantly (at least more than 4 times smaller than usual). The last option stores index blocks uncompressed, which saves decompression on cache miss. It increases space for index blocks, but if index block size is small enough, overhead will be negligible.

Here is index and bloom filter size in one example DB.
```
+------------------+----------------+----------------+
| data_gb          | index_gb       | filter_gb      |
+------------------+----------------+----------------+
| 624.570878885686 | 0.972540895455 | 1.161030644551 |
+------------------+----------------+----------------+
```
Index and Filter size is pre-compressed size, while data size is after compressed size. Index block size is less than 0.1% so we’re fine with operating with enable_index_compression=false.

## Bloom Filter
See details: https://fb.workplace.com/notes/yoshinori-matsunobu/rocksdb-index-and-filter-tuning-for-low-memory-hosts-timelinedb-case/2765405317072966/

## Compression Algorithm
Use Zstandard compression algorithm, at least in the bottommost level. There is no reason to use Zlib anymore.

## Compression Level 
Default Zstandard compression level is 3 and it is also a default in RocksDB. Zstandard allows to set compression level from 1 to 19 and it trades compression speed and ratio (https://github.com/facebook/zstd). In one typical DB, we use level 6 and in general we use “compression_opts=-14:6:0”.

## Hybrid Compression
RocksDB has multiple levels from L0 to Lmax. While RocksDB has 90% of the data in Lmax by default, most compactions typically happen at higher levels than Lmax. This drove us to configure different compression algorithms between levels. It made lots of sense to use a strong compression algorithm in Lmax, and to use faster algorithms such as LZ4 or even no compression in higher levels. RocksDB allows users to explicitly set a specific compression algorithm in botttommost level. We recommend “compression_per_level=kLZ4Compression;bottommost_compression=kZSTD”.

## Dictionary Compression
Dictionary Compression benefits vary depending on data characteristics, compression level and block size. If data blocks from the same SST files contain similar data, it can dramatically compress data better. Dictionary compression is less efficient with larger block sizes. With 16 KB blocks, which is no longer "small", dictionary can sometimes still provide some benefits. If users use 4KB blocks for read performance reasons, dictionary compression might provide some good space saving.
