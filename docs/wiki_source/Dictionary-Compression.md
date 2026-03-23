## Purpose

Dynamic dictionary compression algorithms have trouble compressing small data. With default usage, the compression dictionary starts off empty and is constructed during a single pass over the input data. Thus small input leads to a small dictionary, which cannot achieve good compression ratios.

In RocksDB, each block in a block-based table (SST file) is compressed individually. Block size defaults to 4KB, from which the compression algorithm cannot build a sizable dictionary. The dictionary compression feature presets the dictionary with data sampled from multiple blocks, which improves compression ratio when there are repetitions across blocks.

## Usage

Set `rocksdb::CompressionOptions::max_dict_bytes` (in `include/rocksdb/options.h`) to a nonzero value indicating the maximum per-file dictionary size.

Also `rocksdb::CompressionOptions::zstd_max_train_bytes` may be used to generate a training dictionary of max bytes for ZStd compression. Using ZStd's dictionary trainer can achieve even better compression ratio improvements than using `max_dict_bytes` alone. The training data will be used to generate a dictionary of `max_dict_bytes`.

## Implementation

The dictionary will be constructed for each SST file in a subcompaction by sampling entire data blocks in the file. When dictionary compression is enabled, the uncompressed data blocks in the file being generated will be buffered in memory, upto ```target_file_size``` bytes. Once the limit is reached, or the file is finished, data blocks are taken uniformly/randomly from the buffered data blocks and used to train the ZStd dictionary trainer.

Since 6.25, the memory usage during buffering uncompressed data blocks for gathering training samples is charged to block cache. Unbuffering those data blocks can now be triggered if the block cache becomes full and `strict_capacity_limit=true` for the block cache, in addition to existing conditions that can trigger unbuffering.

The dictionary is stored in the file's meta-block in order for it to be known when uncompressing. During reads, if ```BlockBasedTableOptions::cache_index_and_filter_blocks``` is ```false```, the dictionary meta-block is read and pinned in memory but not charged to the block cache. If it is ```true```, the dictionary meta-block is cached in the block cache, with high priority if `cache_index_and_filter_blocks_with_high_priority` is `true`, and with low priority otherwise. As for prefetching and pinning the dictionary meta-block in the cache, the behavior depends on the RocksDB version. In RocksDB versions lower than 6.4, the dictionary meta-block is not prefetched or pinned in the cache, unlike index and filter blocks. Starting RocksDB version 6.4, the dictionary meta-block is prefetched and pinned in the block cache the same way as index/filter blocks (for instance, setting `pin_l0_filter_and_index_blocks_in_cache` to `true` results in the dictionary meta-block getting pinned as well for L0 files).

The in-memory uncompression dictionary is a digested form of the raw dictionary stored on disk, and is larger in size. The digested form makes uncompression faster, but does consume more memory.