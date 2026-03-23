## What is compressed?
In each SST file, data blocks and index blocks can be compressed individually. Users can specify what compression types to use. Filter blocks are not compressed.

## Configuration
Compression configuration is per column family.

Use `options.compression` to specify the compression to use. By default it is Snappy. We believe LZ4 is almost always better than Snappy. We leave Snappy as default to avoid unexpected compatibility problems to previous users. LZ4/Snappy is a lightweight compression algorithm so it usually strikes a good balance between space and CPU usage.

If you want to further reduce space and have some free CPU to use, you can try to set a heavy-weight compression by setting `options.bottommost_compression`. The bottommost level will be compressed using this compression style. Usually the bottommost level contains majority of the data, so users get an almost optimal space setting, without paying CPU for compressing all the data at any level. We recommend ZSTD. If it is not available, Zlib is the second choice.

If you have a lot of free CPU and want to reduce not just space but write amplification too, try to set `options.compression` to heavy weight compression type. We recommend ZSTD. Use Zlib if it is not available.

With a countermanded legacy setting `options.compression_per_level`, you can have an even finer control of compression style of each level. When this option is used, `options.compression` is ignored, while `options.bottommost_compression` still applies. But we believe there are very few use cases where this tuning will help.

Be aware that when you set different compression to different levels, compaction "trivial moves" that violate the compression styles will not be executed, and the file will be rewrite using the expected compression.

The specified compression type always applies to both of index and data blocks. You can disable compression for index blocks by setting `BlockBasedTableOptions.enable_index_compression = false`.

## Compression level and window size setting
Some compression types support different compression level and window setting. You can set them through `options.compression_opts`. If the compression type doesn't support these setting, it will be a no-op.

## Dictionary Compression
Users can choose to compress each SST file of their bottommost level with a dictionary stored in the file. In some use cases, this can save some space. See [[Dictionary Compression]].

## Compression Library
If you pick a compression type but the library for it is not available, RocksDB will fall back to no compression. RocksDB will print out availability of compression types in the header of log files like this:
```
2017/12/01-17:34:59.368239 7f768b5d0200 Compression algorithms supported:
2017/12/01-17:34:59.368240 7f768b5d0200         Snappy supported: 1
2017/12/01-17:34:59.368241 7f768b5d0200         Zlib supported: 1
2017/12/01-17:34:59.368242 7f768b5d0200         Bzip supported: 0
2017/12/01-17:34:59.368243 7f768b5d0200         LZ4 supported: 1
2017/12/01-17:34:59.368244 7f768b5d0200         ZSTDNotFinal supported: 1
2017/12/01-17:34:59.368282 7f768b5d0200         ZSTD supported: 1
```
Check the logging for potential compilation problems.
