## Introduction
Direct I/O is a system-wide feature that supports direct reads/writes from/to storage device to/from user memory space bypassing system page cache. Buffered I/O is usually the default I/O mode enabled by most operating systems.

### Why do we need it?
With buffered I/O, the data is copied twice between storage and memory because of the page cache as the proxy between the two. In most cases, the introduction of page cache could achieve better performance. But for self-caching applications such as RocksDB, the application itself should have a better knowledge of the logical semantics of the data than OS, which provides a chance that the applications could implement more efficient replacement algorithm for cache with any application-defined data block as a unit by leveraging their knowledge of data semantics. On the other hand, in some situations, we want some data to opt-out of system cache. At this time, direct I/O would be a better choice.

### Implementation
The way to enable direct I/O depends on the operating system and the support of direct I/O depends on the file system. Before using this feature, please check whether the file system supports direct I/O. RocksDB has dealt with these OS-dependent complications for you, but we are glad to share some implementation details here.

1. File Open

   For LINUX, the `O_DIRECT` flag has to be included.
For Mac OSX, `O_DIRECT` is not available. Instead, `fcntl(fd, F_NOCACHE, 1)` looks to be the canonical solution where `fd` is the file descriptor of the file.
For Windows, there is a flag called `FILE_FLAG_NO_BUFFERING` as the counterpart in Windows of `O_DIRECT`.

2. File R/W

   Direct I/O requires file R/W to be aligned, which means, the position indicator (offset), #bytes and the buffer address must be aligned to the _logical sector size_ of the underlying storage device. So the position indicator should and the buffer pointer must be aligned on a _logical sector size_ boundary and the number of bytes to be read or written must be in multiples of the _logical sector size_.
RocksDB implements all the alignment logic inside `FileReader/FileWriter`, one layer higher abstraction on top of File classes to make the alignment ignorant to the OS. Thus, different OSs could have their own implementations of File Classes.

## API
It is easy to use Direct I/O as two new options are provided in [options.h](https://github.com/facebook/rocksdb/blob/79b6ab43ce495cb6cd922fff80462597916dcda6/include/rocksdb/options.h#L645-L662):
```cpp
  // Enable direct I/O mode for read/write
  // they may or may not improve performance depending on the use case
  //
  // Files will be opened in "direct I/O" mode
  // which means that data r/w from the disk will not be cached or
  // buffered. The hardware buffer of the devices may however still
  // be used. Memory mapped files are not impacted by these parameters.

  // Use O_DIRECT for user and compaction reads.
  // When true, we also force new_table_reader_for_compaction_inputs to true.
  // Default: false
  // Not supported in ROCKSDB_LITE mode!
  bool use_direct_reads = false;

  // Use O_DIRECT for writes in background flush and compactions.
  // Default: false
  // Not supported in ROCKSDB_LITE mode!
  bool use_direct_io_for_flush_and_compaction = false;
```
The code is self-explanatory.

You may also need other options to optimize direct I/O performance.
```cpp
// options.h
// Option to enable readahead in compaction
// If not set, it will be set to 2MB internally
size_t compaction_readahead_size = 2 * 1024 * 1024; // recommend at least 2MB
// Option to tune write buffer for direct writes
size_t writable_file_max_buffer_size = 1024 * 1024; // 1MB by default
```
```cpp
// DEPRECATED!
// table.h
// If true, block will not be explicitly flushed to disk during building
// a SstTable. Instead, buffer in WritableFileWriter will take
// care of the flushing when it is full.
// This option is deprecated and always be true
bbto.skip_table_builder_flush = true;
```
Recent releases have these options automatically set if direct I/O is enabled.

### Notes 
1.  `allow_mmap_reads` cannot be used with `use_direct_reads` or `use_direct_io_for_flush_and_compaction`. `allow_mmap_writes` cannot be used with `use_direct_io_for_flush_and_compaction`, i.e., they cannot be set to true at the same time.
2.  `use_direct_io_for_flush_and_compaction` and `use_direct_reads` will only be applied to SST file I/O but not WAL I/O or MANIFEST I/O. Direct I/O for WAL and Manifest files is not supported yet.
3. After enable direct I/O, compaction writes will no longer be in the OS page cache, so first read will do real IO. Some users may know RocksDB has a feature called compressed block cache which is supposed to be able to replace page cache with direct I/O enabled. But please read the following comments before enable it:
  * Fragmentation. RocksDB's compressed block is not aligned to page size. A compressed block resides in malloc'ed memory in RocksDB's compressed block cache. It usually means a fragmentation in memory usage. OS page cache does slightly better, since it caches the whole physical page. If some continuous blocks are all hot, OS page cache uses less memory to cache them.
  * OS page cache provides read ahead. By default this is turned off in RocksDB but users can choose turn it on. This is going to be useful in range-loop dominated workloads. RocksDB compressed cache doesn't have anything to match the functionality.
  * Possible bugs. The RocksDB compressed block cache code has never been used before. We did see external users reporting bugs to it, but we never took more steps improve this component.
4. [Automatic Readahead](https://github.com/facebook/rocksdb/wiki/Iterator#read-ahead) is enabled for Iterators in Direct IO mode as well. With this, long-range and full-table scans benchmarks in Sysbench (via a MyRocks build) match that of the Buffered IO mode.
5. It is advisable to turn on mid-point insertion strategy for the Block Cache if your workload is a mix of point and range queries, by setting `LRUCacheOptions.high_pri_pool_ratio = 0.5`. (Note that this depends on `BlockBasedTableOptions.cache_index_and_filter_blocks` and `cache_index_and_filter_blocks_with_high_priority` as well).