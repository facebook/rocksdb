# RocksDB Features that are not in LevelDB

We stopped maintaining this page since 2016. New features are not added to the lists.

## Performance

* Multithread compaction
* Multithread memtable inserts
* Reduced DB mutex holding
* Optimized level-based compaction style and universal compaction style
* Prefix bloom filter
* Memtable bloom filter
* Single bloom filter covering the whole SST file
* Write lock optimization
* Improved Iter::Prev() performance
* Fewer comparator calls during SkipList searches
* Allocate memtable memory using huge page.

## Features

* Column Families
* Transactions and WriteBatchWithIndex
* Backup and Checkpoints
* Merge Operators
* Compaction Filters
* RocksDB Java
* Manual Compactions Run in Parallel with Automatic Compactions
* Persistent Cache
* Bulk loading
* Forward Iterators/ Tailing iterator
* Single delete
* Delete files in range
* Pin iterator key/value 

## Alternative Data Structures And Formats

* Plain Table format for memory-only use cases
* Vector-based and hash-based memtable format
* Clock-based cache (coming soon)
* Pluggable information log
* Annotate transaction log write with blob (for replication)

## Tunability

* Rate limiting
* Tunable Slowdown and Stop threshold
* Option to keep all files open
* Option to keep all index and bloom filter blocks in block cache
* Multiple WAL recovery modes
* Fadvise hints for readahead and to avoid caching in OS page cache
* Option to pin indexes and bloom filters of L0 files in memory
* More Compression Types: zlib, lz4, zstd
* Compression Dictionary
* Checksum Type: xxhash
* Different level size multiplier and compression type for each level.

## Manageability

* Statistics
* Thread-local profiling
* More commands in command-line tools
* User-defined table properties
* Event listeners
* More DB Properties
* Dynamic option changes
* Get options from a string or map
* Persistent options to option files
