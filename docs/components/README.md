# RocksDB Component Documentation

AI-context documentation for RocksDB internals. Each document covers a major subsystem, describing what it does, how it works, key invariants, and interactions with other components.

Start with [ARCHITECTURE.md](../../ARCHITECTURE.md) for the high-level overview.

## Documents

| File | Components | Description |
|------|-----------|-------------|
| [write_path.md](write_path.md) | WriteBatch, WriteThread, WAL, MemTable, WriteController, WriteBufferManager | How writes flow from user API to durable storage |
| [version_management.md](version_management.md) | VersionEdit, VersionSet, Version, VersionBuilder, MANIFEST, ColumnFamily | How RocksDB tracks which files exist and manages metadata changes |
| [sst_table_format.md](sst_table_format.md) | BlockBasedTable, Block encoding, Filters, Index, TableCache | SST file on-disk format, reading, writing, and caching of table readers |
| [compaction.md](compaction.md) | CompactionPicker, CompactionJob, CompactionIterator, MergingIterator | Background merging of SST files across levels |
| [flush_and_read_path.md](flush_and_read_path.md) | FlushJob, DBIter, Get/MultiGet, ForwardIterator, RangeDelAggregator | MemTable flush to L0 and all read operations |
| [cache.md](cache.md) | LRUCache, HyperClockCache, SecondaryCache, CompressedSecondaryCache, CacheReservation | Block cache implementations and memory management |
| [file_io_and_blob.md](file_io_and_blob.md) | Env, FileSystem, WritableFileWriter, RandomAccessFileReader, RateLimiter, BlobDB | File I/O abstractions and blob storage |
| [db_impl.md](db_impl.md) | DBImpl, DB Open, Background ops, ErrorHandler, InternalStats, Snapshots | Top-level database implementation and lifecycle |
