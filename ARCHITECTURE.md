# RocksDB Architecture

RocksDB is a persistent key-value store based on the Log-Structured Merge-tree (LSM-tree). Writes go to an in-memory buffer (MemTable) and a Write-Ahead Log (WAL) for durability. When the MemTable is full, it is flushed to sorted files on disk (SST files). Background compaction merges SST files across levels to reclaim space and maintain read performance. Reads check MemTable first, then SST files from newest to oldest.

## Core Data Model

```
DBImpl
  +-- ColumnFamilySet (one per DB)
        +-- ColumnFamilyData (one per column family)
              +-- MemTable (active, mutable)
              +-- MemTableList (immutable memtables awaiting flush)
              +-- SuperVersion = ReadOnlyMemTable* + MemTableListVersion* + Version*
              +-- Version (set of SST/blob files per level)

VersionSet (one per DB)
  +-- MANIFEST (log of VersionEdits describing file additions/deletions)
  +-- Versions (ref-counted, linked list per column family)
```

**SuperVersion** is the key concurrency primitive: readers acquire a ref-counted SuperVersion to get a point-in-time consistent view of memtables + SST files without holding the DB mutex.

## Key Data Flows

### Write Path
```
Put/Delete/Merge -> WriteBatch -> WriteThread (leader election)
  -> WAL (leader writes group) -> assign sequence numbers -> MemTable insert
```
See [docs/components/write_path.md](docs/components/write_path.md)

### Flush
```
MemTable full -> schedule flush -> FlushJob -> build SST file
  -> VersionEdit (add L0 file) -> LogAndApply to MANIFEST -> install new Version
```
See [docs/components/flush_and_read_path.md](docs/components/flush_and_read_path.md)

### Compaction
```
Compaction trigger (score/size) -> CompactionPicker selects files
  -> CompactionJob reads + merges via CompactionIterator
  -> writes new SST files -> VersionEdit -> LogAndApply
```
See [docs/components/compaction.md](docs/components/compaction.md)

### Read Path (Point Lookup)
```
Get(key) -> acquire SuperVersion -> check MemTable -> check immutable MemTables
  -> check L0 files (all, newest first) -> check L1+ (binary search per level)
  -> release SuperVersion
```

### Read Path (Iterator)
```
NewIterator -> snapshot SuperVersion -> MergingIterator over:
  [MemTable iter, ImmutableMemTable iters, L0 file iters, per-level concat iters]
  -> DBIter wraps MergingIterator (resolves merges, deletions, range tombstones)
```
See [docs/components/flush_and_read_path.md](docs/components/flush_and_read_path.md)

## Critical Invariants

| Invariant | Why It Matters |
|-----------|----------------|
| WAL written before MemTable | Crash recovery can replay WAL to restore MemTable state |
| Sequence numbers are monotonically increasing | Ensures snapshot isolation and correct merge ordering |
| L0 flush commit order matches MemTable FIFO order | Prevents data loss: older data cannot overwrite newer data |
| Compaction inputs/outputs are tracked in `pending_outputs_` | Prevents file deletion while background jobs use them |
| `mutex_` acquired before `wal_write_mutex_` | Avoids deadlock between write path and background threads |
| `LogAndApply` is serialized per column family | MANIFEST consistency; one writer at a time |
| Older Versions kept alive while referenced | Live iterators/snapshots see consistent view of files |

## Source Directory Map

| Directory | What Lives Here |
|-----------|----------------|
| `include/rocksdb/` | Public API: `db.h`, `options.h`, `cache.h`, `env.h`, `table.h`, `status.h`, `write_batch.h` |
| `db/` | Core engine: versioning, WAL, memtable, write path, flush, recovery, snapshots |
| `db/db_impl/` | `DBImpl` class split across `db_impl_write.cc`, `db_impl_open.cc`, `db_impl_compaction_flush.cc`, etc. |
| `db/compaction/` | Compaction picker (Level/Universal/FIFO), compaction job, compaction iterator |
| `db/blob/` | Integrated BlobDB: blob file builder/reader, blob index, blob source |
| `table/` | SST file formats: block-based table (dominant), plain table, cuckoo table |
| `table/block_based/` | Block builder/reader, filter blocks, index structures, block cache integration |
| `cache/` | LRUCache, HyperClockCache, secondary cache, compressed cache, cache reservation |
| `file/` | File I/O wrappers: `WritableFileWriter`, `RandomAccessFileReader`, `FilePrefetchBuffer` |
| `memtable/` | MemTable representations: skiplist (default), hash-skiplist, hash-linklist, vector |
| `util/` | Core utilities: coding (varint), CRC32c, xxhash, bloom, arena, rate limiter |
| `utilities/` | Optional features: transactions, backup, checkpoint, merge operators |
| `env/` | Environment/FileSystem implementations: posix, mock, composite |
| `monitoring/` | Statistics, histograms, perf context, instrumented mutex |
| `options/` | Internal option representations, parsing, serialization |
| `port/` | Platform portability: mutexes, atomics, endianness |
| `tools/` | CLI tools: `db_bench`, `ldb`, `sst_dump` |
| `db_stress_tool/` | Crash/stress testing framework |
| `memory/` | Arena, ConcurrentArena, jemalloc allocator |

## Internal Key Format

Every key stored internally is an InternalKey: `user_key + sequence_number (7 bytes) + type (1 byte)`. The 8-byte trailer is packed as `(sequence << 8) | type`. Keys are sorted by user_key ascending, then sequence number descending, so the newest version of a key is found first.

Defined in `db/dbformat.h`.

## Documentation

Detailed documentation is in [docs/components/](docs/components/README.md), organized in two layers:

### End-to-End Flow Docs
| Document | Coverage |
|----------|----------|
| [write_flow.md](docs/components/write_flow.md) | Put/Delete/Merge → WAL → MemTable → Flush → Compaction → Flow control |
| [read_flow.md](docs/components/read_flow.md) | Get/MultiGet/Iterator → SuperVersion → MemTable → Cache → SST files |

### Component Deep-Dives
| Document | Coverage |
|----------|----------|
| [memtable.md](docs/components/memtable.md) | SkipList, concurrent insert, arena, flush triggers |
| [wal.md](docs/components/wal.md) | WAL format, sync modes, recycling, lifecycle |
| [flush.md](docs/components/flush.md) | FlushJob, atomic flush, pipelined flush |
| [compaction.md](docs/components/compaction.md) | CompactionPicker, CompactionJob, CompactionIterator |
| [sst_table_format.md](docs/components/sst_table_format.md) | BlockBasedTable, block encoding, filters, index |
| [version_management.md](docs/components/version_management.md) | VersionSet, Version, MANIFEST, ColumnFamily |
| [iterator.md](docs/components/iterator.md) | Block/Level/Merging/DB/Compaction iterators |
| [file_io.md](docs/components/file_io.md) | Env, FileSystem, WritableFileWriter, RateLimiter |
| [blob_db.md](docs/components/blob_db.md) | BlobDB, blob file format, blob GC |
| [cache.md](docs/components/cache.md) | LRUCache, HyperClockCache, block cache |
| [secondary_cache.md](docs/components/secondary_cache.md) | Secondary cache, row cache |
| [compression.md](docs/components/compression.md) | Snappy/LZ4/ZSTD, dictionary compression |
| [filter.md](docs/components/filter.md) | Bloom, Ribbon, partitioned filters |
| [transaction.md](docs/components/transaction.md) | Pessimistic/Optimistic/WritePrepared, 2PC |
| [wide_column.md](docs/components/wide_column.md) | WideColumn API, PutEntity/GetEntity |
| [user_defined_timestamp.md](docs/components/user_defined_timestamp.md) | UDT encoding, timestamp-aware compaction |
| [user_defined_index.md](docs/components/user_defined_index.md) | UDI interface, index-based lookups |
| [snapshot.md](docs/components/snapshot.md) | Snapshot implementation, sequence numbers |
| [db_impl.md](docs/components/db_impl.md) | DBImpl, Open/Close, background ops, ErrorHandler |
| [threading_model.md](docs/components/threading_model.md) | Thread pools, WriteThread, mutex hierarchy |
| [options.md](docs/components/options.md) | Options hierarchy, mutable/immutable, validation |
| [crash_recovery.md](docs/components/crash_recovery.md) | MANIFEST recovery, WAL replay |
| [data_integrity.md](docs/components/data_integrity.md) | Checksums, paranoid checks, handoff |
| [monitoring.md](docs/components/monitoring.md) | Statistics, PerfContext, IOStatsContext |
| [listener.md](docs/components/listener.md) | EventListener callbacks |
| [tiered_storage.md](docs/components/tiered_storage.md) | Data temperature, tiered compaction |
| [secondary_instance.md](docs/components/secondary_instance.md) | Secondary reader, remote compaction |
| [checkpoint.md](docs/components/checkpoint.md) | Checkpoint, BackupEngine |
| [public_api_read.md](docs/components/public_api_read.md) | Get, MultiGet, iterators, async I/O |
| [public_api_write.md](docs/components/public_api_write.md) | Put, Delete, Merge, WriteBatch |
| [debugging_tools.md](docs/components/debugging_tools.md) | ldb, sst_dump, RepairDB |
| [stress_test.md](docs/components/stress_test.md) | db_stress, crash test, fault injection |
| [db_bench.md](docs/components/db_bench.md) | Benchmarking tool, methodology |
