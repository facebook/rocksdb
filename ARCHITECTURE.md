# RocksDB Architecture

RocksDB is a persistent key-value store based on the Log-Structured Merge-tree (LSM-tree). Writes go to an in-memory buffer (MemTable) and a Write-Ahead Log (WAL) for durability. When the MemTable is full, it is flushed to sorted files on disk (SST files). Background compaction merges SST files across levels to reclaim space and maintain read performance. Reads check MemTable first, then SST files from newest to oldest.

## Core Data Model

```
DBImpl
  +-- ColumnFamilySet (one per DB)
        +-- ColumnFamilyData (one per column family)
              +-- MemTable (active, mutable)
              +-- MemTableList (immutable memtables awaiting flush)
              +-- SuperVersion = MemTable + MemTableList + Version
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
| `tools/` | CLI tools: `db_bench`, `ldb`, `sst_dump`, `db_stress` |
| `memory/` | Arena, ConcurrentArena, jemalloc allocator |

## Internal Key Format

Every key stored internally is an InternalKey: `user_key + sequence_number (7 bytes) + type (1 byte)`. The 8-byte trailer is packed as `(sequence << 8) | type`. Keys are sorted by user_key ascending, then sequence number descending, so the newest version of a key is found first.

Defined in `db/dbformat.h`.

## Component Documentation

Detailed documentation for each subsystem is in [docs/components/](docs/components/README.md):

| Document | Components |
|----------|-----------|
| [write_path.md](docs/components/write_path.md) | WriteBatch, WriteThread, WAL, MemTable, WriteController |
| [version_management.md](docs/components/version_management.md) | VersionEdit, VersionSet, Version, VersionBuilder, MANIFEST, ColumnFamily |
| [sst_table_format.md](docs/components/sst_table_format.md) | BlockBasedTable, Block encoding, Filters, Index, TableCache |
| [compaction.md](docs/components/compaction.md) | CompactionPicker, CompactionJob, CompactionIterator, MergingIterator |
| [flush_and_read_path.md](docs/components/flush_and_read_path.md) | FlushJob, DBIter, Get/MultiGet, RangeDelAggregator |
| [cache.md](docs/components/cache.md) | LRUCache, HyperClockCache, SecondaryCache, CacheReservation |
| [file_io_and_blob.md](docs/components/file_io_and_blob.md) | Env, FileSystem, WritableFileWriter, RateLimiter, BlobDB |
| [db_impl.md](docs/components/db_impl.md) | DBImpl, DB Open, Background ops, ErrorHandler, Snapshots |
