# RocksDB Component Documentation

Comprehensive documentation for RocksDB internals, targeting AI coding assistants and experienced developers.

## How to Use

- **New to the codebase?** Start with `../ARCHITECTURE.md` for the high-level map, then read the relevant flow doc.
- **Modifying a component?** Read the flow doc first (to understand context), then the component deep-dive.
- **Reviewing a PR?** Check which components are touched and read those docs.

## Layer 1: End-to-End Flow Docs

These trace data through the full system with invariants woven in.

| Document | What It Covers |
|----------|---------------|
| [write_flow.md](write_flow.md) | Put/Delete/Merge → WAL → MemTable → Flush → Compaction → Delete cleanup → Flow control |
| [read_flow.md](read_flow.md) | Get/MultiGet/Iterator → SuperVersion → MemTable → Block cache → SST (L0→Ln) → Bloom → Range deletions |

## Layer 2: Component Deep-Dives

### Core Engine
| Document | What It Covers |
|----------|---------------|
| [memtable.md](memtable.md) | SkipList internals, concurrent insert, arena allocation, flush triggers |
| [wal.md](wal.md) | WAL record format, sync modes, recycling, lifecycle |
| [flush.md](flush.md) | FlushJob, atomic flush, pipelined flush, scheduling |
| [compaction.md](compaction.md) | CompactionPicker, CompactionJob, CompactionIterator, MergingIterator |
| [sst_table_format.md](sst_table_format.md) | BlockBasedTable, block encoding, filters, index, TableCache |
| [version_management.md](version_management.md) | VersionSet, Version, VersionBuilder, MANIFEST, ColumnFamily |
| [iterator.md](iterator.md) | Block/Level/Merging/DB iterators, range deletion, compaction iterator |

### Storage & I/O
| Document | What It Covers |
|----------|---------------|
| [file_io.md](file_io.md) | Env, FileSystem, WritableFileWriter, RateLimiter, FilePrefetchBuffer |
| [blob_db.md](blob_db.md) | BlobDB architecture, blob file format, blob index, blob GC |
| [compression.md](compression.md) | Snappy/LZ4/ZSTD, per-level compression, dictionary compression |
| [tiered_storage.md](tiered_storage.md) | Data temperature, per-level temperature, tiered compaction |

### Caching
| Document | What It Covers |
|----------|---------------|
| [cache.md](cache.md) | LRUCache, HyperClockCache, block cache internals |
| [secondary_cache.md](secondary_cache.md) | Secondary cache tier, compressed secondary cache, row cache |

### Features
| Document | What It Covers |
|----------|---------------|
| [transaction.md](transaction.md) | Pessimistic/Optimistic/WritePrepared/WriteUnprepared transactions, 2PC |
| [wide_column.md](wide_column.md) | WideColumn API, PutEntity/GetEntity, entity encoding |
| [user_defined_timestamp.md](user_defined_timestamp.md) | UDT encoding, comparator integration, timestamp-aware compaction |
| [user_defined_index.md](user_defined_index.md) | UDI interface, index creation, index-based lookups |
| [snapshot.md](snapshot.md) | Snapshot implementation, sequence numbers, compaction interaction |
| [filter.md](filter.md) | Bloom filter, Ribbon filter, partitioned filters, prefix bloom |
| [secondary_instance.md](secondary_instance.md) | Secondary reader, TryCatchUpWithPrimary, remote compaction |

### Public API
| Document | What It Covers |
|----------|---------------|
| [public_api_read.md](public_api_read.md) | Get, MultiGet, iterators, async I/O, ReadOptions |
| [public_api_write.md](public_api_write.md) | Put, Delete, Merge, WriteBatch, WriteOptions, IngestExternalFile |

### Infrastructure
| Document | What It Covers |
|----------|---------------|
| [db_impl.md](db_impl.md) | DBImpl, DB Open/Close, background ops, ErrorHandler |
| [threading_model.md](threading_model.md) | Thread pools, WriteThread, mutex hierarchy, subcompactions |
| [options.md](options.md) | Options hierarchy, mutable vs immutable, serialization, validation |
| [crash_recovery.md](crash_recovery.md) | MANIFEST recovery, WAL replay, WALRecoveryMode, best-efforts recovery |
| [data_integrity.md](data_integrity.md) | Checksums (CRC32c, xxHash), paranoid checks, handoff checksums |
| [monitoring.md](monitoring.md) | Statistics, PerfContext, IOStatsContext, DB properties |
| [listener.md](listener.md) | EventListener callbacks, flush/compaction/error notifications |
| [checkpoint.md](checkpoint.md) | Checkpoint (hard-link snapshots), BackupEngine, ExportColumnFamily |

### Tools
| Document | What It Covers |
|----------|---------------|
| [debugging_tools.md](debugging_tools.md) | ldb, sst_dump, manifest_dump, WAL dump, RepairDB |
| [stress_test.md](stress_test.md) | db_stress, crash test, fault injection, expected state tracking |
| [db_bench.md](db_bench.md) | Benchmarking tool, benchmark types, metrics, methodology |
