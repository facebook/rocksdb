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
| [write_flow/](write_flow/index.md) | Put/Delete/Merge → WAL → MemTable → Flush → Compaction → Delete cleanup → Flow control |
| [read_flow/](read_flow/index.md) | Get/MultiGet/Iterator → SuperVersion → MemTable → Block cache → SST (L0→Ln) → Bloom → Range deletions → Prefetch → Async I/O |

## Layer 2: Component Deep-Dives

### Core Engine
| Document | What It Covers |
|----------|---------------|
| [memtable/](memtable/index.md) | SkipList internals, concurrent insert, arena allocation, bloom filter, flush triggers, range tombstones, data integrity |
| [wal/](wal/index.md) | WAL record format, writer/reader, sync modes, recycling, compression, recovery, lifecycle, 2PC, replication |
| [flush/](flush/index.md) | FlushJob lifecycle, atomic flush, MemPurge, write stalls, scheduling, commit protocol |
| [compaction/](compaction/index.md) | CompactionPicker, CompactionJob, CompactionIterator, leveled/universal/FIFO styles, subcompaction, remote compaction |
| [sst_table_format/](sst_table_format/index.md) | BlockBasedTable, block encoding, filters, index, PlainTable, CuckooTable, table properties |
| [version_management/](version_management/index.md) | VersionSet, Version, VersionBuilder, MANIFEST, ColumnFamily, SuperVersion |
| [iterator/](iterator/index.md) | Iterator hierarchy, DBIter, MergingIterator, block/level iterators, prefix seek, readahead, pinning, range tombstones, multi-CF iterators, MultiScan |

### Storage & I/O
| Document | What It Covers |
|----------|---------------|
| [file_io/](file_io/index.md) | Env, FileSystem, WritableFileWriter, RateLimiter, FilePrefetchBuffer, Direct I/O, Async I/O, IO tagging |
| [blob_db/](blob_db/index.md) | BlobDB architecture, blob file format, blob index, write/read paths, garbage collection, caching, statistics |
| [compression/](compression/index.md) | Snappy/LZ4/ZSTD, per-level compression, dictionary compression, parallel compression |
| [tiered_storage/](tiered_storage/index.md) | Temperature system, per-key placement, seqno-to-time mapping, FIFO temperature migration, TimedPut API |

### Caching
| Document | What It Covers |
|----------|---------------|
| [cache/](cache/index.md) | LRUCache, HyperClockCache, secondary cache, tiered cache, cache reservation, block cache keys, monitoring |
| [secondary_cache/](secondary_cache/index.md) | SecondaryCache interface, CacheWithSecondaryAdapter, compressed secondary cache, tiered cache, admission policies, proportional reservation |

### Features
| Document | What It Covers |
|----------|---------------|
| [transaction/](transaction/index.md) | Pessimistic/Optimistic/WritePrepared/WriteUnprepared transactions, 2PC, lock management, deadlock detection |
| [wide_column/](wide_column/index.md) | WideColumn API, PutEntity/GetEntity, entity serialization, attribute groups, compaction/merge integration |
| [user_defined_timestamp/](user_defined_timestamp/index.md) | UDT encoding, comparator integration, timestamp-aware compaction, persistence modes, recovery, migration |
| [user_defined_index/](user_defined_index/index.md) | UDI framework, trie index (LOUDS-encoded FST), sequence number handling, table integration, configuration |
| [snapshot/](snapshot/index.md) | Snapshot implementation, sequence numbers, compaction interaction, transaction integration |
| [filter/](filter/index.md) | Bloom filter, Ribbon filter, partitioned filters, prefix bloom, memtable bloom, filter caching |
| [secondary_instance/](secondary_instance/index.md) | Secondary reader, TryCatchUpWithPrimary, remote compaction, comparison with ReadOnly/Follower modes |

### Public API
| Document | What It Covers |
|----------|---------------|
| [public_api_read/](public_api_read/index.md) | Get, MultiGet, iterators, PinnableSlice, async I/O, ReadOptions, multi-range/cross-CF scans |
| [public_api_write/](public_api_write/index.md) | Put, Delete, Merge, WriteBatch, SingleDelete, DeleteRange, MergeOperator, IngestExternalFile |

### Infrastructure
| Document | What It Covers |
|----------|---------------|
| [db_impl/](db_impl/index.md) | DBImpl architecture, DB Open/Close, column families, write/read paths, background scheduling, error handling |
| [threading_model/](threading_model/index.md) | Thread pools, WriteThread, group commit, write modes, mutex hierarchy, write stalls, subcompactions, lock-free reads |
| [options/](options/index.md) | Options hierarchy, DBOptions, ColumnFamilyOptions, serialization, Customizable framework, tuning guide |
| [crash_recovery/](crash_recovery/index.md) | MANIFEST recovery, WAL replay, WALRecoveryMode, best-efforts recovery, background error handling, repair |
| [data_integrity/](data_integrity/index.md) | Checksums (CRC32c, xxHash, XXH3), block/WAL/file checksums, per-key protection, handoff checksums, output verification, unique ID, paranoid checks |
| [monitoring/](monitoring/index.md) | Statistics, PerfContext, IOStatsContext, DB properties, compaction stats, event logging, thread status |
| [listener/](listener/index.md) | EventListener callbacks, flush/compaction/error notifications |
| [checkpoint/](checkpoint/index.md) | Checkpoint (hard-link snapshots), BackupEngine, ExportColumnFamily |

### Tools
| Document | What It Covers |
|----------|---------------|
| [debugging_tools/](debugging_tools/index.md) | ldb, sst_dump, tracing (query/block cache/IO), PerfContext, Statistics, DB properties, Logger, thread status, RepairDB |
| [stress_test/](stress_test/index.md) | db_stress, crash test, fault injection, expected state tracking, test modes, parameter randomization |
| [db_bench/](db_bench/index.md) | Benchmarking tool, write/read/mixed benchmarks, metrics, methodology, MixGraph workload modeling |
