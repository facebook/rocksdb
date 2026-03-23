# Block Cache Tracing

**Files:** `trace_replay/block_cache_tracer.h`, `include/rocksdb/block_cache_trace_writer.h`, `include/rocksdb/trace_record.h` (block cache trace types), `table/block_based/block_based_table_reader.cc`, `tools/block_cache_analyzer/block_cache_trace_analyzer.h`

## Overview

Block cache tracing captures every block cache access (hit or miss) with detailed context about the caller, block type, and referenced key. This enables analysis of cache efficiency, access patterns, and the impact of cache configuration changes without requiring live profiling.

## Tracing Lifecycle

Step 1: Create a `TraceWriter` via `NewFileTraceWriter()`.

Step 2: Call `DB::StartBlockCacheTrace()` with `TraceOptions` (or `BlockCacheTraceOptions`) and the writer.

Step 3: All block cache lookups are recorded with access context.

Step 4: Call `DB::EndBlockCacheTrace()` to stop recording.

## Block Types Traced

Block cache trace records are differentiated by `TraceType` (see `include/rocksdb/trace_record.h`):

| TraceType | Description |
|-----------|-------------|
| `kBlockTraceIndexBlock` | Index block access |
| `kBlockTraceFilterBlock` | Filter block access (all filter types) |
| `kBlockTraceDataBlock` | Data block access |
| `kBlockTraceUncompressionDictBlock` | Compression dictionary block access |
| `kBlockTraceRangeDeletionBlock` | Range deletion block access |

## BlockCacheLookupContext

The `BlockCacheLookupContext` struct (see `trace_replay/block_cache_tracer.h`) propagates tracing context through block cache lookups. It is created at five instrumentation points:

1. `BlockBasedTable::GetFilter` -- filter block access
2. `BlockBasedTable::GetUncompressedDict` -- compression dictionary access
3. `BlockBasedTable::MaybeReadAndLoadToCache` -- data, index, and range deletion block access
4. `BlockBasedTable::Get` -- records the referenced key and whether it was found (for `kUserGet` callers)
5. `BlockBasedTable::MultiGet` -- same as Get but for multi-key lookups

The context is created by the caller (Get, MultiGet, NewIterator, Open, ApproximateOffsetOf) and carries the `TableReaderCaller` enum to identify the access source (user read vs. compaction vs. prefetch).

Context fields include:

| Field | Description |
|-------|-------------|
| `caller` | Who triggered the access (e.g., `kUserGet`, `kCompaction`, `kPrefetch`) |
| `is_cache_hit` | Whether the block was found in cache |
| `no_insert` | Whether the block was read with `no_cache` option |
| `block_type` | Type of block accessed |
| `block_size` | Size of the block in bytes |
| `block_key` | Cache key for the block |
| `num_keys_in_block` | Number of keys in the block (data blocks only) |
| `get_id` | Unique ID linking multiple block accesses to a single Get/MultiGet request |
| `referenced_key` | The user key being looked up (Get/MultiGet only) |

## BlockCacheTraceHelper

The `BlockCacheTraceHelper` class (see `trace_replay/block_cache_tracer.h`) provides utility methods for trace analysis:

- `IsGetOrMultiGetOnDataBlock()` -- identifies data block accesses from point lookups
- `IsUserAccess()` -- distinguishes user-initiated accesses from internal operations (compaction, prefetch)
- `ComputeRowKey()` -- creates a unique key by concatenating file descriptor number and user key
- `GetTableId()` / `GetSequenceNumber()` -- extracts metadata from the referenced key
- `GetBlockOffsetInFile()` -- extracts block offset from the block key

## Analysis Use Cases

Block cache traces enable answering questions such as:

- What is the cache hit rate per block type (data vs. index vs. filter)?
- Which SST files or key ranges are hot?
- How many block accesses does a typical Get or Seek require?
- What fraction of block cache space is used by index/filter vs. data blocks?
- Would a larger cache meaningfully reduce I/O?
- Are there redundant block accesses that could be eliminated?

The `get_id` field is particularly useful for correlating multiple block accesses to a single user request, enabling per-request block access analysis.
