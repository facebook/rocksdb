# Review: debugging_tools -- Claude Code

## Summary
Overall quality rating: **good**

The documentation provides solid coverage of RocksDB's debugging and diagnostic toolkit across 12 chapters. The structure is clean, the index is within spec (42 lines), and there are no box-drawing characters or line number references. The strongest chapters are the tracing chapters (3-5), PerfContext (6), and Statistics (7), which provide accurate and actionable information. The weakest areas are: a factually wrong claim about PerfLevel defaults (Chapter 6), wrong class name for FileSystemTracingWrapper (Chapter 5), 10 undocumented ldb subcommands (Chapter 1), a missing OperationType in Thread Status (Chapter 10), and imprecise temperature tier descriptions in IOStatsContext. The workflows chapter (12) is practical and well-organized but references a few properties without verifying they work as described.

## Correctness Issues

### [WRONG] PerfLevel default described as build-mode dependent
- **File:** 06_perf_context.md, "PerfLevel Control" section
- **Claim:** "The default level depends on the build mode: INFO_LEVEL in release, DEBUG_LEVEL in debug."
- **Reality:** The default PerfLevel is `kEnableCount` unconditionally. The claim conflates PerfLevel with InfoLogLevel (the Logger default). PerfLevel is initialized to `kEnableCount` via `thread_local PerfLevel perf_level = kEnableCount;` in `monitoring/perf_level.cc:13`.
- **Source:** `monitoring/perf_level.cc` line 13
- **Fix:** Replace with: "The default level is `kEnableCount` for all build modes."

### [WRONG] Run() dispatch flow description
- **File:** 01_ldb_tool.md, "Architecture and Command Dispatch" section
- **Claim:** "Step 4: `Run()` calls `PrepareOptions()` to set defaults, `OverrideBaseOptions()` to apply user-specified options, `OpenDB()` to open the database, `DoCommand()` to execute the command logic, and `CloseDB()` to close the database."
- **Reality:** `Run()` calls `OpenDB()` and then `DoCommand()`. `PrepareOptions()` and `OverrideBaseOptions()` are called internally by `OpenDB()`, not directly by `Run()`. The doc implies a flat sequence of 5 calls, but the actual hierarchy is: `Run()` -> `OpenDB()` -> `PrepareOptions()` -> `OverrideBaseOptions()`.
- **Source:** `tools/ldb_cmd.cc` lines 442-477 (Run), 563-564 (OpenDB calls PrepareOptions), 1256 (PrepareOptions calls OverrideBaseOptions)
- **Fix:** "Step 4: `Run()` calls `OpenDB()` (which internally calls `PrepareOptions()` and `OverrideBaseOptions()`), then `DoCommand()`, then `CloseDB()`."

### [MISLEADING] IOStatsContext temperature tiers omit split "unknown"
- **File:** 06_perf_context.md, "IO by Temperature" section
- **Claim:** "tracks read bytes and read counts per file temperature tier (`hot`, `warm`, `cool`, `cold`, `ice`, `unknown`)"
- **Reality:** The "unknown" tier is split into two: `unknown_non_last_level` and `unknown_last_level`. This distinction matters for tiered storage debugging.
- **Source:** `include/rocksdb/iostats_context.h` lines 42-44, 56-58 (FileIOByTemperature struct)
- **Fix:** Replace `unknown` with `unknown_non_last_level`, `unknown_last_level` and explain the split.

### [MISLEADING] StatsLevel kDisableAll / kExceptTickers presentation
- **File:** 07_statistics.md, "StatsLevel Control" section
- **Claim:** "`kDisableAll` / `kExceptTickers` | Disable all metrics"
- **Reality:** `kExceptTickers = kDisableAll` -- they are aliases for the same value. The presentation with a slash suggests they are two separate levels with the same effect, when in fact they are one level with two names. The name `kExceptTickers` is misleading on its own (it sounds like "all except tickers"), but since it equals `kDisableAll`, both disable everything.
- **Source:** `include/rocksdb/statistics.h` lines 762-765
- **Fix:** Clarify: "`kDisableAll` (also named `kExceptTickers`) | Disable all metrics"

### [WRONG] FileSystemTracer class name is incorrect
- **File:** 05_io_tracing.md, "FileSystem Integration" section
- **Claim:** "wrapping the `FileSystem` with a tracing layer (`FileSystemTracer`)"
- **Reality:** The actual class is `FileSystemTracingWrapper` (in `env/file_system_tracer.h`), not `FileSystemTracer`. The file name `file_system_tracer.h` likely caused the confusion, but no class named `FileSystemTracer` exists.
- **Source:** `env/file_system_tracer.h` line 20 (class FileSystemTracingWrapper)
- **Fix:** Replace `FileSystemTracer` with `FileSystemTracingWrapper`

### [MISLEADING] Block cache tracing lifecycle oversimplifies API
- **File:** 04_block_cache_tracing.md, "Tracing Lifecycle" section
- **Claim:** "Call `DB::StartBlockCacheTrace()` with `TraceOptions` (or `BlockCacheTraceOptions`) and the writer."
- **Reality:** The API signature takes `BlockCacheTraceOptions` (with `sampling_frequency`) AND a `BlockCacheTraceWriter` (not a raw `TraceWriter`). The `max_trace_file_size` is in `BlockCacheTraceWriterOptions`, not in `BlockCacheTraceOptions`. The doc conflates these structures.
- **Source:** `include/rocksdb/block_cache_trace_writer.h` lines 113-122, `include/rocksdb/db.h` line 2181
- **Fix:** Describe the actual API: `DB::StartBlockCacheTrace(BlockCacheTraceOptions, unique_ptr<BlockCacheTraceWriter>)`. Note that `BlockCacheTraceWriterOptions` (with `max_trace_file_size`) is used when constructing the writer.

### [STALE] LDBTool::Run() is deprecated
- **File:** 01_ldb_tool.md, "Embedding ldb in Applications" section
- **Claim:** "embedded in applications via `LDBTool::Run()`"
- **Reality:** `LDBTool::Run()` is marked DEPRECATED. The preferred API is `LDBTool::RunAndReturn()` which returns an int status code.
- **Source:** `include/rocksdb/ldb_tool.h` lines 38 (deprecated Run) and 43 (RunAndReturn)
- **Fix:** Mention `RunAndReturn()` as the preferred API and note that `Run()` is deprecated.

### [MISLEADING] SingleDelete constraint description oversimplified
- **File:** 01_ldb_tool.md, "delete / singledelete / deleterange" section
- **Claim:** "requires the key was written exactly once without overwrites"
- **Reality:** The actual constraint is "exactly one Put since the previous SingleDelete for this key" -- not "written exactly once" in absolute terms. A key can have had multiple Puts in its lifetime as long as there was a SingleDelete between them.
- **Source:** `include/rocksdb/db.h` lines 479-482 (DB::SingleDelete documentation)
- **Fix:** Replace with: "requires exactly one Put for this key since the previous call to SingleDelete for the same key"

## Completeness Gaps

### 10 ldb subcommands not documented
- **Why it matters:** Users looking for a command in the docs won't find it, may not know it exists
- **Where to look:** `tools/ldb_cmd.cc` SelectCommand(), corresponding command classes in `tools/ldb_cmd_impl.h`
- **Missing commands:**
  1. `approxsize` (ApproxSizeCommand) -- estimates key range size
  2. `query` (DBQuerierCommand) -- interactive query mode
  3. `dump` (DBDumperCommand) -- dump database contents (different from db_dump)
  4. `load` (DBLoaderCommand) -- load data into database
  5. `idump` (InternalDumpCommand) -- dump with internal key format
  6. `create_column_family` (CreateColumnFamilyCommand) -- create CF
  7. `drop_column_family` (DropColumnFamilyCommand) -- drop CF
  8. `list_file_range_deletes` (ListFileRangeDeletesCommand) -- list range deletes per SST
  9. `update_manifest` (UpdateManifestCommand) -- update MANIFEST metadata
  10. `dump_compaction_progress` (CompactionProgressDumpCommand) -- show compaction progress
- **Suggested scope:** Add brief descriptions to the existing sections in Chapter 1 (data query, metadata inspection, or manipulation as appropriate)

### Missing OperationType: OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST
- **Why it matters:** Someone looking at GetThreadList output may see this operation type and not understand it
- **Where to look:** `include/rocksdb/thread_status.h` line 59
- **Suggested scope:** Add one row to the OperationType table in Chapter 10

### Block cache trace analyzer tool not documented
- **Why it matters:** The `block_cache_trace_analyzer` is a significant offline analysis tool that the docs reference only via header path in the Files line but never describe. It provides miss ratio curve simulation, reuse distance analysis, per-caller access timeline analysis, cache policy simulation (LRU, LFU, HyperClockCache), and Python plotting tools.
- **Where to look:** `tools/block_cache_analyzer/block_cache_trace_analyzer.h` and `.cc`, `tools/block_cache_analyzer/block_cache_trace_analyzer_plot.py`
- **Suggested scope:** Add a section in Chapter 4 describing the analyzer's capabilities, similar to how Chapter 3 describes `trace_analyzer`

### New PerfContext counters (2024-2026) not documented
- **Why it matters:** Recently added metrics for per-block-type read bytes, block cache bytes read, blob metrics, and file ingestion timing are not mentioned
- **Where to look:** PR #14473 (per-block-type block read byte counters: `data_block_read_byte`, `index_block_read_byte`, `filter_block_read_byte`, etc.), PR #13219 (file ingestion timing: `file_ingestion_nanos`, `file_ingestion_blocking_live_writes_nanos`), PR #12459 (block cache bytes read: `block_cache_index_read_byte`, `block_cache_filter_read_byte`, etc.)
- **Suggested scope:** Add to the "Block Cache and Read Metrics" section in Chapter 6

### Many PerfContext fields not mentioned at all
- **Why it matters:** Blob-related metrics (`blob_cache_hit_count`, `blob_read_count`, `blob_read_byte`, `blob_read_time`, `blob_checksum_time`, `blob_decompress_time`), transaction lock wait metrics (`key_lock_wait_time`, `key_lock_wait_count`), and iterator call counts (`iter_next_count`, `iter_prev_count`, `iter_seek_count`) are completely absent from the docs
- **Where to look:** `include/rocksdb/perf_context.h` (PerfContextBase struct)
- **Suggested scope:** Add new subsections for "Blob Metrics", "Transaction Metrics", and "Iterator Call Counts" in Chapter 6

### Many recent Statistics tickers and histograms not documented
- **Why it matters:** 30+ tickers and 10+ histograms added in 2024-2026 are not mentioned, including MultiScan stats (`MULTISCAN_*`), compaction abort (`COMPACTION_ABORTED`), IODispatcher memory stats (`PREFETCH_MEMORY_*`), corruption retry stats (`FILE_READ_CORRUPTION_RETRY_*`), FIFO compaction breakdown (`FIFO_MAX_SIZE_COMPACTIONS`, etc.), and per-IOActivity file read/write latency histograms
- **Where to look:** `include/rocksdb/statistics.h` -- Tickers and Histograms enums
- **Suggested scope:** Add new categories in Chapter 7 for these ticker/histogram groups

### Many DB Properties not listed
- **Why it matters:** ~20 properties are undocumented including `rocksdb.estimate-num-keys`, `rocksdb.compaction-abort-count` (new 2026), blob cache properties (`rocksdb.blob-cache-capacity/usage/pinned-usage`), `rocksdb.sstables`, `rocksdb.options-statistics`, and memtable entry/delete counts
- **Where to look:** `include/rocksdb/db.h` Properties struct
- **Suggested scope:** Add missing properties to appropriate tables in Chapter 8

### sst_dump new options not documented
- **Why it matters:** 10+ options added in 2025-2026 are missing, including `--list_meta_blocks` (new diagnostic), `--compression_parallel_threads`, `--compression_manager`, dictionary training options, `--show_sequence_number_type`, and standalone file argument support (no longer requires `--file=`)
- **Where to look:** `tools/sst_dump_tool.cc` help text and option parsing
- **Suggested scope:** Update Additional Options table and recompress section in Chapter 2

### EventListener not mentioned in debugging tools docs
- **Why it matters:** The workflow chapter (12) references "EventListener callbacks" in the Tool Selection Guide but the debugging tools docs never explain what EventListener is or how to use it for debugging. This is covered in the separate `listener.md` component doc, but a cross-reference would help.
- **Suggested scope:** Brief mention in Chapter 12 with cross-reference to listener component docs

## Depth Issues

### PerfLevel kUninitialized not mentioned
- **Current:** PerfLevel table starts at kDisable=1
- **Missing:** `kUninitialized = 0` exists as a sentinel value. While users shouldn't set it, they may encounter it when debugging and should know it means "not yet set."
- **Source:** `include/rocksdb/perf_level.h` line 29

### Block cache tracing: BlockCacheTraceWriterOptions vs BlockCacheTraceOptions separation
- **Current:** The doc mentions `BlockCacheTraceOptions` but doesn't explain the separate `BlockCacheTraceWriterOptions`
- **Missing:** `BlockCacheTraceOptions` only has `sampling_frequency`. The `max_trace_file_size` is in `BlockCacheTraceWriterOptions`. Users need to configure both.
- **Source:** `include/rocksdb/block_cache_trace_writer.h` lines 113-122

### RepairDB next_file_number calculation is simplified
- **Current:** "next file number to 1 + max file found"
- **Missing:** `next_file_number_` is also incremented during WAL-to-SST conversion (Phase 2), so the final value may exceed "1 + max file found in initial scan"
- **Source:** `db/repair.cc` lines 319-320 (scan), line 446 (WAL conversion increment)

## Structure and Style Violations

### No violations found
- index.md is 42 lines (within 40-80 range)
- All chapters have **Files:** lines with correct paths
- No line number references found
- No inline code quotes (backtick usage is for option names, which is allowed)
- No box-drawing characters
- INVARIANT used appropriately (only for true correctness invariants in index.md)
- Options referenced with field names and header paths

## Undocumented Complexity

### ldb --txn_write_policy flag undocumented
- **What it is:** PR #14304 (2026-02) added `--txn_write_policy=<0|1|2>` to control TransactionDB write policy (WRITE_COMMITTED, WRITE_PREPARED, WRITE_UNPREPARED) when using `--use_txn`
- **Why it matters:** Required for inspecting databases created with WritePrepared or WriteUnprepared transactions
- **Key source:** `tools/ldb_cmd.cc`, commit `a668dcbe8`
- **Suggested placement:** Add to Chapter 1, Global Options table

### CompactionProgressDumpCommand in ldb
- **What it is:** A recently added ldb subcommand that shows compaction progress information
- **Why it matters:** Useful for monitoring long-running compactions without needing GetThreadList
- **Key source:** `tools/ldb_cmd.cc` line 433 (CompactionProgressDumpCommand), corresponding class in `tools/ldb_cmd_impl.h`
- **Suggested placement:** Add to Chapter 1, "Metadata Inspection Commands" section

### UpdateManifestCommand in ldb
- **What it is:** An ldb subcommand that can update MANIFEST metadata (e.g., file temperatures) without rewriting SST files
- **Why it matters:** Critical for correcting metadata after storage tier changes; avoids full compaction
- **Key source:** `tools/ldb_cmd.cc` line 429, corresponding class in `tools/ldb_cmd_impl.h`
- **Suggested placement:** Add to Chapter 1, "Database Manipulation Commands" section

### Per-block-type read byte perf counters
- **What it is:** Recent addition (PR #14473) adding `block_read_byte_index`, `block_read_byte_filter`, `block_read_byte_data`, etc. to PerfContext
- **Why it matters:** Enables understanding which block types dominate read I/O bandwidth
- **Key source:** `include/rocksdb/perf_context.h` (PerfContextBase struct)
- **Suggested placement:** Add to Chapter 6, "Block Cache and Read Metrics" section

### IOTracer TSAN suppression pattern
- **What it is:** The `is_tracing_enabled()` flag in IOTracer is intentionally non-atomic for performance, with a detailed comment explaining the safety via mutex-protected writer checks. This is a noteworthy concurrency pattern.
- **Why it matters:** Anyone modifying the tracing code needs to understand this deliberate data race pattern
- **Key source:** `trace_replay/io_tracer.h`
- **Suggested placement:** Already partially documented in Chapter 5 (the Note about non-atomic flag), but could benefit from more detail about the TSAN suppression

### Abort background compaction support
- **What it is:** PR #14227 added ability to abort running background compaction jobs
- **Why it matters:** Useful debugging/operational tool when a compaction is taking too long or consuming too many resources
- **Key source:** Background compaction abort logic
- **Suggested placement:** Mention in Chapter 12, "Workflow 6: Analyzing Compaction Behavior" or Thread Status chapter

## Positive Notes

- The 12-chapter organization is logical and comprehensive, progressing from CLI tools to programmatic APIs to workflows
- TraceOptions defaults table (Chapter 3) is accurate and well-formatted, verified against `include/rocksdb/options.h`
- The tracing lifecycle descriptions (Chapters 3-5) all follow a consistent Step 1-4 pattern that accurately reflects the API
- The Tool Selection Guide table in Chapter 12 is genuinely useful for quickly identifying the right debugging approach
- PerfContext metric categorization (Chapter 6) is well-organized by operation type (Get, Write, Iterator)
- Statistics chapter (7) accurately documents all key tickers and histograms with correct categorization
- DB Properties chapter (8) has a thorough and accurate listing of properties verified against `include/rocksdb/db.h`
- The db_dump format description (Chapter 11) is byte-accurate (verified against `tools/dump/db_dump_tool.cc`)
- RepairDB four-phase description is accurate and matches the code flow in `db/repair.cc`
- The debugging workflows (Chapter 12) provide practical, step-by-step guidance that correctly chains multiple tools together
