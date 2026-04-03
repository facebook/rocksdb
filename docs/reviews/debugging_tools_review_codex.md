# Review: debugging_tools — Codex

## Summary
Overall quality rating: significant issues

The chapter set covers the right surface area, and the index is compressed reasonably well, but too many chapters describe idealized behavior instead of current code. The biggest problems are in ldb command flow, checkconsistency, trace option semantics, repair flow, thread tracking, and several command examples. Multiple chapters also flatten cross-component boundaries in ways that would mislead someone debugging production behavior.

The test suite reinforces many of these mismatches. tools/trace_analyzer_test.cc codifies MultiGet-as-per-key accounting, logging/auto_roll_logger_test.cc codifies db_log_dir-specific log naming, db/db_test.cc codifies idle background thread visibility in GetThreadList(), and tools/ldb_cmd_test.cc shows checkconsistency is just forcing consistency checks during open. These docs need a code-first rewrite, not incremental polishing.

## Correctness Issues

### [WRONG] ldb Run() / OpenDB() control flow is misdescribed
- File: docs/components/debugging_tools/01_ldb_tool.md, Architecture and Command Dispatch
- Claim: "Step 4: Run() calls PrepareOptions() to set defaults, OverrideBaseOptions() to apply user-specified options, OpenDB() to open the database, DoCommand() to execute the command logic, and CloseDB() to close the database."
- Reality: LDBCommand::Run does not call PrepareOptions() or OverrideBaseOptions() directly. OpenDB() calls PrepareOptions() internally. Run() also intentionally continues to DoCommand() after DB-open failure for file-oriented commands, unless try_load_options caused the failure.
- Source: tools/ldb_cmd.cc, LDBCommand::Run; tools/ldb_cmd.cc, LDBCommand::OpenDB
- Fix: Rewrite the step list around the real control flow: Run() conditionally opens the DB, OpenDB() prepares options, and DoCommand() can still run after open failure for subcommands that accept direct file paths.

### [WRONG] manifest_dump does not call the function the chapter names
- File: docs/components/debugging_tools/01_ldb_tool.md, Metadata Inspection Commands / manifest_dump
- Claim: "Uses VersionSet::DumpManifest() internally."
- Reality: ManifestDumpCommand::DoCommand() calls DumpManifestFile(). If --path is omitted, it searches the DB directory, requires exactly one MANIFEST, and fails when multiple descriptor files are present.
- Source: tools/ldb_cmd.cc, ManifestDumpCommand::DoCommand
- Fix: Document DumpManifestFile() and the auto-discovery behavior, including the multiple-MANIFEST failure mode.

### [WRONG] dump_live_files and list_live_files_metadata are conflated
- File: docs/components/debugging_tools/01_ldb_tool.md, Metadata Inspection Commands / dump_live_files / list_live_files_metadata
- Claim: "Show information about live SST and blob files, including file metadata such as level, size, and key range."
- Reality: dump_live_files is DBFileDumperCommand, which prints the current MANIFEST, then dumps SST contents, blob contents, and WAL contents. list_live_files_metadata is DBLiveFilesMetadataDumperCommand, which lists file paths grouped or sorted by column family and level, but does not print key ranges or file sizes.
- Source: tools/ldb_cmd_impl.h, DBFileDumperCommand::Name; tools/ldb_cmd.cc, DBFileDumperCommand::DoCommand; tools/ldb_cmd.cc, DBLiveFilesMetadataDumperCommand::DoCommand
- Fix: Split the two subcommands into separate descriptions and describe their real outputs.

### [WRONG] checkconsistency is described as a full consistency verifier when it is only an open-time check
- File: docs/components/debugging_tools/01_ldb_tool.md, Database Manipulation Commands / checkconsistency
- Claim: "Verifies database consistency without opening for writes. Checks that SST files exist, MANIFEST references are valid, and key ranges are correct."
- Reality: CheckConsistencyCommand::DoCommand() only sets paranoid_checks=true and num_levels=64, then opens the DB. There is no dedicated SST walk, MANIFEST audit, or explicit key-range verification pass in this command.
- Source: tools/ldb_cmd.cc, CheckConsistencyCommand::DoCommand
- Fix: Say that the command forces stricter consistency checks during DB open and reports whether open succeeds.

### [WRONG] change_compaction_style is not a bidirectional style-conversion tool
- File: docs/components/debugging_tools/01_ldb_tool.md, Database Manipulation Commands / change_compaction_style
- Claim: "Changes between leveled and universal compaction styles."
- Reality: ChangeCompactionStyleCommand explicitly rejects universal-to-level conversion. The implemented workflow is level-to-universal only: it disables auto compactions, forces a single-file compaction to L0, and verifies that result.
- Source: tools/ldb_cmd.cc, ChangeCompactionStyleCommand::ChangeCompactionStyleCommand; tools/ldb_cmd.cc, ChangeCompactionStyleCommand::OverrideBaseCFOptions; tools/ldb_cmd.cc, ChangeCompactionStyleCommand::DoCommand
- Fix: Describe the supported direction only and call out the universal-to-level rejection.

### [WRONG] sst_dump check compares against the wrong property count
- File: docs/components/debugging_tools/02_sst_dump_tool.md, Commands / check
- Claim: "Iterates through all entries and verifies that the entry count matches the num_entries value in table properties."
- Reality: When the check is unrestricted and read_num is unlimited, SstFileDumper::ReadSequential() compares the number of records read against num_entries minus num_range_deletions, not raw num_entries.
- Source: table/sst_file_dumper.cc, SstFileDumper::ReadSequential
- Fix: Document the exact validation rule and its dependence on range tombstones and range or read limits.

### [WRONG] IO tracing timestamps and latencies are documented in the wrong units
- File: docs/components/debugging_tools/05_io_tracing.md, IOTraceRecord Format
- Claim: "access_timestamp | Microsecond timestamp of the operation" and "latency | Operation latency in microseconds"
- Reality: FileSystemTracingWrapper records access timestamps with NowNanos() and latencies with ElapsedNanos(). The IO trace file is in nanoseconds.
- Source: env/file_system_tracer.cc, FileSystemTracingWrapper methods; trace_replay/io_tracer.h, IOTraceRecord
- Fix: Change both fields to nanoseconds and make the unit consistent throughout the chapter.

### [WRONG] io_tracer_parser usage is not positional
- File: docs/components/debugging_tools/05_io_tracing.md, io_tracer_parser Tool
- Claim: "Usage: io_tracer_parser <trace_file>"
- Reality: The tool is gflags-based and requires --io_trace_file=<path>. It exits with an error if that flag is absent.
- Source: tools/io_tracer_parser_tool.cc, DEFINE_string(io_trace_file, ...); tools/io_tracer_parser_tool.cc, io_tracer_parser
- Fix: Replace the usage example with the flag-based form.

### [WRONG] PerfContext chapter assigns the logger default to PerfLevel
- File: docs/components/debugging_tools/06_perf_context.md, PerfLevel Control
- Claim: "The default level depends on the build mode: INFO_LEVEL in release, DEBUG_LEVEL in debug."
- Reality: The thread-local default PerfLevel is kEnableCount. INFO_LEVEL and DEBUG_LEVEL are InfoLogLevel values for logging, not perf instrumentation levels.
- Source: monitoring/perf_level.cc, perf_level thread-local initialization; include/rocksdb/env.h, Logger::kDefaultLogLevel
- Fix: State that PerfLevel defaults to kEnableCount unless the application changes it per thread.

### [WRONG] IO temperature buckets are missing two real categories
- File: docs/components/debugging_tools/06_perf_context.md, IO by Temperature
- Claim: "The FileIOByTemperature struct within IOStatsContext tracks read bytes and read counts per file temperature tier (hot, warm, cool, cold, ice, unknown)."
- Reality: FileIOByTemperature splits unknown into unknown_non_last_level and unknown_last_level. There is no single combined unknown bucket.
- Source: include/rocksdb/iostats_context.h, FileIOByTemperature
- Fix: List all seven categories exactly as defined.

### [WRONG] The DB properties chapter misclassifies dual-form properties
- File: docs/components/debugging_tools/08_db_properties.md, Key Properties / Compaction and Level Stats and Write Stall Stats
- Claim: "rocksdb.cfstats | map | Column family stats per level"
- Reality: rocksdb.cfstats is a string property and also supports map form. The same dual-form behavior applies to rocksdb.dbstats, rocksdb.block-cache-entry-stats, rocksdb.cf-write-stall-stats, rocksdb.db-write-stall-stats, and rocksdb.aggregated-table-properties.
- Source: include/rocksdb/db.h, DB::Properties comments; db/internal_stats.cc, property table
- Fix: Mark these as string or map where applicable, and explain that GetMapProperty() is not their only access path.

### [WRONG] Rotated info-log names are wrong when db_log_dir is set
- File: docs/components/debugging_tools/09_logger.md, AutoRollLogger
- Claim: "Old log files are named LOG.old.<timestamp> and are retained up to keep_log_file_num files."
- Reality: When db_log_dir is empty, old files are named LOG.old.<timestamp> under the DB directory. When db_log_dir is set, OldInfoLogFileName() uses a DB-path-derived prefix inside db_log_dir, and tests assert that the DB directory name appears in rotated filenames.
- Source: file/filename.cc, OldInfoLogFileName; logging/auto_roll_logger_test.cc, CreateLoggerFromOptions and AutoDeleting tests
- Fix: Document both naming modes separately.

### [WRONG] Disabling thread tracking does not make GetThreadList() empty
- File: docs/components/debugging_tools/10_thread_status.md, Enabling Thread Tracking
- Claim: "When disabled, GetThreadList() returns an empty list."
- Reality: ThreadStatusUpdater::GetThreadList() returns all registered threads in thread_data_set_. Disabling tracking only makes GetLocalThreadStatus() return null for status updates, so operation details are suppressed, but idle registered background threads are still listed.
- Source: monitoring/thread_status_updater.cc, ThreadStatusUpdater::GetThreadList; monitoring/thread_status_updater.cc, ThreadStatusUpdater::GetLocalThreadStatus; db/db_test.cc, GetThreadStatus; db/db_test.cc, ThreadStatusSingleCompaction
- Fix: Say that enable_thread_tracking controls per-DB operation tracking, not thread registration itself.

### [WRONG] RepairDB WAL replay is described as repeated flush-on-full behavior, but the code does per-WAL, per-CF table creation
- File: docs/components/debugging_tools/11_repair_and_recovery.md, Repair Process / Phase 2
- Claim: "Each WAL record is parsed, applied to a MemTable, and flushed to an L0 SST when the memtable fills."
- Reality: ConvertLogToTable() replays an entire WAL into one MemTable per column family, then emits at most one SST per non-empty column family for that WAL. The chapter’s "flush when full" description is not what the repair path does.
- Source: db/repair.cc, Repairer::ConvertLogToTable
- Fix: Describe the actual per-WAL replay flow and the per-column-family table emission.

### [WRONG] The dump and restore binary names and flags do not match the shipped tools
- File: docs/components/debugging_tools/11_repair_and_recovery.md, db_dump and db_undump / Usage
- Claim: "Dump: db_dump --db=/path/to/db --dump_location=/path/to/backup.dump" and "Restore: db_undump --db_path=/path/to/restored --dump_location=/path/to/backup.dump"
- Reality: The binaries are rocksdb_dump and rocksdb_undump. Both use --db_path, not --db. The wrapper sources are separate binaries guarded by GFLAGS.
- Source: tools/dump/rocksdb_dump.cc, main; tools/dump/rocksdb_undump.cc, main
- Fix: Rename the commands and correct the flag spelling in every example.

### [WRONG] The slow-read workflow computes "hit rate" from incompatible PerfContext counters
- File: docs/components/debugging_tools/12_debugging_workflows.md, Workflow 2 / Step 3
- Claim: "Compare block_cache_hit_count vs block_read_count in PerfContext. Low hit rate indicates the working set exceeds cache capacity."
- Reality: block_cache_hit_count counts cache hits, while block_read_count counts block reads from storage. They are not numerator and denominator of a cache-hit rate. A higher hit count can coexist with low block_read_count because many accesses never go to storage.
- Source: include/rocksdb/perf_context.h, PerfContext fields
- Fix: Use block-cache-related pairs that actually share a denominator, or direct users to Statistics tickers and block cache tracing for hit-rate analysis.

### [WRONG] The workflow guide uses an invalid sst_dump command form
- File: docs/components/debugging_tools/12_debugging_workflows.md, Tool Selection Guide
- Claim: "Is this SST file corrupted? | sst_dump --verify | ldb checkconsistency"
- Reality: sst_dump expects --command=verify. There is no bare --verify command switch in tools/sst_dump_tool.cc.
- Source: tools/sst_dump_tool.cc, help text and command parsing
- Fix: Replace the example with sst_dump --file=<path> --command=verify.

### [MISLEADING] file_checksum_dump is not a direct MANIFEST printer
- File: docs/components/debugging_tools/01_ldb_tool.md, Metadata Inspection Commands / file_checksum_dump
- Claim: "Displays file checksums for all SST files. Uses FileChecksumList from the MANIFEST."
- Reality: file_checksum_dump reconstructs a VersionSet, lists column families, recovers the manifest state, and then calls GetLiveFilesChecksumInfo(). The output is derived from recovered live-file metadata, not a raw one-step manifest dump.
- Source: tools/ldb_cmd.cc, GetLiveFilesChecksumInfoFromVersionSet; tools/ldb_cmd.cc, FileChecksumDumpCommand::DoCommand
- Fix: Explain that the command recovers live-file checksum info through VersionSet.

### [MISLEADING] reduce_levels does not operate on the configured num_levels and can fail after compaction
- File: docs/components/debugging_tools/01_ldb_tool.md, Database Manipulation Commands / reduce_levels
- Claim: "Changes the number of levels in an existing database. Opens the DB, compacts all data into the highest level, closes, then calls VersionSet::ReduceNumberOfLevels() to rewrite the MANIFEST with fewer levels."
- Reality: GetOldNumOfLevels() computes the highest non-empty level plus one, not the configured num_levels. Later, VersionSet::ReduceNumberOfLevels() only succeeds when at most one level in the range from new_levels minus one to current_levels minus one contains files.
- Source: tools/ldb_cmd.cc, ReduceDBLevelsCommand::GetOldNumOfLevels; tools/ldb_cmd.cc, ReduceDBLevelsCommand::DoCommand; db/version_set.cc, VersionSet::ReduceNumberOfLevels
- Fix: Document both the "highest non-empty level" definition and the single-nonempty-level precondition.

### [MISLEADING] read_timestamp is not a generic optional read knob
- File: docs/components/debugging_tools/01_ldb_tool.md, Global Options
- Claim: "--read_timestamp=<ts> | Read at a specific user-defined timestamp"
- Reality: The option is required for column families whose comparator has user-defined timestamps and invalid for column families that do not use them. An empty value is also rejected when timestamps are required.
- Source: tools/ldb_cmd.cc, LDBCommand::MaybePopulateReadTimestamp
- Fix: State the exact validity rules instead of presenting it as an always-optional timestamp hint.

### [MISLEADING] sst_dump raw output naming is oversimplified
- File: docs/components/debugging_tools/02_sst_dump_tool.md, Commands / raw
- Claim: "Dumps raw block-level data to a file named <filename>_dump.txt."
- Reality: The tool strips a trailing .sst before appending _dump.txt, so foo.sst becomes foo_dump.txt, not foo.sst_dump.txt.
- Source: tools/sst_dump_tool.cc, raw command output filename construction
- Fix: Describe the basename-based naming rule.

### [MISLEADING] sst_dump scan does not emit a fixed key or seqno or type string
- File: docs/components/debugging_tools/02_sst_dump_tool.md, Commands / scan
- Claim: "Output format per entry: '<key>' seq:<seqno>, type:<type> => <value>"
- Reality: SstFileDumper::ReadSequential() prints ParsedInternalKey::DebugString(), includes user-defined timestamps when present, and has special output paths for wide-column entities, packed preferred-seqno values, and decoded blob indexes.
- Source: table/sst_file_dumper.cc, SstFileDumper::ReadSequential
- Fix: Replace the single fixed template with a description of the actual format families.

### [MISLEADING] trace_analyzer features are presented as always-on, exhaustive outputs
- File: docs/components/debugging_tools/03_operation_tracing.md, Trace Analyzer Tool
- Claim: "The trace_analyzer tool ... computes per-operation statistics including: Access frequency and key distribution per column family, QPS (queries per second) over time, Key size and value size distributions, Operation type correlations (e.g., Get after Put patterns), and Per-key access counts and temporal patterns."
- Reality: Most of these outputs are optional gflags. Analysis is enabled per operation type via analyze_* flags, QPS output requires output_qps_stats, correlations require print_correlation, whole-keyspace stats require key_space_dir, and value-size distributions are meaningful only for Put and Merge.
- Source: tools/trace_analyzer_tool.cc, DEFINE_bool and DEFINE_string flag declarations; tools/trace_analyzer_tool.cc, TraceAnalyzer constructor; tools/trace_analyzer_tool.cc, OutputAnalysisResult
- Fix: Reframe the analyzer as a flag-driven tool with optional reports rather than a fixed report generator.

### [MISLEADING] MultiGet analysis is per key, not per MultiGet request
- File: docs/components/debugging_tools/03_operation_tracing.md, Trace Analyzer Tool
- Claim: "QPS (queries per second) over time" and "The analyzer defines operation types via TraceOperationType enum: Get, Put, Delete, SingleDelete, RangeDelete, Merge, IteratorSeek, IteratorSeekForPrev, MultiGet, and PutEntity."
- Reality: The analyzer counts MultiGet on a per-requested-key basis, not per MultiGet call. The test explicitly expects three MultiGet calls requesting nine keys to contribute MultiGet QPS of nine. PutEntity is present in the enum and handler, but there is no analyze_put_entity flag or type-name wiring to make it a normal analyzable CLI mode.
- Source: tools/trace_analyzer_tool.cc, DEFINE_bool(analyze_multiget, ...); tools/trace_analyzer_tool.cc, TraceAnalyzer constructor; tools/trace_analyzer_tool.cc, OutputAnalysisResult; tools/trace_analyzer_test.cc, MultiGet
- Fix: Call out MultiGet’s per-key accounting and either document PutEntity’s missing CLI plumbing or stop listing it as a supported analyzer mode.

### [MISLEADING] Block cache trace overload semantics are incomplete
- File: docs/components/debugging_tools/04_block_cache_tracing.md, Tracing Lifecycle
- Claim: "Step 2: Call DB::StartBlockCacheTrace() with TraceOptions (or BlockCacheTraceOptions) and the writer."
- Reality: The TraceOptions overload only forwards sampling_frequency and max_trace_file_size. TraceOptions.filter and TraceOptions.preserve_write_order are ignored by block-cache tracing.
- Source: db/db_impl/db_impl.cc, DBImpl::StartBlockCacheTrace; include/rocksdb/block_cache_trace_writer.h, BlockCacheTraceOptions; include/rocksdb/block_cache_trace_writer.h, BlockCacheTraceWriterOptions
- Fix: Spell out the overload behavior and say which fields are honored.

### [MISLEADING] RepairDB’s high-level phase narrative skips the actual cross-component control flow
- File: docs/components/debugging_tools/11_repair_and_recovery.md, Repair Process
- Claim: "The repair proceeds through four phases:"
- Reality: Repairer::Run() archives old MANIFEST files, creates a fresh descriptor via DBImpl::NewDB(), recovers a VersionSet from that new manifest, scans existing SSTs to recreate column family metadata, clears the temporary table list, replays WALs, rescans generated tables, and only then applies VersionEdits for recovered files.
- Source: db/repair.cc, Repairer::Run
- Fix: Replace the simplified four-step story with the real sequence of interactions across DBImpl, VersionSet, table scanning, and WAL replay.

### [MISLEADING] GetThreadList is not "one per active thread"
- File: docs/components/debugging_tools/10_thread_status.md, Using GetThreadList
- Claim: "The function returns a vector of ThreadStatus structs, one per active thread."
- Reality: GetThreadList() returns registered RocksDB threads, including idle background pool threads whose db_name and cf_name are empty and whose operation_type is OP_UNKNOWN. The GetThreadStatus test counts HIGH, LOW, and BOTTOM threads after SetBackgroundThreads() without requiring active operations.
- Source: monitoring/thread_status_updater.cc, ThreadStatusUpdater::GetThreadList; db/db_test.cc, GetThreadStatus
- Fix: Say "registered RocksDB threads, including idle background threads" and explain how idle threads are represented.

### [STALE] The thread-status operation table is missing a current enum value
- File: docs/components/debugging_tools/10_thread_status.md, Operation Types
- Claim: "The Operation Types table" lists OP_COMPACTION through OP_MULTIGETENTITY as the current set.
- Reality: ThreadStatus::OperationType now also includes OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST.
- Source: include/rocksdb/thread_status.h, ThreadStatus::OperationType
- Fix: Add the missing operation and describe when it appears.

## Completeness Gaps

### ldb open-mode restrictions and option interactions
- Why it matters: The current global-options table invites invalid combinations. Users will hit errors when combining ttl, use_txn, secondary, follower, read-only, or read_timestamp, and the docs give them no warning.
- Where to look: tools/ldb_cmd.cc, LDBCommand::OpenDB; tools/ldb_cmd.cc, LDBCommand::MaybePopulateReadTimestamp
- Suggested scope: Expand chapter 01 with a dedicated "Open modes and restrictions" subsection.

### scan-specific TTL, timestamp, and iterator-property behavior
- Why it matters: scan is the main human debugging command for TTL DBs, user-defined timestamps, wide-column values, and write-time inspection, but the chapter only documents a narrow key-range iterator.
- Where to look: tools/ldb_cmd.cc, ScanCommand::Help; tools/ldb_cmd.cc, ScanCommand::DoCommand
- Suggested scope: Expand the scan section in chapter 01 rather than burying these behaviors in workflows.

### Recent sst_dump options and recompress controls
- Why it matters: Several real debugging levers are missing from the docs, including show_summary, list_meta_blocks, readahead_size, paired compression-level semantics, and recent raw-mode output improvements.
- Where to look: tools/sst_dump_tool.cc; table/sst_file_dumper.h; recent history touching tools/sst_dump_tool.cc and table/sst_file_dumper.cc
- Suggested scope: Extend chapter 02’s option table and recompress discussion.

### Statistics history persistence to disk
- Why it matters: Chapter 07 explains periodic dumps and in-memory history, but omits persist_stats_to_disk and the hidden column family used for durable stats history. That is a major observability choice.
- Where to look: include/rocksdb/options.h, persist_stats_to_disk and related fields; monitoring/stats_history_test.cc
- Suggested scope: Add a new subsection to chapter 07 under Stats Dumping and Persistence.

### Build-time gating of auxiliary debugging binaries
- Why it matters: trace_analyzer, io_tracer_parser, rocksdb_dump, and rocksdb_undump are all wrapped in GFLAGS guards. A reader can otherwise assume these tools always exist in a local build.
- Where to look: tools/trace_analyzer_tool.cc; tools/io_tracer_parser_tool.cc; tools/dump/rocksdb_dump.cc; tools/dump/rocksdb_undump.cc
- Suggested scope: Brief notes in chapters 03, 05, and 11.

### Partial TraceOptions semantics outside query tracing
- Why it matters: The docs currently encourage readers to think TraceOptions behaves uniformly across tracing subsystems, but block-cache tracing and IO tracing ignore most TraceOptions fields.
- Where to look: db/db_impl/db_impl.cc, DBImpl::StartBlockCacheTrace; trace_replay/io_tracer.cc, IOTraceWriter::WriteIOOp
- Suggested scope: Add explicit option tables to chapters 04 and 05.

### Logger retention and naming caveats
- Why it matters: keep_log_file_num has a hard validation rule and db_log_dir changes file naming. Operators managing external log directories will otherwise misidentify files or assume zero is a valid retention setting.
- Where to look: db/db_impl/db_impl_open.cc, keep_log_file_num validation; file/filename.cc, OldInfoLogFileName; logging/auto_roll_logger_test.cc
- Suggested scope: Extend chapter 09’s AutoRollLogger and options sections.

## Depth Issues

### RepairDB chapter needs the real cross-component control flow
- Current: A flat four-phase list with one sentence per phase.
- Missing: Archiving old MANIFEST files, creating a fresh descriptor via DBImpl::NewDB(), recovering VersionSet from that descriptor, scanning SSTs before WAL replay to recreate column families, rescanning after WAL conversion, and recovering epoch numbers before applying edits.
- Source: db/repair.cc, Repairer::Run; db/repair.cc, Repairer::AddTables

### Thread status chapter needs the lock-free consistency model
- Current: A simple API tour with a few field tables.
- Missing: The ordering discipline that makes GetThreadList() consistent despite lock-free per-thread updates, the difference between registered threads and enabled tracking, and the rule that lower-level fields are only meaningful when higher-level fields are present.
- Source: monitoring/thread_status_updater.h, file header comments; monitoring/thread_status_updater.cc, ThreadStatusUpdater::GetThreadList

### Trace analyzer chapter needs operational detail, not just a feature list
- Current: A static bullet list of outputs and operation types.
- Missing: Which outputs require which flags, how MultiGet is counted, which operation types are actually analyzable from the CLI, and the fact that some QPS assertions are disabled in tests because the output is not fully robust.
- Source: tools/trace_analyzer_tool.cc, flag declarations and constructor; tools/trace_analyzer_test.cc, file header comment and MultiGet test

### scan needs a real description of what it prints
- Current: A minimal forward-iteration description.
- Missing: TTL time filtering, optional human-readable TTL timestamps, user-defined timestamp handling, wide-column output, and the extra iterator property lookup behind get_write_unix_time.
- Source: tools/ldb_cmd.cc, ScanCommand::DoCommand

## Structure and Style Violations

### Widespread inline code quoting violates the requested documentation style
- File: entire docs/components/debugging_tools set
- Details: Nearly every section uses inline code formatting for filenames, APIs, enum names, commands, and options despite the requested "NO inline code quotes" rule.

### Several Files lines are incomplete or inaccurate
- File: docs/components/debugging_tools/03_operation_tracing.md; docs/components/debugging_tools/05_io_tracing.md; docs/components/debugging_tools/11_repair_and_recovery.md
- Details: Chapter 03 omits utilities/trace/replayer_impl.cc even though it implements the documented replay behavior. Chapter 05 omits env/file_system_tracer.cc, which performs the actual IO interception. Chapter 11 lists tools/dump/db_dump_tool.cc but not the actual CLI wrappers tools/dump/rocksdb_dump.cc and tools/dump/rocksdb_undump.cc.

### Invariant language is used for defaults and outcomes, not true invariants
- File: docs/components/debugging_tools/index.md; docs/components/debugging_tools/11_repair_and_recovery.md
- Details: "The LOG file is written to the database directory by default; db_log_dir option redirects it elsewhere" and "After repair, all data resides in L0" are operational defaults or outcomes, not crash-or-corruption invariants.

## Undocumented Complexity

### ldb has a real open-mode matrix, not independent boolean flags
- What it is: ttl, use_txn, read-only, secondary, follower, and read_timestamp interact through a set of mutual exclusions and preconditions. Some combinations fail immediately, some silently switch DB open APIs, and read_timestamp is required or forbidden based on column-family comparator state.
- Why it matters: This is exactly the kind of debugging pitfall operators hit when moving between normal DBs, TTL DBs, secondary instances, follower instances, and timestamped schemas.
- Key source: tools/ldb_cmd.cc, LDBCommand::OpenDB; tools/ldb_cmd.cc, LDBCommand::MaybePopulateReadTimestamp
- Suggested placement: New subsection in chapter 01 after Global Options.

### Query tracing has a fidelity versus latency tradeoff for writes
- What it is: When preserve_write_order is false, writes are traced early in the write path for lower latency. When it is true, writes are traced later under trace_mutex_ in batch or WAL order. Replay also treats missing footer or EOF as success and tolerates partially closed traces.
- Why it matters: This changes what "ground truth ordering" means in a trace and affects how safe it is to compare live traces against WAL order or replay partial files.
- Key source: db/db_impl/db_impl_write.cc, early tracer write and ordered tracing paths; utilities/trace/replayer_impl.cc, ReplayerImpl::Replay
- Suggested placement: Chapter 03, TraceOptions Configuration and Replayer API.

### Block cache tracing uses spatial sampling and richer records than the chapter shows
- What it is: sampling_frequency hashes block_key so that full histories are kept for sampled blocks. Records also carry cf_id, cf_name, level, sst_fd_number, get_from_user_specified_snapshot, referenced_data_size, and referenced_key_exist_in_block.
- Why it matters: Readers will otherwise assume simple one-per-N temporal sampling and miss the fields needed to join traces back to SST files and snapshot behavior.
- Key source: trace_replay/block_cache_tracer.cc, ShouldTrace; include/rocksdb/block_cache_trace_writer.h, BlockCacheTraceRecord
- Suggested placement: Chapter 04, options and record-format subsections.

### IO tracing is deliberately lossy in some ways and richer in others
- What it is: It records nanosecond timestamps and latencies, stores only the basename rather than the full path, propagates request_id from IODebugContext, and has no trace footer on EndIOTrace().
- Why it matters: These details directly affect downstream analysis, especially when multiple DBs share filenames or when operators expect a closed trace marker.
- Key source: env/file_system_tracer.cc, FileSystemTracingWrapper methods; trace_replay/io_tracer.cc, IOTraceWriter and IOTracer
- Suggested placement: Chapter 05, IOTraceRecord Format and IOTracer Architecture.

### Persistent stats history changes the storage model, not just retention
- What it is: stats_persist_period_sec uses in-memory history by default, but persist_stats_to_disk moves snapshots into a hidden column family named ___rocksdb_stats_history___. Tests cover toggling the behavior, read-only cases, and hidden-CF interactions.
- Why it matters: This is a major observability durability choice with storage and compatibility consequences.
- Key source: include/rocksdb/options.h, persist_stats_to_disk and related comments; monitoring/stats_history_test.cc
- Suggested placement: Chapter 07, Stats Dumping and Persistence.

### AutoRollLogger file naming depends on db_log_dir and retention is enforced across restarts
- What it is: keep_log_file_num must be greater than zero, old-log trimming runs both on rollover and on logger startup, and filenames under db_log_dir include a DB-path-derived prefix rather than plain LOG.
- Why it matters: This affects log discovery, cleanup, and multi-DB separation when a shared log directory is used.
- Key source: db/db_impl/db_impl_open.cc, keep_log_file_num validation; logging/auto_roll_logger.cc, AutoRollLogger constructor and TrimOldLogFiles; file/filename.cc, OldInfoLogFileName
- Suggested placement: Chapter 09, Log Configuration Options and AutoRollLogger.

### dump_live_files is much heavier than its name suggests
- What it is: The command does not just list live files. It reads CURRENT, dumps the active MANIFEST, iterates every live SST through SstFileDumper, iterates every blob file through BlobDumpTool, and dumps all sorted WAL files.
- Why it matters: Users will otherwise underestimate runtime cost and output volume when running it on large databases.
- Key source: tools/ldb_cmd.cc, DBFileDumperCommand::DoCommand
- Suggested placement: Chapter 01, separate subcommand entry for dump_live_files.

## Positive Notes

- The index is a reasonable high-level map. It stays within the requested line budget and gives a useful chapter table for navigation.
- The docs at least separate query tracing, block-cache tracing, and IO tracing into distinct chapters instead of collapsing them into one vague "tracing" section.
- The Statistics and PerfContext chapters identify the right major APIs and metric families, even though several defaults and option details need correction.
- The workflows chapter has the right instinct of combining logs, properties, live instrumentation, and offline tools rather than pretending one tool answers everything.
