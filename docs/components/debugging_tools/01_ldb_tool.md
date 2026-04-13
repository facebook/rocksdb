# ldb Tool

**Files:** tools/ldb_tool.cc, tools/ldb_cmd.cc, tools/ldb_cmd_impl.h, include/rocksdb/utilities/ldb_cmd.h, include/rocksdb/ldb_tool.h

## Overview

The ldb tool (inherited from LevelDB ancestry) is the primary CLI for inspecting, querying, modifying, and recovering RocksDB databases. It provides approximately 39 subcommands organized by function: data queries, metadata inspection, database manipulation, and recovery.

## Architecture and Command Dispatch

All commands inherit from the LDBCommand base class (see include/rocksdb/utilities/ldb_cmd.h). The dispatch flow is:

Step 1: LDBCommand::InitFromCmdLineArgs() parses the full command line into a ParsedParams struct containing the subcommand name, positional arguments, option key-value pairs, and boolean flags.

Step 2: LDBCommand::SelectCommand() maps the subcommand name to a concrete command class (e.g., GetCommand, ScanCommand, ManifestDumpCommand).

Step 3: The command constructor validates command-specific options and flags.

Step 4: Run() calls OpenDB() (which internally calls PrepareOptions() and OverrideBaseOptions()), then DoCommand(), then CloseDB(). Some commands (e.g., checkconsistency, manifest_dump) set NoDBOpen() to true, causing Run() to skip OpenDB() and let DoCommand() handle database access directly. If OpenDB() fails and --try_load_options was set, Run() returns early; otherwise it still proceeds to DoCommand(), allowing file-oriented commands to work without a successful DB open.

## Data Query Commands

### get

Retrieves the value for a single key. Uses DB::Get() internally. Supports hex encoding for keys/values (--key_hex, --value_hex), column family selection (--column_family), and read timestamps for user-defined timestamp databases (--read_timestamp).

### scan

Range scan with forward iteration. Accepts --from (inclusive start key), --to (exclusive end key), --max_keys (limit), and --no_value (keys only). Creates an iterator and seeks to the --from key, then iterates forward. Also supports --ttl mode with --start_time/--end_time filtering, --timestamp to display human-readable TTL timestamps, --read_timestamp for user-defined timestamp databases, --get_write_unix_time to print per-key write time via iterator property lookup, and wide-column output for entities.

**Note:** Scan uses forward iteration only. Reverse iteration is not supported by the scan command.

### put / batchput

Write single or multiple key-value pairs. put writes one key-value via DB::Put(). batchput accepts multiple key-value pairs as positional arguments (must be even count) and writes them in a single WriteBatch.

### delete / singledelete / deleterange

Delete operations. delete issues a standard DB::Delete(). singledelete uses DB::SingleDelete() (requires that there has been only one Put() for this key since the previous call to SingleDelete() for the same key). deleterange deletes all keys in the range [start, end) via DB::DeleteRange().

### multi_get / get_entity / multi_get_entity / put_entity

Wide-column and multi-key variants. get_entity retrieves a wide-column entity for a key. put_entity writes a wide-column entity.

## Metadata Inspection Commands

### manifest_dump

Decodes and displays MANIFEST file contents. Shows column family metadata, VersionEdit sequence (file additions/deletions, level assignments), log numbers, sequence numbers, and table file properties. Supports --verbose for detailed output and --json for machine-readable output.

Internally calls DumpManifestFile(), which creates a VersionSet and calls VersionSet::DumpManifest(). If --path is omitted, the command searches the DB directory for MANIFEST files and requires exactly one descriptor file to be present (fails if multiple are found).

### dump_wal

Parses and displays WAL file contents. Shows sequence numbers, operation counts, byte sizes, and optionally the key-value data (--print_value). The --only_print_seqno_gaps flag detects sequence number discontinuities that may indicate corruption or missing writes.

Uses log::Reader to parse WAL records and WriteBatch::Handler to iterate operations within each batch.

### list_column_families

Lists all column families in the database via DB::ListColumnFamilies().

### file_checksum_dump

Displays file checksums for all live SST files. Recovers a VersionSet from the MANIFEST and calls GetLiveFilesChecksumInfo() to obtain checksum data. Does not open the database; operates directly on MANIFEST metadata.

### dump_live_files

Dumps the full contents of live database files. Prints the current MANIFEST, then iterates every live SST file through SstFileDumper (showing key-value pairs), iterates every blob file through BlobDumpTool, and dumps all sorted WAL files. This command can produce very large output and take significant time on large databases.

### list_live_files_metadata

Lists live file paths grouped or sorted by column family and level via DB::GetAllColumnFamilyMetaData(). Does not read file contents. Supports --sort_by_filename to sort by filename instead of grouping by column family.

### get_property

Retrieves a named DB property value (e.g., rocksdb.stats, rocksdb.estimate-num-keys). See chapter 8 (DB Properties) for the full list of available properties.

## Database Manipulation Commands

### compact

Triggers manual compaction via DB::CompactRange(). Accepts --from and --to for range-specific compaction, and --column_family for column family selection.

### checkconsistency

Forces stricter consistency checks during database open and reports whether open succeeds. Sets paranoid_checks = true and num_levels = 64, then opens the database. Consistency verification happens implicitly as part of the DB open process. Does not perform a separate SST walk or explicit key-range verification pass.

### reduce_levels

Changes the number of levels in an existing database. Computes the current number of occupied levels by finding the highest non-empty level (not using the configured num_levels). If already at or below the target, exits. Otherwise compacts all data, then calls VersionSet::ReduceNumberOfLevels() to rewrite the MANIFEST with fewer levels. The reduction only succeeds when at most one level in the range from new_levels - 1 to the current highest occupied level contains files.

**Important:** Requires exclusive access to the database.

### change_compaction_style

Converts from leveled to universal compaction style. The command disables auto compactions, forces a single-file compaction to L0, and verifies the result. Only the level-to-universal direction is supported; universal-to-level conversion is explicitly rejected.

### checkpoint

Creates a database checkpoint via Checkpoint::Create().

### backup / restore

Creates and restores backups using BackupEngine.

### write_extern_sst / ingest_extern_sst

Creates external SST files and ingests them into the database via IngestExternalFile().

### unsafe_remove_sst_file

Removes an SST file reference from the MANIFEST. This causes data loss for any keys stored only in that file. Use only as a last resort when a corrupted SST file prevents the database from opening.

### repair

Runs RepairDB() to recover a corrupted database. See chapter 11 (RepairDB and Recovery) for the detailed repair process.

### approxsize

Estimates the on-disk size of a key range via DB::GetApproximateSizes().

### query

Interactive query mode via DBQuerierCommand. Opens a REPL for issuing get/put/delete commands against the database.

### dump / load

dump (DBDumperCommand) dumps database contents in a text format (different from rocksdb_dump binary). load (DBLoaderCommand) loads data from a dump file into the database.

### idump

Dumps entries with full internal key format, including sequence numbers and entry types. Useful for low-level debugging of the key space.

### create_column_family / drop_column_family

Create or drop a column family in the database.

### list_file_range_deletes

Lists range deletion tombstones per SST file. Useful for diagnosing range deletion accumulation.

### update_manifest

Updates MANIFEST metadata (e.g., file temperatures) without rewriting SST files. Useful for correcting metadata after storage tier changes without requiring full compaction.

### compaction_progress_dump

Shows progress information about currently running compaction jobs. Useful for monitoring long-running compactions.

## Global Options

| Option | Description |
|--------|-------------|
| --db=<path> | Database directory path |
| --column_family=<name> | Target column family (default: default) |
| --hex | Both keys and values in hex encoding |
| --key_hex / --value_hex | Individual hex encoding for keys or values |
| --try_load_options | Load options from the OPTIONS file in the database directory |
| --ignore_unknown_options | Ignore unknown options when loading from OPTIONS file |
| --create_if_missing | Create the database if it does not exist |
| --secondary_path=<path> | Open as a secondary instance (read-only follower) |
| --ttl | Open with TTL support via DBWithTTL |
| --use_txn | Open as TransactionDB |
| --read_timestamp=<ts> | Read at a specific user-defined timestamp. Required for column families whose comparator enables user-defined timestamps; rejected for column families that do not use them. The value is parsed as a uint64_t |
| --txn_write_policy=<0\|1\|2> | Transaction write policy when using --use_txn: 0=WRITE_COMMITTED, 1=WRITE_PREPARED, 2=WRITE_UNPREPARED |

Additional options control compression (--compression_type), block size (--block_size), write buffer (--write_buffer_size), bloom filter (--bloom_bits), and blob storage (--enable_blob_files, --min_blob_size, etc.). See LDBCommand argument constants in include/rocksdb/utilities/ldb_cmd.h for the complete list.

## Embedding ldb in Applications

The ldb tool can be embedded in applications via LDBTool::RunAndReturn() (see include/rocksdb/ldb_tool.h), which returns an int status code. This allows applications to expose a built-in debugging interface that operates on their own database instances with custom options and column family descriptors.

**Note:** The older LDBTool::Run() method is deprecated because it calls exit() and can cause memory leaks. Use RunAndReturn() instead.
