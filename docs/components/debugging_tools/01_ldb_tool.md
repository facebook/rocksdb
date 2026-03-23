# ldb Tool

**Files:** `tools/ldb_tool.cc`, `tools/ldb_cmd.cc`, `tools/ldb_cmd_impl.h`, `include/rocksdb/utilities/ldb_cmd.h`, `include/rocksdb/ldb_tool.h`

## Overview

The `ldb` tool (inherited from LevelDB ancestry) is the primary CLI for inspecting, querying, modifying, and recovering RocksDB databases. It provides approximately 39 subcommands organized by function: data queries, metadata inspection, database manipulation, and recovery.

## Architecture and Command Dispatch

All commands inherit from the `LDBCommand` base class (see `include/rocksdb/utilities/ldb_cmd.h`). The dispatch flow is:

Step 1: `LDBCommand::InitFromCmdLineArgs()` parses the full command line into a `ParsedParams` struct containing the subcommand name, positional arguments, option key-value pairs, and boolean flags.

Step 2: `LDBCommand::SelectCommand()` maps the subcommand name to a concrete command class (e.g., `GetCommand`, `ScanCommand`, `ManifestDumpCommand`).

Step 3: The command constructor validates command-specific options and flags.

Step 4: `Run()` calls `PrepareOptions()` to set defaults, `OverrideBaseOptions()` to apply user-specified options, `OpenDB()` to open the database, `DoCommand()` to execute the command logic, and `CloseDB()` to close the database.

## Data Query Commands

### get

Retrieves the value for a single key. Uses `DB::Get()` internally. Supports hex encoding for keys/values (`--key_hex`, `--value_hex`), column family selection (`--column_family`), and read timestamps for user-defined timestamp databases (`--read_timestamp`).

### scan

Range scan with forward iteration. Accepts `--from` (inclusive start key), `--to` (exclusive end key), `--max_keys` (limit), and `--no_value` (keys only). Creates an iterator and seeks to the `--from` key, then iterates forward.

**Note:** Scan uses forward iteration only. Reverse iteration is not supported by the `scan` command.

### put / batchput

Write single or multiple key-value pairs. `put` writes one key-value via `DB::Put()`. `batchput` accepts multiple key-value pairs as positional arguments (must be even count) and writes them in a single `WriteBatch`.

### delete / singledelete / deleterange

Delete operations. `delete` issues a standard `DB::Delete()`. `singledelete` uses `DB::SingleDelete()` (requires the key was written exactly once without overwrites). `deleterange` deletes all keys in the range `[start, end)` via `DB::DeleteRange()`.

### multi_get / get_entity / multi_get_entity / put_entity

Wide-column and multi-key variants. `get_entity` retrieves a wide-column entity for a key. `put_entity` writes a wide-column entity.

## Metadata Inspection Commands

### manifest_dump

Decodes and displays MANIFEST file contents. Shows column family metadata, `VersionEdit` sequence (file additions/deletions, level assignments), log numbers, sequence numbers, and table file properties. Supports `--verbose` for detailed output and `--json` for machine-readable output.

Uses `VersionSet::DumpManifest()` internally.

### dump_wal

Parses and displays WAL file contents. Shows sequence numbers, operation counts, byte sizes, and optionally the key-value data (`--print_value`). The `--only_print_seqno_gaps` flag detects sequence number discontinuities that may indicate corruption or missing writes.

Uses `log::Reader` to parse WAL records and `WriteBatch::Handler` to iterate operations within each batch.

### list_column_families

Lists all column families in the database via `DB::ListColumnFamilies()`.

### file_checksum_dump

Displays file checksums for all SST files. Uses `FileChecksumList` from the MANIFEST.

### dump_live_files / list_live_files_metadata

Show information about live SST and blob files, including file metadata such as level, size, and key range.

### get_property

Retrieves a named DB property value (e.g., `rocksdb.stats`, `rocksdb.estimate-num-keys`). See chapter 8 (DB Properties) for the full list of available properties.

## Database Manipulation Commands

### compact

Triggers manual compaction via `DB::CompactRange()`. Accepts `--from` and `--to` for range-specific compaction, and `--column_family` for column family selection.

### checkconsistency

Verifies database consistency without opening for writes. Checks that SST files exist, MANIFEST references are valid, and key ranges are correct.

### reduce_levels

Changes the number of levels in an existing database. Opens the DB, compacts all data into the highest level, closes, then calls `VersionSet::ReduceNumberOfLevels()` to rewrite the MANIFEST with fewer levels. Useful for migrating from leveled to universal compaction.

**Important:** Requires exclusive access to the database.

### change_compaction_style

Changes between leveled and universal compaction styles.

### checkpoint

Creates a database checkpoint via `Checkpoint::Create()`.

### backup / restore

Creates and restores backups using `BackupEngine`.

### write_extern_sst / ingest_extern_sst

Creates external SST files and ingests them into the database via `IngestExternalFile()`.

### unsafe_remove_sst_file

Removes an SST file reference from the MANIFEST. This causes data loss for any keys stored only in that file. Use only as a last resort when a corrupted SST file prevents the database from opening.

### repair

Runs `RepairDB()` to recover a corrupted database. See chapter 11 (RepairDB and Recovery) for the detailed repair process.

## Global Options

| Option | Description |
|--------|-------------|
| `--db=<path>` | Database directory path |
| `--column_family=<name>` | Target column family (default: `default`) |
| `--hex` | Both keys and values in hex encoding |
| `--key_hex` / `--value_hex` | Individual hex encoding for keys or values |
| `--try_load_options` | Load options from the OPTIONS file in the database directory |
| `--ignore_unknown_options` | Ignore unknown options when loading from OPTIONS file |
| `--create_if_missing` | Create the database if it does not exist |
| `--secondary_path=<path>` | Open as a secondary instance (read-only follower) |
| `--ttl` | Open with TTL support via `DBWithTTL` |
| `--use_txn` | Open as `TransactionDB` |
| `--read_timestamp=<ts>` | Read at a specific user-defined timestamp |

Additional options control compression (`--compression_type`), block size (`--block_size`), write buffer (`--write_buffer_size`), bloom filter (`--bloom_bits`), and blob storage (`--enable_blob_files`, `--min_blob_size`, etc.). See `LDBCommand` argument constants in `include/rocksdb/utilities/ldb_cmd.h` for the complete list.

## Embedding ldb in Applications

The `ldb` tool can be embedded in applications via `LDBTool::Run()` (see `include/rocksdb/ldb_tool.h`). This allows applications to expose a built-in debugging interface that operates on their own database instances with custom options and column family descriptors.
