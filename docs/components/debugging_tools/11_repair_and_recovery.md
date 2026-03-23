# RepairDB and Recovery

**Files:** db/repair.cc, include/rocksdb/db.h (RepairDB), tools/ldb_cmd_impl.h (RepairCommand, ReduceDBLevelsCommand), tools/dump/db_dump_tool.cc, tools/dump/rocksdb_dump.cc, tools/dump/rocksdb_undump.cc, include/rocksdb/db_dump_tool.h

## Overview

RocksDB provides several recovery and migration tools: RepairDB for reconstructing corrupted databases, reduce_levels for level structure migration, and rocksdb_dump/rocksdb_undump for portable database serialization.

## RepairDB

RepairDB recovers corrupted databases by reconstructing state from surviving SST files. It is available as a C++ API (RepairDB() in include/rocksdb/db.h) and via the ldb repair command.

### Repair Process

The repair proceeds through four phases:

**Phase 1 -- Find Files:** Scans the database directory, archives old MANIFEST files, creates a fresh descriptor via DBImpl::NewDB(), and classifies files by type: SST files, WAL files, and other metadata. See FindFiles() in db/repair.cc.

**Phase 2 -- Convert Logs to Tables:** Replays each WAL file into per-column-family MemTable instances, then emits at most one SST per non-empty column family per WAL via BuildTable(). Corrupted WAL records are skipped with a warning. See ConvertLogToTable() in db/repair.cc.

**Important:** WAL replay is best-effort. Corrupted records are skipped, which may result in data loss for uncommitted writes.

**Phase 3 -- Extract Metadata:** Scans each SST file to extract the smallest and largest keys, largest sequence number, entry count, and oldest blob file reference (for BlobDB). If an SST file cannot be opened (e.g., corrupted header or footer), it is ignored and its data is lost. See ScanTable() in db/repair.cc.

**Phase 4 -- Write Descriptor:** Generates a new MANIFEST via VersionEdit and VersionSet::LogAndApply(). The new MANIFEST sets log number to 0, next file number to 1 + max file found (which may exceed the initial scan value due to WAL-to-SST conversion), last sequence to max sequence across all SSTs, and places all SST files at L0. See AddTables() in db/repair.cc.

**Key Invariant:** After repair, all data resides in L0. The database will reorganize levels through normal compaction upon the next open.

### When to Use RepairDB

| Scenario | Repair Helps? | Notes |
|----------|--------------|-------|
| Corrupted MANIFEST | Yes | Rebuilds MANIFEST from SST files |
| Corrupted WAL | Yes | Recovers committed data from SSTs |
| Missing SST file | Partially | Data in the missing file is lost |
| Bad compaction state | Yes | Reconstructs level structure |
| Crash loop from software bug | Maybe | Fix the bug first, then repair |

### Using RepairDB

Via C++ API: Call RepairDB(db_path, options) (see include/rocksdb/db.h).

Via ldb: Run ldb repair --db=/path/to/db.

## ReduceDBLevels

Changes the number of levels in an existing database. Useful for migrating between compaction styles (e.g., from leveled with 7 levels to universal with fewer levels).

### Algorithm

Step 1: Open the database.

Step 2: If current level count is already <= the target, exit (no work needed).

Step 3: Compact entire database to push all files to the highest level.

Step 4: Close the database.

Step 5: Call VersionSet::ReduceNumberOfLevels() to rewrite the MANIFEST with the new level count.

Step 6: All files end up in the bottom level of the new structure.

**Important:** Requires exclusive access to the database. The command fails if level reduction would violate key ordering invariants (overlapping key ranges at the same level).

### Usage

Via ldb: ldb --db=/path/to/db reduce_levels --new_levels=3

Add --print_old_levels to see current level distribution without modifying.

## rocksdb_dump and rocksdb_undump

Binary dump format for portable database backup/restore. These are standalone binaries (requiring GFLAGS) separate from the ldb tool.

### rocksdb_dump

Exports database contents to a portable dump file. Uses DB::OpenForReadOnly() and iterates all key-value pairs in the default column family.

Dump file format: An 8-byte magic number ("ROCKDUMP"), 8-byte version, 4-byte metadata JSON size, metadata JSON (hostname, database path, creation time), followed by repeated entries of [4-byte key size][key][4-byte value size][value].

**Note:** rocksdb_dump only exports the default column family. For other column families, use ldb dump with --column_family.

### rocksdb_undump

Restores a database from a dump file. Parses the dump header, reads key-value pairs, writes each via DB::Put(), and optionally compacts the database.

**Note:** The restored database will have a different SST file structure than the original (determined by compaction during restore). Use rocksdb_dump/rocksdb_undump for logical backup, not physical file-level backup. For physical backups, use BackupEngine or Checkpoint.

### Usage

Dump: rocksdb_dump --db_path=/path/to/db --dump_location=/path/to/backup.dump

Restore: rocksdb_undump --db_path=/path/to/restored --dump_location=/path/to/backup.dump

Add --anonymous to rocksdb_dump to exclude hostname and timestamp metadata.
