# RocksDB Debugging Tools

## Overview

RocksDB provides a suite of command-line debugging tools for inspecting database state, diagnosing corruption, analyzing performance issues, and recovering from failures. These tools operate on RocksDB files (SST, MANIFEST, WAL) and databases without requiring application-level integration.

### Tool Categories

1. **Database Inspector (`ldb`)**: Multi-purpose CLI for querying, modifying, and inspecting live or offline databases
2. **SST File Dumper (`sst_dump`)**: Low-level SST file inspector for block-level analysis
3. **Backup/Restore (`db_dump`/`db_undump`)**: Database serialization to portable dump format
4. **Recovery Tools (`RepairDB`, `reduce_levels`)**: Disaster recovery and schema migration

---

## 1. ldb Tool

**Files:** `tools/ldb_tool.cc`, `tools/ldb_cmd.cc`, `include/rocksdb/utilities/ldb_cmd.h`

The `ldb` (LevelDB tool, inherited from LevelDB ancestry) is the primary debugging interface. It provides ~30 subcommands organized by function.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  ldb [global-options] <subcommand> [command-specific-args]  │
└──────────────────────┬──────────────────────────────────────┘
                       │
       ┌───────────────┴───────────────┬─────────────────┐
       v                               v                 v
┌──────────────┐              ┌─────────────────┐  ┌──────────────┐
│ LDBCommand   │              │ Data Commands   │  │ Meta Commands│
│ Base Class   │──────────────│ get, put, scan  │  │ manifest_dump│
│              │              │ delete, compact │  │ dump_wal     │
│ - ParseArgs  │              └─────────────────┘  │ file_checksum│
│ - OpenDB     │                                   └──────────────┘
│ - ValidateOpts│
│ - DoCommand  │
└──────────────┘
```

**Files:** `include/rocksdb/utilities/ldb_cmd.h:31-144`

⚠️ **INVARIANT:** All ldb commands share a common argument parsing framework. Global options (`--db`, `--column_family`, `--hex`) are parsed by `LDBCommand::InitFromCmdLineArgs`, command-specific options are handled in each subclass constructor.

**Files:** `tools/ldb_cmd.cc:2683-2786`

### Core Subcommands

#### Data Query Commands

##### `ldb get <key>`

Retrieve value for a single key.

```bash
# Basic get
ldb --db=/path/to/db get mykey

# Get with hex encoding
ldb --db=/path/to/db --key_hex --value_hex get 0x6D796B6579

# Get from specific column family
ldb --db=/path/to/db --cf_name=cf1 get mykey

# Get with timestamp (for user-defined timestamp DBs)
ldb --db=/path/to/db --timestamp=123456789 get mykey
```

**Implementation:** `tools/ldb_cmd_impl.h:393-407`, uses `DB::Get()` API

##### `ldb scan`

Range scan with filtering options.

```bash
# Scan all keys
ldb --db=/path/to/db scan

# Scan range [from, to)
ldb --db=/path/to/db scan --from=key1 --to=key10

# Scan with prefix
ldb --db=/path/to/db scan --from=prefix

# Scan with limit
ldb --db=/path/to/db scan --max_keys=100

# Scan without printing values (keys only)
ldb --db=/path/to/db scan --no_value
```

**Implementation:** `tools/ldb_cmd_impl.h:495-515`, creates iterator and seeks to `--from` key

⚠️ **INVARIANT:** Scan uses forward iteration only. To iterate backwards, use application-level code with `Iterator::SeekForPrev()` and `Prev()`.

##### `ldb put <key> <value>`

Write a key-value pair.

```bash
# Basic put
ldb --db=/path/to/db put mykey myvalue

# Put with hex encoding
ldb --db=/path/to/db --key_hex --value_hex put 0x6B6579 0x76616C7565

# Put to specific column family
ldb --db=/path/to/db --cf_name=cf1 put mykey myvalue

# Put with TTL (requires --ttl flag at DB open)
ldb --db=/path/to/db --ttl put tempkey tempvalue
```

**Implementation:** `tools/ldb_cmd_impl.h:566-583`

##### `ldb delete <key>`

Delete a key.

```bash
# Standard delete
ldb --db=/path/to/db delete mykey

# Single delete (optimization for non-overwritten keys)
ldb --db=/path/to/db singledelete mykey

# Range delete [start, end)
ldb --db=/path/to/db deleterange start_key end_key
```

**Files:** `tools/ldb_cmd_impl.h:517-564`

⚠️ **INVARIANT:** `single_delete` requires the key was never overwritten (written exactly once). Violating this causes undefined behavior (typically read-after-delete or space amplification).

#### Metadata Inspection Commands

##### `ldb manifest_dump`

Decode and display MANIFEST file contents.

```bash
# Dump MANIFEST (auto-detects current MANIFEST)
ldb --db=/path/to/db manifest_dump

# Dump with verbose column family details
ldb --db=/path/to/db manifest_dump --verbose

# Output as JSON
ldb --db=/path/to/db manifest_dump --json

# Dump specific MANIFEST file
ldb --db=/path/to/db manifest_dump --path=/path/to/db/MANIFEST-000042
```

**Output includes:**
- Column family metadata (name, ID, comparator)
- VersionEdit sequence: file additions/deletions, level assignments
- Compaction pointers
- Log numbers, sequence numbers
- Table file properties (size, key range, entries)

**Implementation:** `tools/ldb_cmd_impl.h:178-200`, uses `VersionSet::DumpManifest()`

**Files:** `db/version_set.cc:7089-7142` (DumpManifest implementation)

##### `ldb dump_wal`

Parse and display WAL file contents.

```bash
# Dump all WAL files in DB directory
ldb --db=/path/to/db dump_wal

# Dump specific WAL file
ldb --db=/path/to/db dump_wal --wal_file=/path/to/000123.log

# Print WriteBatch headers only (no key-value data)
ldb dump_wal --print_header

# Print full key-value data
ldb dump_wal --print_value

# Check for sequence number gaps (detect missing writes)
ldb dump_wal --only_print_seqno_gaps
```

**Output format:**
```
Sequence,Count,ByteSize,Physical Offset,Key(s)
1,1,12,0,PUT(foo) : bar
2,3,45,12,PUT(key1) : val1, DELETE(key2), MERGE(key3) : delta
```

**Implementation:** `tools/ldb_cmd_impl.h:364-390`, uses `log::Reader`

**Files:** `tools/ldb_cmd.cc:188-200` (DumpWalFile declaration), `tools/ldb_cmd.cc:3165+` (DumpWalFile implementation)

⚠️ **INVARIANT:** WAL records are always read in chronological order (sorted by log file number). This matches recovery replay order.

##### `ldb list_column_families`

List all column families in the database.

```bash
ldb --db=/path/to/db list_column_families
```

**Output:** One column family name per line.

**Implementation:** `tools/ldb_cmd_impl.h:259-272`, calls `DB::ListColumnFamilies()`

##### `ldb file_checksum_dump`

Display file checksums for all SST files.

```bash
ldb --db=/path/to/db file_checksum_dump
```

**Output:** File number, checksum type, checksum value (hex)

**Implementation:** `tools/ldb_cmd_impl.h:224-242`, uses `FileChecksumList` from MANIFEST

#### Database Manipulation Commands

##### `ldb compact`

Trigger manual compaction on a key range.

```bash
# Compact entire database
ldb --db=/path/to/db compact

# Compact specific range
ldb --db=/path/to/db compact --from=start_key --to=end_key

# Compact specific column family
ldb --db=/path/to/db --cf_name=cf1 compact
```

**Implementation:** `tools/ldb_cmd_impl.h:17-34`, calls `DB::CompactRange()`

##### `ldb checkconsistency`

Verify database consistency without opening for writes.

```bash
ldb --db=/path/to/db checkconsistency
```

Checks:
- SST files exist and are readable
- MANIFEST references are valid
- Key ranges within/across levels are correct
- File sizes match metadata

**Implementation:** `tools/ldb_cmd_impl.h:629-644`

#### Batch Operations

##### `ldb batchput`

Write multiple key-value pairs from stdin.

```bash
# Read "key value" pairs from stdin (one per line)
ldb --db=/path/to/db batchput < input.txt

# With hex encoding
ldb --db=/path/to/db --key_hex --value_hex batchput < hex_input.txt
```

**Input format:** `<key> <value>` per line, space-separated

**Implementation:** `tools/ldb_cmd_impl.h:474-493`

##### `ldb load`

Bulk load from dump file (created by `dump` command or `db_dump` tool).

```bash
ldb --db=/path/to/db load --input_file=dump.txt

# Create DB if missing
ldb --db=/path/to/db load --input_file=dump.txt --create_if_missing

# With custom options
ldb --db=/path/to/db load --input_file=dump.txt \
  --compression_type=lz4 --write_buffer_size=67108864
```

**Implementation:** `tools/ldb_cmd_impl.h:152-176`

#### Advanced Commands

##### `ldb reduce_levels`

Reduce the number of levels in an existing database.

```bash
# Reduce to 3 levels (compacts L3+ into L2)
ldb --db=/path/to/db reduce_levels --new_levels=3

# Print level info without modifying
ldb --db=/path/to/db reduce_levels --new_levels=3 --print_old_levels
```

**Use case:** Migrate from leveled compaction (7 levels) to universal compaction (fewer levels)

**Files:** `tools/ldb_cmd_impl.h:310-339`

⚠️ **INVARIANT:** Cannot be used on live database. Must close DB, run `reduce_levels`, then reopen with new level configuration.

**Implementation flow:**
1. Open DB read-only
2. Check if reduction is possible (no overlapping ranges in target level)
3. Move files from higher levels to target level via VersionEdit
4. Write new MANIFEST

##### `ldb approxsize`

Estimate size of key range.

```bash
ldb --db=/path/to/db approxsize --from=key1 --to=key1000
```

**Output:** Approximate size in bytes (based on SST metadata, not exact)

**Implementation:** `tools/ldb_cmd_impl.h:457-472`, calls `DB::GetApproximateSizes()`

##### `ldb dump`

Export database to text dump format.

```bash
# Dump entire DB
ldb --db=/path/to/db dump --path=output.txt

# Dump specific range
ldb --db=/path/to/db dump --path=output.txt --from=key1 --to=key100

# Dump with hex encoding
ldb --db=/path/to/db dump --path=output.txt --hex
```

**Output format:** `<key> ==> <value>` per line

**Implementation:** `tools/ldb_cmd_impl.h:72-118`

### Global Options

All commands accept these options:

| Option | Description | Example |
|--------|-------------|---------|
| `--db=<path>` | Database directory path | `--db=/tmp/rocksdb` |
| `--cf_name=<name>` | Target column family (default: `default`) | `--cf_name=metadata` |
| `--hex` | Both keys and values in hex | `--hex` |
| `--key_hex` | Keys in hex encoding | `--key_hex` |
| `--value_hex` | Values in hex encoding | `--value_hex` |
| `--try_load_options` | Load options from OPTIONS file | `--try_load_options` |
| `--create_if_missing` | Create DB if doesn't exist | `--create_if_missing` |
| `--secondary_path=<path>` | Open as secondary instance | `--secondary_path=/tmp/secondary` |
| `--ttl` | Open with TTL support | `--ttl` |
| `--use_txn` | Open as TransactionDB | `--use_txn` |

**Files:** `include/rocksdb/utilities/ldb_cmd.h:34-81`

---

## 2. sst_dump Tool

**Files:** `tools/sst_dump_tool.cc`, `table/sst_file_dumper.h`, `include/rocksdb/sst_dump_tool.h`

Low-level SST file inspector for block-by-block analysis, compression benchmarking, and corruption diagnosis.

### Usage

```bash
sst_dump <options> <file1.sst> [file2.sst ...]
```

### Commands

#### `--command=check` (default)

Iterate through all entries and verify entry count matches table properties.

```bash
# Check file integrity
sst_dump --file=/path/to/123456.sst --command=check

# Check with checksum verification
sst_dump --file=/path/to/123456.sst --command=check --verify_checksum

# Check specific range
sst_dump --file=/path/to/123456.sst --command=check \
  --from=key1 --to=key100
```

**Checks performed:**
- Iterator can read all entries
- Entry count == `num_entries` in table properties (if no range specified)
- No iterator errors

#### `--command=scan`

Print all key-value pairs.

```bash
# Scan entire file
sst_dump --file=/path/to/123456.sst --command=scan

# Scan with hex output
sst_dump --file=/path/to/123456.sst --command=scan --output_hex

# Scan with prefix filter
sst_dump --file=/path/to/123456.sst --command=scan --prefix=user_

# Scan with blob index decoding (for blob-enabled DBs)
sst_dump --file=/path/to/123456.sst --command=scan --decode_blob_index
```

**Output format:**
```
'key1' seq:100, type:1 => value1
'key2' seq:99, type:0 => (deleted)
'key3' seq:98, type:7 => <blob_index>
```

#### `--command=raw`

Dump raw block-level data to `<filename>_dump.txt`.

```bash
sst_dump --file=/path/to/123456.sst --command=raw

# Show internal key format (sequence number + type)
sst_dump --file=/path/to/123456.sst --command=raw --show_sequence_number_type
```

**Output includes:**
- Data block contents (all key-value pairs with internal format)
- Index block structure
- Filter block (bloom filter bits)
- Meta blocks (properties, compression dictionaries)
- Footer

**Files:** `table/sst_file_dumper.cc:245-384` (DumpTable implementation)

#### `--command=verify`

Verify checksums of all blocks (data, index, filter, meta).

```bash
sst_dump --file=/path/to/123456.sst --command=verify
```

**Verification:**
- Read each block
- Compute checksum
- Compare against stored checksum in block trailer
- Report first mismatch or success

**Implementation:** `table/sst_file_dumper.cc:153-183` (VerifyChecksum)

⚠️ **INVARIANT:** Checksum verification requires reading entire block into memory. For large blocks (>1GB), this may cause OOM. Use `--read_num` to limit blocks read.

#### `--command=recompress`

Benchmark SST file size under different compression algorithms.

```bash
# Test all supported compressions
sst_dump --file=/path/to/123456.sst --command=recompress

# Test specific compressions
sst_dump --file=/path/to/123456.sst --command=recompress \
  --compression_types=kSnappyCompression,kZSTD

# Test compression levels
sst_dump --file=/path/to/123456.sst --command=recompress \
  --compression_types=kZSTD \
  --compression_level_from=1 --compression_level_to=9

# Custom block size
sst_dump --file=/path/to/123456.sst --command=recompress \
  --block_size=32768
```

**Output:**
```
Compression: kNoCompression   Size: 12345678 bytes  Read: 45ms  Write: 123ms
Compression: kSnappyCompression  Size: 8901234 bytes  Read: 67ms  Write: 156ms
Compression: kZSTD (level 3)     Size: 7654321 bytes  Read: 89ms  Write: 234ms
```

**Implementation:** `table/sst_file_dumper.cc:100-151` (ShowAllCompressionSizes)

**Use case:** Evaluate compression trade-offs before changing `compression` or `compression_per_level` options.

#### `--command=identify`

Check if file is valid SST or list all SST files in directory.

```bash
# Check single file
sst_dump --file=/path/to/123456.sst --command=identify

# List all SST files in directory
sst_dump --file=/path/to/db_dir --command=identify
```

**Output:** `<filename> is a valid SST file` or error

### Additional Options

| Option | Description | Example |
|--------|-------------|---------|
| `--show_properties` | Print table properties after command | `--show_properties` |
| `--from=<key>` | Start key for scan/check | `--from=user_1000` |
| `--to=<key>` | End key (exclusive) | `--to=user_2000` |
| `--prefix=<key>` | Filter by key prefix | `--prefix=user_` |
| `--read_num=<n>` | Limit entries read | `--read_num=1000` |
| `--input_key_hex` | `--from`/`--to` are hex-encoded | `--input_key_hex` |
| `--decode_blob_index` | Decode blob references to human-readable | `--decode_blob_index` |
| `--parse_internal_key=<hex>` | Parse and print internal key format | `--parse_internal_key=0x...` |

**Files:** `tools/sst_dump_tool.cc:26-144` (help text)

### Table Properties Output

When `--show_properties` is enabled:

```
SST file format: block-based
Table Properties:
  # data blocks: 1234
  # entries: 567890
  # deletions: 123
  # merge operands: 456
  # range deletions: 7
  raw key size: 12345678
  raw value size: 98765432
  data size: 87654321
  index size: 123456
  filter size: 45678
  compression: Snappy
  comparator: leveldb.BytewiseComparator
  oldest key time: 1609459200
  file creation time: 1609545600
  filter policy: bloomfilter
  column family ID: 0
  column family name: default
```

**Files:** `table/sst_file_dumper.cc:57-98` (ShowAllCompressionSizes output)

---

## 3. db_dump / db_undump

**Files:** `tools/dump/db_dump_tool.cc`, `include/rocksdb/db_dump_tool.h`

Binary dump format for database backup/restore across platforms.

### db_dump

Export database to portable dump file.

```bash
# Dump entire database
db_dump --db=/path/to/db --dump_location=/path/to/backup.dump

# Anonymous dump (exclude hostname/timestamp metadata)
db_dump --db=/path/to/db --dump_location=backup.dump --anonymous
```

**Dump format:**
```
[8 bytes: "ROCKDUMP" magic]
[8 bytes: version (0x0000000000000001)]
[4 bytes: metadata JSON size]
[N bytes: metadata JSON {"database-path": ..., "hostname": ..., "creation-time": ...}]
[4 bytes: key size]
[N bytes: key]
[4 bytes: value size]
[M bytes: value]
...
[4 bytes: 0 (end marker)]
```

**Files:** `tools/dump/db_dump_tool.cc:17+` (DbDumpTool::Run)

⚠️ **INVARIANT:** Dump includes ALL column families. To dump a subset, use `ldb dump` with `--cf_name` filter.

### db_undump

Restore database from dump file.

```bash
# Restore to new database
db_undump --db_path=/path/to/restored_db --dump_location=backup.dump

# Restore with custom options
db_undump --db_path=/path/to/restored_db --dump_location=backup.dump \
  --create_if_missing --write_buffer_size=67108864
```

**Files:** `tools/dump/db_dump_tool.cc:130+` (DbUndumpTool::Run)

**Restoration process:**
1. Parse dump header
2. Create WriteBatch for each key-value pair
3. Write batches to DB
4. Sync and close

⚠️ **INVARIANT:** Restored database will have different SST file structure (determined by compaction) than original. Use db_dump for logical backup, not physical file-level backup.

---

## 4. MANIFEST Inspection Details

**Files:** `db/version_set.cc:5764-5990`

MANIFEST files contain the database schema evolution: all VersionEdits (file additions/deletions, level changes, column family operations).

### Parsing MANIFEST

```bash
ldb --db=/path/to/db manifest_dump --verbose
```

**Key information extracted:**

#### VersionEdit Sequence

Each edit represents an atomic schema change:

```
--- Begin VersionEdit (Log Number: 123) ---
Comparator: leveldb.BytewiseComparator
Log Number: 123
Prev Log Number: 122
Next File Number: 500
Last Sequence: 987654

AddFile: Level=0 File=456 Size=1234567 Entries=10000
  SmallestKey='user_1000' @ 100:1
  LargestKey='user_9999' @ 200:1
DeleteFile: Level=1 File=123
CompactionPointer: Level=2 Key='user_5000'
```

**Files:** `db/version_edit.cc:688-943` (DebugString implementation)

#### Column Family Metadata

For each column family:
```
--- Column Family [id=0, name=default] ---
Comparator: leveldb.BytewiseComparator
Level 0: 3 files (total 4567890 bytes)
  File 456: size=1234567 entries=10000 range=['a','m']
  File 457: size=2345678 entries=20000 range=['n','z']
  File 458: size=987645 entries=5000 range=['aa','bb']
Level 1: 5 files (total 12345678 bytes)
Level 2: 12 files (total 45678901 bytes)
...
```

### Common MANIFEST Issues

**Corruption detection:**
```bash
# Check for truncation or corruption
ldb --db=/path/to/db manifest_dump
# Output: "MANIFEST file is truncated" or "Checksum mismatch"
```

**Missing files:**
```
AddFile references: 123456.sst
Actual files in directory: 123457.sst, 123458.sst
```

**Solution:** Use `RepairDB` to reconstruct MANIFEST from existing SST files.

---

## 5. WAL Inspection Details

**Files:** `tools/ldb_cmd.cc:188-373`, `db/log_reader.h`

WAL files contain write-ahead log records: serialized WriteBatches with sequence numbers.

### WAL Dump Output

```bash
ldb --db=/path/to/db dump_wal --print_header --print_value
```

**Record format:**
```
File: /path/to/db/000123.log
----- Batch 0 -----
Sequence: 1
Count: 1
ByteSize: 23
PUT(key1) : value1

----- Batch 1 -----
Sequence: 2
Count: 3
ByteSize: 67
PUT(key2) : value2
DELETE(key3)
MERGE(key4) : delta1
```

**Files:** `db/write_batch.cc:2074-2332` (WriteBatch::Data() format, iteration)

### Detecting Corruption

#### Sequence Number Gaps

```bash
ldb dump_wal --only_print_seqno_gaps
```

**Expected:** Continuous sequence numbers `1, 2, 3, 4, ...`

**Gap detected:**
```
Sequence number gap detected: prev_batch_end=100, current_batch_start=105
Missing sequences: 101, 102, 103, 104
```

**Causes:**
- Partial WAL write (crash mid-write)
- Corruption
- Multiple column families with different WAL files (normal)

#### Checksum Failures

WAL records include CRC32 checksums. `dump_wal` reports checksum mismatches:

```
Corruption: checksum mismatch in record at offset 12345
  Expected: 0xABCD1234
  Actual: 0xBCDE2345
Skipping corrupted record
```

**Files:** `db/log_reader.cc:250-397` (ReadPhysicalRecord)

⚠️ **INVARIANT:** WAL corruption only affects uncommitted writes (after last flush). Use `RepairDB` to recover committed data from SST files.

---

## 6. Checksum Verification

### SST File Checksums

```bash
# Verify individual file
sst_dump --file=/path/to/123456.sst --command=verify

# Verify all blocks
sst_dump --file=/path/to/123456.sst --verify_checksum
```

**Verified components:**
- Data blocks (CRC32 or xxHash64)
- Index blocks
- Filter blocks
- Meta blocks
- Footer

**Files:** `table/block_based/block_based_table_reader.cc:458-523` (VerifyChecksum)

### Database-Level Checksum

```bash
# List all file checksums
ldb --db=/path/to/db file_checksum_dump
```

**Output:**
```
File 123456: crc32c 0xABCD1234
File 123457: crc32c 0xBCDE2345
File 123458: crc32c 0xCDEF3456
```

**Use case:** Detect silent data corruption (bit flips) by comparing checksums over time.

**Files:** `db/db_impl/db_impl_files.cc:315-402` (GetLiveFilesChecksumInfo)

---

## 7. RepairDB

**Files:** `db/repair.cc`, `include/rocksdb/db.h:2281-2292`

Recover corrupted databases by reconstructing state from existing SST files.

### Usage

```cpp
#include "rocksdb/db.h"

Status s = RepairDB("/path/to/db", Options());
if (!s.ok()) {
  // Repair failed
}
```

Command-line (via ldb):
```bash
# No direct ldb command; use C++ API or write small program
```

### Repair Process

**Four phases:**

#### (a) Find Files

Scan database directory and classify files by type:
- SST files (`*.sst`)
- WAL files (`*.log`)
- MANIFEST files (`MANIFEST-*`)
- OPTIONS files
- Other metadata

**Files:** `db/repair.cc:286+` (FindFiles)

#### (b) Convert Logs to Tables

Replay all WAL files, writing contents to new SST files:
- Parse each WAL record
- Apply to MemTable
- Flush MemTable to L0 SST when full
- Skip corrupted records (log warning)

**Files:** `db/repair.cc:340+` (ConvertLogFilesToTables)

⚠️ **INVARIANT:** WAL replay is best-effort. Corrupted records are skipped, potentially losing some writes. Checksummed data in SST files is preferred.

#### (c) Extract Metadata

Scan each SST file to extract:
- Smallest/largest key
- Largest sequence number
- Entry count
- Oldest blob file reference (if BlobDB enabled)

**Files:** `db/repair.cc:534+` (ScanTable)

**Failure handling:** If SST file cannot be opened (corrupted header/footer), file is ignored. Data loss for that file.

#### (d) Write Descriptor

Generate new MANIFEST:
- Set log number = 0 (no active WAL)
- Set next file number = 1 + max file number found
- Set last sequence number = max sequence across all SST files
- Place all SST files at L0 (no level structure assumed)
- Clear compaction pointers

**Files:** `db/repair.cc:669+` (AddTables - generates new MANIFEST via VersionEdit)

⚠️ **INVARIANT:** Repaired database has all data in L0. Compaction will reorganize levels on next open.

### When to Use RepairDB

| Scenario | Repair Helps? | Alternative |
|----------|--------------|-------------|
| Corrupted MANIFEST | ✅ Yes | Manual MANIFEST rebuild (experts only) |
| Corrupted WAL | ✅ Yes (recovers SST data) | Accept data loss since last flush |
| Missing SST file | ❌ No (data in file lost) | Restore from backup |
| Bad compaction state | ✅ Yes | May fix by reconstructing levels |
| Software bug causing crash loop | ⚠️ Maybe | Fix bug first, repair if needed |

**Example scenario:**

```
Error: Corruption: missing SST file 123456.sst
  MANIFEST references it, but file not found

Solution:
  Status s = RepairDB("/path/to/db", options);
  // Repair removes 123456.sst reference, rebuilds MANIFEST
  // Data in 123456.sst is lost
```

---

## 8. ReduceDBLevels

**Files:** `tools/ldb_cmd_impl.h:310-339`

Change the number of levels in an existing database.

### Usage

```bash
# Reduce from 7 levels to 3
ldb --db=/path/to/db reduce_levels --new_levels=3

# Print current level distribution (no modification)
ldb --db=/path/to/db reduce_levels --new_levels=3 --print_old_levels
```

### Algorithm

1. Open DB read-only
2. Check if reduction is possible:
   - No overlapping ranges in target level
   - All files in levels ≥ new_levels can fit in new_levels-1
3. Create VersionEdit to move files:
   ```
   DeleteFile: Level=6 File=123
   AddFile: Level=2 File=123
   ```
4. Write new MANIFEST
5. Close DB

**Files:** `tools/ldb_cmd.cc:2301-2447` (ReduceDBLevelsCommand::DoCommand)

⚠️ **INVARIANT:** Cannot reduce levels if it would violate key ordering invariants (overlapping ranges at same level). Command fails with error in this case.

### Use Case: Compaction Style Migration

**Scenario:** Migrate from leveled compaction (7 levels) to universal compaction (typically 1-2 levels).

**Steps:**
1. Close DB
2. Run `reduce_levels --new_levels=2`
3. Update options: `options.compaction_style = kCompactionStyleUniversal`
4. Reopen DB

**Why needed:** Universal compaction doesn't use L1-L6. Reducing levels avoids wasted space reservations.

---

## 9. Common Debugging Workflows

### Workflow 1: Investigating Data Corruption

**Symptoms:** `Corruption: checksum mismatch` errors during reads

**Steps:**

1. **Identify corrupted file:**
   ```bash
   # Check logs for filename in error message
   grep "Corruption" /path/to/db/LOG
   # Output: Corruption in file 123456.sst at offset 12345
   ```

2. **Verify file integrity:**
   ```bash
   sst_dump --file=/path/to/db/123456.sst --command=verify
   # If fails: Block corruption detected
   ```

3. **Determine data loss scope:**
   ```bash
   sst_dump --file=/path/to/db/123456.sst --command=scan --output_hex
   # Last readable key before corruption
   ```

4. **Recovery options:**
   - **Option A:** Delete corrupted file, lose data in that file
     ```bash
     rm /path/to/db/123456.sst
     # Repair to rebuild MANIFEST
     # (Use C++ RepairDB API)
     ```
   - **Option B:** Restore from backup (if available)

### Workflow 2: Debugging Performance Issues

**Symptoms:** Slow reads or writes

**Steps:**

1. **Check level distribution:**
   ```bash
   ldb --db=/path/to/db manifest_dump --verbose | grep "Level [0-9]:"
   # Output: Level 0: 50 files (too many!)
   ```

2. **Identify large files:**
   ```bash
   ldb --db=/path/to/db manifest_dump --verbose | grep "Size=" | sort -t= -k2 -n
   # Output: File 123: Size=5000000000 (5GB - too large)
   ```

3. **Check compression effectiveness:**
   ```bash
   sst_dump --file=/path/to/db/123456.sst --command=recompress \
     --compression_types=kSnappyCompression,kZSTD
   # Compare original vs. recompressed sizes
   ```

4. **Analyze key distribution:**
   ```bash
   ldb --db=/path/to/db scan --max_keys=100 --no_value
   # Check for hot keys (repeated prefixes)
   ```

5. **Solutions:**
   - Too many L0 files → Increase `level0_file_num_compaction_trigger`
   - Large files → Reduce `target_file_size_base`
   - Poor compression → Change `compression_per_level`

### Workflow 3: Investigating Space Amplification

**Symptoms:** Database size much larger than expected

**Steps:**

1. **Get per-level statistics:**
   ```bash
   ldb --db=/path/to/db manifest_dump --verbose > manifest.txt
   grep -A 20 "--- Column Family" manifest.txt
   # Sum sizes per level
   ```

2. **Check for obsolete data:**
   ```bash
   ldb --db=/path/to/db scan | grep "(deleted)"
   # Many deletes → Space not reclaimed
   ```

3. **Estimate live data:**
   ```bash
   # Count keys
   ldb --db=/path/to/db scan --no_value | wc -l

   # Estimate size
   ldb --db=/path/to/db approxsize --from="" --to="~"
   ```

4. **Solutions:**
   - Manual compaction: `ldb compact`
   - Enable `level_compaction_dynamic_level_bytes`
   - Reduce `max_bytes_for_level_base`

### Workflow 4: Recovering from Crash

**Symptoms:** Database won't open after crash

**Steps:**

1. **Check LOG file:**
   ```bash
   tail -n 100 /path/to/db/LOG
   # Look for last error message
   ```

2. **Common errors:**

   **Error:** `Corruption: missing WAL file 000123.log`

   **Solution:**
   ```bash
   # WAL file lost - data since last flush gone
   # Disable WAL requirement (loses durability guarantee)
   # options.wal_recovery_mode = kTolerateCorruptedTailRecords;
   ```

   **Error:** `Corruption: bad magic number in MANIFEST-000042`

   **Solution:**
   ```bash
   # MANIFEST corrupted - repair
   # (Use RepairDB C++ API)
   ```

   **Error:** `IO error: No such file: 123456.sst`

   **Solution:**
   ```bash
   # SST file missing - repair
   # (Use RepairDB C++ API)
   # Data in that file lost
   ```

3. **If repair fails:**
   - Restore from backup
   - Accept data loss, create new DB

### Workflow 5: Validating Backup Integrity

**Steps:**

1. **Create backup:**
   ```bash
   db_dump --db=/path/to/db --dump_location=backup_$(date +%Y%m%d).dump
   ```

2. **Verify backup readability:**
   ```bash
   # Restore to temporary location
   db_undump --db_path=/tmp/restore_test --dump_location=backup_20250101.dump
   ```

3. **Compare key counts:**
   ```bash
   # Original
   ldb --db=/path/to/db scan --no_value | wc -l

   # Restored
   ldb --db=/tmp/restore_test scan --no_value | wc -l

   # Should match
   ```

4. **Spot-check data:**
   ```bash
   # Get sample keys from original
   ldb --db=/path/to/db scan --max_keys=10

   # Verify in restored
   ldb --db=/tmp/restore_test get <key>
   ```

---

## 10. Internal Implementation Notes

### LDBCommand Architecture

**Files:** `include/rocksdb/utilities/ldb_cmd.h:31-603`

All commands inherit from `LDBCommand` base class:

```cpp
class LDBCommand {
 public:
  // Command registration
  static LDBCommand* SelectCommand(const ParsedParams& params);

  // Lifecycle
  virtual void PrepareOptions();  // Set default options
  virtual void OverrideBaseOptions();  // Modify options based on args
  virtual void DoCommand() = 0;  // Execute command

  // DB access
  void OpenDB();  // Open database with configured options
  void CloseDB();  // Close and cleanup

 protected:
  std::string db_path_;
  DB* db_;
  std::map<std::string, std::string> option_map_;
  std::vector<std::string> flags_;
};
```

**Command dispatch flow:**

```
main() → LDBCommand::InitFromCmdLineArgs()
  ↓
  ParseSingleParam() [parse all args]
  ↓
  SelectCommand(parsed_params)
  ↓
  Command-specific constructor (e.g., GetCommand::GetCommand())
  ↓
  ValidateCmdLineOptions()
  ↓
  PrepareOptions() / OverrideBaseOptions()
  ↓
  Run() → OpenDB() → DoCommand() → CloseDB()
```

**Files:** `tools/ldb_cmd.cc:2683-2786` (SelectCommand), `tools/ldb_cmd.cc:2788-2939` (InitFromCmdLineArgs)

### SstFileDumper Architecture

**Files:** `table/sst_file_dumper.h:17-103`

```cpp
class SstFileDumper {
 public:
  SstFileDumper(const Options& options, const std::string& file_name, ...);

  // Commands
  Status ReadSequential(...);  // Scan/check
  Status DumpTable(...);  // Raw dump
  Status VerifyChecksum();  // Verify
  Status ShowAllCompressionSizes(...);  // Recompress

 private:
  Status GetTableReader(...);  // Open SST file
  std::unique_ptr<TableReader> table_reader_;
  std::unique_ptr<RandomAccessFileReader> file_;
};
```

**Read flow:**

```
SstFileDumper::ReadSequential()
  ↓
  GetTableReader() [open file, parse footer/index]
  ↓
  table_reader_->NewIterator()
  ↓
  iterator->SeekToFirst()
  ↓
  while (iterator->Valid()) {
    PrintKey(iterator->key());
    PrintValue(iterator->value());
    iterator->Next();
  }
```

**Files:** `table/sst_file_dumper.cc:184-245` (ReadSequential)

### WAL Dumper Architecture

**Files:** `tools/ldb_cmd.cc:188-373`

```cpp
void DumpWalFile(Options options, const std::string& wal_file, ...) {
  // Open WAL file
  SequentialFileReader file_reader(...);
  log::Reader reader(...);

  // Read records
  std::string scratch;
  Slice record;
  while (reader.ReadRecord(&record, &scratch)) {
    // Parse WriteBatch
    WriteBatch batch(record);
    PrintBatchInfo(batch);

    if (print_values) {
      // Iterate WriteBatch contents
      WriteBatchIterator iter;
      batch.Iterate(&iter);
    }
  }
}
```

**WriteBatch iteration:**

```cpp
class WALDumperBatchHandler : public WriteBatch::Handler {
  void Put(const Slice& key, const Slice& value) override {
    printf("PUT(%s) : %s\n", key.ToString().c_str(), value.ToString().c_str());
  }
  void Delete(const Slice& key) override {
    printf("DELETE(%s)\n", key.ToString().c_str());
  }
  void Merge(const Slice& key, const Slice& value) override {
    printf("MERGE(%s) : %s\n", key.ToString().c_str(), value.ToString().c_str());
  }
  // ... other operations
};
```

**Files:** `tools/ldb_cmd_impl.h:364-391` (WALDumperCommand class)

---

## Summary

RocksDB's debugging tools provide comprehensive inspection and recovery capabilities:

| Tool | Primary Use | Key Commands |
|------|-------------|--------------|
| **ldb** | Database inspection/modification | `get`, `scan`, `put`, `manifest_dump`, `dump_wal`, `compact` |
| **sst_dump** | SST file analysis | `check`, `scan`, `raw`, `verify`, `recompress` |
| **db_dump/undump** | Backup/restore | Binary dump format |
| **RepairDB** | Corruption recovery | Reconstruct from SST files |
| **reduce_levels** | Schema migration | Change level count |

**Key invariants to remember:**

⚠️ **INVARIANT:** ldb commands auto-detect OPTIONS file if `--try_load_options` specified. Otherwise use default options.

⚠️ **INVARIANT:** sst_dump operates on closed SST files. Do not run on files in active database (may read stale/corrupted blocks).

⚠️ **INVARIANT:** RepairDB is best-effort recovery. Always maintain external backups for critical data.

⚠️ **INVARIANT:** Checksum verification catches data corruption but cannot fix it. Use backups for recovery.

For production debugging workflows, combine multiple tools:
1. `manifest_dump` for schema understanding
2. `dump_wal` for recent write history
3. `sst_dump --verify` for corruption detection
4. `RepairDB` for recovery
5. `db_dump` for logical backups
