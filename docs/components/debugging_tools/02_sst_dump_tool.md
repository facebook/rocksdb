# sst_dump Tool

**Files:** tools/sst_dump_tool.cc, table/sst_file_dumper.h, table/sst_file_dumper.cc, include/rocksdb/sst_dump_tool.h

## Overview

The sst_dump tool provides low-level SST file inspection for block-by-block analysis, checksum verification, compression benchmarking, and corruption diagnosis. It operates on individual SST files without requiring a database to be opened.

## Architecture

The tool is implemented in the SstFileDumper class (see table/sst_file_dumper.h). The class opens an SST file via GetTableReader(), which parses the footer, loads the index block, and creates a TableReader. All operations then use the table reader's iterator or verification APIs.

## Commands

### check (default)

Iterates through all entries and verifies integrity. When no range filter (--from/--to) or read limit is specified, it also verifies that the entry count matches num_entries - num_range_deletions from table properties (range tombstones are not traversed by the data iterator). Optionally verifies checksums with --verify_checksum. Accepts --from and --to for range-scoped checks.

The checks performed are: (1) the iterator can read all entries without error, (2) entry count matches table properties when no range is specified, and (3) no iterator errors occur.

### scan

Prints all key-value pairs with internal key format (sequence number and type). Supports hex output (--output_hex), prefix filtering (--prefix), and blob index decoding (--decode_blob_index for BlobDB-enabled databases).

Output format varies by entry type: standard entries print as '<key>' seq:<seqno>, type:<type> => <value>. Wide-column entities, preferred-seqno values, and blob indexes have specialized output formats. When user-defined timestamps are enabled, timestamps are included in the key display.

### raw

Dumps raw block-level data to a file. The output filename is constructed by stripping the trailing .sst extension and appending _dump.txt (e.g., foo.sst becomes foo_dump.txt). The output includes data block contents with all key-value pairs in internal format, index block structure, filter block data, meta blocks (properties, compression dictionaries), and footer. Use --show_sequence_number_type to include sequence number and type details.

### verify

Verifies checksums of all blocks (data, index, filter, meta). Reads each block, computes the checksum, and compares against the stored checksum in the block trailer. Reports the first mismatch or success.

### recompress

Benchmarks SST file size under different compression algorithms. Decompresses all data blocks and recompresses them with the specified algorithms, reporting the resulting file size and compression/decompression time for each.

Useful options:
- --compression_types=kSnappyCompression,kZSTD -- test specific algorithms
- --compression_level_from=1 --compression_level_to=9 -- test compression level range
- --block_size=32768 -- test with custom block size

This command is valuable for evaluating compression tradeoffs before changing compression or compression_per_level options (see ColumnFamilyOptions in include/rocksdb/advanced_options.h).

### identify

Checks if a file is a valid SST file, or lists all SST files in a directory.

## Additional Options

| Option | Description |
|--------|-------------|
| --file=<path> | SST file or directory to analyze |
| --show_properties | Print table properties after command |
| --from=<key> / --to=<key> | Start/end key for scan and check |
| --prefix=<key> | Filter by key prefix during scan |
| --read_num=<n> | Limit number of entries read |
| --input_key_hex | Interpret --from/--to as hex-encoded |
| --output_hex | Output keys and values in hex |
| --decode_blob_index | Decode blob index values to human-readable format |
| --parse_internal_key=<hex> | Parse and display the components of an internal key |

## Table Properties

When --show_properties is enabled, the output includes:

- Number of data blocks, entries, deletions, merge operands, and range deletions
- Raw key size and raw value size (uncompressed)
- Data size, index size, and filter size
- Compression algorithm used
- Comparator name
- Oldest key time and file creation time
- Filter policy name
- Column family ID and name

These properties are stored in the SST file's meta block and are read from TableProperties (see include/rocksdb/table_properties.h).

## Embedding sst_dump in Applications

The sst_dump tool can be embedded via SSTDumpTool::Run() (see include/rocksdb/sst_dump_tool.h), similar to ldb embedding.

## Common Use Cases

| Scenario | Command |
|----------|---------|
| Verify SST file integrity | sst_dump --file=<path> --command=verify |
| Inspect key-value contents | sst_dump --file=<path> --command=scan --output_hex |
| Check compression ratio | sst_dump --file=<path> --show_properties |
| Evaluate alternative compression | sst_dump --file=<path> --command=recompress |
| Diagnose block-level corruption | sst_dump --file=<path> --command=raw |
| Check if file is valid SST | sst_dump --file=<path> --command=identify |
