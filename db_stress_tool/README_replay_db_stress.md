# replay_db_stress

A debugging tool for RocksDB db_stress test failures that replays iterator operations from error logs.

## Purpose

When db_stress fails with an iterator verification error, it outputs an `op_logs` string showing the sequence of operations that led to the failure. This tool helps debug these failures by:

1. Parsing the op_logs string
2. Opening the database that failed
3. Replaying the exact sequence of operations
4. Showing the iterator state after each operation

This makes it easier to identify the root cause of iterator inconsistencies.

## Building

```bash
make replay_db_stress
```

## Usage

### Basic Usage

```bash
./replay_db_stress \
  --db=/path/to/failed/db \
  --op_logs="S 000000000000038D000000000000012B0000000000000299 *N*N*N* SFP 000000000000038D000000000000012B00000000000002A2 *P*P*P*"
```

### With Column Family

```bash
./replay_db_stress \
  --db=/path/to/failed/db \
  --op_logs="..." \
  --column_family=my_cf
```

### Extracting op_logs from Error Output

When db_stress fails, look for output like:

```
Verification failed: iterator has key 000000000000038D000000000000012B000000000000029E, but expected state does not.
Column family: 3, op_logs: S 000000000000038D000000000000012B0000000000000299 *N*N*N* SFP 000000000000038D000000000000012B00000000000002A2 *P*P*P* SFP 000000000000038D000000000000012B000000000000029E *
```

Copy the part after `op_logs:` and use it with this tool.

## Operation Codes

The op_logs string contains these operation codes:

- `S <key>` - Seek(key)
- `SFP <key>` - SeekForPrev(key)
- `STF` - SeekToFirst()
- `STL` - SeekToLast()
- `N` - Next()
- `P` - Prev()
- `*` - PrepareValue() (when allow_unprepared_value is enabled)
- `Refresh` - Refresh()

Keys are typically in hexadecimal format.

## Example Workflow

1. db_stress fails with an iterator error
2. Copy the op_logs from the error message
3. Run replay_db_stress on the same database:

```bash
./replay_db_stress \
  --db=/tmp/rocksdb_crashtest_expected \
  --op_logs="S 000000000000038D000000000000012B0000000000000299 *N*N*N* SFP 000000000000038D000000000000012B00000000000002A2 *P*P*P*" \
  --column_family=default
```

4. The tool will replay each operation and show:
   - Each operation being performed
   - Iterator validity after each operation
   - Current key/value when valid
   - Any errors encountered

5. Use this information to identify where the iterator state diverges from expected behavior

## Flags

- `--db` (required) - Path to the RocksDB database
- `--op_logs` (required) - Operation logs string from db_stress failure
- `--column_family` - Column family name (default: "default")
- `--hex_keys` - Whether keys in op_logs are in hex format (default: true)
- `--verbose` - Print verbose output (default: true)
- `--allow_unprepared_value` - Set ReadOptions::allow_unprepared_value (default: false)
- `--try_load_options` - Try to load options from the database's OPTIONS file (default: true)
- `--ignore_unknown_options` - Ignore unknown options when loading OPTIONS file (default: false)

### Options Loading

By default, `replay_db_stress` will try to load the database options from the OPTIONS file in the database directory. This is important because:

1. **Merge Operators**: If your database uses custom merge operators, they will be loaded automatically
2. **Compression**: Proper compression settings will be restored
3. **Other Settings**: Table formats, block sizes, and other configurations will match the original database

If you encounter errors loading options (e.g., due to version mismatch), you can:
- Use `--ignore_unknown_options` to skip unknown options
- Use `--try_load_options=false` to disable options loading entirely (uses default options)

## Tips

- Keep the original database that failed - don't let db_stress overwrite it
- If db_stress prints a column family number, you may need to find the corresponding name
- **The tool opens the database in read-only mode**, so it's safe to use on production DBs and won't modify the database
- Verbose output shows the iterator state after each operation, which helps identify the exact point of failure
