# Parameter Randomization

**Files:** `tools/db_crashtest.py`, `db_stress_tool/db_stress_gflags.cc`, `db_stress_tool/db_stress_test_base.cc`, `db_stress_tool/db_stress_compaction_filter.h`

## Overview

Parameter randomization is central to the stress test's effectiveness. Each run uses randomly generated DB configurations to explore the vast space of option combinations that would be impractical to enumerate manually.

## Parameter Dicts

`db_crashtest.py` defines multiple parameter dictionaries that are merged in priority order:

### Merge Priority (lowest to highest)

1. `default_params` -- Base configuration with hundreds of randomized options
2. `blackbox_default_params` or `whitebox_default_params` -- Mode-specific overrides
3. Feature-specific params (one of): `simple_default_params`, `cf_consistency_params`, `txn_params`, `optimistic_txn_params`, `best_efforts_recovery_params`, `ts_params`, `multiops_txn_params`, `tiered_params`
4. `blob_params` (merged with ~10% probability when compatible with other test options)
5. Command-line arguments (highest priority)

### Parameter Types

Parameters can be either static values or lambdas:

| Type | Behavior | Example |
|------|----------|---------|
| Static value | Fixed for the entire test run, same across crash-restart | `"max_key": 100000` |
| Lambda | Evaluated once per `finalize_and_sanitize()` call, giving different values per run | `"compression_type": lambda: random.choice([...])` |

Important: Some parameters (like `max_key`, `seed`, `use_put_entity_one_in`, `use_trie_index`) must remain fixed across invocations for verification to work. These are intentionally set as static values, not lambdas.

## Feature-Specific Parameter Sets

### simple_default_params

Simplified configuration for focused testing:
- Single column family (`column_families=1`)
- Single background compaction thread
- Skip list memtable only
- Optional secondary instance testing

### cf_consistency_params

For cross-CF atomicity testing:
- Enables `test_cf_consistency=1`
- Small `write_buffer_size` (1MB) to trigger frequent flushes
- Disables snapshot-incompatible features (compaction filter, inplace update)
- Disables unimplemented operations (ingest external file, iterator verification)

### txn_params

For pessimistic TransactionDB testing:
- Enables `use_txn=1`, randomly selects `txn_write_policy` (0=WriteCommitted, 1=WritePrepared, 2=WriteUnprepared)
- Disables WAL disable (transactions require WAL)
- Disables checkpoint (incompatible with WritePrepared)
- Disables pipelined write (incompatible with WritePrepared)
- Disables inplace update (incompatible with snapshot-based transactions)

### multiops_txn_params

For relational-style transaction testing:
- Custom operation mix: 80% custom ops, 5% reads, 15% iterations, 0% writes/deletes
- Very small write buffer (65536 bytes) and frequent flushes
- Single column family
- Extensive fault injection restrictions

### ts_params

For user-defined timestamp testing:
- Sets `user_timestamp_size=8`
- Disables incompatible features (merge, transactions, file ingestion, PutEntity, TimedPut)

### tiered_params

For tiered storage testing:
- Configures `preclude_last_level_data_seconds` and `last_level_temperature`
- For FIFO: sets `file_temperature_age_thresholds`
- Disables blob DB (incompatible with tiered storage)

## finalize_and_sanitize

The `finalize_and_sanitize()` function (see `tools/db_crashtest.py`) resolves lambdas and enforces compatibility rules. Key sanitization rules include:

### Compression

- If `compression_max_dict_bytes=0`, disable dictionary training and buffer
- If compression type is not zstd, disable ZSTD-specific training

### I/O Mode

- If `mmap_read=1`, disable direct I/O and async multiget
- If direct I/O is enabled but not supported, either disable it (release) or mock it (debug)

### Batched Snapshots Mode

- Disables compaction filter, inplace update, fault injection, and multiscan
- Ensures `prefix_size > 0`

### Inplace Update

- Disables delete range, prefix operations, and concurrent memtable write
- Disables sync fault injection and WAL disable

### WAL Disabled

- Enables atomic flush
- Disables sync, write fault injection, reopen, and WAL recycling

### Transactions

- Disables external file ingestion and delete range
- Write-prepared transactions require specific snapshot policies

### FIFO Compaction

- Disables compaction TTL and periodic compaction
- Disables async open files
- Disables tiered storage options for non-FIFO styles

### User-Defined Index (Trie)

- Disables mmap_read (incompatible with zero-copy pointers)
- Disables parallel compression
- Blocks TransactionDB (`use_txn=0`)
- Disables prefix scanning (`prefix_size=0`)
- Disables interpolation search

### Remote Compaction

- Disables blob DB, inplace update, checkpoint, timed put, secondary instances, mmap
- Disables DB-open fault injection (to avoid timeout waiting for remote compaction threads)

### Compaction Filter

When `--enable_compaction_filter=1`, `DbStressCompactionFilter` (see `db_stress_tool/db_stress_compaction_filter.h`) is installed. It coordinates with expected state using `TryLock()` (non-blocking) on key locks to avoid deadlocks with foreground threads. It distinguishes between Remove (SingleDelete-compatible) and Purge (for non-SD-compatible cases) to maintain expected state consistency.

## Dynamic Option Changes

### BuildOptionsTable

`StressTest::BuildOptionsTable()` (see `db_stress_tool/db_stress_test_base.cc`) constructs a table of valid dynamic option values. When `--set_options_one_in > 0`, the test randomly selects an option from this table and calls `SetOptions()`.

The table includes:
- `write_buffer_size`: Original, 2x, and 4x values
- `max_write_buffer_number`: Original, 2x, and 4x values
- `level0_slowdown_writes_trigger` and `level0_stop_writes_trigger`: Various thresholds
- `target_file_size_base`: Various sizes

### Seed Management

The Python-side seed controls parameter randomization:
- `--initial_random_seed_override`: Seeds the initial parameter generation
- `--per_iteration_random_seed_override`: Seeds parameter re-randomization between crash iterations

The C++-side seed (`--seed` / `FLAGS_seed`) controls key generation and operation sequencing. These are independent, allowing the same key sequence with different DB configurations.
