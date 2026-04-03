# Test Modes

**Files:** `db_stress_tool/no_batched_ops_stress.cc`, `db_stress_tool/batched_ops_stress.cc`, `db_stress_tool/cf_consistency_stress.cc`, `db_stress_tool/multi_ops_txns_stress.{h,cc}`, `db_stress_tool/db_stress_test_base.h`

## Overview

The stress test framework provides four test modes, each targeting a different correctness property. All modes inherit from the `StressTest` base class (see `db_stress_tool/db_stress_test_base.h`) and must implement the pure virtual methods for operations (`TestGet`, `TestPut`, `TestDelete`, etc.) and verification (`VerifyDb`, `ContinuouslyVerifyDb`).

| Mode | Flag | State Tracked | Primary Goal |
|------|------|---------------|-------------|
| NonBatchedOps | (default) | Yes | Individual operation correctness with expected state verification |
| BatchedOps | `--test_batches_snapshots=1` | No | WriteBatch atomicity |
| CfConsistency | `--test_cf_consistency=1` | No | Cross-column-family write atomicity |
| MultiOpsTxns | `--test_multi_ops_txns=1` | No | Multi-operation transaction correctness |

## NonBatchedOpsStressTest

This is the default and most comprehensive test mode. Each operation (Put, Get, Delete, Merge) is performed individually with per-key lock protection and full expected state tracking.

### State Tracking

`IsStateTracked()` returns `true`. Expected state is maintained in `SharedState::expected_state_manager_`. For every write or delete, the test:

Step 1: Acquire the key lock via `SharedState::GetMutexForKey()`. Step 2: Create a `PendingExpectedValue` via `PreparePut()` or `PrepareDelete()`. Step 3: Perform the DB operation. Step 4: Call `Commit()` on success or `Rollback()` on failure. Step 5: Release the key lock.

### Verification Methods

During `VerifyDb()`, the key space is partitioned among threads. Each thread verifies its assigned range using a randomly selected method:

| Method | Description |
|--------|-------------|
| Iterator | Seeks to start key and iterates forward, comparing each key against expected state |
| Get | Issues individual `Get()` calls for each key |
| GetEntity | Uses the wide-column `GetEntity()` API |
| MultiGet | Batches `Get()` calls |
| MultiGetEntity | Batches wide-column `GetEntity()` calls |
| GetMergeOperands | Validates merge operand history (not available with user-defined timestamps) |

Important: During iterator verification, if the iterator's key is less than the expected key, the test aborts with an out-of-range error, as this indicates key ordering corruption.

### Secondary Instance Validation

When `--test_secondary=1` is enabled, the test opens a secondary (read-only) instance and validates it against the primary. Pre-read expected values establish a lower bound, and post-read expected values establish an upper bound for acceptable values from the secondary.

## BatchedOpsStressTest

Enabled with `--test_batches_snapshots=1`. Tests WriteBatch atomicity by writing multiple related key-value pairs in a single batch and verifying they are all visible (or none visible) when read within a snapshot.

### Atomicity Scheme

For a base key `K` and value `V`, the test writes 10 key-value pairs in a single WriteBatch: keys are formed by prepending digits "0" through "9" to `K`, and values are formed by appending the same digits to `V`. During reads, all 10 keys are read in the same snapshot, and the test verifies that all values share the same base -- detecting any partial batch application.

### Limitations

- `IsStateTracked()` returns `false` -- no external expected state tracking
- `TestDeleteRange()` and `TestIngestExternalFile()` are not implemented
- Requires `--prefix_size > 0` (validated at startup)
- Incompatible with fault injection (`finalize_and_sanitize` disables it)

## CfConsistencyStressTest

Enabled with `--test_cf_consistency=1`. Tests atomicity of writes across multiple column families. Writes the same key-value pair to all specified column families in a single WriteBatch, then uses snapshots to verify consistent visibility.

### Cross-CF Verification

When verifying, the test reads the same key from all column families at a single snapshot. If some column families have the key and others do not (partial visibility), atomicity is violated.

Key Invariant: With `atomic_flush=true`, writes spanning multiple CFs must be all-or-nothing -- no partial visibility across CFs.

### Configuration Restrictions

The `cf_consistency_params` dict in `db_crashtest.py` enforces:

| Restriction | Reason |
|------------|--------|
| `enable_compaction_filter=0` | Incompatible with snapshot-based verification |
| `inplace_update_support=0` | Incompatible with snapshot-based verification |
| `ingest_external_file_one_in=0` | Not implemented for CF consistency |
| `verify_iterator_with_expected_state_one_in=0` | Not implemented |

### State Tracking

`IsStateTracked()` returns `false`. Verification is done by cross-CF comparison rather than against external expected state. An atomic batch ID counter (`batch_id_`) provides unique values for each write.

## MultiOpsTxnsStressTest

Enabled with `--test_multi_ops_txns=1`. Simulates a relational table with primary and secondary indexes, testing multi-operation transactions that maintain referential integrity.

### Table Schema

The emulated table is equivalent to:

```
CREATE TABLE t1 (a INT PRIMARY KEY, b INT, c INT, KEY(c))
```

Primary index key: `| index_id | BigEndian(a) |` with value `| b | c |`. Secondary index key: `| index_id | BigEndian(c) | BigEndian(a) |` with value `| crc32 |`.

### Transaction Types

| Operation | SQL Equivalent | Steps |
|-----------|---------------|-------|
| Primary key update | `UPDATE t1 SET a=3 WHERE a=2` | GetForUpdate on old and new primary keys, Delete old, Put new, SingleDelete old secondary, Put new secondary |
| Secondary key update | `UPDATE t1 SET c=3 WHERE c=2` | Seek secondary index, GetForUpdate primary, Put updated primary, SingleDelete old secondary, Put new secondary |
| Point lookup | `SELECT * FROM t1 WHERE a=?` | Get primary key, verify secondary index consistency |
| Range scan | `SELECT * FROM t1 WHERE c BETWEEN ? AND ?` | Iterator scan on secondary index, verify primary-secondary consistency |

### Configuration Restrictions

The `multiops_txn_params` dict significantly restricts the operation mix: `writepercent=0`, `delpercent=0`, `delrangepercent=0`, `customopspercent=80`, `readpercent=5`, `iterpercent=15`. All write operations are performed through custom transaction operations, not through the standard Put/Delete paths.

Additional restrictions include `column_families=1` (primary and secondary indexes share the default CF), `use_txn=1` (TransactionDB required), and various incompatible features disabled.
