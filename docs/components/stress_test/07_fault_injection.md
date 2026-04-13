# Fault Injection

**Files:** `utilities/fault_injection_fs.h`, `db_stress_tool/db_stress_tool.cc`, `db_stress_tool/db_stress_driver.cc`, `db_stress_tool/db_stress_env_wrapper.h`

## Overview

The stress test framework uses `FaultInjectionTestFS` (see `utilities/fault_injection_fs.h`) to wrap the base filesystem and inject errors at configurable probabilities. This tests RocksDB's error handling, retry logic, and recovery behavior under I/O failure conditions.

## Error Types

### Read Errors

| Flag | Target | Description |
|------|--------|-------------|
| `--read_fault_one_in` | `Read()` calls | Fails read operations with probability 1/N |
| `--metadata_read_fault_one_in` | `GetFileSize()`, `GetFileModificationTime()`, `FileExists()`, `GetChildren()`, `IsDirectory()`, and other metadata read operations | Fails metadata read operations |

Important: Read fault injection requires debug builds because it relies on sync points (`IGNORE_STATUS_IF_ERROR`) to distinguish faults that are not expected to cause test failure. In release mode, the process exits if these flags are nonzero.

### Write Errors

| Flag | Target | Description |
|------|--------|-------------|
| `--write_fault_one_in` | `Write()` calls | Fails write operations with probability 1/N |
| `--metadata_write_fault_one_in` | `Close()`, `RenameFile()`, `LinkFile()`, `DeleteFile()`, directory `Fsync()` | Fails metadata write operations |

The `--exclude_wal_from_write_fault_injection` flag (default: false) can exclude WAL files from write fault injection, allowing SST write failures while keeping WAL writes reliable.

### DB-Open Errors

| Flag | Target | Description |
|------|--------|-------------|
| `--open_read_fault_one_in` | Reads during DB open | Fails reads during the open sequence |
| `--open_write_fault_one_in` | Writes during DB open | Fails writes during open |
| `--open_metadata_read_fault_one_in` | Metadata reads during open | Fails metadata reads during open |
| `--open_metadata_write_fault_one_in` | Metadata writes during open | Fails metadata writes during open |

DB-open fault injection is implemented by enabling faults before the `DB::Open()` call, then disabling them after open succeeds. If open fails due to an injected fault, the test retries with a clean filesystem state.

## Unsynced Data Loss Simulation

### sync_fault_injection

When `--sync_fault_injection=1`, the fault injection layer simulates power failure by tracking unsynced data:

Step 1: All write operations append data but mark it as "unsynced" internally. Step 2: Only explicit `Sync()` or `Fsync()` calls mark data as durable. Step 3: On simulated crash (process restart), unsynced data is discarded. Step 4: The database reopens and must recover correctly from only the synced data.

The implementation uses two key methods in `FaultInjectionTestFS`:

- `DropUnsyncedFileData()`: Truncates all files to their last synced offset, simulating data loss of unsynced writes.
- `DeleteFilesCreatedAfterLastDirSync()`: Removes files whose creation was not followed by a directory sync, simulating file creation loss. For files that overwrote existing files, the original content is restored rather than deleted.

### Interaction with Expected State

When unsynced data loss is possible (`sync_fault_injection` or `disable_wal` or `manual_wal_flush_one_in`), the expected state system must account for writes that may not survive a crash. `StressTest::MightHaveUnsyncedDataLoss()` returns true in these cases, and the trace-and-replay mechanism in `FileExpectedStateManager` uses sequence numbers to determine which operations survived.

## Error Severity

The `--inject_error_severity` flag (default: 1) controls whether injected errors are retryable or permanent:

| Value | Error Type | DB Behavior |
|-------|-----------|-------------|
| 1 | Retryable (soft error) | DB retries the operation and should eventually succeed |
| 2 | Data loss (non-retryable with has_data_loss=true) | DB may enter read-only mode; data loss is expected |

Key Invariant: The DB must not lose acknowledged data on retryable errors. Only errors with the data-loss flag set are allowed to cause data loss.

Note: The stress test supports both retryable and data-loss error injection via `--inject_error_severity`. When severity is 2, injected errors set `has_data_loss=true`, and the DB is expected to handle data loss accordingly.

### Write Failure Visibility

A subtle aspect of IO error injection: a write that returns an error may still be visible to subsequent reads. For example, if a WAL write and memtable insert succeed but the subsequent manifest write fails, the write returns a non-ok status, yet the data is readable from the memtable. The returned error status does not indicate which step failed. The expected state system must account for this by treating failed writes as "may or may not exist" rather than "definitely does not exist."

### Error Recovery

When `--error_recovery_with_no_fault_injection=1`, the test disables fault injection during error recovery, allowing the DB to recover without additional injected failures. This tests the recovery path in isolation.

## Fault Injection Lifecycle

The fault injection layer is managed carefully during the stress test lifecycle:

Step 1: At startup, `FaultInjectionTestFS` is created and set to "direct writable" mode, bypassing all fault injection during initial DB open. Step 2: After DB open and crash-recovery verification, `RunStressTestImpl()` disables direct writable mode and configures fault injection parameters. Step 3: During operations, faults are injected according to the configured probabilities. Step 4: When the process is killed and restarted, the fault injection layer is re-created in direct writable mode for recovery, then re-enabled after recovery verification.

### Crash Callback

A crash callback is registered via `port::RegisterCrashCallback()` that calls `FaultInjectionTestFS::PrintRecentInjectedErrors()`. When the process crashes (SIGABRT, SIGSEGV, etc.), this prints recently injected errors to help diagnose whether the crash was caused by fault injection or a genuine bug.

### Fault Injection Log

The fault injection layer writes detailed error logs to a file (configured via `SetInjectedErrorLogPath()`). The log path is set outside the DB directory (in `$TEST_TMPDIR` or `/tmp`) so it survives DB reopen (which cleans untracked files) and is included in test artifacts for post-failure analysis.

## Filter Block Read Errors

Read errors during filter block reads require special handling because filter lookups are advisory -- a filter error should not cause a false-positive test failure. The `SharedState::ignore_read_error` thread-local variable is set via a sync point callback when a filter block read error is injected. Operations that encounter this flag treat the result as ambiguous rather than a verification failure.
