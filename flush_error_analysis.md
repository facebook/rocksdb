# RocksDB Flush Failure, Background Error, and Auto-Recovery Analysis

## 1. Does RocksDB set a background error when flush fails?

**Yes.** When a flush fails, RocksDB sets a background error whose severity depends on the error type and the `paranoid_checks` setting (see `db/error_handler.cc:46-61`):

| Error Type | paranoid_checks=true | paranoid_checks=false |
|---|---|---|
| `IOError::NoSpace` | **kHardError** (blocks writes) | kNoError |
| `IOError::IOFenced` | **kFatalError** | **kFatalError** |
| Generic `IOError` | **kFatalError** | kNoError |

When severity >= `kHardError`, `is_db_stopped_` is set to `true`, blocking all writes (`error_handler.cc:832-834`):

```cpp
if (bg_error_.severity() >= Status::Severity::kHardError) {
  is_db_stopped_.store(true, std::memory_order_release);
}
```

## 2. Is there auto-retry for transient/retryable errors?

**Yes**, RocksDB has a background recovery mechanism controlled by two `DBOptions`:

- **`max_bgerror_resume_count`** — max number of retry attempts
- **`bgerror_resume_retry_interval`** — wait interval (microseconds) between retries

When a retryable IO error occurs during flush, RocksDB spawns a recovery thread (`RecoverFromRetryableBGIOError` in `error_handler.cc:736-823`) that:

1. Calls `db_->ResumeImpl()` which re-schedules the flush
2. If the error is still retryable, waits `bgerror_resume_retry_interval` then retries
3. Repeats up to `max_bgerror_resume_count` times
4. Stops early on non-retryable errors or DB shutdown

### What qualifies as "retryable" for auto-recovery?

From `error_handler.cc:470-472`, an error enters the retryable recovery path if:

```cpp
if (bg_io_err.subcode() != IOStatus::SubCode::kNoSpace &&
    (bg_io_err.GetScope() == IOStatus::IOErrorScope::kIOErrorScopeFile ||
     bg_io_err.GetRetryable()))
```

Two conditions qualify:
1. **File-scope IO errors** (`IOErrorScope::kIOErrorScopeFile`) — automatically treated as retryable
2. **Any IOError with `retryable_` flag set to `true`** — explicitly marked by the filesystem implementation

### Retryable error classification by operation (`error_handler.cc:486-530`):

- **Compaction retryable errors** → Mapped to soft error, compaction reschedules itself, NO auto-resume thread
- **Flush with no WAL** → Mapped to soft error, sets `soft_error_no_bg_work_ = true`, ENABLES auto-resume
- **All other retryable errors** → Hard error, ENABLES auto-resume

## 3. Does successful recovery clear the background error?

**Yes.** When `ResumeImpl()` succeeds (`error_handler.cc:601-631`):

- `is_db_stopped_` is set back to `false` (writes unblocked)
- `bg_error_` is cleared to `Status::OK()`
- Listeners are notified via `NotifyOnErrorRecoveryEnd`
- Background work (compaction, flush) resumes normally

```cpp
Status ErrorHandler::ClearBGError() {
  if (recovery_error_.ok()) {
    is_db_stopped_.store(false, std::memory_order_release);
    bg_error_ = Status::OK();
    recovery_error_ = IOStatus::OK();
    recovery_in_prog_ = false;
    soft_error_no_bg_work_ = false;
    EventHelpers::NotifyOnErrorRecoveryEnd(
        db_options_.listeners, old_bg_error, bg_error_, db_mutex_);
  }
  return recovery_error_;
}
```

## 4. What errors are retryable in the default Posix implementation?

The retryable flag is a **property set by the `FileSystem`/`Env` implementation**, not something RocksDB decides automatically based on error codes. It's a boolean on `IOStatus` (`include/rocksdb/io_status.h:52`):

```cpp
void SetRetryable(bool retryable) { retryable_ = retryable; }
```

In the default Posix implementation (`env/io_posix.cc:60-78`), the **only** errno marked as retryable is:

- **`ENOSPC`** (No space left on device) — but this is handled separately by `SstFileManager`, not the general retry path

All other IO errors (`EIO`, `EACCES`, `ESTALE`, `ETIMEDOUT`, etc.) are **not** marked as retryable by default.

## 5. Implications for remote/network filesystems

**The default Posix `Env` does NOT mark transient network errors (like `EIO`, `ETIMEDOUT`, `ECONNRESET`, etc.) as retryable.** This means out of the box, a transient network filesystem error during flush will be classified as a **fatal error** (with `paranoid_checks=true`), with **no auto-recovery**.

To get auto-recovery for transient network errors, a **custom `FileSystem` implementation** is needed that:

1. Identifies transient network errors (e.g., `EIO`, `ETIMEDOUT`, `EHOSTUNREACH`)
2. Calls `IOStatus::SetRetryable(true)` on those errors
3. Optionally sets the error scope to `kIOErrorScopeFile` for file-level errors

This is by design — RocksDB leaves it to the filesystem layer to decide which errors are transient, since only the filesystem implementation knows the semantics of the underlying storage.

## 6. NoSpace errors have special handling

`ENOSPC` errors are handled separately (`error_handler.cc:591-599`):

- Delegated to `SstFileManager::StartErrorRecovery()`
- SFM polls for disk space availability
- When space becomes available, SFM triggers recovery

## 7. Relevant metrics

- `ERROR_HANDLER_BG_ERROR_COUNT` — Total background errors
- `ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT` — Retryable errors
- `ERROR_HANDLER_AUTORESUME_COUNT` — Auto-resume attempts
- `ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT` — Successful recoveries
- `ERROR_HANDLER_AUTORESUME_RETRY_COUNT` — Number of retry attempts

## Key source files

- `db/error_handler.cc` — Main error handling and recovery logic
- `db/error_handler.h` — ErrorHandler class definition
- `env/io_posix.cc` — Default Posix IO error mapping
- `include/rocksdb/io_status.h` — IOStatus with retryable flag
- `db/error_handler_fs_test.cc` — Tests for error recovery
