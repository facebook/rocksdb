# Flow Control and Write Stalls

**Files:** `db/write_controller.h`, `db/write_controller.cc`, `include/rocksdb/write_buffer_manager.h`, `db/write_buffer_manager.cc`, `db/column_family.cc`, `db/db_impl/db_impl_write.cc`

## Overview

RocksDB uses back-pressure mechanisms to prevent the write rate from exceeding compaction and flush capacity. Two complementary subsystems coordinate flow control:

1. **WriteController**: Per-DB, triggered by compaction lag (L0 file count, pending compaction bytes, immutable memtable count)
2. **WriteBufferManager**: Optionally shared across DB instances, triggered by total memtable memory usage

## Write Stall Conditions

`ColumnFamilyData::RecalculateWriteStallConditions()` (see `db/column_family.cc`) evaluates three categories of conditions, checked in priority order:

### Stop Conditions (writes fully blocked)

| Condition | Threshold | Option |
|-----------|-----------|--------|
| Too many immutable memtables | `num_unflushed >= max_write_buffer_number` | `max_write_buffer_number` in `ColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) |
| Too many L0 files | `num_l0_files >= level0_stop_writes_trigger` | `level0_stop_writes_trigger` in `ColumnFamilyOptions` |
| Too many pending compaction bytes | `compaction_needed_bytes >= hard_pending_compaction_bytes_limit` | `hard_pending_compaction_bytes_limit` in `ColumnFamilyOptions` |

### Delay Conditions (writes rate-limited)

| Condition | Threshold | Option |
|-----------|-----------|--------|
| Near-full immutable memtables | `max_write_buffer_number > 3 && num_unflushed >= max_write_buffer_number - 1 && num_unflushed - 1 >= min_write_buffer_number_to_merge` | Same as above |
| L0 file slowdown | `num_l0_files >= level0_slowdown_writes_trigger` | `level0_slowdown_writes_trigger` in `ColumnFamilyOptions` |
| Soft pending compaction bytes | `compaction_needed_bytes >= soft_pending_compaction_bytes_limit` | `soft_pending_compaction_bytes_limit` in `ColumnFamilyOptions` |

### Compaction Pressure (increased parallelism, no stall)

When conditions are elevated but below stall thresholds, a `CompactionPressureToken` increases compaction thread parallelism. This is triggered when L0 files reach a speedup threshold (`min(2 * level0_file_num_compaction_trigger, trigger + (slowdown_trigger - trigger) / 4)`) or when pending compaction bytes exceed a threshold derived from bottommost size, `max_bytes_for_level_base`, and `soft_pending_compaction_bytes_limit / 4`.

## WriteController

`WriteController` (see `db/write_controller.h`) manages RAII token-based flow control. All methods require holding `db_mutex_`.

### Token Types

| Token | Counter | Effect |
|-------|---------|--------|
| `StopWriteToken` | `total_stopped_` | Full write stop; writers wait on `bg_cv_` |
| `DelayWriteToken` | `total_delayed_` | Rate-limited writes via credit-based delay |
| `CompactionPressureToken` | `total_compaction_pressure_` | Increases compaction parallelism |

Tokens are held by `ColumnFamilyData` and automatically released (RAII destructor decrements the counter) when compaction catches up and `RecalculateWriteStallConditions()` replaces or clears the token.

### Rate Limiting Algorithm

`WriteController::GetDelay()` implements a credit-based rate limiter:

Step 1 - If writes are stopped or no delay is needed, return 0.

Step 2 - If sufficient credit exists (`credit_in_bytes_ >= num_bytes`), deduct and return 0.

Step 3 - Refill credit based on elapsed time since last refill (every 1 ms): `credit += elapsed_seconds * delayed_write_rate_`.

Step 4 - If credit is still insufficient, compute delay: `bytes_over_budget / delayed_write_rate_ * 1000000` microseconds.

The `delayed_write_rate_` is dynamically adjusted by `RecalculateWriteStallConditions()` via `SetupDelay()` (see `db/column_family.cc`) using these ratio constants:

| Constant | Value | When Applied |
|----------|-------|-------------|
| `kNearStopSlowdownRatio` | 0.6 | Near stop or just exited stop: aggressive slowdown |
| `kIncSlowdownRatio` | 0.8 | Compaction debt stayed same or increased |
| `kDecSlowdownRatio` | 1.25 | Compaction debt decreased: speed up |
| `kDelayRecoverSlowdownRatio` | 1.4 | Recovery from delay to normal: reward |

The minimum rate is 16 KB/s. The maximum is `max_delayed_write_rate` (default 32 MB/s, see `WriteController` constructor in `db/write_controller.h`). When recovering from delay, the low-priority rate limiter is also set to 1/4 of the delayed write rate.

### Delay Execution

`DBImpl::DelayWrite()` (see `db/db_impl/db_impl_write.cc`) executes the delay:

**For rate-limited writes:** The leader calls `BeginWriteStall()` on the write thread, releases `db_mutex_`, and sleeps in 1 ms intervals, checking every iteration whether delay is still needed. After sleep, re-acquires `db_mutex_` and calls `EndWriteStall()`.

**For stopped writes:** The leader blocks on `bg_cv_` (condvar on `db_mutex_`) until `total_stopped_` drops to zero, checking for shutdown and background errors between waits.

**For `no_slowdown` writes:** Returns `Status::Incomplete("Write stall")` immediately instead of blocking.

## WriteBufferManager

`WriteBufferManager` (see `include/rocksdb/write_buffer_manager.h`) tracks total memtable memory across all DB instances sharing the same manager.

### Memory Counters

| Counter | Updated By | Description |
|---------|-----------|-------------|
| `memory_used_` | `ReserveMem()` / `FreeMem()` | Total bytes in active + being-flushed memtables |
| `memory_active_` | `ReserveMem()` / `ScheduleFreeMem()` | Bytes in mutable (active) memtables only |

`ScheduleFreeMem()` is called when a memtable transitions from mutable to immutable (starts flushing), moving its bytes from `memory_active_` to the being-flushed category.

### Flush Trigger

`ShouldFlush()` returns `true` when:

1. Active memtable memory exceeds `mutable_limit_` (which is `buffer_size * 7/8`), or
2. Total memory usage exceeds `buffer_size` AND active memory is at least half of `buffer_size`

The second condition prevents triggering flushes when most memory is already being flushed (flush-in-progress), since additional flushes would not help reduce memory faster.

When `ShouldFlush()` returns `true`, `PreprocessWrite()` calls `HandleWriteBufferManagerFlush()`, which selects the column family with the oldest mutable memtable (lowest creation sequence) and switches its memtable. With `atomic_flush`, all column families are flushed together.

### Stall Trigger

`ShouldStall()` returns `true` when:
- `allow_stall` was set to `true` at construction
- `memory_usage() >= buffer_size_`

When stall triggers, `WriteBufferManagerStallWrites()` calls `write_thread_.BeginWriteStall()`, then blocks the current thread via `write_buffer_manager_->BeginWriteStall(wbm_stall)` until memory drops below the threshold.

`FreeMem()` is called after flush completion. If the stall is active and memory usage drops below the threshold, `MaybeEndWriteStall()` signals all blocked DB instances to resume.

### Cache-Aware Mode

When a `Cache` is provided to the `WriteBufferManager` constructor, memtable memory is accounted against the block cache via `CacheReservationManager`. This allows memtable and block cache memory to share a unified budget, preventing the combined usage from exceeding the cache capacity.

## PreprocessWrite Flow

`DBImpl::PreprocessWrite()` (see `db/db_impl/db_impl_write.cc`) runs before every write group and handles flow control in this order:

Step 1 - Check for background errors (DB stopped).

Step 2 - If total WAL size exceeds `max_total_wal_size` (see `DBOptions` in `include/rocksdb/options.h`; defaults to `4 * max_total_in_memory_state` if not explicitly set), switch WAL to allow old WAL files to be reclaimed. Only applies when multiple column families are active.

Step 3 - If `WriteBufferManager::ShouldFlush()`, flush the oldest CF's memtable.

Step 4 - If trim history scheduler is non-empty, trim old memtable history.

Step 5 - If flush scheduler is non-empty, schedule pending flushes.

Step 6 - If `WriteController` needs delay or stop, execute `DelayWrite()`.

Step 7 - If `WriteBufferManager::ShouldStall()`, block until memory drops.

Step 8 - Prepare WAL context (sync coordination, writer pointer).

## Monitoring Write Stalls

Write stalls are tracked via several statistics and properties:

| Metric | Description |
|--------|-------------|
| `STALL_MICROS` | Total time spent stalled (ticker) |
| `WRITE_STALL` | Write stall duration histogram |
| `L0_FILE_COUNT_LIMIT_STOPS` | Count of L0 stop events |
| `L0_FILE_COUNT_LIMIT_DELAYS` | Count of L0 delay events |
| `MEMTABLE_LIMIT_STOPS` / `MEMTABLE_LIMIT_DELAYS` | Memtable-related stall counts |
| `PENDING_COMPACTION_BYTES_LIMIT_STOPS` / `..._DELAYS` | Pending bytes stall counts |

These can be accessed via `DB::GetProperty("rocksdb.cfstats")` or `DB::GetMapProperty("rocksdb.cfstats-no-file-histogram")`.
