# RocksDB Threading Model

RocksDB uses a sophisticated multi-threaded architecture that separates foreground (user) threads from background workers, coordinates writes through a leader/follower pattern, and enables lock-free reads via ref-counted snapshots. This document explains the threading model, synchronization mechanisms, and performance optimizations.

## Table of Contents
- [Overview](#overview)
- [Thread Pools](#thread-pools)
- [Background Work Scheduling](#background-work-scheduling)
- [WriteThread: Group Commit](#writethread-group-commit)
- [Foreground vs Background Threads](#foreground-vs-background-threads)
- [Mutex Hierarchy](#mutex-hierarchy)
- [Lock-Free Read Path](#lock-free-read-path)
- [Concurrent Memtable Writes](#concurrent-memtable-writes)
- [Subcompactions](#subcompactions)
- [Thread Safety Guarantees](#thread-safety-guarantees)
- [Shutdown and Cleanup](#shutdown-and-cleanup)

## Overview

RocksDB's threading architecture achieves high concurrency through:

1. **Priority-based thread pools** for background work (flush, compaction)
2. **Group commit** via WriteThread leader/follower pattern
3. **Lock-free reads** using ref-counted SuperVersion snapshots
4. **Careful mutex hierarchy** to prevent deadlocks
5. **Adaptive synchronization** (spin → yield → block) based on contention

```
FOREGROUND THREADS (User threads: Put/Get/Scan)
  |                                    |
  | Writes                             | Reads
  v                                    v
WriteThread                    SuperVersion
(Group Commit)                 (Lock-Free Ref)
  |
  | WAL + Memtable
  v
BACKGROUND THREADS (3 pools)
  - HIGH_PRIORITY (Flush)
  - LOW_PRIORITY (Compaction)
  - BOTTOM_PRIORITY (Bottom Compact)
```

## Thread Pools

RocksDB uses three priority-based thread pools for background work, implemented in `util/threadpool_imp.cc`.

### Pool Structure

Each thread pool maintains:

```cpp
// util/threadpool_imp.cc:45-159 (key fields, simplified)
struct ThreadPoolImpl::Impl {
  Env::Priority priority_;          // Pool priority level
  int total_threads_limit_;         // Max threads
  std::atomic_uint queue_len_;      // Queue depth
  int reserved_threads_;            // Reserved capacity
  int num_waiting_threads_;         // Idle threads
  BGQueue queue_;                   // Work queue (deque<BGItem>)
  std::vector<port::Thread> bgthreads_;
};
```

### Priority Levels

**HIGH_PRIORITY** — Flush operations
- Converts immutable memtables to L0 SST files
- Critical for write path (prevents stalling)
- Typically 1-2 threads

**LOW_PRIORITY** — Regular compaction
- Merges SST files across levels
- Most compaction work runs here
- Scales with CPU cores

**BOTTOM_PRIORITY** — Bottommost compaction
- Special handling for last level
- May use different compression
- Lower I/O priority

### Thread Main Loop

```cpp
// util/threadpool_imp.cc:215-313
void ThreadPoolImpl::Impl::BGThread(size_t thread_id) {
  while (true) {
    std::unique_lock<std::mutex> lock(mu_);
    num_waiting_threads_++;

    // Wait for work or termination
    while (!exit_all_threads_ && !IsLastExcessiveThread(thread_id) &&
           (queue_.empty() || IsExcessiveThread(thread_id) ||
            num_waiting_threads_ <= reserved_threads_)) {
      bgsignal_.wait(lock);
    }
    num_waiting_threads_--;

    if (exit_all_threads_) {
      if (!wait_for_jobs_to_complete_ || queue_.empty()) break;
    } else if (IsLastExcessiveThread(thread_id)) {
      // Terminate excessive threads in reverse creation order
      auto& terminating_thread = bgthreads_.back();
      terminating_thread.detach();
      bgthreads_.pop_back();
      break;
    }

    auto func = std::move(queue_.front().function);
    queue_.pop_front();
    lock.unlock();

    func();  // Execute background work
  }
}
```

**⚠️ INVARIANT**: Threads scale dynamically up to `total_threads_limit_`. Excessive threads terminate in **reverse creation order** for predictable scaling.

### I/O and CPU Priority Control

Background threads lower their priority to avoid impacting foreground operations:

```cpp
// util/threadpool_imp.cc:285-303 (Linux only)
#ifdef OS_LINUX
if (decrease_io_priority) {
  // Set to IOPRIO_CLASS_IDLE (lowest priority)
  syscall(SYS_ioprio_set, 1, 0, IOPRIO_PRIO_VALUE(3, 0));
  low_io_priority = true;
}
#endif
```

CPU priority is controlled via `port::SetCpuPriority()`.

## Background Work Scheduling

### Env::Schedule Interface

Background work is submitted via `Env::Schedule`:

```cpp
// include/rocksdb/env.h
void Schedule(void (*function)(void* arg), void* arg,
              Priority pri = LOW,
              void* tag = nullptr,
              void (*unschedFunction)(void* arg) = nullptr);
```

Implementation routes to appropriate thread pool:

```cpp
// util/threadpool_imp.cc:401-431
void Submit(std::function<void()>&& schedule,
            std::function<void()>&& unschedule, void* tag) {
  std::lock_guard<std::mutex> lock(mu_);

  if (exit_all_threads_) return;

  StartBGThreads();  // Create threads if needed

  queue_.push_back(BGItem());
  auto& item = queue_.back();
  item.tag = tag;
  item.function = std::move(schedule);
  item.unschedFunction = std::move(unschedule);

  queue_len_.store(queue_.size(), std::memory_order_relaxed);

  if (!HasExcessiveThread()) {
    bgsignal_.notify_one();
  } else {
    WakeUpAllThreads();  // Wake all to avoid waking thread to terminate
  }
}
```

### Background Job Options

**max_background_jobs** — Total concurrency budget
- Controls combined flush + compaction parallelism
- Split internally between pools
- Default: 2

**max_background_flushes** (deprecated) — Flush limit
- Prefer `max_background_jobs` for new code

**max_background_compactions** (deprecated) — Compaction limit
- Prefer `max_background_jobs` for new code

**Modern approach**: Configure only `max_background_jobs`. RocksDB internally allocates 1-2 threads for flush, remaining for compaction.

### Flush Scheduling Example

```cpp
// db/db_impl/db_impl_compaction_flush.cc:145-386
Status DBImpl::FlushMemTableToOutputFile(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    bool* made_progress, JobContext* job_context,
    FlushReason flush_reason, ...) {
  mutex_.AssertHeld();

  FlushJob flush_job(...);
  flush_job.PickMemTable();

  // Execute flush (releases/reacquires mutex_)
  s = flush_job.Run(&logs_with_prep_tracker_, &file_meta, ...);

  // Install new SuperVersion
  InstallSuperVersionAndScheduleWork(cfd, superversion_context);
}
```

**⚠️ INVARIANT**: Background jobs acquire `mutex_`, prepare work, **release `mutex_`** for I/O, then **reacquire `mutex_`** for MANIFEST updates. This prevents blocking foreground operations during I/O.

## WriteThread: Group Commit

WriteThread implements a leader/follower pattern to batch writes, amortizing the cost of WAL fsync and MANIFEST updates across multiple operations.

### Writer States

```cpp
// db/write_thread.h:32-82
enum State : uint8_t {
  STATE_INIT = 1,                       // Waiting to join
  STATE_GROUP_LEADER = 2,               // Elected leader, builds group
  STATE_MEMTABLE_WRITER_LEADER = 4,     // Memtable write leader
  STATE_PARALLEL_MEMTABLE_WRITER = 8,   // Parallel memtable writer
  STATE_COMPLETED = 16,                 // Write completed
  STATE_LOCKED_WAITING = 32,            // Blocking wait
  STATE_PARALLEL_MEMTABLE_CALLER = 64,  // Parallel write coordinator
};
```

### Group Formation Flow

```
Writer enters
  |
  v
CAS on newest_writer_
  |
  +-- First? -> Become leader (loop back to CAS for next group)
  |
  +-- Not first -> Wait for state change
                     |
                     v
                   Leader scans linked list, builds group
                     |
                     v
                   Leader executes WAL write
                     |
                     v
                   Wake followers / hand off
```

**⚠️ INVARIANT**: Writers link into `newest_writer_` via **atomic CAS**. First writer becomes leader. Leader scans the linked list **forwards** (oldest to newest via `link_newer`) to build the write group. Incompatible writers are unlinked into a temporary list and grafted back after the group.

### Group Building Logic

```cpp
// db/write_thread.cc:440-569
size_t WriteThread::EnterAsBatchGroupLeader(
    Writer* leader, WriteGroup* write_group) {
  size_t size = WriteBatchInternal::ByteSize(leader->batch);
  size_t max_size = max_write_batch_group_size_bytes;

  CreateMissingNewerLinks(newest_writer);

  // Incompatible writers are unlinked into a separate r_list
  Writer* rb = nullptr;  // r_list begin
  Writer* re = nullptr;  // r_list end

  Writer* w = leader;
  while (w != newest_writer) {
    w = w->link_newer;

    // Check compatibility
    if ((w->sync && !leader->sync) ||
        (w->no_slowdown != leader->no_slowdown) ||
        (w->disable_wal != leader->disable_wal) ||
        (w->protection_bytes_per_key != leader->protection_bytes_per_key) ||
        (w->rate_limiter_priority != leader->rate_limiter_priority) ||
        (w->batch == nullptr) ||
        (w->callback != nullptr && !w->callback->AllowWriteBatching()) ||
        (size + WriteBatchInternal::ByteSize(w->batch) > max_size) ||
        (leader->ingest_wbwi || w->ingest_wbwi)) {
      // Unlink from main list and append to r_list
      w->link_older->link_newer = w->link_newer;
      if (w->link_newer) w->link_newer->link_older = w->link_older;
      // ... insert into r_list ...
    } else {
      // Add to write group
      w->write_group = write_group;
      size += WriteBatchInternal::ByteSize(w->batch);
      write_group->last_writer = w;
      write_group->size++;
    }
  }

  // Graft r_list back after write_group end (preserves arrival order
  // for incompatible writers so they can become next leader)
  if (rb != nullptr) { /* ... CAS to re-attach ... */ }

  return size;
}
```

**Compatibility criteria**:
- `sync`: a sync writer cannot join a non-sync leader's group (but non-sync writers can join a sync group)
- `no_slowdown` must match
- `disable_wal` must match
- `protection_bytes_per_key` must match
- `rate_limiter_priority` must match
- `batch` must not be nullptr
- `callback` must allow batching (if non-null)
- `ingest_wbwi` must not be set
- Total batch size ≤ `max_write_batch_group_size_bytes`

### Adaptive Waiting: Spin → Yield → Block

WriteThread uses a three-phase waiting strategy optimized for low latency under low contention:

```cpp
// db/write_thread.cc:64-210
uint8_t WriteThread::AwaitState(Writer* w, uint8_t goal_mask,
                                AdaptationContext* ctx) {
  // Phase 1: Busy spin with pause (~1.4 µs, 200 iterations)
  for (uint32_t tries = 0; tries < 200; ++tries) {
    state = w->state.load(std::memory_order_acquire);
    if ((state & goal_mask) != 0) return state;
    port::AsmVolatilePause();  // PAUSE instruction
  }

  // Phase 2: Adaptive yield (up to max_yield_usec_, default 100µs)
  if (max_yield_usec_ > 0) {
    auto spin_begin = std::chrono::steady_clock::now();
    size_t slow_yield_count = 0;

    auto iter_begin = spin_begin;

    while ((iter_begin - spin_begin) <=
           std::chrono::microseconds(max_yield_usec_)) {
      std::this_thread::yield();
      state = w->state.load(std::memory_order_acquire);
      if ((state & goal_mask) != 0) return state;

      // Track slow yields (involuntary context switch)
      auto now = std::chrono::steady_clock::now();
      if (now == iter_begin ||
          now - iter_begin >=
          std::chrono::microseconds(slow_yield_usec_)) {
        if (++slow_yield_count >= kMaxSlowYieldsWhileSpinning)
          break;
      }
      iter_begin = now;
    }

    // Update yield_credit with exponential decay
    yield_credit = yield_credit - (yield_credit / 1024) +
                   (would_spin_again ? 1 : -1) * 131072;
  }

  // Phase 3: Blocking wait on condition variable
  if ((state & goal_mask) == 0) {
    state = BlockingAwaitState(w, goal_mask);
  }

  return state;
}
```

**Optimization**: `yield_credit` tracks yield effectiveness with exponential decay. If yields repeatedly succeed (low contention), continue yielding. If yields become slow (context switches), fall back to blocking faster.

### Pipelined Write

Pipelined write separates WAL write from memtable write to increase throughput:

```cpp
// db/write_thread.h:424,433,437
const bool enable_pipelined_write_;

std::atomic<Writer*> newest_writer_;           // WAL queue
std::atomic<Writer*> newest_memtable_writer_;  // Memtable queue
```

**Flow**:
1. Leader writes WAL
2. Leader links group to `newest_memtable_writer_`
3. Leader wakes next WAL leader
4. **Leader continues** to memtable write (pipelining)

```cpp
// db/write_thread.cc:771-843
if (enable_pipelined_write_) {
  // Complete writers that don't need memtable writes
  for (Writer* w = last_writer; w != leader;) {
    if (!w->ShouldWriteToMemtable()) {
      CompleteFollower(w, write_group);
    }
  }

  // Link remaining to memtable writer queue
  if (write_group.size > 0) {
    if (LinkGroup(write_group, &newest_memtable_writer_)) {
      SetState(write_group.leader, STATE_MEMTABLE_WRITER_LEADER);
    }
  }

  // Wake next WAL writer leader
  SetState(new_leader, STATE_GROUP_LEADER);

  // Current leader continues to memtable write
  AwaitState(leader, STATE_MEMTABLE_WRITER_LEADER |
                     STATE_PARALLEL_MEMTABLE_CALLER |
                     STATE_PARALLEL_MEMTABLE_WRITER |
                     STATE_COMPLETED, ...);
}
```

**Benefit**: WAL and memtable writes overlap, increasing throughput on multi-core systems.

## Foreground vs Background Threads

### Foreground Threads (User Threads)

**Responsibilities**:
- Execute `Put/Get/Delete/Scan` operations
- Write to memtable via WriteThread coordination
- Participate in group commit as leader or follower
- Read via lock-free SuperVersion acquisition

**Key characteristic**: Do **not** hold `mutex_` during I/O operations.

**Example read path**:
```cpp
SuperVersion* sv = cfd->GetThreadLocalSuperVersion(db);
// Read from sv->mem, sv->imm, sv->current (no mutex held)
cfd->ReturnThreadLocalSuperVersion(sv);
```

### Background Threads

**Flush threads** (HIGH_PRIORITY pool):
- Monitor memtable size triggers
- Flush immutable memtables to L0 SST files
- Update MANIFEST via `LogAndApply`

**Compaction threads** (LOW_PRIORITY pool):
- Merge SST files across levels
- Apply compaction filters
- Remove deleted/overwritten keys
- Update MANIFEST

**Bottom compaction threads** (BOTTOM_PRIORITY pool):
- Compact bottommost level
- Special compression settings
- Lower I/O priority

**Coordination**:
```cpp
// db/db_impl/db_impl.cc:472-478
void DBImpl::WaitForBackgroundWork() {
  while (bg_bottom_compaction_scheduled_ ||
         bg_compaction_scheduled_ ||
         bg_flush_scheduled_ ||
         bg_pressure_callback_in_progress_) {
    bg_cv_.Wait();
  }
}
```

## Mutex Hierarchy

RocksDB uses multiple mutexes to reduce contention. **Strict lock ordering** prevents deadlocks.

### Primary Mutexes

**DB Mutex** (`db/db_impl/db_impl.h:1406`):
```cpp
mutable CacheAlignedInstrumentedMutex mutex_;  // Cache-aligned
```

Protects:
- `ColumnFamilyData` structures
- Version management (`current`, `super_version_`)
- Background job scheduling state
- Memtable lists
- MANIFEST updates

**WAL Mutex**:
- `wal_write_mutex_`: Protects writes to WAL (`logs_` and `cur_wal_number_`). With `two_write_queues` it also protects `alive_wal_files_` and `wal_empty_`

**Options Mutex**:
- `options_mutex_`: Protects `SetOptions`/`SetDBOptions`/`CreateColumnFamily` calls, serializing option changes and OPTIONS file writes

**Writer State Mutex** (`db/write_thread.h:278-287`):
```cpp
std::mutex& StateMutex() {
  assert(made_waitable);
  return *static_cast<std::mutex*>(
      static_cast<void*>(&state_mutex_bytes));
}
```

### Lock Ordering Rules

**⚠️ INVARIANT (Critical)**: `mutex_` **MUST** be acquired before `wal_write_mutex_`.

**⚠️ INVARIANT**: `Writer::StateMutex()` is **always last** in lock order. No other mutexes may be acquired while holding `StateMutex()`.

**Typical write path ordering**:
1. Acquire `mutex_` (if switching memtable)
2. Release `mutex_`
3. WriteThread coordination (lock-free or StateMutex)
4. Acquire `wal_write_mutex_` for WAL write
5. Write to memtable (may hold Writer::StateMutex for parallel writes)

**Background flush ordering**:
1. Acquire `mutex_`
2. Select memtables, create `FlushJob`
3. **Release `mutex_`**
4. Execute I/O (flush SST files)
5. **Reacquire `mutex_`** for `LogAndApply` (MANIFEST update)

Violating lock order causes **deadlock**.

## Lock-Free Read Path

RocksDB achieves lock-free reads through **ref-counted SuperVersion snapshots**.

### SuperVersion Structure

```cpp
// db/column_family.h:206-275
struct SuperVersion {
  ColumnFamilyData* cfd;
  ReadOnlyMemTable* mem;           // Current memtable
  MemTableListVersion* imm;        // Immutable memtables
  Version* current;                // SST file list
  MutableCFOptions mutable_cf_options;
  uint64_t version_number;
  WriteStallCondition write_stall_condition;
  std::string full_history_ts_low; // Effective UDT ts low for this SV
  std::shared_ptr<const SeqnoToTimeMapping> seqno_to_time_mapping;

  SuperVersion* Ref();             // Increment refs
  bool Unref();                    // Decrement, returns true if cleanup needed
  void Cleanup();                  // Unref mem/imm/current

 private:
  std::atomic<uint32_t> refs;      // Reference count

  // Sentinel values for thread-local cache
  static void* const kSVInUse;     // = &dummy (thread using SuperVersion)
  static void* const kSVObsolete;  // = nullptr (SuperVersion outdated)
};
```

**⚠️ INVARIANT**: SuperVersion **must** have `refs > 0` while any thread accesses `mem`, `imm`, or `current`. This prevents use-after-free in concurrent reads.

### Lock-Free Acquisition

```cpp
// db/column_family.h:478-487
// Thread-safe: get referenced SuperVersion
SuperVersion* GetReferencedSuperVersion(DBImpl* db);

// Thread-safe: get from thread-local cache or reference current
SuperVersion* GetThreadLocalSuperVersion(DBImpl* db);

bool ReturnThreadLocalSuperVersion(SuperVersion* sv);
```

**Thread-local cache**:
- Each thread caches its SuperVersion pointer
- Cache hit: reuse without atomic ref-count operation
- Cache miss: acquire reference to current SuperVersion
- Sentinel values mark cache state (defined in `SuperVersion` as static members):
  - `kSVInUse` (`= &dummy`): Thread using SuperVersion
  - `kSVObsolete` (`= nullptr`): SuperVersion outdated, refresh needed

**Performance benefit**: Thread-local cache eliminates atomic ref-count operations on read path under steady state.

### SuperVersion Lifecycle

```
Flush/Compaction completes
  |
  v
New SuperVersion created (refs=1)
  |
  v
Installed as cfd->super_version_ (under mutex_)
  |
  v
Old SuperVersion->Unref()
  |
  +-- refs > 0? -> Kept alive (readers still using)
  |
  +-- refs == 0? -> Cleanup queued
                      |
                      v
                    Unrefs mem/imm/current
                    (may trigger further cleanup)
```

**Installation**:
```cpp
// db/column_family.h:496-500
void InstallSuperVersion(SuperVersionContext* sv_context,
                         InstrumentedMutex* db_mutex, ...);
```

**Key mechanism**: Old SuperVersion remains valid until all readers release references. Cleanup happens asynchronously when `refs` drops to 0.

## Concurrent Memtable Writes

By default, only the group commit leader writes to the memtable. Enabling `allow_concurrent_memtable_write` allows **parallel memtable insertion** from multiple group members.

### Option

```cpp
// db/write_thread.h:420-421
const bool allow_concurrent_memtable_write_;
```

Set via `DBOptions::allow_concurrent_memtable_write`.

### Parallel Launch Strategy

```cpp
// db/write_thread.cc:680-715
void WriteThread::LaunchParallelMemTableWriters(WriteGroup* write_group) {
  size_t group_size = write_group->size;
  write_group->running.store(group_size);  // Barrier counter

  const size_t MinParallelSize = 20;

  if (group_size < MinParallelSize) {
    // Small group: set all to parallel writer
    for (auto w : *write_group) {
      SetState(w, STATE_PARALLEL_MEMTABLE_WRITER);
    }
    return;
  }

  // Large group: use sqrt(N) callers to avoid O(N) SetState cost
  size_t stride = static_cast<size_t>(std::sqrt(group_size));
  auto w = write_group->leader;
  SetState(w, STATE_PARALLEL_MEMTABLE_WRITER);

  for (size_t i = 1; i < stride; i++) {
    w = w->link_newer;
    SetState(w, STATE_PARALLEL_MEMTABLE_CALLER);
  }

  // Each caller wakes every stride-th writer
  w = w->link_newer;
  SetMemWritersEachStride(w);
}
```

**Optimization**: For groups ≥20, use **√N callers** to wake others in parallel, reducing `SetState` overhead from O(N) to O(√N).

### Completion Barrier

```cpp
// db/write_thread.cc:720-737
bool WriteThread::CompleteParallelMemTableWriter(Writer* w) {
  auto* write_group = w->write_group;

  // Accumulate errors
  if (!w->status.ok()) {
    std::lock_guard<std::mutex> guard(write_group->leader->StateMutex());
    write_group->status = w->status;
  }

  if (write_group->running-- > 1) {
    // Not last writer: wait for completion
    AwaitState(w, STATE_COMPLETED, &cpmtw_ctx);
    return false;
  }

  // Last writer: performs exit duties
  w->status = write_group->status;
  return true;
}
```

**Atomic barrier**: `write_group->running` counts down. Last writer (reaching 0) completes the group.

**Requirements**:
- Memtable implementation must support concurrent inserts
- SkipList memtable supports this via lock-free concurrent insert

## Subcompactions

Subcompactions divide a single compaction job into multiple **key-range sub-jobs** that execute in parallel.

### Configuration

**Option**: `max_subcompactions` (per `CompactionOptions`)
- Number of parallel sub-ranges
- Default: 1 (no subcompaction)

### Mechanism

```
CompactionJob
  |
  +-- Divide input key range into N sub-ranges
  |
  +-- Submit N sub-jobs to compaction thread pool
  |
  v
Sub-job [a, m)  |  Sub-job [m, r)  |  Sub-job [r, u)  |  Sub-job [u, z]
  |                  |                  |                  |
  +------------------+------------------+------------------+
                     |
                     v
             Merge output files
                     |
                     v
               LogAndApply
```

**Each sub-job**:
- Runs `CompactionIterator` over its key range
- Writes output SST files independently
- Executes on separate thread pool worker

**Final merge**:
- Output files collected under `mutex_`
- Single `LogAndApply` installs all files atomically

**Benefit**: Utilize multiple cores for large single compactions.

**⚠️ INVARIANT**: Subcompaction ranges must be **disjoint** and **cover the full input range** to preserve correctness.

## Thread Safety Guarantees

### Thread-Safe Operations (No External Synchronization Required)

- **Get/MultiGet**: Via ref-counted SuperVersion
- **Iterator creation**: Snapshots SuperVersion, safe for concurrent use
- **Put/Delete/Merge/Write**: WriteThread coordinates, fully thread-safe
- **CreateSnapshot/ReleaseSnapshot**: Thread-safe
- **GetProperty/GetIntProperty**: Thread-safe (reads atomic stats)
- **ColumnFamilyData::GetReferencedSuperVersion()**: Thread-safe acquisition
- **Env methods**: All thread-safe

### NOT Thread-Safe (Require External Synchronization)

- **Close()**: Must not be called concurrently with other operations
- **DestroyDB()**: Must not be called while DB is open

### Thread-Safe but Require Care

- **SetOptions() / SetDBOptions()**: Acquires `options_mutex_` and `mutex_` internally; thread-safe but slow (serializes OPTIONS file) and not fully stress-tested
- **CreateColumnFamily()**: Acquires `options_mutex_` internally; thread-safe
- **DropColumnFamily()**: Acquires `mutex_` internally; thread-safe
- **CompactRange()**: Acquires `mutex_` internally; thread-safe. Multiple concurrent manual compactions are coordinated via `manual_compaction_paused_` and internal state
- **DisableFileDeletions/EnableFileDeletions**: Acquires `mutex_` internally; thread-safe (uses ref-counted `disable_delete_obsolete_files_` counter)

### Internal Thread Safety Mechanisms

**Atomic variables**:
- `shutting_down_`: Shutdown flag checked by background threads
- `SuperVersion::refs`: Reference counting for lock-free reads
- `WriteGroup::running`: Barrier counter for parallel memtable writes
- `queue_len_`: Thread pool queue depth

**Memory ordering**:
- `memory_order_acquire/release`: Publish-subscribe patterns (e.g., SuperVersion installation)
- `memory_order_relaxed`: Statistics counters (ordering not required)

**Lock-free structures**:
- Writer linked list (CAS on `newest_writer_`)
- SuperVersion thread-local cache (sentinel values)

## Shutdown and Cleanup

### CancelAllBackgroundWork

```cpp
// db/db_impl/db_impl.cc:481-506
void DBImpl::CancelAllBackgroundWork(bool wait) {
  CancelPeriodicTaskScheduler();

  InstrumentedMutexLock l(&mutex_);

  // Optional flush before shutdown
  if (!shutting_down_.load(std::memory_order_acquire) &&
      has_unpersisted_data_.load(std::memory_order_relaxed) &&
      !mutable_db_options_.avoid_flush_during_shutdown) {
    DBImpl::FlushAllColumnFamilies(FlushOptions(),
                                    FlushReason::kShutDown);
  }

  // Cancel awaiting remote compactions
  if (immutable_db_options_.compaction_service) {
    immutable_db_options_.compaction_service->CancelAwaitingJobs();
  }

  shutting_down_.store(true, std::memory_order_release);
  bg_cv_.SignalAll();

  if (!wait) return;
  WaitForBackgroundWork();  // Waits on bg_cv_ for all bg counters to reach 0
}
```

**⚠️ INVARIANT**: `shutting_down_` is checked **atomically** by background threads before scheduling new work. This prevents new jobs during shutdown.

### WaitForCompact

**Interface**: Blocks until all scheduled background work completes.

**Implementation**: Uses condition variable `bg_cv_` + counters for pending jobs (`bg_compaction_scheduled_`, `bg_flush_scheduled_`, etc.).

### Thread Pool Shutdown

```cpp
// util/threadpool_imp.cc:179-203
void ThreadPoolImpl::Impl::JoinThreads(bool wait_for_jobs_to_complete) {
  std::unique_lock<std::mutex> lock(mu_);

  wait_for_jobs_to_complete_ = wait_for_jobs_to_complete;
  exit_all_threads_ = true;
  total_threads_limit_ = 0;  // Prevent thread recreation
  reserved_threads_ = 0;
  num_waiting_threads_ = 0;

  lock.unlock();
  bgsignal_.notify_all();  // Wake all threads

  for (auto& th : bgthreads_) {
    th.join();  // Block until thread exits
  }

  bgthreads_.clear();
  exit_all_threads_ = false;
  wait_for_jobs_to_complete_ = false;
}
```

**Shutdown sequence**:
1. Set `exit_all_threads_ = true`
2. Wake all threads via `notify_all()`
3. Each thread checks flag, drains work (optional), and exits
4. Main thread joins all workers

## Performance Implications

### Hot Path Optimizations

**WriteThread Spin**: 200 iterations of `AsmVolatilePause()` (~1.4µs) before yielding
- Avoids syscall overhead for low-latency writes
- Effective under low/medium contention

**SuperVersion Thread-Local Cache**: Eliminates atomic ref-count operations on read path
- Typical read: 0 atomic ops (cache hit)
- Cache miss: 1 atomic increment + 1 atomic decrement

**Group Commit**: Amortizes WAL fsync and MANIFEST update
- Single fsync for N writes
- Reduces latency variance

### Cache-Line Alignment

```cpp
// db/db_impl/db_impl.h:1406
mutable CacheAlignedInstrumentedMutex mutex_;  // Prevent false sharing
```

Aligns `mutex_` to cache line boundary (64 bytes) to prevent false sharing with adjacent hot fields.

### Adaptive Yield Credit

Exponential decay formula:
```
yield_credit = yield_credit - (yield_credit / 1024) +
               (would_spin_again ? 1 : -1) * 131072
```

- Low contention → high credit → more spinning
- High contention → low credit → faster fallback to blocking
- Adapts to workload dynamically

## Key File References

| Component | Files |
|-----------|-------|
| Thread Pool | `util/threadpool_imp.{cc,h}` |
| Env Interface | `include/rocksdb/env.h` |
| WriteThread | `db/write_thread.{cc,h}` |
| SuperVersion | `db/column_family.h:206-275` |
| Background Jobs | `db/db_impl/db_impl_compaction_flush.cc` |
| DB Impl | `db/db_impl/db_impl.{h,cc}` |
| Options | `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h` |

## Critical Invariants Summary

| Invariant | Why It Matters |
|-----------|----------------|
| `mutex_` acquired before `wal_write_mutex_` | Prevents deadlock between write path and background threads |
| `Writer::StateMutex` always last in lock order | Prevents deadlock in group commit coordination |
| SuperVersion `refs > 0` while in use | Memory safety for concurrent reads |
| `shutting_down_` checked atomically | Clean shutdown without new jobs |
| Excessive threads terminate in reverse creation order | Predictable thread pool scaling |
| Background jobs release `mutex_` during I/O | Prevents blocking foreground operations |
| Subcompaction ranges are disjoint | Correctness of parallel compaction |

---

**Further Reading**:
- `ARCHITECTURE.md` — High-level architecture overview
- `docs/components/write_flow.md` — Detailed write path analysis
- `docs/components/flush.md` — Flush process deep dive
- `docs/components/compaction.md` — Compaction mechanics
