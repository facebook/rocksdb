# Review: monitoring Documentation

You are a senior RocksDB engineer performing a thorough, adversarial review of AI-generated documentation. Your job is to find every error, omission, and weakness. Assume the documentation is wrong until you verify it against the code. A human maintainer will read your review to decide what to fix — be precise enough that they can act on each issue without re-investigating.

## Review Scope

Review all files in `docs/components/monitoring/`:
- `index.md` (the index file)
- All chapter files (`NN_*.md`)

## Your Unique Review Angle

Another reviewer (Claude Code) is also reviewing this documentation in parallel. To maximize total coverage, focus especially on:
- **Cross-component boundary accuracy**: Does the doc correctly describe how monitoring interacts with other RocksDB subsystems? Trace the actual call chains.
- **Option semantics precision**: For every option mentioned, verify the exact semantics — default value, valid range, interaction with other options, behavior when set to 0 or empty, sanitization in `Sanitize()` methods.
- **Concurrency correctness**: Are locking, ordering, and thread-safety claims accurate? Check the actual mutex/atomic usage.
- **Test coverage alignment**: Do the documented behaviors match what the unit tests and stress tests actually exercise? Are there tested edge cases the docs miss?

## Review Procedure

exec
/bin/bash -lc 'git rev-parse --show-toplevel' in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc 'git status --short' in /home/xbw/workspace/ws36/rocksdb succeeded in 487ms:
/home/xbw/workspace/ws36/rocksdb
 succeeded in 486ms:
 M docs/components/README.md
D docs/components/cache.md
D docs/components/compaction.md
D docs/components/file_io.md
D docs/components/filter.md
D docs/components/flush.md
D docs/components/iterator.md
D docs/components/listener.md
D docs/components/memtable.md
D docs/components/monitoring.md
D docs/components/public_api_write.md
D docs/components/secondary_cache.md
D docs/components/snapshot.md
D docs/components/sst_table_format.md
D docs/components/threading_model.md
D docs/components/wal.md
D docs/components/wide_column.md
?? blob_db_prompt.txt
?? checkpoint_prompt.txt
?? compression_prompt.txt
codex
I have the review prompt. Next I’m reading the monitoring docs end-to-end and mapping each chapter to the implementation files it claims to cover, so I can verify behavior rather than reviewing prose in isolation.
exec
/bin/bash -lc 'wc -l docs/components/monitoring/*' in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  123 docs/components/monitoring/01_statistics_framework.md
72 docs/components/monitoring/02_core_local_implementation.md
95 docs/components/monitoring/03_perf_context.md
68 docs/components/monitoring/04_iostats_context.md
94 docs/components/monitoring/05_db_properties.md
83 docs/components/monitoring/06_compaction_stats.md
65 docs/components/monitoring/07_event_logger.md
60 docs/components/monitoring/08_thread_status.md
49 docs/components/monitoring/09_stats_history.md
47 docs/components/monitoring/10_instrumented_mutex.md
66 docs/components/monitoring/11_performance_best_practices.md
42 docs/components/monitoring/index.md
864 total
exec
/bin/bash -lc 'find docs/components/monitoring -maxdepth 1 -type f | sort' in /home/xbw/workspace/ws36/rocksdb succeeded in 618ms:
docs/components/monitoring/01_statistics_framework.md
docs/components/monitoring/02_core_local_implementation.md
docs/components/monitoring/03_perf_context.md
docs/components/monitoring/04_iostats_context.md
docs/components/monitoring/05_db_properties.md
docs/components/monitoring/06_compaction_stats.md
docs/components/monitoring/07_event_logger.md
docs/components/monitoring/08_thread_status.md
docs/components/monitoring/09_stats_history.md
docs/components/monitoring/10_instrumented_mutex.md
docs/components/monitoring/11_performance_best_practices.md
docs/components/monitoring/index.md
exec
/bin/bash -lc "for f in docs/components/monitoring/index.md docs/components/monitoring/0*.md docs/components/monitoring/1*.md; do printf '\\n===== %s =====\\n' \""'$f"; sed -n '"'1,220p' \""'$f"; done' in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:

===== docs/components/monitoring/index.md =====
# RocksDB Monitoring

## Overview

RocksDB provides a comprehensive monitoring subsystem for tracking performance metrics, diagnosing bottlenecks, and understanding runtime behavior. The subsystem spans three scopes: global cumulative counters (Statistics), per-thread fine-grained counters (PerfContext/IOStatsContext), and runtime database introspection (DB Properties). All monitoring is optional and configurable to balance insight against overhead.

**Key source files:** `include/rocksdb/statistics.h`, `include/rocksdb/perf_context.h`, `include/rocksdb/iostats_context.h`, `monitoring/statistics_impl.h`, `db/internal_stats.h`, `logging/event_logger.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Statistics Framework | [01_statistics_framework.md](01_statistics_framework.md) | `Statistics` object, tickers (counters), histograms (distributions), `StatsLevel` overhead control, and the `CreateDBStatistics()` API. |
| 2. Core-Local Implementation | [02_core_local_implementation.md](02_core_local_implementation.md) | `StatisticsImpl` per-core sharding via `CoreLocalArray`, cache-line alignment, lock-free ticker increments, and aggregation under `aggregate_lock_`. |
| 3. PerfContext | [03_perf_context.md](03_perf_context.md) | Thread-local `PerfContext` for per-request performance counters, `PerfLevel` control hierarchy, per-level breakdown, and compile-time disabling. |
| 4. IOStatsContext | [04_iostats_context.md](04_iostats_context.md) | Thread-local I/O statistics, temperature-based file I/O tracking, `disable_iostats` backdoor, and relationship with `PerfLevel`. |
| 5. DB Properties | [05_db_properties.md](05_db_properties.md) | Runtime introspection via `GetProperty()`/`GetIntProperty()`/`GetMapProperty()`, per-level stats, block cache entry stats, and aggregated properties. |
| 6. Compaction Stats and DB Status | [06_compaction_stats.md](06_compaction_stats.md) | Periodic stats dumping, compaction stats output format (per-level and interval), write stall counters, and `InternalStats` tracking. |
codex
I’ve read the docs. A few claims already look suspicious, especially around defaults, scheduler integration, and what is actually tracked under thread status and stats history. I’m switching to code verification now, starting with options, APIs, and the real call paths behind each chapter.
exec
/bin/bash -lc "rg -n \"stats_dump_period_sec|stats_persist_period_sec|persist_stats_to_disk|stats_history_buffer_size|enable_thread_tracking|CreateDBStatistics|StatsLevel|kExceptDetailedTimers|DB_MUTEX_WAIT_MICROS|PerfLevel|IOStatsContext|disable_iostats|GetStatsHistory|StatsHistoryIterator|PeriodicTaskScheduler|EventLogger|EVENT_LOG_v1|InstrumentedMutex|FileIOByTemperature|GetThreadList|ThreadStatusUpdater\" include monitoring db logging table util -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "find docs/components/monitoring -maxdepth 1 -type f | xargs -I{} sh -c \"printf '%s: ' '{}' && rg -n '\\\\b[0-9]+\\\\b' '{}' | head -n 3\"" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc 'find monitoring db logging include test utilities -maxdepth 2 -type f | rg "(statistics|perf_context|iostats_context|internal_stats|event_logger|thread_status|stats_history|instrumented_mutex|file_read_sample)"' in /home/xbw/workspace/ws36/rocksdb succeeded in 742ms:
docs/components/monitoring/index.md: 13:| 1. Statistics Framework | [01_statistics_framework.md](01_statistics_framework.md) | `Statistics` object, tickers (counters), histograms (distributions), `StatsLevel` overhead control, and the `CreateDBStatistics()` API. |
14:| 2. Core-Local Implementation | [02_core_local_implementation.md](02_core_local_implementation.md) | `StatisticsImpl` per-core sharding via `CoreLocalArray`, cache-line alignment, lock-free ticker increments, and aggregation under `aggregate_lock_`. |
15:| 3. PerfContext | [03_perf_context.md](03_perf_context.md) | Thread-local `PerfContext` for per-request performance counters, `PerfLevel` control hierarchy, per-level breakdown, and compile-time disabling. |
docs/components/monitoring/02_core_local_implementation.md: 9:The `CoreLocalArray<StatisticsData>` template (see `util/core_local.h`) allocates an array of `StatisticsData` entries, one per core. The array size is the smallest power of two greater than or equal to `std::thread::hardware_concurrency()` (minimum 8). Each access maps the current physical core ID to an array index using `port::PhysicalCoreID()`.
23:Step 1: `recordTick()` checks `StatsLevel` -- if stats are disabled (`<= kExceptTickers`), returns immediately.
25:Step 2: `per_core_stats_.Access()` retrieves the `StatisticsData` for the current core using `port::PhysicalCoreID()`. If the core ID is unavailable (returns negative), a random index is chosen.
docs/components/monitoring/03_perf_context.md: 15:| `kDisable` | 1 | None |
16:| `kEnableCount` | 2 | Count and byte metrics (e.g., `block_read_count`, `block_read_byte`) |
17:| `kEnableWait` | 3 | Wait/delay time metrics (e.g., `write_delay_time`, `write_thread_wait_nanos`) |
docs/components/monitoring/04_iostats_context.md: docs/components/monitoring/05_db_properties.md: 27:- `rocksdb.compression-ratio-at-level<N>` -- compression ratio at level N (returns `-1.0` if no files)
44:- `rocksdb.mem-table-flush-pending` -- 1 if flush pending, 0 otherwise
51:- `rocksdb.compaction-pending` -- 1 if compaction pending, 0 otherwise
docs/components/monitoring/10_instrumented_mutex.md: 33:Step 1: On `Start()`, records the current time (wall-clock or CPU time based on `use_cpu_time_`).
35:Step 2: On `Stop()` (or destructor), computes elapsed time and:
39:Step 3: `Measure()` records elapsed time since last start and resets the start time (for multi-phase timing).
docs/components/monitoring/11_performance_best_practices.md: 11:| PerfContext counters (`kEnableCount`) | Thread-local increment | ~1 ns | No contention, no atomics |
12:| Statistics tickers | Core-local `atomic_uint_fast64_t` with `fetch_add(relaxed)` | ~2-5 ns | Minimal contention via core-local sharding |
13:| Histograms | Core-local atomic bucket update | ~5-10 ns | Multiple atomic ops per sample (min, max, sum, bucket) |
docs/components/monitoring/09_stats_history.md: 15:| `stats_persist_period_sec` | 600 | Seconds between stats snapshots (0 to disable) |
21:Step 1: `PeriodicTaskScheduler` triggers a snapshot at the configured interval.
 succeeded in 3.52s:
/usr/bin/find: ‘test’: No such file or directory
monitoring/in_memory_stats_history.cc
monitoring/in_memory_stats_history.h
monitoring/instrumented_mutex.cc
monitoring/instrumented_mutex.h
monitoring/iostats_context.cc
monitoring/iostats_context_imp.h
monitoring/iostats_context_test.cc
monitoring/perf_context_imp.h
monitoring/persistent_stats_history.cc
monitoring/persistent_stats_history.h
monitoring/statistics_impl.h
monitoring/statistics_test.cc
monitoring/thread_status_util_debug.cc
monitoring/stats_history_test.cc.d
monitoring/statistics_test.cc.d
monitoring/iostats_context_test.cc.d
monitoring/thread_status_util_debug.cc.d
monitoring/thread_status_updater_debug.cc.d
monitoring/thread_status_util.cc.d
 succeeded in 4.19s:
logging/event_logger_test.cc:14:class EventLoggerTest : public testing::Test {};
logging/event_logger_test.cc:28:TEST_F(EventLoggerTest, SimpleTest) {
logging/event_logger_test.cc:30:  EventLogger event_logger(&logger);
logging/event_logger.h:138:class EventLoggerStream {
logging/event_logger.h:141:  EventLoggerStream& operator<<(const T& val) {
logging/event_logger.h:152:  ~EventLoggerStream();
logging/event_logger.h:164:  friend class EventLogger;
logging/event_logger.h:165:  explicit EventLoggerStream(Logger* logger);
logging/event_logger.h:166:  explicit EventLoggerStream(LogBuffer* log_buffer, const size_t max_log_size);
logging/event_logger.h:176:// 2015/01/15-14:13:25.788019 1105ef000 EVENT_LOG_v1 {"time_micros":
logging/event_logger.h:179:class EventLogger {
logging/event_logger.h:181:  static const char* Prefix() { return "EVENT_LOG_v1"; }
logging/event_logger.h:183:  explicit EventLogger(Logger* logger) : logger_(logger) {}
logging/event_logger.h:184:  EventLoggerStream Log() { return EventLoggerStream(logger_); }
logging/event_logger.h:185:  EventLoggerStream LogToBuffer(LogBuffer* log_buffer) {
logging/event_logger.h:186:    return EventLoggerStream(log_buffer, LogBuffer::kDefaultMaxLogSize);
logging/event_logger.h:188:  EventLoggerStream LogToBuffer(LogBuffer* log_buffer,
logging/event_logger.h:190:    return EventLoggerStream(log_buffer, max_log_size);
logging/event_logger.cc:17:EventLoggerStream::EventLoggerStream(Logger* logger)
logging/event_logger.cc:23:EventLoggerStream::EventLoggerStream(LogBuffer* log_buffer,
exec
/bin/bash -lc "sed -n '1,220p' docs/components/monitoring/04_iostats_context.md" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,260p' docs/components/monitoring/05_db_properties.md" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
# IOStatsContext

**Files:** `include/rocksdb/iostats_context.h`, `monitoring/iostats_context.cc`, `monitoring/iostats_context_imp.h`

## Overview

`IOStatsContext` provides thread-local I/O statistics for diagnosing I/O bottlenecks. It tracks bytes read/written, time spent in various I/O system calls, and per-temperature file I/O breakdowns.

## Key Metrics

**Basic I/O** (see `IOStatsContext` in `include/rocksdb/iostats_context.h`):
- `bytes_written` / `bytes_read` -- total bytes transferred
- `thread_pool_id` -- identifies which thread pool the thread belongs to

**I/O operation timing** (wall-clock, enabled at `PerfLevel::kEnableTimeExceptForMutex`):

| Metric | System Call |
|--------|-------------|
| `open_nanos` | `open()` / `fopen()` |
| `allocate_nanos` | `fallocate()` |
 succeeded in 51ms:
# DB Properties

**Files:** `include/rocksdb/db.h`, `db/internal_stats.h`, `db/internal_stats.cc`

## Overview

DB properties provide runtime introspection into database state without requiring a `Statistics` object. Properties can return strings, integers, or maps, and are queried on demand via `DB::GetProperty()`, `DB::GetIntProperty()`, or `DB::GetMapProperty()`.

## Property API

Three methods retrieve properties, each for a different value type:

| Method | Return Type | Use Case |
|--------|-------------|----------|
| `GetProperty(property, &value)` | `std::string` | Human-readable formatted output |
| `GetIntProperty(property, &value)` | `uint64_t` | Single numeric values |
| `GetMapProperty(property, &value)` | `std::map<std::string, std::string>` | Structured key-value data |

All methods accept a `ColumnFamilyHandle*` parameter (defaults to the default column family). They return `true` on success, `false` if the property is not recognized or unavailable.

codex
The docs are broad enough that I need to structure the verification pass to avoid shallow review. I’m breaking it into APIs/options, implementation/concurrency, and tests/history so I can tie each finding to code and not rely on intuition.
Plan update
Reviewing documentation against current code, with emphasis on option semantics, cross-component behavior, concurrency, and test alignment.
✓ Read monitoring docs and identify claims to verify
→ Verify APIs, defaults, option semantics, and implementation details in source
• Check tests and recent history for behavioral contracts or stale docs
• Write actionable review to docs/reviews/monitoring_review_codex.md
exec
/bin/bash -lc "sed -n '1110,1165p' include/rocksdb/options.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1260,1285p' include/rocksdb/options.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,240p' include/rocksdb/stats_history.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,220p' include/rocksdb/iostats_context.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,260p' include/rocksdb/perf_level.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
  // preallocation. The file space preallocation is used to increase the file
// write/append performance. By default, RocksDB preallocates space for WAL,
// SST, Manifest files, the extra space is truncated when the file is written.
// Warning: if you're using btrfs, we would recommend setting
// `allow_fallocate=false` to disable preallocation. As on btrfs, the extra
// allocated space cannot be freed, which could be significant if you have
// lots of files. More details about this limitation:
// https://github.com/btrfs/btrfs-dev-docs/blob/471c5699336e043114d4bca02adcd57d9dab9c44/data-extent-reference-counts.md
bool allow_fallocate = true;

// Disable child process inherit open files. Default: true
bool is_fd_close_on_exec = true;

// if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
//
// Default: 600 (10 min)
//
// Dynamically changeable through SetDBOptions() API.
unsigned int stats_dump_period_sec = 600;

 succeeded in 51ms:
  //
// Note: Enabling this option does not provide any additional persistence
// guarantees, as it may use `sync_file_range`, which does not write out
// metadata.
//
// Default: false
bool strict_bytes_per_sync = false;

// A vector of EventListeners whose callback functions will be called
// when specific RocksDB event happens.
std::vector<std::shared_ptr<EventListener>> listeners;

// If true, then the status of the threads involved in this DB will
// be tracked and available via GetThreadList() API.
//
// Default: false
bool enable_thread_tracking = false;

// The limited write rate to DB if soft_pending_compaction_bytes_limit or
// level0_slowdown_writes_trigger is triggered, or we are writing to the
exec
/bin/bash -lc "sed -n '1,320p' include/rocksdb/perf_context.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <map>
#include <string>

#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;

 succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <stdint.h>

#include <string>

#include "rocksdb/perf_level.h"

/*
* NOTE:
* If you plan to add new metrics, please read documentation in perf_level.h and
* try to come up with a metric name that follows the naming conventions
* mentioned there. It helps to indicate the metric's starting enabling P
* erfLevel. Document this starting PerfLevel if the metric name cannot meet the
* naming conventions.
*/
 succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// How much perf stats to collect. Affects perf_context and iostats_context.
// These levels are incremental, which means a new set of metrics will get
// collected when PerfLevel is upgraded from level n to level n + 1.
// Each level's documentation specifies the incremental set of metrics it
// enables. As an example, kEnableWait will also enable collecting all the
 succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <map>
#include <string>

#include "rocksdb/perf_level.h"

namespace ROCKSDB_NAMESPACE {

/*
* NOTE:
* Please do not reorder the fields in this structure. If you plan to do that or
* add/remove fields to this structure, builds would fail. The way to fix the
exec
/bin/bash -lc "sed -n '320,420p' include/rocksdb/perf_context.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  void DisablePerLevelPerfContext();

// free the space for PerfContextByLevel, also disable per level perf context
void ClearPerLevelPerfContext();

std::map<uint32_t, PerfContextByLevel>* level_to_perf_context = nullptr;
bool per_level_perf_context_enabled = false;

void copyMetrics(const PerfContext* other) noexcept;
};

// If RocksDB is compiled with -DNPERF_CONTEXT, then a pointer to a global,
// non-thread-local PerfContext object will be returned. Attempts to update
// this object will be ignored, and reading from it will also be no-op.
// Otherwise,
// a) if thread-local is supported on the platform, then a pointer to
//    a thread-local PerfContext object will be returned.
// b) if thread-local is NOT supported, then compilation will fail.
//
// This function never returns nullptr.
exec
/bin/bash -lc "sed -n '1,360p' include/rocksdb/statistics.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

exec
/bin/bash -lc 'rg -n "enum StatsLevel|CreateDBStatistics|get_stats_level|set_stats_level|class Statistics" include/rocksdb/statistics.h monitoring/statistics.cc monitoring/statistics_impl.h' in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
include/rocksdb/statistics.h:760://   options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
include/rocksdb/statistics.h:761:enum StatsLevel : uint8_t {
include/rocksdb/statistics.h:785://  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
include/rocksdb/statistics.h:795:class Statistics : public Customizable {
include/rocksdb/statistics.h:814:    if (get_stats_level() <= StatsLevel::kExceptTimers) {
include/rocksdb/statistics.h:853:  void set_stats_level(StatsLevel sl) {
include/rocksdb/statistics.h:856:  StatsLevel get_stats_level() const {
include/rocksdb/statistics.h:865:std::shared_ptr<Statistics> CreateDBStatistics();
monitoring/statistics.cc:378:std::shared_ptr<Statistics> CreateDBStatistics() {
monitoring/statistics.cc:500:  if (get_stats_level() <= StatsLevel::kExceptTickers) {
monitoring/statistics.cc:516:  if (get_stats_level() <= StatsLevel::kExceptHistogramOrTimers) {
monitoring/statistics_impl.h:42:class StatisticsImpl : public Statistics {
exec
/bin/bash -lc "sed -n '740,880p' include/rocksdb/statistics.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '378,620p' monitoring/statistics.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,240p' monitoring/statistics_impl.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:

extern const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap;

struct HistogramData {
double median;
double percentile95;
double percentile99;
double average;
double standard_deviation;
// zero-initialize new members since old Statistics::histogramData()
// implementations won't write them.
double max = 0.0;
uint64_t count = 0;
uint64_t sum = 0;
double min = 0.0;
};

// StatsLevel can be used to reduce statistics overhead by skipping certain
// types of stats in the stats collection process.
// Usage:
 succeeded in 51ms:
std::shared_ptr<Statistics> CreateDBStatistics() {
return std::make_shared<StatisticsImpl>(nullptr);
}

static int RegisterBuiltinStatistics(ObjectLibrary& library,
const std::string& /*arg*/) {
library.AddFactory<Statistics>(
StatisticsImpl::kClassName(),
[](const std::string& /*uri*/, std::unique_ptr<Statistics>* guard,
std::string* /* errmsg */) {
guard->reset(new StatisticsImpl(nullptr));
return guard->get();
});
return 1;
}

Status Statistics::CreateFromString(const ConfigOptions& config_options,
const std::string& id,
std::shared_ptr<Statistics>* result) {
static std::once_flag once;
 succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "monitoring/histogram.h"
#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/statistics.h"
#include "util/core_local.h"
#include "util/mutexlock.h"

#ifdef __clang__
#define ROCKSDB_FIELD_UNUSED __attribute__((__unused__))
exec
/bin/bash -lc "sed -n '1,260p' util/core_local.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstddef>
#include <thread>
#include <utility>
#include <vector>

#include "port/likely.h"
#include "port/port.h"
#include "util/math.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

exec
/bin/bash -lc "sed -n '1,260p' monitoring/histogram.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,280p' monitoring/histogram.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "rg -n \"109|BucketMapper|HistogramStat|Merge|Data\\(|Add\\(|Clear\\(\" monitoring/histogram.h monitoring/histogram.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <cassert>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "rocksdb/statistics.h"

namespace ROCKSDB_NAMESPACE {

 succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "monitoring/histogram.h"

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdio>

#include "port/port.h"
#include "util/cast_util.h"

 succeeded in 50ms:
monitoring/histogram.h:21:class HistogramBucketMapper {
monitoring/histogram.h:23:  HistogramBucketMapper();
monitoring/histogram.h:46:struct HistogramStat {
monitoring/histogram.h:47:  HistogramStat();
monitoring/histogram.h:48:  ~HistogramStat() {}
monitoring/histogram.h:50:  HistogramStat(const HistogramStat&) = delete;
monitoring/histogram.h:51:  HistogramStat& operator=(const HistogramStat&) = delete;
monitoring/histogram.h:53:  void Clear();
monitoring/histogram.h:55:  void Add(uint64_t value);
monitoring/histogram.h:56:  void Merge(const HistogramStat& other);
monitoring/histogram.h:73:  void Data(HistogramData* const data) const;
monitoring/histogram.h:76:  // To be able to use HistogramStat as thread local variable, it
monitoring/histogram.h:78:  // using manually values from BucketMapper
monitoring/histogram.h:84:  std::atomic_uint_fast64_t buckets_[109];  // 109==BucketMapper::BucketCount()
monitoring/histogram.h:93:  virtual void Clear() = 0;
monitoring/histogram.h:95:  virtual void Add(uint64_t value) = 0;
monitoring/histogram.h:96:  virtual void Merge(const Histogram&) = 0;
monitoring/histogram.h:107:  virtual void Data(HistogramData* const data) const = 0;
monitoring/histogram.h:112:  HistogramImpl() { Clear(); }
monitoring/histogram.h:117:  void Clear() override;
exec
/bin/bash -lc "sed -n '1,280p' monitoring/perf_context.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <sstream>

#include "monitoring/perf_context_imp.h"

namespace ROCKSDB_NAMESPACE {

/*
* Please add new metrics to this macro and appropriate fields will be copied,
* and/or emitted when converted to string.
* When people need to add new metrics please add the metric to the macro below
* and enclose the name of the specific metric within defCmd().
* The position of the field will be dictated by the
* order in which the macros are enumerated and the offsets of the fields will
* be matched against ''PerfContextByLevelBase'' declared in perf_context.h.
exec
/bin/bash -lc "sed -n '1,260p' monitoring/perf_context_imp.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,260p' monitoring/iostats_context.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,260p' monitoring/perf_level_imp.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,260p' monitoring/iostats_context_imp.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,220p' monitoring/perf_step_timer.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/perf_context.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
#if defined(NPERF_CONTEXT)
extern PerfContext perf_context;
#else
#if defined(OS_SOLARIS)
extern thread_local PerfContext perf_context_;
#define perf_context (*get_perf_context())
#else
extern thread_local PerfContext perf_context;
#endif
 succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <sstream>

#include "monitoring/iostats_context_imp.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

#ifdef NIOSTATS_CONTEXT
// Should not be used because the counters are not thread-safe.
// Put here just to make get_iostats_context() simple without ifdef.
static IOStatsContext iostats_context;
#else
thread_local IOStatsContext iostats_context;
#endif

 succeeded in 50ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "port/port.h"
#include "rocksdb/perf_level.h"

namespace ROCKSDB_NAMESPACE {

extern thread_local PerfLevel perf_level;

}  // namespace ROCKSDB_NAMESPACE
 succeeded in 50ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/iostats_context.h"

#if !defined(NIOSTATS_CONTEXT)
namespace ROCKSDB_NAMESPACE {
extern thread_local IOStatsContext iostats_context;
}  // namespace ROCKSDB_NAMESPACE

// increment a specific counter by the specified value
#define IOSTATS_ADD(metric, value)        \
if (!iostats_context.disable_iostats) { \
iostats_context.metric += value;      \
}

 succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_level_imp.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

class PerfStepTimer {
public:
explicit PerfStepTimer(
uint64_t* metric, SystemClock* clock = nullptr, bool use_cpu_time = false,
PerfLevel enable_level = PerfLevel::kEnableTimeExceptForMutex,
Statistics* statistics = nullptr, uint32_t ticker_type = 0)
: perf_counter_enabled_(perf_level >= enable_level),
use_cpu_time_(use_cpu_time),
exec
/bin/bash -lc "rg -n \"thread_local PerfLevel|SetPerfLevel\\(|GetPerfLevel\\(\" monitoring/perf_context.cc monitoring/perf_level_imp.h include/rocksdb/perf_level.h db/perf_context_test.cc db/* | head -n 80" in /home/xbw/workspace/ws36/rocksdb succeeded in 2.68s:
db/version_set_sync_and_async.h:21:  bool timer_enabled = GetPerfLevel() >= PerfLevel::kEnableTimeExceptForMutex &&
db/perf_context_test.cc:502:  SetPerfLevel(kEnableCount);
db/perf_context_test.cc:505:  SetPerfLevel(kDisable);
db/perf_context_test.cc:508:  SetPerfLevel(kEnableTime);
db/perf_context_test.cc:547:  SetPerfLevel(kEnableTime);
db/perf_context_test.cc:607:        SetPerfLevel(perf_level_test);
db/perf_context_test.cc:628:  SetPerfLevel(kEnableTime);
db/perf_context_test.cc:674:  SetPerfLevel(kEnableTime);
db/perf_context_test.cc:849:  SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
db/perf_context_test.cc:1127:  SetPerfLevel(PerfLevel::kEnableWait);
db/perf_context_test.cc:1133:  SetPerfLevel(PerfLevel::kEnableCount);
monitoring/perf_level_imp.h:12:extern thread_local PerfLevel perf_level;
db/flush_job.cc:246:    prev_perf_level = GetPerfLevel();
db/flush_job.cc:247:    SetPerfLevel(PerfLevel::kEnableTime);
db/flush_job.cc:366:      SetPerfLevel(prev_perf_level);
db/db_test.cc:6297:  SetPerfLevel(kEnableTime);
db/db_test.cc:6339:  SetPerfLevel(kEnableTime);
db/db_iter_test.cc:428:    SetPerfLevel(kEnableCount);
db/db_iter_test.cc:429:    ASSERT_TRUE(GetPerfLevel() == kEnableCount);
db/db_iter_test.cc:439:    SetPerfLevel(kDisable);
exec
/bin/bash -lc "rg -n \"thread_local PerfLevel perf_level|SetPerfLevel\\(|GetPerfLevel\\(\" -g '*.*' ." in /home/xbw/workspace/ws36/rocksdb succeeded in 619ms:
./fix_monitoring_prompt.txt:70:- The doc never states that the default thread-local `PerfLevel` is `kEnableCount`, which materially changes what users should expect before calling `SetPerfLevel()`. Source: `monitoring/perf_level.cc:13-21`.
./db/c.cc:5627:  SetPerfLevel(level);
./db/version_set_sync_and_async.h:21:  bool timer_enabled = GetPerfLevel() >= PerfLevel::kEnableTimeExceptForMutex &&
./db/db_iter_test.cc:428:    SetPerfLevel(kEnableCount);
./db/db_iter_test.cc:429:    ASSERT_TRUE(GetPerfLevel() == kEnableCount);
./db/db_iter_test.cc:439:    SetPerfLevel(kDisable);
./db/db_iter_test.cc:553:    SetPerfLevel(kEnableCount);
./db/db_iter_test.cc:554:    ASSERT_TRUE(GetPerfLevel() == kEnableCount);
./db/db_iter_test.cc:564:    SetPerfLevel(kDisable);
./db/version_set.cc:2773:        GetPerfLevel() >= PerfLevel::kEnableTimeExceptForMutex &&
./include/rocksdb/perf_context.h:57:// Use SetPerfLevel(PerfLevel::kEnableTime) to enable time stats.
./db/perf_context_test.cc:502:  SetPerfLevel(kEnableCount);
./db/perf_context_test.cc:505:  SetPerfLevel(kDisable);
./db/perf_context_test.cc:508:  SetPerfLevel(kEnableTime);
./db/perf_context_test.cc:547:  SetPerfLevel(kEnableTime);
./db/perf_context_test.cc:607:        SetPerfLevel(perf_level_test);
./db/perf_context_test.cc:628:  SetPerfLevel(kEnableTime);
./db/perf_context_test.cc:674:  SetPerfLevel(kEnableTime);
./db/perf_context_test.cc:849:  SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
./db/perf_context_test.cc:1127:  SetPerfLevel(PerfLevel::kEnableWait);
exec
/bin/bash -lc "sed -n '1,80p' monitoring/perf_level.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <cassert>

#include "monitoring/perf_level_imp.h"

namespace ROCKSDB_NAMESPACE {

thread_local PerfLevel perf_level = kEnableCount;

void SetPerfLevel(PerfLevel level) {
assert(level > kUninitialized);
assert(level < kOutOfBounds);
perf_level = level;
}

exec
/bin/bash -lc "sed -n '1,280p' include/rocksdb/thread_status.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the structures for exposing run-time status of any
// rocksdb-related thread.  Such run-time status can be obtained via
// GetThreadList() API.
//
// Note that all thread-status features are still under-development, and
// thus APIs and class definitions might subject to change at this point.
// Will remove this comment once the APIs have been finalized.

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
exec
/bin/bash -lc "sed -n '1,320p' monitoring/thread_status_updater.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,340p' monitoring/thread_status_updater.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// The implementation of ThreadStatus.
//
// Note that we make get and set access to ThreadStatusData lockless.
// As a result, ThreadStatusData as a whole is not atomic.  However,
// we guarantee consistent ThreadStatusData all the time whenever
// user call GetThreadList().  This consistency guarantee is done
// by having the following constraint in the internal implementation
// of set and get order:
//
// 1. When reset any information in ThreadStatusData, always start from
//    clearing up the lower-level information first.
// 2. When setting any information in ThreadStatusData, always start from
//    setting the higher-level information.
// 3. When returning ThreadStatusData to the user, fields are fetched from
//    higher-level to lower-level.  In addition, where there's a nullptr
exec
/bin/bash -lc "sed -n '1,260p' monitoring/thread_status_util.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,260p' monitoring/thread_status_util.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/thread_status_updater.h"

#include <memory>

#include "port/likely.h"
#include "rocksdb/env.h"
#include "rocksdb/system_clock.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

#ifndef NROCKSDB_THREAD_STATUS

thread_local ThreadStatusData* ThreadStatusUpdater::thread_status_data_ =
nullptr;
 succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "monitoring/thread_status_updater.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/thread_status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;

// The static utility class for updating thread-local status.
//
 succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/thread_status_util.h"

#include "monitoring/thread_status_updater.h"
#include "rocksdb/env.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

#ifndef NROCKSDB_THREAD_STATUS
thread_local ThreadStatusUpdater*
ThreadStatusUtil::thread_updater_local_cache_ = nullptr;
thread_local bool ThreadStatusUtil::thread_updater_initialized_ = false;

void ThreadStatusUtil::RegisterThread(const Env* env,
ThreadStatus::ThreadType thread_type) {
exec
/bin/bash -lc "rg -n \"GetThreadList\\(|enable_thread_tracking|SetEnableTracking|OP_GET|OP_MULTIGET|OP_DBITERATOR|OP_DBOPEN|OP_VERIFY_DB_CHECKSUM|OP_VERIFY_FILE_CHECKSUMS|HIGH_PRIORITY|LOW_PRIORITY|BOTTOM_PRIORITY|USER|STAGE_\" db monitoring include | head -n 300" in /home/xbw/workspace/ws36/rocksdb succeeded in 2.14s:
monitoring/thread_status_util.h:55:  static void SetEnableTracking(bool enable_tracking);
monitoring/thread_status_util.cc:36:void ThreadStatusUtil::SetEnableTracking(bool enable_tracking) {
monitoring/thread_status_util.cc:40:  thread_updater_local_cache_->SetEnableTracking(enable_tracking);
monitoring/thread_status_util.cc:79:    return ThreadStatus::STAGE_UNKNOWN;
monitoring/thread_status_util.cc:179:void ThreadStatusUtil::SetEnableTracking(bool /*enable_tracking*/) {}
monitoring/thread_status_updater.h:11:// user call GetThreadList().  This consistency guarantee is done
monitoring/thread_status_updater.h:28:// returned by the GetThreadList() is guaranteed to be consistent.
monitoring/thread_status_updater.h:68:    thread_type.store(ThreadStatus::USER);
monitoring/thread_status_updater.h:120:  void SetEnableTracking(bool enable_tracking);
monitoring/thread_status_updater.h:169:  Status GetThreadList(std::vector<ThreadStatus>* thread_list);
monitoring/thread_status_updater.cc:50:void ThreadStatusUpdater::SetEnableTracking(bool enable_tracking) {
monitoring/thread_status_updater.cc:87:    data->operation_stage.store(ThreadStatus::STAGE_UNKNOWN,
monitoring/thread_status_updater.cc:131:  data->operation_stage.store(ThreadStatus::STAGE_UNKNOWN,
monitoring/thread_status_updater.cc:152:    return ThreadStatus::STAGE_UNKNOWN;
monitoring/thread_status_updater.cc:174:Status ThreadStatusUpdater::GetThreadList(
monitoring/thread_status_updater.cc:186:    // which is currently held by GetThreadList(), here we can safely
monitoring/thread_status_updater.cc:191:    ThreadStatus::OperationStage op_stage = ThreadStatus::STAGE_UNKNOWN;
monitoring/thread_status_updater.cc:236:  // Acquiring same lock as GetThreadList() to guarantee
monitoring/thread_status_updater.cc:246:  // Acquiring same lock as GetThreadList() to guarantee
monitoring/thread_status_updater.cc:266:  // Acquiring same lock as GetThreadList() to guarantee
exec
/bin/bash -lc "sed -n '1,260p' logging/event_logger.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,220p' logging/event_logger.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <chrono>
#include <memory>
#include <sstream>
#include <string>

#include "logging/log_buffer.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter {
public:
JSONWriter() : state_(kExpectKey), first_element_(true), in_array_(false) {
 succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "logging/event_logger.h"

#include <cassert>
#include <cinttypes>
#include <sstream>
#include <string>

#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

EventLoggerStream::EventLoggerStream(Logger* logger)
: logger_(logger),
log_buffer_(nullptr),
max_log_size_(0),
exec
/bin/bash -lc "sed -n '1,220p' logging/event_logger_test.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "logging/event_logger.h"

#include <string>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class EventLoggerTest : public testing::Test {};

class StringLogger : public Logger {
public:
using Logger::Logv;
void Logv(const char* format, va_list ap) override {
vsnprintf(buffer_, sizeof(buffer_), format, ap);
exec
/bin/bash -lc "rg -n \"EVENT_LOG_v1|event_logger_|LogToBuffer\\(|flush_started|flush_finished|compaction_started|compaction_finished|recovery_started|recovery_finished|recovery_failed|table_file_creation|table_file_deletion\" db logging util file include utilities -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb succeeded in 4.05s:
logging/logging.h:51:  ROCKSDB_NAMESPACE::LogToBuffer(LOG_BUF, ROCKS_LOG_PREPEND_FILE_LINE(FMT), \
logging/logging.h:56:  ROCKSDB_NAMESPACE::LogToBuffer(                                \
logging/log_buffer.h:28:  void AddLogToBuffer(size_t max_log_size, const char* format, va_list ap);
logging/log_buffer.h:52:void LogToBuffer(LogBuffer* log_buffer, size_t max_log_size, const char* format,
logging/log_buffer.h:55:void LogToBuffer(LogBuffer* log_buffer, const char* format, ...);
logging/log_buffer.cc:16:void LogBuffer::AddLogToBuffer(size_t max_log_size, const char* format,
logging/log_buffer.cc:72:void LogToBuffer(LogBuffer* log_buffer, size_t max_log_size, const char* format,
logging/log_buffer.cc:77:    log_buffer->AddLogToBuffer(max_log_size, format, ap);
logging/log_buffer.cc:82:void LogToBuffer(LogBuffer* log_buffer, const char* format, ...) {
logging/log_buffer.cc:86:    log_buffer->AddLogToBuffer(LogBuffer::kDefaultMaxLogSize, format, ap);
logging/event_logger.h:176:// 2015/01/15-14:13:25.788019 1105ef000 EVENT_LOG_v1 {"time_micros":
logging/event_logger.h:177:// 1421360005788015, "event": "table_file_creation", "file_number": 12,
logging/event_logger.h:181:  static const char* Prefix() { return "EVENT_LOG_v1"; }
logging/event_logger.h:185:  EventLoggerStream LogToBuffer(LogBuffer* log_buffer) {
logging/event_logger.h:188:  EventLoggerStream LogToBuffer(LogBuffer* log_buffer,
logging/event_logger.h:194:  static void LogToBuffer(
logging/event_logger.cc:40:      EventLogger::LogToBuffer(log_buffer_, *json_writer_, max_log_size_);
logging/event_logger.cc:57:void EventLogger::LogToBuffer(LogBuffer* log_buffer, const JSONWriter& jwriter,
logging/event_logger.cc:63:  ROCKSDB_NAMESPACE::LogToBuffer(log_buffer, max_log_size, "%s %s", Prefix(),
db/event_helpers.cc:89:            << "table_file_creation" << "file_number" << fd.GetNumber()
exec
/bin/bash -lc "sed -n '920,980p' db/flush_job.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
              ? m->NewTimestampStrippingRangeTombstoneIterator(
ro, kMaxSequenceNumber, ts_sz)
: m->NewRangeTombstoneIterator(ro, kMaxSequenceNumber,
true /* immutable_memtable */);
if (range_del_iter != nullptr) {
range_del_iters.emplace_back(range_del_iter);
}
total_num_input_entries += m->NumEntries();
total_num_deletes += m->NumDeletion();
total_data_size += m->GetDataSize();
total_memory_usage += m->ApproximateMemoryUsage();
total_num_range_deletes += m->NumRangeDeletion();
}

// TODO(cbi): when memtable is flushed due to number of range deletions
//  hitting limit memtable_max_range_deletions, flush_reason_ is still
//  "Write Buffer Full", should make update flush_reason_ accordingly.
event_logger_->Log() << "job" << job_context_->job_id << "event"
<< "flush_started" << "num_memtables" << mems_.size()
<< "total_num_input_entries" << total_num_input_entries
exec
/bin/bash -lc "sed -n '330,370p' db/flush_job.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
                              but 'false' if mempurge successful: no new min log number
or new level 0 file path to write to manifest. */);
}
}

if (s.ok() && file_meta != nullptr) {
*file_meta = meta_;
}
RecordFlushIOStats();

// When measure_io_stats_ is true, the default 512 bytes is not enough.
auto stream = event_logger_->LogToBuffer(log_buffer_, 1024);
stream << "job" << job_context_->job_id << "event" << "flush_finished";
stream << "output_compression"
<< CompressionTypeToString(output_compression_);
stream << "lsm_state";
stream.StartArray();
auto vstorage = cfd_->current()->storage_info();
for (int level = 0; level < vstorage->num_levels(); ++level) {
stream << vstorage->NumLevelFiles(level);
exec
/bin/bash -lc "sed -n '2770,2825p' db/compaction/compaction_job.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1158,1175p' db/db_impl/db_impl_open.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1918,1935p' db/db_impl/db_impl_open.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1238,1288p' db/compaction/compaction_job.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
    Compaction::InputLevelSummaryBuffer inputs_summary;
ROCKS_LOG_INFO(
db_options_.info_log, "[%s] [JOB %d] Compacting %s, score %.2f",
cfd->GetName().c_str(), job_id_,
compaction->InputLevelSummary(&inputs_summary), compaction->score());
char scratch[2345];
compaction->Summary(scratch, sizeof(scratch));
ROCKS_LOG_INFO(db_options_.info_log, "[%s]: Compaction start summary: %s\n",
cfd->GetName().c_str(), scratch);
// build event logger report
auto stream = event_logger_->Log();
stream << "job" << job_id_ << "event" << "compaction_started" << "cf_name"
<< cfd->GetName() << "compaction_reason"
<< GetCompactionReasonString(compaction->compaction_reason());
for (size_t i = 0; i < compaction->num_input_levels(); ++i) {
stream << ("files_L" + std::to_string(compaction->level(i)));
stream.StartArray();
for (auto f : *compaction->inputs(i)) {
stream << f->fd.GetNumber();
}
 succeeded in 50ms:
    version_edits->insert({cfd->GetID(), edit});
}

*job_id = next_job_id_.fetch_add(1);
{
auto stream = event_logger_.Log();
stream << "job" << *job_id;
stream << "event" << "recovery_started";
stream << "wal_files";
stream.StartArray();
for (auto wal_number : wal_numbers) {
stream << wal_number;
}
stream.EndArray();
}

// No-op for immutable_db_options_.wal_filter == nullptr.
InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap();
 succeeded in 51ms:
      "a software bug or a data corruption. Prev next seqno: " +
std::to_string(prev_next_seqno) +
" , current next seqno: " + std::to_string(current_next_seqno);
return Status::Corruption(msg);
}

void DBImpl::FinishLogFilesRecovery(int job_id, const Status& status) {
event_logger_.Log() << "job" << job_id << "event"
<< (status.ok() ? "recovery_finished" : "recovery_failed")
<< "status" << status.ToString();
}

Status DBImpl::GetLogSizeAndMaybeTruncate(uint64_t wal_number, bool truncate,
WalFileNumberSize* log_ptr) {
WalFileNumberSize log(wal_number);
std::string fname =
LogFileName(immutable_db_options_.GetWalDir(), wal_number);
Status s;
 succeeded in 51ms:
                     internal_stats_.proximal_level_stats.num_output_files,
internal_stats_.proximal_level_stats.num_output_records);
}

TEST_SYNC_POINT_CALLBACK(
"CompactionJob::Install:AfterUpdateCompactionJobStats", job_stats_);

auto stream = event_logger_->LogToBuffer(log_buffer_, 8192);
stream << "job" << job_id_ << "event" << "compaction_finished"
<< "compaction_time_micros" << stats.micros
<< "compaction_time_cpu_micros" << stats.cpu_micros << "output_level"
<< compact_->compaction->output_level() << "num_output_files"
<< stats.num_output_files << "total_output_size"
<< stats.bytes_written;

if (stats.num_output_files_blob > 0) {
stream << "num_blob_output_files" << stats.num_output_files_blob
<< "total_blob_output_size" << stats.bytes_written_blob;
}

exec
/bin/bash -lc "sed -n '80,130p' db/event_helpers.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  if (!event_logger && listeners.empty()) {
s.PermitUncheckedError();
return;
}

if (event_logger) {
JSONWriter jwriter;
AppendCurrentTime(&jwriter);
jwriter << "cf_name" << cf_name << "job" << job_id << "event"
<< "table_file_creation" << "file_number" << fd.GetNumber()
<< "file_size" << fd.GetFileSize() << "file_checksum"
<< Slice(file_checksum).ToString(true) << "file_checksum_func_name"
<< file_checksum_func_name << "smallest_seqno" << fd.smallest_seqno
<< "largest_seqno" << fd.largest_seqno;

// table_properties
{
jwriter << "table_properties";
jwriter.StartObject();

exec
/bin/bash -lc "sed -n '205,235p' db/event_helpers.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
    const std::vector<std::shared_ptr<EventListener>>& listeners) {
if (!event_logger && listeners.empty()) {
status.PermitUncheckedError();
return;
}

if (event_logger) {
JSONWriter jwriter;
AppendCurrentTime(&jwriter);

jwriter << "job" << job_id << "event" << "table_file_deletion"
<< "file_number" << file_number << "status" << status.ToString();

jwriter.EndObject();

event_logger->Log(jwriter);
}

if (listeners.empty()) {
return;
codex
I’m through the thread-status and event-logger paths now. Several docs overstate guarantees that the code doesn’t actually make, especially where behavior depends on compile-time flags or on whether tracking/statistics were enabled before `DB::Open()`. Next I’m checking stats history, periodic dumping, and property handlers against tests.
exec
/bin/bash -lc "sed -n '1,260p' monitoring/in_memory_stats_history.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,320p' monitoring/in_memory_stats_history.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,260p' monitoring/persistent_stats_history.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,340p' monitoring/persistent_stats_history.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,260p' db/periodic_task_scheduler.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/stats_history.h"

namespace ROCKSDB_NAMESPACE {

// InMemoryStatsHistoryIterator can be used to access stats history that was
// stored by an in-memory two level std::map(DBImpl::stats_history_). It keeps
// a copy of the stats snapshot (in stats_map_) that is currently being pointed
// to, which allows the iterator to access the stats snapshot even when
// the background garbage collecting thread purges it from the source of truth
// (`DBImpl::stats_history_`). In that case, the iterator will continue to be
 succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "monitoring/in_memory_stats_history.h"

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {

InMemoryStatsHistoryIterator::~InMemoryStatsHistoryIterator() = default;

bool InMemoryStatsHistoryIterator::Valid() const { return valid_; }

Status InMemoryStatsHistoryIterator::status() const { return status_; }

 succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/db_impl/db_impl.h"
#include "rocksdb/stats_history.h"

namespace ROCKSDB_NAMESPACE {

extern const std::string kFormatVersionKeyString;
extern const std::string kCompatibleVersionKeyString;
extern const uint64_t kStatsCFCurrentFormatVersion;
extern const uint64_t kStatsCFCompatibleFormatVersion;

 succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "monitoring/persistent_stats_history.h"

#include <cstring>
#include <string>
#include <utility>

#include "db/db_impl/db_impl.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
// 10 digit seconds timestamp => [Sep 9, 2001 ~ Nov 20, 2286]
const int kNowSecondsStringLength = 10;
 succeeded in 50ms:
//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_task_scheduler.h"

#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

// `timer_mutex` is a global mutex serves 3 purposes currently:
// (1) to ensure calls to `Start()` and `Shutdown()` are serialized, as
//     they are currently not implemented in a thread-safe way; and
// (2) to ensure the `Timer::Add()`s and `Timer::Start()` run atomically, and
//     the `Timer::Cancel()`s and `Timer::Shutdown()` run atomically.
// (3) protect tasks_map_ in PeriodicTaskScheduler
// Note: It's not efficient to have a static global mutex, for
exec
/bin/bash -lc "rg -n \"StartPeriodicTaskScheduler|stats_dump_period_sec|stats_persist_period_sec|persist_stats_to_disk|GetStatsHistory\\(|GetStatsHistoryInMemory|GetStatsHistoryFromDisk|StatsHistory\" db monitoring include | head -n 300" in /home/xbw/workspace/ws36/rocksdb succeeded in 2.23s:
monitoring/stats_history_test.cc:32:class StatsHistoryTest : public DBTestBase {
monitoring/stats_history_test.cc:34:  StatsHistoryTest() : DBTestBase("stats_history_test", /*env_do_fsync=*/true) {
monitoring/stats_history_test.cc:46:        "DBImpl::StartPeriodicTaskScheduler:Init", [&](void* arg) {
monitoring/stats_history_test.cc:54:TEST_F(StatsHistoryTest, RunStatsDumpPeriodSec) {
monitoring/stats_history_test.cc:58:  options.stats_dump_period_sec = kPeriodSec;
monitoring/stats_history_test.cc:64:  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_dump_period_sec);
monitoring/stats_history_test.cc:76:  ASSERT_OK(dbfull()->SetDBOptions({{"stats_dump_period_sec", "0"}}));
monitoring/stats_history_test.cc:86:TEST_F(StatsHistoryTest, StatsPersistScheduling) {
monitoring/stats_history_test.cc:90:  options.stats_persist_period_sec = kPeriodSec;
monitoring/stats_history_test.cc:96:  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_persist_period_sec);
monitoring/stats_history_test.cc:108:  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "0"}}));
monitoring/stats_history_test.cc:118:TEST_F(StatsHistoryTest, PersistentStatsFreshInstall) {
monitoring/stats_history_test.cc:122:  options.stats_persist_period_sec = 0;
monitoring/stats_history_test.cc:129:      {{"stats_persist_period_sec", std::to_string(kPeriodSec)}}));
monitoring/stats_history_test.cc:130:  ASSERT_EQ(kPeriodSec, dbfull()->GetDBOptions().stats_persist_period_sec);
monitoring/stats_history_test.cc:139:TEST_F(StatsHistoryTest, GetStatsHistoryInMemory) {
monitoring/stats_history_test.cc:143:  options.stats_persist_period_sec = kPeriodSec;
monitoring/stats_history_test.cc:158:  std::unique_ptr<StatsHistoryIterator> stats_iter;
monitoring/stats_history_test.cc:160:      db_->GetStatsHistory(0, mock_clock_->NowSeconds() + 1, &stats_iter));
monitoring/stats_history_test.cc:163:  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "0"}}));
exec
/bin/bash -lc "sed -n '980,1165p' db/db_impl/db_impl.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '820,920p' db/db_impl/db_impl.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1500,1555p' db/db_impl/db_impl.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '2230,2268p' db/db_impl/db_impl_open.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '2550,2625p' db/db_impl/db_impl_open.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '2440,2475p' db/db_impl/db_impl_compaction_flush.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  // unsanitized.
return min_interval > UINT64_MAX / 2 ? 0 : min_interval;
}

uint64_t DBImpl::ComputeTriggerCompactionPeriod() {
// Start with a maximum period of every 12 hours.
uint64_t period_sec = 12 * 60 * 60;

// Consider DB-level options that have the DB waking up periodically anyway.
// Waking up to check for compactions at the same interval should be no
// problem, as it should be less overhead than these.
if (mutable_db_options_.stats_dump_period_sec > 0) {
period_sec = std::min(period_sec,
(uint64_t)mutable_db_options_.stats_dump_period_sec);
}
if (mutable_db_options_.stats_persist_period_sec > 0) {
period_sec = std::min(
period_sec, (uint64_t)mutable_db_options_.stats_persist_period_sec);
}

 succeeded in 51ms:
    }
}
return s;
}

// esitmate the total size of stats_history_
size_t DBImpl::EstimateInMemoryStatsHistorySize() const {
stats_history_mutex_.AssertHeld();
size_t size_total =
sizeof(std::map<uint64_t, std::map<std::string, uint64_t>>);
if (stats_history_.size() == 0) {
return size_total;
}
size_t size_per_slice =
sizeof(uint64_t) + sizeof(std::map<std::string, uint64_t>);
// non-empty map, stats_history_.begin() guaranteed to exist
for (const auto& pairs : stats_history_.begin()->second) {
size_per_slice +=
pairs.first.capacity() + sizeof(pairs.first) + sizeof(pairs.second);
}
 succeeded in 51ms:
        if (max_flushes_increased) {
env_->IncBackgroundThreadsIfNeeded(new_bg_job_limits.max_flushes,
Env::Priority::HIGH);
}
if (max_compactions_increased) {
env_->IncBackgroundThreadsIfNeeded(new_bg_job_limits.max_compactions,
Env::Priority::LOW);
}
if (offpeak_time_changed) {
versions_->ChangeOffpeakTimeOption(
new_db_options.daily_offpeak_time_utc);
}

MaybeScheduleFlushOrCompaction();
}

mutex_.Unlock();
if (new_options.stats_dump_period_sec == 0) {
s = periodic_task_scheduler_.Unregister(PeriodicTaskType::kDumpStats);
} else {
 succeeded in 51ms:
  DBOptions db_options(options);
ColumnFamilyOptions cf_options(options);
std::vector<ColumnFamilyDescriptor> column_families;
column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
if (db_options.persist_stats_to_disk) {
column_families.emplace_back(kPersistentStatsColumnFamilyName, cf_options);
}
std::vector<ColumnFamilyHandle*> handles;
Status s = DB::Open(db_options, dbname, column_families, &handles, dbptr);
if (s.ok()) {
if (db_options.persist_stats_to_disk) {
assert(handles.size() == 2);
} else {
assert(handles.size() == 1);
}
// i can delete the handle since DBImpl is always holding a reference to
// default column family
if (db_options.persist_stats_to_disk && handles[1] != nullptr) {
delete handles[1];
}
 succeeded in 51ms:
  }
if (s.ok()) {
s = impl->LogAndApplyForRecovery(recovery_ctx);
}

if (s.ok() && !impl->immutable_db_options_.write_identity_file) {
// On successful recovery, delete an obsolete IDENTITY file to avoid DB ID
// inconsistency
impl->env_->DeleteFile(IdentityFileName(impl->dbname_))
.PermitUncheckedError();
}

if (s.ok() && impl->immutable_db_options_.persist_stats_to_disk) {
impl->mutex_.AssertHeld();
s = impl->InitPersistStatsColumnFamily();
}

// After reaching the post-recovery seqno but before creating SuperVersions
// ensure seqno to time mapping is pre-populated as needed.
if (s.ok() && recovery_ctx.is_new_db_ && preserve_info.IsEnabled()) {
 succeeded in 50ms:
        nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
}
}
WaitForPendingWrites();

if (!cfd->mem()->IsEmpty() || !cached_recoverable_state_empty_.load() ||
IsRecoveryFlush(flush_reason)) {
s = SwitchMemtable(cfd, &context);
}
const uint64_t flush_memtable_id = std::numeric_limits<uint64_t>::max();
if (s.ok()) {
if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
!cached_recoverable_state_empty_.load() ||
IsRecoveryFlush(flush_reason)) {
FlushRequest req{flush_reason, {{cfd, flush_memtable_id}}};
flush_reqs.emplace_back(std::move(req));
memtable_ids_to_wait.emplace_back(
cfd->imm()->GetLatestMemTableID(false /* for_atomic_flush */));
}
if (immutable_db_options_.persist_stats_to_disk) {
exec
/bin/bash -lc 'rg -n "kPersistentStatsColumnFamilyName|InitPersistStatsColumnFamily|PersistentStatsProcessFormatVersion|__persistent_stats|___rocksdb_stats_history___|stats_history" db/db_impl/db_impl*.cc db/db_impl/db_impl.h monitoring/persistent_stats_history.* include/rocksdb/options.h' in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
monitoring/persistent_stats_history.cc.d:1:monitoring/persistent_stats_history.cc.d \
monitoring/persistent_stats_history.cc.d:2: monitoring/persistent_stats_history.o \
monitoring/persistent_stats_history.cc.d:3: monitoring/persistent_stats_history.o: \
monitoring/persistent_stats_history.cc.d:4: monitoring/persistent_stats_history.cc \
monitoring/persistent_stats_history.cc.d:5: monitoring/persistent_stats_history.h db/db_impl/db_impl.h \
monitoring/persistent_stats_history.cc.d:98: include/rocksdb/stats_history.h
monitoring/persistent_stats_history.h:12:#include "rocksdb/stats_history.h"
db/db_impl/db_impl.cc:67:#include "monitoring/in_memory_stats_history.h"
db/db_impl/db_impl.cc:71:#include "monitoring/persistent_stats_history.h"
db/db_impl/db_impl.cc:89:#include "rocksdb/stats_history.h"
db/db_impl/db_impl.cc:122:const std::string kPersistentStatsColumnFamilyName(
db/db_impl/db_impl.cc:123:    "___rocksdb_stats_history___");
db/db_impl/db_impl.cc:985:// esitmate the total size of stats_history_
db/db_impl/db_impl.cc:987:  stats_history_mutex_.AssertHeld();
db/db_impl/db_impl.cc:990:  if (stats_history_.size() == 0) {
db/db_impl/db_impl.cc:995:  // non-empty map, stats_history_.begin() guaranteed to exist
db/db_impl/db_impl.cc:996:  for (const auto& pairs : stats_history_.begin()->second) {
db/db_impl/db_impl.cc:1000:  size_total = size_per_slice * stats_history_.size();
db/db_impl/db_impl.cc:1017:  size_t stats_history_size_limit = 0;
db/db_impl/db_impl.cc:1020:    stats_history_size_limit = mutable_db_options_.stats_history_buffer_size;
exec
/bin/bash -lc "sed -n '860,1010p' db/db_impl/db_impl_open.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '470,555p' monitoring/stats_history_test.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '139,370p' monitoring/stats_history_test.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
      uint64_t options_file_size = 0;
if (options_file_number > 0) {
s = env_->GetFileSize(OptionsFileName(GetName(), options_file_number),
&options_file_size);
}
versions_->options_file_size_ = options_file_size;
}
}
return s;
}

Status DBImpl::PersistentStatsProcessFormatVersion() {
mutex_.AssertHeld();
Status s;
// persist version when stats CF doesn't exist
bool should_persist_format_version = !persistent_stats_cfd_exists_;
mutex_.Unlock();
if (persistent_stats_cfd_exists_) {
// Check persistent stats format version compatibility. Drop and recreate
// persistent stats CF if format version is incompatible
 succeeded in 50ms:
  // correctness
Close();
}

// TODO(Zhongyi): add test for different format versions

TEST_F(StatsHistoryTest, PersistentStatsCreateColumnFamilies) {
constexpr int kPeriodSec = 5;
Options options;
options.create_if_missing = true;
options.stats_persist_period_sec = kPeriodSec;
options.statistics = CreateDBStatistics();
options.persist_stats_to_disk = true;
options.env = mock_env_.get();
ASSERT_OK(TryReopen(options));
CreateColumnFamilies({"one", "two", "three"}, options);
ASSERT_OK(Put(1, "foo", "bar"));
ReopenWithColumnFamilies({"default", "one", "two", "three"}, options);
ASSERT_EQ(Get(2, "foo"), "bar");
CreateColumnFamilies({"four"}, options);
 succeeded in 50ms:
TEST_F(StatsHistoryTest, GetStatsHistoryInMemory) {
constexpr int kPeriodSec = 5;
Options options;
options.create_if_missing = true;
options.stats_persist_period_sec = kPeriodSec;
options.statistics = CreateDBStatistics();
options.env = mock_env_.get();
CreateColumnFamilies({"pikachu"}, options);
ASSERT_OK(Put("foo", "bar"));
ReopenWithColumnFamilies({"default", "pikachu"}, options);

// make sure the first stats persist to finish
dbfull()->TEST_WaitForPeriodicTaskRun(
[&] { mock_clock_->MockSleepForSeconds(kPeriodSec - 1); });

// Wait for stats persist to finish
dbfull()->TEST_WaitForPeriodicTaskRun(
[&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });

std::unique_ptr<StatsHistoryIterator> stats_iter;
Reconnecting... 1/5 (stream disconnected before completion: response.failed event received)
exec
/bin/bash -lc "sed -n '1,360p' db/internal_stats.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '360,820p' db/internal_stats.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,340p' db/internal_stats.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '340,900p' db/internal_stats.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "rg -n \"rocksdb\\.|Properties::|DBPropertyInfo|need_out_of_mutex|GetMapProperty|GetAggregatedIntProperty|fast-block-cache-entry-stats|block-cache-entry-stats|cfstats-no-file-histogram|dbstats|cf-write-stall-stats|db-write-stall-stats|oldest-snapshot|num-running-compactions|num-running-flushes|options-statistics|estimate-live-data-size\" db/internal_stats.h db/internal_stats.cc include/rocksdb/db.h include/rocksdb/options.h db/db_properties_test.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "cache/cache_entry_roles.h"
#include "db/version_set.h"
#include "rocksdb/system_clock.h"
 succeeded in 50ms:
      this->bytes_skipped_non_output_levels = 0;
this->bytes_skipped_output_level = 0;
this->bytes_read_blob = 0;
this->bytes_written = 0;
this->bytes_written_pre_comp = 0;
this->bytes_written_blob = 0;
this->bytes_moved = 0;
this->num_input_files_in_non_output_levels = 0;
this->num_input_files_in_output_level = 0;
this->num_filtered_input_files_in_non_output_levels = 0;
this->num_filtered_input_files_in_output_level = 0;
this->num_output_files = 0;
this->num_output_files_blob = 0;
this->num_input_records = 0;
this->num_dropped_records = 0;
this->num_output_records = 0;
this->count = 0;
int num_of_reasons = static_cast<int>(CompactionReason::kNumOfReasons);
for (int i = 0; i < num_of_reasons; i++) {
counts[i] = 0;
 succeeded in 51ms:
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/internal_stats.h"

#include <algorithm>
#include <cinttypes>
#include <cstddef>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
 succeeded in 51ms:
const std::string DB::Properties::kCFStatsNoFileHistogram =
rocksdb_prefix + cfstats_no_file_histogram;
const std::string DB::Properties::kCFFileHistogram =
rocksdb_prefix + cf_file_histogram;
const std::string DB::Properties::kCFWriteStallStats =
rocksdb_prefix + cf_write_stall_stats;
const std::string DB::Properties::kDBWriteStallStats =
rocksdb_prefix + db_write_stall_stats;
const std::string DB::Properties::kDBStats = rocksdb_prefix + dbstats;
const std::string DB::Properties::kLevelStats = rocksdb_prefix + levelstats;
const std::string DB::Properties::kBlockCacheEntryStats =
rocksdb_prefix + block_cache_entry_stats;
const std::string DB::Properties::kFastBlockCacheEntryStats =
rocksdb_prefix + fast_block_cache_entry_stats;
const std::string DB::Properties::kNumImmutableMemTable =
rocksdb_prefix + num_immutable_mem_table;
const std::string DB::Properties::kNumImmutableMemTableFlushed =
rocksdb_prefix + num_immutable_mem_table_flushed;
const std::string DB::Properties::kMemTableFlushPending =
rocksdb_prefix + mem_table_flush_pending;
 succeeded in 51ms:
db/internal_stats.h:31:struct DBPropertyInfo {
db/internal_stats.h:32:  bool need_out_of_mutex;
db/internal_stats.h:40:  //      be "5" for the property "rocksdb.num-files-at-level5". So far, only
db/internal_stats.h:61:const DBPropertyInfo* GetPropertyInfo(const Slice& property);
db/internal_stats.h:632:  bool GetStringProperty(const DBPropertyInfo& property_info,
db/internal_stats.h:635:  bool GetMapProperty(const DBPropertyInfo& property_info,
db/internal_stats.h:639:  bool GetIntProperty(const DBPropertyInfo& property_info, uint64_t* value,
db/internal_stats.h:642:  bool GetIntPropertyOutOfMutex(const DBPropertyInfo& property_info,
db/internal_stats.h:665:  // DBPropertyInfo struct used internally for retrieving properties.
db/internal_stats.h:666:  static const UnorderedMap<std::string, DBPropertyInfo> ppt_name_to_info;
include/rocksdb/db.h:1078:  // GetMapProperty(). Each is a "string" property for retrieval with
include/rocksdb/db.h:1079:  // GetProperty() unless noted as a "map" property, for GetMapProperty().
include/rocksdb/db.h:1084:    //  "rocksdb.num-files-at-level<N>" - returns string containing the number
include/rocksdb/db.h:1089:    //  "rocksdb.compression-ratio-at-level<N>" - returns string containing the
include/rocksdb/db.h:1096:    //  "rocksdb.stats" - returns a multi-line string containing the data
include/rocksdb/db.h:1100:    //  "rocksdb.sstables" - returns a multi-line string summarizing current
include/rocksdb/db.h:1104:    //  "rocksdb.cfstats" - Raw data from "rocksdb.cfstats-no-file-histogram"
include/rocksdb/db.h:1105:    //      and "rocksdb.cf-file-histogram" as a "map" property.
include/rocksdb/db.h:1108:    //  "rocksdb.cfstats-no-file-histogram" - returns a multi-line string with
include/rocksdb/db.h:1114:    //  "rocksdb.cf-file-histogram" - print out how many file reads to every
exec
/bin/bash -lc 'rg -n "HandleNumRunningCompactions|HandleNumRunningFlushes|HandleEstimateLiveDataSize|HandleEstimateOldestKeyTime|HandleCompactionPending|HandleMemTableFlushPending|HandleOptionsStatistics|GetPropertyHandleOptionsStatistics|GetAggregatedIntProperty|GetIntPropertyOutOfMutex|HandleOldestSnapshotTime|HandleOldestSnapshotSequence|HandleCompressionRatioAtLevelPrefix|HandleBlockCacheUsage|HandleBlockCachePinnedUsage|HandleBlockCacheCapacity" db/internal_stats.cc db/internal_stats.h db/db_impl/db_impl.cc db/db_impl/db_impl.h' in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
db/internal_stats.cc:463:         {false, &InternalStats::HandleCompressionRatioAtLevelPrefix, nullptr,
db/internal_stats.cc:512:         {false, nullptr, &InternalStats::HandleMemTableFlushPending, nullptr,
db/internal_stats.cc:515:         {false, nullptr, &InternalStats::HandleCompactionPending, nullptr,
db/internal_stats.cc:554:         {false, nullptr, &InternalStats::HandleOldestSnapshotTime, nullptr,
db/internal_stats.cc:557:         {false, nullptr, &InternalStats::HandleOldestSnapshotSequence, nullptr,
db/internal_stats.cc:566:         {true, nullptr, &InternalStats::HandleEstimateLiveDataSize, nullptr,
db/internal_stats.cc:592:         {false, nullptr, &InternalStats::HandleNumRunningFlushes, nullptr,
db/internal_stats.cc:595:         {false, nullptr, &InternalStats::HandleNumRunningCompactions, nullptr,
db/internal_stats.cc:610:         {false, nullptr, &InternalStats::HandleEstimateOldestKeyTime, nullptr,
db/internal_stats.cc:613:         {false, nullptr, &InternalStats::HandleBlockCacheCapacity, nullptr,
db/internal_stats.cc:616:         {false, nullptr, &InternalStats::HandleBlockCacheUsage, nullptr,
db/internal_stats.cc:619:         {false, nullptr, &InternalStats::HandleBlockCachePinnedUsage, nullptr,
db/internal_stats.cc:623:          &DBImpl::GetPropertyHandleOptionsStatistics}},
db/internal_stats.cc:1017:bool InternalStats::GetIntPropertyOutOfMutex(
db/internal_stats.cc:1040:bool InternalStats::HandleCompressionRatioAtLevelPrefix(std::string* value,
db/internal_stats.cc:1262:bool InternalStats::HandleMemTableFlushPending(uint64_t* value, DBImpl* /*db*/,
db/internal_stats.cc:1268:bool InternalStats::HandleNumRunningFlushes(uint64_t* value, DBImpl* db,
db/internal_stats.cc:1274:bool InternalStats::HandleCompactionPending(uint64_t* value, DBImpl* /*db*/,
db/internal_stats.cc:1283:bool InternalStats::HandleNumRunningCompactions(uint64_t* value, DBImpl* db,
db/internal_stats.cc:1394:bool InternalStats::HandleOldestSnapshotTime(uint64_t* value, DBImpl* db,
exec
/bin/bash -lc "sed -n '1010,1075p' db/internal_stats.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1035,1065p' db/internal_stats.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1250,1575p' db/internal_stats.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '4760,4825p' db/db_impl/db_impl.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  assert(value != nullptr);
assert(property_info.handle_int != nullptr &&
!property_info.need_out_of_mutex);
db->mutex_.AssertHeld();
return (this->*(property_info.handle_int))(value, db, nullptr /* version */);
}

bool InternalStats::GetIntPropertyOutOfMutex(
const DBPropertyInfo& property_info, Version* version, uint64_t* value) {
assert(value != nullptr);
assert(property_info.handle_int != nullptr &&
property_info.need_out_of_mutex);
return (this->*(property_info.handle_int))(value, nullptr /* db */, version);
}

bool InternalStats::HandleNumFilesAtLevel(std::string* value, Slice suffix) {
uint64_t level;
const auto* vstorage = cfd_->current()->storage_info();
bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
if (!ok || static_cast<int>(level) >= number_levels_) {
 succeeded in 51ms:
    *value = buf;
return true;
}
}

bool InternalStats::HandleCompressionRatioAtLevelPrefix(std::string* value,
Slice suffix) {
uint64_t level;
const auto* vstorage = cfd_->current()->storage_info();
bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
if (!ok || level >= static_cast<uint64_t>(number_levels_)) {
return false;
}
*value = std::to_string(
vstorage->GetEstimatedCompressionRatioAtLevel(static_cast<int>(level)));
return true;
}

bool InternalStats::HandleLevelStats(std::string* value, Slice /*suffix*/) {
char buf[1000];
 succeeded in 51ms:
                                               Version* /*version*/) {
*value = cfd_->imm()->NumNotFlushed();
return true;
}

bool InternalStats::HandleNumImmutableMemTableFlushed(uint64_t* value,
DBImpl* /*db*/,
Version* /*version*/) {
*value = cfd_->imm()->NumFlushed();
return true;
}

bool InternalStats::HandleMemTableFlushPending(uint64_t* value, DBImpl* /*db*/,
Version* /*version*/) {
*value = (cfd_->imm()->IsFlushPending() ? 1 : 0);
return true;
}

bool InternalStats::HandleNumRunningFlushes(uint64_t* value, DBImpl* db,
Version* /*version*/) {
 succeeded in 51ms:
      InstrumentedMutexLock l(&mutex_);
return cfd->internal_stats()->GetIntProperty(property_info, value, this);
}
} else {
SuperVersion* sv = nullptr;
if (is_locked) {
mutex_.Unlock();
}
sv = GetAndRefSuperVersion(cfd);

bool ret = cfd->internal_stats()->GetIntPropertyOutOfMutex(
property_info, sv->current, value);

ReturnAndCleanupSuperVersion(cfd, sv);
if (is_locked) {
mutex_.Lock();
}

return ret;
}
exec
/bin/bash -lc "sed -n '4825,4875p' db/db_impl/db_impl.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
      if (ret) {
aggregator->Add(cfd, value);
} else {
ret = false;
break;
}
}
}
*aggregated_value = aggregator->Aggregate();
return ret;
}

SuperVersion* DBImpl::GetAndRefSuperVersion(ColumnFamilyData* cfd) {
// TODO(ljin): consider using GetReferencedSuperVersion() directly
return cfd->GetThreadLocalSuperVersion(this);
}

// REQUIRED: this function should only be called on the write thread or if the
// mutex is held.
SuperVersion* DBImpl::GetAndRefSuperVersion(uint32_t column_family_id) {
exec
/bin/bash -lc "rg -n \"GetEstimatedCompressionRatioAtLevel\\(\" db version_set table include -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb exited 2 in 2.06s:
rg: version_set: No such file or directory (os error 2)
db/internal_stats.cc:1049:      vstorage->GetEstimatedCompressionRatioAtLevel(static_cast<int>(level)));
db/version_set.h:600:  double GetEstimatedCompressionRatioAtLevel(int level) const;
db/version_set.cc:2319:double VersionStorageInfo::GetEstimatedCompressionRatioAtLevel(
exec
/bin/bash -lc "sed -n '590,615p' db/version_set.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '2310,2335p' db/version_set.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
    }
assert(accumulated_raw_key_size_ + accumulated_raw_value_size_ > 0);
assert(accumulated_file_size_ > 0);
return accumulated_raw_value_size_ / accumulated_num_non_deletions_ *
accumulated_file_size_ /
(accumulated_raw_key_size_ + accumulated_raw_value_size_);
}

uint64_t GetEstimatedActiveKeys() const;

double GetEstimatedCompressionRatioAtLevel(int level) const;

// re-initializes the index that is used to offset into
// files_by_compaction_pri_
// to find the next compaction candidate file.
void ResetNextCompactionIndex(int level) {
next_file_to_compact_by_size_[level] = 0;
}

const InternalKeyComparator* InternalComparator() const {
 succeeded in 51ms:
    if (multiplier >= maximum_multiplier) {
return std::numeric_limits<uint64_t>::max();
}
return static_cast<uint64_t>(est * multiplier);
} else {
return est;
}
}

double VersionStorageInfo::GetEstimatedCompressionRatioAtLevel(
int level) const {
assert(level < num_levels_);
uint64_t sum_file_size_bytes = 0;
uint64_t sum_data_size_bytes = 0;
for (auto* file_meta : files_[level]) {
auto raw_size = file_meta->raw_key_size + file_meta->raw_value_size;
// Check if the table property is properly initialized. It might not be
// because in `UpdateAccumulatedStats` we limit the maximum number of
// properties to read once.
if (raw_size > 0) {
exec
/bin/bash -lc "sed -n '2335,2350p' db/version_set.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
    return -1.0;
}
return static_cast<double>(sum_data_size_bytes) / sum_file_size_bytes;
}

void Version::AddIterators(const ReadOptions& read_options,
const FileOptions& soptions,
MergeIteratorBuilder* merge_iter_builder,
bool allow_unprepared_value) {
assert(storage_info_.finalized_);

for (int level = 0; level < storage_info_.num_non_empty_levels(); level++) {
AddIteratorsForLevel(read_options, soptions, merge_iter_builder, level,
allow_unprepared_value);
}
}
exec
/bin/bash -lc "sed -n '1,220p' monitoring/instrumented_mutex.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,180p' monitoring/instrumented_mutex.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '580,650p' db/perf_context_test.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '140,220p' db/db_statistics_test.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/instrumented_mutex.h"

#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
namespace {
#ifndef NPERF_CONTEXT
Statistics* stats_for_report(SystemClock* clock, Statistics* stats) {
if (clock != nullptr && stats != nullptr &&
stats->get_stats_level() > kExceptTimeForMutex) {
return stats;
} else {
 succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/thread_status.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
class InstrumentedCondVar;

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutex {
 succeeded in 51ms:
    ASSERT_EQ(iter->value().ToString(), value);
hist_seek.Add(get_perf_context()->user_key_comparison_count);
}

std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
for (iter->SeekToFirst(); iter->Valid();) {
get_perf_context()->Reset();
iter->Next();
hist_next.Add(get_perf_context()->user_key_comparison_count);
}
ASSERT_OK(iter->status());
if (FLAGS_verbose) {
std::cout << "Seek:\n"
<< hist_seek.ToString() << "Next:\n"
<< hist_next.ToString();
}
}

TEST_F(PerfContextTest, DBMutexLockCounter) {
int stats_code[] = {0, static_cast<int>(DB_MUTEX_WAIT_MICROS)};
 succeeded in 50ms:
  CreateAndReopenWithCF({"pikachu"}, options);
const uint64_t kMutexWaitDelay = 100;
ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
kMutexWaitDelay);
ASSERT_OK(Put("hello", "rocksdb"));
ASSERT_EQ(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), 0);
ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

TEST_F(DBStatisticsTest, MutexWaitStats) {
Options options = CurrentOptions();
options.create_if_missing = true;
options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
options.statistics->set_stats_level(StatsLevel::kAll);
CreateAndReopenWithCF({"pikachu"}, options);
const uint64_t kMutexWaitDelay = 100;
ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
kMutexWaitDelay);
ASSERT_OK(Put("hello", "rocksdb"));
ASSERT_GE(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), kMutexWaitDelay);
exec
/bin/bash -lc "sed -n '1,240p' monitoring/iostats_context_test.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '6200,6735p' db/db_test2.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '820,900p' db/perf_context_test.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1110,1155p' db/perf_context_test.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/iostats_context.h"

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

TEST(IOStatsContextTest, ToString) {
get_iostats_context()->Reset();
get_iostats_context()->bytes_read = 12345;

std::string zero_included = get_iostats_context()->ToString();
ASSERT_NE(std::string::npos, zero_included.find("= 0"));
ASSERT_NE(std::string::npos, zero_included.find("= 12345"));

std::string zero_excluded = get_iostats_context()->ToString(true);
 succeeded in 50ms:
    }
}
}

TEST_F(DBTest2, LastLevelTemperature) {
class TestListener : public EventListener {
public:
void OnFileReadFinish(const FileOperationInfo& info) override {
UpdateFileTemperature(info);
}

void OnFileWriteFinish(const FileOperationInfo& info) override {
UpdateFileTemperature(info);
}

void OnFileFlushFinish(const FileOperationInfo& info) override {
UpdateFileTemperature(info);
}

void OnFileSyncFinish(const FileOperationInfo& info) override {
 succeeded in 51ms:
    }
};

// Verify counters when reading from memtable
verify();

// Verify counters when reading from table files
ASSERT_OK(db->Flush(FlushOptions()));

verify();
}

TEST_F(PerfContextTest, WriteMemtableTimePerfLevel) {
// Write and check time
ASSERT_OK(DestroyDB(kDbName, Options()));
auto db = OpenDb();

SetPerfLevel(PerfLevel::kEnableWait);
PerfContext* perf_ctx = get_perf_context();
perf_ctx->Reset();
 succeeded in 51ms:
  ASSERT_EQ(
2,
(*(get_perf_context()->level_to_perf_context))[3].block_cache_miss_count);
ASSERT_EQ(
4,
(*(get_perf_context()->level_to_perf_context))[1].block_cache_miss_count);
std::string zero_excluded = get_perf_context()->ToString(true);
ASSERT_NE(std::string::npos,
zero_excluded.find("bloom_filter_useful = 1@level5, 2@level7"));
ASSERT_NE(std::string::npos,
zero_excluded.find("bloom_filter_full_positive = 1@level0"));
ASSERT_NE(std::string::npos,
zero_excluded.find("bloom_filter_full_true_positive = 1@level2"));
ASSERT_NE(std::string::npos,
zero_excluded.find("block_cache_hit_count = 1@level0, 5@level2"));
ASSERT_NE(std::string::npos,
zero_excluded.find("block_cache_miss_count = 4@level1, 2@level3"));
}

TEST_F(PerfContextTest, CPUTimer) {
exec
/bin/bash -lc "rg -n \"disable_iostats|IOSTATS_SET_DISABLE|logger_nanos|BackupEngine|logv|IOSTATS_ADD\\(|file_io_stats_by_temperature|unknown_non_last_level|unknown_last_level|HOT_FILE_READ_BYTES|LAST_LEVEL_READ_BYTES\" -g '"'!**/*.o'"' ." in /home/xbw/workspace/ws36/rocksdb succeeded in 604ms:
./checkpoint_prompt.txt:7:4. Backup engine: BackupEngine, incremental backups, backup to remote storage
./fix_checkpoint_prompt.txt:15:Source: `utilities/backup/backup_engine.cc:1168-1181` only creates `shared/` or `shared_checksum/` when `share_table_files` is true. There is no `LATEST_BACKUP` constant, path, or marker file in the current BackupEngine implementation.
./fix_checkpoint_prompt.txt:16:Impact: The on-disk layout section does not match what BackupEngine actually creates.
./fix_checkpoint_prompt.txt:20:Source: `utilities/backup/backup_engine.cc:1012-1045` acquires read locks on BackupEngine objects only; `utilities/backup/backup_engine.cc:2145-2155` and `utilities/backup/backup_engine.cc:2355-2495` restore by copy/create, not hard-link.
./fix_checkpoint_prompt.txt:83:16. MISLEADING: The comparison table says BackupEngine "always copies data" and "copies all WAL files".
./fix_checkpoint_prompt.txt:93:18. MISLEADING: Incomplete backup cleanup is not tied to ordinary `BackupEngine::Open()`.
./fix_checkpoint_prompt.txt:133:Overall assessment: the checkpoint half is closer to current code than the backup half, but the doc still has several incorrect invariants and examples. The BackupEngine sections contain the largest concentration of hard errors and stale behavior.
./fix_monitoring_prompt.txt:21:- `docs/components/monitoring.md:376-403` says temperature-based I/O stats are only populated when using tiered storage with temperature-annotated SSTs. Current code also records `unknown_non_last_level_*` and `unknown_last_level_*` on reads of `Temperature::kUnknown` files, and the hot/warm/cool/cold/ice counters are keyed off each file's temperature, not a separate "tiered storage mode" switch. Source: `file/random_access_file_reader.cc:56-116`.
./fix_monitoring_prompt.txt:50:- `docs/components/monitoring.md:405-414` overstates what `disable_iostats` does and why it exists. The header comment says BackupEngine is the reason some IOStats counters remain independent of PerfLevel; `disable_iostats` is a backdoor for avoiding pollution of IOStats counters. It only gates the `IOSTATS_*` macros and does not suppress Statistics tickers such as `HOT_FILE_READ_BYTES` or `LAST_LEVEL_READ_BYTES`. Source: `include/rocksdb/iostats_context.h:116-121`, `monitoring/iostats_context_imp.h:15-46`, `file/random_access_file_reader.cc:56-116`.
./logging/env_logger.h:60:      IOSTATS_SET_DISABLE(true);
./logging/env_logger.h:65:      IOSTATS_SET_DISABLE(false);
./logging/env_logger.h:109:    IOSTATS_TIMER_GUARD(logger_nanos);
./file/line_file_reader.cc:59:    IOSTATS_ADD(bytes_read, result.size());
./file/writable_file_writer.cc:652:    IOSTATS_ADD(bytes_written, allowed);
./file/writable_file_writer.cc:743:  IOSTATS_ADD(bytes_written, left);
./file/writable_file_writer.cc:859:    IOSTATS_ADD(bytes_written, size);
./file/writable_file_writer.cc:967:  IOSTATS_ADD(bytes_written, left);
./ARCHITECTURE.md:144:| [checkpoint.md](docs/components/checkpoint.md) | Checkpoint, BackupEngine |
./logging/auto_roll_logger.cc:229:  // Note that logv itself is not mutex protected to allow maximum concurrency,
./file/random_access_file_reader.cc:58:  IOSTATS_ADD(bytes_read, size);
exec
/bin/bash -lc "sed -n '1,150p' file/random_access_file_reader.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '48,125p' logging/env_logger.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  }

private:
// A guard to prepare file operations, such as mutex and skip
// I/O context.
class FileOpGuard {
public:
explicit FileOpGuard(EnvLogger& logger)
: logger_(logger), prev_perf_level_(GetPerfLevel()) {
// Preserve iostats not to pollute writes from user writes. We might
// need a better solution than this.
SetPerfLevel(PerfLevel::kDisable);
IOSTATS_SET_DISABLE(true);
logger.mutex_.Lock();
}
~FileOpGuard() {
logger_.mutex_.Unlock();
IOSTATS_SET_DISABLE(false);
SetPerfLevel(prev_perf_level_);
}
 succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/random_access_file_reader.h"

#include <algorithm>
#include <mutex>

#include "file/file_util.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "table/format.h"
#include "test_util/sync_point.h"
exec
/bin/bash -lc "rg -n \"FILE_READ_.*MICROS|FILE_WRITE_.*MICROS|COMPRESSION_TIMES_NANOS|DECOMPRESSION_TIMES_NANOS|HISTOGRAM_ENUM_MAX\" include/rocksdb/statistics.h monitoring/statistics.cc file/* db/* -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb succeeded in 2.75s:
db/perf_context_test.cc:192:                  Histograms::HISTOGRAM_ENUM_MAX, &elapsed);
file/random_access_file_reader.cc:29:      return Histograms::FILE_READ_FLUSH_MICROS;
file/random_access_file_reader.cc:31:      return Histograms::FILE_READ_COMPACTION_MICROS;
file/random_access_file_reader.cc:33:      return Histograms::FILE_READ_DB_OPEN_MICROS;
file/random_access_file_reader.cc:41:        return Histograms::FILE_READ_GET_MICROS;
file/random_access_file_reader.cc:43:        return Histograms::FILE_READ_MULTIGET_MICROS;
file/random_access_file_reader.cc:45:        return Histograms::FILE_READ_DB_ITERATOR_MICROS;
file/random_access_file_reader.cc:47:        return Histograms::FILE_READ_VERIFY_DB_CHECKSUM_MICROS;
file/random_access_file_reader.cc:49:        return Histograms::FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS;
file/random_access_file_reader.cc:54:  return Histograms::HISTOGRAM_ENUM_MAX;
db/version_set.cc:6138:            io_tracer_, nullptr, Histograms::HISTOGRAM_ENUM_MAX /* hist_type */,
file/writable_file_writer.h:177:      Histograms hist_type = Histograms::HISTOGRAM_ENUM_MAX,
db/db_impl/db_impl_open.cc:337:        Histograms::HISTOGRAM_ENUM_MAX /* hist_type */,
db/db_impl/db_impl_open.cc:2383:        Histograms::HISTOGRAM_ENUM_MAX /* hist_type */, listeners, nullptr,
file/random_access_file_reader.h:125:      uint32_t hist_type = Histograms::HISTOGRAM_ENUM_MAX,
include/rocksdb/statistics.h:597: *  1. Add the new histogram before HISTOGRAM_ENUM_MAX.
include/rocksdb/statistics.h:627:  FILE_READ_FLUSH_MICROS,
include/rocksdb/statistics.h:628:  FILE_READ_COMPACTION_MICROS,
include/rocksdb/statistics.h:629:  FILE_READ_DB_OPEN_MICROS,
include/rocksdb/statistics.h:632:  FILE_READ_GET_MICROS,
exec
/bin/bash -lc "sed -n '1,220p' monitoring/file_read_sample.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "db/version_edit.h"
#include "test_util/sync_point.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
static const uint32_t kFileReadSampleRate = 1024;
static const uint32_t kFileReadNextSampleRate =
kFileReadSampleRate * 64;  // Must be kept a power of 2

inline bool should_sample_file_read() {
bool result = (Random::GetTLSInstance()->Next() % kFileReadSampleRate == 307);
TEST_SYNC_POINT_CALLBACK("should_sample_file_read:override", &result);
return result;
}
exec
/bin/bash -lc "sed -n '2050,2145p' db/db_properties_test.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc 'rg -n "kIntStatsWriteStallMicros|DBWriteStallStats|CFWriteStallStats|DumpDBStatsWriteStall|DumpCFStatsWriteStall|WRITE_STALL" db/internal_stats.cc db/write_stall_stats.h db/*write* db/*properties*' in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
      db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));
ASSERT_EQ(kSize2 + kSize3, value);

// Check size after release.
block_cache->Release(item2);
block_cache->Release(item3);
ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheCapacity, &value));
ASSERT_EQ(kCapacity, value);
ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheUsage, &value));
// item2 will be evicted, while item3 remain in cache after release.
ASSERT_EQ(kSize3, value);
ASSERT_TRUE(
db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));
ASSERT_EQ(0, value);
}

TEST_F(DBPropertiesTest, GetMapPropertyDbStats) {
auto mock_clock = std::make_shared<MockSystemClock>(env_->GetSystemClock());
CompositeEnvWrapper env(env_, mock_clock);

exec
/bin/bash -lc "sed -n '2230,2335p' db/db_properties_test.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
db/db_properties_test.cc:2248:    ASSERT_TRUE(dbfull()->GetMapProperty(DB::Properties::kDBWriteStallStats,
db/db_properties_test.cc:2258:          handles_[cf], DB::Properties::kCFWriteStallStats, &cf_values));
db/db_properties_test.cc:2300:      EXPECT_TRUE(dbfull()->GetMapProperty(DB::Properties::kDBWriteStallStats,
db/db_properties_test.cc:2311:            handles_[cf], DB::Properties::kCFWriteStallStats, &cf_values));
db/db_properties_test.cc:2321:            handles_[cf], DB::Properties::kCFWriteStallStats, &cf_values));
db/internal_stats.cc:81:        {InternalStats::kIntStatsWriteStallMicros,
db/internal_stats.cc:344:const std::string DB::Properties::kCFWriteStallStats =
db/internal_stats.cc:346:const std::string DB::Properties::kDBWriteStallStats =
db/internal_stats.cc:481:        {DB::Properties::kCFWriteStallStats,
db/internal_stats.cc:482:         {false, &InternalStats::HandleCFWriteStallStats, nullptr,
db/internal_stats.cc:483:          &InternalStats::HandleCFWriteStallStatsMap, nullptr}},
db/internal_stats.cc:487:        {DB::Properties::kDBWriteStallStats,
db/internal_stats.cc:488:         {false, &InternalStats::HandleDBWriteStallStats, nullptr,
db/internal_stats.cc:489:          &InternalStats::HandleDBWriteStallStatsMap, nullptr}},
db/internal_stats.cc:1135:bool InternalStats::HandleCFWriteStallStats(std::string* value,
db/internal_stats.cc:1137:  DumpCFStatsWriteStall(value);
db/internal_stats.cc:1141:bool InternalStats::HandleCFWriteStallStatsMap(
db/internal_stats.cc:1158:bool InternalStats::HandleDBWriteStallStats(std::string* value,
db/internal_stats.cc:1160:  DumpDBStatsWriteStall(value);
db/internal_stats.cc:1164:bool InternalStats::HandleDBWriteStallStatsMap(
 succeeded in 51ms:
}
TEST_F(DBPropertiesTest, GetMapPropertyWriteStallStats) {
Options options = CurrentOptions();
CreateAndReopenWithCF({"heavy_write_cf"}, options);

for (auto test_cause : {WriteStallCause::kWriteBufferManagerLimit,
WriteStallCause::kMemtableLimit}) {
if (test_cause == WriteStallCause::kWriteBufferManagerLimit) {
options.write_buffer_manager.reset(
new WriteBufferManager(100000, nullptr, true));
} else if (test_cause == WriteStallCause::kMemtableLimit) {
options.max_write_buffer_number = 2;
options.disable_auto_compactions = true;
}
ReopenWithColumnFamilies({"default", "heavy_write_cf"}, options);

// Assert initial write stall stats are all 0
std::map<std::string, std::string> db_values;
ASSERT_TRUE(dbfull()->GetMapProperty(DB::Properties::kDBWriteStallStats,
&db_values));
exec
/bin/bash -lc 'rg -n "num_reads_sampled|1024|65536|FileReadSampleRate|read sampling|Sampled" db table file monitoring | head -n 200' in /home/xbw/workspace/ws36/rocksdb succeeded in 2.69s:
db/db_clip_test.cc:19:  options.write_buffer_size = 10 * 1024 * 1024;
db/db_clip_test.cc:27:  int32_t value_size = 10 * 1024;  // 10 KB
db/db_tailing_iter_test.cc:77:    std::string value(1024, 'a');
db/db_tailing_iter_test.cc:116:    std::string value(1024, 'a');
db/db_tailing_iter_test.cc:173:  const uint64_t k150KB = 150 * 1024;
db/db_tailing_iter_test.cc:200:  std::string value(1024, 'a');
db/db_tailing_iter_test.cc:345:    std::string value(1024, 'A');
db/db_merge_operand_test.cc:417:  const int kNumOperands = 1024;
db/db_merge_operand_test.cc:418:  const int kOperandLen = 1024;
file/writable_file_writer.h:211:    buf_.AllocateNewBuffer(std::min((size_t)65536, max_buffer_size_));
file/writable_file_writer.cc:424:        1024 * 1024;                                // recent 1MB is not synced.
file/writable_file_writer.cc:425:    const uint64_t kBytesAlignWhenSync = 4 * 1024;  // Align 4KB.
monitoring/stats_history_test.cc:584:  options.write_buffer_size = 1024 * 1024 * 10;  // 10 Mb
monitoring/file_read_sample.h:12:static const uint32_t kFileReadSampleRate = 1024;
monitoring/file_read_sample.h:14:    kFileReadSampleRate * 64;  // Must be kept a power of 2
monitoring/file_read_sample.h:17:  bool result = (Random::GetTLSInstance()->Next() % kFileReadSampleRate == 307);
monitoring/file_read_sample.h:32:  meta->stats.num_reads_sampled.fetch_add(kFileReadSampleRate,
monitoring/file_read_sample.h:38:      kFileReadSampleRate, std::memory_order_relaxed);
table/table_test.cc:673:  std::vector<int> restart_intervals = {16, 1, 1024};
table/table_test.cc:1363:                     testing::Values(0, 128 * 1024, 512 * 1024,
exec
/bin/bash -lc "rg -n \"CreateIntPropertyAggregator|IntPropertyAggregator|Aggregate\\(\" db/db_impl/db_impl.cc db/db_impl/db_impl.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
db/db_impl/db_impl.cc:4808:  auto aggregator = CreateIntPropertyAggregator(property);
db/db_impl/db_impl.cc:4833:  *aggregated_value = aggregator->Aggregate();
exec
/bin/bash -lc "rg -n \"class .*PropertyAggregator|CreateIntPropertyAggregator\\(\" db include util -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb succeeded in 2.02s:
db/db_impl/db_impl.cc:4808:  auto aggregator = CreateIntPropertyAggregator(property);
db/internal_stats.h:945:class IntPropertyAggregator {
db/internal_stats.h:960:std::unique_ptr<IntPropertyAggregator> CreateIntPropertyAggregator(
db/internal_stats.cc:2182:class SumPropertyAggregator : public IntPropertyAggregator {
db/internal_stats.cc:2200:class BlockCachePropertyAggregator : public IntPropertyAggregator {
db/internal_stats.cc:2229:std::unique_ptr<IntPropertyAggregator> CreateIntPropertyAggregator(
exec
/bin/bash -lc "sed -n '930,980p' db/internal_stats.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '2180,2245p' db/internal_stats.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:

// This is a rolling count of the number of sorted runs being processed by
// currently running compactions. Other metrics are only incremented, but this
// metric is also decremented. Additionally, we also do not want to reset this
// count to zero at a periodic interval.
std::atomic<uint64_t> num_running_compaction_sorted_runs_;

const int number_levels_;
SystemClock* clock_;
ColumnFamilyData* cfd_;
uint64_t started_at_;
};

// IntPropertyAggregator aggregates an integer property across all column
// families.
class IntPropertyAggregator {
public:
IntPropertyAggregator() {}
virtual ~IntPropertyAggregator() {}

 succeeded in 51ms:
namespace {

class SumPropertyAggregator : public IntPropertyAggregator {
public:
SumPropertyAggregator() : aggregated_value_(0) {}
virtual ~SumPropertyAggregator() override = default;

void Add(ColumnFamilyData* cfd, uint64_t value) override {
(void)cfd;
aggregated_value_ += value;
}

uint64_t Aggregate() const override { return aggregated_value_; }

private:
uint64_t aggregated_value_;
};

// A block cache may be shared by multiple column families.
// BlockCachePropertyAggregator ensures that the same cache is only added once.
exec
/bin/bash -lc "rg -n \"BLOCK_CACHE_HIT|BLOCK_CACHE_MISS|BLOCK_CACHE_COMPRESSION_DICT_HIT|BLOCK_CACHE_COMPRESSION_DICT_MISS|RecordTick\\(.*BLOCK_CACHE\" table db cache file -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb succeeded in 2.65s:
table/table_test.cc:3970:    block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_MISS);
table/table_test.cc:3971:    block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_HIT);
table/sst_file_reader_test.cc:856:  uint64_t cache_hits = options.statistics->getTickerCount(BLOCK_CACHE_HIT);
table/sst_file_reader_test.cc:857:  uint64_t cache_misses = options.statistics->getTickerCount(BLOCK_CACHE_MISS);
cache/tiered_secondary_cache_test.cc:642:  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 3u);
cache/tiered_secondary_cache_test.cc:657:  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 6u);
cache/tiered_secondary_cache_test.cc:672:  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 6u);
cache/tiered_secondary_cache_test.cc:689:  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 8u);
cache/tiered_secondary_cache_test.cc:704:  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 8u);
cache/tiered_secondary_cache_test.cc:1039:  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 4u);
table/get_context.cc:119:    RecordTick(statistics_, BLOCK_CACHE_HIT, get_context_stats_.num_cache_hit);
table/get_context.cc:122:    RecordTick(statistics_, BLOCK_CACHE_INDEX_HIT,
table/get_context.cc:126:    RecordTick(statistics_, BLOCK_CACHE_DATA_HIT,
table/get_context.cc:130:    RecordTick(statistics_, BLOCK_CACHE_FILTER_HIT,
table/get_context.cc:134:    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_HIT,
table/get_context.cc:138:    RecordTick(statistics_, BLOCK_CACHE_INDEX_MISS,
table/get_context.cc:142:    RecordTick(statistics_, BLOCK_CACHE_FILTER_MISS,
table/get_context.cc:146:    RecordTick(statistics_, BLOCK_CACHE_DATA_MISS,
table/get_context.cc:150:    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_MISS,
table/get_context.cc:154:    RecordTick(statistics_, BLOCK_CACHE_BYTES_READ,
exec
/bin/bash -lc 'rg -n "EnablePerLevelPerfContext|DisablePerLevelPerfContext|ClearPerLevelPerfContext" monitoring/perf_context.cc include/rocksdb/perf_context.h' in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
monitoring/perf_context.cc:207:  ClearPerLevelPerfContext();
monitoring/perf_context.cc:244:    ClearPerLevelPerfContext();
monitoring/perf_context.cc:304:void PerfContext::EnablePerLevelPerfContext() {
monitoring/perf_context.cc:311:void PerfContext::DisablePerLevelPerfContext() {
monitoring/perf_context.cc:315:void PerfContext::ClearPerLevelPerfContext() {
include/rocksdb/perf_context.h:317:  void EnablePerLevelPerfContext();
include/rocksdb/perf_context.h:320:  void DisablePerLevelPerfContext();
include/rocksdb/perf_context.h:323:  void ClearPerLevelPerfContext();
exec
/bin/bash -lc "sed -n '296,325p' monitoring/perf_context.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
#undef PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER
}
std::string str = ss.str();
str.erase(str.find_last_not_of(", ") + 1);
return str;
#endif
}

void PerfContext::EnablePerLevelPerfContext() {
if (level_to_perf_context == nullptr) {
level_to_perf_context = new std::map<uint32_t, PerfContextByLevel>();
}
per_level_perf_context_enabled = true;
}

void PerfContext::DisablePerLevelPerfContext() {
per_level_perf_context_enabled = false;
}

void PerfContext::ClearPerLevelPerfContext() {
exec
/bin/bash -lc "sed -n '5210,5435p' db/db_test.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  int op_count = 0;
std::vector<ThreadStatus> thread_list;
EXPECT_OK(env->GetThreadList(&thread_list));
for (const auto& thread : thread_list) {
if (thread.operation_type == op_type) {
op_count++;
}
}
if (op_count != expected_count) {
fprintf(stderr, "op_count: %d, expected_count %d\n", op_count,
expected_count);
for (const auto& thread : thread_list) {
fprintf(stderr, "thread id: %" PRIu64 ", thread status: %s, cf_name %s\n",
thread.thread_id,
thread.GetOperationName(thread.operation_type).c_str(),
thread.cf_name.c_str());
}
}
return op_count == expected_count;
}
exec
/bin/bash -lc "rg -n \"RegisterThread\\(|SetEnableTracking\\(|ThreadStatusUtil::RegisterThread|ThreadStatusUtil::SetEnableTracking\" env db util port -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb succeeded in 2.72s:
util/threadpool_imp.cc:349:  ThreadStatusUtil::RegisterThread(tp->GetHostEnv(), thread_type);
util/thread_list_test.cc:46:    Env::Default()->GetThreadStatusUpdater()->SetEnableTracking(true);
db/flush_job.cc:145:  ThreadStatusUtil::SetEnableTracking(db_options_.enable_thread_tracking);
db/db_impl/db_impl_open.cc:2261:  ThreadStatusUtil::SetEnableTracking(db_options.enable_thread_tracking);
db/compaction/compaction_job.cc:204:  ThreadStatusUtil::SetEnableTracking(db_options_.enable_thread_tracking);
exec
/bin/bash -lc "sed -n '330,380p' util/threadpool_imp.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,140p' util/thread_list_test.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
  ThreadStatus::ThreadType thread_type = ThreadStatus::NUM_THREAD_TYPES;
switch (tp->GetThreadPriority()) {
case Env::Priority::HIGH:
thread_type = ThreadStatus::HIGH_PRIORITY;
break;
case Env::Priority::LOW:
thread_type = ThreadStatus::LOW_PRIORITY;
break;
case Env::Priority::BOTTOM:
thread_type = ThreadStatus::BOTTOM_PRIORITY;
break;
case Env::Priority::USER:
thread_type = ThreadStatus::USER;
break;
case Env::Priority::TOTAL:
assert(false);
return;
}
assert(thread_type != ThreadStatus::NUM_THREAD_TYPES);
ThreadStatusUtil::RegisterThread(tp->GetHostEnv(), thread_type);
 succeeded in 50ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <condition_variable>
#include <mutex>

#include "monitoring/thread_status_updater.h"
#include "rocksdb/db.h"
#include "test_util/testharness.h"

#ifndef NROCKSDB_THREAD_STATUS

namespace ROCKSDB_NAMESPACE {

class SimulatedBackgroundTask {
public:
SimulatedBackgroundTask(
const void* db_key, const std::string& db_name, const void* cf_key,
exec
/bin/bash -lc "rg -n \"SetThreadOperation\\(ThreadStatus::OperationType::OP_(GET|MULTIGET|DBITERATOR|VERIFY_DB_CHECKSUM|VERIFY_FILE_CHECKSUMS|GETENTITY|MULTIGETENTITY|GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST|COMPACTION|FLUSH|DBOPEN)\" db table file utilities -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb succeeded in 3.17s:
db/db_impl/db_impl_open.cc:2262:  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_DBOPEN);
exec
/bin/bash -lc "rg -n \"OP_GET|OP_MULTIGET|OP_DBITERATOR|OP_VERIFY_DB_CHECKSUM|OP_VERIFY_FILE_CHECKSUMS|OP_GETENTITY|OP_MULTIGETENTITY|OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST|OP_COMPACTION|OP_FLUSH\" db table file utilities monitoring include -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb succeeded in 4.01s:
monitoring/thread_status_impl.cc:74:    case ThreadStatus::OP_COMPACTION:
monitoring/thread_status_impl.cc:79:    case ThreadStatus::OP_FLUSH:
monitoring/thread_status_impl.cc:93:    case OP_COMPACTION:
monitoring/thread_status_impl.cc:96:    case OP_FLUSH:
monitoring/thread_status_impl.cc:105:    if (op_type == OP_COMPACTION && i == COMPACTION_INPUT_OUTPUT_LEVEL) {
monitoring/thread_status_impl.cc:109:    } else if (op_type == OP_COMPACTION && i == COMPACTION_PROP_FLAGS) {
monitoring/thread_status_util_debug.cc:33:    case ThreadStatus::OperationType::OP_FLUSH:
monitoring/thread_status_util_debug.cc:35:    case ThreadStatus::OperationType::OP_COMPACTION:
monitoring/thread_status_util_debug.cc:39:    case ThreadStatus::OperationType::OP_GET:
monitoring/thread_status_util_debug.cc:41:    case ThreadStatus::OperationType::OP_MULTIGET:
monitoring/thread_status_util_debug.cc:43:    case ThreadStatus::OperationType::OP_DBITERATOR:
monitoring/thread_status_util_debug.cc:45:    case ThreadStatus::OperationType::OP_VERIFY_DB_CHECKSUM:
monitoring/thread_status_util_debug.cc:47:    case ThreadStatus::OperationType::OP_VERIFY_FILE_CHECKSUMS:
monitoring/thread_status_util_debug.cc:49:    case ThreadStatus::OperationType::OP_GETENTITY:
monitoring/thread_status_util_debug.cc:51:    case ThreadStatus::OperationType::OP_MULTIGETENTITY:
monitoring/thread_status_util_debug.cc:54:        OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST:
db/listener_test.cc:238:      if (thread_status.operation_type == ThreadStatus::OP_FLUSH ||
db/listener_test.cc:239:          thread_status.operation_type == ThreadStatus::OP_COMPACTION) {
include/rocksdb/thread_status.h:49:    OP_COMPACTION,
include/rocksdb/thread_status.h:50:    OP_FLUSH,
exec
/bin/bash -lc "sed -n '1,220p' util/thread_operation.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the structures for thread operation and state.
// Thread operations are used to describe high level action of a
// thread such as doing compaction or flush, while thread state
// are used to describe lower-level action such as reading /
// writing a file or waiting for a mutex.  Operations and states
// are designed to be independent.  Typically, a thread usually involves
// in one operation and one state at any specific point in time.

#pragma once

#include <string>

#include "rocksdb/thread_status.h"

namespace ROCKSDB_NAMESPACE {
exec
/bin/bash -lc "rg -n \"void Log\\(Logger\\*|Log\\(Logger\\* logger|info_log_level|Header.*Logv|Logger::Logv\" logging include/rocksdb/env.h logging/* include/rocksdb/status.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 573ms:
logging/event_logger.h:193:  static void Log(Logger* logger, const JSONWriter& jwriter);
include/rocksdb/env.h:1363:  // DEBUG_LEVEL when compiled in debug mode. See DBOptions::info_log_level.
include/rocksdb/env.h:1489:void Log(Logger* info_log, const char* format, ...)
logging/auto_roll_logger.cc:194:void AutoRollLogger::Logv(const char* format, va_list ap) {
logging/auto_roll_logger.cc:316:        options.keep_log_file_num, options.info_log_level);
logging/auto_roll_logger.cc:361:    (*logger)->SetInfoLogLevel(options.info_log_level);
logging/event_logger_test.cc:18:  using Logger::Logv;
logging/auto_roll_logger.h:34:  using Logger::Logv;
logging/auto_roll_logger.o: binary file matches (found "\0" byte around offset 7)
logging/env_logger.h:107:  using Logger::Logv;
logging/auto_roll_logger_test.cc:238:  options.info_log_level = InfoLogLevel::FATAL_LEVEL;
logging/event_logger.cc:49:void EventLogger::Log(Logger* logger, const JSONWriter& jwriter) {
logging/event_logger.cc:49:void EventLogger::Log(Logger* logger, const JSONWriter& jwriter) {
logging/auto_roll_logger_test.cc:238:  options.info_log_level = InfoLogLevel::FATAL_LEVEL;
logging/env_logger.h:107:  using Logger::Logv;
logging/event_logger_test.cc:18:  using Logger::Logv;
logging/auto_roll_logger.cc:194:void AutoRollLogger::Logv(const char* format, va_list ap) {
logging/auto_roll_logger.cc:316:        options.keep_log_file_num, options.info_log_level);
logging/auto_roll_logger.cc:361:    (*logger)->SetInfoLogLevel(options.info_log_level);
logging/auto_roll_logger.h:34:  using Logger::Logv;
exec
/bin/bash -lc "sed -n '1470,1515p' include/rocksdb/env.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '230,255p' logging/auto_roll_logger_test.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '186,235p' logging/auto_roll_logger.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Warn(const std::shared_ptr<Logger>& info_log, const char* format, ...)
ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Error(const std::shared_ptr<Logger>& info_log, const char* format, ...)
ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Fatal(const std::shared_ptr<Logger>& info_log, const char* format, ...)
ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

// Log the specified data to *info_log if info_log is non-nullptr.
// The default info log level is InfoLogLevel::INFO_LEVEL.
void Log(const std::shared_ptr<Logger>& info_log, const char* format, ...)
ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

void LogFlush(Logger* info_log);

void Log(const InfoLogLevel log_level, Logger* info_log, const char* format,
...) ROCKSDB_PRINTF_FORMAT_ATTR(3, 4);

// The default info log level is InfoLogLevel::INFO_LEVEL.
void Log(Logger* info_log, const char* format, ...)
 succeeded in 51ms:

RollLogFileByTimeTest(default_env->GetFileSystem(), nsc, &logger, time,
kSampleMessage + ":RollLogFileByTime");
}

TEST_F(AutoRollLoggerTest, SetInfoLogLevel) {
InitTestDb();
Options options;
options.info_log_level = InfoLogLevel::FATAL_LEVEL;
options.max_log_file_size = 1024;
std::shared_ptr<Logger> logger;
ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
auto* auto_roll_logger = dynamic_cast<AutoRollLogger*>(logger.get());
ASSERT_NE(nullptr, auto_roll_logger);
ASSERT_EQ(InfoLogLevel::FATAL_LEVEL, auto_roll_logger->GetInfoLogLevel());
ASSERT_EQ(InfoLogLevel::FATAL_LEVEL,
auto_roll_logger->TEST_inner_logger()->GetInfoLogLevel());
auto_roll_logger->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);
ASSERT_EQ(InfoLogLevel::DEBUG_LEVEL, auto_roll_logger->GetInfoLogLevel());
ASSERT_EQ(InfoLogLevel::DEBUG_LEVEL, logger->GetInfoLogLevel());
 succeeded in 50ms:
  }

va_list args;
va_start(args, format);
logger_->Logv(format, args);
va_end(args);
}

void AutoRollLogger::Logv(const char* format, va_list ap) {
std::shared_ptr<Logger> logger;
{
MutexLock l(&mutex_);
assert(GetStatus().ok());
if (!logger_) {
return;
}
if ((kLogFileTimeToRoll > 0 && LogExpired()) ||
(kMaxLogFileSize > 0 && logger_->GetLogFileSize() >= kMaxLogFileSize)) {
RollLogFile();
Status s = ResetLogger();
exec
/bin/bash -lc 'rg -n "class Logger|InfoLogLevel GetInfoLogLevel|void SetInfoLogLevel|virtual void Logv" include/rocksdb/env.h logging/env_logger.h logging/auto_roll_logger.h' in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
include/rocksdb/env.h:53:class Logger;
include/rocksdb/env.h:1358:class Logger {
include/rocksdb/env.h:1395:  virtual void Logv(const char* /* format */, va_list /* ap */) {
include/rocksdb/env.h:1403:  virtual void Logv(const InfoLogLevel log_level, const char* format,
include/rocksdb/env.h:1409:  virtual InfoLogLevel GetInfoLogLevel() const { return log_level_; }
include/rocksdb/env.h:1410:  virtual void SetInfoLogLevel(const InfoLogLevel log_level) {
include/rocksdb/env.h:2019:class LoggerWrapper : public Logger {
include/rocksdb/env.h:2036:  InfoLogLevel GetInfoLogLevel() const override {
include/rocksdb/env.h:2039:  void SetInfoLogLevel(const InfoLogLevel log_level) override {
logging/auto_roll_logger.h:78:  InfoLogLevel GetInfoLogLevel() const override {
logging/auto_roll_logger.h:87:  void SetInfoLogLevel(const InfoLogLevel log_level) override {
exec
/bin/bash -lc "sed -n '1358,1415p' include/rocksdb/env.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
class Logger {
public:
static constexpr size_t kDoNotSupportGetLogFileSize = SIZE_MAX;

// Set to INFO_LEVEL when RocksDB is compiled in release mode, and
// DEBUG_LEVEL when compiled in debug mode. See DBOptions::info_log_level.
static const InfoLogLevel kDefaultLogLevel;

explicit Logger(const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL)
: closed_(false), log_level_(log_level) {}
// No copying allowed
Logger(const Logger&) = delete;
void operator=(const Logger&) = delete;

virtual ~Logger();

// Because Logger is typically a shared object, Close() may or may not be
// called before the object is destroyed, but is recommended to reveal any
// final errors in finishing outstanding writes. No other functions are
// supported after calling Close(), and the Logger is considered closed
exec
/bin/bash -lc "rg -n \"void Log\\(Logger\\* info_log, const char\\* format|Log\\(const InfoLogLevel log_level, Logger\\* info_log\" -g '"'!**/*.o'"' ." in /home/xbw/workspace/ws36/rocksdb succeeded in 580ms:
./env/env.cc:919:void Log(Logger* info_log, const char* format, ...) {
./env/env.cc:970:void Log(const InfoLogLevel log_level, Logger* info_log, const char* format,
./include/rocksdb/env.h:1485:void Log(const InfoLogLevel log_level, Logger* info_log, const char* format,
./include/rocksdb/env.h:1489:void Log(Logger* info_log, const char* format, ...)
exec
/bin/bash -lc "sed -n '919,980p' env/env.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
void Log(Logger* info_log, const char* format, ...) {
va_list ap;
va_start(ap, format);
Logv(info_log, format, ap);
va_end(ap);
}

void Logger::Logv(const InfoLogLevel log_level, const char* format,
va_list ap) {
static const char* kInfoLogLevelNames[5] = {"DEBUG", "INFO", "WARN", "ERROR",
"FATAL"};
if (log_level < log_level_) {
return;
}

if (log_level == InfoLogLevel::INFO_LEVEL) {
// Doesn't print log level if it is INFO level.
// This is to avoid unexpected performance regression after we add
// the feature of log level. All the logs before we add the feature
// are INFO level. We don't want to add extra costs to those existing
codex
I have enough code context to start separating hard errors from mere omissions. I’m doing one more pass through tests and recent history now, mainly to catch behaviors the docs miss entirely: stats-history deltas, temperature-specific I/O accounting, and which thread-status operations are actually instrumented today.
exec
/bin/bash -lc "git log --oneline --since='2024-01-01' -- db/db_impl/db_impl.cc db/db_impl/db_impl_open.cc monitoring/persistent_stats_history.cc monitoring/in_memory_stats_history.cc monitoring/stats_history_test.cc include/rocksdb/options.h include/rocksdb/stats_history.h | head -n 80" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "git log --oneline --since='2024-01-01' -- include/rocksdb/iostats_context.h file/random_access_file_reader.cc db/db_test2.cc | head -n 60" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "git log --oneline --since='2024-01-01' -- monitoring/thread_status_updater.cc monitoring/thread_status_util.cc include/rocksdb/thread_status.h db/db_test.cc util/threadpool_imp.cc | head -n 60" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "git log --oneline --since='2024-01-01' -- monitoring/perf_level.cc include/rocksdb/perf_context.h monitoring/perf_context.cc db/perf_context_test.cc | head -n 60" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "git log --oneline --since='2024-01-01' -- db/internal_stats.cc db/internal_stats.h include/rocksdb/db.h db/db_properties_test.cc | head -n 80" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
63c86160c Add OnBackgroundJobPressureChanged listener callback (#14474)
b6f498b2c Add verify_manifest_content_on_close option (#14451)
ec2290391 Support all operation types in User Defined Index (UDI) interface (#14399)
e27114b13 Skip flush during recovery if read_only (#14440)
3aa706c2b Enforce WriteBufferManager during WAL recovery (#14305)
f25fb41da Add option to validate sst files in the background on DB open (#14322)
da2c3c0ee Fix & improve compaction trigger on a "quiet" DB (#14396)
29819f37e Remove deprecated `ReadOptions::managed`, `ColumnFamilyOptions::snap_refresh_nanos (#14350)
3556c2205 Remove deprecated option skip_checking_sst_file_sizes_on_db_open (#14346)
d3817f058 Remove deprecated DB::Open raw pointer variants (and more) (#14335)
653fd9c65 Bug fix for bg error recovery in TransactionDB (#14313)
09bda51c5 Propagate file_checksum through FileOptions on NewRandomAccessFile (#14321)
feffb6730 Replace Prefetch Logic in BlockBasedTableIterator with IODispatcher. (#14255)
656b734a5 Support abort background compaction jobs. (#14227)
b89d290c2 Add MultiScan statistics (#14248)
ea5e64922 Fix an infinite compaction loop bug with udt (#14228)
4bf2bcdbb Allow setting options for multiple column families (#14201)
9fbb68be1 Api to get SST file with key ranges for a particular level and key range (startKey, EndKey)rocksdb [Internal version] (#14009)
b33c547b0 Add trivial move support in CompactFiles API (#14112)
37176a4a4 Auto-tune manifest file size (#14076)
 succeeded in 483ms:
9a9cc1e2e Fix uninitialized wal_in_db_path_ in read-only and compacted DB open paths (#14419)
d3817f058 Remove deprecated DB::Open raw pointer variants (and more) (#14335)
5f692d747 Fall back to sync read when async IO is unavailable (#14337)
88aff40c9 New io stats for unknown file temperature last vs. non-last (#14243)
37176a4a4 Auto-tune manifest file size (#14076)
1c8a01272 Add kCool Temperature (#14000)
2620c8563 Support async IO for MultiScan (#13932)
67af5bdc3 Add Temperature::kIce (#13927)
799079cac Handle drop column family version edit in file checksum retriever (#13832)
f9f7ad702 Move some tests from db_test(2) to compression_test (#13763)
78c83ac1e Publish/support format_version=7, related enhancements (#13713)
fdc2970d3 Connect custom compression to crash test and ObjectLibrary (#13710)
9d490593d Preliminary support for custom compression algorithms (#13659)
4bdfb7e7d support canceling ongoing CompactFiles (#13687)
2dcfc5475 Mixed compressor adding RandomCompressorManager to db_stress_test (#13691)
fccc88189 Implement MixedCompressor that Round robins on compression algorithm (#13647)
8dc3d77b5 Experimental, preliminary support for custom CompressionManager (#13626)
acab405fc propagate request_id from app -> Rocks -> FS (#13616)
7c9b58068 Big refactor for preliminary custom compression API (#13540)
7f271a3fa Require ZSTD >= 1.4.0, greatly simplifying related code (#13362)
 succeeded in 483ms:
42eff8b63 Add new heauristic 'num_collapsible_entry_reads_sampled' (#14434)
3b66de3fa Fix out of disk unit test issue (#14425)
f25fb41da Add option to validate sst files in the background on DB open (#14322)
d3817f058 Remove deprecated DB::Open raw pointer variants (and more) (#14335)
d8b1893c9 DROP support for block-based SST format_version < 2 (#14315)
656b734a5 Support abort background compaction jobs. (#14227)
9fbb68be1 Api to get SST file with key ranges for a particular level and key range (startKey, EndKey)rocksdb [Internal version] (#14009)
befa6b805 Fix and check for potential ODR violations (#14096)
f9f7ad702 Move some tests from db_test(2) to compression_test (#13763)
78c83ac1e Publish/support format_version=7, related enhancements (#13713)
9d490593d Preliminary support for custom compression algorithms (#13659)
e3a91ec1e Add copy constructor and assignment operator to IODebugContext (#13690)
acab405fc propagate request_id from app -> Rocks -> FS (#13616)
613e1a9a3 Verify flush output file record count + minor clean up (#13556)
476a98ca3 Add a new GetNewestUserDefinedTimestamp API (#13547)
2a0ee4ddd Refactor wal related naming and more (#13490)
48eb64678 Mark MaxMemCompactionLevel() deprecated (#13503)
82794e0a4 Deprecate RangePtr, favor new RangeOpt and OptSlice (#13481)
934cf2d40 Implement the DB::GetPropertiesOfTablesForLevels API (#13469)
2e175124d Rename Env::IOActivity::kReadManifest (#13471)
 succeeded in 483ms:
b23fc77ac Add per-block-type block read byte perf counters (#14473)
d3817f058 Remove deprecated DB::Open raw pointer variants (and more) (#14335)
871f79d6e Reformat source files (#14331)
c8bc2b63f Add time measuring metrics for file ingestion in PerfContext (#13219)
c72e79a26 Standardize on clang-format version 18 (#13233)
f75f033d7 initialize member variables in `PerfContext`'s default constructor (#12581)
98d8a8562 New PerfContext counters for block cache bytes read (#12459)
4aed229fa Add `write_memtable_time` to perf level `kEnableWait` (#12394)
ec5b1be18 Deflake `PerfContextTest.CPUTimer` (#12252)
 succeeded in 482ms:
ea83aa4 Remove the force mode for EnableFileDeletions API (#12337)
10d02456b Add support to bulk load external files with user-defined timestamps (#12343)
95b41eec6 Fix potential incorrect result for duplicate key in MultiGet (#12295)
2b4245559 Don't warn on (recursive) disable file deletion (#12310)
4e60663b3 Remove unnecessary, confusing 'extern' (#12300)
3079a7e7c Remove extra semi colon from internal_repo_rocksdb/repo/db/internal_stats.h (#12278)
7fe93162c Log pending compaction bytes in a couple places (#12267)
5a9ecf661 Automated modernization (#12210)
exec
/bin/bash -lc "printf '%s
' '#include <iostream>' '#include \"include/rocksdb/perf_context.h\"' '#include \"include/rocksdb/iostats_context.h\"' 'int main(){ using namespace ROCKSDB_NAMESPACE; std::cout << sizeof(PerfContext) << \"\\n\" << sizeof(IOStatsContext) << \"\\n\" << sizeof(PerfContextByLevel) << \"\\n\"; }' | g++ -std=c++20 -I. -x c++ - -o /tmp/monitoring_sizes && /tmp/monitoring_sizes" in /home/xbw/workspace/ws36/rocksdb exited 1 in 51ms:
In file included from <stdin>:2:
include/rocksdb/perf_context.h:13:10: fatal error: rocksdb/perf_level.h: No such file or directory
13 | #include "rocksdb/perf_level.h"
|          ^~~~~~~~~~~~~~~~~~~~~~
compilation terminated.
exec
/bin/bash -lc "printf '%s
' '#include <iostream>' '#include \"rocksdb/perf_context.h\"' '#include \"rocksdb/iostats_context.h\"' 'int main(){ using namespace ROCKSDB_NAMESPACE; std::cout << sizeof(PerfContext) << \"\\n\" << sizeof(IOStatsContext) << \"\\n\" << sizeof(PerfContextByLevel) << \"\\n\"; }' | g++ -std=c++20 -Iinclude -I. -x c++ - -o /tmp/monitoring_sizes && /tmp/monitoring_sizes" in /home/xbw/workspace/ws36/rocksdb succeeded in 199ms:
912
224
56
exec
/bin/bash -lc "printf '%s
' '#include <iostream>' '#define private public' '#include \"monitoring/statistics_impl.h\"' '#undef private' 'int main(){ using namespace ROCKSDB_NAMESPACE; std::cout << sizeof(StatisticsImpl::StatisticsData) << \"\\n\"; }' | g++ -std=c++20 -Iinclude -I. -x c++ - -o /tmp/statistics_size && /tmp/statistics_size" in /home/xbw/workspace/ws36/rocksdb exited 1 in 607ms:
In file included from monitoring/statistics_impl.h:16,
from <stdin>:3:
./util/core_local.h: In member function ‘std::pair<T*, long unsigned int> rocksdb::CoreLocalArray<T>::AccessElementAndIndex() const’:
./util/core_local.h:68:15: error: ‘port’ has not been declared
68 |   int cpuid = port::PhysicalCoreID();
|               ^~~~
In file included from monitoring/statistics_impl.h:17,
from <stdin>:3:
./util/mutexlock.h: At global scope:
./util/mutexlock.h:37:22: error: ‘port’ has not been declared
37 |   explicit MutexLock(port::Mutex* mu) : mu_(mu) { this->mu_->Lock(); }
|                      ^~~~
./util/mutexlock.h:37:33: error: expected ‘)’ before ‘*’ token
37 |   explicit MutexLock(port::Mutex* mu) : mu_(mu) { this->mu_->Lock(); }
|                     ~           ^
|                                 )
./util/mutexlock.h:45:3: error: ‘port’ does not name a type
45 |   port::Mutex* const mu_;
|   ^~~~
./util/mutexlock.h: In destructor ‘rocksdb::MutexLock::~MutexLock()’:
exec
/bin/bash -lc "rg -n \"WITH_PERF_CONTEXT|WITH_IOSTATS_CONTEXT|NPERF_CONTEXT|NIOSTATS_CONTEXT|NROCKSDB_THREAD_STATUS\" CMakeLists.txt cmake Makefile buckifier src.mk -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb succeeded in 1.33s:
CMakeLists.txt:360:option(WITH_IOSTATS_CONTEXT "Enable IO stats context" ON)
CMakeLists.txt:361:if (NOT WITH_IOSTATS_CONTEXT)
CMakeLists.txt:362:  add_definitions(-DNIOSTATS_CONTEXT)
CMakeLists.txt:365:option(WITH_PERF_CONTEXT "Enable perf context" ON)
CMakeLists.txt:366:if (NOT WITH_PERF_CONTEXT)
CMakeLists.txt:367:  add_definitions(-DNPERF_CONTEXT)
exec
/bin/bash -lc "rg -n \"NROCKSDB_THREAD_STATUS|thread status\" CMakeLists.txt cmake src.mk Makefile buckifier -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb exited 1 in 1.30s:

exec
/bin/bash -lc "rg -n \"stats_history_buffer_size|persist_stats_to_disk|enable_thread_tracking|Sanitize\" db/db_options_test.cc options db include -g '"'!**/*.o'"'" in /home/xbw/workspace/ws36/rocksdb succeeded in 1.93s:
db/db_options_test.cc:74:        SanitizeCfOptions(db_options, /*read_only*/ false, options);
db/db_options_test.cc:84:    auto sanitized_options = SanitizeOptions(dbname_, db_options);
db/db_options_test.cc:924:TEST_F(DBOptionsTest, SanitizeDelayedWriteRate) {
db/db_options_test.cc:936:TEST_F(DBOptionsTest, SanitizeUniversalTTLCompaction) {
db/db_options_test.cc:966:TEST_F(DBOptionsTest, SanitizeTtlDefault) {
db/db_options_test.cc:982:TEST_F(DBOptionsTest, SanitizeFIFOPeriodicCompaction) {
options/options_test.cc:176:      {"persist_stats_to_disk", "false"},
options/options_test.cc:177:      {"stats_history_buffer_size", "69"},
options/options_test.cc:358:  ASSERT_EQ(new_db_opt.persist_stats_to_disk, false);
options/options_test.cc:359:  ASSERT_EQ(new_db_opt.stats_history_buffer_size, 69U);
options/options_test.cc:2511:      {"persist_stats_to_disk", "false"},
options/options_test.cc:2512:      {"stats_history_buffer_size", "69"},
options/options_test.cc:2698:  ASSERT_EQ(new_db_opt.persist_stats_to_disk, false);
options/options_test.cc:2699:  ASSERT_EQ(new_db_opt.stats_history_buffer_size, 69U);
options/options_settable_test.cc:427:      "enable_thread_tracking=false;"
options/options_settable_test.cc:439:      "persist_stats_to_disk=true;"
options/options_settable_test.cc:440:      "stats_history_buffer_size=14159;"
options/options_helper.cc:122:  options.persist_stats_to_disk = immutable_db_options.persist_stats_to_disk;
options/options_helper.cc:123:  options.stats_history_buffer_size =
options/options_helper.cc:124:      mutable_db_options.stats_history_buffer_size;
exec
/bin/bash -lc "sed -n '80,110p' options/db_options.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '205,220p' options/db_options.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '430,445p' options/options_settable_test.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '320,335p' options/db_options.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
         {offsetof(struct MutableDBOptions, delayed_write_rate),
OptionType::kUInt64T, OptionVerificationType::kNormal,
OptionTypeFlags::kMutable}},
{"max_total_wal_size",
{offsetof(struct MutableDBOptions, max_total_wal_size),
OptionType::kUInt64T, OptionVerificationType::kNormal,
OptionTypeFlags::kMutable}},
{"delete_obsolete_files_period_micros",
{offsetof(struct MutableDBOptions,
delete_obsolete_files_period_micros),
OptionType::kUInt64T, OptionVerificationType::kNormal,
OptionTypeFlags::kMutable}},
{"stats_dump_period_sec",
{offsetof(struct MutableDBOptions, stats_dump_period_sec),
OptionType::kUInt, OptionVerificationType::kNormal,
OptionTypeFlags::kMutable}},
{"stats_persist_period_sec",
{offsetof(struct MutableDBOptions, stats_persist_period_sec),
OptionType::kUInt, OptionVerificationType::kNormal,
OptionTypeFlags::kMutable}},
 succeeded in 50ms:
          OptionTypeFlags::kNone}},
{"disableDataSync",
{0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
OptionTypeFlags::kNone}},
{"disable_data_sync",  // for compatibility
{0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
OptionTypeFlags::kNone}},
{"enable_thread_tracking",
{offsetof(struct ImmutableDBOptions, enable_thread_tracking),
OptionType::kBoolean, OptionVerificationType::kNormal,
OptionTypeFlags::kNone}},
{"error_if_exists",
{offsetof(struct ImmutableDBOptions, error_if_exists),
OptionType::kBoolean, OptionVerificationType::kNormal,
OptionTypeFlags::kNone}},
{"experimental_allow_mempurge",
 succeeded in 50ms:
      "log_file_time_to_roll=3097;"
"max_background_flushes=35;"
"create_if_missing=false;"
"error_if_exists=true;"
"delayed_write_rate=4294976214;"
"manifest_preallocation_size=1222;"
"allow_mmap_writes=false;"
"stats_dump_period_sec=70127;"
"stats_persist_period_sec=54321;"
"persist_stats_to_disk=true;"
"stats_history_buffer_size=14159;"
"allow_fallocate=true;"
"allow_mmap_reads=false;"
"use_direct_reads=false;"
"use_direct_io_for_flush_and_compaction=false;"
"max_log_file_size=4607;"
 succeeded in 50ms:
         {offsetof(struct ImmutableDBOptions, WAL_size_limit_MB),
OptionType::kUInt64T, OptionVerificationType::kNormal,
OptionTypeFlags::kNone}},
{"WAL_ttl_seconds",
{offsetof(struct ImmutableDBOptions, WAL_ttl_seconds),
OptionType::kUInt64T, OptionVerificationType::kNormal,
OptionTypeFlags::kNone}},
{"persist_stats_to_disk",
{offsetof(struct ImmutableDBOptions, persist_stats_to_disk),
OptionType::kBoolean, OptionVerificationType::kNormal,
OptionTypeFlags::kNone}},
{"fail_if_options_file_error",
{0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
OptionTypeFlags::kNone}},
{"enable_pipelined_write",
{offsetof(struct ImmutableDBOptions, enable_pipelined_write),
exec
/bin/bash -lc "rg -n \"kPeriodicCFStats|no_cf_change_period_since_dump_|DumpStats\\(|DumpCFStatsNoFileHistogram\\(|has_cf_change_since_dump_\" db/internal_stats.cc db/db_impl/db_impl.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
db/internal_stats.cc:453:const std::string InternalStats::kPeriodicCFStats =
db/internal_stats.cc:472:        {InternalStats::kPeriodicCFStats,
db/internal_stats.cc:657:      has_cf_change_since_dump_(true),
db/internal_stats.cc:1093:  bool has_change = has_cf_change_since_dump_;
db/internal_stats.cc:1107:    no_cf_change_period_since_dump_ = 0;
db/internal_stats.cc:1108:    has_cf_change_since_dump_ = false;
db/internal_stats.cc:1109:  } else if (no_cf_change_period_since_dump_++ > 0) {
db/internal_stats.cc:1111:    if (no_cf_change_period_since_dump_ == kMaxNoChangePeriodSinceDump) {
db/internal_stats.cc:1113:      no_cf_change_period_since_dump_ = 0;
db/internal_stats.cc:1118:  DumpCFStatsNoFileHistogram(/*is_periodic=*/true, value);
db/internal_stats.cc:1125:  DumpCFStatsNoFileHistogram(/*is_periodic=*/false, value);
db/internal_stats.cc:1963:  DumpCFStatsNoFileHistogram(/*is_periodic=*/false, value);
db/internal_stats.cc:1967:void InternalStats::DumpCFStatsNoFileHistogram(bool is_periodic,
db/db_impl/db_impl.cc:248:                                   [this]() { this->DumpStats(); });
db/db_impl/db_impl.cc:1152:void DBImpl::DumpStats() {
db/db_impl/db_impl.cc:1197:    property = &InternalStats::kPeriodicCFStats;
exec
/bin/bash -lc "sed -n '1085,1135p' db/internal_stats.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:

bool InternalStats::HandleCFStats(std::string* value, Slice /*suffix*/) {
DumpCFStats(value);
return true;
}

bool InternalStats::HandleCFStatsPeriodic(std::string* value,
Slice /*suffix*/) {
bool has_change = has_cf_change_since_dump_;
if (!has_change) {
// If file histogram changes, there is activity in this period too.
uint64_t new_histogram_num = 0;
for (int level = 0; level < number_levels_; level++) {
new_histogram_num += file_read_latency_[level].num();
}
new_histogram_num += blob_file_read_latency_.num();
if (new_histogram_num != last_histogram_num) {
has_change = true;
last_histogram_num = new_histogram_num;
}
exec
/bin/bash -lc "sed -n '1,220p' fix_monitoring_prompt.txt" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
You are a senior RocksDB engineer. A Codex review found issues in docs/components/monitoring.md.

Here are the Codex findings:

---
Review of `docs/components/monitoring.md` against current source

I checked the doc against the current repo state and focused on concrete API/behavior mismatches. Findings are categorized below.

## ERROR

- `docs/components/monitoring.md:300-303` says per-level `bloom_filter_full_positive` means "Bloom false positives." In current source it counts all positive full-filter results; only `bloom_filter_full_true_positive` narrows that to positives where the data actually exists. Source: `include/rocksdb/perf_context.h:37-42`.

- `docs/components/monitoring.md:216-247`, `docs/components/monitoring.md:325-326`, and `docs/components/monitoring.md:907-912` assign the wrong enable levels to several metrics.
`get_from_memtable_count`, `seek_on_memtable_count`, `next_on_memtable_count`, `prev_on_memtable_count`, and the bloom hit/miss counters are count metrics enabled at `PerfLevel::kEnableCount`.
`write_wal_time`, `write_scheduling_flushes_compactions_time`, and `write_pre_and_post_process_time` use `PERF_TIMER_GUARD`, so they start at `PerfLevel::kEnableTimeExceptForMutex`, not `kEnableWait`.
Wall-clock IOStats timers use `IOSTATS_TIMER_GUARD`, so they also start at `PerfLevel::kEnableTimeExceptForMutex`; CPU I/O timers start at `PerfLevel::kEnableTimeAndCPUTimeExceptForMutex`.
The overhead section also says histograms are enabled at `StatsLevel::kExceptHistogramOrTimers`, but `recordInHistogram()` returns early at that exact level.
Source: `monitoring/perf_context_imp.h:79-99`, `monitoring/perf_step_timer.h:15-27`, `monitoring/iostats_context_imp.h:35-44`, `monitoring/statistics.cc:514-523`.

context compacted
warning: Heads up: Long threads and multiple compactions can cause the model to be less accurate. Start a new thread when possible to keep threads small and targeted.
codex
Reviewing the monitoring docs and the prompt requirements first, then I’ll write the requested review file with findings tied to the current code paths.
exec
/bin/bash -lc 'ls docs/reviews' in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,220p' /tmp/review_codex_prompt_monitoring.md" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
cache_debates.md
cache_fix_summary.md
cache_review_cc.md
cache_review_codex.md
compaction_debates.md
compaction_fix_summary.md
compaction_review_cc.md
compaction_review_codex.md
file_io_review_cc.md
filter_debates.md
filter_fix_summary.md
filter_review_cc.md
filter_review_codex.md
flush_debates.md
flush_fix_summary.md
flush_review_cc.md
flush_review_codex.md
iterator_debates.md
iterator_fix_summary.md
iterator_review_cc.md
 succeeded in 50ms:
# Review: monitoring Documentation

You are a senior RocksDB engineer performing a thorough, adversarial review of AI-generated documentation. Your job is to find every error, omission, and weakness. Assume the documentation is wrong until you verify it against the code. A human maintainer will read your review to decide what to fix — be precise enough that they can act on each issue without re-investigating.

## Review Scope

Review all files in `docs/components/monitoring/`:
- `index.md` (the index file)
- All chapter files (`NN_*.md`)

## Your Unique Review Angle

Another reviewer (Claude Code) is also reviewing this documentation in parallel. To maximize total coverage, focus especially on:
- **Cross-component boundary accuracy**: Does the doc correctly describe how monitoring interacts with other RocksDB subsystems? Trace the actual call chains.
- **Option semantics precision**: For every option mentioned, verify the exact semantics — default value, valid range, interaction with other options, behavior when set to 0 or empty, sanitization in `Sanitize()` methods.
- **Concurrency correctness**: Are locking, ordering, and thread-safety claims accurate? Check the actual mutex/atomic usage.
- **Test coverage alignment**: Do the documented behaviors match what the unit tests and stress tests actually exercise? Are there tested edge cases the docs miss?

## Review Procedure

exec
/bin/bash -lc 'git status --short' in /home/xbw/workspace/ws36/rocksdb succeeded in 497ms:
 M docs/components/README.md
D docs/components/cache.md
D docs/components/compaction.md
D docs/components/file_io.md
D docs/components/filter.md
D docs/components/flush.md
D docs/components/iterator.md
D docs/components/listener.md
D docs/components/memtable.md
D docs/components/monitoring.md
D docs/components/options.md
D docs/components/public_api_write.md
D docs/components/secondary_cache.md
D docs/components/snapshot.md
D docs/components/sst_table_format.md
D docs/components/threading_model.md
D docs/components/wal.md
D docs/components/wide_column.md
?? blob_db_prompt.txt
?? checkpoint_prompt.txt
codex
I’ve confirmed the prompt and the workspace state. Next I’m pulling exact doc wording for the already-verified issues, plus a quick pass over the existing Claude review so this one stays focused on distinct, code-backed findings.
exec
/bin/bash -lc "sed -n '1,260p' docs/reviews/monitoring_review_cc.md" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "for f in docs/components/monitoring/*.md; do printf '%s\\n' \"--- "'$f"; sed -n '"'1,220p' \""'$f"; done' in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
# Review: monitoring -- Claude Code

## Summary
Overall quality rating: **good**

The monitoring documentation covers a broad surface area well, with 11 chapters spanning statistics, PerfContext, IOStatsContext, DB properties, compaction stats, event logging, thread status, stats history, instrumented mutex, and best practices. The architecture is clearly explained and the structure is logical. However, there are several factual errors -- most notably an incorrect persistent stats column family name, a wrong claim about EventLogger log level independence, a wrong PerfLevel count, and a misordered ThreadType enum. The documentation also has completeness gaps around recently added metrics and some enum values.

## Correctness Issues

### [STALE] BLOCK_CACHE_HIT invariant comment in source is stale
- **File:** `index.md` line 40, `01_statistics_framework.md` "Important" note
- **Claim:** "BLOCK_CACHE_HIT equals the sum of BLOCK_CACHE_INDEX_HIT + BLOCK_CACHE_FILTER_HIT + BLOCK_CACHE_DATA_HIT + BLOCK_CACHE_COMPRESSION_DICT_HIT"
- **Reality:** The documentation is actually CORRECT. The source code comment at `include/rocksdb/statistics.h` is stale -- it says `BLOCK_CACHE_HIT == INDEX_HIT + FILTER_HIT + DATA_HIT` without COMPRESSION_DICT_HIT, but `UpdateCacheHitMetrics()` in `block_based_table_reader.cc` increments BLOCK_CACHE_HIT for ALL block types including compression dict. The doc correctly includes COMPRESSION_DICT_HIT; the source comment needs updating.
- **Source:** `table/block_based/block_based_table_reader.cc`, `UpdateCacheHitMetrics()` -- BLOCK_CACHE_HIT is incremented at line 280 before the block-type switch, so it includes all types.
- **Fix:** No doc fix needed. The source comment in `statistics.h` should be updated to include COMPRESSION_DICT_HIT. One subtlety the doc could note: `BlockType::kRangeDeletion` and other uncategorized block types fall into the `default` case in `UpdateCacheHitMetrics()` and are counted as DATA_HIT.

### [WRONG] PerfLevel count in index.md
- **File:** `index.md` line 29
- **Claim:** "Seven levels from kDisable to kEnableTime"
- **Reality:** There are 6 levels from kDisable(1) to kEnableTime(6). kUninitialized(0) and kOutOfBounds(7) are sentinel values, not usable levels.
 succeeded in 50ms:
--- docs/components/monitoring/01_statistics_framework.md
# Statistics Framework

**Files:** `include/rocksdb/statistics.h`, `monitoring/statistics.cc`, `monitoring/statistics_impl.h`

## Overview

The `Statistics` object provides global, cumulative metrics across the entire database lifetime. It tracks two types of metrics: tickers (simple counters) and histograms (value distributions). Statistics must be explicitly enabled by setting `Options::statistics` before opening the database.

## Creating and Using Statistics

Create a `Statistics` object via `CreateDBStatistics()` (see `include/rocksdb/statistics.h`). Pass it to `Options::statistics` before `DB::Open()`. The same `Statistics` object can be shared across multiple column families within a single DB, or even across multiple DBs (in which case values are aggregated across all DBs).

**Note:** The typical overhead of Statistics is 5-10% depending on workload and `StatsLevel` configuration.

Key query methods on the `Statistics` class:

| Method | Description |
|--------|-------------|
| `getTickerCount(type)` | Get cumulative counter value |
exec
/bin/bash -lc "rg -n \"enable_thread_tracking|GetThreadList|operation type|stats_history|persist_stats_to_disk|__stats_history__|EventLogger|info_log_level|JSON|disable_iostats|Reset\\(|DB_MUTEX_WAIT_MICROS|kAll|need_out_of_mutex|compression-ratio-at-level|AggregatedIntProperty|WPreComp|periodic|num_collapsible_entry_reads_sampled|kEnableCount|nanosecond|Key Invariants\" docs/components/monitoring -S" in /home/xbw/workspace/ws36/rocksdb succeeded in 610ms:
docs/components/monitoring/07_event_logger.md:7:`EventLogger` logs important database events as structured JSON to the LOG file. Events include flush, compaction, table file creation, and recovery. The output is designed for machine parsing and post-mortem analysis.
docs/components/monitoring/07_event_logger.md:11:Each event is written as a single log line with the prefix `EVENT_LOG_v1` followed by a JSON object:
docs/components/monitoring/07_event_logger.md:17:Every JSON object includes a `time_micros` field (Unix microsecond timestamp) and an `event` field identifying the event type.
docs/components/monitoring/07_event_logger.md:38:## JSONWriter
docs/components/monitoring/07_event_logger.md:40:`JSONWriter` (see `logging/event_logger.h`) provides a streaming JSON builder with key-value pairs, arrays, and nested objects. It enforces a key-value alternation protocol via internal state (`kExpectKey`, `kExpectValue`, `kInArray`).
docs/components/monitoring/07_event_logger.md:42:`EventLoggerStream` wraps `JSONWriter` and automatically:
docs/components/monitoring/07_event_logger.md:45:3. Flushes the JSON string to the `Logger` on destruction
docs/components/monitoring/07_event_logger.md:47:## Writing to EventLogger
docs/components/monitoring/07_event_logger.md:49:EventLogger writes can go directly to the `Logger` or to a `LogBuffer` for deferred output:
docs/components/monitoring/07_event_logger.md:50:- `EventLogger::Log()` -- writes immediately to the LOG file
docs/components/monitoring/07_event_logger.md:51:- `EventLogger::LogToBuffer(log_buffer)` -- buffers the entry for later flushing (used in critical sections where I/O should be deferred)
docs/components/monitoring/07_event_logger.md:61:Each extracted line is a valid JSON object that can be processed with standard JSON tools.
docs/components/monitoring/07_event_logger.md:63:**Note:** Background errors are not surfaced through EventLogger. They are exposed via `EventListener::OnBackgroundError()` callbacks (see `include/rocksdb/listener.h`).
docs/components/monitoring/07_event_logger.md:65:**Note:** EventLogger output is independent of `DBOptions::info_log_level`. Events are always logged regardless of the configured log level.
docs/components/monitoring/01_statistics_framework.md:25:| `Reset()` | Reset all tickers and histograms to zero |
docs/components/monitoring/01_statistics_framework.md:63:- `DB_MUTEX_WAIT_MICROS` -- time waiting for DB mutex (requires `StatsLevel::kAll`)
docs/components/monitoring/01_statistics_framework.md:84:| `COMPRESSION_TIMES_NANOS` | nanoseconds | Compression time |
docs/components/monitoring/01_statistics_framework.md:85:| `DECOMPRESSION_TIMES_NANOS` | nanoseconds | Decompression time |
docs/components/monitoring/01_statistics_framework.md:90:- `FILE_READ_GET_MICROS`, `FILE_READ_MULTIGET_MICROS`, `FILE_READ_DB_ITERATOR_MICROS` -- file reads broken down by operation type
docs/components/monitoring/01_statistics_framework.md:107:| `kAll` | Yes | Yes | All | Yes |
codex
The main remaining work is extracting the exact wording from the larger DB properties and compaction stats chapters, then tying each correction to the concrete functions and recent code changes. After that I’ll write the review in one pass.
exec
/bin/bash -lc "sed -n '1,260p' docs/components/monitoring/05_db_properties.md" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,260p' docs/components/monitoring/06_compaction_stats.md" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
# Compaction Stats and DB Status

**Files:** `db/internal_stats.h`, `db/internal_stats.cc`, `include/rocksdb/options.h`

## Overview

RocksDB periodically dumps detailed compaction and database statistics to the LOG file. This output provides per-level compaction I/O stats, write stall tracking, and interval-based comparisons. The stats are also accessible programmatically via DB properties.

## Periodic Stats Dumping

Controlled by `stats_dump_period_sec` (see `DBOptions` in `include/rocksdb/options.h`). Default: 600 seconds (10 minutes). Set to 0 to disable.

When enabled, `DBImpl::DumpStats()` runs on a background thread managed by `PeriodicTaskScheduler`. Each dump writes:
1. `rocksdb.dbstats` -- database-wide statistics
2. Per-column-family stats using the internal periodic formatting

## Compaction Stats Output Format

The `rocksdb.cfstats-no-file-histogram` property produces per-level compaction stats. The output contains three sections:

 succeeded in 51ms:
# DB Properties

**Files:** `include/rocksdb/db.h`, `db/internal_stats.h`, `db/internal_stats.cc`

## Overview

DB properties provide runtime introspection into database state without requiring a `Statistics` object. Properties can return strings, integers, or maps, and are queried on demand via `DB::GetProperty()`, `DB::GetIntProperty()`, or `DB::GetMapProperty()`.

## Property API

Three methods retrieve properties, each for a different value type:

| Method | Return Type | Use Case |
|--------|-------------|----------|
| `GetProperty(property, &value)` | `std::string` | Human-readable formatted output |
| `GetIntProperty(property, &value)` | `uint64_t` | Single numeric values |
| `GetMapProperty(property, &value)` | `std::map<std::string, std::string>` | Structured key-value data |

All methods accept a `ColumnFamilyHandle*` parameter (defaults to the default column family). They return `true` on success, `false` if the property is not recognized or unavailable.

exec
/bin/bash -lc "git log --oneline --since='2024-01-01' -- monitoring db/internal_stats.h db/internal_stats.cc include/rocksdb/{statistics.h,perf_context.h,perf_level.h,iostats_context.h,thread_status.h,stats_history.h,db.h,options.h} logging/event_logger.h logging/event_logger.cc | sed -n '1,120p'" in /home/xbw/workspace/ws36/rocksdb succeeded in 478ms:
3555)
476a98ca3 Add a new GetNewestUserDefinedTimestamp API (#13547)
925c63a96 Experimental API `IngestWriteBatchWithIndex()` (#13550)
29c661061 Add compaction explicit prefetch stats (#13520)
84a8dd994 Some MultiScan code cleanup (#13530)
2a0ee4ddd Refactor wal related naming and more (#13490)
f7764cb6b Remove fail_if_options_file_error DB option (#13504)
24e2b05e6 Multi scan API (#13473)
b7a9d414c Fix WriteBatch atomicity and WAL recovery for some failures (#13489)
48eb64678 Mark MaxMemCompactionLevel() deprecated (#13503)
325dcdf2e Deprecate `ReadOptions::ignore_range_deletions` and `experimental::PromoteL0()` (#13500)
82794e0a4 Deprecate RangePtr, favor new RangeOpt and OptSlice (#13481)
934cf2d40 Implement the DB::GetPropertiesOfTablesForLevels API (#13469)
7f3ee34cd Experimental ingestion option atomic_replace_range (#13453)
d0374a0a7 Control SST write lifetime hints based on compaction style (#13472)
12829883d Fix CompactionStats when max_sub_compaction > 1 (#13470)
2e175124d Rename Env::IOActivity::kReadManifest (#13471)
cc487ba36 Fix Compaction Stats for Remote Compaction and Tiered Storage (#13464)
ca7367a00 Replace penultimate naming with proximal (#13460)
68b2d941b Introduce kAborted Status (#13438)
codex
I have the main doc claims extracted. I’m validating the exact implementation hooks now so each review item can point to a concrete function or enum instead of a generic file-level citation.
exec
/bin/bash -lc "rg -n \"enable_thread_tracking|SetThreadOperation\\(|GetThreadList\\(|NotSupported|RegisterThread|UnregisterThread|SetThreadTrackingEnabled|MaybeSetThreadTrackingEnabled|ThreadStatusUpdater\" include/rocksdb/thread_status.h monitoring/thread_status_updater.h monitoring/thread_status_updater.cc monitoring/thread_status_util.h monitoring/thread_status_util.cc util/threadpool_imp.cc include/rocksdb/env.h env/env.cc db/flush_job.cc db/compaction/compaction_job.cc db/db_impl/db_impl_open.cc -S" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "rg -n \"EventLogger::Log\\(|LogToBuffer\\(|info_log_level|EVENT_LOG_v1|JSONWriter|AppendString|Escape|flush_started|flush_finished|compaction_started|compaction_finished|recovery_started|recovery_finished|recovery_failed|table_file_creation|table_file_deletion\" logging/event_logger.h logging/event_logger.cc env/env.cc db/event_helpers.cc db/flush_job.cc db/compaction/compaction_job.cc db/db_impl/db_impl_open.cc -S" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "rg -n \"kPersistentStatsColumnFamilyName|PersistStats\\(|GetStatsHistory\\(|persist_stats_to_disk|stats_persist_period_sec|statistics == nullptr|getTickerMap|previous_tickers_|EncodePersistentStatsKey|OptimizeForPersistentStats|___rocksdb_stats_history___|__stats_history__\" include/rocksdb/stats_history.h include/rocksdb/options.h db/db_impl/db_impl.cc db/db_impl/db_impl_open.cc monitoring/in_memory_stats_history.h monitoring/in_memory_stats_history.cc monitoring/persistent_stats_history.h monitoring/persistent_stats_history.cc monitoring/stats_history_test.cc -S" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc 'rg -n "need_out_of_mutex|compression-ratio-at-level|GetAggregatedIntProperty|BlockCachePropertyAggregator|AggregatedTableProperties|aggregated-table-properties|base-level|estimate-oldest-key-time|background-errors|num-running-compaction-sorted-runs|compaction-abort-count|num-deletes-|is-file-deletions-enabled|min-log-number-to-keep|min-obsolete-sst-number-to-keep|live-sst-files-size-at-temperature" db/internal_stats.h db/internal_stats.cc db/version_set.cc db/db_properties_test.cc include/rocksdb/db.h -S' in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "rg -n \"DB_MUTEX_WAIT_MICROS|StatsLevel::kAll|db_mutex_lock_nanos|db_condition_wait_nanos|PerfStepTimer|InstrumentedMutex::Lock\\(|InstrumentedCondVar::Wait\\(|stats_code_|clock_ == nullptr|SystemClock::Default|timing_enabled|conditional\" monitoring/instrumented_mutex.h monitoring/instrumented_mutex.cc monitoring/perf_step_timer.h db/perf_context_test.cc db/db_statistics_test.cc -S" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc 'rg -n "kEnableCount|default_perf_level|PerfLevel perf_level|SetPerfLevel|GetPerfLevel|thread_local PerfLevel" include/rocksdb/perf_level.h monitoring/perf_level.cc monitoring/perf_level_imp.h -S' in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "rg -n \"disable_iostats|IOSTATS_ADD\\(|IOSTATS_TIMER_GUARD\\(|IOSTATS_CPU_TIMER_GUARD\\(|Reset\\(|unknown_non_last_level|unknown_last_level\" include/rocksdb/iostats_context.h monitoring/iostats_context.cc monitoring/iostats_context_imp.h logging/env_logger.h file/random_access_file_reader.cc -S" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc 'rg -n "WRITE_PRE_COMP_GB|cfstats.periodic|periodic|num_collapsible_entry_reads_sampled|sample_collapsible_entry_file_read_inc|sample_file_read_inc|1-in-1024|1-in-65536|stats_dump_period_sec|DumpCFStats|DumpDBStats|InternalCFStatsType|InternalDBStatsType|comp_stats_|AddCompactionStats|AddCFStats" db/internal_stats.h db/internal_stats.cc monitoring/file_read_sample.h db/periodic_task_scheduler.h db/periodic_task_scheduler.cc db/db_impl/db_impl.cc db/db_properties_test.cc monitoring/stats_history_test.cc -S' in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
include/rocksdb/thread_status.h:8:// GetThreadList() API.
include/rocksdb/thread_status.h:29:// ROCKSDB_NAMESPACE::GetThreadList().
include/rocksdb/thread_status.h:32:  // ROCKSDB_NAMESPACE::GetThreadList() to be supported.
monitoring/thread_status_updater.h:11:// user call GetThreadList().  This consistency guarantee is done
monitoring/thread_status_updater.h:28:// returned by the GetThreadList() is guaranteed to be consistent.
monitoring/thread_status_updater.h:100:class ThreadStatusUpdater {
monitoring/thread_status_updater.h:102:  ThreadStatusUpdater() {}
monitoring/thread_status_updater.h:105:  virtual ~ThreadStatusUpdater() {}
monitoring/thread_status_updater.h:108:  void UnregisterThread();
monitoring/thread_status_updater.h:118:  void RegisterThread(ThreadStatus::ThreadType ttype, uint64_t thread_id);
monitoring/thread_status_updater.h:130:  void SetThreadOperation(const ThreadStatus::OperationType type);
monitoring/thread_status_updater.h:169:  Status GetThreadList(std::vector<ThreadStatus>* thread_list);
monitoring/thread_status_util.h:26:// Env::GetThreadStatusUpdater().  When thread_updater_local_cache_
monitoring/thread_status_util.h:33:  static void RegisterThread(const Env* env,
monitoring/thread_status_util.h:37:  static void UnregisterThread();
monitoring/thread_status_util.h:63:  static void SetThreadOperation(ThreadStatus::OperationType type);
monitoring/thread_status_util.h:88:  // Initialize the thread-local ThreadStatusUpdater when it finds
monitoring/thread_status_util.h:97:  // than UnregisterThread().  It will be set to false when
monitoring/thread_status_util.h:98:  // UnregisterThread() is called.
monitoring/thread_status_util.h:102:  // in UnregisterThread().
 succeeded in 50ms:
logging/event_logger.h:18:class JSONWriter {
logging/event_logger.h:20:  JSONWriter() : state_(kExpectKey), first_element_(true), in_array_(false) {
logging/event_logger.h:105:  JSONWriter& operator<<(const char* val) {
logging/event_logger.h:114:  JSONWriter& operator<<(const std::string& val) {
logging/event_logger.h:119:  JSONWriter& operator<<(const T& val) {
logging/event_logger.h:126:  enum JSONWriterState {
logging/event_logger.h:132:  JSONWriterState state_;
logging/event_logger.h:157:      json_writer_ = new JSONWriter();
logging/event_logger.h:172:  JSONWriter* json_writer_;
logging/event_logger.h:176:// 2015/01/15-14:13:25.788019 1105ef000 EVENT_LOG_v1 {"time_micros":
logging/event_logger.h:177:// 1421360005788015, "event": "table_file_creation", "file_number": 12,
logging/event_logger.h:181:  static const char* Prefix() { return "EVENT_LOG_v1"; }
logging/event_logger.h:185:  EventLoggerStream LogToBuffer(LogBuffer* log_buffer) {
logging/event_logger.h:188:  EventLoggerStream LogToBuffer(LogBuffer* log_buffer,
logging/event_logger.h:192:  void Log(const JSONWriter& jwriter);
logging/event_logger.h:193:  static void Log(Logger* logger, const JSONWriter& jwriter);
logging/event_logger.h:194:  static void LogToBuffer(
logging/event_logger.h:195:      LogBuffer* log_buffer, const JSONWriter& jwriter,
logging/event_logger.cc:37:      EventLogger::Log(logger_, *json_writer_);
logging/event_logger.cc:40:      EventLogger::LogToBuffer(log_buffer_, *json_writer_, max_log_size_);
 succeeded in 50ms:
include/rocksdb/stats_history.h:29://   Status s = db->GetStatsHistory(0 /* start_time */,
include/rocksdb/options.h:1130:  // if not zero, dump rocksdb.stats to RocksDB every stats_persist_period_sec
include/rocksdb/options.h:1132:  unsigned int stats_persist_period_sec = 600;
include/rocksdb/options.h:1135:  // family name: ___rocksdb_stats_history___) every
include/rocksdb/options.h:1136:  // stats_persist_period_sec seconds; otherwise, write to an in-memory
include/rocksdb/options.h:1139:  // which have previously set persist_stats_to_disk to true, the column family
include/rocksdb/options.h:1144:  bool persist_stats_to_disk = false;
db/db_impl/db_impl_open.cc:917:        OptimizeForPersistentStats(&cfo);
db/db_impl/db_impl_open.cc:920:                                   kPersistentStatsColumnFamilyName, &handle);
db/db_impl/db_impl_open.cc:959:          kPersistentStatsColumnFamilyName);
db/db_impl/db_impl_open.cc:973:    OptimizeForPersistentStats(&cfo);
db/db_impl/db_impl_open.cc:976:                               kPersistentStatsColumnFamilyName, &handle);
db/db_impl/db_impl_open.cc:2234:  if (db_options.persist_stats_to_disk) {
db/db_impl/db_impl_open.cc:2235:    column_families.emplace_back(kPersistentStatsColumnFamilyName, cf_options);
db/db_impl/db_impl_open.cc:2240:    if (db_options.persist_stats_to_disk) {
db/db_impl/db_impl_open.cc:2247:    if (db_options.persist_stats_to_disk && handles[1] != nullptr) {
db/db_impl/db_impl_open.cc:2562:  if (s.ok() && impl->immutable_db_options_.persist_stats_to_disk) {
db/db_impl/db_impl_open.cc:2609:  if (s.ok() && impl->immutable_db_options_.persist_stats_to_disk) {
db/db_impl/db_impl.cc:122:const std::string kPersistentStatsColumnFamilyName(
db/db_impl/db_impl.cc:123:    "___rocksdb_stats_history___");
 succeeded in 50ms:
db/internal_stats.h:32:  bool need_out_of_mutex;
db/internal_stats.h:837:  bool HandleAggregatedTableProperties(std::string* value, Slice suffix);
db/internal_stats.h:838:  bool HandleAggregatedTablePropertiesAtLevel(std::string* value, Slice suffix);
db/internal_stats.h:839:  bool HandleAggregatedTablePropertiesMap(
db/internal_stats.h:841:  bool HandleAggregatedTablePropertiesAtLevelMap(
db/internal_stats.cc:251:    "compression-ratio-at-level";
db/internal_stats.cc:270:static const std::string background_errors = "background-errors";
db/internal_stats.cc:280:    "num-deletes-active-mem-table";
db/internal_stats.cc:282:    "num-deletes-imm-mem-tables";
db/internal_stats.cc:287:    "is-file-deletions-enabled";
db/internal_stats.cc:295:static const std::string min_log_number_to_keep_str = "min-log-number-to-keep";
db/internal_stats.cc:297:    "min-obsolete-sst-number-to-keep";
db/internal_stats.cc:298:static const std::string base_level_str = "base-level";
db/internal_stats.cc:303:    "live-sst-files-size-at-temperature";
db/internal_stats.cc:307:    "aggregated-table-properties";
db/internal_stats.cc:312:    "num-running-compaction-sorted-runs";
db/internal_stats.cc:313:static const std::string compaction_abort_count = "compaction-abort-count";
db/internal_stats.cc:318:static const std::string estimate_oldest_key_time = "estimate-oldest-key-time";
db/internal_stats.cc:417:const std::string DB::Properties::kAggregatedTableProperties =
db/internal_stats.cc:419:const std::string DB::Properties::kAggregatedTablePropertiesAtLevel =
 succeeded in 51ms:
monitoring/instrumented_mutex.cc:27:void InstrumentedMutex::Lock() {
monitoring/instrumented_mutex.cc:29:      db_mutex_lock_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
monitoring/instrumented_mutex.cc:30:      stats_for_report(clock_, stats_), stats_code_);
monitoring/instrumented_mutex.cc:39:  if (stats_code_ == DB_MUTEX_WAIT_MICROS) {
monitoring/instrumented_mutex.cc:51:      SystemClock::Default()->SleepForMicroseconds(sleep_us);
monitoring/instrumented_mutex.cc:58:void InstrumentedCondVar::Wait() {
monitoring/instrumented_mutex.cc:60:      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
monitoring/instrumented_mutex.cc:61:      stats_for_report(clock_, stats_), stats_code_);
monitoring/instrumented_mutex.cc:74:      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
monitoring/instrumented_mutex.cc:75:      stats_for_report(clock_, stats_), stats_code_);
monitoring/instrumented_mutex.h:23:      : mutex_(adaptive), stats_(nullptr), clock_(nullptr), stats_code_(0) {}
monitoring/instrumented_mutex.h:26:      : mutex_(adaptive), stats_(nullptr), clock_(clock), stats_code_(0) {}
monitoring/instrumented_mutex.h:33:        stats_code_(stats_code) {}
monitoring/instrumented_mutex.h:41:        stats_code_(stats_code),
monitoring/instrumented_mutex.h:57:  int stats_code_;
monitoring/instrumented_mutex.h:107:        stats_code_(instrumented_mutex->stats_code_) {}
monitoring/instrumented_mutex.h:123:  int stats_code_;
monitoring/perf_step_timer.h:13:class PerfStepTimer {
monitoring/perf_step_timer.h:15:  explicit PerfStepTimer(
monitoring/perf_step_timer.h:23:                   ? (clock ? clock : SystemClock::Default().get())
 succeeded in 51ms:
include/rocksdb/perf_level.h:21:// metrics that kEnableCount enables, and its documentation only specifies which
include/rocksdb/perf_level.h:34:  kEnableCount = 2,
include/rocksdb/perf_level.h:54:void SetPerfLevel(PerfLevel level);
include/rocksdb/perf_level.h:57:PerfLevel GetPerfLevel();
monitoring/perf_level.cc:13:thread_local PerfLevel perf_level = kEnableCount;
monitoring/perf_level.cc:15:void SetPerfLevel(PerfLevel level) {
monitoring/perf_level.cc:21:PerfLevel GetPerfLevel() { return perf_level; }
monitoring/perf_level_imp.h:12:extern thread_local PerfLevel perf_level;
 succeeded in 50ms:
include/rocksdb/iostats_context.h:42:  uint64_t unknown_non_last_level_bytes_read;
include/rocksdb/iostats_context.h:44:  uint64_t unknown_last_level_bytes_read;
include/rocksdb/iostats_context.h:56:  uint64_t unknown_non_last_level_read_count;
include/rocksdb/iostats_context.h:58:  uint64_t unknown_last_level_read_count;
include/rocksdb/iostats_context.h:61:  void Reset() {
include/rocksdb/iostats_context.h:67:    unknown_non_last_level_bytes_read = 0;
include/rocksdb/iostats_context.h:68:    unknown_last_level_bytes_read = 0;
include/rocksdb/iostats_context.h:74:    unknown_non_last_level_read_count = 0;
include/rocksdb/iostats_context.h:75:    unknown_last_level_read_count = 0;
include/rocksdb/iostats_context.h:81:  void Reset();
include/rocksdb/iostats_context.h:121:  bool disable_iostats = false;
monitoring/iostats_context_imp.h:16:#define IOSTATS_ADD(metric, value)        \
monitoring/iostats_context_imp.h:17:  if (!iostats_context.disable_iostats) { \
monitoring/iostats_context_imp.h:25:#define IOSTATS_RESET_ALL() (iostats_context.Reset())
monitoring/iostats_context_imp.h:35:#define IOSTATS_TIMER_GUARD(metric)                                     \
monitoring/iostats_context_imp.h:40:#define IOSTATS_CPU_TIMER_GUARD(metric, clock)         \
monitoring/iostats_context_imp.h:46:#define IOSTATS_SET_DISABLE(disable) (iostats_context.disable_iostats = disable)
monitoring/iostats_context_imp.h:50:#define IOSTATS_ADD(metric, value)
monitoring/iostats_context_imp.h:59:#define IOSTATS_TIMER_GUARD(metric)
monitoring/iostats_context_imp.h:60:#define IOSTATS_CPU_TIMER_GUARD(metric, clock) static_cast<void>(clock)
 succeeded in 51ms:
db/internal_stats.h:74:  WRITE_PRE_COMP_GB,
db/internal_stats.h:107:  enum InternalCFStatsType {
db/internal_stats.h:130:  enum InternalDBStatsType {
db/internal_stats.h:153:  static const std::map<InternalDBStatsType, DBStatInfo> db_stats_type_to_info;
db/internal_stats.h:157:  // Per level compaction stats.  comp_stats_[level] stores the stats for
db/internal_stats.h:554:    for (auto& comp_stat : comp_stats_) {
db/internal_stats.h:557:    per_key_placement_comp_stats_.Clear();
db/internal_stats.h:569:  void AddCompactionStats(int level, Env::Priority thread_pri,
db/internal_stats.h:571:    comp_stats_[level].Add(stats);
db/internal_stats.h:572:    comp_stats_by_pri_[thread_pri].Add(stats);
db/internal_stats.h:575:  void AddCompactionStats(int level, Env::Priority thread_pri,
db/internal_stats.h:576:                          const CompactionStatsFull& comp_stats_full) {
db/internal_stats.h:577:    AddCompactionStats(level, thread_pri, comp_stats_full.output_level_stats);
db/internal_stats.h:578:    if (comp_stats_full.has_proximal_level_output) {
db/internal_stats.h:579:      per_key_placement_comp_stats_.Add(comp_stats_full.proximal_level_stats);
db/internal_stats.h:584:    comp_stats_[level].bytes_moved += amount;
db/internal_stats.h:587:  void AddCFStats(InternalCFStatsType type, uint64_t value) {
db/internal_stats.h:607:  void AddDBStats(InternalDBStatsType type, uint64_t value,
db/internal_stats.h:618:  uint64_t GetDBStats(InternalDBStatsType type) {
db/internal_stats.h:655:    return comp_stats_;
exec
/bin/bash -lc "sed -n '1000,1060p' db/db_impl/db_impl.cc
sed -n '1128,1160p' db/db_impl/db_impl.cc
sed -n '520,550p' monitoring/stats_history_test.cc
sed -n '392,460p' monitoring/stats_history_test.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,240p' monitoring/thread_status_util.cc
sed -n '1,260p' monitoring/thread_status_updater.cc
sed -n '330,380p' util/threadpool_imp.cc
sed -n '130,170p' db/flush_job.cc
sed -n '190,215p' db/compaction/compaction_job.cc
sed -n '2250,2270p' db/db_impl/db_impl_open.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,120p' monitoring/iostats_context_imp.h
sed -n '1,120p' monitoring/iostats_context.cc
sed -n '1,120p' include/rocksdb/iostats_context.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,220p' logging/event_logger.h
sed -n '1,120p' logging/event_logger.cc
sed -n '930,980p' db/flush_job.cc
sed -n '1238,1298p' db/compaction/compaction_job.cc
sed -n '2760,2810p' db/compaction/compaction_job.cc
sed -n '1158,1180p' db/db_impl/db_impl_open.cc
sed -n '1918,1945p' db/db_impl/db_impl_open.cc
sed -n '70,120p' db/event_helpers.cc
sed -n '200,240p' db/event_helpers.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,140p' monitoring/instrumented_mutex.h
sed -n '1,120p' monitoring/instrumented_mutex.cc
sed -n '1,120p' monitoring/perf_step_timer.h
sed -n '590,650p' db/perf_context_test.cc
sed -n '135,165p' db/db_statistics_test.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '980,1045p' db/internal_stats.cc
sed -n '1050,1095p' db/internal_stats.cc
sed -n '2188,2245p' db/internal_stats.cc
sed -n '1080,1325p' include/rocksdb/db.h
sed -n '70,90p' db/internal_stats.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1088,1135p' db/internal_stats.cc
sed -n '1800,1875p' db/internal_stats.cc
sed -n '1960,2145p' db/internal_stats.cc
sed -n '1,80p' monitoring/file_read_sample.h
sed -n '20,45p' db/periodic_task_scheduler.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
  size_total = size_per_slice * stats_history_.size();
return size_total;
}

void DBImpl::PersistStats() {
TEST_SYNC_POINT("DBImpl::PersistStats:Entry");
if (shutdown_initiated_) {
return;
}
TEST_SYNC_POINT("DBImpl::PersistStats:StartRunning");
uint64_t now_seconds =
immutable_db_options_.clock->NowMicros() / kMicrosInSecond;

Statistics* statistics = immutable_db_options_.stats;
if (!statistics) {
return;
}
size_t stats_history_size_limit = 0;
{
InstrumentedMutexLock l(&mutex_);
 succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/thread_status_util.h"

#include "monitoring/thread_status_updater.h"
#include "rocksdb/env.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

#ifndef NROCKSDB_THREAD_STATUS
thread_local ThreadStatusUpdater*
ThreadStatusUtil::thread_updater_local_cache_ = nullptr;
thread_local bool ThreadStatusUtil::thread_updater_initialized_ = false;

void ThreadStatusUtil::RegisterThread(const Env* env,
ThreadStatus::ThreadType thread_type) {
 succeeded in 51ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/iostats_context.h"

#if !defined(NIOSTATS_CONTEXT)
namespace ROCKSDB_NAMESPACE {
extern thread_local IOStatsContext iostats_context;
}  // namespace ROCKSDB_NAMESPACE

// increment a specific counter by the specified value
#define IOSTATS_ADD(metric, value)        \
if (!iostats_context.disable_iostats) { \
iostats_context.metric += value;      \
}

 succeeded in 50ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/thread_status.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
class InstrumentedCondVar;

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutex {
 succeeded in 50ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <chrono>
#include <memory>
#include <sstream>
#include <string>

#include "logging/log_buffer.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter {
public:
JSONWriter() : state_(kExpectKey), first_element_(true), in_array_(false) {
 succeeded in 50ms:

const DBPropertyInfo* GetPropertyInfo(const Slice& property) {
std::string ppt_name = GetPropertyNameAndArg(property).first.ToString();
auto ppt_info_iter = InternalStats::ppt_name_to_info.find(ppt_name);
if (ppt_info_iter == InternalStats::ppt_name_to_info.end()) {
return nullptr;
}
return &ppt_info_iter->second;
}

bool InternalStats::GetStringProperty(const DBPropertyInfo& property_info,
const Slice& property,
std::string* value) {
assert(value != nullptr);
assert(property_info.handle_string != nullptr);
Slice arg = GetPropertyNameAndArg(property).second;
return (this->*(property_info.handle_string))(value, arg);
}

bool InternalStats::GetMapProperty(const DBPropertyInfo& property_info,
 succeeded in 51ms:
  return true;
}

bool InternalStats::HandleCFStatsPeriodic(std::string* value,
Slice /*suffix*/) {
bool has_change = has_cf_change_since_dump_;
if (!has_change) {
// If file histogram changes, there is activity in this period too.
uint64_t new_histogram_num = 0;
for (int level = 0; level < number_levels_; level++) {
new_histogram_num += file_read_latency_[level].num();
}
new_histogram_num += blob_file_read_latency_.num();
if (new_histogram_num != last_histogram_num) {
has_change = true;
last_histogram_num = new_histogram_num;
}
}
if (has_change) {
no_cf_change_period_since_dump_ = 0;
codex
I’ve got enough code context for the main factual issues. I’m doing one last targeted pass on property semantics and enum definitions so the review can separate outright errors from broader coverage gaps cleanly.
exec
/bin/bash -lc "sed -n '1,220p' include/rocksdb/thread_status.h
sed -n '1125,1150p' include/rocksdb/options.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1,160p' monitoring/perf_context_imp.h
sed -n '1,120p' monitoring/perf_level.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '1040,1085p' db/internal_stats.cc
sed -n '240,325p' db/internal_stats.cc
sed -n '142,240p' db/db_properties_test.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the structures for exposing run-time status of any
// rocksdb-related thread.  Such run-time status can be obtained via
// GetThreadList() API.
//
// Note that all thread-status features are still under-development, and
// thus APIs and class definitions might subject to change at this point.
// Will remove this comment once the APIs have been finalized.

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
 succeeded in 50ms:
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/perf_context.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
#if defined(NPERF_CONTEXT)
extern PerfContext perf_context;
#else
#if defined(OS_SOLARIS)
extern thread_local PerfContext perf_context_;
#define perf_context (*get_perf_context())
#else
extern thread_local PerfContext perf_context;
#endif
 succeeded in 50ms:
bool InternalStats::HandleCompressionRatioAtLevelPrefix(std::string* value,
Slice suffix) {
uint64_t level;
const auto* vstorage = cfd_->current()->storage_info();
bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
if (!ok || level >= static_cast<uint64_t>(number_levels_)) {
return false;
}
*value = std::to_string(
vstorage->GetEstimatedCompressionRatioAtLevel(static_cast<int>(level)));
return true;
}

bool InternalStats::HandleLevelStats(std::string* value, Slice /*suffix*/) {
char buf[1000];
const auto* vstorage = cfd_->current()->storage_info();
snprintf(buf, sizeof(buf),
"Level Files Size(MB)\n"
"--------------------\n");
value->append(buf);
exec
/bin/bash -lc "sed -n '120,210p' include/rocksdb/iostats_context.h" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc 'rg -n "GetEstimatedCompressionRatioAtLevel|raw_key_size|raw_value_size|return -1|compression ratio" db/version_set.cc db/version_set.h -S' in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '430,520p' db/internal_stats.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  // turning this off.
bool disable_iostats = false;
};

// If RocksDB is compiled with -DNIOSTATS_CONTEXT, then a pointer to a global,
// non-thread-local IOStatsContext object will be returned. Attempts to update
// this object will be ignored, and reading from it will also be no-op.
// Otherwise, a pointer to a thread-local IOStatsContext object will be
// returned.
//
// This function never returns nullptr.
IOStatsContext* get_iostats_context();

}  // namespace ROCKSDB_NAMESPACE
 succeeded in 51ms:
db/version_set.h:591:    assert(accumulated_raw_key_size_ + accumulated_raw_value_size_ > 0);
db/version_set.h:593:    return accumulated_raw_value_size_ / accumulated_num_non_deletions_ *
db/version_set.h:595:           (accumulated_raw_key_size_ + accumulated_raw_value_size_);
db/version_set.h:600:  double GetEstimatedCompressionRatioAtLevel(int level) const;
db/version_set.h:782:  uint64_t accumulated_raw_key_size_;
db/version_set.h:784:  uint64_t accumulated_raw_value_size_;
db/version_set.cc:2319:double VersionStorageInfo::GetEstimatedCompressionRatioAtLevel(
db/version_set.cc:2325:    auto raw_size = file_meta->raw_key_size + file_meta->raw_value_size;
db/version_set.cc:2335:    return -1.0;
db/version_set.cc:2504:      accumulated_raw_key_size_(0),
db/version_set.cc:2505:      accumulated_raw_value_size_(0),
db/version_set.cc:2520:    accumulated_raw_key_size_ = ref_vstorage->accumulated_raw_key_size_;
db/version_set.cc:2521:    accumulated_raw_value_size_ = ref_vstorage->accumulated_raw_value_size_;
db/version_set.cc:3469:  file_meta->raw_value_size = tp->raw_value_size;
db/version_set.cc:3470:  file_meta->raw_key_size = tp->raw_key_size;
db/version_set.cc:3485:  accumulated_raw_key_size_ += file_meta->raw_key_size;
db/version_set.cc:3486:  accumulated_raw_value_size_ += file_meta->raw_value_size;
db/version_set.cc:3547:       storage_info_.accumulated_raw_value_size_ == 0 && level >= 0; --level) {
db/version_set.cc:3549:         storage_info_.accumulated_raw_value_size_ == 0 && i >= 0; --i) {
db/version_set.cc:3795:      // setting, or very high compression ratios, or lots of
 succeeded in 50ms:
    rocksdb_prefix + block_cache_usage;
const std::string DB::Properties::kBlockCachePinnedUsage =
rocksdb_prefix + block_cache_pinned_usage;
const std::string DB::Properties::kOptionsStatistics =
rocksdb_prefix + options_statistics;
const std::string DB::Properties::kLiveSstFilesSizeAtTemperature =
rocksdb_prefix + live_sst_files_size_at_temperature;
const std::string DB::Properties::kNumBlobFiles =
rocksdb_prefix + num_blob_files;
const std::string DB::Properties::kBlobStats = rocksdb_prefix + blob_stats;
const std::string DB::Properties::kTotalBlobFileSize =
rocksdb_prefix + total_blob_file_size;
const std::string DB::Properties::kLiveBlobFileSize =
rocksdb_prefix + live_blob_file_size;
const std::string DB::Properties::kLiveBlobFileGarbageSize =
rocksdb_prefix + live_blob_file_garbage_size;
const std::string DB::Properties::kBlobCacheCapacity =
rocksdb_prefix + blob_cache_capacity;
const std::string DB::Properties::kBlobCacheUsage =
rocksdb_prefix + blob_cache_usage;
exec
/bin/bash -lc "sed -n '2315,2345p' db/version_set.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
    return est;
}
}

double VersionStorageInfo::GetEstimatedCompressionRatioAtLevel(
int level) const {
assert(level < num_levels_);
uint64_t sum_file_size_bytes = 0;
uint64_t sum_data_size_bytes = 0;
for (auto* file_meta : files_[level]) {
auto raw_size = file_meta->raw_key_size + file_meta->raw_value_size;
// Check if the table property is properly initialized. It might not be
// because in `UpdateAccumulatedStats` we limit the maximum number of
// properties to read once.
if (raw_size > 0) {
sum_file_size_bytes += file_meta->fd.GetFileSize();
sum_data_size_bytes += raw_size;
}
}
if (sum_file_size_bytes == 0) {
exec
/bin/bash -lc "sed -n '1,220p' monitoring/persistent_stats_history.cc
sed -n '1,220p' monitoring/persistent_stats_history.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "monitoring/persistent_stats_history.h"

#include <cstring>
#include <string>
#include <utility>

#include "db/db_impl/db_impl.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
// 10 digit seconds timestamp => [Sep 9, 2001 ~ Nov 20, 2286]
const int kNowSecondsStringLength = 10;
exec
/bin/bash -lc "sed -n '1,220p' monitoring/in_memory_stats_history.cc
sed -n '1,220p' monitoring/in_memory_stats_history.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "monitoring/in_memory_stats_history.h"

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {

InMemoryStatsHistoryIterator::~InMemoryStatsHistoryIterator() = default;

bool InMemoryStatsHistoryIterator::Valid() const { return valid_; }

Status InMemoryStatsHistoryIterator::status() const { return status_; }

exec
/bin/bash -lc "sed -n '90,140p' logging/env_logger.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  }

Status CloseImpl() override { return CloseHelper(); }

Status CloseHelper() {
FileOpGuard guard(*this);
const auto close_status = file_.Close(IOOptions());

if (close_status.ok()) {
return close_status;
}
return Status::IOError("Close of log file failed with error:" +
(close_status.getState()
? std::string(close_status.getState())
: std::string()));
}

using Logger::Logv;
void Logv(const char* format, va_list ap) override {
IOSTATS_TIMER_GUARD(logger_nanos);
exec
/bin/bash -lc 'rg -n "IOSTATS_SET_DISABLE|disable_iostats" -S .' in /home/xbw/workspace/ws36/rocksdb succeeded in 630ms:
./fix_monitoring_prompt.txt:50:- `docs/components/monitoring.md:405-414` overstates what `disable_iostats` does and why it exists. The header comment says BackupEngine is the reason some IOStats counters remain independent of PerfLevel; `disable_iostats` is a backdoor for avoiding pollution of IOStats counters. It only gates the `IOSTATS_*` macros and does not suppress Statistics tickers such as `HOT_FILE_READ_BYTES` or `LAST_LEVEL_READ_BYTES`. Source: `include/rocksdb/iostats_context.h:116-121`, `monitoring/iostats_context_imp.h:15-46`, `file/random_access_file_reader.cc:56-116`.
./monitoring/iostats_context_imp.h:17:  if (!iostats_context.disable_iostats) { \
./monitoring/iostats_context_imp.h:46:#define IOSTATS_SET_DISABLE(disable) (iostats_context.disable_iostats = disable)
./monitoring/iostats_context_imp.h:57:#define IOSTATS_SET_DISABLE(disable)
./include/rocksdb/iostats_context.h:121:  bool disable_iostats = false;
./docs/reviews/monitoring_review_cc.md:120:### IOSTATS_TIMER_GUARD does not check disable_iostats
./docs/reviews/monitoring_review_cc.md:121:- **Current:** Chapter 04 says IOSTATS_ADD "checks disable_iostats" and lists IOSTATS_TIMER_GUARD as another recording macro, potentially implying both check it
./docs/reviews/monitoring_review_cc.md:122:- **Missing:** IOSTATS_TIMER_GUARD does NOT check `disable_iostats`. It creates a PerfStepTimer that checks PerfLevel only. Only IOSTATS_ADD checks `disable_iostats`. This distinction matters for understanding when I/O timing is collected vs. counters.
./logging/env_logger.h:60:      IOSTATS_SET_DISABLE(true);
./logging/env_logger.h:65:      IOSTATS_SET_DISABLE(false);
./docs/components/monitoring/04_iostats_context.md:45:## disable_iostats Backdoor
./docs/components/monitoring/04_iostats_context.md:47:The `disable_iostats` field on `IOStatsContext` (see `include/rocksdb/iostats_context.h`) gates the `IOSTATS_*` macros used internally. When set to `true`, counter increments and timer guards in `monitoring/iostats_context_imp.h` become no-ops.
./docs/components/monitoring/04_iostats_context.md:49:This is useful for preventing background operations like logging from polluting I/O metrics. BackupEngine relies on counter metrics being active (independent of PerfLevel), so `disable_iostats` provides a way to suppress specific operations without disabling PerfLevel globally.
./docs/components/monitoring/04_iostats_context.md:51:**Note:** `disable_iostats` does not affect Statistics tickers such as `HOT_FILE_READ_BYTES` or `LAST_LEVEL_READ_BYTES`.
./docs/components/monitoring/04_iostats_context.md:56:- `IOSTATS_ADD(metric, value)` -- increment a counter (checks `disable_iostats`)
./docs/components/monitoring/04_iostats_context.md:59:- `IOSTATS_SET_DISABLE(disable)` -- set/clear the `disable_iostats` flag
./docs/components/monitoring/04_iostats_context.md:68:IOStatsContext timing metrics share the PerfLevel hierarchy with PerfContext. Setting `PerfLevel::kEnableTimeExceptForMutex` enables both PerfContext time metrics and IOStatsContext wall-clock timers. Counter metrics (`bytes_read`, `bytes_written`) are always active unless `disable_iostats` is set.
./docs/components/monitoring/index.md:16:| 4. IOStatsContext | [04_iostats_context.md](04_iostats_context.md) | Thread-local I/O statistics, temperature-based file I/O tracking, `disable_iostats` backdoor, and relationship with `PerfLevel`. |
exec
/bin/bash -lc "sed -n '48,72p' logging/env_logger.h" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  }

private:
// A guard to prepare file operations, such as mutex and skip
// I/O context.
class FileOpGuard {
public:
explicit FileOpGuard(EnvLogger& logger)
: logger_(logger), prev_perf_level_(GetPerfLevel()) {
// Preserve iostats not to pollute writes from user writes. We might
// need a better solution than this.
SetPerfLevel(PerfLevel::kDisable);
IOSTATS_SET_DISABLE(true);
logger.mutex_.Lock();
}
~FileOpGuard() {
logger_.mutex_.Unlock();
IOSTATS_SET_DISABLE(false);
SetPerfLevel(prev_perf_level_);
}
exec
/bin/bash -lc "sed -n '238,260p' db/db_impl/db_impl.cc
sed -n '876,918p' db/db_impl/db_impl.cc" in /home/xbw/workspace/ws36/rocksdbexec
/bin/bash -lc "sed -n '900,990p' db/db_impl/db_impl_open.cc
sed -n '2228,2255p' db/db_impl/db_impl_open.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
  co.num_shard_bits = immutable_db_options_.table_cache_numshardbits;
co.metadata_charge_policy = kDontChargeCacheMetadata;
// TODO: Consider a non-fixed seed once test fallout (prefetch_test) is
// dealt with
co.hash_seed = 0;
table_cache_ = NewLRUCache(co);
SetDbSessionId();
assert(!db_session_id_.empty());

periodic_task_functions_.emplace(PeriodicTaskType::kDumpStats,
[this]() { this->DumpStats(); });
periodic_task_functions_.emplace(PeriodicTaskType::kPersistStats,
[this]() { this->PersistStats(); });
periodic_task_functions_.emplace(PeriodicTaskType::kFlushInfoLog,
[this]() { this->FlushInfoLog(); });
periodic_task_functions_.emplace(
PeriodicTaskType::kRecordSeqnoTime,
[this]() { this->RecordSeqnoToTimeMapping(); });
periodic_task_functions_.emplace(
PeriodicTaskType::kTriggerCompaction,
 succeeded in 51ms:
            "key: %s",
s_format.ToString().c_str(), s_compatible.ToString().c_str());
} else {
ROCKS_LOG_WARN(
immutable_db_options_.info_log,
"Recreating persistent stats column family due to corrupted or "
"incompatible format version. Recovered format: %" PRIu64
"; recovered format compatible since: %" PRIu64 "\n",
format_version_recovered, compatible_version_recovered);
}
s = DropColumnFamily(persist_stats_cf_handle_);
if (s.ok()) {
s = DestroyColumnFamilyHandle(persist_stats_cf_handle_);
}
ColumnFamilyHandle* handle = nullptr;
if (s.ok()) {
ColumnFamilyOptions cfo;
OptimizeForPersistentStats(&cfo);
s = CreateColumnFamilyImpl(ReadOptions(Env::IOActivity::kDBOpen),
WriteOptions(Env::IOActivity::kDBOpen), cfo,
exec
/bin/bash -lc "sed -n '1170,1215p' db/internal_stats.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
bool InternalStats::HandleSsTables(std::string* value, Slice /*suffix*/) {
auto* current = cfd_->current();
*value = current->DebugString(true, true);
return true;
}

bool InternalStats::HandleAggregatedTableProperties(std::string* value,
Slice /*suffix*/) {
std::shared_ptr<const TableProperties> tp;
// TODO: plumb Env::IOActivity, Env::IOPriority
const ReadOptions read_options;
auto s = cfd_->current()->GetAggregatedTableProperties(read_options, &tp);
if (!s.ok()) {
return false;
}
*value = tp->ToString();
return true;
}

static std::map<std::string, std::string> MapUint64ValuesToString(
exec
/bin/bash -lc "rg -n \"GetAggregatedIntProperty\\(|CreateIntPropertyAggregator|BlockCachePropertyAggregator\" db/db_impl/db_impl.cc db/db_impl/db_impl_readonly.cc db/db_impl/db_impl_secondary.cc include/rocksdb/db.h -S" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
db/db_impl/db_impl.cc:4802:bool DBImpl::GetAggregatedIntProperty(const Slice& property,
db/db_impl/db_impl.cc:4808:  auto aggregator = CreateIntPropertyAggregator(property);
include/rocksdb/db.h:1454:  virtual bool GetAggregatedIntProperty(const Slice& property,
exec
/bin/bash -lc "sed -n '4798,4835p' db/db_impl/db_impl.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 50ms:
  }
return Status::OK();
}

bool DBImpl::GetAggregatedIntProperty(const Slice& property,
uint64_t* aggregated_value) {
const DBPropertyInfo* property_info = GetPropertyInfo(property);
if (property_info == nullptr || property_info->handle_int == nullptr) {
return false;
}
auto aggregator = CreateIntPropertyAggregator(property);
if (aggregator == nullptr) {
return false;
}

bool ret = true;
{
// Needs mutex to protect the list of column families.
InstrumentedMutexLock l(&mutex_);
uint64_t value;
exec
/bin/bash -lc "sed -n '1,240p' docs/reviews/wal_review_codex.md" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
# Review: wal — Codex

## Summary
Overall quality rating: significant issues

The WAL docs cover a broad surface area and the chapter split is sensible. The index is the right size, the lifecycle/purge material points readers toward the correct subsystems, and the docs at least attempt to cover newer features like WAL compression, recyclable records, predecessor-WAL verification, and user-defined timestamp metadata.

The main problem is accuracy. Several chapters describe the wrong code path entirely, especially replication/tailing, 2PC, and MANIFEST/WAL verification. The configuration chapter also claims to be a complete reference but omits multiple WAL-affecting options added or exercised in 2024-2026 code and tests. The docs need a pass from someone reading the current implementation, not just the APIs.

## Correctness Issues

### [WRONG] 2PC chapter reverses the WritePrepared WAL/memtable flow
- **File:** `docs/components/wal/11_2pc_transactions.md` — Overview, Prepare Phase, Commit Phase
- **Claim:** "the prepare phase writes to the WAL only (deferring MemTable insertion to the commit phase)." / "The prepare phase ... is written via `WriteImpl()` with `disable_memtable=true`" / "The commit triggers MemTable insertion of the prepared data."
- **Reality:** WritePrepared prepare writes to both WAL and memtable. `WritePreparedTxn::PrepareInternal()` passes `!DISABLE_MEMTABLE`, i.e. memtable writes are enabled. The common commit path is often WAL-only: `WritePreparedTxn::CommitInternal()` computes `disable_memtable = !includes_data`, so a pure commit marker does not insert the prepared data into the memtable.
- **Source:** `utilities/transactions/write_prepared_txn.cc` (`WritePreparedTxn::PrepareInternal`, `WritePreparedTxn::CommitInternal`), `utilities/transactions/write_prepared_txn.h` (phase walkthrough comments)
- **Fix:** Rewrite the chapter around the actual WritePrepared flow and explicitly separate WritePrepared from WriteUnprepared. Do not describe a universal "prepare=WAL only, commit=memtable insert" model.

### [WRONG] Replication docs claim `TransactionLogIteratorImpl` uses `FragmentBufferedReader`
- **File:** `docs/components/wal/03_reader.md` — FragmentBufferedReader
exec
/bin/bash -lc "rg -n \"RegisterThread\\(.*USER|ThreadStatusUtil::RegisterThread\\(|HIGH_PRIORITY|LOW_PRIORITY|USER\\)\" -S ." in /home/xbw/workspace/ws36/rocksdb succeeded in 630ms:
./threading_model_prompt.txt:4:1. Thread pools: HIGH_PRIORITY (flush), LOW_PRIORITY (compaction), BOTTOM_PRIORITY (bottommost compaction)
./docs/reviews/monitoring_review_cc.md:40:- **Claim:** Lists thread types as: HIGH_PRIORITY, LOW_PRIORITY, BOTTOM_PRIORITY, USER
./docs/reviews/monitoring_review_cc.md:41:- **Reality:** The actual enum order is: HIGH_PRIORITY(0), LOW_PRIORITY(1), USER(2), BOTTOM_PRIORITY(3)
./docs/reviews/monitoring_review_cc.md:43:- **Fix:** Reorder to match the actual enum: HIGH_PRIORITY, LOW_PRIORITY, USER, BOTTOM_PRIORITY
./include/rocksdb/thread_status.h:37:    HIGH_PRIORITY = 0,  // RocksDB BG thread in high-pri thread pool
./include/rocksdb/thread_status.h:38:    LOW_PRIORITY,       // RocksDB BG thread in low-pri thread pool
./include/rocksdb/thread_status.h:131:  // The type of the thread, it could be HIGH_PRIORITY,
./include/rocksdb/thread_status.h:132:  // LOW_PRIORITY, and USER
./docs/reviews/threading_model_review_cc.md:37:- **Fix:** Say "RocksDB uses four priority-based thread pools. Three are used for internal background work (HIGH for flush, LOW for compaction, BOTTOM for bottommost compaction). A fourth pool (USER) is available for application use."
./monitoring/thread_status_util.cc:19:void ThreadStatusUtil::RegisterThread(const Env* env,
./monitoring/thread_status_util.cc:174:void ThreadStatusUtil::RegisterThread(
./utilities/blob_db/blob_db_test.cc:989:              options.statistics->getTickerCount(COMPACTION_KEY_DROP_USER));
./monitoring/thread_status_updater.h:68:    thread_type.store(ThreadStatus::USER);
./monitoring/thread_status_impl.cc:22:    case ThreadStatus::ThreadType::HIGH_PRIORITY:
./monitoring/thread_status_impl.cc:24:    case ThreadStatus::ThreadType::LOW_PRIORITY:
./db_stress_tool/db_stress_driver.cc:18:  ThreadStatusUtil::RegisterThread(db_stress_env, ThreadStatus::USER);
./db_stress_tool/db_stress_driver.cc:280:  ThreadStatusUtil::RegisterThread(db_stress_env, ThreadStatus::USER);
./java/rocksjni/portal.h:7204:      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::HIGH_PRIORITY:
./java/rocksjni/portal.h:7206:      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::LOW_PRIORITY:
./java/rocksjni/portal.h:7223:        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::HIGH_PRIORITY;
codex
I’ve narrowed the review to the main code-backed issues: thread tracking semantics, stats-history persistence behavior, EventLogger filtering/JSON guarantees, IOStats and mutex timing semantics, and property/compaction coverage gaps. I’m writing the review file now, then I’ll sanity-check it against the prompt sections.
exec
/bin/bash -lc "sed -n '914,936p' env/env.cc" in /home/xbw/workspace/ws36/rocksdb succeeded in 51ms:
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
}
}

void Log(Logger* info_log, const char* format, ...) {
va_list ap;
va_start(ap, format);
Logv(info_log, format, ap);
va_end(ap);
}

void Logger::Logv(const InfoLogLevel log_level, const char* format,
va_list ap) {
static const char* kInfoLogLevelNames[5] = {"DEBUG", "INFO", "WARN", "ERROR",
"FATAL"};
if (log_level < log_level_) {
return;
}

codex
Writing `docs/reviews/monitoring_review_codex.md` now. I’m keeping the findings code-backed and scoped to the prompt’s required sections so the maintainer can act on each one directly.
