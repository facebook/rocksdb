# Monitoring: Statistics, PerfContext, IOStatsContext

RocksDB provides a comprehensive monitoring subsystem for tracking performance metrics, debugging performance issues, and understanding system behavior. This subsystem consists of three main components: **Statistics** (global DB metrics), **PerfContext** (per-thread performance counters), and **IOStatsContext** (per-thread I/O metrics).

## Table of Contents
- [Statistics: Global Database Metrics](#statistics-global-database-metrics)
- [PerfContext: Per-Thread Performance Counters](#perfcontext-per-thread-performance-counters)
- [IOStatsContext: Per-Thread I/O Statistics](#iostatscontext-per-thread-io-statistics)
- [Histograms: Latency Distributions](#histograms-latency-distributions)
- [StatsLevel: Controlling Overhead](#statslevel-controlling-overhead)
- [InstrumentedMutex: Mutex Timing](#instrumentedmutex-mutex-timing)
- [DB Properties: Runtime Database Information](#db-properties-runtime-database-information)
- [EventLogger: JSON Event Logging](#eventlogger-json-event-logging)
- [Implementation Details](#implementation-details)

---

## Statistics: Global Database Metrics

### Overview

`Statistics` is a global, shared object that tracks cumulative metrics across the entire database lifetime. It provides two types of metrics:
- **Tickers**: Simple counters (e.g., `BLOCK_CACHE_HIT`, `BYTES_WRITTEN`)
- **Histograms**: Distributions of values (e.g., `DB_GET` latency, `COMPACTION_TIME`)

**Source files:**
- `include/rocksdb/statistics.h` - Public API
- `monitoring/statistics_impl.h` - Core-local implementation
- `monitoring/statistics.cc` - Ticker/histogram name mappings

### Creating Statistics

```cpp
#include "rocksdb/statistics.h"

Options options;
options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
Status s = DB::Open(options, kDBPath, &db);

// Later: query statistics
uint64_t cache_hits = options.statistics->getTickerCount(BLOCK_CACHE_HIT);
uint64_t cache_misses = options.statistics->getTickerCount(BLOCK_CACHE_MISS);
double hit_rate = static_cast<double>(cache_hits) / (cache_hits + cache_misses);
```

### Common Tickers (Counters)

Defined in `include/rocksdb/statistics.h:Tickers`:

| Category | Ticker | Description |
|----------|--------|-------------|
| **Block Cache** | `BLOCK_CACHE_HIT` | Total block cache hits across all block types |
| | `BLOCK_CACHE_MISS` | Total block cache misses |
| | `BLOCK_CACHE_DATA_HIT` | Data block cache hits |
| | `BLOCK_CACHE_INDEX_HIT` | Index block cache hits |
| | `BLOCK_CACHE_FILTER_HIT` | Filter block cache hits |
| | `BLOCK_CACHE_BYTES_READ` | Total bytes read from cache |
| | `BLOCK_CACHE_BYTES_WRITE` | Total bytes written to cache |
| **Bloom Filter** | `BLOOM_FILTER_USEFUL` | # times bloom filter avoided file reads (negatives) |
| | `BLOOM_FILTER_FULL_POSITIVE` | # times bloom returned positive |
| | `BLOOM_FILTER_FULL_TRUE_POSITIVE` | # times bloom positive and data actually existed |
| **MemTable** | `MEMTABLE_HIT` | Key found in memtable |
| | `MEMTABLE_MISS` | Key not found in memtable |
| **Compaction** | `COMPACT_READ_BYTES` | Bytes read during compaction |
| | `COMPACT_WRITE_BYTES` | Bytes written during compaction |
| | `COMPACTION_KEY_DROP_NEWER_ENTRY` | Keys dropped due to newer version |
| | `COMPACTION_KEY_DROP_OBSOLETE` | Keys dropped as obsolete |
| **Read/Write** | `NUMBER_KEYS_WRITTEN` | Total keys written via Put/Write |
| | `NUMBER_KEYS_READ` | Total keys read |
| | `BYTES_WRITTEN` | Uncompressed bytes written |
| | `BYTES_READ` | Uncompressed bytes read from Get |
| **WAL** | `WAL_FILE_SYNCED` | Number of WAL syncs |
| | `WAL_FILE_BYTES` | Bytes written to WAL |
| **Stalls** | `STALL_MICROS` | Time writers waited for compaction/flush |
| | `DB_MUTEX_WAIT_MICROS` | Time waiting for DB mutex (requires `StatsLevel::kAll`) |

**⚠️ INVARIANT:** Ticker relationships must hold:
```
BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT + BLOCK_CACHE_FILTER_HIT + BLOCK_CACHE_DATA_HIT
BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS + BLOCK_CACHE_FILTER_MISS + BLOCK_CACHE_DATA_MISS
```

See `include/rocksdb/statistics.h:31-587` for the complete list of 200+ tickers.

### Using Tickers

```cpp
// Record a ticker (internal use, called by RocksDB code)
statistics->recordTick(BLOCK_CACHE_HIT, 1);

// Query ticker value
uint64_t hits = statistics->getTickerCount(BLOCK_CACHE_HIT);

// Set ticker to specific value
statistics->setTickerCount(COMPACTION_KEY_DROP_OBSOLETE, 0);

// Get and reset atomically
uint64_t val = statistics->getAndResetTickerCount(STALL_MICROS);

// Get all tickers as a map
std::map<std::string, uint64_t> ticker_map;
if (statistics->getTickerMap(&ticker_map)) {
  for (auto& [name, value] : ticker_map) {
    printf("%s: %llu\n", name.c_str(), value);
  }
}
```

### StatsLevel: Controlling Statistics Overhead

Statistics collection has performance overhead. Use `StatsLevel` to control granularity:

```cpp
enum StatsLevel : uint8_t {
  kDisableAll,                  // Disable all metrics (tickers + histograms)
  kExceptTickers = kDisableAll, // Alias for kDisableAll
  kExceptHistogramOrTimers,     // Disable timer stats and histogram stats
  kExceptTimers,                // Skip timer stats, keep histograms
  kExceptDetailedTimers,        // Skip detailed timers (DEFAULT)
  kExceptTimeForMutex,          // Skip mutex lock time measurement
  kAll,                         // Collect all stats including mutex timing
};

options.statistics->set_stats_level(StatsLevel::kExceptDetailedTimers);
```

**⚠️ INVARIANT:** `StatsLevel` is incremental. Higher levels include all metrics from lower levels.

**Levels explained:**
- `kExceptDetailedTimers` (default): Collects most metrics, skips expensive detailed timers
- `kExceptTimeForMutex`: Adds detailed timers but skips mutex lock timing
- `kAll`: Adds mutex lock timing (`DB_MUTEX_WAIT_MICROS`). **Warning:** Can reduce write scalability if getting time is expensive.

**Performance tip:** `DB_MUTEX_WAIT_MICROS` ticker requires `StatsLevel::kAll` and measuring time inside mutex locks, which can hurt multi-threaded write performance on platforms with expensive clock_gettime().

---

## PerfContext: Per-Thread Performance Counters

### Overview

`PerfContext` provides **thread-local** fine-grained performance counters for diagnosing performance issues in specific workloads. Unlike `Statistics` (global), `PerfContext` tracks metrics per thread and can be reset/queried at any time.

**Source files:**
- `include/rocksdb/perf_context.h` - Public API (70+ counters)
- `monitoring/perf_context.cc` - Implementation
- `include/rocksdb/perf_level.h` - PerfLevel enum
- `monitoring/perf_level.cc` - PerfLevel management

### Using PerfContext

```cpp
#include "rocksdb/perf_context.h"

// Enable perf stats for this thread
SetPerfLevel(PerfLevel::kEnableTime);

// Get thread-local PerfContext (always returns non-null)
PerfContext* perf = get_perf_context();
perf->Reset();

// Do some work
db->Get(read_options, key, &value);

// Check counters
printf("Block cache hits: %llu\n", perf->block_cache_hit_count);
printf("Block reads: %llu\n", perf->block_read_count);
printf("Block read time: %llu ns\n", perf->block_read_time);
printf("Get from memtable time: %llu ns\n", perf->get_from_memtable_time);

// Human-readable output
std::string stats = perf->ToString(true);  // true = exclude zero counters
printf("%s\n", stats.c_str());

// Disable perf stats
SetPerfLevel(PerfLevel::kDisable);
```

### PerfLevel: Controlling PerfContext Overhead

**⚠️ INVARIANT:** PerfLevel is thread-local and incremental. Setting a higher level enables all metrics from lower levels plus additional metrics.

```cpp
enum PerfLevel : unsigned char {
  kUninitialized = 0,          // Unknown (internal only)
  kDisable = 1,                // Disable perf stats
  kEnableCount = 2,            // Enable count metrics (no time measurement)
  kEnableWait = 3,             // + Wait time in RocksDB (not external like mutexes/IO)
  kEnableTimeExceptForMutex = 4, // + End-to-end operation time
  kEnableTimeAndCPUTimeExceptForMutex = 5, // + CPU time
  kEnableTime = 6,             // + Mutex time (most expensive)
  kOutOfBounds = 7
};
```

**Naming conventions (from `include/rocksdb/perf_level.h:23-42`):**
- Count metrics: keywords like "count" or "byte" (e.g., `block_read_count`, `block_read_byte`)
- Wait time: pattern `_[wait|delay]_*_[time|nanos]` (e.g., `write_delay_time`)
- End-to-end time: keywords "time" or "nanos" (e.g., `get_from_memtable_time`)
- CPU time: pattern `_cpu_*_[time|nanos]` (e.g., `block_read_cpu_time`)
- Mutex time: pattern `_[mutex|condition]_*_[time|nanos]` (e.g., `db_mutex_lock_nanos`)

### Key PerfContext Metrics

From `include/rocksdb/perf_context.h:73-301`:

**Block/Cache metrics (kEnableCount):**
```cpp
uint64_t block_cache_hit_count;           // Total block cache hits
uint64_t block_read_count;                // Total blocks read from storage
uint64_t block_read_byte;                 // Bytes read from blocks
uint64_t block_cache_index_hit_count;     // Index block cache hits
uint64_t block_cache_filter_hit_count;    // Filter block cache hits
```

**Read path metrics (kEnableTime):**
```cpp
uint64_t get_from_memtable_time;          // Time querying memtables
uint64_t get_from_memtable_count;         // # memtables queried
uint64_t get_from_output_files_time;      // Time reading from SST files
uint64_t get_post_process_time;           // Time after finding key
uint64_t block_read_time;                 // Time reading blocks
uint64_t block_read_cpu_time;             // CPU time reading blocks (kEnableTimeAndCPUTimeExceptForMutex)
uint64_t block_checksum_time;             // Time verifying checksums
uint64_t block_decompress_time;           // Time decompressing blocks
```

**Iterator metrics:**
```cpp
uint64_t internal_key_skipped_count;      // Internal keys skipped during iteration
uint64_t internal_delete_skipped_count;   // Tombstones skipped
uint64_t internal_recent_skipped_count;   // Keys skipped due to snapshot
uint64_t seek_on_memtable_time;           // Time seeking in memtable
uint64_t seek_on_memtable_count;          // # seeks on memtable
uint64_t next_on_memtable_count;          // # Next() calls on memtable
uint64_t prev_on_memtable_count;          // # Prev() calls on memtable
```

**Write path metrics (kEnableWait):**
```cpp
uint64_t write_wal_time;                  // Time writing to WAL
uint64_t write_memtable_time;             // Time writing to memtable
uint64_t write_delay_time;                // Time delayed/throttled by write controller
uint64_t write_scheduling_flushes_compactions_time; // Time scheduling background work
uint64_t write_pre_and_post_process_time; // Other write overhead
uint64_t write_thread_wait_nanos;         // Time waiting for batch group
```

**Bloom filter stats (kEnableCount):**
```cpp
uint64_t bloom_memtable_hit_count;        // Memtable bloom hits
uint64_t bloom_memtable_miss_count;       // Memtable bloom misses
uint64_t bloom_sst_hit_count;             // SST bloom hits
uint64_t bloom_sst_miss_count;            // SST bloom misses
```

**Mutex metrics (kEnableTime):**
```cpp
uint64_t db_mutex_lock_nanos;             // Time acquiring DB mutex
uint64_t db_condition_wait_nanos;         // Time waiting on condition variable
uint64_t key_lock_wait_time;              // Time waiting on transaction locks (kEnableTimeExceptForMutex)
uint64_t key_lock_wait_count;             // # times blocked by transaction lock
```

**BlobDB metrics:**
```cpp
uint64_t blob_cache_hit_count;            // Blob cache hits
uint64_t blob_read_count;                 // Blobs read from storage
uint64_t blob_read_byte;                  // Bytes read from blobs
uint64_t blob_read_time;                  // Time reading blobs
uint64_t blob_checksum_time;              // Time verifying blob checksums
uint64_t blob_decompress_time;            // Time decompressing blobs
```

### Per-Level PerfContext

For detailed analysis, enable per-level tracking:

```cpp
PerfContext* perf = get_perf_context();
perf->EnablePerLevelPerfContext();  // Allocates per-level storage

// Do work...
db->Get(read_options, key, &value);

// Access per-level stats
if (perf->level_to_perf_context) {
  for (auto& [level, level_perf] : *perf->level_to_perf_context) {
    printf("Level %u: bloom_filter_useful=%llu, block_cache_hit=%llu\n",
           level, level_perf.bloom_filter_useful, level_perf.block_cache_hit_count);
  }
}

perf->DisablePerLevelPerfContext();  // Temporarily disable without freeing
perf->ClearPerLevelPerfContext();    // Free memory and disable
```

**⚠️ INVARIANT:** Per-level PerfContext must be explicitly enabled/disabled. It consumes memory proportional to the number of levels with activity.

From `include/rocksdb/perf_context.h:33-63`, per-level metrics include:
- `bloom_filter_useful` - Bloom filter prevented reads
- `bloom_filter_full_positive` - Bloom false positives
- `bloom_filter_full_true_positive` - Bloom true positives
- `user_key_return_count` - Keys found and returned
- `get_from_table_nanos` - Time reading from SST files
- `block_cache_hit_count` / `block_cache_miss_count`

---

## IOStatsContext: Per-Thread I/O Statistics

### Overview

`IOStatsContext` tracks **thread-local** I/O operations (bytes read/written, latency). Useful for understanding I/O patterns and diagnosing I/O bottlenecks.

**Source files:**
- `include/rocksdb/iostats_context.h` - Public API
- `monitoring/iostats_context.cc` - Implementation

### Using IOStatsContext

```cpp
#include "rocksdb/iostats_context.h"

// Enable time stats (required for I/O timing)
SetPerfLevel(PerfLevel::kEnableTime);

// Get thread-local IOStatsContext (always returns non-null)
IOStatsContext* io = get_iostats_context();
io->Reset();

// Do some work
db->Get(read_options, key, &value);

// Check I/O stats
printf("Bytes read: %llu\n", io->bytes_read);
printf("Bytes written: %llu\n", io->bytes_written);
printf("Read time: %llu ns\n", io->read_nanos);
printf("Write time: %llu ns\n", io->write_nanos);
printf("Fsync time: %llu ns\n", io->fsync_nanos);

// Human-readable output
std::string stats = io->ToString(true);  // true = exclude zero counters
printf("%s\n", stats.c_str());
```

### Key IOStatsContext Metrics

From `include/rocksdb/iostats_context.h:79-122`:

**Basic I/O metrics:**
```cpp
uint64_t thread_pool_id;       // Thread pool ID (e.g., LOW, HIGH priority)
uint64_t bytes_written;        // Total bytes written
uint64_t bytes_read;           // Total bytes read
```

**I/O operation timing:**
```cpp
uint64_t open_nanos;           // Time in open() and fopen()
uint64_t allocate_nanos;       // Time in fallocate()
uint64_t write_nanos;          // Time in write() and pwrite()
uint64_t read_nanos;           // Time in read() and pread()
uint64_t range_sync_nanos;     // Time in sync_file_range()
uint64_t fsync_nanos;          // Time in fsync()
uint64_t prepare_write_nanos;  // Time preparing writes (fallocate, etc.)
uint64_t logger_nanos;         // Time in Logger::Logv()
```

**CPU time (separate from wall clock time):**
```cpp
uint64_t cpu_write_nanos;      // CPU time in write/pwrite
uint64_t cpu_read_nanos;       // CPU time in read/pread
```

### Tiered Storage I/O Stats (EXPERIMENTAL)

From `include/rocksdb/iostats_context.h:28-77`:

```cpp
struct FileIOByTemperature {
  // Bytes read per temperature tier
  uint64_t hot_file_bytes_read;
  uint64_t warm_file_bytes_read;
  uint64_t cool_file_bytes_read;
  uint64_t cold_file_bytes_read;
  uint64_t ice_file_bytes_read;
  uint64_t unknown_non_last_level_bytes_read;
  uint64_t unknown_last_level_bytes_read;

  // Read counts per temperature tier
  uint64_t hot_file_read_count;
  uint64_t warm_file_read_count;
  // ... similar for other tiers
};

IOStatsContext* io = get_iostats_context();
printf("Hot file reads: %llu bytes in %llu operations\n",
       io->file_io_stats_by_temperature.hot_file_bytes_read,
       io->file_io_stats_by_temperature.hot_file_read_count);
```

**⚠️ INVARIANT:** Temperature-based I/O stats are only populated when using tiered storage with `Temperature` annotations on SST files.

### Disabling IOStatsContext Selectively

```cpp
IOStatsContext* io = get_iostats_context();
io->disable_iostats = true;  // Temporarily disable collection
// ... operations that shouldn't pollute stats (e.g., logging)
io->disable_iostats = false;
```

**Use case:** `disable_iostats` prevents background operations like logging or stats dumping from polluting I/O metrics. From `include/rocksdb/iostats_context.h:116-121`, BackupEngine and other tools rely on this to avoid counting their own I/O.

---

## Histograms: Latency Distributions

### Overview

Histograms track distributions of values (typically latency) rather than simple counts. They provide percentiles (p50, p95, p99), average, standard deviation, min/max.

**Source files:**
- `monitoring/histogram.h` - Histogram implementation
- `monitoring/histogram.cc` - Bucket mapping and calculations

### Common Histograms

From `include/rocksdb/statistics.h:604-739`:

| Histogram | Description |
|-----------|-------------|
| `DB_GET` | Get() latency distribution |
| `DB_WRITE` | Write() latency distribution |
| `DB_SEEK` | Iterator Seek() latency |
| `DB_MULTIGET` | MultiGet() latency |
| `COMPACTION_TIME` | Total compaction time |
| `COMPACTION_CPU_TIME` | CPU time spent in compaction |
| `FLUSH_TIME` | Time to flush memtable to L0 |
| `WAL_FILE_SYNC_MICROS` | WAL fsync latency |
| `MANIFEST_FILE_SYNC_MICROS` | MANIFEST fsync latency |
| `TABLE_SYNC_MICROS` | SST file fsync latency |
| `COMPACTION_OUTFILE_SYNC_MICROS` | Compaction output fsync latency |
| `WRITE_STALL` | Write stall duration |
| `SST_READ_MICROS` | Time reading SST/blob files |
| `COMPRESSION_TIMES_NANOS` | Compression time |
| `DECOMPRESSION_TIMES_NANOS` | Decompression time |
| `READ_BLOCK_COMPACTION_MICROS` | Block read time during compaction |
| `READ_BLOCK_GET_MICROS` | Block read time during Get |
| `NUM_FILES_IN_SINGLE_COMPACTION` | # files in a compaction |
| `BYTES_PER_READ` | Value size distribution for reads |
| `BYTES_PER_WRITE` | Value size distribution for writes |
| `BYTES_PER_MULTIGET` | Value size distribution for MultiGet |

**File I/O histograms by activity (require `StatsLevel > kExceptDetailedTimers`):**
- `FILE_READ_GET_MICROS` - File reads during Get
- `FILE_READ_MULTIGET_MICROS` - File reads during MultiGet
- `FILE_READ_DB_ITERATOR_MICROS` - File reads during iteration
- `FILE_READ_COMPACTION_MICROS` - File reads during compaction
- `FILE_READ_FLUSH_MICROS` - File reads during flush

### Using Histograms

```cpp
// Query histogram data
HistogramData hist_data;
statistics->histogramData(DB_GET, &hist_data);

printf("DB::Get latency:\n");
printf("  Count: %llu\n", hist_data.count);
printf("  Sum: %llu\n", hist_data.sum);
printf("  Min: %.2f us\n", hist_data.min);
printf("  Median: %.2f us\n", hist_data.median);
printf("  P95: %.2f us\n", hist_data.percentile95);
printf("  P99: %.2f us\n", hist_data.percentile99);
printf("  Average: %.2f us\n", hist_data.average);
printf("  StdDev: %.2f us\n", hist_data.standard_deviation);
printf("  Max: %.2f us\n", hist_data.max);

// String representation
std::string hist_str = statistics->getHistogramString(DB_GET);
printf("%s\n", hist_str.c_str());
```

### Histogram Implementation Details

From `monitoring/histogram.h:21-86`:

**Bucket mapping:** Histograms use `HistogramBucketMapper` with 109 buckets covering range [0, ~1.8e14) with increasing granularity. Small values get finer buckets, large values get coarser buckets.

**Thread safety:** `HistogramImpl` uses a mutex for merge operations. For core-local histograms in `StatisticsImpl`, each core has its own histogram to avoid contention.

**Data structure:**
```cpp
struct HistogramStat {
  std::atomic_uint_fast64_t min_;
  std::atomic_uint_fast64_t max_;
  std::atomic_uint_fast64_t num_;        // Count of samples
  std::atomic_uint_fast64_t sum_;        // Sum of all values
  std::atomic_uint_fast64_t sum_squares_; // For std dev calculation
  std::atomic_uint_fast64_t buckets_[109]; // Value distribution
};
```

**⚠️ INVARIANT:** Histogram percentiles are approximate. Exact value depends on bucket granularity. Median (p50) and percentiles are computed from bucket distribution, not exact values.

---

## InstrumentedMutex: Mutex Timing

### Overview

`InstrumentedMutex` wraps `port::Mutex` to automatically collect lock acquisition timing statistics. Used extensively in `DBImpl` for tracking DB mutex contention.

**Source files:**
- `monitoring/instrumented_mutex.h` - Wrapper class
- `monitoring/instrumented_mutex.cc` - Implementation

### Usage

```cpp
#include "monitoring/instrumented_mutex.h"

// Create instrumented mutex with stats tracking
InstrumentedMutex mu(statistics.get(), clock, DB_MUTEX_WAIT_MICROS);

// Lock/unlock - automatically records time to acquire lock
mu.Lock();
// ... critical section ...
mu.Unlock();

// RAII helper
{
  InstrumentedMutexLock lock(&mu);  // Acquires lock, records time
  // ... critical section ...
}  // Automatically releases lock

// Check mutex wait time (requires StatsLevel::kAll)
uint64_t wait_time = statistics->getTickerCount(DB_MUTEX_WAIT_MICROS);
```

### InstrumentedCondVar

```cpp
InstrumentedCondVar cv(&instrumented_mutex);

// Wait and signal work like std::condition_variable
cv.Wait();           // Releases mutex, waits, reacquires (records time)
cv.TimedWait(abs_time_us);  // Wait with timeout
cv.Signal();         // Wake one waiter
cv.SignalAll();      // Wake all waiters
```

**⚠️ INVARIANT:** `InstrumentedMutex` only records timing when:
1. `Statistics` object is provided (not nullptr)
2. `SystemClock` is provided
3. `StatsLevel` is set to `kAll` (mutex timing enabled)

**Performance consideration:** Measuring mutex lock time requires calling clock_gettime() inside the lock path. On systems where this is expensive, it can reduce write throughput in highly concurrent workloads.

From `monitoring/instrumented_mutex.h:20-61`, cache-aligned variant exists:
```cpp
class CacheAlignedInstrumentedMutex : public InstrumentedMutex {};
```
Used to avoid false sharing when mutexes are in an array or struct.

---

## DB Properties: Runtime Database Information

### Overview

DB properties provide runtime introspection into database state without needing a `Statistics` object. Properties can return strings, integers, or maps.

**Source files:**
- `include/rocksdb/db.h:1378-1455` - Public API
- `db/internal_stats.h` - Property handlers
- `db/internal_stats.cc` - Property implementation

### Property Types

**String properties (`GetProperty`):**
```cpp
std::string value;
db->GetProperty("rocksdb.stats", &value);  // Human-readable DB stats
db->GetProperty("rocksdb.sstables", &value);  // SST file listing
db->GetProperty("rocksdb.cfstats", &value);  // Column family stats
printf("%s\n", value.c_str());
```

**Integer properties (`GetIntProperty`):**
```cpp
uint64_t num_files;
db->GetIntProperty("rocksdb.num-files-at-level0", &num_files);
printf("L0 files: %llu\n", num_files);

uint64_t memtable_size;
db->GetIntProperty("rocksdb.cur-size-all-mem-tables", &memtable_size);
```

**Map properties (`GetMapProperty`):**
```cpp
std::map<std::string, std::string> props;
db->GetMapProperty("rocksdb.cfstats", &props);
for (auto& [key, value] : props) {
  printf("%s: %s\n", key.c_str(), value.c_str());
}
```

### Common Properties

From `db/internal_stats.cc` and `db/internal_stats.h:64-101`:

**Database-wide stats:**
- `rocksdb.stats` - Overall DB stats (formatted)
- `rocksdb.sstables` - List of all SST files with details
- `rocksdb.num-immutable-mem-table` - # of immutable memtables
- `rocksdb.cur-size-all-mem-tables` - Total memory used by memtables
- `rocksdb.size-all-mem-tables` - Total allocated memory for memtables
- `rocksdb.num-running-flushes` - # of flushes currently running
- `rocksdb.num-running-compactions` - # of compactions currently running
- `rocksdb.is-write-stopped` - Whether writes are currently stopped (1 or 0)
- `rocksdb.base-level` - Current base level for leveled compaction

**Column family stats:**
- `rocksdb.cfstats` - Detailed CF stats (formatted string or map)
- `rocksdb.cfstats-no-file-histogram` - CF stats without file size histogram
- `rocksdb.cf-file-histogram` - File size histogram for the CF

**Per-level stats (`rocksdb.num-files-at-levelN` where N=0..6):**
- `rocksdb.num-files-at-level0` - # of files in L0
- `rocksdb.num-files-at-level1` - # of files in L1
- ... etc

**Compression stats:**
- `rocksdb.compression-ratio-at-levelN` - Compression ratio at level N

**Aggregated properties (`GetAggregatedIntProperty` - across all CFs):**
```cpp
uint64_t total_l0_files;
db->GetAggregatedIntProperty("rocksdb.num-files-at-level0", &total_l0_files);
```

### Property Handlers

From `db/internal_stats.h:30-59`, properties are handled by callbacks:
```cpp
struct DBPropertyInfo {
  bool need_out_of_mutex;  // Can be queried without holding DB mutex?

  // One of these is set:
  bool (InternalStats::*handle_string)(std::string* value, Slice suffix);
  bool (InternalStats::*handle_int)(uint64_t* value, DBImpl* db, Version* version);
  bool (InternalStats::*handle_map)(std::map<std::string, std::string>* props, Slice suffix);
  bool (DBImpl::*handle_string_dbimpl)(std::string* value);
};
```

**⚠️ INVARIANT:** Properties with `need_out_of_mutex = true` can be safely queried concurrently. Properties with `need_out_of_mutex = false` may require holding the DB mutex internally.

### Stats Dumping

Periodic stats dumping to LOG file:

```cpp
options.stats_dump_period_sec = 600;  // Dump stats every 10 minutes (default: 600)
```

When enabled, RocksDB periodically calls `DBImpl::DumpStats()` which writes:
- `rocksdb.stats` - Overall stats
- `rocksdb.cfstats` for each column family
- `rocksdb.dbstats` - DB-wide statistics

The output is logged to the RocksDB LOG file for monitoring and debugging.

**⚠️ INVARIANT:** Stats dumping happens on a background thread managed by `PeriodicTaskScheduler`. Setting `stats_dump_period_sec = 0` disables periodic dumping.

---

## EventLogger: JSON Event Logging

### Overview

`EventLogger` logs important database events (flush, compaction, recovery) as JSON to the LOG file. Useful for debugging, monitoring, and post-mortem analysis.

**Source files:**
- `logging/event_logger.h` - EventLogger class
- `logging/event_logger.cc` - Implementation

### Event Types

Common events logged by RocksDB:

**Flush events:**
```json
{
  "time_micros": 1234567890,
  "job": 123,
  "event": "flush_started",
  "column_family": "default",
  "reason": "Write Buffer Full"
}
{
  "time_micros": 1234567950,
  "job": 123,
  "event": "flush_finished",
  "column_family": "default",
  "output_level": 0,
  "num_output_files": 1,
  "total_output_size": 1048576,
  "num_entries": 10000,
  "write_rate_bytes_per_sec": 10485760
}
```

**Compaction events:**
```json
{
  "time_micros": 1234568000,
  "job": 456,
  "event": "compaction_started",
  "compaction_reason": "LevelL0FilesNum",
  "files_L0": [1, 2, 3],
  "files_L1": [10, 11],
  "score": 4.5,
  "input_data_size": 41943040
}
{
  "time_micros": 1234578000,
  "job": 456,
  "event": "compaction_finished",
  "compaction_time_micros": 10000000,
  "compaction_time_cpu_micros": 9500000,
  "output_level": 1,
  "num_output_files": 2,
  "total_output_size": 41943040,
  "num_input_records": 100000,
  "num_output_records": 95000,
  "num_subcompactions": 4,
  "bytes_read": 41943040,
  "bytes_written": 39845888,
  "write_amplification": 0.95
}
```

**Recovery events:**
```json
{
  "time_micros": 1234560000,
  "event": "recovery_started",
  "wal_files": [1, 2, 3]
}
{
  "time_micros": 1234562000,
  "event": "recovery_finished",
  "wal_recovery_time_micros": 2000000
}
```

**Error events:**
```json
{
  "time_micros": 1234590000,
  "event": "background_error",
  "error": "Corruption: block checksum mismatch",
  "file": "/path/to/000123.sst"
}
```

### Using EventLogger

EventLogger is created internally by RocksDB and writes to the same LOG file. To parse events:

```bash
# Extract all JSON events from LOG file
grep '^\*\*JSON:' LOG | sed 's/^\*\*JSON: //' > events.json

# Parse with jq
jq 'select(.event == "compaction_finished")' events.json
jq 'select(.event == "flush_finished") | {time_micros, num_entries, total_output_size}' events.json
```

**Format:** Each event is prefixed with `**JSON: ` in the LOG file for easy extraction.

**⚠️ INVARIANT:** EventLogger output is intended for machine parsing. The JSON format is stable across versions (new fields may be added, but existing fields won't change type).

---

## Implementation Details

### Core-Local Statistics

From `monitoring/statistics_impl.h:85-106`:

`StatisticsImpl` uses **core-local** storage to avoid cache line contention:

```cpp
struct ALIGN_AS(CACHE_LINE_SIZE) StatisticsData {
  std::atomic_uint_fast64_t tickers_[INTERNAL_TICKER_ENUM_MAX];
  HistogramImpl histograms_[INTERNAL_HISTOGRAM_ENUM_MAX];
  // Padding to ensure cache line alignment
};

CoreLocalArray<StatisticsData> per_core_stats_;
```

**Why core-local?**
- Avoids false sharing: Each core updates its own cache line
- Scales to high core counts: No cross-core synchronization on hot path
- Read aggregation: `getTickerCount()` aggregates across all cores under a lock

**⚠️ INVARIANT:** Ticker increments (`recordTick`) are lock-free per-core atomics. Reading total (`getTickerCount`) requires aggregating all cores under `aggregate_lock_`.

### Thread-Local PerfContext/IOStatsContext

From `include/rocksdb/perf_context.h:331-340` and `include/rocksdb/iostats_context.h:124-131`:

```cpp
// Returns thread-local PerfContext (never null)
PerfContext* get_perf_context();

// Returns thread-local IOStatsContext (never null)
IOStatsContext* get_iostats_context();
```

**Implementation:**
- C++11 `thread_local` storage (when available)
- If RocksDB is compiled with `-DNPERF_CONTEXT` or `-DNIOSTATS_CONTEXT`, returns a global dummy object (no-op)

**⚠️ INVARIANT:** These functions never return nullptr. If thread-local is disabled, they return a read-only global object that ignores updates.

### Ticker/Histogram Name Mapping

From `monitoring/statistics.cc:20-591`:

```cpp
const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
  {BLOCK_CACHE_MISS, "rocksdb.block.cache.miss"},
  {BLOCK_CACHE_HIT, "rocksdb.block.cache.hit"},
  // ... 200+ tickers
};

const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap = {
  {DB_GET, "rocksdb.db.get.micros"},
  {DB_WRITE, "rocksdb.db.write.micros"},
  // ... 40+ histograms
};
```

**⚠️ INVARIANT:** The order in these vectors **must** match the order in the enum definitions. This is enforced by assertions at startup.

### Recording Metrics (Internal Helpers)

From `monitoring/statistics_impl.h:114-142`:

```cpp
// Helper functions used throughout RocksDB codebase
inline void RecordTick(Statistics* statistics, uint32_t ticker_type, uint64_t count = 1) {
  if (statistics) {
    statistics->recordTick(ticker_type, count);
  }
}

inline void RecordInHistogram(Statistics* statistics, uint32_t histogram_type, uint64_t value) {
  if (statistics) {
    statistics->recordInHistogram(histogram_type, value);
  }
}

inline void RecordTimeToHistogram(Statistics* statistics, uint32_t histogram_type, uint64_t value) {
  if (statistics) {
    statistics->reportTimeToHistogram(histogram_type, value);  // Respects StatsLevel
  }
}
```

**Usage example from `table/block_based/block_based_table_reader.cc`:**
```cpp
RecordTick(statistics, BLOCK_CACHE_HIT);
RecordTick(statistics, BLOCK_CACHE_DATA_HIT);
RecordInHistogram(statistics, READ_BLOCK_GET_MICROS, elapsed_us);
```

### Disabling Monitoring at Compile Time

```cpp
// Disable PerfContext (all operations become no-ops)
cmake .. -DNPERF_CONTEXT

// Disable IOStatsContext (all operations become no-ops)
cmake .. -DNIOSTATS_CONTEXT
```

When disabled, `get_perf_context()` and `get_iostats_context()` return pointers to global dummy objects that ignore all updates and always return zeros.

---

## Performance Considerations

### Overhead Comparison

From least to most expensive:

1. **Statistics tickers** (`StatsLevel::kExceptDetailedTimers`): ~1-2 atomic increments per operation. Core-local reduces contention.

2. **Histograms** (`StatsLevel::kExceptHistogramOrTimers` or higher): Additional atomic operations for buckets, min/max, sum, sum_squares. Slightly more expensive than tickers.

3. **PerfContext counters** (`kEnableCount`): Thread-local, no contention. Minimal overhead (~1 increment per tracked event).

4. **PerfContext/IOStatsContext timing** (`kEnableTime`): Requires `clock_gettime()` calls. Can be 10-100ns per call depending on platform.

5. **InstrumentedMutex** (`StatsLevel::kAll`): Adds `clock_gettime()` calls **inside the mutex critical path**. Can reduce throughput by 10-20% in mutex-heavy workloads on some platforms.

**Recommendation:**
- **Production**: `StatsLevel::kExceptDetailedTimers` + `PerfLevel::kDisable`
- **Performance debugging**: `StatsLevel::kExceptTimeForMutex` + `PerfLevel::kEnableTimeExceptForMutex`
- **Deep debugging**: `StatsLevel::kAll` + `PerfLevel::kEnableTime` (measure impact before deploying)

### Best Practices

1. **Use Statistics for global trends**, PerfContext/IOStatsContext for per-request debugging
2. **Enable PerfContext only when debugging** - reset before/after the operation of interest
3. **Avoid StatsLevel::kAll in production** unless you've measured the overhead and it's acceptable
4. **Use per-level PerfContext sparingly** - it allocates memory per level
5. **Parse EventLogger JSON offline** - it's designed for post-mortem analysis, not real-time monitoring

### Memory Usage

- **Statistics**: ~100KB for all tickers + histograms (per-core, so ~100KB × num_cores)
- **PerfContext**: ~2KB per thread (only threads that call `get_perf_context()`)
- **IOStatsContext**: ~200 bytes per thread
- **Per-level PerfContext**: +64 bytes per active level when enabled

---

## Summary

| Component | Scope | Use Case | Overhead | When to Use |
|-----------|-------|----------|----------|-------------|
| **Statistics** | Global, cumulative | Production monitoring, long-term trends | Low (core-local atomics) | Always enabled with `StatsLevel::kExceptDetailedTimers` |
| **PerfContext** | Thread-local, resettable | Per-request debugging, profiling specific operations | Low-Medium (depends on PerfLevel) | Enable temporarily for debugging |
| **IOStatsContext** | Thread-local, resettable | I/O bottleneck diagnosis | Low-Medium (depends on PerfLevel) | Enable temporarily for I/O profiling |
| **Histograms** | Global, distributions | Latency analysis (P50/P95/P99) | Low-Medium | Enabled by StatsLevel |
| **InstrumentedMutex** | Specific mutexes | Mutex contention debugging | Medium-High (if timing enabled) | Only enable with `StatsLevel::kAll` when debugging |
| **DB Properties** | Real-time DB state | Runtime introspection, health checks | Negligible (read-only) | Use as needed |
| **EventLogger** | Async JSON logging | Post-mortem analysis, monitoring | Very low (async) | Always enabled (automatic) |

**⚠️ CRITICAL:** Always measure the performance impact of monitoring in your workload before deploying to production. The overhead varies significantly depending on platform, CPU, and access patterns.
