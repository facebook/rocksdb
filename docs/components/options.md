# RocksDB Options and Configuration System

## Overview

RocksDB's configuration system provides fine-grained control over database behavior through a hierarchical options framework. The system separates database-wide settings from per-column-family settings, and distinguishes between options that can be changed at runtime (mutable) and those that are fixed at database creation (immutable).

**Key source files:**
- `include/rocksdb/options.h` - Main options definitions
- `include/rocksdb/advanced_options.h` - Advanced column family options
- `include/rocksdb/table.h` - Table format options (BlockBasedTableOptions)
- `options/db_options.{h,cc}` - DB options implementation
- `options/cf_options.{h,cc}` - Column family options implementation
- `options/options_helper.{h,cc}` - Options parsing and serialization
- `options/options.cc` - Options base implementation

---

## Options Hierarchy

### 1. DBOptions

Database-wide settings that apply to the entire RocksDB instance, regardless of column families.

**Defined in:** `include/rocksdb/options.h:569`

**Key responsibilities:**
- File management (max_open_files, max_file_opening_threads)
- Background thread configuration (max_background_jobs, max_background_compactions)
- WAL management (wal_dir, WAL_ttl_seconds, WAL_size_limit_MB)
- Environment and file system (env, use_fsync, allow_mmap_reads)
- Write throttling (delayed_write_rate)
- Statistics and logging (info_log, statistics)

**Example:**
```cpp
DBOptions db_options;
db_options.create_if_missing = true;
db_options.max_background_jobs = 4;
db_options.max_open_files = 5000;
db_options.wal_dir = "/path/to/wal";
```

### 2. ColumnFamilyOptions

Settings specific to a column family, controlling memtables, compaction, and table formats.

**Defined in:** `include/rocksdb/options.h:70`

**Inherits from:** `AdvancedColumnFamilyOptions`

**Key responsibilities:**
- Memtable configuration (write_buffer_size, max_write_buffer_number)
- Compaction style and triggers (compaction_style, level0_file_num_compaction_trigger)
- Compression (compression, bottommost_compression)
- Table format (table_factory)
- Bloom filters and prefix extraction (prefix_extractor)

**Example:**
```cpp
ColumnFamilyOptions cf_options;
cf_options.write_buffer_size = 64 << 20;  // 64MB
cf_options.compression = kSnappyCompression;
cf_options.level0_file_num_compaction_trigger = 4;
```

### 3. Options (Combined)

Convenience struct combining DBOptions and ColumnFamilyOptions.

**Defined in:** `include/rocksdb/options.h` (via inheritance)

**Inheritance chain:**
```
Options
  ├─ DBOptions
  └─ ColumnFamilyOptions
       └─ AdvancedColumnFamilyOptions
```

**Usage:**
```cpp
Options options;
options.create_if_missing = true;              // DBOptions
options.write_buffer_size = 64 << 20;          // ColumnFamilyOptions
DB* db;
Status s = DB::Open(options, "/path/to/db", &db);
```

⚠️ **INVARIANT**: When opening a database with multiple column families, you must use separate DBOptions and ColumnFamilyOptions for each column family, not the combined Options struct.

### 4. ReadOptions

Per-read operation settings controlling read behavior.

**Defined in:** `include/rocksdb/options.h` (separate header section)

**Key settings:**
- `verify_checksums` - Verify block checksums on read
- `fill_cache` - Should blocks be added to block cache
- `snapshot` - Read from a specific snapshot
- `iterate_lower_bound`, `iterate_upper_bound` - Iterator bounds
- `read_tier` - Control whether to read from cache only
- `tailing` - Enable tailing iterator
- `io_activity` - Tag I/O for monitoring

**Example:**
```cpp
ReadOptions read_options;
read_options.verify_checksums = true;
read_options.fill_cache = true;
std::string value;
db->Get(read_options, "key", &value);
```

### 5. WriteOptions

Per-write operation settings controlling write behavior.

**Defined in:** `include/rocksdb/options.h` (separate header section)

**Key settings:**
- `sync` - Force fsync after write
- `disableWAL` - Skip WAL for this write
- `low_pri` - Mark as low priority
- `no_slowdown` - Return error instead of stalling
- `rate_limiter_priority` - Rate limiter priority tier
- `io_activity` - Tag I/O for monitoring

**Example:**
```cpp
WriteOptions write_options;
write_options.sync = false;  // Don't fsync (faster but less durable)
write_options.disableWAL = false;
db->Put(write_options, "key", "value");
```

---

## Mutable vs Immutable Options

RocksDB distinguishes between options that can be changed while the database is running (mutable) and those that are fixed at creation time (immutable). This separation is enforced through internal structs.

### Immutable Options

Options that cannot be changed after database/column family creation without closing and reopening.

**ImmutableDBOptions** (`options/db_options.h:16`)
```cpp
struct ImmutableDBOptions {
  bool create_if_missing;
  bool paranoid_checks;
  Env* env;
  std::shared_ptr<RateLimiter> rate_limiter;
  std::shared_ptr<Logger> info_log;
  WALRecoveryMode wal_recovery_mode;
  bool atomic_flush;
  std::string wal_dir;
  // ... 50+ more fields
};
```

**ImmutableCFOptions** (`options/cf_options.h:22`)
```cpp
struct ImmutableCFOptions {
  CompactionStyle compaction_style;
  CompactionPri compaction_pri;
  const Comparator* user_comparator;
  std::shared_ptr<MergeOperator> merge_operator;
  int min_write_buffer_number_to_merge;
  std::shared_ptr<MemTableRepFactory> memtable_factory;
  int num_levels;
  bool persist_user_defined_timestamps;
  // ... more fields
};
```

⚠️ **INVARIANT**: Changing immutable options requires closing the database, modifying the OPTIONS file, and reopening. Attempting to set immutable options via `SetOptions()` or `SetDBOptions()` returns an error.

### Mutable Options

Options that can be changed dynamically at runtime using `DB::SetOptions()` or `DB::SetDBOptions()`.

**MutableDBOptions** (`options/db_options.h:124`)
```cpp
struct MutableDBOptions {
  int max_background_jobs;
  int max_background_compactions;
  uint32_t max_subcompactions;
  uint64_t delayed_write_rate;
  uint64_t max_total_wal_size;
  uint64_t bytes_per_sync;
  int max_open_files;
  std::string daily_offpeak_time_utc;
  // ... ~20 fields
};
```

**MutableCFOptions** (`options/cf_options.h:108`)
```cpp
struct MutableCFOptions {
  // Memtable options
  size_t write_buffer_size;
  int max_write_buffer_number;
  double memtable_prefix_bloom_size_ratio;

  // Compaction options
  bool disable_auto_compactions;
  int level0_file_num_compaction_trigger;
  int level0_slowdown_writes_trigger;
  int level0_stop_writes_trigger;
  uint64_t max_bytes_for_level_base;
  uint64_t soft_pending_compaction_bytes_limit;
  uint64_t hard_pending_compaction_bytes_limit;

  // Compression options
  CompressionType compression;
  CompressionType bottommost_compression;

  // Blob options
  bool enable_blob_files;
  uint64_t min_blob_size;
  uint64_t blob_file_size;

  // ... ~60 fields total
};
```

### Changing Options at Runtime

**For DB-wide options:**
```cpp
std::unordered_map<std::string, std::string> new_options;
new_options["max_background_jobs"] = "6";
new_options["bytes_per_sync"] = "1048576";  // 1MB
Status s = db->SetDBOptions(new_options);
```

**For column family options:**
```cpp
std::unordered_map<std::string, std::string> new_options;
new_options["write_buffer_size"] = "134217728";  // 128MB
new_options["level0_file_num_compaction_trigger"] = "2";
Status s = db->SetOptions(cf_handle, new_options);
```

⚠️ **INVARIANT**: `SetOptions()` validates that all provided options are mutable. If any immutable option is included, the entire operation fails and no options are changed.

**Implementation:** `db/db_impl/db_impl.cc` - `DBImpl::SetOptions()` and `DBImpl::SetDBOptions()`

---

## Options Serialization and Persistence

### OPTIONS File Format

RocksDB automatically persists options to disk in OPTIONS-XXXXXX files in the database directory. These are human-readable text files using an INI-like format.

**File naming:** `OPTIONS-{file-number}` where file-number is a sequence number

**Generated during:**
- DB::Open() - Creates initial OPTIONS file
- Option changes via SetOptions() - Creates new OPTIONS file
- DB recovery - Preserves options from MANIFEST

**Format structure:**
```ini
[Version]
  rocksdb_version=9.8.0
  options_file_version=1.1

[DBOptions]
  stats_dump_period_sec=600
  max_background_jobs=2
  bytes_per_sync=1048576
  wal_dir=
  create_if_missing=false
  # ... all DB options

[CFOptions "default"]
  comparator=leveldb.BytewiseComparator
  merge_operator=nullptr
  compaction_style=kCompactionStyleLevel
  write_buffer_size=67108864
  compression=kSnappyCompression
  num_levels=7
  # ... all CF options

[TableOptions/BlockBasedTable "default"]
  block_size=4096
  block_cache=0x7f8a9c000000
  filter_policy=nullptr
  cache_index_and_filter_blocks=false
  # ... table options
```

**Reading OPTIONS files:**
```cpp
DBOptions db_options;
std::vector<ColumnFamilyDescriptor> cf_descs;
Status s = LoadLatestOptions("/path/to/db", Env::Default(),
                             &db_options, &cf_descs);
```

**Writing OPTIONS files manually:**
```cpp
std::string options_str;
Status s = GetStringFromDBOptions(&options_str, db_options);
// Write options_str to file
```

### GetStringFromDBOptions / GetStringFromColumnFamilyOptions

Convert options structs to string representation.

**Defined in:** `include/rocksdb/convenience.h`

```cpp
Status GetStringFromDBOptions(std::string* opt_string,
                               const DBOptions& db_options,
                               const std::string& delimiter = ";  ");

Status GetStringFromColumnFamilyOptions(std::string* opt_string,
                                       const ColumnFamilyOptions& cf_options,
                                       const std::string& delimiter = ";  ");
```

**Example:**
```cpp
std::string db_opts_str;
GetStringFromDBOptions(&db_opts_str, db_options);
// Output: "max_background_jobs=4;  bytes_per_sync=1048576;  ..."

std::string cf_opts_str;
GetStringFromColumnFamilyOptions(&cf_opts_str, cf_options);
// Output: "write_buffer_size=67108864;  compression=kSnappyCompression;  ..."
```

### Options Parsing from Strings

Parse option strings back into options structs.

**Defined in:** `include/rocksdb/convenience.h`

```cpp
Status GetDBOptionsFromString(const DBOptions& base_options,
                              const std::string& opts_str,
                              DBOptions* new_options);

Status GetColumnFamilyOptionsFromString(const ColumnFamilyOptions& base_options,
                                       const std::string& opts_str,
                                       ColumnFamilyOptions* new_options);
```

**Example:**
```cpp
DBOptions base_options;
DBOptions new_options;
std::string opts = "max_background_jobs=6;bytes_per_sync=1048576";
Status s = GetDBOptionsFromString(base_options, opts, &new_options);

ColumnFamilyOptions base_cf;
ColumnFamilyOptions new_cf;
std::string cf_opts = "write_buffer_size=134217728;compression=kZSTD";
s = GetColumnFamilyOptionsFromString(base_cf, cf_opts, &new_cf);
```

**Parsing from map:**
```cpp
Status GetDBOptionsFromMap(const DBOptions& base_options,
                           const std::unordered_map<std::string, std::string>& opts_map,
                           DBOptions* new_options);

Status GetColumnFamilyOptionsFromMap(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options);
```

⚠️ **INVARIANT**: Option parsing is type-aware and validates values. Invalid option names or malformed values return Status::InvalidArgument.

---

## OptionTypeInfo Framework

The OptionTypeInfo system provides type-safe, metadata-driven option registration, parsing, and serialization. It enables RocksDB to automatically handle option conversion between strings and native types.

**Defined in:** `include/rocksdb/utilities/options_type.h`

### OptionType Enumeration

Defines the type system for options:

```cpp
enum class OptionType {
  kBoolean,
  kInt, kInt32T, kInt64T,
  kUInt, kUInt8T, kUInt32T, kUInt64T,
  kSizeT,
  kDouble,
  kString,
  kCompactionStyle,
  kCompactionPri,
  kCompressionType,
  kEnum,
  kStruct,
  kVector,
  kConfigurable,
  kCustomizable,
  // ... more specialized types
};
```

### OptionTypeInfo Class

Maps option names to their types, offsets within structs, and parsing/serialization functions.

**Core structure:**
```cpp
class OptionTypeInfo {
  OptionType type_;
  int offset_;  // Offset of field within struct
  ParseFunc parse_func_;
  SerializeFunc serialize_func_;
  EqualsFunc equals_func_;
  // ... more metadata
};
```

**Usage in cf_options.cc:**
```cpp
static std::unordered_map<std::string, OptionTypeInfo> cf_options_type_info = {
  {"write_buffer_size",
   {offsetof(struct MutableCFOptions, write_buffer_size),
    OptionType::kSizeT, OptionVerificationType::kNormal,
    OptionTypeFlags::kMutable}},
  {"max_write_buffer_number",
   {offsetof(struct MutableCFOptions, max_write_buffer_number),
    OptionType::kInt, OptionVerificationType::kNormal,
    OptionTypeFlags::kMutable}},
  {"compression",
   {offsetof(struct MutableCFOptions, compression),
    OptionType::kCompressionType, OptionVerificationType::kNormal,
    OptionTypeFlags::kMutable}},
  // ... hundreds more entries
};
```

### Automatic Parsing and Serialization

The OptionTypeInfo framework enables automatic conversion:

**String → Native Type:**
```cpp
// Framework looks up "write_buffer_size" in type map
// Finds: type=kSizeT, offset=offsetof(..., write_buffer_size)
// Parses "134217728" as size_t
// Writes to options struct at correct offset
```

**Native Type → String:**
```cpp
// Framework iterates all registered options
// For each option, reads value at offset
// Converts to string based on OptionType
// Produces "write_buffer_size=134217728"
```

⚠️ **INVARIANT**: All options exposed via SetOptions() / GetOptionsFromString() must be registered in the OptionTypeInfo maps. Unregistered options cannot be parsed or serialized.

---

## Options Validation

### SanitizeOptions

Validates and adjusts options for consistency before use. Called automatically during DB::Open().

**Defined in:** `db/column_family.cc`, `db/db_impl/db_impl_open.cc`

**Column family sanitization:**
```cpp
ColumnFamilyOptions SanitizeOptions(const ImmutableDBOptions& db_options,
                                   const ColumnFamilyOptions& src) {
  ColumnFamilyOptions result = src;

  // Ensure write_buffer_size is reasonable
  if (result.write_buffer_size < 1024) {
    result.write_buffer_size = 1024;
  }

  // Adjust max_write_buffer_number_to_merge
  if (result.max_write_buffer_number_to_merge < 1) {
    result.max_write_buffer_number_to_merge = 1;
  }

  // Ensure num_levels is valid
  if (result.num_levels < 1) {
    result.num_levels = 1;
  }

  // Set default table factory if missing
  if (result.table_factory == nullptr) {
    result.table_factory.reset(new BlockBasedTableFactory());
  }

  // More validation...
  return result;
}
```

**Key validations:**
- Enforces minimum values (write_buffer_size >= 1KB)
- Sets defaults for nullptr shared_ptrs (table_factory, merge_operator)
- Adjusts derived values (max_bytes_for_level_multiplier_additional)
- Validates compression settings
- Ensures comparator and merge operator are non-null if needed

### Consistency Checks

Additional validation beyond SanitizeOptions:

**ValidateOptions** (`options/options_helper.cc:41`):
```cpp
Status ValidateOptions(const DBOptions& db_opts,
                       const ColumnFamilyOptions& cf_opts) {
  auto db_cfg = DBOptionsAsConfigurable(db_opts);
  auto cf_cfg = CFOptionsAsConfigurable(cf_opts);
  Status s = db_cfg->ValidateOptions(db_opts, cf_opts);
  if (s.ok()) {
    s = cf_cfg->ValidateOptions(db_opts, cf_opts);
  }
  return s;
}
```

**Common validation errors:**
- Incompatible compaction_style and compaction_pri
- Invalid level0_file_num_compaction_trigger values
- Conflicting prefix_extractor and comparator
- Invalid table factory configurations

⚠️ **INVARIANT**: SanitizeOptions runs automatically on DB::Open(). Users should not call it directly. Manually sanitized options may be overwritten.

---

## Advanced Options

### AdvancedColumnFamilyOptions

Extended column family options for advanced tuning.

**Defined in:** `include/rocksdb/advanced_options.h`

**Key advanced settings:**

**Memtable:**
- `inplace_update_support` - Enable in-place updates for small values
- `inplace_update_num_locks` - Concurrency for in-place updates
- `experimental_mempurge_threshold` - Memtable garbage collection trigger
- `memtable_insert_with_hint_prefix_extractor` - Optimize memtable inserts

**Compaction:**
- `max_compaction_bytes` - Max bytes in a single compaction job
- `soft_pending_compaction_bytes_limit` - Soft limit for pending compaction
- `hard_pending_compaction_bytes_limit` - Hard limit (stops writes)
- `compaction_options_universal` - Universal compaction tuning
- `compaction_options_fifo` - FIFO compaction tuning

**Performance:**
- `max_sequential_skip_in_iterations` - Skip deleted keys in iterator
- `optimize_filters_for_hits` - Optimize bloom filters for Get()
- `paranoid_file_checks` - Extra data integrity checks
- `report_bg_io_stats` - Report background I/O to statistics

**Blob storage (BlobDB):**
- `enable_blob_files` - Store large values separately
- `min_blob_size` - Minimum value size to store as blob
- `blob_file_size` - Target blob file size
- `blob_compression_type` - Compression for blobs
- `enable_blob_garbage_collection` - Compact blob files

**Example:**
```cpp
ColumnFamilyOptions options;
options.write_buffer_size = 128 << 20;
options.max_compaction_bytes = 10 * options.target_file_size_base;
options.optimize_filters_for_hits = true;

// Blob storage for values >= 4KB
options.enable_blob_files = true;
options.min_blob_size = 4096;
options.blob_file_size = 256 << 20;
options.enable_blob_garbage_collection = true;
```

### BlockBasedTableOptions

Configuration for the block-based table format (default SST format).

**Defined in:** `include/rocksdb/table.h`

**Key settings:**

**Block cache:**
- `block_cache` - Shared cache for data blocks
- `block_cache_compressed` - Cache for compressed blocks
- `cache_index_and_filter_blocks` - Cache index/filter in block cache
- `pin_l0_filter_and_index_blocks_in_cache` - Pin L0 metadata

**Block configuration:**
- `block_size` - Target size for data blocks (default 4KB)
- `block_size_deviation` - Allowed deviation from target
- `block_restart_interval` - Keys between restart points

**Index and filters:**
- `index_type` - kBinarySearch, kHashSearch, kTwoLevelIndexSearch
- `data_block_index_type` - kDataBlockBinarySearch, kDataBlockBinaryAndHash
- `filter_policy` - Bloom filter policy
- `whole_key_filtering` - Add full keys to bloom filter
- `partition_filters` - Partition filters for top-level index

**Checksum and compression:**
- `checksum` - Checksum type (kCRC32c, kxxHash, kXXH3)
- `format_version` - Block format version (0-5)
- `enable_index_compression` - Compress index blocks
- `data_block_hash_table_util_ratio` - Hash table load factor

**Performance:**
- `prepopulate_block_cache` - Warm cache on SST creation
- `read_amp_bytes_per_bit` - Read amplification tracking granularity

**Example:**
```cpp
BlockBasedTableOptions table_options;
table_options.block_cache = NewLRUCache(512 << 20);  // 512MB cache
table_options.block_size = 16 * 1024;  // 16KB blocks
table_options.cache_index_and_filter_blocks = true;
table_options.pin_l0_filter_and_index_blocks_in_cache = true;

// Use bloom filter with 10 bits per key
table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
table_options.whole_key_filtering = true;

// Two-level index for large SSTs
table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;

// XXH3 checksum (fastest)
table_options.checksum = kXXH3;

ColumnFamilyOptions cf_options;
cf_options.table_factory.reset(NewBlockBasedTableFactory(table_options));
```

⚠️ **INVARIANT**: BlockBasedTableOptions must be set before DB::Open(). They cannot be changed dynamically via SetOptions().

---

## Common Configuration Patterns

### Write-Heavy Workload

Optimize for high write throughput with acceptable read latency.

```cpp
Options options;

// Large memtables to batch writes
options.write_buffer_size = 256 << 20;  // 256MB
options.max_write_buffer_number = 4;
options.min_write_buffer_number_to_merge = 2;

// Delay L0→L1 compaction for larger L1
options.level0_file_num_compaction_trigger = 8;
options.level0_slowdown_writes_trigger = 17;
options.level0_stop_writes_trigger = 24;

// Larger L1 to match L0 size
options.max_bytes_for_level_base = 1024 << 20;  // 1GB
options.target_file_size_base = 128 << 20;  // 128MB

// More background threads
options.max_background_jobs = 8;
options.max_subcompactions = 4;

// Disable compression on L0/L1 for faster writes
options.compression_per_level.resize(7);
options.compression_per_level[0] = kNoCompression;
options.compression_per_level[1] = kNoCompression;
for (int i = 2; i < 7; i++) {
  options.compression_per_level[i] = kZSTD;
}

// Blob storage for large values to avoid write amplification
options.enable_blob_files = true;
options.min_blob_size = 1024;  // 1KB threshold
```

### Read-Heavy Workload

Optimize for low read latency and high cache hit rate.

```cpp
Options options;

// Large block cache
BlockBasedTableOptions table_options;
table_options.block_cache = NewLRUCache(4ULL << 30);  // 4GB
table_options.cache_index_and_filter_blocks = true;
table_options.pin_l0_filter_and_index_blocks_in_cache = true;

// Bloom filter for point lookups
table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
table_options.whole_key_filtering = true;
options.table_factory.reset(NewBlockBasedTableFactory(table_options));

// Optimize filters for Get() hit rate
options.optimize_filters_for_hits = true;

// Smaller blocks for better cache efficiency
table_options.block_size = 4 * 1024;  // 4KB

// More aggressive compaction to reduce read amplification
options.level0_file_num_compaction_trigger = 2;
options.max_bytes_for_level_base = 256 << 20;  // 256MB

// Use all CPU cores for parallelism
options.max_background_jobs = 16;

// Keep more files open
options.max_open_files = -1;  // Keep all files open
```

### Space-Optimized (Minimize Storage)

Minimize storage footprint with aggressive compression.

```cpp
Options options;

// Smaller memtables to reduce memory
options.write_buffer_size = 32 << 20;  // 32MB
options.max_write_buffer_number = 2;

// Aggressive compression
options.compression = kZSTD;
options.bottommost_compression = kZSTD;
options.compression_opts.level = 9;  // Max compression
options.bottommost_compression_opts.level = 19;  // Ultra compression

// Smaller target file sizes
options.target_file_size_base = 32 << 20;  // 32MB

// Enable blob GC to reclaim space
options.enable_blob_files = true;
options.min_blob_size = 4096;
options.enable_blob_garbage_collection = true;
options.blob_garbage_collection_age_cutoff = 0.5;

// Periodic compaction to clean up old data
options.periodic_compaction_seconds = 7 * 24 * 3600;  // Weekly

// Smaller block cache
BlockBasedTableOptions table_options;
table_options.block_cache = NewLRUCache(256 << 20);  // 256MB
options.table_factory.reset(NewBlockBasedTableFactory(table_options));
```

### Universal Compaction (Minimize Write Amplification)

Use universal compaction style for write-heavy workloads with large datasets.

```cpp
Options options;
options.compaction_style = kCompactionStyleUniversal;

// Configure universal compaction
options.compaction_options_universal.size_ratio = 1;
options.compaction_options_universal.min_merge_width = 2;
options.compaction_options_universal.max_merge_width = 10;
options.compaction_options_universal.max_size_amplification_percent = 200;
options.compaction_options_universal.compression_size_percent = 80;
options.compaction_options_universal.stop_style = kCompactionStopStyleTotalSize;

// Large memtables since compaction is less frequent
options.write_buffer_size = 128 << 20;  // 128MB
options.max_write_buffer_number = 4;
options.level0_file_num_compaction_trigger = 2;

// Only one level in universal compaction
options.num_levels = 1;

// High background job count
options.max_background_jobs = 8;
```

### Small Database (< 1GB)

Optimized for small datasets to minimize memory overhead.

```cpp
Options options;
options.OptimizeForSmallDb();

// Equivalent to:
// DBOptions:
//   max_file_opening_threads = 1
//   max_open_files = 5000
//   write_buffer_manager = shared with block cache
//
// ColumnFamilyOptions:
//   write_buffer_size = 2MB
//   target_file_size_base = 2MB
//   max_bytes_for_level_base = 10MB
//   block_cache = 16MB
//   cache_index_and_filter_blocks = true
//   index_type = kTwoLevelIndexSearch
```

---

## Options Change Workflow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     User Application                        │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
  ┌──────────┐        ┌──────────┐       ┌──────────┐
  │DB::Open()│        │SetOptions│       │LoadLatest│
  │          │        │          │       │Options   │
  └─────┬────┘        └─────┬────┘       └─────┬────┘
        │                   │                   │
        ▼                   ▼                   ▼
  ┌─────────────────────────────────────────────────┐
  │         Options Validation & Sanitization        │
  │  - SanitizeOptions()                             │
  │  - ValidateOptions()                             │
  │  - Set defaults for nullptr                      │
  └──────────────────┬──────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
  ┌──────────┐ ┌──────────┐ ┌──────────┐
  │Immutable │ │ Mutable  │ │ OPTIONS  │
  │DBOptions │ │DBOptions │ │   File   │
  └──────────┘ └──────────┘ └──────────┘
        │            │            │
        ▼            ▼            ▼
  ┌──────────┐ ┌──────────┐ ┌──────────┐
  │Immutable │ │ Mutable  │ │ MANIFEST │
  │CFOptions │ │CFOptions │ │          │
  └──────────┘ └──────────┘ └──────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │  Runtime Option Changes    │
        │  via SetOptions() allowed  │
        │  only for Mutable fields   │
        └────────────────────────────┘
```

---

## Testing and Validation

### options_settable_test.cc

Comprehensive test ensuring all options can be set/get correctly via string API.

**Key tests:**
- All registered options are parseable from strings
- Parsed options serialize back to the same string
- Mutable options can be changed via SetOptions()
- Immutable options are rejected by SetOptions()

### options_test.cc

Tests option parsing, validation, and special behaviors.

**Coverage:**
- GetDBOptionsFromString() / GetColumnFamilyOptionsFromString()
- GetDBOptionsFromMap() / GetColumnFamilyOptionsFromMap()
- Option validation edge cases
- Backward compatibility with old option names

---

## Key Invariants Summary

⚠️ **OPTIONS HIERARCHY**:
- DBOptions applies database-wide
- ColumnFamilyOptions applies per-column-family
- Options combines both for single-CF convenience
- Multi-CF databases must use separate DBOptions and ColumnFamilyOptions

⚠️ **MUTABILITY**:
- Immutable options: Fixed at DB/CF creation, require reopen to change
- Mutable options: Can change via SetOptions() / SetDBOptions() at runtime
- SetOptions() validates mutability and rejects immutable changes atomically

⚠️ **PERSISTENCE**:
- OPTIONS-XXXXXX files auto-generated on DB::Open() and option changes
- OPTIONS files are human-readable INI format
- Latest OPTIONS file loaded automatically on recovery
- Manual OPTIONS edits require DB reopen to take effect

⚠️ **PARSING & SERIALIZATION**:
- All dynamically settable options must be registered in OptionTypeInfo maps
- String parsing is type-aware and validates values
- Invalid option names or malformed values return Status::InvalidArgument
- Option names are case-sensitive

⚠️ **VALIDATION**:
- SanitizeOptions() runs automatically in DB::Open()
- Enforces minimum values and sets missing defaults
- ValidateOptions() checks cross-option consistency
- Validation failures prevent DB from opening

⚠️ **TABLE OPTIONS**:
- BlockBasedTableOptions cannot be changed dynamically
- Table options set before DB::Open() are immutable
- Changing table format requires closing DB, updating OPTIONS, and reopening

---

## See Also

- `ARCHITECTURE.md` - Overall RocksDB architecture
- `docs/components/write_flow.md` - How write_buffer_size affects write path
- `docs/components/compaction.md` - Compaction-related options in detail
- `docs/components/cache.md` - block_cache configuration and tuning
- RocksDB Wiki: Tuning Guide - https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
