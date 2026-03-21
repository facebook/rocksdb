# RocksDB Tiered Storage and Data Temperature System

**Authors**: RocksDB Team
**Last Updated**: 2026-03-21

## Overview

RocksDB's tiered storage system enables storing data at different temperature tiers based on access patterns and data age. The temperature abstraction allows the FileSystem layer to place files on different storage media (e.g., fast SSD for hot data, cheaper HDD or cloud storage for cold data) and apply different I/O policies (caching, rate limiting, prefetching) based on temperature hints.

**Key Concepts**:
- Per-file temperature hints guide storage placement and I/O policies
- Temperatures flow through compaction: cold data migrates to lower levels, hot data stays in upper levels
- Temperature assignment happens automatically based on level, age, or explicit configuration
- FileSystem receives temperature hints for all file operations (create, read, write)

**Related Components**: Compaction (temperature-aware file selection), FileSystem (temperature-based placement), TableCache (temperature-aware caching), BlobDB (blob file temperature)

---

## 1. Temperature Concept

### Temperature Enum

RocksDB defines six temperature levels in `include/rocksdb/types.h:118`:

```cpp
enum class Temperature : uint8_t {
  kUnknown = 0,    // No temperature assigned (default)
  kHot = 0x04,     // Frequently accessed data
  kWarm = 0x08,    // Moderately accessed data
  kCool = 0x0A,    // Rarely accessed data
  kCold = 0x0C,    // Very rarely accessed data
  kIce = 0x10,     // Archival data
  kLastTemperature // Sentinel value (invalid)
};
```

**⚠️ INVARIANT**: Temperature values are sparse (gaps between values) to allow future insertion of intermediate tiers without breaking compatibility.

**⚠️ INVARIANT**: `kUnknown` means "no explicit temperature", not "unknown temperature". Files with `kUnknown` temperature use default placement policies.

### Temperature Semantics

- **kHot**: Data likely to be read soon (recently written, frequently accessed)
- **kWarm**: Data accessed occasionally (aging data in middle levels)
- **kCool**: Data accessed infrequently (older data in lower levels)
- **kCold**: Data rarely accessed (last-level data, archival)
- **kIce**: Data almost never accessed (cold archival tier)

Temperature is a **hint**, not a strict guarantee. The FileSystem implementation decides actual placement based on temperature, available storage tiers, and policy.

---

## 2. Per-Level Temperature Assignment

### Configuration Options

Three main options control temperature assignment (`include/rocksdb/advanced_options.h:956-989`):

1. **`last_level_temperature`** (default: `Temperature::kUnknown`)
   - Temperature for files in the last level (bottommost level)
   - When set (not `kUnknown`), all files compacted to the last level get this temperature
   - Dynamically changeable via `SetOptions()`

2. **`default_write_temperature`** (default: `Temperature::kUnknown`)
   - Fallback temperature when no other option determines temperature
   - Used for flush output and non-last-level compaction output
   - Dynamically changeable via `SetOptions()`

3. **`level_compaction_dynamic_level_bytes`** (default: `false`)
   - When enabled, RocksDB uses dynamic leveling: levels grow dynamically, and the "last level" can shift
   - Affects which level is considered "last" for `last_level_temperature` assignment

### Temperature Assignment Rules

**Flush Output**:
- Flushed SST files get `default_write_temperature`
- Never get `last_level_temperature` (flush always produces L0 files)

**Compaction Output** (see `db/compaction/compaction.cc:1132-1143`):
```cpp
Temperature Compaction::GetOutputTemperature(bool is_proximal_level) const {
  // Precedence order:
  // 1. output_temperature_override (manual compaction option)
  if (output_temperature_override_ != Temperature::kUnknown) {
    return output_temperature_override_;
  }

  // 2. last_level_temperature (if output is last level)
  if (is_last_level() && !is_proximal_level &&
      mutable_cf_options_.last_level_temperature != Temperature::kUnknown) {
    return mutable_cf_options_.last_level_temperature;
  }

  // 3. default_write_temperature (fallback)
  return mutable_cf_options_.default_write_temperature;
}
```

**⚠️ INVARIANT**: Temperature assignment follows strict precedence: override > last_level > default_write.

**Example Configuration**:
```cpp
options.default_write_temperature = Temperature::kWarm;
options.last_level_temperature = Temperature::kCold;
// Result: L0-L5 files are kWarm, L6 (last level) files are kCold
```

---

## 3. Precluded/Last-Level Compaction

### Precluded Last-Level Data

The `preclude_last_level_data_seconds` option (`include/rocksdb/advanced_options.h:989`) reserves the last level for data older than a threshold:

```cpp
// Keep data newer than 7 days out of the last level
options.preclude_last_level_data_seconds = 7 * 24 * 3600;
```

**How It Works**:
- RocksDB reserves the last level exclusively for data older than the threshold
- Data newer than the threshold cannot be compacted to the last level (stays in penultimate level)
- When data ages beyond the threshold, it migrates to the last level during compaction
- The last level gets `last_level_temperature` (typically `kCold`)

**⚠️ INVARIANT**: When `preclude_last_level_data_seconds > 0`, the last level contains **only** data older than the threshold (barring clock skew).

**Use Case**: Separate hot recent data (in penultimate level, fast storage) from cold historical data (in last level, slow/cheap storage).

### Per-Key Placement (Advanced)

RocksDB can split a single compaction's output into two levels based on per-key age:
- Keys newer than threshold → penultimate level (non-last-level temperature)
- Keys older than threshold → last level (`last_level_temperature`)

This is called **per-key placement** or **tiered compaction**.

**⚠️ INVARIANT**: Per-key placement requires `SupportsPerKeyPlacement()` to return true (controlled by `preclude_last_level_data_seconds` and other conditions).

---

## 4. File Temperature in FileMetaData

### FileMetaData Structure

Each SST file's temperature is stored in `FileMetaData` (`db/version_edit.h:244-280`):

```cpp
struct FileMetaData {
  FileDescriptor fd;
  InternalKey smallest;
  InternalKey largest;
  // ... other metadata ...

  Temperature temperature = Temperature::kUnknown;  // Line 280

  // ... more metadata ...
};
```

**Key Points**:
- Temperature is immutable after file creation (set once during flush/compaction)
- Stored in MANIFEST via `VersionEdit` (tag `kTemperature`, see `db/version_edit.h:106`)
- Survives DB restart (persisted in MANIFEST)

### Temperature Persistence

Temperature is encoded in MANIFEST as a custom field in `NewFile` records:

```cpp
enum NewFileCustomTag : uint32_t {
  kTemperature = 9,  // db/version_edit.h:106
  // ...
};
```

**Encoding**: Serialized as a uint8_t (1 byte) in the MANIFEST.

**⚠️ INVARIANT**: A file's temperature is immutable. Changing temperature requires rewriting the file (via `kChangeTemperature` compaction).

---

## 5. Temperature-Based I/O

### FileSystem Integration

RocksDB passes temperature hints to the FileSystem layer via `FileOperationInfo` (`include/rocksdb/listener.h:260-273`):

```cpp
struct FileOperationInfo {
  FileOperationType type;
  const std::string& path;
  Temperature temperature;  // Line 273: temperature hint
  uint64_t offset;
  size_t length;
  // ...
};
```

**File Operations with Temperature**:
- **File creation** (flush, compaction output): FileSystem knows temperature at creation time
- **File reads**: Temperature passed to `Read()`, `RandomAccessFile::Prefetch()`, etc.
- **File writes**: Temperature passed to `WritableFile::Append()`

### Temperature-Aware Policies

FileSystem implementations can use temperature to:

1. **Storage Placement**:
   - `kHot` → fast local SSD
   - `kWarm` → standard SSD
   - `kCold` → HDD or cold cloud storage (e.g., S3 Glacier)
   - `kIce` → archival tier (e.g., tape, S3 Deep Archive)

2. **Caching**:
   - `kHot` files: high cache priority, prefetch aggressively
   - `kCold` files: low cache priority, no prefetching
   - See `table/block_based/block_based_table_reader.cc` for cache priority logic

3. **Rate Limiting**:
   - `kCold` file reads: apply strict rate limits to avoid impacting hot path
   - `kHot` file reads: bypass rate limiter for low latency

4. **Prefetching**:
   - `kHot` files: large prefetch buffer, speculative prefetching
   - `kCold` files: minimal or no prefetching

**Example (Conceptual FileSystem)**:
```cpp
Status FileSystemImpl::NewRandomAccessFile(
    const std::string& fname,
    Temperature temperature,
    std::unique_ptr<FSRandomAccessFile>* result) {

  if (temperature == Temperature::kCold || temperature == Temperature::kIce) {
    // Route to S3 or HDD
    return OpenColdStorageFile(fname, result);
  } else {
    // Route to local SSD
    return OpenHotStorageFile(fname, result);
  }
}
```

---

## 6. Compaction Output Temperature

### Temperature Determination Logic

Compaction output temperature follows the precedence defined in `Compaction::GetOutputTemperature()` (see Section 2).

**Code Reference**: `db/compaction/compaction.cc:1132-1143`

**Decision Tree**:
```
┌─────────────────────────────────────┐
│ Compaction Output Temperature       │
└─────────────────────────────────────┘
              │
              ├─ output_temperature_override != kUnknown?
              │  └─ YES → return output_temperature_override
              │
              ├─ is_last_level() && last_level_temperature != kUnknown?
              │  └─ YES → return last_level_temperature
              │
              └─ return default_write_temperature
```

### Manual Compaction Override

Manual compaction via `CompactFiles()` can override temperature using `CompactionOptions::output_temperature_override`:

```cpp
CompactionOptions opts;
opts.output_temperature_override = Temperature::kCold;
std::vector<std::string> input_files = {/* file names */};
db->CompactFiles(opts, input_files, output_level);
// All output files forced to kCold temperature
```

**Use Case**: Explicitly migrate specific files to a different temperature tier (e.g., archive old data).

**⚠️ INVARIANT**: Override temperature takes precedence over all other rules (highest priority).

**Note**: `CompactRange()` does not support temperature override. Use `CompactFiles()` for explicit temperature control on manual compactions.

### Per-Key Placement in Compaction

When `SupportsPerKeyPlacement()` is true, compaction can produce output at **two** levels with different temperatures:

- **Last level output**: Gets `last_level_temperature` (e.g., `kCold`)
- **Penultimate level output**: Gets `default_write_temperature` (e.g., `kWarm`)

Keys are routed to the appropriate level based on age (via `preclude_last_level_data_seconds` threshold).

**Code Reference**: `db/compaction/compaction_job.cc` (see `CompactionJob::ProcessKeyValueCompaction()` for per-key routing logic).

---

## 7. Tiered Compaction Interactions

### Level-Based Compaction

Temperature assignment in level-based compaction:

- **L0 → L1 compaction**: Output gets `default_write_temperature`
- **L1 → L2, ..., Ln-1 → Ln compaction**: Output gets `default_write_temperature`
- **Ln-1 → Ln (last level) compaction**: Output gets `last_level_temperature`

**With `level_compaction_dynamic_level_bytes=true`**:
- The "last level" can shift as data grows (e.g., from L5 to L6)
- Files moving from "old last level" to "new last level" get `last_level_temperature`

### Universal Compaction

Temperature in universal compaction:
- All files stay in L0 until final compaction to L1 (last level)
- L0 files: `default_write_temperature`
- L1 files (last level): `last_level_temperature`

**⚠️ INVARIANT**: In universal compaction, L1 is always the last level (receives `last_level_temperature`).

### FIFO Compaction with Temperature

FIFO compaction supports age-based temperature migration via `file_temperature_age_thresholds` (`include/rocksdb/advanced_options.h:114`):

```cpp
CompactionOptionsFIFO fifo_opts;
fifo_opts.file_temperature_age_thresholds = {
  {Temperature::kWarm, 3600},      // 1 hour old → kWarm
  {Temperature::kCold, 86400},     // 1 day old → kCold
  {Temperature::kIce, 7 * 86400}   // 7 days old → kIce
};
options.compaction_options_fifo = fifo_opts;
```

**How It Works**:
1. RocksDB checks file age (based on `file_creation_time` or `oldest_ancester_time`)
2. If file is older than a threshold, trigger `kChangeTemperature` compaction
3. Compaction rewrites the file with the new temperature (can be trivial copy if `allow_trivial_copy_when_change_temperature=true`)

**⚠️ INVARIANT**: Temperature migration happens one file at a time (oldest first) to avoid I/O spikes.

**Code Reference**: `db/compaction/compaction_picker.cc` (search for `kChangeTemperature` compaction reason).

---

## 8. Secondary Cache Integration

### Temperature-Aware Caching

RocksDB's `SecondaryCache` (e.g., compressed cache, persistent cache) can use temperature to decide promotion/eviction:

- **kHot blocks**: Promote aggressively from secondary cache to primary cache
- **kCold blocks**: Keep in secondary cache only, avoid primary cache pollution

**Example Policy**:
```cpp
if (block_temperature == Temperature::kCold) {
  // Insert into secondary cache only (e.g., compressed cache)
  secondary_cache->Insert(key, block, CompressionType::kLZ4);
} else {
  // Insert into primary cache (uncompressed, fast access)
  primary_cache->Insert(key, block);
}
```

**⚠️ INVARIANT**: Temperature hints do not guarantee cache behavior; the cache implementation decides actual policy.

### Cache Priority

Block-based table reader assigns cache priority based on temperature (conceptual):

```cpp
Cache::Priority GetCachePriority(Temperature temp) {
  switch (temp) {
    case Temperature::kHot:
      return Cache::Priority::HIGH;
    case Temperature::kWarm:
      return Cache::Priority::LOW;
    case Temperature::kCold:
    case Temperature::kIce:
      return Cache::Priority::BOTTOM;  // Evict first
    default:
      return Cache::Priority::LOW;
  }
}
```

**Code Reference**: `table/block_based/block_based_table_reader.cc` (cache insertion logic).

---

## 9. BlobDB Temperature

**⚠️ EXPERIMENTAL / LIMITED SUPPORT**: Temperature support for BlobDB is limited. Blob files do not currently expose temperature metadata in the same way as SST files. The FileSystem layer receives temperature hints during blob file creation (inherited from the SST context), but explicit per-blob-file temperature tracking is not implemented.

**Current Behavior**:
- Blob file creation receives temperature hints from the creating SST context
- FileSystem implementations can use these hints for placement decisions
- No explicit blob file temperature metadata is persisted in MANIFEST or blob file headers

**Future Work**: Full temperature support for blob files (explicit metadata, temperature changes, monitoring) may be added in future RocksDB versions.

---

## 10. Monitoring

### Temperature-Related Statistics

RocksDB exposes temperature information via:

1. **`DB::GetLiveFilesMetaData()`**:
   ```cpp
   std::vector<LiveFileMetaData> metadata;
   db->GetLiveFilesMetaData(&metadata);
   for (const auto& file : metadata) {
     std::cout << "File " << file.name
               << " temperature: " << static_cast<int>(file.temperature)
               << std::endl;
   }
   ```

2. **`DB::GetLiveFilesStorageInfo()`**:
   ```cpp
   std::vector<LiveFileStorageInfo> storage_info;
   db->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(), &storage_info);
   // Contains per-file temperature and storage location
   ```

3. **`DB::GetColumnFamilyMetaData()`**:
   ```cpp
   ColumnFamilyMetaData cf_meta;
   db->GetColumnFamilyMetaData(&cf_meta);
   for (const auto& level : cf_meta.levels) {
     for (const auto& file : level.files) {
       // file.temperature available (if exposed by API)
     }
   }
   ```

### Compaction Reason Statistics

Temperature-related compaction reasons (`include/rocksdb/listener.h:151`):

- **`kChangeTemperature`**: Compaction triggered to change file temperature (e.g., age-based migration)
- Tracked in `DB::GetProperty("rocksdb.stats")` under compaction reasons

**Example Output**:
```
Compaction Reason:
  kChangeTemperature: 42 compactions
```

### EventListener Hooks

Temperature information can be accessed through `FileOperationInfo` callbacks, which include temperature hints for file operations:

```cpp
class MyListener : public EventListener {
  void OnFileOperation(const FileOperationInfo& info) override {
    if (info.type == FileOperationType::kCreate) {
      std::cout << "Created file " << info.path
                << " with temperature: "
                << static_cast<int>(info.temperature) << std::endl;
    }
  }
};
```

**Note**: `FlushJobInfo` and `CompactionJobInfo` do not currently expose per-file temperature fields. To monitor output file temperatures, use `FileOperationInfo` callbacks or query file metadata via `GetLiveFilesMetaData()` after flush/compaction completes.

**Code Reference**: `include/rocksdb/listener.h:260-290` (FileOperationInfo structure).

---

## Key Invariants Summary

1. **Temperature immutability**: A file's temperature is set at creation and never changes (changing requires rewriting the file).

2. **Temperature precedence**: `output_temperature_override > last_level_temperature > default_write_temperature`.

3. **Precluded last-level guarantee**: When `preclude_last_level_data_seconds > 0`, the last level contains only data older than the threshold.

4. **Universal compaction last level**: In universal compaction, L1 is always the last level (gets `last_level_temperature`).

5. **FIFO temperature migration**: Temperature changes in FIFO compaction happen one file at a time (oldest first).

6. **Temperature as hint**: Temperature is advisory; the FileSystem decides actual placement and policies.

---

## Configuration Examples

### Example 1: Two-Tier Storage (Hot L0-L5, Cold L6)

```cpp
Options options;
options.default_write_temperature = Temperature::kWarm;
options.last_level_temperature = Temperature::kCold;
options.num_levels = 7;

DB* db;
DB::Open(options, "/path/to/db", &db);
// L0-L5 files: kWarm (fast SSD)
// L6 files: kCold (HDD or cloud storage)
```

### Example 2: Age-Based Tiering with Precluded Last Level

```cpp
Options options;
options.default_write_temperature = Temperature::kWarm;
options.last_level_temperature = Temperature::kCold;
options.preclude_last_level_data_seconds = 7 * 24 * 3600;  // 7 days
options.num_levels = 7;

DB* db;
DB::Open(options, "/path/to/db", &db);
// Data < 7 days old: stays in L0-L5 (kWarm, fast SSD)
// Data >= 7 days old: migrates to L6 (kCold, cheap storage)
```

### Example 3: FIFO with Multi-Tier Aging

```cpp
Options options;
options.compaction_style = kCompactionStyleFIFO;

CompactionOptionsFIFO fifo_opts;
fifo_opts.max_table_files_size = 100ULL << 30;  // 100GB
fifo_opts.file_temperature_age_thresholds = {
  {Temperature::kWarm, 3600},          // 1 hour → kWarm
  {Temperature::kCold, 86400},         // 1 day → kCold
  {Temperature::kIce, 7 * 86400}       // 7 days → kIce
};
fifo_opts.allow_trivial_copy_when_change_temperature = true;

options.compaction_options_fifo = fifo_opts;

DB* db;
DB::Open(options, "/path/to/db", &db);
// Fresh data: kUnknown (default)
// 1 hour old: kWarm (moved to warm storage)
// 1 day old: kCold (moved to cold storage)
// 7 days old: kIce (moved to archival storage)
```

### Example 4: Manual Temperature Migration

```cpp
// Force compact specific files to kCold temperature using CompactFiles()
CompactionOptions opts;
opts.output_temperature_override = Temperature::kCold;

// Get list of files to migrate
std::vector<LiveFileMetaData> metadata;
db->GetLiveFilesMetaData(&metadata);
std::vector<std::string> files_to_compact;
for (const auto& file : metadata) {
  if (file.temperature != Temperature::kCold) {
    files_to_compact.push_back(file.name);
  }
}

// Compact files to cold storage
db->CompactFiles(opts, files_to_compact, file.level);
// Specified files now have kCold temperature (migrated to cold storage)
```

---

## Implementation Files

**Key Source Files**:
- `include/rocksdb/types.h:118` — `Temperature` enum definition
- `include/rocksdb/advanced_options.h:956-989` — Temperature options (`last_level_temperature`, `default_write_temperature`, `preclude_last_level_data_seconds`)
- `include/rocksdb/advanced_options.h:114` — FIFO temperature options (`file_temperature_age_thresholds`)
- `include/rocksdb/options.h:2481` — `CompactionOptions::output_temperature_override` for manual compaction
- `db/version_edit.h:280` — `FileMetaData::temperature` field
- `db/version_edit.h:107` — `kTemperature` MANIFEST encoding tag
- `db/compaction/compaction.h` — `Compaction::GetOutputTemperature()` declaration
- `db/compaction/compaction.cc:1132-1143` — `GetOutputTemperature()` implementation
- `db/compaction/compaction_picker.cc` — Temperature-aware compaction picking (search for `kChangeTemperature`)
- `db/compaction/compaction_picker_fifo.cc` — FIFO age-based temperature migration
- `db/compaction/compaction_job.cc` — Compaction output temperature assignment
- `include/rocksdb/listener.h:273` — `FileOperationInfo::temperature` field
- `include/rocksdb/listener.h:151` — `CompactionReason::kChangeTemperature` enum value
- `include/rocksdb/file_system.h` — FileSystem interface (temperature hints in file operations)

**Related Options**:
- `ColumnFamilyOptions::last_level_temperature`
- `ColumnFamilyOptions::default_write_temperature`
- `ColumnFamilyOptions::preclude_last_level_data_seconds`
- `CompactionOptionsFIFO::file_temperature_age_thresholds`
- `CompactionOptionsFIFO::allow_trivial_copy_when_change_temperature`
- `CompactionOptions::output_temperature_override`

---

## Diagrams

### Temperature Flow Through Compaction

```
┌─────────────────────────────────────────────────────────────┐
│                    Write Path                               │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │   MemTable    │
                    └───────────────┘
                            │ Flush
                            ▼
                    ┌───────────────┐
                    │ L0 SST Files  │ ← default_write_temperature (e.g., kWarm)
                    └───────────────┘
                            │ Compaction
                            ▼
                    ┌───────────────┐
                    │ L1-L5 Files   │ ← default_write_temperature (e.g., kWarm)
                    └───────────────┘
                            │ Compaction (to last level)
                            ▼
                    ┌───────────────┐
                    │  L6 Files     │ ← last_level_temperature (e.g., kCold)
                    │  (Last Level) │
                    └───────────────┘
```

### Temperature Precedence in Compaction Output

```
┌──────────────────────────────────────────────────────────────┐
│        Compaction Output Temperature Decision Tree           │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────┐
        │ output_temperature_override set?      │
        └───────────────────────────────────────┘
                  │                    │
                YES│                   │NO
                  │                    │
                  ▼                    ▼
        ┌──────────────────┐  ┌─────────────────────────┐
        │ Return override  │  │ Is last level output?   │
        │  temperature     │  └─────────────────────────┘
        └──────────────────┘            │              │
                                      YES│             │NO
                                        │              │
                                        ▼              ▼
                        ┌──────────────────────┐  ┌───────────────────┐
                        │ last_level_temp set? │  │ Return default_   │
                        └──────────────────────┘  │ write_temperature │
                                  │              └───────────────────┘
                                YES│    │NO
                                  │    │
                                  ▼    ▼
                        ┌──────────────────────┐
                        │ Return last_level_   │
                        │ temperature          │
                        └──────────────────────┘
```

### FIFO Age-Based Temperature Migration

```
Time ────────────────────────────────────────────────────────────►

File Age:    0 hr        1 hr         1 day         7 days
             │           │            │             │
Temperature: kUnknown    kWarm        kCold         kIce
             │           │            │             │
Storage:     [SSD]  ───► [Warm SSD] ─► [HDD/Cloud] ─► [Archival]
             │           │            │             │
             └───────────┴────────────┴─────────────┘
                      Triggered by kChangeTemperature compaction
                      (based on file_temperature_age_thresholds)
```

---

## Testing Considerations

### Testing Temperature Assignment

1. **Verify flush output temperature**:
   ```cpp
   ASSERT_EQ(file_meta->temperature, options.default_write_temperature);
   ```

2. **Verify last-level compaction temperature**:
   ```cpp
   // After compaction to last level
   ASSERT_EQ(file_meta->temperature, options.last_level_temperature);
   ```

3. **Verify manual override**:
   ```cpp
   CompactRangeOptions opts;
   opts.output_temperature_override = Temperature::kIce;
   db->CompactRange(opts, nullptr, nullptr);
   ASSERT_EQ(GetAllFileTemperatures(), std::vector{Temperature::kIce});
   ```

### Testing FIFO Temperature Migration

```cpp
// Set up age thresholds
fifo_opts.file_temperature_age_thresholds = {
  {Temperature::kWarm, 100},
  {Temperature::kCold, 200}
};

// Advance mock time
env->MockSleepForSeconds(150);

// Trigger compaction
db->CompactRange({}, nullptr, nullptr);

// Verify temperature changed
ASSERT_EQ(GetOldestFileTemperature(), Temperature::kWarm);
```

### Testing Precluded Last Level

```cpp
options.preclude_last_level_data_seconds = 1000;
options.last_level_temperature = Temperature::kCold;

// Write recent data
Put("key", "value");
Flush();
db->CompactRange({}, nullptr, nullptr);

// Verify data NOT in last level
ASSERT_EQ(NumFilesAtLevel(num_levels - 1), 0);

// Age data and compact
env->MockSleepForSeconds(1001);
db->CompactRange({}, nullptr, nullptr);

// Verify data NOW in last level with kCold temperature
ASSERT_GT(NumFilesAtLevel(num_levels - 1), 0);
ASSERT_EQ(GetLastLevelFileTemperature(), Temperature::kCold);
```

---

## Performance Considerations

1. **FileSystem overhead**: Temperature hints add metadata to every file operation. Ensure FileSystem implementation handles temperature efficiently (avoid expensive lookups per I/O).

2. **Compaction I/O**: Changing temperature via `kChangeTemperature` compaction may require file copy/rewrite. Use `allow_trivial_copy_when_change_temperature=true` for trivial moves when possible.

3. **Cache pollution**: Cold data can evict hot data from cache. Use temperature-aware cache priority to prevent this.

4. **Rate limiting**: Apply rate limits to cold-tier reads to avoid impacting hot-path latency.

---

## Common Pitfalls

1. **Temperature doesn't auto-migrate**: Setting temperature options doesn't immediately migrate existing data. Trigger compaction to apply new temperatures.

2. **kUnknown != kHot**: `kUnknown` means "use default policy", not "hot data". Explicitly set `default_write_temperature` if needed.

3. **Dynamic leveling confusion**: With `level_compaction_dynamic_level_bytes=true`, the "last level" can shift. Monitor actual level assignments.

4. **FIFO age threshold sort order**: Thresholds must be in ascending age order. Violating this causes undefined behavior.

---

## Further Reading

- [Blog Post: Time-Aware Tiered Storage](https://rocksdb.org/blog/2022/11/09/time-aware-tiered-storage.html)
- [FileSystem API](../include/rocksdb/file_system.h) — Temperature hints in file operations
- [Compaction Overview](./compaction.md) — How compaction uses temperature
- [Advanced Options Reference](../include/rocksdb/advanced_options.h) — Temperature configuration options
