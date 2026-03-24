# Plan: Blob Compression Configuration Improvements

## Summary

Add two new CF options:
1. `blob_compression_opts` (`CompressionOptions`) — allows tuning compression
   parameters (especially zstd level) for blob files, mirroring SST's
   `compression_opts`.
2. `blob_compression_manager` (`std::shared_ptr<CompressionManager>`) — allows
   custom compression strategies for blob files, mirroring SST's
   `compression_manager`.

Both options are **mutable** (changeable via `SetOptions()`), defaulting to
current behavior (`CompressionOptions{}` and `nullptr`).

---

## Approach

Follow the exact same patterns used by SST compression:
- `compression_opts` → `blob_compression_opts`
- `compression_manager` → `blob_compression_manager`

### Write path (BlobFileBuilder)

`BlobFileBuilder` already receives `MutableCFOptions*`. Replace the hardcoded
`CompressionOptions{}` and `GetBuiltinV2CompressionManager()` with the new
options:

```cpp
// Current (blob_file_builder.cc:70-76):
blob_compressor_(GetBuiltinV2CompressionManager()->GetCompressor(
    CompressionOptions{}, blob_compression_type_)),

// New:
auto* mgr = mutable_cf_options->blob_compression_manager.get();
if (mgr == nullptr) {
  mgr = GetBuiltinV2CompressionManager().get();
}
blob_compressor_(mgr->GetCompressor(
    mutable_cf_options->blob_compression_opts, blob_compression_type_)),
```

### Read path (BlobFileReader)

`BlobFileReader::Create` receives `ImmutableOptions&`. The decompressor does not
depend on `CompressionOptions` (level is only for compression). We need the
`CompressionManager` to create the decompressor.

Add an optional `CompressionManager*` parameter to `BlobFileReader::Create`.
Thread it through `BlobFileCache::GetBlobFileReader` and
`BlobSource::GetBlobFileReader`. `BlobSource` stores the manager from
`MutableCFOptions` at construction time.

```cpp
// BlobFileReader::Create - new signature:
static Status Create(const ImmutableOptions& immutable_options,
                     const ReadOptions& read_options,
                     const FileOptions& file_options,
                     uint32_t column_family_id,
                     HistogramImpl* blob_file_read_hist,
                     uint64_t blob_file_number,
                     const std::shared_ptr<IOTracer>& io_tracer,
                     std::unique_ptr<BlobFileReader>* reader,
                     CompressionManager* blob_compression_manager = nullptr);

// In Create():
if (compression_type != kNoCompression) {
  auto* mgr = blob_compression_manager;
  if (mgr == nullptr) {
    mgr = GetBuiltinV2CompressionManager().get();
  }
  decompressor = mgr->GetDecompressorOptimizeFor(compression_type);
}
```

### Validation

Update `CheckCompressionSupported` in `db/column_family.cc` to use
`CheckCompressionSupportedWithManager()` for blob compression when a custom
manager is configured:

```cpp
// Current:
if (!CompressionTypeSupported(cf_options.blob_compression_type)) { ... }

// New:
Status s = CheckCompressionSupportedWithManager(
    cf_options.blob_compression_type,
    cf_options.blob_compression_manager.get());
if (!s.ok()) { return s; }
```

---

## Detailed Changes

### 1. `include/rocksdb/advanced_options.h` — Public API

Add after `blob_compression_type` (line 1056):

```cpp
  // The compression options to use for large values stored in blob files.
  // Allows tuning compression parameters such as compression level.
  // Note that enable_blob_files has to be set in order for this option to have
  // any effect.
  //
  // Default: CompressionOptions()
  //
  // Dynamically changeable through the SetOptions() API
  CompressionOptions blob_compression_opts;

  // EXPERIMENTAL
  // Customized compression through a callback interface for blob files.
  // When non-nullptr, supersedes the built-in compression manager for blob
  // compression/decompression. The blob_compression_type and
  // blob_compression_opts are still passed as hints/suggestions.
  // See advanced_compression.h
  //
  // Default: nullptr (uses built-in compression manager)
  //
  // Dynamically changeable through the SetOptions() API
  std::shared_ptr<CompressionManager> blob_compression_manager;
```

### 2. `options/cf_options.h` — MutableCFOptions

Add fields after `blob_compression_type` (line 328):

```cpp
CompressionType blob_compression_type;
CompressionOptions blob_compression_opts;
std::shared_ptr<CompressionManager> blob_compression_manager;
```

Add to both constructors (from `ColumnFamilyOptions` and default).

### 3. `options/cf_options.cc` — Registration, Dump, Type Info

**a) Register `blob_compression_opts` in `cf_mutable_options_type_info`:**

Use `OptionTypeInfo::Struct()` with the existing `compression_options_type_info`
map, similar to how `compression_opts` is registered. Use the name
`blob_compression_opts`. No legacy colon-separated format support needed (new
option, no backwards compatibility concern).

```cpp
static const std::string kOptNameBlobCompOpts = "blob_compression_opts";

{kOptNameBlobCompOpts,
 OptionTypeInfo::Struct(
     kOptNameBlobCompOpts, &compression_options_type_info,
     offsetof(struct MutableCFOptions, blob_compression_opts),
     OptionVerificationType::kNormal,
     (OptionTypeFlags::kMutable | OptionTypeFlags::kCompareNever))},
```

**b) Register `blob_compression_manager`:**

```cpp
{"blob_compression_manager",
 OptionTypeInfo::AsCustomSharedPtr<CompressionManager>(
     offsetof(struct MutableCFOptions, blob_compression_manager),
     OptionVerificationType::kByNameAllowNull,
     (OptionTypeFlags::kMutable | OptionTypeFlags::kAllowNull))},
```

**c) Add to `MutableCFOptions::Dump()`:**

```cpp
ROCKS_LOG_INFO(log, "                blob_compression_opts: w_bits: %d; level: %d; "
               "strategy: %d; max_dict_bytes: %u; zstd_max_train_bytes: %u; "
               "max_compressed_bytes_per_kb: %d; checksum: %d",
               blob_compression_opts.window_bits,
               blob_compression_opts.level,
               blob_compression_opts.strategy,
               blob_compression_opts.max_dict_bytes,
               blob_compression_opts.zstd_max_train_bytes,
               blob_compression_opts.max_compressed_bytes_per_kb,
               blob_compression_opts.checksum);
```

### 4. `options/options_helper.cc` — UpdateColumnFamilyOptions

Add after `blob_compression_type` copy (line 281):

```cpp
cf_opts->blob_compression_type = moptions.blob_compression_type;
cf_opts->blob_compression_opts = moptions.blob_compression_opts;
cf_opts->blob_compression_manager = moptions.blob_compression_manager;
```

### 5. `db/blob/blob_file_builder.cc` — Write path

Replace lines 69-76 in the constructor:

```cpp
blob_compression_type_(mutable_cf_options->blob_compression_type),
blob_compressor_([&]() -> std::unique_ptr<Compressor> {
  auto* mgr = mutable_cf_options->blob_compression_manager.get();
  if (mgr == nullptr) {
    mgr = GetBuiltinV2CompressionManager().get();
  }
  return mgr->GetCompressor(
      mutable_cf_options->blob_compression_opts, blob_compression_type_);
}()),
```

Remove the TODO comments (lines 70-73) since they are being addressed.

### 6. `db/blob/blob_file_reader.h` and `db/blob/blob_file_reader.cc` — Read path

**blob_file_reader.h**: Add optional `CompressionManager*` parameter to
`Create`:

```cpp
static Status Create(const ImmutableOptions& immutable_options,
                     const ReadOptions& read_options,
                     const FileOptions& file_options,
                     uint32_t column_family_id,
                     HistogramImpl* blob_file_read_hist,
                     uint64_t blob_file_number,
                     const std::shared_ptr<IOTracer>& io_tracer,
                     std::unique_ptr<BlobFileReader>* reader,
                     CompressionManager* blob_compression_manager = nullptr);
```

**blob_file_reader.cc**: Use the manager for decompressor creation:

```cpp
std::shared_ptr<Decompressor> decompressor;
if (compression_type != kNoCompression) {
  auto* mgr = blob_compression_manager;
  if (mgr == nullptr) {
    mgr = GetBuiltinV2CompressionManager().get();
  }
  decompressor = mgr->GetDecompressorOptimizeFor(compression_type);
}
```

### 7. `db/blob/blob_file_cache.h` and `db/blob/blob_file_cache.cc` — Thread through

Add `CompressionManager*` parameter to `GetBlobFileReader`:

```cpp
Status GetBlobFileReader(const ReadOptions& read_options,
                         uint64_t blob_file_number,
                         CacheHandleGuard<BlobFileReader>* blob_file_reader,
                         CompressionManager* blob_compression_manager = nullptr);
```

Pass through to `BlobFileReader::Create`.

### 8. `db/blob/blob_source.h` and `db/blob/blob_source.cc` — Store manager

Add `CompressionManager*` member extracted from `MutableCFOptions`:

```cpp
// blob_source.h private members:
CompressionManager* blob_compression_manager_;

// blob_source.cc constructor:
blob_compression_manager_(mutable_cf_options.blob_compression_manager.get()),
```

Note: We store the raw pointer since `BlobSource` doesn't own the manager.
The `MutableCFOptions` holds the `shared_ptr` and outlives `BlobSource`.

Update `GetBlobFileReader` inline to pass the manager:

```cpp
inline Status GetBlobFileReader(
    const ReadOptions& read_options, uint64_t blob_file_number,
    CacheHandleGuard<BlobFileReader>* blob_file_reader) {
  return blob_file_cache_->GetBlobFileReader(
      read_options, blob_file_number, blob_file_reader,
      blob_compression_manager_);
}
```

### 9. `db/column_family.cc` — Validation

Replace the simple `CompressionTypeSupported()` check (lines 176-183) with
`CheckCompressionSupportedWithManager()`:

```cpp
{
  Status s = CheckCompressionSupportedWithManager(
      cf_options.blob_compression_type,
      cf_options.blob_compression_manager.get());
  if (!s.ok()) {
    return s;
  }
}
```

### 10. `options/options_settable_test.cc`

Add `blob_compression_opts` and `blob_compression_manager` to the
`ColumnFamilyOptionsAllFieldsSettable` test string (after line 665):

```
"blob_compression_type=kBZip2Compression;"
"blob_compression_opts={level=3};"
```

Add `blob_compression_manager` to the excluded list (it's a `shared_ptr` that
can't be byte-compared), similar to how `compression_manager` is excluded.

### 11. `options/options_test.cc`

Add a test for `blob_compression_opts` round-trip:

```cpp
// Test blob_compression_opts parsing
ASSERT_OK(GetColumnFamilyOptionsFromString(
    config_options, base_cf_opt,
    "blob_compression_opts={level=5;checksum=true};",
    &new_cf_opt));
ASSERT_EQ(new_cf_opt.blob_compression_opts.level, 5);
ASSERT_EQ(new_cf_opt.blob_compression_opts.checksum, true);
```

### 12. `db/blob/db_blob_basic_test.cc` — Integration test

Add an integration test that exercises blob compression with a custom level:

```cpp
TEST_F(DBBlobBasicTest, BlobCompressionWithLevel) {
  if (!ZSTD_Supported()) { return; }
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_compression_type = kZSTD;
  options.blob_compression_opts.level = 1;  // fast compression
  Reopen(options);

  // Write and read back
  ASSERT_OK(Put("key1", std::string(1000, 'x')));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("key1"), std::string(1000, 'x'));

  // Change level via SetOptions
  ASSERT_OK(db_->SetOptions({{"blob_compression_opts", "{level=19}"}}));
  ASSERT_OK(Put("key2", std::string(1000, 'y')));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("key2"), std::string(1000, 'y'));
}
```

### 13. Stress test updates

**`db_stress_tool/db_stress_test_base.cc`**: Add `blob_compression_opts` to the
dynamic options table with varying levels:

```cpp
options_tbl.emplace("blob_compression_opts",
                    std::vector<std::string>{
                        "{level=1}", "{level=3}", "{level=6}"});
```

---

## Files Modified

| File | Type of change |
|------|---------------|
| `include/rocksdb/advanced_options.h` | Add `blob_compression_opts` and `blob_compression_manager` fields |
| `options/cf_options.h` | Add to `MutableCFOptions` |
| `options/cf_options.cc` | Register in type info map, add to Dump() |
| `options/options_helper.cc` | Add to `UpdateColumnFamilyOptions` |
| `db/blob/blob_file_builder.cc` | Use new options for compressor creation |
| `db/blob/blob_file_reader.h` | Add `CompressionManager*` parameter |
| `db/blob/blob_file_reader.cc` | Use manager for decompressor creation |
| `db/blob/blob_file_cache.h` | Add `CompressionManager*` parameter |
| `db/blob/blob_file_cache.cc` | Pass through to `BlobFileReader::Create` |
| `db/blob/blob_source.h` | Store `CompressionManager*`, pass to cache |
| `db/blob/blob_source.cc` | Extract manager from `MutableCFOptions` |
| `db/column_family.cc` | Update blob compression validation |
| `options/options_settable_test.cc` | Add new options to settable test |
| `options/options_test.cc` | Add round-trip parsing test |
| `db/blob/db_blob_basic_test.cc` | Add integration test |
| `db_stress_tool/db_stress_test_base.cc` | Add to dynamic options table |

No new files. No build system changes (no new .cc files).

---

## Considerations

1. **Backwards compatibility**: Default values match current behavior.
   `blob_compression_opts` defaults to `CompressionOptions{}` and
   `blob_compression_manager` defaults to `nullptr`. Existing OPTIONS files
   without these fields will work unchanged.

2. **No blob file format change**: Compression options affect algorithm
   parameters, not data format. The blob file header still stores compression
   type as a single byte. Existing blob files remain readable.

3. **Dynamic changes via SetOptions()**: Both options are mutable. Changing
   `blob_compression_opts` or `blob_compression_manager` via `SetOptions()`
   affects newly created blob files. Existing blob files and cached readers
   retain their original compression settings.

4. **Dictionary compression**: `CompressionOptions` includes dictionary-related
   fields (`max_dict_bytes`, `zstd_max_train_bytes`). These are passed through
   to the compressor but blob files don't currently use dictionary compression.
   The fields are harmless — the compressor handles them, and without dictionary
   training data, they have no effect.

5. **max_compressed_bytes_per_kb**: This field controls the compression ratio
   threshold. The blob path uses `LegacyForceBuiltinCompression()` which always
   compresses regardless. This is a pre-existing design issue (documented as a
   "WART" in the code). The field is passed through to the compressor but the
   forced compression behavior is unchanged.

6. **Read path manager lifetime**: `BlobSource` stores a raw
   `CompressionManager*`. The `shared_ptr` is held by `MutableCFOptions` which
   is managed by `ColumnFamilyData`. The `BlobSource` is also owned by
   `ColumnFamilyData`, so the manager outlives the source.
