# Research: Blob Compression Configuration Improvements

## Goal

Add two features to blob compression:
1. **`blob_compression_opts`** — A new `CompressionOptions` field (like SST's `compression_opts`) to allow tuning compression parameters (especially zstd compression level) for blob files.
2. **`blob_compression_manager`** — A new `std::shared_ptr<CompressionManager>` field to allow custom compression strategies for blob files.

---

## How SST Compression Configuration Works Today (Reference Pattern)

### Option Fields in `ColumnFamilyOptions`

**File:** `include/rocksdb/options.h:218-244`

```cpp
CompressionType compression;                              // line 218
CompressionType bottommost_compression;                   // line 227
CompressionOptions bottommost_compression_opts;           // line 233
CompressionOptions compression_opts;                      // line 236
std::shared_ptr<CompressionManager> compression_manager;  // line 244
```

### `CompressionOptions` Struct

**File:** `include/rocksdb/compression_type.h:169-317`

Key fields:
- `level` (int, default `kDefaultCompressionLevel = 32767`) — zstd/zlib/LZ4HC compression level
- `window_bits` (int, default -14) — zlib-specific
- `strategy` (int, default 0) — zlib-specific
- `max_dict_bytes` (uint32_t) — dictionary size limit
- `zstd_max_train_bytes` (uint32_t) — training data limit
- `max_compressed_bytes_per_kb` (int) — min compression ratio threshold
- `checksum` (bool) — zstd frame checksums
- `enabled` (bool) — whether explicitly set by user

### Storage in `MutableCFOptions`

**File:** `options/cf_options.h:340-344`

```cpp
CompressionOptions compression_opts;
CompressionOptions bottommost_compression_opts;
std::shared_ptr<CompressionManager> compression_manager;
```

All three are mutable options that can be changed via `SetOptions()`.

### Registration in OptionTypeInfo

**File:** `options/cf_options.cc:225-731`

`compression_options_type_info` is a static map that defines per-field serialization for `CompressionOptions`:

```cpp
static std::unordered_map<std::string, OptionTypeInfo>
    compression_options_type_info = {
        {"window_bits", ...},
        {"level", ...},
        {"strategy", ...},
        {"max_compressed_bytes_per_kb", ...},
        {"max_dict_bytes", ...},
        // ... etc.
};
```

Both `compression_opts` and `bottommost_compression_opts` are registered using `OptionTypeInfo::Struct()` with a custom parse fallback for the legacy colon-separated format (lines 687-726):

```cpp
{kOptNameCompOpts,
 OptionTypeInfo::Struct(
     kOptNameCompOpts, &compression_options_type_info,
     offsetof(struct MutableCFOptions, compression_opts),
     OptionVerificationType::kNormal,
     (OptionTypeFlags::kMutable | OptionTypeFlags::kCompareNever),
     [](const ConfigOptions& opts, const std::string& name,
        const std::string& value, void* addr) {
       if (name == kOptNameCompOpts && value.find('=') == std::string::npos) {
         auto* compression = static_cast<CompressionOptions*>(addr);
         return ParseCompressionOptions(value, name, *compression);
       } else {
         return OptionTypeInfo::ParseStruct(...);
       }
     })},
```

`compression_manager` is registered as a `Customizable` shared pointer (lines 727-731):

```cpp
{"compression_manager",
 OptionTypeInfo::AsCustomSharedPtr<CompressionManager>(
     offsetof(struct MutableCFOptions, compression_manager),
     OptionVerificationType::kByNameAllowNull,
     (OptionTypeFlags::kMutable | OptionTypeFlags::kAllowNull))},
```

### How SST Compression Uses `CompressionManager`

**File:** `table/block_based/block_based_table_builder.cc:1097-1116`

```cpp
auto* mgr = tbo.moptions.compression_manager.get();
if (mgr == nullptr) {
  mgr = GetBuiltinV2CompressionManager().get();  // fallback to built-in
}
basic_compressor = mgr->GetCompressorForSST(
    filter_context, tbo.compression_opts, tbo.compression_type);
```

Key: When `compression_manager` is null, the built-in V2 manager is used. Both `CompressionOptions` (including level) and `CompressionType` are passed as parameters.

### How Compression Level is Applied (ZSTD example)

**File:** `util/compression.cc:1093-1123`

In `BuiltinZSTDCompressorV2::ObtainWorkingArea()`:
```cpp
auto level = opts_.level;
if (level == CompressionOptions::kDefaultCompressionLevel) {
  level = ZSTD_CLEVEL_DEFAULT;
}
ZSTD_CCtx_setParameter(ctx, ZSTD_c_compressionLevel, level);
```

### Compression Validation

**File:** `db/column_family.cc:113-183`

SST compression is validated with `CheckCompressionSupportedWithManager()` which checks the `CompressionManager` if present, or falls back to `CompressionTypeSupported()`. Blob compression currently only uses `CompressionTypeSupported()` directly (line 176-183).

### UpdateColumnFamilyOptions

**File:** `options/options_helper.cc:298-302`

When mutable options are applied back to `ColumnFamilyOptions`:
```cpp
cf_opts->compression_opts = moptions.compression_opts;
cf_opts->bottommost_compression_opts = moptions.bottommost_compression_opts;
cf_opts->compression_manager = moptions.compression_manager;
```

---

## How Blob Compression Works Today

### Option Definition

**File:** `include/rocksdb/advanced_options.h:1049-1056`

Only a single option exists:
```cpp
CompressionType blob_compression_type = kNoCompression;
```

There is **no** `blob_compression_opts` and **no** `blob_compression_manager`.

### Storage in `MutableCFOptions`

**File:** `options/cf_options.h:328`

```cpp
CompressionType blob_compression_type;
```

### Registration

**File:** `options/cf_options.cc:616-619`

```cpp
{"blob_compression_type",
 {offsetof(struct MutableCFOptions, blob_compression_type),
  OptionType::kCompressionType, OptionVerificationType::kNormal,
  OptionTypeFlags::kMutable}},
```

### Write Path: BlobFileBuilder

**File:** `db/blob/blob_file_builder.cc:69-79`

```cpp
blob_compression_type_(mutable_cf_options->blob_compression_type),
// TODO: support most CompressionOptions with a new CF option
// blob_compression_opts
// TODO with schema change: support custom compression manager and options
// such as max_compressed_bytes_per_kb
// NOTE: returns nullptr for kNoCompression
blob_compressor_(GetBuiltinV2CompressionManager()->GetCompressor(
    CompressionOptions{}, blob_compression_type_)),
```

Key problems (noted by TODO comments):
1. Uses default `CompressionOptions{}` — no way to set compression level
2. Hardcodes `GetBuiltinV2CompressionManager()` — no custom compression manager support
3. No support for `max_compressed_bytes_per_kb` threshold

### BlobFileBuilder Members

**File:** `db/blob/blob_file_builder.h:96-98`

```cpp
CompressionType blob_compression_type_;
std::unique_ptr<Compressor> blob_compressor_;
mutable Compressor::ManagedWorkingArea blob_compressor_wa_;
```

### Compression Execution

**File:** `db/blob/blob_file_builder.cc:266-293`

```cpp
Status BlobFileBuilder::CompressBlobIfNeeded(
    Slice* blob, GrowableBuffer* compressed_blob) const {
  if (!blob_compressor_) {
    return Status::OK();
  }
  s = LegacyForceBuiltinCompression(*blob_compressor_, &blob_compressor_wa_,
                                    *blob, compressed_blob);
  ...
}
```

Uses `LegacyForceBuiltinCompression()` which always compresses (never skips even if output is larger).

### Read Path: BlobFileReader

**File:** `db/blob/blob_file_reader.cc:28-84`

```cpp
std::shared_ptr<Decompressor> decompressor;
if (compression_type != kNoCompression) {
  decompressor = GetBuiltinV2CompressionManager()->GetDecompressorOptimizeFor(
      compression_type);
}
```

Also hardcodes `GetBuiltinV2CompressionManager()` for the decompressor.

### Validation

**File:** `db/column_family.cc:176-183`

```cpp
if (!CompressionTypeSupported(cf_options.blob_compression_type)) {
    // ... error ...
}
```

Only checks `CompressionTypeSupported()`, does not check `CompressionManager`.

### Dump

**File:** `options/cf_options.cc:1283-1284`

```cpp
ROCKS_LOG_INFO(log, "                    blob_compression_type: %s",
               CompressionTypeToString(blob_compression_type).c_str());
```

### UpdateColumnFamilyOptions

**File:** `options/options_helper.cc:281`

```cpp
cf_opts->blob_compression_type = moptions.blob_compression_type;
```

### options_settable_test

**File:** `options/options_settable_test.cc:665`

```
"blob_compression_type=kBZip2Compression;"
```

---

## CompressionManager Architecture

### Interface

**File:** `include/rocksdb/advanced_compression.h:402-508`

```cpp
class CompressionManager : public std::enable_shared_from_this<CompressionManager>,
                           public Customizable {
  virtual const char* CompatibilityName() const = 0;
  virtual bool SupportsCompressionType(CompressionType type) const = 0;
  virtual std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext&, const CompressionOptions& opts,
      CompressionType preferred);
  virtual std::unique_ptr<Compressor> GetCompressor(
      const CompressionOptions& opts, CompressionType type) = 0;
  virtual std::shared_ptr<Decompressor> GetDecompressor() = 0;
  virtual std::shared_ptr<Decompressor> GetDecompressorOptimizeFor(CompressionType);
};
```

### Built-in Implementation

**File:** `util/compression.cc:1720-1815`

`BuiltinCompressionManagerV2` creates the appropriate compressor based on type:
```cpp
switch (type) {
  case kNoCompression: return nullptr;
  case kZSTD:          return make_unique<BuiltinZSTDCompressorV2>(opts);
  // ... etc.
}
```

### Singleton Access

**File:** `util/compression.cc:1891-1895`

```cpp
const std::shared_ptr<CompressionManager>& GetBuiltinV2CompressionManager();
```

---

## Data Flow Summary

### Current Blob Compression (Write)

```
ColumnFamilyOptions::blob_compression_type
  → MutableCFOptions::blob_compression_type
    → BlobFileBuilder::blob_compression_type_
      → GetBuiltinV2CompressionManager()->GetCompressor(CompressionOptions{}, type)
        → BuiltinZSTDCompressorV2(default_opts)  // level=kDefaultCompressionLevel
          → ZSTD_compress2(ctx, ...)  // uses default compression level
```

### Desired Blob Compression (Write)

```
ColumnFamilyOptions::blob_compression_type + blob_compression_opts + blob_compression_manager
  → MutableCFOptions::blob_compression_type + blob_compression_opts + blob_compression_manager
    → BlobFileBuilder
      → (blob_compression_manager ?? GetBuiltinV2CompressionManager())
          ->GetCompressor(blob_compression_opts, type)
        → BuiltinZSTDCompressorV2(user_opts)  // level=user_specified
          → ZSTD_compress2(ctx, ...)  // uses user's compression level
```

---

## Files That Need Modification

### Public API
1. `include/rocksdb/advanced_options.h` — Add `blob_compression_opts` and `blob_compression_manager` fields
2. `include/rocksdb/options.h` — No changes needed (advanced_options.h is included)

### Options Infrastructure
3. `options/cf_options.h` — Add fields to `MutableCFOptions`
4. `options/cf_options.cc` — Register in `cf_mutable_options_type_info`, add to Dump()
5. `options/options_helper.cc` — Add to `UpdateColumnFamilyOptions`

### Blob Write Path
6. `db/blob/blob_file_builder.h` — No new members needed (options are read at construction time)
7. `db/blob/blob_file_builder.cc` — Use `blob_compression_opts` and `blob_compression_manager` from `MutableCFOptions`

### Blob Read Path
8. `db/blob/blob_file_reader.cc` — Use `blob_compression_manager` for decompressor

### Validation
9. `db/column_family.cc` — Update `CheckCompressionSupported` to use `blob_compression_manager`

### Tests
10. `options/options_settable_test.cc` — Update blob options string
11. `options/options_test.cc` — Add tests for new options
12. `db/blob/blob_file_builder_test.cc` — Test with custom compression opts
13. `db/blob/db_blob_basic_test.cc` — Integration test with compression level
14. `db/blob/db_blob_compaction_test.cc` — Test with compression manager

---

## Key Architectural Considerations

1. **Mutability**: Both `compression_opts` and `compression_manager` are mutable for SST. The blob equivalents should also be mutable for consistency.

2. **Read path**: `BlobFileReader` needs the `CompressionManager` to create its decompressor. However, decompression doesn't need `CompressionOptions` (level is only needed for compression, not decompression). The decompressor is obtained once when the reader is created. The `blob_compression_manager` needs to be available in `ImmutableCFOptions` or passed through to the blob reader creation path.

3. **Backwards compatibility**: Default values should match current behavior. `blob_compression_opts` defaults to `CompressionOptions{}` (which is what's used today). `blob_compression_manager` defaults to `nullptr` (which triggers `GetBuiltinV2CompressionManager()` fallback, matching current behavior).

4. **No schema change needed**: The blob file format stores compression type but NOT compression options. Compression options only affect the compression algorithm parameters, not the data format. So existing blob files remain readable.

5. **BlobFileReader decompressor path**: Currently `BlobFileReader::Create()` in `db/blob/blob_file_reader.cc` gets the decompressor from `GetBuiltinV2CompressionManager()`. To support custom `blob_compression_manager`, we need to pass it through the creation path. The `ImmutableCFOptions` is already available in this path.

6. **Validation consistency**: Currently blob compression validation only checks `CompressionTypeSupported()`. With a custom manager, it should use `CheckCompressionSupportedWithManager()` to be consistent with SST compression.

7. **Compression ratio threshold**: The SST path uses `max_compressed_bytes_per_kb` from `CompressionOptions` to skip compression when the ratio is poor. The blob path uses `LegacyForceBuiltinCompression()` which always compresses. Supporting `blob_compression_opts` means `max_compressed_bytes_per_kb` would be passed to the compressor, but `LegacyForceBuiltinCompression` ignores it. This is a pre-existing design issue (the TODO mentions it). For now, we should focus on passing the options through, and the behavior of `LegacyForceBuiltinCompression` is a separate concern.
