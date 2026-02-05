# Adding New Options to RocksDB Public API

This document provides guidance on how to add new options to RocksDB's public API. There are two main categories of options:

1. **Standard Column Family Options** (Options/DBOptions/AdvancedColumnFamilyOptions)
2. **BlockBasedTableOptions** (options specific to block-based table format)

## Overview of Files to Modify

### For Standard Column Family Options

| File | Purpose |
|------|---------|
| `include/rocksdb/advanced_options.h` | Define the option with documentation |
| `include/rocksdb/options.h` | Add reference in related option groups if needed |
| `options/cf_options.h` | Add to `MutableCFOptions` or `ImmutableCFOptions` struct |
| `options/cf_options.cc` | Register option for serialization/deserialization and logging |
| `options/options_helper.cc` | Add to `UpdateColumnFamilyOptions()` for mutable options |
| `options/options_settable_test.cc` | Add to test string for option parsing |
| `db_stress_tool/db_stress_common.h` | Declare gflag |
| `db_stress_tool/db_stress_gflags.cc` | Define gflag with default value |
| `db_stress_tool/db_stress_test_base.cc` | Apply flag to options |
| `tools/db_bench_tool.cc` | Add flag definition and apply to options |
| `tools/db_crashtest.py` | Add randomized values for stress testing |
| `unreleased_history/new_features/` | Add release note markdown file |

### For BlockBasedTableOptions

| File | Purpose |
|------|---------|
| `include/rocksdb/table.h` | Define the option in `BlockBasedTableOptions` struct |
| `table/block_based/block_based_table_factory.cc` | Register for serialization, validation, and printing |
| `options/options_settable_test.cc` | Add to `BlockBasedTableOptionsAllFieldsSettable` test |
| `options/options_test.cc` | Add to `MutableCFOptions` test if applicable |
| `db_stress_tool/db_stress_common.h` | Declare gflag |
| `db_stress_tool/db_stress_gflags.cc` | Define gflag |
| `db_stress_tool/db_stress_test_base.cc` | Apply flag to `block_based_options` |
| `tools/db_bench_tool.cc` | Add flag definition and apply to `block_based_options` |
| `tools/db_crashtest.py` | Add randomized values |
| `java/src/main/java/org/rocksdb/BlockBasedTableConfig.java` | Java API |
| `java/rocksjni/portal.h` | JNI portal for Java bindings |
| `java/rocksjni/table.cc` | JNI implementation |
| `java/src/test/java/org/rocksdb/BlockBasedTableConfigTest.java` | Java unit test |

---

## Pattern 1: Adding a Standard Column Family Option

Example reference: commit `94e65a2e0b4f817aa4bfa4c96cdf867e7980d7bc` (memtable_veirfy_per_key_checksum_on_seek)

### Step 1: Define the Option in Public Header

**File: `include/rocksdb/advanced_options.h`**

Add the option with documentation in `AdvancedColumnFamilyOptions` struct:

```cpp
// Enables additional integrity checks during seek.
// Specifically, for skiplist-based memtables, key checksum validation could
// be enabled during seek optionally. This is helpful to detect corrupted
// memtable keys during reads. Enabling this feature incurs a performance
// overhead due to additional key checksum validation during memtable seek
// operation.
// This option depends on memtable_protection_bytes_per_key to be non zero.
// If memtable_protection_bytes_per_key is zero, no validation is performed.
bool memtable_veirfy_per_key_checksum_on_seek = false;
```

### Step 2: Add to Internal Options Structs

**File: `options/cf_options.h`**

Add to `MutableCFOptions` struct (or `ImmutableCFOptions` for immutable options):

```cpp
// In MutableCFOptions constructor from Options:
memtable_veirfy_per_key_checksum_on_seek(
    options.memtable_veirfy_per_key_checksum_on_seek),

// In MutableCFOptions default constructor:
memtable_veirfy_per_key_checksum_on_seek(false),

// In MutableCFOptions struct member declarations:
bool memtable_veirfy_per_key_checksum_on_seek;
```

### Step 3: Register for Serialization/Deserialization

**File: `options/cf_options.cc`**

Add to the options type info map for serialization:

```cpp
{"memtable_veirfy_per_key_checksum_on_seek",
 {offsetof(struct MutableCFOptions,
           memtable_veirfy_per_key_checksum_on_seek),
  OptionType::kBoolean, OptionVerificationType::kNormal,
  OptionTypeFlags::kMutable}},
```

Add logging in `MutableCFOptions::Dump()`:

```cpp
ROCKS_LOG_INFO(log, "memtable_veirfy_per_key_checksum_on_seek: %d",
               memtable_veirfy_per_key_checksum_on_seek);
```

### Step 4: Update Options Helper

**File: `options/options_helper.cc`**

Add to `UpdateColumnFamilyOptions()`:

```cpp
cf_opts->memtable_veirfy_per_key_checksum_on_seek =
    moptions.memtable_veirfy_per_key_checksum_on_seek;
```

### Step 5: Add to Options Settable Test

**File: `options/options_settable_test.cc`**

Add to the test string in `ColumnFamilyOptionsAllFieldsSettable`:

```cpp
"memtable_veirfy_per_key_checksum_on_seek=1;"
```

### Step 6: Add db_stress Support

**File: `db_stress_tool/db_stress_common.h`**

```cpp
DECLARE_bool(memtable_veirfy_per_key_checksum_on_seek);
```

**File: `db_stress_tool/db_stress_gflags.cc`**

```cpp
DEFINE_bool(
    memtable_veirfy_per_key_checksum_on_seek,
    ROCKSDB_NAMESPACE::Options().memtable_veirfy_per_key_checksum_on_seek,
    "Sets CF option memtable_veirfy_per_key_checksum_on_seek.");
```

**File: `db_stress_tool/db_stress_test_base.cc`**

```cpp
options.memtable_veirfy_per_key_checksum_on_seek =
    FLAGS_memtable_veirfy_per_key_checksum_on_seek;
```

### Step 7: Add db_bench Support

**File: `tools/db_bench_tool.cc`**

```cpp
// Flag definition (near related flags):
DEFINE_bool(memtable_veirfy_per_key_checksum_on_seek, false,
            "Sets CF option memtable_veirfy_per_key_checksum_on_seek");

// Apply flag to options (in InitializeOptionsFromFlags or similar):
options.memtable_veirfy_per_key_checksum_on_seek =
    FLAGS_memtable_veirfy_per_key_checksum_on_seek;
```

### Step 8: Add Crash Test Support

**File: `tools/db_crashtest.py`**

```python
"memtable_veirfy_per_key_checksum_on_seek": lambda: random.choice([0] * 7 + [1]),
```

Also add constraint handling in `finalize_and_sanitize()` if needed:

```python
# only skip list memtable representation supports paranoid memory checks
if dest_params.get("memtablerep") != "skip_list":
    dest_params["memtable_veirfy_per_key_checksum_on_seek"] = 0
```

### Step 9: Add Release Note

**File: `unreleased_history/new_features/<descriptive_name>.md`**

```markdown
A new flag memtable_veirfy_per_key_checksum_on_seek is added to AdvancedColumnFamilyOptions. When it is enabled, it will validate key checksum along the binary search path on skiplist based memtable during seek operation.
```

---

## Pattern 2: Adding a BlockBasedTableOptions Option

Example reference: commit `742741b175c5f238374c1714f9db3340d49de569` (super_block_alignment_size)

### Step 1: Define the Option in Public Header

**File: `include/rocksdb/table.h`**

Add to `BlockBasedTableOptions` struct with documentation:

```cpp
// Align data blocks on super block alignment. Avoid a data block split across
// super block boundaries. Works with/without compression.
//
// Here a "super block" refers to an aligned unit of underlying Filesystem
// storage for which there is an extra cost when a random read involves two
// such super blocks instead of just one. Configuring that size here suggests
// inserting padding in the SST file to avoid a single SST block splitting
// across two super blocks. Only power-of-two sizes are supported. See also
// super_block_alignment_space_overhead_ratio. Default to 0, which means super
// block alignment is disabled.
size_t super_block_alignment_size = 0;

// This option controls the storage space overhead of super block alignment.
// It is used to calculate the max padding size allowed for super block
// alignment. It is calculated in this way. If super_block_alignment_size is
// 2MB, and super_block_alignment_overhead_ratio is 128, then the max padding
// size allowed for super block alignment is 2MB / 128 = 16KB.
// Note that, when it is set to 0, super block alignment is disabled.
size_t super_block_alignment_space_overhead_ratio = 128;
```

### Step 2: Register for Serialization in Table Factory

**File: `table/block_based/block_based_table_factory.cc`**

Add to the type info map:

```cpp
{"super_block_alignment_size",
 {offsetof(struct BlockBasedTableOptions, super_block_alignment_size),
  OptionType::kSizeT, OptionVerificationType::kNormal}},
{"super_block_alignment_space_overhead_ratio",
 {offsetof(struct BlockBasedTableOptions,
           super_block_alignment_space_overhead_ratio),
  OptionType::kSizeT, OptionVerificationType::kNormal}},
```

Add validation in `ValidateOptions()`:

```cpp
if ((table_options_.super_block_alignment_size &
     (table_options_.super_block_alignment_size - 1))) {
  return Status::InvalidArgument(
      "Super Block alignment requested but super block alignment size is not "
      "a power of 2");
}
if (table_options_.super_block_alignment_size >
    std::numeric_limits<uint32_t>::max()) {
  return Status::InvalidArgument(
      "Super block alignment size exceeds maximum number (4GiB) allowed");
}
```

Add printing in `GetPrintableOptions()`:

```cpp
snprintf(buffer, kBufferSize,
         "  super_block_alignment_size: %" ROCKSDB_PRIszt "\n",
         table_options_.super_block_alignment_size);
ret.append(buffer);
```

### Step 3: Add to Options Settable Test

**File: `options/options_settable_test.cc`**

Add to `BlockBasedTableOptionsAllFieldsSettable` test:

```cpp
"super_block_alignment_size=65536;"
"super_block_alignment_space_overhead_ratio=4096;"
```

### Step 4: Add to Options Test

**File: `options/options_test.cc`**

```cpp
ASSERT_OK(GetColumnFamilyOptionsFromString(
    config_options, cf_opts,
    "block_based_table_factory.super_block_alignment_size=65536; "
    "block_based_table_factory.super_block_alignment_space_overhead_ratio=4096;",
    &cf_opts));
ASSERT_EQ(bbto->super_block_alignment_size, 65536);
ASSERT_EQ(bbto->super_block_alignment_space_overhead_ratio, 4096);
```

### Step 5: Add db_stress Support

**File: `db_stress_tool/db_stress_common.h`**

```cpp
DECLARE_uint64(super_block_alignment_size);
DECLARE_uint64(super_block_alignment_space_overhead_ratio);
```

**File: `db_stress_tool/db_stress_gflags.cc`**

```cpp
DEFINE_uint64(
    super_block_alignment_size,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().super_block_alignment_size,
    "BlockBasedTableOptions.super_block_alignment_size");

DEFINE_uint64(
    super_block_alignment_space_overhead_ratio,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions()
        .super_block_alignment_space_overhead_ratio,
    "BlockBasedTableOptions.super_block_alignment_space_overhead_ratio");
```

**File: `db_stress_tool/db_stress_test_base.cc`**

```cpp
block_based_options.super_block_alignment_size =
    fLU64::FLAGS_super_block_alignment_size;
block_based_options.super_block_alignment_space_overhead_ratio =
    fLU64::FLAGS_super_block_alignment_space_overhead_ratio;
```

### Step 6: Add db_bench Support

**File: `tools/db_bench_tool.cc`**

```cpp
// Flag definitions:
DEFINE_uint64(
    super_block_alignment_size,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().super_block_alignment_size,
    "Configure super block size");

DEFINE_uint64(super_block_alignment_space_overhead_ratio,
              ROCKSDB_NAMESPACE::BlockBasedTableOptions()
                  .super_block_alignment_space_overhead_ratio,
              "Configure space overhead for super block alignment");

// Apply to block_based_options (in the block where other options are set):
block_based_options.super_block_alignment_size = FLAGS_super_block_alignment_size;
block_based_options.super_block_alignment_space_overhead_ratio =
    FLAGS_super_block_alignment_space_overhead_ratio;
```

### Step 7: Add Crash Test Support

**File: `tools/db_crashtest.py`**

```python
"super_block_alignment_size": lambda: random.choice(
    [0, 128 * 1024, 512 * 1024, 2 * 1024 * 1024]
),
"super_block_alignment_space_overhead_ratio": lambda: random.choice([0, 32, 4096]),
```

### Step 8: Add Java API Support

**File: `java/src/main/java/org/rocksdb/BlockBasedTableConfig.java`**

Add getter and setter methods:

```java
/**
 * Get the super block alignment size.
 *
 * @return the super block alignment size.
 */
public long superBlockAlignmentSize() {
  return superBlockAlignmentSize;
}

/**
 * Set the super block alignment size.
 * When set to 0, super block alignment is disabled.
 *
 * @param superBlockAlignmentSize the super block alignment size.
 *
 * @return the reference to the current option.
 */
public BlockBasedTableConfig setSuperBlockAlignmentSize(final long superBlockAlignmentSize) {
  this.superBlockAlignmentSize = superBlockAlignmentSize;
  return this;
}
```

Add member variable:

```java
private long superBlockAlignmentSize;
```

Update constructor and native method signature.

**File: `java/rocksjni/portal.h`**

Update `GetMethodID` signature and add fields to Java object construction.

**File: `java/rocksjni/table.cc`**

Add parameters to JNI function and apply to options.

**File: `java/src/test/java/org/rocksdb/BlockBasedTableConfigTest.java`**

Add unit tests:

```java
@Test
public void superBlockAlignmentSize() {
  final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
  blockBasedTableConfig.setSuperBlockAlignmentSize(1024 * 1024);
  assertThat(blockBasedTableConfig.superBlockAlignmentSize()).isEqualTo(1024 * 1024);
}
```

---

## Pattern 3: Adding C API for Existing Option

Example reference: commit `429b36c22d76403d275dd0e6877b08d4cea2bc90` (block_align C API)

If an option already exists but needs C API support:

**File: `db/c.cc`**

```cpp
void rocksdb_block_based_options_set_block_align(
    rocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.block_align = v;
}
```

**File: `include/rocksdb/c.h`**

```cpp
extern ROCKSDB_LIBRARY_API void rocksdb_block_based_options_set_block_align(
    rocksdb_block_based_table_options_t*, unsigned char);
```

---

## Unit Testing Guidelines

### For Standard Options

Add tests in appropriate test files (e.g., `db/db_memtable_test.cc`, `db/db_options_test.cc`):

```cpp
TEST_F(DBMemTableTest, YourOptionTest) {
  Options options;
  options.your_new_option = true;
  Reopen(options);
  // Test the behavior
}
```

### For BlockBasedTableOptions

Add tests in `db/db_flush_test.cc`, `table/block_based/block_based_table_reader_test.cc`, or `table/table_test.cc`:

```cpp
TEST_P(DBFlushYourFeatureTest, YourFeature) {
  Options options;
  BlockBasedTableOptions block_options;
  block_options.your_new_option = some_value;
  options.table_factory.reset(NewBlockBasedTableFactory(block_options));

  ASSERT_OK(options.table_factory->ValidateOptions(
      DBOptions(options), ColumnFamilyOptions(options)));

  Reopen(options);
  // Test the behavior
}
```

---

## Option Type Reference

Common option types used in serialization:

| OptionType | C++ Type | Example |
|------------|----------|---------|
| `kBoolean` | `bool` | `paranoid_memory_checks` |
| `kInt` | `int` | `max_write_buffer_number` |
| `kInt32T` | `int32_t` | `level0_file_num_compaction_trigger` |
| `kUInt32T` | `uint32_t` | `memtable_protection_bytes_per_key` |
| `kUInt64T` | `uint64_t` | `target_file_size_base` |
| `kSizeT` | `size_t` | `block_size` |
| `kDouble` | `double` | `compression_ratio` |
| `kString` | `std::string` | `db_log_dir` |

---

## Checklist Summary

- [ ] Public header file with option definition and documentation
- [ ] Internal options struct (MutableCFOptions or ImmutableCFOptions)
- [ ] Options serialization/deserialization registration
- [ ] Options logging in Dump() method
- [ ] UpdateColumnFamilyOptions() for mutable options
- [ ] options_settable_test.cc
- [ ] db_stress_common.h (DECLARE)
- [ ] db_stress_gflags.cc (DEFINE)
- [ ] db_stress_test_base.cc (apply flag)
- [ ] db_bench_tool.cc (DEFINE and apply)
- [ ] db_crashtest.py (randomized values)
- [ ] Unit tests
- [ ] unreleased_history markdown file
- [ ] Java API (for BlockBasedTableOptions)
- [ ] C API (if needed)

