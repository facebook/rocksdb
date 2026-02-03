# RocksDB API Development Guide

This document provides guidance for adding new public APIs to RocksDB, following the established patterns used by existing APIs like `CompactRange`.

## API Layer Architecture

RocksDB exposes public APIs through multiple layers. Users can access RocksDB through any of the three public APIs: C++ headers, C headers, or Java bindings.

Here is an example for public header db.h:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Level 1: Public APIs (User Entry Points)                │
├───────────────────────┬─────────────────────────┬───────────────────────────┤
│   C++ Public API      │     C API Bindings      │       Java/JNI API        │
│ include/rocksdb/db.h  │   include/rocksdb/c.h   │ java/src/.../RocksDB.java │
│ include/rocksdb/*.h   │                         │ java/src/.../*.java       │
└───────────────────────┴────────────┬────────────┴───────────────────────────┘
                                     ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│              Level 2: C++ Implementation (Internal Core)                    │
│              db/db_impl/db_impl*.cc, db/c.cc, java/rocksjni/*.cc            │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Step-by-Step Guide: Adding a New Public API

### Step 1: Define the C++ Public Interface

**File:** `include/rocksdb/db.h`

Add the virtual method declaration in the `DB` class:

\`\`\`cpp
// Pure virtual - must be implemented by DBImpl
virtual Status YourNewAPI(const YourAPIOptions& options,
                          ColumnFamilyHandle* column_family,
                          /* other params */) = 0;

// Convenience overload for default column family
virtual Status YourNewAPI(const YourAPIOptions& options,
                          /* other params */) {
  return YourNewAPI(options, DefaultColumnFamily(), /* other params */);
}
\`\`\`

**Key Patterns:**
- Use `Status` return type for error handling
- Use `OptSlice` to avoid unnecessary levels of indirection and use of raw pointers.
- Use `ColumnFamilyHandle*` for column family support
- Provide convenience overloads for the default column family

### Step 2: Define Options Struct (If Needed)

**File:** `include/rocksdb/options.h`

If your API has multiple configuration options, define an options struct:

\`\`\`cpp
struct YourAPIOptions {
  // Document each option with clear comments
  bool some_boolean_option = false;

  // Default value explanation
  int some_int_option = -1;

  // Pointer options require careful lifetime management
  std::atomic<bool>* canceled = nullptr;

  // Enum options for multi-choice settings
  YourEnumType some_enum = YourEnumType::kDefault;
};
\`\`\`

**Key Patterns:**
- Use sensible default values specified inline (e.g., `= false`, `= -1`)
- Do NOT redundantly document the default value in comments; instead, document the rationale (why this default), historical context, and how different values are interpreted
- Group related options logically
- Consider thread-safety for pointer options

### Step 3: Implement in DBImpl

**Header:** `db/db_impl/db_impl.h`

\`\`\`cpp
using DB::YourNewAPI;
Status YourNewAPI(const YourAPIOptions& options,
                  ColumnFamilyHandle* column_family,
                  /* other params */) override;

// Private internal implementation if needed
Status YourNewAPIInternal(const YourAPIOptions& options,
                          ColumnFamilyHandle* column_family,
                          /* other params */);
\`\`\`

**Implementation:** `db/db_impl/db_impl_<category>.cc`

Choose the appropriate implementation file based on functionality:
- `db_impl_compaction_flush.cc` - Compaction and flush operations
- `db_impl_write.cc` - Write operations
- `db_impl_open.cc` - DB opening/closing
- `db_impl_files.cc` - File operations
- `db_impl.cc` - General operations

\`\`\`cpp
Status DBImpl::YourNewAPI(const YourAPIOptions& options,
                          ColumnFamilyHandle* column_family,
                          /* other params */) {
  // 1. Input validation
  if (/* invalid input */) {
    return Status::InvalidArgument("Error message");
  }

  // 2. Check for cancellation/abort conditions
  if (options.canceled && options.canceled->load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  // 3. Get column family data
  auto cfh = static_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  // 4. Core implementation logic
  // ...

  return Status::OK();
}
\`\`\`

### Step 4: Handle Special DB Types

**StackableDB (Wrapper DBs):**
**File:** `include/rocksdb/utilities/stackable_db.h`

\`\`\`cpp
using DB::YourNewAPI;
Status YourNewAPI(const YourAPIOptions& options,
                  ColumnFamilyHandle* column_family,
                  /* other params */) override {
  return db_->YourNewAPI(options, column_family, /* other params */);
}
\`\`\`

**Secondary DB (Read-Only):**
**File:** `db/db_impl/db_impl_secondary.h`

\`\`\`cpp
using DBImpl::YourNewAPI;
Status YourNewAPI(const YourAPIOptions& /*options*/,
                  ColumnFamilyHandle* /*column_family*/,
                  /* other params */) override {
  return Status::NotSupported("Not supported in secondary DB");
}
\`\`\`

**CompactedDB (Read-Only):**
**File:** `db/db_impl/compacted_db_impl.h`

\`\`\`cpp
using DBImpl::YourNewAPI;
Status YourNewAPI(const YourAPIOptions& /*options*/,
                  ColumnFamilyHandle* /*column_family*/,
                  /* other params */) override {
  return Status::NotSupported("Not supported for read-only DB");
}
\`\`\`

### Step 5: Add C API Bindings

**Header:** `include/rocksdb/c.h`

\`\`\`c
// Basic version
extern ROCKSDB_LIBRARY_API void rocksdb_your_new_api(
    rocksdb_t* db,
    const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len);

// Column family version
extern ROCKSDB_LIBRARY_API void rocksdb_your_new_api_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len);

// With options and error handling
extern ROCKSDB_LIBRARY_API void rocksdb_your_new_api_opt(
    rocksdb_t* db, rocksdb_your_api_options_t* opt,
    const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len,
    char** errptr);
\`\`\`

**Implementation:** `db/c.cc`

\`\`\`cpp
void rocksdb_your_new_api(rocksdb_t* db, const char* start_key,
                          size_t start_key_len, const char* limit_key,
                          size_t limit_key_len) {
  Slice a, b;
  db->rep->YourNewAPI(
      YourAPIOptions(),  // Default options
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void rocksdb_your_new_api_cf(rocksdb_t* db,
                             rocksdb_column_family_handle_t* column_family,
                             const char* start_key, size_t start_key_len,
                             const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->YourNewAPI(
      YourAPIOptions(),
      column_family->rep,
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}
\`\`\`

**If you have options, also add:**

\`\`\`cpp
// Options struct wrapper
struct rocksdb_your_api_options_t {
  YourAPIOptions rep;
};

rocksdb_your_api_options_t* rocksdb_your_api_options_create() {
  return new rocksdb_your_api_options_t;
}

void rocksdb_your_api_options_destroy(rocksdb_your_api_options_t* opt) {
  delete opt;
}

void rocksdb_your_api_options_set_some_option(
    rocksdb_your_api_options_t* opt, unsigned char value) {
  opt->rep.some_boolean_option = value;
}
\`\`\`

### Step 6: Add Java Bindings

**Java API:** `java/src/main/java/org/rocksdb/RocksDB.java`

\`\`\`java
// Basic version
public void yourNewAPI() throws RocksDBException {
  yourNewAPI(null);
}

// Column family version
public void yourNewAPI(ColumnFamilyHandle columnFamilyHandle)
    throws RocksDBException {
  yourNewAPI(nativeHandle_, null, -1, null, -1, 0,
      columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
}

// Range version
public void yourNewAPI(final byte[] begin, final byte[] end)
    throws RocksDBException {
  yourNewAPI(null, begin, end);
}

// Full-featured version with options
public void yourNewAPI(ColumnFamilyHandle columnFamilyHandle,
                       final byte[] begin, final byte[] end,
                       final YourAPIOptions options)
    throws RocksDBException {
  yourNewAPI(nativeHandle_,
      begin, begin == null ? -1 : begin.length,
      end, end == null ? -1 : end.length,
      options.nativeHandle_,
      columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
}

// Native method declaration
private static native void yourNewAPI(final long handle,
    /* @Nullable */ final byte[] begin, final int beginLen,
    /* @Nullable */ final byte[] end, final int endLen,
    final long optionsHandle,
    final long cfHandle);
\`\`\`

**Options Class:** `java/src/main/java/org/rocksdb/YourAPIOptions.java`

\`\`\`java
public class YourAPIOptions extends RocksObject {

  public YourAPIOptions() {
    super(newYourAPIOptions());
  }

  // Builder pattern setters
  public YourAPIOptions setSomeBooleanOption(boolean value) {
    setSomeBooleanOption(nativeHandle_, value);
    return this;
  }

  // Getters
  public boolean someBooleanOption() {
    return someBooleanOption(nativeHandle_);
  }

  // Native method declarations
  private static native long newYourAPIOptions();
  private static native void disposeInternalJni(long handle);
  private static native void setSomeBooleanOption(long handle, boolean value);
  private static native boolean someBooleanOption(long handle);

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
}
\`\`\`

**JNI Implementation:** `java/rocksjni/rocksjni.cc`

\`\`\`cpp
void Java_org_rocksdb_RocksDB_yourNewAPI(
    JNIEnv* env, jclass,
    jlong jdb_handle, jbyteArray jbegin, jint jbegin_len,
    jbyteArray jend, jint jend_len,
    jlong joptions_handle, jlong jcf_handle) {

  // 1. Convert Java byte arrays to C++ strings
  jboolean has_exception = JNI_FALSE;
  std::string str_begin;
  if (jbegin_len > 0) {
    str_begin = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, jbegin, jbegin_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) return;
  }

  std::string str_end;
  if (jend_len > 0) {
    str_end = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, jend, jend_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) return;
  }

  // 2. Get or create options
  ROCKSDB_NAMESPACE::YourAPIOptions* options = nullptr;
  if (joptions_handle == 0) {
    options = new ROCKSDB_NAMESPACE::YourAPIOptions();
  } else {
    options = reinterpret_cast<ROCKSDB_NAMESPACE::YourAPIOptions*>(joptions_handle);
  }

  // 3. Unwrap handles
  auto* db = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle =
      jcf_handle == 0 ? db->DefaultColumnFamily()
                      : reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);

  // 4. Create Slices
  std::unique_ptr<ROCKSDB_NAMESPACE::Slice> begin;
  std::unique_ptr<ROCKSDB_NAMESPACE::Slice> end;
  if (jbegin_len > 0) begin.reset(new ROCKSDB_NAMESPACE::Slice(str_begin));
  if (jend_len > 0) end.reset(new ROCKSDB_NAMESPACE::Slice(str_end));

  // 5. Call C++ API
  ROCKSDB_NAMESPACE::Status s = db->YourNewAPI(*options, cf_handle, begin.get(), end.get());

  // 6. Cleanup if we created options
  if (joptions_handle == 0) delete options;

  // 7. Throw Java exception on error
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}
\`\`\`

**Options JNI:** `java/rocksjni/your_api_options.cc`

\`\`\`cpp
jlong Java_org_rocksdb_YourAPIOptions_newYourAPIOptions(JNIEnv*, jclass) {
  auto* options = new ROCKSDB_NAMESPACE::YourAPIOptions();
  return GET_CPLUSPLUS_POINTER(options);
}

void Java_org_rocksdb_YourAPIOptions_disposeInternalJni(JNIEnv*, jclass, jlong jhandle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::YourAPIOptions*>(jhandle);
  delete options;
}

void Java_org_rocksdb_YourAPIOptions_setSomeBooleanOption(
    JNIEnv*, jclass, jlong jhandle, jboolean value) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::YourAPIOptions*>(jhandle);
  options->some_boolean_option = static_cast<bool>(value);
}

jboolean Java_org_rocksdb_YourAPIOptions_someBooleanOption(JNIEnv*, jclass, jlong jhandle) {
  auto* options = reinterpret_cast<ROCKSDB_NAMESPACE::YourAPIOptions*>(jhandle);
  return static_cast<jboolean>(options->some_boolean_option);
}
\`\`\`

### Step 7: Update Build Files

**Java CMakeLists.txt:** `java/CMakeLists.txt`

Add your new Java source files:
\`\`\`cmake
src/main/java/org/rocksdb/YourAPIOptions.java
src/test/java/org/rocksdb/YourAPIOptionsTest.java
\`\`\`

### Step 8: Add Release Notes

**Directory:** `unreleased_history/`

RocksDB uses individual files in the `unreleased_history/` directory rather than directly editing `HISTORY.md`. This avoids merge conflicts and ensures changes are attributed to the correct release version.

Add a file to the appropriate subdirectory:
- `unreleased_history/new_features/` - For new functionality
- `unreleased_history/public_api_changes/` - For API changes
- `unreleased_history/behavior_changes/` - For behavior modifications
- `unreleased_history/bug_fixes/` - For bug fixes

**Example:** `unreleased_history/new_features/your_new_api.md`

\`\`\`markdown
Added `YourNewAPI()` to support [describe functionality]. See `YourAPIOptions` for configuration.
\`\`\`

**Example:** `unreleased_history/public_api_changes/your_api_options.md`

**Note:** Files should contain one line of markdown. The "* " prefix is automatically added if not included. These files are compiled into `HISTORY.md` during the release process.

### Step 9: Add Tests

**C++ Unit Tests:** `db/db_your_api_test.cc` or add to existing test file

\`\`\`cpp
TEST_F(DBTest, YourNewAPIBasic) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  // Setup test data
  ASSERT_OK(Put(1, "key1", "value1"));
  ASSERT_OK(Put(1, "key2", "value2"));

  // Test your API
  YourAPIOptions api_options;
  api_options.some_boolean_option = true;
  ASSERT_OK(db_->YourNewAPI(api_options, handles_[1], nullptr, nullptr));

  // Verify results
  // ...
}
\`\`\`

**Java Tests:** `java/src/test/java/org/rocksdb/YourAPIOptionsTest.java`

\`\`\`java
public class YourAPIOptionsTest {
  @Test
  public void yourAPIOptions() {
    try (final YourAPIOptions options = new YourAPIOptions()) {
      assertFalse(options.someBooleanOption());
      options.setSomeBooleanOption(true);
      assertTrue(options.someBooleanOption());
    }
  }
}
\`\`\`

## File Summary Checklist


| Component | File(s) | Required |
|-----------|---------|----------|
| C++ Public Interface | `include/rocksdb/db.h` | ✓ |
| Options Struct | `include/rocksdb/options.h` | If needed |
| DBImpl Declaration | `db/db_impl/db_impl.h` | ✓ |
| DBImpl Implementation | `db/db_impl/db_impl_*.cc` | ✓ |
| StackableDB | `include/rocksdb/utilities/stackable_db.h` | ✓ |
| Secondary DB | `db/db_impl/db_impl_secondary.h` | If not supported |
| Compacted DB | `db/db_impl/compacted_db_impl.h` | If not supported |
| C API Header | `include/rocksdb/c.h` | ✓ |
| C API Implementation | `db/c.cc` | ✓ |
| Java API | `java/src/main/java/org/rocksdb/RocksDB.java` | ✓ |
| Java Options | `java/src/main/java/org/rocksdb/YourAPIOptions.java` | If needed |
| JNI Implementation | `java/rocksjni/rocksjni.cc` | ✓ |
| JNI Options | `java/rocksjni/your_api_options.cc` | If needed |
| Java CMake | `java/CMakeLists.txt` | If new files |
| Changelog | `unreleased_history/*.md` | ✓ |
| C++ Tests | `db/db_*_test.cc` | ✓ |
| Java Tests | `java/src/test/java/org/rocksdb/*Test.java` | ✓ |

## Best Practices

1. **Error Handling**: Always return `Status` objects in C++, throw exceptions in Java
2. **Default Values**: Provide sensible defaults for all options
3. **Documentation**: Add clear comments for all public methods and options
4. **Column Family Support**: Always support column family operations
5. **Thread Safety**: Document thread-safety guarantees
6. **Backward Compatibility**: Avoid breaking existing API contracts
7. **Testing**: Add comprehensive unit tests for all code paths
