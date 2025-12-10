# Symbol Visibility Implementation for RocksDB

### What I've Accomplished

#### 1. Created the Visibility Macro (port_defs.h)
Added `ROCKSDB_API` macro that works across platforms:
- Linux/Unix: Uses GCC/Clang's `__attribute__((visibility("default")))`
- Windows: Uses `__declspec(dllexport/dllimport)`
- Static builds: No overhead (expands to nothing)

This lets us mark which classes/functions should be exported.

#### 2. Updated Build Systems
**CMake**: Added `HIDE_PRIVATE_SYMBOLS` option that:
- Sets compiler to hide all symbols by default
- Only exports what we explicitly mark with `ROCKSDB_API`
- Default is OFF (backward compatible)

**Makefile**: Added support via `HIDE_PRIVATE_SYMBOLS=1` environment variable

#### 3. Started Marking Public APIs
Applied `ROCKSDB_API` to the main `DB` class as a proof of concept.

#### 4. Fixed a Bug
Found and fixed a Clang compatibility issue with `using enum` - changed to fully qualified enum names in `db.h`.

### Testing Done

**Task 1: Checked compiler options** - Visibility flags working correctly  
**Task 2: Build without breaking** - Both default and hidden modes compile fine  
**Task 3: Tested with Clang** - Works with both GCC and Clang

**Important**: This doesn't affect functionality at all. When disabled (default), it's like the code changes don't exist. When enabled, it only affects which symbols are visible in the symbol table - the actual code behavior is identical.

---

## Overview
This implementation addresses GitHub issue #12929: "RocksDB library leaks private symbols"

## The Problem We're Solving

Right now, RocksDB exports everything - all internal classes, templates, helper functions, etc. This causes:
- Thousands of symbols leaking (seen in Debian package builds)
- ABI breaks when we change internal code
- Larger libraries and slower linking
- Conflicts with other libraries

## What Needs to Happen Next

### Phase 1: Infrastructure (Pretty Done)
The big job is going through all the public API headers and marking classes with `ROCKSDB_API`. Here's what's left:

### Must Mark (High Priority - ~20 classes)
These are the core APIs that users interact with:
- `Options`, `DBOptions`, `ColumnFamilyOptions` (options.h)
- `ReadOptions`, `WriteOptions` (options.h)
- `Status`, `IOStatus` (status.h, io_status.h)
- `Iterator`, `WriteBatch` (iterator.h, write_batch.h)
- `Snapshot`, `Cache` (snapshot.h, cache.h)
- `Env`, `FileSystem` (env.h, file_system.h)
- `Logger`, `Statistics` (env.h, statistics.h)
- `ColumnFamilyHandle` (db.h)
- `Slice`, `PinnableSlice` (slice.h)
- `Comparator`, `MergeOperator` (comparator.h, merge_operator.h)

### Should Mark (Medium Priority - ~30-40 classes)
- Everything in `include/rocksdb/utilities/*.h`
- Listener/callback classes
- Filter and table factory classes
- SST file readers/writers

### Can Skip (Internal/Private)
- Anything not in `include/` directory
- Implementation details in `db/`, `util/`, etc.

## Usage Examples

### For Users (Default Behavior - Backward Compatible)
```bash
# No changes needed - works as before
make shared_lib
cmake .. && make
```

### For Packagers (Enable Symbol Hiding)
```bash
# CMake
cmake -DHIDE_PRIVATE_SYMBOLS=ON ..

# Makefile
HIDE_PRIVATE_SYMBOLS=1 make shared_lib
```

### For Developers (Marking New Public APIs)
```cpp
// In public header include/rocksdb/my_new_class.h
#include "rocksdb/port_defs.h"

namespace ROCKSDB_NAMESPACE {

// Export this class in shared library
class ROCKSDB_API MyPublicClass {
 public:
  ROCKSDB_API void PublicMethod();
  
 private:
  void PrivateMethod(); // Not exported
};

} // namespace ROCKSDB_NAMESPACE
```

## Benefits

### When Fully Implemented
1. **ABI Stability**: Only intentional API changes break compatibility
2. **Smaller Binaries**: Less symbols to export/resolve
3. **Faster Linking**: Dynamic linker has less work
4. **Better Encapsulation**: Internal changes invisible to users
5. **Package Maintainer Friendly**: Clean symbol files for Debian, etc.

### Backward Compatibility
- **Default behavior unchanged**: All symbols still exported by default
- **Opt-in feature**: Must explicitly enable `HIDE_PRIVATE_SYMBOLS`
- **No source-level changes** required for existing code
- **Binary compatibility**: Only affects shared library symbol table

## References

- **GitHub Issue**: https://github.com/facebook/rocksdb/issues/12929
- **Related PR**: https://github.com/facebook/rocksdb/pull/12944
- **GCC Visibility**: https://gcc.gnu.org/wiki/Visibility
- **Clang Visibility**: https://clang.llvm.org/docs/AttributeReference.html#visibility

## Rollout Plan

### Phase 1 (Done)
- Infrastructure implementation
- Feature flag protection
- CMake and Makefile support
- Initial API marking (DB class)

### Phase 2 (TODO)
- Mark all public API classes (~50-70 classes)
- Comprehensive testing
- Documentation updates

### Phase 3 (FUTURE)
- Default to ON in RocksDB 10.0
- Deprecation warnings for old behavior
- Remove feature flag in 11.0

## Known Limitations

1. **JNI**: Needs testing to ensure Java bindings still work
2. **Plugins**: Plugin API must be explicitly exported
3. **Custom Implementations**: Users with custom Env, Cache, etc. may need updates
4. **Template Instantiations**: Some might need explicit instantiation

## How to Use This

### Default (Unchanged Behavior)
Just build normally - nothing changes:
```bash
make shared_lib
# or
cmake .. && make
```

### Enable Symbol Hiding (For Testing/Packaging)
```bash
# With Make:
HIDE_PRIVATE_SYMBOLS=1 make shared_lib

# With CMake:
cmake -DHIDE_PRIVATE_SYMBOLS=ON ..
make
```

## Files I Changed

```
include/rocksdb/port_defs.h          - Added ROCKSDB_API macro
include/rocksdb/db.h                 - Fixed Clang enum issue, marked DB class
include/rocksdb/options.h            - Added port_defs.h include
CMakeLists.txt                       - Added visibility preset logic
build_tools/build_detect_platform    - Added visibility flags for Make
```

---