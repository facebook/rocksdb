# User Defined Index (UDI)

**Status:** EXPERIMENTAL - subject to change while under development

## Overview

User Defined Index (UDI) is an experimental feature that allows users to plug custom index implementations into RocksDB's block-based table format. While RocksDB's default index uses binary search over separator keys, UDI enables alternative indexing strategies optimized for specific access patterns or data characteristics.

UDI indexes are built alongside the internal index during SST file construction and stored as separate meta blocks in the SST file (written before the metaindex block and footer; the footer only stores handles to the metaindex and index blocks). At read time, users can choose to use the UDI instead of the internal index for forward iteration and point lookups.

### Key Characteristics

- **Supplemental, not replacement:** UDI complements the internal index rather than replacing it. The internal index is always built and maintained for correctness and backward compatibility.
- **Custom encoding:** UDI implementations control their own serialization format, enabling space-optimized representations like tries, hash tables, or compressed bitmaps.
- **Read-time selection:** Users choose whether to use UDI on a per-ReadOptions basis, allowing experimentation without schema migration.
- **Forward-only:** Current UDI implementations support forward iteration (SeekToFirst, Seek, Next) and point lookups (Get), but not reverse iteration (SeekToLast, SeekForPrev, Prev).

## Architecture

### Component Diagram

```
SST File (on disk):
  - Data Block 0
  - Data Block 1
  - ... (more blocks)
  - Internal Index Block
      (separator_0, handle_0), (separator_1, handle_1)...
  - User Defined Index Block (optional meta block)
      Name: "rocksdb.user_defined_index.<factory_name>"
      Format: implementation-specific serialization
  - Meta Index Block
      References: filter, properties, UDI block, etc.
  - Footer
```

### Key Classes

| Class | Responsibility | Location |
|-------|----------------|----------|
| `UserDefinedIndexBuilder` | Abstract interface for building UDI during SST writes | `include/rocksdb/user_defined_index.h:33` |
| `UserDefinedIndexIterator` | Abstract interface for iterating UDI during SST reads | `include/rocksdb/user_defined_index.h:130` |
| `UserDefinedIndexReader` | Abstract interface for reading UDI and creating iterators | `include/rocksdb/user_defined_index.h:194` |
| `UserDefinedIndexFactory` | Abstract factory for creating builders and readers | `include/rocksdb/user_defined_index.h:213` |
| `UserDefinedIndexBuilderWrapper` | Forwards AddIndexEntry/OnKeyAdded to both internal and UDI builders | `table/block_based/user_defined_index_wrapper.h:27` |
| `UserDefinedIndexIteratorWrapper` | Adapts UserDefinedIndexIterator to InternalIteratorBase interface | `table/block_based/user_defined_index_wrapper.h:224` |
| `UserDefinedIndexReaderWrapper` | Routes NewIterator calls to either internal or UDI reader | `table/block_based/user_defined_index_wrapper.h:353` |

## Interface Design

### Building a UDI

During SST file construction, RocksDB calls the `UserDefinedIndexBuilder` interface in lockstep with the internal index builder:

#### AddIndexEntry

Called at each data block boundary to record the block separator and handle.

```cpp
virtual Slice AddIndexEntry(
    const Slice& last_key_in_current_block,   // Last user key in block N
    const Slice* first_key_in_next_block,     // First user key in block N+1 (or nullptr)
    const BlockHandle& block_handle,          // Offset/size of block N
    std::string* separator_scratch,           // Scratch buffer for separator
    const IndexEntryContext& context          // Sequence numbers for boundary keys
) = 0;
```

**⚠️ INVARIANT:** Keys passed to `AddIndexEntry` are **user keys** (without the 8-byte internal key trailer). The wrapper extracts user keys from internal keys before forwarding.

**⚠️ INVARIANT:** The separator returned must satisfy:
```
last_key_in_current_block <= separator < first_key_in_next_block
```
in user key order (ignoring sequence numbers).

**Sequence Number Handling:** The `IndexEntryContext` provides sequence numbers for block boundary keys. This is critical when the **same user key** spans a data block boundary:

```
Block N ends:   "foo"|seq=100
Block N+1 starts: "foo"|seq=50
```

Without sequence numbers, a UDI cannot distinguish which block to return for `Seek("foo")` with a specific snapshot. Implementations that need correct snapshot isolation must use the provided sequence numbers (see Trie Index example below).

**Call Order:** `AddIndexEntry` is called **before** `OnKeyAdded` for `first_key_in_next_block`.

Implementation in wrapper (`table/block_based/user_defined_index_wrapper.h:42-82`):
```cpp
Slice AddIndexEntry(...) override {
  // 1. Parse internal keys to extract user keys and sequence numbers
  ParsedInternalKey pkey_last, pkey_first;
  ParseInternalKey(last_key_in_current_block, &pkey_last, ...);
  if (first_key_in_next_block) {
    ParseInternalKey(*first_key_in_next_block, &pkey_first, ...);
  }

  // 2. Forward user keys + seqnos to UDI builder
  UserDefinedIndexBuilder::IndexEntryContext ctx;
  ctx.last_key_seq = pkey_last.sequence;
  ctx.first_key_seq = first_key_in_next_block ? pkey_first.sequence : 0;
  user_defined_index_builder_->AddIndexEntry(
      pkey_last.user_key,
      first_key_in_next_block ? &pkey_first.user_key : nullptr,
      handle, separator_scratch, ctx);

  // 3. Forward to internal index builder
  return internal_index_builder_->AddIndexEntry(...);
}
```

#### OnKeyAdded

Called for every key-value pair added to the SST file, **except range tombstones** (`kTypeRangeDeletion`). Range tombstones are written to a separate range-deletion block and are never forwarded to UDI builders (`table/block_based/block_based_table_builder.cc:1531-1587`). UDI builders may override this to collect per-key information (e.g., for secondary indexes). Builders that only use separator keys from `AddIndexEntry()` (e.g., trie-based indexes) can leave this as a no-op.

```cpp
virtual void OnKeyAdded(
    const Slice& key,       // User key (without sequence number or type suffix)
    ValueType type,         // kValue (Put), kDelete, kMerge, or kOther
    const Slice& value      // Associated value (may be empty for deletions)
) {}
```

**⚠️ INVARIANT:** In SST files produced by flush or compaction, there may be **multiple entries for the same user key** with different sequence numbers (when snapshots are active). UDI builders that use `OnKeyAdded()` must handle this.

**Thread Safety:** For a given builder instance, `OnKeyAdded()` and `AddIndexEntry()` are always called from a single thread. Builders do **not** need internal synchronization (`include/rocksdb/user_defined_index.h:114-116`).

**Value Type Mapping:**

| Internal ValueType | UDI ValueType | Notes |
|-------------------|---------------|-------|
| `kTypeValue` | `kValue` | Full user value |
| `kTypeValuePreferredSeqno` | `kValue` | Preferred seqno suffix stripped before forwarding |
| `kTypeDeletion`, `kTypeSingleDeletion`, `kTypeDeletionWithTimestamp` | `kDelete` | Value typically empty |
| `kTypeMerge` | `kMerge` | Merge operand |
| `kTypeBlobIndex`, `kTypeWideColumnEntity` | `kOther` | Value format is type-specific, may not be actual user data |

See mapping logic at `table/block_based/user_defined_index_wrapper.h:193-215`.

#### Finish

Finalize the index and return the serialized contents.

```cpp
virtual Status Finish(Slice* index_contents) = 0;
```

**⚠️ INVARIANT:** The memory backing `index_contents` must remain valid until the builder is destructed. RocksDB holds a Slice reference, not a copy.

**Storage:** The serialized UDI is stored as a meta block with name:
```
"rocksdb.user_defined_index.<factory_name>"
```
Block type: `BlockType::kUserDefinedIndex` (`table/block_based/block_type.h:30`)

### Reading a UDI

At table open time, the `UserDefinedIndexFactory` creates a reader from the serialized UDI block. The factory has two API layers:

**Deprecated pure-virtual API** (must be overridden by all implementations):
```cpp
virtual UserDefinedIndexBuilder* NewBuilder() const = 0;
virtual std::unique_ptr<UserDefinedIndexReader> NewReader(Slice& index_block) const = 0;
```

**Option-aware virtual API** (default implementations delegate to the deprecated API):
```cpp
virtual Status NewBuilder(
    const UserDefinedIndexOption& option,
    std::unique_ptr<UserDefinedIndexBuilder>& builder) const {
  builder.reset(NewBuilder());  // Delegates to deprecated pure-virtual
  return Status::OK();
}

virtual Status NewReader(
    const UserDefinedIndexOption& option,
    Slice& index_block,
    std::unique_ptr<UserDefinedIndexReader>& reader) const {
  reader = NewReader(index_block);  // Delegates to deprecated pure-virtual
  return Status::OK();
}
```

The block-based table builder and reader always call the option-aware overloads (`include/rocksdb/user_defined_index.h:232-244`). Implementations that need the comparator (e.g., `TrieIndexFactory`) override the option-aware methods and provide stubs for the deprecated pure virtuals.

The reader creates iterators for scans:

```cpp
virtual std::unique_ptr<UserDefinedIndexIterator> NewIterator(
    const ReadOptions& read_options
) = 0;
```

**⚠️ INVARIANT:** The `index_block` Slice is typically backed by a block cache entry. It remains valid for the lifetime of the `UserDefinedIndexReader` (tied to table reader lifetime or cache eviction).

### Iterating a UDI

The `UserDefinedIndexIterator` interface mirrors RocksDB's iterator pattern:

#### Prepare

Prefetch and buffer results for a batch of scans.

```cpp
virtual void Prepare(const ScanOptions scan_opts[], size_t num_opts) = 0;
```

#### SeekToFirstAndGetResult

Position at the very first index entry.

```cpp
virtual Status SeekToFirstAndGetResult(IterateResult* result) {
  // Default: delegates to SeekAndGetResult(Slice(), ...)
  // Works for BytewiseComparator (empty string is smallest)
  // Override if you use a different comparator or can optimize
}
```

#### SeekAndGetResult

Seek to the first index entry >= target.

```cpp
virtual Status SeekAndGetResult(
    const Slice& target,          // User key (without sequence number)
    IterateResult* result,        // Output: index key + bound check result
    const SeekContext& context    // Provides target_seq for snapshot isolation
) = 0;
```

**⚠️ INVARIANT:** The target is a **user key**. The wrapper extracts the user key from the internal key before forwarding.

**Sequence Number Handling:** When the same user key spans multiple data blocks with different sequence numbers, the `SeekContext::target_seq` allows the UDI to distinguish which block to return. Without this, snapshot isolation breaks.

**Bound Checking:** The UDI must set `result->bound_check_result`:

- `kInbound`: Data block is definitely within bounds
- `kOutOfBound`: No block satisfies the target (causes iteration to stop)
- `kUnknown`: Either partially within bounds (block must be scanned to determine), or no matching leaf exists in this SST (EOF). The `UserDefinedIndexIteratorWrapper` treats any non-`kInbound` result as invalid (`table/block_based/user_defined_index_wrapper.h:235-243, 266, 278`). Note: the trie iterator returns `kUnknown` (not `kOutOfBound`) when exhausted, because exhausting one SST's trie says nothing about whether the next SST on the level has in-bound keys.

**⚠️ INVARIANT:** A UDI can only return `kOutOfBound` if it can **prove** the block is out of bounds. If a limit key is specified in `ScanOptions`, an implementation that does not store the first key in each block cannot reliably determine bounds. It must compare against the **previous** index key (not the current separator, which is an upper bound).

See contract details at `include/rocksdb/user_defined_index.h:158-176`.

#### NextAndGetResult

Advance to the next index entry.

```cpp
virtual Status NextAndGetResult(IterateResult* result) = 0;
```

#### value

Return the BlockHandle of the current block.

```cpp
virtual UserDefinedIndexBuilder::BlockHandle value() = 0;
```

## Integration with Block-Based Table

### Table Building

The UDI integration is controlled by `BlockBasedTableOptions::user_defined_index_factory`.

**Wrapper Creation** (`table/block_based/block_based_table_builder.cc:1266-1290`):

```cpp
if (table_options.user_defined_index_factory != nullptr) {
  // UDI disables parallel compression (see Limitations section)
  if (tbo.moptions.compression_opts.parallel_threads > 1 || ...) {
    return Status::InvalidArgument("user_defined_index_factory not supported "
                                    "with parallel compression");
  }

  std::unique_ptr<UserDefinedIndexBuilder> user_defined_index_builder;
  UserDefinedIndexOption udi_options;
  udi_options.comparator = internal_comparator.user_comparator();
  auto s = table_options.user_defined_index_factory->NewBuilder(
      udi_options, user_defined_index_builder);

  if (s.ok() && user_defined_index_builder != nullptr) {
    index_builder = std::make_unique<UserDefinedIndexBuilderWrapper>(
        std::string(table_options.user_defined_index_factory->Name()),
        std::move(index_builder),           // Internal index builder
        std::move(user_defined_index_builder),
        &internal_comparator, ts_sz, persist_user_defined_timestamps);
  }
}
```

**Write Path Flow:**

1. **Add key-value to data block:** `BlockBasedTableBuilder::Add(key, value)`
2. **OnKeyAdded forwarding:** Wrapper calls both:
   - `internal_index_builder_->OnKeyAdded(internal_key, value)`
   - `user_defined_index_builder_->OnKeyAdded(user_key, mapped_type, value)`
3. **Data block flush:** When block size threshold reached
4. **AddIndexEntry forwarding:** Wrapper calls both:
   - `user_defined_index_builder_->AddIndexEntry(user_keys, handle, ctx)`
   - `internal_index_builder_->AddIndexEntry(internal_keys, handle, ...)`
5. **Finish:** Both indexes finalized, UDI serialized to meta block

**Error Handling:** Since `AddIndexEntry()` cannot return a Status, errors are cached in the wrapper and returned during `Finish()` (`table/block_based/user_defined_index_wrapper.h:52-53, 152-154`).

### Table Reading

**UDI Block Loading** (`table/block_based/block_based_table_reader.cc:1315-1376`):

```cpp
if (table_options.user_defined_index_factory != nullptr) {
  std::string udi_name(table_options.user_defined_index_factory->Name());
  BlockHandle udi_block_handle;

  // 1. Locate UDI meta block by name
  s = FindMetaBlock(meta_iter, kUserDefinedIndexPrefix + udi_name,
                    &udi_block_handle);

  // 2. Handle missing UDI block
  if (!s.ok()) {
    RecordTick(statistics, SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT);
    if (table_options.fail_if_no_udi_on_open) {
      // Hard error if fail_if_no_udi_on_open is set
      ROCKS_LOG_ERROR(logger, "Failed to find the the UDI block %s in file %s; %s",
                      udi_name.c_str(), file_name.c_str(), s.ToString().c_str());
      return Status::Corruption(s.ToString(), file_name);
    } else {
      // Soft warning otherwise (allows reading old SST files)
      ROCKS_LOG_WARN(logger, "Failed to find the the UDI block %s in file %s; %s",
                     udi_name.c_str(), file_name.c_str(), s.ToString().c_str());
      s = Status::OK();
    }
  }

  // 3. Read and cache UDI block (if found and non-empty)
  if (udi_block_handle.size() > 0) {
    s = RetrieveBlock(prefetch_buffer, ro, udi_block_handle,
                      rep_->decompressor.get(), &rep_->udi_block,
                      /*get_context=*/nullptr, lookup_context,
                      /*for_compaction=*/false, use_cache,
                      /*async_read=*/false,
                      /*use_block_cache_for_lookup=*/false);

    // 4. Create UDI reader
    std::unique_ptr<UserDefinedIndexReader> udi_reader;
    UserDefinedIndexOption udi_option;
    udi_option.comparator = internal_comparator.user_comparator();
    s = table_options.user_defined_index_factory->NewReader(
        udi_option, rep_->udi_block.GetValue()->data, udi_reader);

    // 5. Wrap internal index reader
    index_reader = std::make_unique<UserDefinedIndexReaderWrapper>(
        udi_name, std::move(index_reader), std::move(udi_reader));
  }
}
```

**Read Path Flow:**

1. **Iterator creation:** `db_iter = NewIterator(read_options)`
2. **Index selection:** If `read_options.table_index_factory` is set, use UDI; otherwise use internal index
3. **Seek/Next:** UDI iterator navigates to appropriate data block
4. **Data block fetch:** BlockHandle returned by UDI used to read data block
5. **Data block iteration:** Standard iteration within data block

**Iterator Routing** (`table/block_based/user_defined_index_wrapper.h:363-384`):

```cpp
InternalIteratorBase<IndexValue>* NewIterator(
    const ReadOptions& read_options, ...) override {
  // Use UDI if read_options.table_index_factory matches this UDI
  if (read_options.table_index_factory &&
      name_ == read_options.table_index_factory->Name()) {
    auto udi_iter = udi_reader_->NewIterator(read_options);
    return new UserDefinedIndexIteratorWrapper(std::move(udi_iter));
  }
  // Otherwise use internal index
  return reader_->NewIterator(read_options, ...);
}
```

### Empty and Nullable UDI Cases

The UDI integration handles several edge cases around empty or absent UDI:

**Builder side:** `NewBuilder(option, builder)` may return `Status::OK()` with `builder == nullptr`. This simply leaves the internal index builder unwrapped — no UDI is built for this SST file (`table/block_based/block_based_table_builder.cc:1283-1288`).

**Reader side:** `NewReader(option, ..., reader)` returning `Status::OK()` with `reader == nullptr` is treated as corruption — the UDI block exists but the factory failed to create a reader, which indicates a programming error (`table/block_based/block_based_table_reader.cc:1364-1372`).

**Zero-size UDI block:** A UDI block handle with `size() == 0` is treated as "effectively no UDI" and skipped, even when `fail_if_no_udi_on_open=true` is set (`table/block_based/block_based_table_reader.cc:1343-1345`). This case is exercised by the `IngestEmptyUDI` test (`table/table_test.cc:8827`).

## UDI and Compaction

### Index Rebuilding

During compaction, UDI indexes are **rebuilt from scratch** for output SST files:

1. **Input SSTs:** Each input SST may have a UDI (if it was built with `user_defined_index_factory` configured)
2. **Compaction reads:** Use internal index (not UDI) to read input blocks
3. **Output SST:** If `user_defined_index_factory` is configured in current options, a new UDI is built for the output SST
4. **Input UDIs discarded:** Old UDI blocks are discarded when input SSTs are deleted

**⚠️ INVARIANT:** UDI indexes are **not merged or copied** between SST files. Each SST's UDI is an independent encoding of its data block boundaries.

### Configuration Evolution

- **Adding UDI:** Can add `user_defined_index_factory` to options. New flushes/compactions will build UDI. Old SST files without UDI can still be read (UDI reader wrapper handles missing UDI gracefully).
- **Removing UDI:** Can remove `user_defined_index_factory` from options. New flushes/compactions will not build UDI. Old SST files with UDI can still be read (UDI block is ignored).
- **Changing UDI type:** Must ensure old UDI type can still be read during transition, or force full compaction to rebuild all UDI blocks with new type.

## UDI and Write Path

### Memtable to L0 (Flush)

When flushing memtable to L0:

1. **TableBuilder creation:** `BlockBasedTableBuilder` created with `user_defined_index_factory` (if configured)
2. **Key iteration:** MemTable iterator yields keys in internal key order
3. **OnKeyAdded:** For each key, wrapper forwards (user_key, type, value) to UDI builder
4. **Data block flush:** When block size threshold reached, `AddIndexEntry` called
5. **SST finalization:** Both internal index and UDI finalized and written to SST file

**⚠️ INVARIANT:** During flush, keys are written in **internal key order** (user key ascending, sequence number descending). The same user key may appear multiple times if snapshots are active.

### L0 to L1+ (Compaction)

Compaction merges multiple SST files:

1. **Input iteration:** Merging iterator over input SST files (using **internal index**)
2. **Key deduplication:** Compaction drops old versions and tombstones based on snapshots
3. **Output building:** New SST file(s) created with UDI (if configured)
4. **Separator keys:** Block boundaries may differ from input SST files (due to merging, deduplication, and block size thresholds)

**⚠️ INVARIANT:** Output SST's UDI reflects the **post-compaction** key distribution, not the input SST's key distribution.

## UDI Storage Format

### Meta Block Naming

UDI blocks are stored as meta blocks with a standardized naming scheme:

```
"rocksdb.user_defined_index.<factory_name>"
```

Where `<factory_name>` is `UserDefinedIndexFactory::Name()`.

**Example:** For `TrieIndexFactory`, the meta block name is:
```
"rocksdb.user_defined_index.trie_index"
```

See prefix definition at `include/rocksdb/user_defined_index.h:24-25`.

### Block Type

UDI blocks use `BlockType::kUserDefinedIndex` (enum value 10, `table/block_based/block_type.h:30`).

### Serialization

The serialization format is **entirely controlled by the UDI implementation**. RocksDB treats it as an opaque byte array:

1. **Builder:** `UserDefinedIndexBuilder::Finish(Slice* index_contents)` returns a Slice pointing to serialized data
2. **Storage:** RocksDB writes the Slice contents to a meta block (with checksum, per standard block format)
3. **Reader:** `UserDefinedIndexFactory::NewReader(Slice& index_block)` receives the same byte array

**⚠️ INVARIANT:** The UDI implementation is responsible for:
- **Versioning:** Including version information in serialized format for forward/backward compatibility
- **Endianness:** Handling platform endianness if needed
- **Checksums:** Meta block checksum covers the UDI data, but implementations may add additional internal checksums

### Example: Trie Index Serialization

The trie UDI (`utilities/trie_index/`) uses a custom LOUDS-encoded trie format:

```
Trie Index Block:

Fixed Header (56 bytes):
  - Magic (4 bytes, uint32_t)
  - Format Version (4 bytes, uint32_t)
  - num_keys (8 bytes, uint64_t)
  - cutoff_level (4 bytes, uint32_t)
  - max_depth (4 bytes, uint32_t)
  - dense_leaf_count (8 bytes, uint64_t)
  - dense_node_count (8 bytes, uint64_t)
  - dense_child_count (8 bytes, uint64_t)
  - Flags (4 bytes, uint32_t): has_seqno_encoding flag
  - Reserved padding (4 bytes, for 8-byte alignment)

LOUDS Trie:
  - Dense section: bitvectors for labels, has_child,
    is_prefix_key, suffix labels/suffixes
  - Sparse section: bitvectors for labels, has_child,
    is_prefix_key, chain suffixes

Block Handles (fixed-width uint32_t arrays):
  - offsets[num_keys] (uint32_t, padded to 8-byte)
  - sizes[num_keys] (uint32_t, padded to 8-byte)

Sequence Number Side Tables (if has_seqno_encoding flag):
  - num_overflow_blocks (uint32_t + 4-byte padding)
  - leaf_seqnos[num_keys] (uint64_t array)
  - leaf_block_counts[num_keys] (uint32_t, padded to 8-byte)
  - overflow_offsets[num_overflow] (uint32_t, padded)
  - overflow_sizes[num_overflow] (uint32_t, padded)
  - overflow_seqnos[num_overflow] (uint64_t array)
  Note: overflow_base_ is computed during deserialization
  as a prefix sum of (block_count-1), not serialized.
```

See serialization at `utilities/trie_index/louds_trie.cc:489-509` (header), `utilities/trie_index/louds_trie.cc:876-923` (handles), `utilities/trie_index/louds_trie.cc:924-` (seqno side-table).
See deserialization at `utilities/trie_index/louds_trie.cc:1018-1089` (header), `utilities/trie_index/louds_trie.cc:1483-1494` (overflow_base_ computation).

## Configuration

### Table Options

**BlockBasedTableOptions::user_defined_index_factory**

```cpp
std::shared_ptr<UserDefinedIndexFactory> user_defined_index_factory = nullptr;
```

Location: `include/rocksdb/table.h:538`

When set, RocksDB builds a UDI alongside the internal index during SST file construction. The UDI is stored as a meta block in the SST file (before the metaindex block and footer).

**Parallel Compression Restriction:** UDI is incompatible with parallel compression. If `user_defined_index_factory` is set and `compression_opts.parallel_threads > 1` (or `bottommost_compression_opts.parallel_threads > 1`), option validation and table builder initialization fail with:
```
Status::InvalidArgument("user_defined_index_factory not supported with parallel compression")
```

Reason: The wrapper does not yet implement the `PrepareIndexEntry`/`FinishIndexEntry` split required for parallel compression (`table/block_based/user_defined_index_wrapper.h:84-107`).

**BlockBasedTableOptions::fail_if_no_udi_on_open**

```cpp
bool fail_if_no_udi_on_open = false;
```

Location: `include/rocksdb/table.h:542-544`

Controls error handling when a UDI block is missing from an SST file:

- `false` (default): Log a warning and continue (allows reading old SST files built without UDI)
- `true`: Return `Status::Corruption` and fail table open (enforces UDI presence)

Statistic: `SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT` is incremented on missing UDI block (`include/rocksdb/statistics.h:555`).

### Read Options

**ReadOptions::table_index_factory**

```cpp
const UserDefinedIndexFactory* table_index_factory = nullptr;
```

Location: `include/rocksdb/options.h:2289`

When set, RocksDB uses the UDI (if present in the SST file) instead of the internal index for iteration. The factory name must match the UDI block name in the SST file; otherwise, an error iterator is returned.

**⚠️ PREREQUISITE:** `ReadOptions::table_index_factory` alone is not sufficient. The SST file must have been opened with a matching `BlockBasedTableOptions::user_defined_index_factory` so that `BlockBasedTableReader` can locate the UDI meta block, create the `UserDefinedIndexReader`, and wrap the internal index reader (`table/block_based/block_based_table_reader.cc:1315-1376`). If the open-time factory is absent, `ReadOptions::table_index_factory` is effectively ignored because there is no UDI reader wrapper to consult.

**⚠️ INVARIANT:** Forward scans (SeekToFirst, Seek, Next) and point lookups (Get) are supported. Reverse operations (SeekToLast, SeekForPrev, Prev) return `Status::NotSupported` when using UDI.

### Example: Trie Index Configuration

```cpp
// At table creation time (during DB open or ColumnFamily creation)
#include "utilities/trie_index/trie_index_factory.h"

auto trie_factory = std::make_shared<trie_index::TrieIndexFactory>();
BlockBasedTableOptions table_options;
table_options.user_defined_index_factory = trie_factory;

Options db_options;
db_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

DB* db;
DB::Open(db_options, "/tmp/testdb", &db);

// At read time (per-query basis)
ReadOptions read_options;
read_options.table_index_factory = trie_factory.get();
auto iter = db->NewIterator(read_options);

iter->SeekToFirst();
while (iter->Valid()) {
  // Iteration uses trie index for block navigation
  process(iter->key(), iter->value());
  iter->Next();
}

delete iter;
delete db;
```

## Operation Type Support

### Supported Value Types

UDI builders receive all value-type keys via `OnKeyAdded` (range tombstones are excluded — they go to the range-deletion block and are never forwarded to UDI):

| Operation | Internal Type | UDI ValueType | Notes |
|-----------|---------------|---------------|-------|
| Put | `kTypeValue` | `kValue` | Full user value |
| Put (preferred seqno) | `kTypeValuePreferredSeqno` | `kValue` | Seqno suffix stripped before forwarding |
| Delete | `kTypeDeletion` | `kDelete` | Value typically empty |
| SingleDelete | `kTypeSingleDeletion` | `kDelete` | Value typically empty |
| DeleteWithTimestamp | `kTypeDeletionWithTimestamp` | `kDelete` | Value typically empty |
| Merge | `kTypeMerge` | `kMerge` | Merge operand value |
| BlobDB | `kTypeBlobIndex` | `kOther` | Value is blob reference, not user data |
| Wide Column | `kTypeWideColumnEntity` | `kOther` | Value is serialized entity, not simple value |

See mapping at `table/block_based/user_defined_index_wrapper.h:193-215`.

### Iteration Operations

**Supported:**
- `SeekToFirst()`: Position at first index entry
- `Seek(target)`: Seek to first entry >= target
- `Next()`: Advance to next entry
- `Get(key)`: Point lookup (uses Seek internally)
- `MultiGet`: Uses Seek internally for each key
- `Prepare(scan_opts)`: Prefetch for batched scans

**Not Supported:**
- `SeekToLast()`: Returns `Status::NotSupported` (`table/block_based/user_defined_index_wrapper.h:247-250`)
- `SeekForPrev(target)`: Returns `Status::NotSupported` (`table/block_based/user_defined_index_wrapper.h:303-306`)
- `Prev()`: Returns `Status::NotSupported` (`table/block_based/user_defined_index_wrapper.h:308-311`)

## Limitations and Constraints

### 1. Parallel Compression Incompatibility

**Limitation:** When `user_defined_index_factory` is set, parallel compression (`compression_opts.parallel_threads > 1` or `bottommost_compression_opts.parallel_threads > 1`) is **rejected** with `Status::InvalidArgument`. This validation happens in two places:

1. **Option validation:** `BlockBasedTableFactory::ValidateOptions()` returns `Status::InvalidArgument` if parallel threads > 1 with UDI configured (`table/block_based/block_based_table_factory.cc:760-764`).
2. **Builder initialization:** The `Rep` constructor also checks and sets a failure status (`table/block_based/block_based_table_builder.cc:1269-1274`).

Note: Internally, the builder's `compression_parallel_threads` member is sanitized to 1 when UDI is present (`table/block_based/block_based_table_builder.cc:1082-1087`), but this is a defense-in-depth measure — callers still receive the `InvalidArgument` error from validation.

**Reason:** The `UserDefinedIndexBuilderWrapper` does not implement the `PrepareIndexEntry()`/`FinishIndexEntry()` split required for parallel compression. The stubs assert-fail if called (`table/block_based/user_defined_index_wrapper.h:89-107`).

**Workaround:** None currently. This is a performance tradeoff for UDI users.

**Code Reference:** Option sanitization at `table/block_based/block_based_table_builder.cc:1085-1087, 1269-1274`.

### 2. Forward-Only Iteration

**Limitation:** Reverse iteration operations (SeekToLast, SeekForPrev, Prev) are not supported.

**Reason:** Many UDI implementations (e.g., tries) are optimized for forward traversal. Supporting bidirectional iteration would require additional index structures.

**Workaround:** For reverse iteration, do not set `ReadOptions::table_index_factory` (uses internal index).

**Code Reference:** Error returns at `table/block_based/user_defined_index_wrapper.h:247-250, 303-311`.

### 3. No Partitioned UDI Block Support

**Limitation:** The UDI block itself is always a **monolithic (single) meta block**. It cannot be partitioned into multiple sub-blocks with a top-level index.

**Clarification:** This limitation applies only to the UDI block, not to the internal index that UDI wraps. UDI works on top of a partitioned / two-level internal index (`BlockBasedTableOptions::kTwoLevelIndexSearch`). The code supports this combination (`table/block_based/block_based_table_builder.cc:1251-1264`), and tests exercise it (`table/table_test.cc:8109-8112`).

**Impact:** For SST files with millions of keys, the UDI block is loaded entirely into memory (or block cache), which may be prohibitive even if the underlying internal index is partitioned.

**Workaround:** None currently for partitioning the UDI block itself.

### 4. Same-User-Key Boundaries Complexity

**Limitation:** When the same user key spans a data block boundary (due to snapshots keeping multiple versions), UDI implementations must handle sequence number encoding to maintain correctness.

**Example Scenario:**
```
Block N ends:   Put("foo", seq=100, "v1")
Block N+1 starts: Put("foo", seq=50,  "v2")
```

A snapshot at seq=75 reading "foo" should return "v1" (from Block N). Without sequence numbers, the UDI cannot distinguish which block to return.

**Solution:** The `IndexEntryContext` provides sequence numbers. Implementations can:
1. **Ignore seqnos** (simple, but breaks snapshot isolation for same-user-key boundaries)
2. **Encode seqnos selectively** (trie index approach: switch to all-seqno mode when any boundary detected)
3. **Always encode seqnos** (simple but higher space overhead)

**Code Reference:** Trie index handling at `utilities/trie_index/trie_index_factory.h:87-117`.

### 5. Configuration Evolution Challenges

**Limitation:** Changing UDI factory type requires careful migration.

**Scenario:** Database has SST files built with `TrieIndexFactory`. Operator wants to switch to `HashIndexFactory`.

**Challenge:** Old SST files have trie UDI blocks (name: `rocksdb.user_defined_index.trie_index`). New factory expects hash UDI blocks (name: `rocksdb.user_defined_index.hash_index`). Mismatch causes UDI load failures.

**Migration Strategies:**
1. **Soft migration:** Set `fail_if_no_udi_on_open=false`. New flushes/compactions build new UDI type. Old SST files fall back to internal index. Full compaction eventually rebuilds all UDI blocks.
2. **Hard migration:** Set `fail_if_no_udi_on_open=true` only after full compaction completes.
3. **Hybrid approach:** Support multiple UDI factories simultaneously (requires code changes to wrapper logic).

### 6. Comparator Support

**Framework:** The UDI framework supports custom comparators. The comparator is passed via `UserDefinedIndexOption` to both builder and reader (`include/rocksdb/user_defined_index.h:208-210`).

**Trie Implementation:** The bundled `TrieIndexFactory` still **requires `BytewiseComparator()`** and rejects any other comparator in both `NewBuilder()` and `NewReader()` with `Status::NotSupported` (`utilities/trie_index/trie_index_factory.cc:525-556`). This is because the trie traverses keys byte-by-byte in lexicographic order; non-bytewise comparators would produce separator keys in a different order than the trie's byte-level traversal.

**Breaking Change:** HISTORY.md notes at 10.8.0: "Allow UDIs with a non BytewiseComparator" (`HISTORY.md:85`) — this refers to the framework change, not the trie implementation. Custom UDI implementations can now use arbitrary comparators.

### 7. Memory Overhead

**Limitation:** UDI blocks are loaded into memory (either pinned or cached) for the lifetime of table reader access.

**Impact:** For workloads with many open SST files, UDI blocks consume additional memory beyond internal index blocks.

**Mitigation:** UDI blocks can be cached in the block cache (controlled by `use_cache` parameter at table open). Cache eviction policy applies.

**Metric:** `UserDefinedIndexReader::ApproximateMemoryUsage()` reports UDI memory usage for monitoring (`include/rocksdb/user_defined_index.h:204`).

## Performance Considerations

### Space Efficiency

UDI enables space-optimized index representations. For example, the trie index exploits common key prefixes:

**Binary Search Index (Internal):**
```
Separator keys: ["apple", "application", "apply", "banana", "bandana"]
Space: ~40 bytes (assuming 8-byte average key length)
```

**Trie Index (UDI):**
```
Trie structure:
    a
      p
        p (le) -> block_0
          l (ication) -> block_1
        l (y) -> block_2
      b
        a (nana) -> block_3
        a (ndana) -> block_4

Space: ~25 bytes (compressed edge labels + LOUDS bit vector)
```

**Tradeoff:** Trie UDI can save significant space for prefix-heavy key sets (e.g., ~38% in external benchmarks from the SuRF paper), at the cost of more complex seek logic. Actual savings depend on workload and key distribution.

### Seek Performance

- **Binary search index:** O(log N) comparisons, predictable performance
- **Trie index:** O(M) where M is key length, but with better cache locality for prefix-similar keys

Benchmark results from external research (SuRF paper, SIGMOD 2018) suggest tries can achieve comparable or better seek performance while using less space, though results vary by workload. These claims have not been independently validated against the current RocksDB trie implementation.

### Iteration Performance

- **Sequential iteration:** Trie index may have **better** cache locality (prefix compression keeps related keys close)
- **Random seeks:** Binary search index may have **better** worst-case latency (fewer pointer chases)

**Recommendation:** Benchmark with your workload to determine if UDI improves performance.

## Example Implementation: Trie Index

The trie index (`utilities/trie_index/`) demonstrates a complete UDI implementation:

### Key Features

1. **LOUDS-encoded trie:** Space-efficient trie representation using Level-Order Unary Degree Sequence
2. **Prefix compression:** Exploits common key prefixes to reduce index size
3. **Sequence number handling:** Supports same-user-key boundaries via seqno side-tables
4. **Fast seek:** O(key_length) seek time

### Implementation Highlights

**Builder** (`utilities/trie_index/trie_index_factory.cc:139-214`):

```cpp
Status Finish(Slice* index_contents) override {
  // 1. Use seqno side-table when any same-user-key block boundary was detected.
  bool use_seqno = must_use_separator_with_seq_;
  trie_builder_.SetHasSeqnoEncoding(use_seqno);

  if (use_seqno) {
    // Feed de-duplicated separators with seqno side-table metadata.
    // Consecutive identical separators form a "run" — only the first
    // occurrence goes into the trie (as the primary block). Remaining
    // blocks in the run are stored as overflow blocks.
    size_t i = 0;
    while (i < buffered_entries_.size()) {
      const auto& entry = buffered_entries_[i];

      // Count consecutive entries sharing this separator key.
      size_t run_end = i + 1;
      while (run_end < buffered_entries_.size() &&
             buffered_entries_[run_end].separator_key == entry.separator_key) {
        run_end++;
      }
      uint32_t block_count = static_cast<uint32_t>(run_end - i);
      uint64_t seqno = (entry.seqno == kMaxSequenceNumber) ? 0 : entry.seqno;

      // Add primary block for this separator.
      trie_builder_.AddKeyWithSeqno(Slice(entry.separator_key), entry.handle,
                                    seqno, block_count);

      // Add overflow blocks (2nd, 3rd, ... in the run).
      for (size_t j = i + 1; j < run_end; j++) {
        trie_builder_.AddOverflowBlock(buffered_entries_[j].handle,
                                       buffered_entries_[j].seqno);
      }
      i = run_end;
    }
  } else {
    // Common case: no same-user-key boundaries, add separators directly.
    for (const auto& entry : buffered_entries_) {
      trie_builder_.AddKey(Slice(entry.separator_key), entry.handle);
    }
  }

  // 2. Finalize trie and get serialized data.
  trie_builder_.Finish();
  *index_contents = trie_builder_.GetSerializedData();
  return Status::OK();
}
```

**Iterator** (`utilities/trie_index/trie_index_factory.cc:276-401`):

```cpp
Status SeekAndGetResult(const Slice& target, IterateResult* result,
                        const SeekContext& context) override {
  // 1. Seek trie with user key only — trie stores user-key separators.
  if (!iter_.Seek(target)) {
    // No leaf has key >= target: past all blocks in this SST.
    // Return kUnknown (not kOutOfBound) — exhausting this SST says nothing
    // about upper bound; next SST on the level may still have in-bound keys.
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->key = Slice();
    return Status::OK();
  }

  result->key = iter_.Key();

  // 2. Post-seek correction for seqno side-table (if has_seqno_encoding_)
  if (has_seqno_encoding_ && iter_.Valid()) {
    uint64_t leaf_idx = iter_.LeafIndex();
    uint64_t leaf_seqno = trie_->GetLeafSeqno(leaf_idx);

    if (leaf_seqno != 0 && context.target_seq < leaf_seqno) {
      // Target's internal key is AFTER the separator. Advance through
      // overflow blocks to find the right one.
      uint32_t block_count = trie_->GetLeafBlockCount(leaf_idx);
      uint32_t base = trie_->GetOverflowBase(leaf_idx);

      bool found = false;
      for (uint32_t oi = 0; oi < block_count - 1; oi++) {
        uint64_t ov_seqno = trie_->GetOverflowSeqno(base + oi);
        if (ov_seqno == 0 || context.target_seq >= ov_seqno) {
          overflow_run_index_ = oi + 1;
          overflow_run_size_ = block_count;
          overflow_base_idx_ = base;
          found = true;
          break;
        }
      }

      if (!found) {
        // target_seq below all seqnos in this run — advance to next leaf.
        if (!iter_.Next()) {
          result->bound_check_result = IterBoundCheck::kUnknown;
          result->key = Slice();
          return Status::OK();
        }
        result->key = iter_.Key();
        // Reset overflow state for the new leaf...
      }
    } else {
      // Right block (common path). Set overflow state for subsequent Next().
      overflow_run_size_ = trie_->GetLeafBlockCount(leaf_idx);
      overflow_base_idx_ = trie_->GetOverflowBase(leaf_idx);
    }
  }

  // 3. Check bounds and set result.
  result->bound_check_result = CheckBounds(target);
  return Status::OK();
}
```

### Usage Example

See configuration example in Configuration section above.

---

## References

### Source Files

| File | Description |
|------|-------------|
| `include/rocksdb/user_defined_index.h` | Public UDI API: Builder, Iterator, Reader, Factory interfaces |
| `table/block_based/user_defined_index_wrapper.h` | Builder/Iterator/Reader wrappers for integration |
| `table/block_based/block_based_table_builder.cc` | UDI building during SST writes |
| `table/block_based/block_based_table_reader.cc` | UDI loading during SST reads |
| `table/block_based/block_type.h` | BlockType::kUserDefinedIndex definition |
| `utilities/trie_index/trie_index_factory.{h,cc}` | Example trie-based UDI implementation |
| `utilities/trie_index/louds_trie.{h,cc}` | LOUDS trie encoding/decoding |

### Related Documentation

- **Block-Based Table Format:** `docs/components/sst_table_format.md` - UDI is stored as a meta block alongside filters and properties
- **Version Management:** `docs/components/version_management.md` - UDI indexes are rebuilt during compaction
- **Read Path:** `docs/components/read_flow.md` - UDI iterators integrate with DBIter and Get operations
- **Write Path:** `docs/components/write_flow.md` - UDI builders are invoked during flush and compaction

### External Resources

- **SuRF Paper:** "SuRF: Practical Range Query Filtering with Fast Succinct Tries" (SIGMOD 2018) - Motivation for trie-based indexing
- **LOUDS Encoding:** "Succinct Representation of Data Structures" (PhD thesis, Jacobson 1988) - Trie encoding technique

---

## Future Work

Potential improvements to UDI infrastructure:

1. **Partitioned UDI:** Support multi-level UDI indexes for very large SST files
2. **Parallel compression:** Implement PrepareIndexEntry/FinishIndexEntry split in wrapper
3. **Bidirectional iteration:** Add reverse iteration support to UDI interface
4. **Multiple UDI types per DB:** Allow different column families or SST files to use different UDI factories
5. **Bloom filter integration:** Combine UDI with bloom filters for negative lookup optimization
6. **Secondary indexes:** Extend UDI API to support true secondary indexes (beyond separator keys)
