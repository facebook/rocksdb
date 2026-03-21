# Wide Columns in RocksDB

Wide Columns extend RocksDB's key-value model to support multiple named columns per key, enabling efficient storage and retrieval of structured data without requiring separate keys for each attribute.

## Overview

Traditional RocksDB stores a single value per key:
```
Key → Value
"user:123" → "{\"name\":\"Alice\", \"age\":30}"
```

Wide Columns allow storing multiple named values (columns) per key:
```
Key → { Column1: Value1, Column2: Value2, ... }
"user:123" → { "name": "Alice", "age": "30", "city": "NYC" }
```

This eliminates the need to:
- Serialize/deserialize entire structures when accessing a single field
- Use multiple keys for related attributes
- Parse JSON or protocol buffers just to read one column

## Core Data Structures

### WideColumn (include/rocksdb/wide_columns.h:23)

Represents a single column as a name-value pair:
```cpp
class WideColumn {
  Slice name_;   // Column name
  Slice value_;  // Column value
};
```

**⚠️ INVARIANT**: Column names within an entity must be **strictly ordered bytewise** and **unique**. Serialization and deserialization reject unordered or duplicate column names with `Status::Corruption`.

### WideColumns

Type alias for `std::vector<WideColumn>` — a collection of columns.

### PinnableWideColumns (include/rocksdb/wide_columns.h:106)

Self-contained wide column result with memory management:
```cpp
class PinnableWideColumns {
  PinnableSlice value_;     // Serialized or pinned value
  WideColumns columns_;     // Deserialized column index
};
```

Supports two input modes:
- **Plain value**: Maps to a single default column (empty name)
- **Wide column value**: Deserializes into multiple columns

### Default Column

**⚠️ INVARIANT**: The **default column** has an **empty name** (`kDefaultWideColumnName = Slice()`). When present, it is **always at index 0** because columns are sorted by name.

The default column provides **backward compatibility**:
- `Put(key, value)` stores a plain value, equivalent to a single-column entity with the default column
- `Get(key)` retrieves the default column's value from an entity
- `Iterator::value()` returns the default column's value when iterating over entities

## Public API

### Write Operations

**PutEntity** — store a wide-column entity (include/rocksdb/db.h:449):
```cpp
Status PutEntity(const WriteOptions& options,
                 ColumnFamilyHandle* column_family,
                 const Slice& key,
                 const WideColumns& columns);
```

Example:
```cpp
WideColumns columns{
  {"", "user_id_123"},         // Default column
  {"age", "30"},
  {"city", "San Francisco"},
  {"name", "Alice"}
};
// Columns will be sorted by name during serialization
Status s = db->PutEntity(WriteOptions(), cf, "user:123", columns);
```

**⚠️ INVARIANT**: User-provided columns need **not be pre-sorted**. Serialization sorts them internally. However, deserialization **requires** sorted columns and rejects unsorted data.

**AttributeGroups** — store an entity across multiple column families (include/rocksdb/db.h:454):
```cpp
Status PutEntity(const WriteOptions& options,
                 const Slice& key,
                 const AttributeGroups& attribute_groups);
```

This splits a logical entity into multiple physical column families for schema isolation or independent TTL/compaction policies.

### Read Operations

**GetEntity** — retrieve all columns for a key (include/rocksdb/db.h:667):
```cpp
Status GetEntity(const ReadOptions& options,
                 ColumnFamilyHandle* column_family,
                 const Slice& key,
                 PinnableWideColumns* columns);
```

**MultiGetEntity** — batch retrieval (include/rocksdb/db.h:876):
```cpp
void MultiGetEntity(const ReadOptions& options,
                    ColumnFamilyHandle* column_family,
                    size_t num_keys,
                    const Slice* keys,
                    PinnableWideColumns* results,
                    Status* statuses,
                    bool sorted_input = false);
```

**Iterators** — access columns via `columns()` method (include/rocksdb/iterator.h:54):
```cpp
Iterator* it = db->NewIterator(ReadOptions(), cf);
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  Slice key = it->key();
  const WideColumns& cols = it->columns();

  // Check if default column exists
  if (!cols.empty() && cols[0].name() == kDefaultWideColumnName) {
    Slice default_value = cols[0].value();
  }

  // Access other columns
  for (const auto& col : cols) {
    Slice name = col.name();
    Slice value = col.value();
  }
}
```

**⚠️ INVARIANT**: `Iterator::value()` returns the default column's value if present, otherwise an empty `Slice`. For accessing all columns, use `Iterator::columns()`.

### WriteBatch Support

Wide columns integrate with `WriteBatch` (db/db_impl/db_impl_write.cc:2821):
```cpp
WriteBatch batch;
batch.PutEntity(cf, key, columns);
batch.Delete(cf, another_key);
db->Write(WriteOptions(), &batch);
```

## Serialization Format

Wide columns are serialized into the value field of the underlying key-value pair. Two format versions exist.

### Version 1 Layout (db/wide/wide_column_serialization.h:30)

**Used when**: All column values are inline (no blob references).

```
┌──────────┬──────────────┬──────────┬───────┬──────────┬────────┬─────────────────┬────────┐
│ version  │ # of columns │  cns_0   │ cn_0  │  cvs_0   │  ...   │     cv_0        │  ...   │
│ varint32 │   varint32   │ varint32 │ bytes │ varint32 │        │     bytes       │        │
└──────────┴──────────────┴──────────┴───────┴──────────┴────────┴─────────────────┴────────┘
         Header                      Name/Size Pairs (sorted)      Column Values
```

**Layout**:
1. **Version** (varint32): Always `1`
2. **Column count** (varint32): Number of columns
3. **For each column** (in sorted order):
   - **Name size** (varint32) + **Name** (bytes)
   - **Value size** (varint32)
4. **Column values** (concatenated bytes in same order)

**⚠️ INVARIANT**: Column names in the serialized format are **strictly ordered**. Deserialization validates ordering and returns `Status::Corruption` if violated.

**Limits**:
- Column count: ≤ `UINT32_MAX`
- Column name size: ≤ `UINT32_MAX` bytes
- Column value size: ≤ `UINT32_MAX` bytes

### Version 2 Layout (db/wide/wide_column_serialization.h:54)

**Used when**: At least one column is a blob reference (stored in blob files).

```
Section 1: HEADER (2 varints)
┌──────────┬──────────────┐
│ version  │ # of columns │
│ varint32 │   varint32   │
└──────────┴──────────────┘

Section 2: SKIP INFO (3 varints)
┌───────────────────┬─────────────────────┬──────────────────┐
│ name_sizes_bytes  │ value_sizes_bytes   │ names_bytes      │
│ varint32          │ varint32            │ varint32         │
└───────────────────┴─────────────────────┴──────────────────┘
  Byte size of       Byte size of         Byte size of
  Section 4          Section 5            Section 6

Section 3: COLUMN TYPES (N bytes, fixed-size)
┌──────┬──────┬─────────┬────────┐
│ ct_0 │ ct_1 │   ...   │ ct_N-1 │
│ byte │ byte │         │  byte  │
└──────┴──────┴─────────┴────────┘
  ValueType for each column:
  - kTypeValue (0x01) = inline value
  - kTypeBlobIndex (0x11) = blob reference

Section 4: NAME SIZES (N varints)
┌──────────┬──────────┬─────────┬────────────┐
│ cns_0    │ cns_1    │   ...   │ cns_{N-1}  │
│ varint32 │ varint32 │         │ varint32   │
└──────────┴──────────┴─────────┴────────────┘

Section 5: VALUE SIZES (N varints)
┌──────────┬──────────┬─────────┬────────────┐
│ cvs_0    │ cvs_1    │   ...   │ cvs_{N-1}  │
│ varint32 │ varint32 │         │ varint32   │
└──────────┴──────────┴─────────┴────────────┘

Section 6: COLUMN NAMES (concatenated, sorted)
┌──────┬──────┬─────────┬────────┐
│ cn_0 │ cn_1 │   ...   │ cn_N-1 │
│ bytes│ bytes│         │ bytes  │
└──────┴──────┴─────────┴────────┘

Section 7: COLUMN VALUES (concatenated)
┌──────┬──────┬─────────┬────────┐
│ cv_0 │ cv_1 │   ...   │ cv_N-1 │
│ bytes│ bytes│         │ bytes  │
└──────┴──────┴─────────┴────────┘
  For blob columns: cv = serialized BlobIndex
  For inline columns: cv = actual value
```

**Design rationale** (db/wide/wide_column_serialization.h:55):
- **Skip info** enables O(1) access patterns:
  - Index-based value access: Skip name data entirely using `names_bytes`
  - Default column access: Read first name size, skip to values using skip offsets
  - Type checks: O(1) access to column types for blob detection
- **Grouped metadata**: All fixed/variable metadata upfront, then raw data
- **SIMD-friendly**: Header + skip info = 5 consecutive varints for future SIMD decoding

**⚠️ INVARIANT**: For V2 entities with blob columns, `kTypeBlobIndex` columns contain **serialized BlobIndex**, not raw values. Use `ResolveEntityBlobColumns` to fetch blob data and convert to V1 format.

### Encoding Details

**Serialization** (db/wide/wide_column_serialization.cc:51):
```cpp
Status WideColumnSerialization::Serialize(const WideColumns& columns,
                                          std::string& output);
```
- Validates column count and sizes (≤ UINT32_MAX)
- Validates column ordering (strict bytewise ascending)
- Uses V1 format (no blob support)

**Serialization V2** (db/wide/wide_column_serialization.cc:227):
```cpp
Status SerializeV2(const WideColumns& columns,
                   const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
                   std::string& output);
```
- Accepts blob column indices and their BlobIndex references
- Two-pass algorithm:
  1. Validate ordering, compute section sizes, serialize blob indices
  2. Pre-allocate output, write all sections in single pass using independent pointers
- Optimizes for cache efficiency by minimizing writes

**Deserialization** (db/wide/wide_column_serialization.cc:389):
```cpp
Status Deserialize(Slice& input, WideColumns& columns);
Status DeserializeV2(Slice& input,
                     std::vector<WideColumn>& columns,
                     std::vector<std::pair<size_t, BlobIndex>>& blob_columns);
```
- Detects version from first varint
- Validates ordering during parsing
- `Deserialize` rejects V2 entities with blob references (`Status::NotSupported`)
- `DeserializeV2` separates inline columns from blob columns

**Fast Default Column Access** (db/wide/wide_column_serialization.cc:514):
```cpp
Status GetValueOfDefaultColumn(Slice& input, Slice& value);
```
V2 fast path leverages skip info to extract default column without full deserialization:
1. Read skip info (3 varints)
2. Check if first column type is blob → reject if so
3. Read first name size from Section 4
4. If name size ≠ 0 → no default column, return empty
5. Skip to Section 7 using offsets, read first value

**⚠️ INVARIANT**: This fast path assumes default column (empty name) is at index 0. Violating sort order breaks this optimization.

## Backward Compatibility

### Plain Put/Get Integration

**Writing**:
```cpp
db->Put(WriteOptions(), cf, key, "plain_value");
```
Internally equivalent to:
```cpp
WideColumns cols{{kDefaultWideColumnName, "plain_value"}};
db->PutEntity(WriteOptions(), cf, key, cols);
```

**Reading**:
```cpp
std::string value;
db->Get(ReadOptions(), cf, key, &value);
```
Retrieves the default column's value from the entity.

**⚠️ INVARIANT**: `Get` on a wide-column entity **returns only the default column**. Other columns are ignored. Use `GetEntity` to retrieve all columns.

### Iterator Compatibility

**value() method** (db/db_iter.cc):
```cpp
Iterator* it = db->NewIterator(ReadOptions(), cf);
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  Slice val = it->value();  // Returns default column or empty
}
```

**columns() method**:
```cpp
const WideColumns& cols = it->columns();
if (!cols.empty() && cols[0].name().empty()) {
  // Default column exists
}
```

**⚠️ INVARIANT**: For entities without a default column, `Iterator::value()` returns an **empty Slice**, not an error. Check `columns()` for actual data.

## Compaction and Merge

### Value Type in LSM Tree

Wide-column entities use `kTypeWideColumnEntity` (0x16) as their ValueType in the internal key format (db/dbformat.h). This distinguishes them from:
- `kTypeValue` (0x01) — plain values
- `kTypeMerge` (0x02) — merge operands
- `kTypeBlobIndex` (0x11) — blob references

**⚠️ INVARIANT**: `kTypeWideColumnEntity` can **only appear at the key level**, not as a per-column type. Per-column types (`kTypeValue`, `kTypeBlobIndex`) exist **only in V2 serialization** and represent inline vs. blob storage.

### Compaction Handling (db/compaction/compaction_iterator.cc)

Compaction treats wide-column entities similarly to plain values:
- **Merging duplicates**: Newer entity replaces older entity (same as kTypeValue)
- **Deletion**: `kTypeDeletion` removes entire entity, not individual columns
- **Single deletion**: `kTypeSingleDeletion` deletes entire entity
- **Compaction filters**: Receive serialized entity value, can inspect/modify via deserialization

**Future extension**: Per-column deletion types (`kTypeColumnDeletion`) would enable granular column removal.

### Merge Operators

Merge operators interact with wide-column entities through serialized format.

**Full merge with entities** (db/merge_helper.cc):
```cpp
merge_operator->FullMergeV3(merge_in, merge_out);
```
- `merge_in.existing_value`: Either `std::monostate`, `Slice` (plain value), or `WideColumns` (entity)
- `merge_in.operand_list`: Merge operand values
- `merge_out->new_value`: Output — can be plain value or `NewColumns` (for entity output)

**Example use case**: Combining columns from base and operands
```cpp
class WideColumnMergeOperator : public MergeOperator {
  bool FullMergeV3(const MergeOperationInputV3& merge_in,
                   MergeOperationOutputV3* merge_out) const override {
    merge_out->new_value = MergeOperationOutputV3::NewColumns();
    auto& new_columns =
        std::get<MergeOperationOutputV3::NewColumns>(merge_out->new_value);

    // Process existing value
    std::visit(overload{
        [&](const std::monostate&) {
          // No existing value
        },
        [&](const Slice& value) {
          // Plain value - add as default column
          new_columns.emplace_back(kDefaultWideColumnName.ToString(),
                                  value.ToString());
        },
        [&](const WideColumns& columns) {
          // Wide-column entity - copy all columns
          for (const auto& col : columns) {
            new_columns.emplace_back(col.name().ToString(),
                                    col.value().ToString());
          }
        }
    }, merge_in.existing_value);

    // Apply merge operands as new columns
    for (const auto& operand : merge_in.operand_list) {
      // Parse operand to extract column name and value
      // (format depends on your application)
      new_columns.emplace_back(ParseColumnName(operand),
                              ParseColumnValue(operand));
    }

    // Ensure columns are sorted by name
    std::sort(new_columns.begin(), new_columns.end(),
              [](const auto& a, const auto& b) {
                return a.first < b.first;
              });

    return true;
  }

  const char* Name() const override {
    return "WideColumnMergeOperator";
  }
};
```

**⚠️ INVARIANT**: Merge operators must **preserve column ordering** when creating entities. The `MergeOperationOutputV3::NewColumns` output will be serialized with the columns in the order provided, so ensure they are sorted by name.

## Blob Integration (V2 Format)

### Blob Column References

Large column values can be stored in blob files to avoid amplifying memtable/WAL writes.

**Write path** (write offloading):
```cpp
// During memtable insert, if column value exceeds min_blob_size:
// 1. Write column value to blob file
// 2. Store BlobIndex reference in memtable
// 3. Serialize entity using SerializeV2 with blob_columns list
```

**Read path** (blob resolution):
```cpp
// GetEntity on V2 entity with blobs:
// 1. Deserialize entity (separates inline columns from blob indices)
// 2. For each blob column, fetch value from blob file
// 3. Merge into final WideColumns result
```

**ResolveEntityBlobColumns** (db/wide/wide_column_serialization.cc:626):
```cpp
Status ResolveEntityBlobColumns(
    const Slice& entity_value,
    const Slice& user_key,
    const BlobFetcher* blob_fetcher,
    PrefetchBufferCollection* prefetch_buffers,
    std::string& resolved_entity,
    bool& resolved,
    uint64_t* total_bytes_read,
    uint64_t* num_blobs_resolved);
```

**Workflow**:
1. Deserialize V2 entity → separate inline and blob columns
2. For each blob column:
   - Check if inlined (`BlobIndex::IsInlined()`)
   - If not, fetch from blob file via `blob_fetcher->FetchBlob()`
3. Merge resolved blob values with inline columns
4. Serialize as V1 entity (all values inline)

**⚠️ INVARIANT**: APIs that only support V1 format (e.g., some merge operators) **require blob resolution**. The read path automatically resolves blobs before passing entities to V1-only code.

**Prefetch optimization**: Pass `PrefetchBufferCollection` to batch blob reads from the same blob file, reducing I/O operations.

### Detecting Blob Columns

**HasBlobColumns** (db/wide/wide_column_serialization.cc:460):
```cpp
Status HasBlobColumns(const Slice& input, bool& has_blob_columns);
```
Fast check without full deserialization:
1. Read version and column count
2. If version < 2 → no blobs
3. Skip to COLUMN TYPES section (skip 3 varints)
4. Linear scan of type bytes for `kTypeBlobIndex`

**⚠️ INVARIANT**: Only V2 entities can have blob columns. V1 entities always have `has_blob_columns = false`.

## Configuration and Usage Patterns

### When to Use Wide Columns

**Good fit**:
- Structured data with multiple attributes (user profiles, product metadata)
- Selective column access (read only `name` and `age`, skip `bio`)
- Schema evolution (add new columns without rewriting existing data)
- Related attributes with shared lifecycle (same TTL, compaction policy)

**Not ideal**:
- Single-attribute records (plain Put/Get is simpler)
- Frequently accessed columns are very large (consider separate keys or blob storage)
- Complex queries across columns (RocksDB is not a relational DB)

### Schema Design

**Column naming**:
- Use short names to minimize serialization overhead
- Use consistent naming across keys for schema clarity
- Consider namespacing: `"profile.name"`, `"profile.age"`

**Default column usage**:
- Store the most frequently accessed value in the default column for Get compatibility
- Example: `{"": "primary_value", "meta": "...", "tags": "..."}`
- This allows legacy code using `Get` to retrieve primary data

**Column count**:
- Entities with <10 columns are typical (db/wide/wide_column_serialization.h:269)
- Linear scans in serialization/deserialization are O(N) in column count
- For 100+ columns, consider restructuring or using separate keys

### Performance Considerations

**Write amplification**:
- Wide columns reduce write amplification vs. multiple keys (single WAL entry, single memtable insert)
- Updating one column requires rewriting entire entity (read-modify-write)
- Use merge operators for incremental updates to avoid RMW

**Read amplification**:
- Reading one column still deserializes the entire entity (V1 format scans all column sizes)
- V2 skip info enables fast default column access without full parse
- For very large entities, consider splitting hot columns into separate keys

**Serialization cost**:
- Varint encoding is compact for small values (<128 bytes → 1-byte length prefix)
- Column name repetition across keys (consider short names)
- Sorting overhead during serialization (O(N log N) in column count)

**Caching**:
- Block cache caches serialized entities, not deserialized WideColumns
- Deserializing entities is CPU-bound (varint decoding, column index building)
- Repeatedly accessing the same entity requires re-deserialization

## Helper Functions (db/wide/wide_columns_helper.h)

**HasDefaultColumn** (line 25):
```cpp
static bool HasDefaultColumn(const WideColumns& columns) {
  return !columns.empty() && columns.front().name() == kDefaultWideColumnName;
}
```

**HasDefaultColumnOnly** (line 29):
```cpp
static bool HasDefaultColumnOnly(const WideColumns& columns) {
  return columns.size() == 1 &&
         columns.front().name() == kDefaultWideColumnName;
}
```

**GetDefaultColumn** (line 34):
```cpp
static const Slice& GetDefaultColumn(const WideColumns& columns) {
  assert(HasDefaultColumn(columns));
  return columns.front().value();
}
```

**SortColumns** (line 39):
```cpp
static void SortColumns(WideColumns& columns) {
  std::sort(columns.begin(), columns.end(),
            [](const WideColumn& lhs, const WideColumn& rhs) {
              return lhs.name().compare(rhs.name()) < 0;
            });
}
```

**Find** (line 47):
```cpp
template <typename Iterator>
static Iterator Find(Iterator begin, Iterator end, const Slice& column_name) {
  // Binary search assuming columns are sorted
  auto it = std::lower_bound(begin, end, column_name, ...);
  if (it == end || it->name() != column_name) {
    return end;
  }
  return it;
}
```

**⚠️ INVARIANT**: `Find` assumes input is **already sorted**. Searching unsorted columns yields undefined behavior.

## Code References

| Component | File | Description |
|-----------|------|-------------|
| Public API | `include/rocksdb/db.h:449,667,876` | PutEntity, GetEntity, MultiGetEntity |
| Core Types | `include/rocksdb/wide_columns.h` | WideColumn, PinnableWideColumns |
| Serialization | `db/wide/wide_column_serialization.h` | V1/V2 encoding, blob support |
| Serialization Impl | `db/wide/wide_column_serialization.cc` | Serialize, Deserialize, ResolveEntityBlobColumns |
| Helpers | `db/wide/wide_columns_helper.h` | HasDefaultColumn, SortColumns, Find |
| WriteBatch | `include/rocksdb/write_batch.h` | WriteBatch::PutEntity |
| Iterator | `include/rocksdb/iterator.h:54` | Iterator::columns() |
| DB Implementation | `db/db_impl/db_impl.cc:2357` | DBImpl::GetEntity |
| DB Write Path | `db/db_impl/db_impl_write.cc:41,2808` | DBImpl::PutEntity, DB::PutEntity |
| Compaction | `db/compaction/compaction_iterator.cc` | kTypeWideColumnEntity handling |
| Value Type | `db/dbformat.h` | kTypeWideColumnEntity (0x13) |

## Limitations and Future Work

**Current limitations**:
- No per-column deletion (must rewrite entire entity minus column)
- No partial column updates (must read-modify-write entire entity)
- No column-level TTL (entity-level only)
- No column-level bloom filters (entity-level key filtering only)
- No lazy deserialization (entire entity parsed on access)

**Potential extensions**:
- `kTypeColumnDeletion` for granular column removal
- Columnar encoding for entities with many columns (parquet-like)
- Column-level compression (different algorithms per column)
- Lazy deserialization with skip offsets (V2 partially supports this)
- Column predicates in compaction filters
- Per-column statistics in table properties
