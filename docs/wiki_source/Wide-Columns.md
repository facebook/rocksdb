## Background
RocksDB has historically been a pure key-value store, where keys and values are opaque and unstructured byte arrays from the storage engine’s point of view. This works well for many applications; however, we also have plenty of use cases where the data has some internal structure. In order to better serve such use cases in RocksDB, we have recently extended our data model to support wide columns. The wide-column model offers a middle ground between the pure key-value model and the relational model: multiple named columns can be associated with a key as in a relational database, however, as opposed to the relational model, the set of columns may differ from row to row. These traits make wide columns a good fit for storing semi-structured data.

## API
First, some terminology: we refer to a wide-column structure associated with a key as an entity. Entities can have multiple columns (or attributes), and each column has a name and a value. For compatibility reasons, we also have a notion of an anonymous default column. The following sections describe the new wide-column specific APIs and how wide-column entities are handled by the existing APIs.

### Write APIs
Wide-column entities can be written to a RocksDB instance using the `PutEntity` API. Mixing regular key-values and entities in the same RocksDB instance is supported, even for the same key: similarly to the regular `Put`, `PutEntity` has “upsert” semantics, and one can overwrite a plain key-value with an entity or vice versa. `PutEntity` is available both as a `DB` and a `WriteBatch` method:

```
class DB {
public:
// ... 
virtual Status PutEntity(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         const WideColumns& columns);
// ...
};

class WriteBatch : public WriteBatchBase {
public:
// ...
Status PutEntity(ColumnFamilyHandle* column_family, const Slice& key,
                 const WideColumns& columns) override;
// ...
};
```

Entities can be deleted using the usual APIs: `Delete`, `DeleteRange`, and `SingleDelete` are all supported, and work the same way as for regular key-values. `Merge` operations are applied to the default column; any other columns the base entity might have are unaffected.

### Read APIs
On the query side, we have introduced a pair of new wide-column-aware point lookup APIs called `GetEntity` and `MultiGetEntity`. `MultiGetEntity` is built on the `MultiGet` logic, and thus benefits from the many performance optimizations introduced over the years like batching and async I/O. Both of the new APIs return their results in the form of self-contained `PinnableWideColumns` objects, which, similarly to the well-known `PinnableSlice`s, can improve performance by eliminating a copy when results are served from the block cache.

```
class DB {
public:
// ...
virtual Status GetEntity(const ReadOptions& /* options */,
                          ColumnFamilyHandle* /* column_family */,
                          const Slice& /* key */,
                          PinnableWideColumns* /* columns */);
// ...
virtual void MultiGetEntity(const ReadOptions& /* options */,
                             ColumnFamilyHandle* /* column_family */,
                             size_t num_keys, const Slice* /* keys */,
                             PinnableWideColumns* /* results */,
                             Status* statuses,
                             bool /* sorted_input */ = false);
virtual void MultiGetEntity(const ReadOptions& /* options */, size_t num_keys,
                             ColumnFamilyHandle** /* column_families */,
                             const Slice* /* keys */,
                             PinnableWideColumns* /* results */,
                             Status* statuses,
                             bool /* sorted_input */ = false);
// ...
};
```

As for range scans, we have extended the `Iterator` API with a new method called `columns`:

```
class Iterator : public Cleanable {
public:
// ...
virtual const WideColumns& columns() const;
// ...
};
```

In order to enable customers to mix plain key-values and wide-column entities and in particular to introduce wide columns into existing databases, all query APIs, old and new, are capable of handling both regular key-values and entities. The classic `Get`, `MultiGet`, `GetMergeOperands`, and `Iterator::value` APIs return the value of the default column when they encounter an entity, while the new APIs `GetEntity`, `MultiGetEntity`, and `Iterator::columns` return any plain key-value in the form of an entity with a single column, namely the anonymous default column.

### Compaction filters
We have also extended the compaction filter logic with support for wide columns. To take advantage of this, customers can implement the new `FilterV3` interface (instead of `FilterV2` or the old `Filter`/`FilterMergeOperand` interfaces). `FilterV3` supports wide-column entities on both the input and output side. RocksDB passes the value type (regular key-value, merge operand, or wide-column entity) of the existing key-value to `FilterV3` along with the existing value or entity, and the application logic can then decide whether to keep it, remove it, or change the value. With `FilterV3`, changing the value actually comes in two flavors, which differ in the value type of the output. The application can either return `kChangeValue` and set the `new_value` output parameter if the new key-value should be a plain old KV, or return `kChangeWideColumnEntity` and set the `new_columns` output parameter. In the latter case, `new_columns` can contain arbitrary columns; that is, the compaction filter is free to add, remove, or modify columns. RocksDB also supports changing the value type using these decisions; regular values can be turned into wide-column entities and vice versa, which can come in handy when dealing with upgrade/downgrade scenarios and data migrations.

For backward compatibility, the default implementation of `FilterV3` keeps all wide-column entities and falls back to `FilterV2` (which in turn might fall back to `Filter`/`FilterMergeOperand`).

```
class CompactionFilter : public Customizable {
public:
// ...
// Value type of the key-value passed to the compaction filter's FilterV2/V3
// methods.
enum ValueType {
   // ...
   kValue,
   // ...
   kWideColumnEntity,
};


// Potential decisions that can be returned by the compaction filter's
// FilterV2/V3 and FilterBlobByKey methods. See decision-specific caveats and
// constraints below.
enum class Decision {
   // ...
   kChangeValue,
   // ...
   kChangeWideColumnEntity,
   // ...
};


// ...
virtual Decision FilterV3(
     int level, const Slice& key, ValueType value_type,
     const Slice* existing_value, const WideColumns* existing_columns,
     std::string* new_value,
     std::vector<std::pair<std::string, std::string>>* /* new_columns */,
     std::string* skip_until) const;
// ...
};
```

## Current status and future work
All the features described above are available as of RocksDB 8.0. Our future plans include:
* Column-level operations, i.e. updating a subset of columns on the write path (partial updates), and retrieving a subset of columns on the read path (projections).
* Column/attribute groups, which could be used to partition entities based on common data types or access patterns (e.g. hot/frequently changing versus cold/rarely changing columns, or attributes that are frequently queried together).

Note that there are a few RocksDB features that are currently not supported in conjunction with wide columns: these include transactions, user-defined timestamps, and the SST file writer. We will prioritize closing these gaps as needed. We also have some plans for performance improvements, including optimizing the encoding and integrating with BlobDB for large entities.