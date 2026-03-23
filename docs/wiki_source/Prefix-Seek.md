# Why Prefix Seek?
Normally, when an iterator seek is executed, RocksDB needs to place the position at every sorted run (memtable, level-0 file, other levels, etc) to the seek position and merge these sorted runs. This can sometimes involve several I/O requests. It is also CPU heavy for decompression of several data blocks and other CPU overheads.

Prefix seek is a feature for mitigating these overheads for some use cases. The basic idea is that, if users know the iterating will be within one key prefix, the common prefix can be used to reduce costs. The most commonly used prefix iterating technique is prefix bloom filter. If many sorted runs don't contain any entry for this prefix, it can be filtered out by a bloom filter, and some I/Os and CPU for the sorted run can be ignored.

A motivating use case for prefix seek is representing multi-maps, such as [secondary indexes in MyRocks](https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#secondary-index-c), where the RocksDB prefix is the key for the multimap and a RocksDB iterator finds all the entries associated with that prefix (multimap key).

Whether prefix seek can improve performance is workload dependent. It's more likely to be effective for very short iterator queries than longer ones. 

# Defining a "prefix"
"Prefix" is defined by options.prefix_extractor, which is a shared_pointer of a `SliceTransform` instance. By calling `SliceTransform.Transform()` against a key, we extract a `Slice` representing a substring of the `Slice`, usually the prefix part. In this wiki page, we use "prefix" to refer to the output of `options.prefix_extractor.Transform()` of a key. You can use fixed length prefix transformer, by calling `NewFixedPrefixTransform(prefix_len)`, a capped length prefix transformer, by calling `NewCappedPrefixTransform(prefix_len)`, or you can implement your own prefix transformer in the way you want and pass it to `options.prefix_extractor`. Prefix extractor is related to comparator. A prefix extractor needs to be used with a comparator where keys with the same prefix are close to each others in the total order defined by the comparator.

The recommendation is to use capped prefix transform with bytewise or reverse bytewise comparators when possible. It will maximize features supported with relatively good performance.

# Configure prefix bloom filter
Although it is not the only way users can take advantage of prefix iterating, prefix bloom filter in block-based SST file is the most commonly used and effective one. Here is an example how you can set up prefix bloom filters in SST files.
```cpp
Options options;

// Set up bloom filter
rocksdb::BlockBasedTableOptions table_options;
table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
table_options.whole_key_filtering = false;  // If you also need Get() to use whole key filters, leave it to true.
options.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(table_options));  // For multiple column family setting, set up specific column family's ColumnFamilyOptions.table_factory instead.

// Define a prefix. In this way, a fixed length prefix extractor. A recommended one to use.
options.prefix_extractor.reset(NewCappedPrefixTransform(3));

DB* db;
Status s = DB::Open(options, "/tmp/rocksdb",  &db);
```
How this bloom filter is used depends on `ReadOptions` setting in read queries. See the section below for details.

`options.prefix_extractor` can be changed when DB is restarted. Usually the end result would be that bloom filters in existing SST files will be ignored in reads. In some cases, old bloom filters can still be used. This will be explained in following sections. 

# Configure reads while prefix bloom filters are set up
## How to ignore prefix bloom filters in read
Users can only use prefix bloom filter to read inside a specific prefix. If iterator is outside a prefix, the feature needs to be disabled for specific iterators to prevent wrong results:
```cpp
ReadOptions read_options;
read_options.total_order_seek = true;
Iterator* iter = db->NewIterator(read_options);
Slice key = "foobar";
iter->Seek(key);  // Seek "foobar" in total order
```
Putting `read_options.total_order_seek = true` will make sure the query returns the same result as if there is no prefix bloom filter.

## Adaptive Prefix Mode
Since Release 6.8, a new adaptive mode is introduced:
```cpp
ReadOptions read_options;
read_options.auto_prefix_mode = true;
Iterator* iter = db->NewIterator(read_options);
// ......
```
This will always generate the same result as `read_options.total_order_seek = true`, while prefix bloom filter might be used, based on seek key and iterator upper bound. It is the recommended way of using prefix bloom filter, because it's less prone to misuse. We will turn this option as default on once it is proven to be stable.

Here is an example how this feature can be used: for a fixed length prefix extractor of 3, following queries will take advantage of bloom filters:
```cpp
options.prefix_extractor.reset(NewCappedPrefixTransform(3));
options.comparator = BytewiseComparator();  // This is the default
// ......
ReadOptions read_options;
read_options.auto_prefix_mode = true;
std::string upper_bound;
Slice upper_bound_slice;

// "foo2" and "foo9" share the same prefix "foo".
upper_bound = "foo9";
upper_bound_slice = Slice(upper_bound);
read_options.iterate_upper_bound = &upper_bound_slice;
Iterator* iter = db->NewIterator(read_options);
iter->Seek("foo2");

// "foobar2" and "foobar9" share longer prefix than "foo".
upper_bound = "foobar9";
upper_bound_slice = Slice(upper_bound);
read_options.iterate_upper_bound = &upper_bound_slice;
Iterator* iter = db->NewIterator(read_options);
iter->Seek("foobar2");

// "foo2" and "fop" doesn't share the same prefix, but "fop" is the successor key of prefix "foo" which is the prefix of "foo2".
upper_bound = "fop";
upper_bound_slice = Slice(upper_bound);
read_options.iterate_upper_bound = &upper_bound_slice;
Iterator* iter = db->NewIterator(read_options);
iter->Seek("foo2");
```
This feature has some limitations:
* It only supports fixed and capped prefix extractor
* The comparator needs to implement `IsSameLengthImmediateSuccessor()`. The built-in byte-wise and reverse byte-wise comparators have it implemented.
* Right now only `Seek()` can automatically take advantage of the feature by comparing seek key with iterator upper bound. `SeekForPrev()` never uses prefix bloom filter. (no fundamental reason why it can't be done. You are welcome to finish the feature)
* Some extra CPU is used to determine qualification of prefix bloom filter in each SST file to query.

## Manual prefix iterating
With this option, users determine their use case qualifies for prefix iterator and are responsible of never iterating outside iterator. **RocksDB will not return error when it is misused** and the iterating result will be undefined. Note that, when iterating outside the iterator range, the result might not be limited to missing keys. Some undefined result might include: deleted keys might show up, key ordering is not followed, or very slow queries. Also note that even data within the prefix range might not be correct if the iterator has moved out of the prefix range and come back again.

Right now this mode is the default for backward compatible reason, but users should be extra careful when using this mode.

How to use the manual mode:
```cpp
options.prefix_extractor.reset(NewCappedPrefixTransform(3));

ReadOptions read_options;
read_options.total_order_seek = false;
read_options.auto_prefix_mode = false;
Iterator* iter = db->NewIterator(read_options);

iter->Seek("foobar");
// Iterate within prefix "foo"

iter->SeekForPrev("foobar");
// iterate within prefix "foo"
```
## Prefix extractor change
If prefix extractor changes when DB restarts, some SST files may contain prefix bloom filters generated using different prefix extractor than the one in current options. When opening those files, RocksDB will compare prefix extractor name stored in the properties of the SST files. If the name is different from the prefix extractor provided in the options, the prefix bloom filter is not used for this file, with following exception: if the previous SST file uses fixed or capped prefix extractor, the filter may sometimes be used if seek key and upper bound indicates that the iterator is within a prefix specified by previous prefix extractor. The behavior within the SST file would be the same as automatic prefix mode.

# Other prefix iterating features
Besides prefix bloom filter. There are several prefix iterating features:
* Prefix bloom filter in memtable. Turn it on with `options.memtable_prefix_bloom_size_ratio`
* Prefix memtables. Hash linked list and hash skip list memtables are supported. See [[MemTable]] for details.
* Prefix hash index in block based table format. See [[Data Block Hash Index]] for details.
* PlainTable format. See [[PlainTable Format]] for details.

# General Prefix Seek API
We introduced how to use prefix bloom filter in detail above. The usage is almost the same for other prefix iterating features. The only difference is that, some options might not be supported. Here is the general workflow:

When `options.prefix_extractor` for your DB or column family is specified, RocksDB is in a "prefix seek" mode. Example of how to use it:

```cpp
Options options;

// <---- Enable some features supporting prefix extraction
options.prefix_extractor.reset(NewFixedPrefixTransform(3));

DB* db;
Status s = DB::Open(options, "/tmp/rocksdb",  &db);

......

Iterator* iter = db->NewIterator(ReadOptions());
iter->Seek("foobar"); // Seek inside prefix "foo"
iter->Next(); // Find next key-value pair inside prefix "foo"
```

When `options.prefix_extractor` is not `nullptr` and with default `ReadOptions`, iterators are not guaranteed a total order of all keys, but only keys for the same prefix. When doing `Iterator.Seek(lookup_key)`, RocksDB will extract the prefix of lookup_key. If there is one or more keys in the database matching prefix of lookup_key, RocksDB will place the iterator to the key equal or larger than lookup_key of the same prefix, as for total ordering mode. If no key of the prefix equals or is larger than lookup_key, or after calling one or more `Next()`, we finish all keys for the prefix, we might return `Valid()=false`, or any key (might be non-existing) that is larger than the previous key. Setting `ReadOptions.prefix_same_as_start=true` guarantees the first of those two behaviors.

From release 4.11, we support `Prev()` in prefix mode, but only when the iterator is still within the range of all the keys for the prefix. The output of `Prev()` is not guaranteed to be correct when the iterator is out of the range.

When prefix seek mode is enabled, RocksDB will freely organize the data or build look-up data structures that can locate keys for specific prefix or rule out non-existing prefixes quickly. Here are some supported optimizations for prefix seek mode: prefix bloom for block based tables and mem tables, [hash-based mem tables](https://github.com/facebook/rocksdb/wiki/Hash-based-memtable-implementations), as well as [PlainTable](https://github.com/facebook/rocksdb/wiki/PlainTable-Format) format. One example setting:

```cpp
Options options;

// Enable prefix bloom for mem tables
options.prefix_extractor.reset(NewFixedPrefixTransform(3));
options.memtable_prefix_bloom_size_ratio = 0.1;

// Enable prefix hash for SST files
BlockBasedTableOptions table_options;
table_options.index_type = BlockBasedTableOptions::IndexType::kHashSearch;

DB* db;
Status s = DB::Open(options, "/tmp/rocksdb",  &db);

......

auto iter = db->NewIterator(ReadOptions());
iter->Seek("foobar"); // Seek inside prefix "foo"

```

From release 3.5, we support a read option to allow RocksDB to use total order even if `options.prefix_extractor` is given. To enable the feature set `ReadOption.total_order_seek=true` to the read option passed when doing `NewIterator()`, example:

```cpp
ReadOptions read_options;
read_options.total_order_seek = true;
auto iter = db->NewIterator(read_options);
Slice key = "foobar";
iter->Seek(key);  // Seek "foobar" in total order
```

Performance might be worse in this mode. Please be aware that not all implementations of prefix seek support this option. For example, some implementations of [PlainTable](https://github.com/facebook/rocksdb/wiki/PlainTable-Format) doesn't support it and you'll see an error in status code when you try to use it. [Hash-based mem tables](https://github.com/facebook/rocksdb/wiki/Hash-based-memtable-implementations) might do a very expensive online sorting if you use it. This mode is supported in prefix bloom and hash index of block based tables.

From release 6.8, we support a similar option while allowing underlying implementation to take advantage of prefix specific information when not impacting results.
```cpp
ReadOptions read_options;
read_options.auto_prefix_mode = true;
auto iter = db->NewIterator(read_options);
Slice key = "foobar";
iter->Seek(key);  // Seek "foobar" in total order
```
Right now, the feature is only supported for forward seeking with prefix bloom filter.

# Limitation
`SeekToLast()` is not supported well with prefix iterating. `SeekToFirst()` is only supported by some configurations. You should use total order mode, if you will execute those types of queries against your iterator. 

One common bug of using prefix iterating is to use prefix mode to iterate in reverse order. But it is not yet supported. If reverse iterating is your common query pattern, you can reorder the data to turn your iterating order to be forward. You can do it through implementing a customized comparator, or encode your key in a different way.

# API change from 2.8 -> 3.0
In this section, we explain the API as of 2.8 release and the change in 3.0.

## Before the Change

As of RocksDB 2.8, there are 3 seek modes:

### Total Order Seek
This is the traditional seek behavior you'd expect. The seek performs on a total ordered key space, positioning the iterator to a key that is greater or equal to the target key you seek.

```cpp
auto iter = db->NewIterator(ReadOptions());
Slice key = "foo_bar";
iter->Seek(key);
```

Not all table formats support total order seek. For example, the newly introduced [PlainTable](https://github.com/facebook/rocksdb/wiki/PlainTable-Format) format only supports prefix-based seek() unless it is opened in total order mode (Options.prefix_extractor == nullptr).

### Use ReadOptions.prefix
This is the least flexible way to do a seek. Prefix needs to be supplied when creating an iterator. 

```cpp
Slice prefix = "foo";
ReadOptions ro;
ro.prefix = &prefix;
auto iter = db->NewIterator(ro);
Slice key = "foo_bar"
iter->Seek(key);
```

`Options.prefix_extractor` is a prerequisite. The `Seek()` is constrained to the prefix provided by `ReadOptions`, which means you will need to create a new iterator to seek a different prefix. The benefit of this approach is that irrelevant files are filtered out at the time of building the new iterator. So if you want to seek multiple keys with the same prefix, it might perform better. However, we consider this is a very rare use case.

### Use ReadOptions.prefix_seek
This mode is more flexible than `ReadOption.prefix`. No pre-filtering is done at iterator creation time. As a result, the same iterator can be reused for seek of different key/prefix.

```cpp
ReadOptions ro;
ro.prefix_seek = true;
auto iter = db->NewIterator(ro);
Slice key = "foo_bar";
iter->Seek(key);
```

Same as `ReadOptions.prefix`, `Options.prefix_extractor` is a prerequisite.

## What's Changed
It becomes obvious that 3 modes of seek are confusing:
* One mode would require another option to be set (e.g. `Options.prefix_extractor`);
* It is not obvious to our users which mode of the last two is preferred under different circumstances

This change tries to address this issue and makes things straight: by default, `Seek()` is performed in prefix mode if `Options.prefix_extractor` is defined and vice versa. The motivation is simple: if `Options.prefix_extractor` is provided, it is a very clear signal that underlying data can be sharded and prefix seek is a natural fit. Usage becomes unified:

```cpp
auto iter = db->NewIterator(ReadOptions());
Slice key = "foo_bar";
iter->Seek(key);
```

## Transition to the New Usage
Transition to the new style should be simple: remove the assignment to `Options.prefix` or `Options.prefix_seek`, since they are deprecated. Now, seek directly with your target key or prefix. Since
`Next()` can go across the boundary to a different prefix, you will need to check the end condition:

```cpp
    auto iter = DB::NewIterator(ReadOptions());
    for (iter.Seek(prefix); iter.Valid() && iter.key().starts_with(prefix); iter.Next()) {
       // do something
    }
```