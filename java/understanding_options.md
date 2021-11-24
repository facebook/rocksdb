# How RocksDB Options and their Java Wrappers Work

Options in RocksDB come in many different flavours. This is an attempt at a taxonomy and explanation.

## RocksDB Options

Initially, I believe, RocksDB had only database options. I don't know if any of these were mutable. Column families came later. Read on to understand the terminology.

So to begin, one sets up a collection of options and starts/creates a database with these options. That's a useful way to think about it, because from a Java point-of-view (and I didn't realise this initially and got very confused), despite making native calls to C++, the `API`s are just manipulating a native C++ configuration object. This object is just a record of configuration, and it must later be passed to the database (at create or open time) in order to apply the options.

### Database versus Column Family

The concept of the *column family* or `CF` is widespread within RocksDB. I think of it as a data namespace, but conveniently transactions can operate across these namespaces. The concept of a default column family exists, and when operations do not refer to a particular `CF`, it refers to the default.

We raise this w.r.t. options because many options, perhaps most that users encounter, are *column family options*. That is to say they apply individually to a particular column family, or to the default column family. Crucially also, many/most/all of these same options are exposed as *database options* and then apply as the default for column families which do not have the option set explicitly. Obviously some database options are naturally database-wide; they apply to the operation of the database and don't make any sense applied to a column family.

### Mutability

There are 2 kinds of options

- Mutable options
- Immutable options. We name these in contrast to the mutable ones, but they are usually referred to unqualified.

Mutable options are those which can be changed on a running `RocksDB` instance. Immutable options can only be configured prior to the start of a database. Of course, we can configure the immutable options at this time too; The entirety of options is a strict superset of the mutable options.

Mutable options (whether column-family specific or database-wide) are manipulated at runtime with builders, so we have `MutableDBOptions.MutableDBOptionsBuilder` and `MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder` which share tooling classes/hierarchy and maintain and manipulate the relevant options as a `(key,value)` map.

Mutable options are then passed using `setOptions()` and `setDBOptions()` methods on the live RocksDB, and then take effect immediately (depending on the semantics of the option) on the database.

### Advanced

There are 2 classes of options

- Advanced options
- Non-advanced options

It's not clear to me what the conceptual distinction is between advanced and not. However, the Java code takes care to reflect it from the underlying C++.

This leads to 2 separate type hierarchies within column family options, one for each `class` of options. The `kind`s are represented by where the options appear in their hierarchy.

```java
interface ColumnFamilyOptionsInterface<T extends ColumnFamilyOptionsInterface<T>>
    extends AdvancedColumnFamilyOptionsInterface<T>
interface MutableColumnFamilyOptionsInterface<T extends MutableColumnFamilyOptionsInterface<T>>
    extends AdvancedMutableColumnFamilyOptionsInterface<T>
```

And then there is ultimately a single concrete implementation class for CF options:

```java
class ColumnFamilyOptions extends RocksObject
    implements ColumnFamilyOptionsInterface<ColumnFamilyOptions>,
    MutableColumnFamilyOptionsInterface<ColumnFamilyOptions>
```

as there is a single concrete implementation class for DB options:

```java
class DBOptions extends RocksObject
    implements DBOptionsInterface<DBOptions>,
    MutableDBOptionsInterface<DBOptions>
```

Interestingly `DBOptionsInterface` doesn't extend `MutableDBOptionsInterface`, if only in order to disrupt our belief in consistent basic laws of the Universe.

## Startup/Creation Options

```java
class Options extends RocksObject
    implements DBOptionsInterface<Options>,
    MutableDBOptionsInterface<Options>,
    ColumnFamilyOptionsInterface<Options>,
    MutableColumnFamilyOptionsInterface<Options>
```

### Example - Blob Options

The `enable_blob_files` and `min_blob_size` options are per-column-family, and are mutable. The options also appear in the unqualified database options. So by initial configuration, we can set up a RocksDB database where for every `(key,value)` with a value of size at least `min_blob_size`, the value is written (indirected) to a blob file. Blobs may share a blob file, subject to the configuration values set. Later, using the `MutableColumnFamilyOptionsInterface` of the `ColumnFamilyOptions`, we can choose to turn this off (`enable_blob_files=false`) , or alter the `min_blob_size` for the default column family, or any other column family. It seems to me that we cannot, though, mutate the column family options for all column families using the
`setOptions()` mechanism, either for all existing column families or for all future column families; but maybe we can do the latter on a re-`open()/create()'
