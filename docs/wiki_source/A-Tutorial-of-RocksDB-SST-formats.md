## What is SST (Static Sorted Table)
All RocksDB's persistent data is stored in a collection of SSTs. We often use `sst`, `table` and `sst file` interchangeably. 

## Choosing a table format

Rocksdb supports different types of SST formats, but how to choose the table format that fits your need best?

Right now we have two types of tables: "plain table" and "block based table".

### Block-based table ###

This is the default table type that we inherited from [LevelDB](http://leveldb.googlecode.com/svn/trunk/doc/index.html), which was designed for storing data in hard disk or flash device.

In block-based table, data is chucked into (almost) fix-sized blocks (default block size is 4k). Each block, in turn, keeps a bunch of entries.

When storing data, we can compress and/or encode data efficiently within a block, which often resulted in a much smaller data size compared with the raw data size.

As for the record retrieval, we'll first locate the block where target record may reside, then read the block to memory, and finally search that record within the block. Of course, to avoid frequent reads of the same block, we introduced the `block cache` to keep the loaded blocks in the memory.

For more information about block-based table, please read this wiki: [Rocksdb BlockBasedTable Format](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format).

### Plain table ###

Block-based table is proven to be efficient when store data in hard disk or flash device. However, for applications that requires low-latency in-memory database, a better alternative emerges: plain table.

Plain table, as its name suggests, stores data in a sequence of key/value pairs. But several features make plain table have not-so-plain (read "excellent") performance when serving as the module of in-memory database:

* No memory copy needed. As part of in-memory database, we can easily mmap a plain table and allows direct access to its data without copying. Also plain table bypasses the concept of "block" and therefore avoids the overhead inherent in block-based table, like extra block lookup, block cache, etc.
* Faster Hash-based index. Compared with block-based table, which employs mostly binary search for entry lookup, the well designed hash-based index in plain table enables us to locate data magnitudes faster.

Of course, currently there're some limitations for this plain table format (more details please see the link provide below):

* File size may not be greater than 2^31 - 1 (i.e., `2147483647`) bytes.
* Data compression/Delta encoding is not supported, which may resulted in bigger file size compared with block-based table.
* Backward (Iterator.Prev()) scan is not supported.
* Non-prefix-based Seek() is not supported
* Table loading is slower since indexes are built on the fly by 2-pass table scanning.
* Only support mmap mode.

For more information about plain table, please read this wiki: [PlainTable Format](https://github.com/facebook/rocksdb/wiki/PlainTable-Format).

## Comparison of SSTs

TBD

## Examples

### Block-based table
By default, a database uses block-based table.

```cpp
#include "rocksdb/db.h"
rocksdb::DB* db;
// Get a db with block-based table without any change.
rocksdb::DB::Open(rocksdb::Options(), "/tmp/testdb", &db);
```

For a more customized block-based table:

```cpp
#include "rocksdb/db.h"
// rocksdb/table.h includes all supported tables.
#include "rocksdb/table.h"

rocksdb::DB* db;
rocksdb::Options options;
options.table_factory.reset(NewBlockBasedTableFactory());
options.block_size = 4096; /* block size for the block-based table */
rocksdb::DB::Open(options, "/tmp/testdb", &db);
```

### Plain table
For plain table, the process is similar:

```cpp
#include "rocksdb/db.h"
// rocksdb/table.h includes all supported tables.
#include "rocksdb/table.h"

rocksdb::DB* db;
rocksdb::Options options;
// To enjoy the benefits provided by plain table, you have to enable
// allow_mmap_reads for plain table.
options.allow_mmap_reads = true;
// plain table will extract the prefix from a key. The prefix will be
// used for the calculating hash code, which will be used in hash-based
// index.
// Unlike Prefix_extractor is a raw pointer, please remember to delete it
// after use.
SliceTransform* prefix_extractor = new NewFixedPrefixTransform(8);
options.prefix_extractor = prefix_extractor;
options.table_factory.reset(NewPlainTableFactory(
    // plain table has optimization for fix-sized keys, which can be
    // specified via user_key_len.  Alternatively, you can pass
    // `kPlainTableVariableLength` if your keys have variable lengths.
    8,
    // For advanced users only. 
    // Bits per key for plain table's bloom filter, which helps rule out non-existent
    // keys faster. If you want to disable it, simply pass `0`.
    // Default: 10.
    10,
    // For advanced users only.
    // Hash table ratio. the desired utilization of the hash table used for prefix
    // hashing. hash_table_ratio = number of prefixes / #buckets in the hash table.
    0.75
));
rocksdb::DB::Open(options, "/tmp/testdb", &db);
...
delete prefix_extractor;
```