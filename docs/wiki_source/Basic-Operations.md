# Basic operations

The <code>rocksdb</code> library provides a persistent key value store. Keys and values are arbitrary byte arrays. The keys are ordered within the key value store according to a user-specified comparator function.

## Opening A Database

A <code>rocksdb</code> database has a name which corresponds to a file system directory. All of the contents of database are stored in this directory. The following example shows how to open a database, creating it if necessary:

```cpp
  #include <cassert>
  #include "rocksdb/db.h"

  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());
  ...
```

If you want to raise an error if the database already exists, add the following line before the <code>rocksdb::DB::Open</code> call:

```cpp
  options.error_if_exists = true;
```

If you are porting code from <code>leveldb</code> to <code>rocksdb</code>, you can convert your <code>leveldb::Options</code> object to a <code>rocksdb::Options</code> object using <code>rocksdb::LevelDBOptions</code>, which has the same functionality as <code>leveldb::Options</code>:

```cpp
  #include "rocksdb/utilities/leveldb_options.h"

  rocksdb::LevelDBOptions leveldb_options;
  leveldb_options.option1 = value1;
  leveldb_options.option2 = value2;
  ...
  rocksdb::Options options = rocksdb::ConvertOptions(leveldb_options);
```

## RocksDB Options
Users can choose to always set options fields explicitly in code, as shown above. Alternatively, you can also set it through a string to string map, or an option string. See [[Option String and Option Map]].

Some options can be changed dynamically while DB is running. For example:

```cpp
rocksdb::Status s;
s = db->SetOptions({{"write_buffer_size", "131072"}});
assert(s.ok());
s = db->SetDBOptions({{"max_background_flushes", "2"}});
assert(s.ok());
```

RocksDB automatically keeps options used in the database in OPTIONS-xxxx files under the DB directory. Users can choose to preserve the option values after DB restart by extracting options from these option files. See [[RocksDB Options File]].

## Status

You may have noticed the <code>rocksdb::Status</code> type above. Values of this type are returned by most functions in <code>rocksdb</code> that may encounter an error. You can check if such a result is ok, and also print an associated error message:

```cpp
   rocksdb::Status s = ...;
   if (!s.ok()) cerr << s.ToString() << endl;
```


## Closing A Database

When you are done with a database, there are 3 ways to gracefully close the database -
1. Simply delete the database object. This will release all the resources that were held while the database was open. However, if any error is encountered when releasing any of the resources, for example error when closing the info_log file, it will be lost.
2. Call ```DB::Close()```, followed by deleting the database object. The ```DB::Close()``` returns ```Status```, which can be examined to determine if there were any errors. Regardless of errors, ```DB::Close()``` will release all resources and is irreversible.
3. Call ```DB::WaitForCompact()``` with ```WaitForCompactOptions.close_db=true```. ```DB::WaitForCompact()``` will internally call ```DB::Close()``` after waiting for running background jobs to finish. _This is a recommended choice for users who want to wait for background work before closing rather than aborting and potentially redoing some work on re-open_.

Example:


```cpp
  ... open the db as described above ...
  ... do something with db ...
  delete db;
```
Or
```cpp
  ... open the db as described above ...
  ... do something with db ...
  Status s = db->Close();
  ... log status ...
  delete db;
```
Or
```cpp
  ... open the db as described above ...
  ... do something with db ...
  opt = WaitForCompactOptions();
  opt.close_db = true;
  Status s = db->WaitForCompact(opt);
  ... log status ...
  delete db;
```

## Reads

The database provides <code>Put</code>, <code>Delete</code>, <code>Get</code>, and <code>MultiGet</code> methods to modify/query the database. For example, the following code moves the value stored under key1 to key2.

```cpp
  std::string value;
  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key1, &value);
  if (s.ok()) s = db->Put(rocksdb::WriteOptions(), key2, value);
  if (s.ok()) s = db->Delete(rocksdb::WriteOptions(), key1);
```
Right now, value size must be smaller than 4GB.
RocksDB also allows [[Single Delete]] which is useful in some special cases.

Each `Get` results into at least a memcpy from the source to the value string. If the source is in the block cache, you can avoid the extra copy by using a PinnableSlice.
```cpp
  PinnableSlice pinnable_val;
  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key1, &pinnable_val);
```
The source will be released once pinnable_val is destructed or ::Reset is invoked on it. Read more [here](http://rocksdb.org/blog/2017/08/24/pinnableslice.html).

When reading multiple keys from the database, `MultiGet` can be used. There are two variations of `MultiGet`: 1. Read multiple keys from a single column family in a more performant manner, i.e it can be faster than calling `Get` in a loop, and 2. Read keys across multiple column families consistent with each other.

For example,
```cpp
  std::vector<Slice> keys;
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;

  for ... {
    keys.emplace_back(key);
  }
  values.resize(keys.size());
  statuses.resize(keys.size());

  db->MultiGet(ReadOptions(), cf, keys.size(), keys.data(), values.data(), statuses.data());
```
In order to avoid the overhead of memory allocations, the ```keys```, ```values``` and ```statuses``` above can be of type ```std::array``` on stack or any other type that provides contiguous storage.

Or
```cpp
  std::vector<ColumnFamilyHandle*> column_families;
  std::vector<Slice> keys;
  std::vector<std::string> values;

  for ... {
    keys.emplace_back(key);
    column_families.emplace_back(column_family);
  }
  values.resize(keys.size());

  std::vector<Status> statuses = db->MultiGet(ReadOptions(), column_families, keys, &values);
```

For a more in-depth discussion of performance benefits of using MultiGet, see [[MultiGet Performance]].

## Writes
### Atomic Updates

Note that if the process dies after the Put of key2 but before the delete of key1, the same value may be left stored under multiple keys. Such problems can be avoided by using the <code>WriteBatch</code> class to atomically apply a set of updates:

```cpp
  #include "rocksdb/write_batch.h"
  ...
  std::string value;
  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key1, &value);
  if (s.ok()) {
    rocksdb::WriteBatch batch;
    batch.Delete(key1);
    batch.Put(key2, value);
    s = db->Write(rocksdb::WriteOptions(), &batch);
  }
```

The <code>WriteBatch</code> holds a sequence of edits to be made to the database, and these edits within the batch are applied in order. Note that we called <code>Delete</code> before <code>Put</code> so that if <code>key1</code> is identical to <code>key2</code>, we do not end up erroneously dropping the value entirely.

Apart from its atomicity benefits, <code>WriteBatch</code> may also be used to speed up bulk updates by placing lots of individual mutations into the
same batch.


### Synchronous Writes
 
By default, each write to <code>rocksdb</code> is asynchronous: it returns after pushing the write from the process into the operating system. The transfer from operating system memory to the underlying persistent storage happens asynchronously. The <code>sync</code> flag can be turned on for a particular write to make the write operation not return until the data being written has been pushed all the way to persistent storage. (On Posix systems, this is implemented by calling either <code>fsync(...)</code> or <code>fdatasync(...)</code> or <code>msync(..., MS_SYNC)</code> before the write operation returns.) 

```cpp
  rocksdb::WriteOptions write_options;
  write_options.sync = true;
  db->Put(write_options, ...);
```


### Non-sync Writes 

With non-sync writes, RocksDB only buffers WAL write in OS buffer or internal buffer (when options.manual_wal_flush = true). They are often much faster than synchronous writes. The downside of non-sync writes is that a crash of the machine may cause the last few updates to be lost. Note that a crash of just the writing process (i.e., not a reboot) will not cause any loss since even when <code>sync</code> is false, an update is pushed from the process memory into the operating system before it is considered done.

Non-sync writes can often be used safely. For example, when loading a large amount of data into the database you can handle lost updates by restarting the bulk load after a crash. A hybrid scheme is also possible where `DB::SyncWAL()` is called by a separate thread.


We also provide a way to completely disable Write Ahead Log for a particular write. If you set <code>write_options.disableWAL</code> to true, the write will not go to the log at all and may be lost in an event of process crash.


RocksDB by default uses <code>fdatasync()</code> to sync files, which might be faster than fsync() in certain cases. If you want to use fsync(), you can set <code>Options::use_fsync</code> to true. You should set this to true on filesystems like ext3 that can lose files after a reboot.

### Advanced
For more information about write performance optimizations and factors influencing performance, see [[Pipelined Write]] and [[Write Stalls]].

## Concurrency

A database may only be opened by one process at a time. The <code>rocksdb</code> implementation acquires a lock from the operating system to prevent misuse. Within a single process, the same <code>rocksdb::DB</code> object may be safely shared by multiple concurrent threads. I.e., different threads may write into or fetch iterators or call <code>Get</code> on the same database without any external synchronization (the rocksdb implementation will automatically do the required synchronization). However other objects (like Iterator and WriteBatch) may require external synchronization. If two threads share such an object, they must protect access to it using their own locking protocol. More details are available in the public header files.


## Merge operators

Merge operators provide efficient support for read-modify-write operation.
More on the interface and implementation can be found on:
* [[Merge Operator | Merge-Operator]]
* [[Merge Operator Implementation | Merge-Operator-Implementation]]
* [Get Merge Operands](https://github.com/facebook/rocksdb/wiki/Merge-Operator#get-merge-operands)


## Iteration

The following example demonstrates how to print all (key, value) pairs in a database.

```cpp
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cout << it->key().ToString() << ": " << it->value().ToString() << endl;
  }
  assert(it->status().ok()); // Check for any errors found during the scan
  delete it;
```
The following variation shows how to process just the keys in the
range <code>[start, limit)</code>:

```cpp
  for (it->Seek(start);
       it->Valid() && it->key().ToString() < limit;
       it->Next()) {
    ...
  }
  assert(it->status().ok()); // Check for any errors found during the scan
```
You can also process entries in reverse order. (Caveat: reverse
iteration may be somewhat slower than forward iteration.)

```cpp
  for (it->SeekToLast(); it->Valid(); it->Prev()) {
    ...
  }
  assert(it->status().ok()); // Check for any errors found during the scan
```

This is an example of processing entries in range (limit, start] in reverse order from one specific key:

```cpp
  for (it->SeekForPrev(start);
       it->Valid() && it->key().ToString() > limit;
       it->Prev()) {
    ...
  }
  assert(it->status().ok()); // Check for any errors found during the scan
```
See [[SeekForPrev]].

For explanation of error handling, different iterating options and best practice, see [[Iterator]].

To know about implementation details, see [Iterator's Implementation](https://github.com/facebook/rocksdb/wiki/Iterator-Implementation)

## Snapshots

Snapshots provide consistent read-only views over the entire state of the key-value store. <code>ReadOptions::snapshot</code> may be non-NULL to indicate that a read should operate on a particular version of the DB state. 

If <code>ReadOptions::snapshot</code> is NULL, the read will operate on an implicit snapshot of the current state.

Snapshots are created by the DB::GetSnapshot() method:

```cpp
  rocksdb::ReadOptions options;
  options.snapshot = db->GetSnapshot();
  ... apply some updates to db ...
  rocksdb::Iterator* iter = db->NewIterator(options);
  ... read using iter to view the state when the snapshot was created ...
  delete iter;
  db->ReleaseSnapshot(options.snapshot);
```

Note that when a snapshot is no longer needed, it should be released using the DB::ReleaseSnapshot interface. This allows the implementation to get rid of state that was being maintained just to support reading as of that snapshot.


## Slice

The return value of the <code>it->key()</code> and <code>it->value()</code> calls above are instances of the <code>rocksdb::Slice</code> type. <code>Slice</code> is a simple structure that contains a length and a pointer to an external byte array. Returning a <code>Slice</code> is a cheaper alternative to returning a <code>std::string</code> since we do not need to copy potentially large keys and values. In addition, <code>rocksdb</code> methods do not return null-terminated C-style strings since <code>rocksdb</code> keys and values are allowed to contain '\0' bytes.

C++ strings and null-terminated C-style strings can be easily converted to a Slice:

```cpp
   rocksdb::Slice s1 = "hello";

   std::string str("world");
   rocksdb::Slice s2 = str;
```

A Slice can be easily converted back to a C++ string:

```cpp
   std::string str = s1.ToString();
   assert(str == std::string("hello"));
```

Be careful when using Slices since it is up to the caller to ensure that the external byte array into which the Slice points remains live while the Slice is in use. For example, the following is buggy:

```cpp
   rocksdb::Slice slice;
   if (...) {
     std::string str = ...;
     slice = str;
   }
   Use(slice);
```

When the <code>if</code> statement goes out of scope, <code>str</code> will be destroyed and the backing storage for <code>slice</code> will disappear.

## Transactions
RocksDB now supports multi-operation transactions. See [[Transactions]]

## Comparators

The preceding examples used the default ordering function for key, which orders bytes lexicographically. You can however supply a custom comparator when opening a database. For example, suppose each database key consists of two numbers and we should sort by the first number, breaking ties by the second number. First, define a proper subclass of <code>rocksdb::Comparator</code> that expresses these rules:

```cpp
  class TwoPartComparator : public rocksdb::Comparator {
   public:
    // Three-way comparison function:
    // if a < b: negative result
    // if a > b: positive result
    // else: zero result
    int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
      int a1, a2, b1, b2;
      ParseKey(a, &a1, &a2);
      ParseKey(b, &b1, &b2);
      if (a1 < b1) return -1;
      if (a1 > b1) return +1;
      if (a2 < b2) return -1;
      if (a2 > b2) return +1;
      return 0;
    }

    // Ignore the following methods for now:
    const char* Name() const { return "TwoPartComparator"; }
    void FindShortestSeparator(std::string*, const rocksdb::Slice&) const { }
    void FindShortSuccessor(std::string*) const { }
  };
```

Now create a database using this custom comparator:

```cpp
  TwoPartComparator cmp;
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  options.comparator = &cmp;
  rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
  ...
```

## Column Families
[[Column Families]] provide a way to logically partition the database. Users can provide atomic writes of multiple keys across multiple column families and read a consistent view from them.

## Bulk Load
You can [[Creating and Ingesting SST files]] to bulk load a large amount of data directly into DB with minimum impacts on the live traffic.

## Backup and Checkpoint
[Backup](https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB%3F) allows users to create periodic incremental backups in a remote file system (think about HDFS or S3) and recover from any of them.

[[Checkpoints]] provides the ability to take a snapshot of a running RocksDB database in a separate directory. Files are hardlinked, rather than copied, if possible, so it is a relatively lightweight operation.

## I/O
By default, RocksDB's I/O goes through operating system's page cache. Setting [[Rate Limiter]] can limit the speed that RocksDB issues file writes, to make room for read I/Os.

Users can also choose to bypass operating system's page cache, using [Direct I/O](https://github.com/facebook/rocksdb/wiki/Direct-IO).

See [[IO]] for more details.

## Backwards compatibility

The result of the comparator's <code>Name</code> method is attached to the database when it is created, and is checked on every subsequent database open. If the name changes, the <code>rocksdb::DB::Open</code> call will fail. Therefore, change the name if and only if the new key format and comparison function are incompatible with existing databases, and it is ok to discard the contents of all existing databases.

You can however still gradually evolve your key format over time with a little bit of pre-planning. For example, you could store a version number at the end of each key (one byte should suffice for most uses). When you wish to switch to a new key format (e.g., adding an optional third part to the keys processed by <code>TwoPartComparator</code>), (a) keep the same comparator name (b) increment the version number for new keys (c) change the comparator function so it uses the version numbers found in the keys to decide how to interpret them.



## MemTable and Table factories

By default, we keep the data in memory in skiplist memtable and the data on disk in a table format described here: <a href="https://github.com/facebook/rocksdb/wiki/Rocksdb-Table-Format">RocksDB Table Format</a>.

Since one of the goals of RocksDB is to have different parts of the system easily pluggable, we support different implementations of both memtable and table format. You can supply your own memtable factory by setting <code>Options::memtable_factory</code> and your own table factory by setting <code>Options::table_factory</code>. For available memtable factories, please refer to <code>rocksdb/memtablerep.h</code> and for table factories to <code>rocksdb/table.h</code>. These features are both in active development and please be wary of any API changes that might break your application going forward.

You can also read more about memtable [[here|MemTable]].

## Performance

Start with [[Setup Options and Basic Tuning]]. For more information about RocksDB performance, see the "Performance" section in the sidebar in the right side.


## Block size

<code>rocksdb</code> groups adjacent keys together into the same block and such a block is the unit of transfer to and from persistent storage. The default block size is approximately 4096 uncompressed bytes. Applications that mostly do bulk scans over the contents of the database may wish to increase this size. Applications that do a lot of point reads of small values may wish to switch to a smaller block size if performance measurements indicate an improvement. There isn't much benefit in using blocks smaller than one kilobyte, or larger than a few megabytes. Also note that compression will be more effective with larger block sizes. To change block size parameter, use <code>Options::block_size</code>.

## Write buffer

<code>Options::write_buffer_size</code> specifies the amount of data to build up in memory before converting to a sorted on-disk file. Larger values increase performance, especially during bulk loads. Up to max_write_buffer_number write buffers may be held in memory at the same time, so you may wish to adjust this parameter to control memory usage. Also, a larger write buffer will result in a longer recovery time the next time the database is opened.

Related option is <code>Options::max_write_buffer_number</code>, which is maximum number of write buffers that are built up in memory. The default is 2, so that when 1 write buffer is being flushed to storage, new writes can continue to the other write buffer. The flush operation is executed in a [[Thread Pool]].

<code>Options::min_write_buffer_number_to_merge</code> is the minimum number of write buffers that will be merged together before writing to storage. If set to 1, then all write buffers are flushed to L0 as individual files and this increases read amplification because a get request has to check all of these files. Also, an in-memory merge may result in writing lesser data to storage if there are duplicate records in each of these individual write buffers. Default: 1

## Compression

Each block is individually compressed before being written to persistent storage. Compression is on by default since the default compression method is very fast, and is automatically disabled for uncompressible data. In rare cases, applications may want to disable compression entirely, but should only do so if benchmarks show a performance improvement:

```cpp
  rocksdb::Options options;
  options.compression = rocksdb::kNoCompression;
  ... rocksdb::DB::Open(options, name, ...) ....
```

Also [[Dictionary Compression]] is also available.

## Cache

The contents of the database are stored in a set of files in the filesystem and each file stores a sequence of compressed blocks. If <code>options.block_cache</code> is non-NULL, it is used to cache frequently used uncompressed block contents. We use operating systems file cache to cache our raw data, which is compressed. So file cache acts as a cache for compressed data.

```cpp
  #include "rocksdb/cache.h"
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = rocksdb::NewLRUCache(100 * 1048576); // 100MB uncompressed cache

  rocksdb::Options options;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
  rocksdb::DB* db;
  rocksdb::DB::Open(options, name, &db);
  ... use the db ...
  delete db
```

When performing a bulk read, the application may wish to disable caching so that the data processed by the bulk read does not end up displacing most of the cached contents. A per-iterator option can be used to achieve this:

```cpp
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  rocksdb::Iterator* it = db->NewIterator(options);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ...
  }
```

You can also disable block cache by setting <code>options.no_block_cache</code> to true.

See [[Block Cache]] for more details.

## Key Layout

Note that the unit of disk transfer and caching is a block. Adjacent keys (according to the database sort order) will usually be placed in the same block. Therefore the application can improve its performance by placing keys that are accessed together near each other and placing infrequently used keys in a separate region of the key space.

For example, suppose we are implementing a simple file system on top of <code>rocksdb</code>. The types of entries we might wish to store are:

```cpp
   filename -> permission-bits, length, list of file_block_ids
   file_block_id -> data
```

We might want to prefix <code>filename</code> keys with one letter (say '/') and the <code>file_block_id</code> keys with a different letter (say '0') so that scans over just the metadata do not force us to fetch and cache bulky file contents.


## Filters

Because of the way <code>rocksdb</code> data is organized on disk, a single <code>Get()</code> call may involve multiple reads from disk. The optional <code>FilterPolicy</code> mechanism can be used to reduce the number of disk reads substantially.

```cpp
   rocksdb::Options options;
   rocksdb::BlockBasedTableOptions bbto;
   bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(
       10 /* bits_per_key */,
       false /* use_block_based_builder*/));
   options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
   rocksdb::DB* db;
   rocksdb::DB::Open(options, "/tmp/testdb", &db);
   ... use the database ...
   delete db;
   delete options.filter_policy;
```

The preceding code associates a [[Bloom Filter | RocksDB-Bloom-Filter]] based filtering policy with the database. Bloom filter based filtering relies on keeping some number of bits of data in memory per key (in this case 10 bits per key since that is the argument we passed to NewBloomFilter). This filter will reduce the number of unnecessary disk reads needed for <code>Get()</code> calls by a factor of approximately a 100. Increasing the bits per key will lead to a larger reduction at the cost of more memory usage. We recommend that applications whose working set does not fit in memory and that do a lot of random reads set a filter policy.

If you are using a custom comparator, you should ensure that the filter policy you are using is compatible with your comparator. For example, consider a comparator that ignores trailing spaces when comparing keys. <code>NewBloomFilter</code> must not be used with such a comparator. Instead, the application should provide a custom filter policy that also ignores trailing spaces. 

For example:

```cpp
  class CustomFilterPolicy : public rocksdb::FilterPolicy {
   private:
    FilterPolicy* builtin_policy_;
   public:
    CustomFilterPolicy() : builtin_policy_(NewBloomFilter(10, false)) { }
    ~CustomFilterPolicy() { delete builtin_policy_; }

    const char* Name() const { return "IgnoreTrailingSpacesFilter"; }

    void CreateFilter(const Slice* keys, int n, std::string* dst) const {
      // Use builtin bloom filter code after removing trailing spaces
      std::vector<Slice> trimmed(n);
      for (int i = 0; i < n; i++) {
        trimmed[i] = RemoveTrailingSpaces(keys[i]);
      }
      return builtin_policy_->CreateFilter(&trimmed[i], n, dst);
    }

    bool KeyMayMatch(const Slice& key, const Slice& filter) const {
      // Use builtin bloom filter code after removing trailing spaces
      return builtin_policy_->KeyMayMatch(RemoveTrailingSpaces(key), filter);
    }
  };
```

Advanced applications may provide a filter policy that does not use a bloom filter but uses some other mechanisms for summarizing a set of keys. See <code>rocksdb/filter_policy.h</code> for detail.

## Checksums

<code>rocksdb</code> associates checksums with all data it stores in the file system. There are two separate controls provided over how aggressively these checksums are verified:

<ul>
<li> 
<code>ReadOptions::verify_checksums</code> forces checksum verification of all data that is read from the file system on behalf of a particular read. This is on by default.

<li> <code>Options::paranoid_checks</code> may be set to true before opening a database to make the database implementation raise an error as soon as it detects an internal corruption. Depending on which portion of the database has been corrupted, the error may be raised when the database is opened, or later by another database operation. By default, paranoid checking is on.
</ul>

Checksum verification can also be manually triggered by calling ```DB::VerifyChecksum()```. This API walks through all the SST files in all levels for all column families, and for each SST file, verifies the checksum embedded in the metadata and data blocks. At present, it is only supported for the BlockBasedTable format. The files are verified serially, so the API call may take a significant amount of time to finish. This API can be useful for proactive verification of data integrity in a distributed system, for example, where a new replica can be created if the database is found to be corrupt.

If a database is corrupted (perhaps it cannot be opened when paranoid checking is turned on), the <code>rocksdb::RepairDB</code> function may be used to recover as much of the data as possible.



## Compaction

RocksDB keeps rewriting existing data files. This is to clean stale versions of keys, and to keep the data structure optimal for reads.

The information about compaction has been moved to [Compaction](https://github.com/facebook/rocksdb/wiki/Compaction). Users don't have to know internal of compactions before operating RocksDB.

## Approximate Sizes

The <code>GetApproximateSizes</code> method can be used to get the approximate number of bytes of file system space used by one or more key ranges.

```cpp
   rocksdb::Range ranges[2];
   ranges[0] = rocksdb::Range("a", "c");
   ranges[1] = rocksdb::Range("x", "z");
   uint64_t sizes[2];
   db->GetApproximateSizes(ranges, 2, sizes);
```

The preceding call will set <code>sizes[0]</code> to the approximate number of bytes of file system space used by the key range <code>[a..c)</code> and <code>sizes[1]</code> to the approximate number of bytes used by the key range <code>[x..z)</code>.


## Environment

All file operations (and other operating system calls) issued by the <code>rocksdb</code> implementation are routed through a <code>rocksdb::Env</code> object. Sophisticated clients may wish to provide their own <code>Env</code> implementation to get better control. For example, an application may introduce artificial delays in the file IO paths to limit the impact of <code>rocksdb</code> on other activities in the system.

```cpp
  class SlowEnv : public rocksdb::Env {
    .. implementation of the Env interface ...
  };

  SlowEnv env;
  rocksdb::Options options;
  options.env = &env;
  Status s = rocksdb::DB::Open(options, ...);
```


## Porting

<code>rocksdb</code> may be ported to a new platform by providing platform specific implementations of the types/methods/functions exported by <code>rocksdb/port/port.h</code>. See <code>rocksdb/port/port_example.h</code> for more details.

In addition, the new platform may need a new default <code>rocksdb::Env</code> implementation. See <code>rocksdb/util/env_posix.h</code> for an example.


## Manageability

To be able to efficiently tune your application, it is always helpful if you have access to usage statistics. You can collect those statistics by setting <code>Options::table_properties_collectors</code> or <code>Options::statistics</code>. For more information, refer to <code>rocksdb/table_properties.h</code> and <code>rocksdb/statistics.h</code>. These should not add significant overhead to your application and we recommend exporting them to other monitoring tools. See [[Statistics]]. You can also profile single requests using [[Perf Context and IO Stats Context]]. Users can register [[EventListener]] for callbacks for some internal events.


## Purging WAL files

By default, old write-ahead logs are deleted automatically when they fall out of scope and application doesn't need them anymore. There are options that enable the user to archive the logs and then delete them lazily, either in TTL fashion or based on size limit.

The options are <code>Options::WAL_ttl_seconds</code> and <code>Options::WAL_size_limit_MB</code>. Here is how they can be used:
<ul>
<li>

If both set to 0, logs will be deleted asap and will never get into the archive.
<li>

If <code>WAL_ttl_seconds</code> is 0 and WAL_size_limit_MB is not 0, WAL files will be checked every 10 min and if total size is greater then <code>WAL_size_limit_MB</code>, they will be deleted starting with the earliest until size_limit is met. All empty files will be deleted.
<li>

If <code>WAL_ttl_seconds</code> is not 0 and WAL_size_limit_MB is 0, then WAL files will be checked every <code>WAL_ttl_seconds / 2</code> and those that are older than WAL_ttl_seconds will be deleted.
<li>

If both are not 0, WAL files will be checked every 10 min and both checks will be performed with ttl being first.
</ul>


## Other Information
To set up RocksDB options:
* [Set Up Options And Basic Tuning](https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
* Some detailed [Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)

Details about the <code>rocksdb</code> implementation may be found in the following documents:
* [RocksDB Overview and Architecture](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview)
* [Format of an immutable Table file](https://github.com/facebook/rocksdb/wiki/Rocksdb-Table-Format)
* [Format of a Write Ahead Log file](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format)
</ul>