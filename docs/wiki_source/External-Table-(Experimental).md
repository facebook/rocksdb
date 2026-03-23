# Introduction
RocksDB has a few pre-defined SST formats, such as block based, plain and (now deprecated) cuckoo. External tables allow a user to extend RocksDB by using a custom table file format. This can be useful for specific use cases that demand more performance than the pre-defined formats. It also makes it easier to experiment with new file formats, such as columnar, and evaluate them before deciding if its worth incorporating into core RocksDB.

## Design
External table support is implemented as a table factory that can be configured in ColumnFamilyOptions. When an external table factory is in use, there are several restrictions on that column family. An external table file can only contain Puts. Tombstones, merges, range deletions etc. are not supported. Sequence numbers for KVs is also not supported and each key has an implicit sequence number of 0. While its not enforced, live writes to the CF are not allowed and might result in undefined behavior if it occurs. Taken together, these restrictions mean that external table files are applicable for read-only ingestion only use cases where the files are generated using SstFileWriter and ingested into a column family. The files should be confined to the bottommost level, which can be accomplished by ingesting files withe the atomic_replace ingestion option.

The external table can support one or both of the following layouts -
1. Total order seek - All the keys in the files are in sorted order, and a
   user can seek to the first, last, or any key in between and iterate
   forwards or backwards till the end of the range. To support this mode,
   the implementation needs to use the comparator passed in
   ExternalTableOptions to enforce the key ordering. The prefix_extractor
   in ExternalTableOptions and the ExternalTableReader interfaces can be
   ignored.
2. Prefix seek - In this mode, the prefix_extractor is used to extract the
   prefix from a key. All the keys sharing the same prefix are ordered in
   ascending order according to the comparator. However, no specific
   ordering is required across prefixes. Users can scan keys by seeking
   to a specific key inside a prefix, and iterate forwards or backwards
   within the prefix. The prefix_same_as_start flag in ReadOptions will
   be true.
3. Both - If supporting both of the above, a user can seek inside a prefix
   and iterate beyond the prefix. The prefix_same_as_start in ReadOptions
   will be false. Additionally, the total_order_seek flag can be set to
   true to seek to the first non-empty prefix (as determined by the key
   order) if the seek prefix is empty.

## Interface
An external table format is implemented as a factory class - `ExternalTableFactory`. There are two main methods that must be implemented - `NewTableBuilder()` and `NewTableReader()`. See [external_table.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/external_table.h) for the full interface.

`NewTableBuilder()` should open a file for writing at the given path, construct and return an `ExternalTableBuilder` object. The sequence of operations to write an external table is as follows -
1. Add() is called one or more times to write all key-values to the table. Its called in increasing key order, as determined by the comparator. The input key is a user key, i.e sequence number and value type are stripped out.
2. After every Add() operation, status() is called to check the current status.
3. After the last key is added, Finish() is called to do whatever is necessary to ensure the data is persisted in the table file.
4. If there is a failure midway for some reason, Abandon() is called instead of Finish().
5. At the end, PutTableProperties() is called to write a properties block. FileSize() and status() are called to                                                                                   get the final size of the file and the final status. GetFileChecksum() and GetFileChecksumFuncName() may also be called to get checksum information about the whole file, but their implementation is optional.

`NewTableReader()` is called when the file is loaded, typically during DB open. It should construct and return an `ExternalTableReader` object which implements methods for point lookups and iterator creation. To scan multiple KVs, `NewIterator()` is called to construct an `ExternalTableIterator`. The scan might have a pointer to an upper bound key specified in `iterate_upper_bound` in `ReadOptions`. The `Prepare()` method may optionally be called to allow the iterator to prefetch data upfront for a series of scans.

## Configuration
An external table factory can be configured for a column family as follows -
```
  std::shared_ptr<ExternalTableFactory> my_factory = std::make_shared<MyExternalTableFactory>();
  Options options;
  options.table_factory = NewExternalTableFactory(my_factory);
  // Disable live writes
  options.disallow_memtable_writes = true;
```
See [here](https://github.com/facebook/rocksdb/blob/main/table/table_test.cc#L7101) for an example of the full workflow of creating, ingesting and querying an external table.