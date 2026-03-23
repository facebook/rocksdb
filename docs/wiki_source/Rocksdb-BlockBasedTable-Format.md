This page is forked from LevelDB's document on [table format](https://github.com/google/leveldb/blob/master/doc/table_format.md), and reflects changes we have made during the development of RocksDB.

BlockBasedTable is the default SST table format in RocksDB.

### File format

    <beginning_of_file>
    [data block 1]
    [data block 2]
    ...
    [data block N]
    [meta block 1: filter block]                  (see section: "filter" Meta Block)
    [meta block 2: index block]
    [meta block 3: compression dictionary block]  (see section: "compression dictionary" Meta Block)
    [meta block 4: range deletion block]          (see section: "range deletion" Meta Block)
    [meta block 5: stats block]                   (see section: "properties" Meta Block)
    ...
    [meta block K: future extended block]  (we may add more meta blocks in the future)
    [metaindex block]
    [Footer]                               (fixed size; starts at file_size - sizeof(Footer))
    <end_of_file>

The file contains internal pointers, called `BlockHandles`, containing the following information:

    offset:         varint64
    size:           varint64

See [this document](https://developers.google.com/protocol-buffers/docs/encoding#varints) for an explanation of varint64 format.

(1) The sequence of key/value pairs in the file are stored in sorted order and partitioned into a sequence of data blocks. These blocks come one after another at the beginning of the file. Each data block is formatted according to the code in `block_builder.cc` (see code comments in the file), and then optionally compressed.

(2) After the data blocks, we store a bunch of meta blocks. The supported meta block types are described below. More meta block types may be added in the future. Each meta block is again formatted using `block_builder.cc` and then optionally compressed.

(3) A `metaindex` block contains one entry for every meta block, where the key is the name of the meta block and the value is a `BlockHandle` pointing to that meta block.

(4) At the very end of the file is a fixed length footer that contains the `BlockHandle` of the `metaindex` and index blocks as well as a magic number.

       metaindex_handle: char[p];      // Block handle for metaindex
       index_handle:     char[q];      // Block handle for index
       padding:          char[40-p-q]; // zeroed bytes to make fixed length
                                       // (40==2*BlockHandle::kMaxEncodedLength)
       magic:            fixed64;      // 0x88e241b785f4cff7 (little-endian)

#### `Index` Block
Index blocks are used to look up a data block containing the range including a lookup key. It is a binary search data structure. A file may contain one index block, or a list of partitioned index blocks (see [[Partitioned Index Filters]]).
Index block format is documented here: [[Index Block Format]].

#### `Filter` Meta Block

Note: `format_version`=5 (Since RocksDB 6.6) uses a faster and more accurate [Bloom filter implementation](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter) for full and partitioned filters.

##### Full filter

In this filter there is one filter block for the entire SST file.

##### Partitioned Filter

The full filter is partitioned into multiple blocks. A top-level index block is added to map keys to corresponding filter partitions. Read more [here](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters).

##### Block-based filter
> Note: the below explains block based filter, which is deprecated.

If a "FilterPolicy" was specified when the database was opened, a filter block is stored in each table. The "metaindex" block contains an entry that maps from "filter.<N>" to the BlockHandle for the filter block, where "<N>" is the string returned by the filter policy's `Name()` method.

The filter block stores a sequence of filters, where filter `i` contains the output of FilterPolicy::CreateFilter() on all keys that are stored in a block whose file offset falls within the range

    [ i*base ... (i+1)*base-1 ]

Currently, "base" is 2KB. So, for example, if blocks X and Y start in the range [ 0KB .. 2KB-1 ], all of the keys in X and Y will be converted to a filter by calling FilterPolicy::CreateFilter(), and the resulting filter will be stored as the first filter in the filter block.

The filter block is formatted as follows:

     [filter 0]
     [filter 1]
     [filter 2]
     ...
     [filter N-1]

     [offset of filter 0]                  : 4 bytes
     [offset of filter 1]                  : 4 bytes
     [offset of filter 2]                  : 4 bytes
     ...
     [offset of filter N-1]                : 4 bytes

     [offset of beginning of offset array] : 4 bytes
     lg(base)                              : 1 byte

The offset array at the end of the filter block allows efficient mapping from a data block offset to the corresponding filter.

#### `Properties` Meta Block

This meta block contains a bunch of properties. The key is the name of the property. The value is the property.

The stats block is formatted as follows:
    
     [prop1]    (Each property is a key/value pair)
     [prop2]
     ...
     [propN]
   
Properties are guaranteed to sort with no duplication.

By default, each table provides the following properties.

     data size               // the total size of all data blocks. 
     index size              // the size of the index block.
     filter size             // the size of the filter block.
     raw key size            // the size of all keys before any processing.
     raw value size          // the size of all value before any processing.
     number of entries
     number of data blocks

RocksDB also provides users the "callback" to collect their interested properties about this table. Please refer to `UserDefinedPropertiesCollector`.

#### `Compression Dictionary` Meta Block

This metablock contains the dictionary used to prime the compression library before compressing/decompressing each block. Its purpose is to address a fundamental problem with dynamic dictionary compression algorithms on small data blocks: the dictionary is built during a single pass over the block, so small data blocks always have small and thus ineffective dictionaries.

Our solution is to initialize the compression library with a dictionary built from data sampled from previously seen blocks. This dictionary is then stored in a file-level meta-block for use during decompression. The upper-bound on the size of this dictionary is configurable via `CompressionOptions::max_dict_bytes`. By default it is zero, i.e., the block is not generated or stored. Currently this feature is supported with `kZlibCompression`, `kLZ4Compression`, `kLZ4HCCompression`, and `kZSTDNotFinalCompression`.

More specifically, the compression dictionary is built only during compaction to the bottommost level, where the data is largest and most stable. To avoid iterating over input data multiple times, the dictionary includes samples from the subcompaction's first output file only. Then, the dictionary is applied to and stored in meta-blocks of all subsequent output files. Note the dictionary is not applied to or stored in the first file since its contents are not finalized until that file has been fully processed.

Currently the sampling is uniformly random and each sample is 64 bytes. We do not know in advance the size of the output file when selecting the sample offsets, so we assume it'll reach the maximum size, which is usually true since it's the first file in the subcompaction. In case the file is smaller, some sample intervals will refer to offsets beyond EOF, which just means the dictionary will be a bit smaller than `CompressionOptions::max_dict_bytes`.

#### `Range Deletion` Meta Block

This metablock contains the range deletions in the file's key-range and seqnum-range. Range deletions cannot be inlined in the data blocks together with point data since the ranges would then not be binary searchable.

The block format is the standard key-value format. A range deletion is encoded as follows:

- User key: the range's begin key
- Sequence number: the sequence number at which the range deletion was inserted to the DB
- Value type: kTypeRangeDeletion
- Value: the range's end key

Range deletions are assigned sequence numbers when inserted using the same mechanism as non-range data types (puts, deletes, etc.). They also traverse through the LSM using the same flush/compaction mechanism as point data. They can be obsoleted (i.e., dropped) only during compaction to the bottommost level.