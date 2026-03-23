### Introduction
PlainTable is a RocksDB's SST file format optimized for low query latency on pure-memory or really low-latency media.
 
Advantages:
* An in-memory index is built to replace plain binary search with hash + binary search
* Bypassing block cache to avoid the overhead of block copy and LRU cache maintenance.
* Avoid any memory copy when querying (mmap)
 
Limitations:
* File size needs to be smaller than 31 bits integer.
* Data compression is not supported
* Delta encoding is not supported
* `Iterator.Prev()` is not supported
* Non-prefix-based `Seek()` is not supported
* Table loading is slower for building indexes

We have plan to reduce some of the limitations.
 
### Usage

You can call two factory functions `NewPlainTableFactory()` or `NewTotalOrderPlainTableFactory()` in `table.h` to generate a table factory for plain table with your parameters and pass it to `Options.table_factory` or `ColumnFamilyOptions.table_factory`. You need to specify a prefix extractor if you use the former function. Examples:

```cpp
options.table_factory.reset(NewPlainTableFactory());
options.prefix_extractor.reset(NewFixedPrefixTransform(8));
```

or

```cpp
options.table_factory.reset(NewTotalOrderPlainTableFactory());
```

See comments of the two functions in [`include/rocksdb/table.h`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/table.h) for explanation to the parameters.

`NewPlainTableFactory()` creates a plain table factory for plain tables with hash-based index using key prefixes. It is what `PlainTable` is optimized for.

While `NewTotalOrderPlainTableFactory()` doesn't require a prefix extractor and uses a totally binary index. This function is mainly to make `PlainTable` feature-complete. We haven't yet highly optimized query performance in this case.

### File Format
#### Basic
```cpp
    <beginning_of_file>
      [data row1]
      [data row1]
      [data row1]
      ...
      [data rowN]
      [Property Block]
      [Footer]                               (fixed size; starts at file_size - sizeof(Footer))
    <end_of_file>
```
 
Format of property block and footer is the same as [BlockBasedTable format](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format)

See [Row Format](#row-format) for the format of each data row.
 
Two properties in property block are used to read data:

```cpp
    data_size: the end of data part of the file

    fixed_key_len: length of the key if all keys have the same length, 0 otherwise.
```

#### Row Format
Each data row is encoded as:
```cpp
    <beginning of a row>
      encoded key
      length of value: varint32
      value bytes
    <end of a row>
```

See [Key Encoding](#key-encoding) for format of encoded key.
 
#### Key Encoding
There are two encoding types to the key: `kPlain` and `kPrefix`, which can be specified when creating the plain table factory.

##### Plain Encoding
If fixed key length is given, the plain internal key is encoded.

If the fixed key length is not given, the key is variable length and will be encoded as

```cpp
[length of key: varint32] + user key + internal bytes
```

See [Internal Bytes Encoding](#internal-bytes-encoding) for details of internal bytes.

##### Prefix Encoding
kPrefix encoding type is a special delta encoding, in which if a row shows the same prefix (determined by prefix extractor given by the user) as the previous key, we can avoid to repeat the prefix part of the key.

In that case, there are three cases to encode a key (remember all keys are sorted so the keys sharing the same prefixes are together):

* the first key of a prefix, where a full key need to be written
* the second key of a prefix, where the prefix length needs to be recorded, as well as bytes other than prefix (to simplify, we call it suffix here)
* the third or later key of a prefix, where only the suffix part needs to be written.

We defined three flags for indicate full key, a prefix, as well as a suffix. For all the three cases, we need a size with it. They are encoded in this format:

The 8 bits of the first byte:

    +----+----+----+----+----+----+----+----+
    |  Type   |            Size             |
    +----+----+----+----+----+----+----+----+

The first 2 bits indicate full key (00), prefix (01), or suffix (10). The last 6 bits are for size. If the size bits are not all 1, it means the size of the key. Otherwise, varint32 is written after this byte. This varint32 value + 0x3F (the value of all 1) will be the key size. In this way, shorter keys only need one byte.

Here are the formats for the three cases mentioned:

(1) Full Key

    +----------------------+---------------+----------------+
    | Full Key Flag + Size | Full User Key | Internal Bytes |
    +----------------------+---------------+----------------+

(2) The second key of the prefix

    +--------------------+--------------------+------------+----------------+
    | Prefix Flag + Size | Suffix Flag + Size | Key Suffix | Internal Bytes |
    +--------------------+--------------------+------------+----------------+

(3) The third and later key of the prefix:

    +--------------------+------------+----------------+
    | Suffix Flag + Size | Key Suffix | Internal Bytes |
    +--------------------+------------+----------------+

See [Internal Bytes Encoding](#internal-bytes-encoding) for details of internal bytes for all the three cases.

With this format, without knowing the prefix, rows key only be seek using file offset of a full key. So if there are too many keys in a prefix, plain table builder might determine to rewrite the full key again, even if it is not the first key of the prefix, to make seek easier.

Here is an example, we for following keys (prefix and suffix are separated by spaces):

    AAAA AAAB
    AAAA AAABA
    AAAA AAAC
    AAABB AA
    AAAC AAAB

Will be encoded like this:

    FK 8 AAAAAAAB
    PF 4 SF 5 AAABA
    SF 4 AAAC
    FK 7 AAABBAA
    FK 8 AAACAAAB

(where FK means full key flag, PF means prefix flag and SF means suffix flag.)

##### Internal Bytes Encoding
In both of Plain and Prefix encoding type, internal bytes of the internal key are encoded in the same way.
In RocksDB, Internal bytes of a key include a row type (value, delete, merge, etc) and a sequence ID. Normally, a key is laid out in this format:

    +----------- ...... -------------+----+---+---+---+---+---+---+---+
    |       user key                 |type|      sequence ID          |
    +----------- ..... --------------+----+---+---+---+---+---+---+---+

where type takes one byte and sequence ID takes 7 bytes.

In Plain Table format, it is also the normal way one key can be optimized. Furthermore, we have an optimization to save some extra bytes for a common case: value type with sequence ID 0. In RocksDB, we have an optimization to fill sequence ID to be 0 for a key when we are sure there is no previous value for this key in the system (to be specifically, the first key of the last level for level-style compaction or last file in universal style) to enable better compression or encoding. In PlainTable, we use 1 byte "0x80" instead of 8 bytes for the internal bytes:

     +----------- ...... -------------+----+
     |       user key                 |0x80|
     +----------- ..... --------------+----+
 
### In-Memory Index Format

#### Basic Idea

In-memory Index is built to be as compact as possible. On top level, the index is a hash table with each bucket to be either offset in the file or a binary search index. The binary search is needed in two cases:

(1) Hash collisions: two or more prefixes are hashed to the same bucket.

(2) Too many keys for one prefix: need to speed-up the look-up inside the prefix.

#### Format

The index consists of two pieces of memory: an array for hash buckets, and a buffer containing the binary searchable indexes (call it "binary search buffer" below, and "file" as the SST file). 

Key is hashed to buckets based on hash of its prefix (extracted using Options.prefix_extractor).

    +--------------+------------------------------------------------------+
    | Flag (1 bit) | Offset to binary search buffer or file (31 bits)     +
    +--------------+------------------------------------------------------+

If Flag = 0 and offset field equals to the offset of end of the data of the file, it means null - no data for this bucket; if the offset is smaller, it means there is only one prefix for the bucket, starting from that file offset. If Flag = 1, it means the offset is for binary search buffer. The format from that offset is shown below.

Starting from the offset of binary search buffer, a binary search index is encoded as following:

    <begin>
      number_of_records:  varint32
      record 1 file offset:  fixedint32
      record 2 file offset:  fixedint32
      ....
      record N file offset:  fixedint32
    <end>

where N = number_of_records. The offsets are in ascending order.

The reason for only storing 31-bit offset and use 1-bit to identify whether a binary search is needed is to make the index compact.


#### An Example of Index

Let's assume here are the contents of a file:

    +----------------------------+ <== offset_0003_0000 = 0
    | row (key: "0003 0000")     |
    +----------------------------+ <== offset_0005_0000
    | row (key: "0005 0000")     |
    +----------------------------+
    | row (key: "0005 0001")     |
    +----------------------------+
    | row (key: "0005 0002")     |
    +----------------------------+
    |                            |
    |    ....                    |
    |                            |
    +----------------------------+
    | row (key: "0005 000F")     |
    +----------------------------+ <== offset_0005_0010
    | row (key: "0005 0010")     |
    +----------------------------+
    |                            |
    |    ....                    |
    |                            |
    +----------------------------+
    | row (key: "0005 001F")     |
    +----------------------------+ <== offset_0005_0020
    | row (key: "0005 0020")     |
    +----------------------------+
    | row (key: "0005 0021")     |
    +----------------------------+
    | row (key: "0005 0022")     |
    +----------------------------+ <== offset_0007_0000
    | row (key: "0007 0000")     |
    +----------------------------+
    | row (key: "0007 0001")     |
    +----------------------------+ <== offset_0008_0000
    | row (key: "0008 0000")     |
    +----------------------------+
    | row (key: "0008 0001")     |
    +----------------------------+
    | row (key: "0008 0002")     |
    +----------------------------+
    |                            |
    |    ....                    |
    |                            |
    +----------------------------+
    | row (key: "0008 000F")     |
    +----------------------------+ <== offset_0008_0010
    | row (key: "0008 0010")     |
    +----------------------------+ <== offset_end_data
    |                            |
    | property block and footer  |
    |                            |
    +----------------------------+

Let's assume in the example, we use 2 bytes fixed length prefix and in each prefix, rows are always incremented by 1.

Now we are building index for the file. By scanning the file, we know there are 4 distinct prefixes ("0003", "0005", "0007" and "0008") and assume we pick to use 5 hash buckets and based on the hash function, prefixes are hashed into the buckets:

    bucket 0: 0005
    bucket 1: empty
    bucket 2: 0007
    bucket 3: 0003 0008
    bucket 4: empty

Bucket 2 doesn't need binary search since there is only one prefix in it ("0007") and it has only 2 (<16) rows.

Bucket 0 needs binary search because prefix 0005 has more than 16 rows.

Bucket 3 needs binary search because it contains more than one prefix.

We need to allocate binary search indexes for bucket 0 and 3. Here are the result:

    +---------------------+ <== bs_offset_bucket_0
    +  2 (in varint32)    |
    +---------------------+----------------+
    +  offset_0005_0000 (in fixedint32)    |
    +--------------------------------------+
    +  offset_0005_0010 (in fixedint32)    |
    +---------------------+----------------+ <== bs_offset_bucket_3
    +  3 (in varint32)    |
    +---------------------+----------------+
    +  offset_0003_0000 (in fixedint32)    |
    +--------------------------------------+
    +  offset_0008_0000 (in fixedint32)    |
    +--------------------------------------+
    +  offset_0008_0010 (in fixedint32)    |
    +--------------------------------------+

Then here are the data in hash buckets:

    +---+---------------------------------------+
    | 1 |    bs_offset_bucket_0 (31 bits)       |  <=== bucket 0
    +---+---------------------------------------+
    | 0 |    offset_end_data    (31 bits)       |  <=== bucket 1
    +---+---------------------------------------+
    | 0 |    offset_0007_0000   (31 bits)       |  <=== bucket 2
    +---+---------------------------------------+
    | 1 |    bs_offset_bucket_3 (31 bits)       |  <=== bucket 3
    +---+---------------------------------------+
    | 0 |    offset_end_data    (31 bits)       |  <=== bucket 4
    +---+---------------------------------------+


#### Index Look-up

To look up a key, first calculate prefix of the key using Options.prefix_extractor, and find the bucket for the prefix. If the bucket has no record on it (Flag=0 and offset is the offset of data end in file), the key is not found. Otherwise,

If Flag=0, it means there is only one prefix for the bucket and there are not many keys for the prefix, so the offset field points to the file offset of the prefix. We just need to do linear search from there.

If Flag=1, a binary search is needed for this bucket. The binary search index can be retrieved from the offset field. After the binary search, do the linear search from the offset found by the binary search.

#### Building the Index

When building indexes, scan the file. For each key, calculate its prefix, remember (hash value of the prefix, offset) information for the (16n+1)th row of each prefix (n=0,1,2...), starting from the first one. 16 is the maximum number of rows that need to be checked in the linear search following the binary search. By increasing the number, we would save memory consumption for indexes but paying more costs for linear search. Decreasing the number vise verse. Based on the number of prefixes, determine an optimal bucket size. Allocate exact buckets and binary search buffer needed and fill in the indexes according to the bucket size.

#### Bloom Filter
A bloom filter on prefixes can be configured for queries. User can config how many bits are allocated for every prefix. When doing the query (Seek() or Get()), bloom filter is checked and filter out non-existing prefixes before looking up the indexes.

### Future Plan
 
* May consider to materialize the index to be a part of the SST file.
* Add an option to remove the restriction of file size, by trading off memory consumption of indexes.
* May build extra more sparse indexes to enable general iterator seeking.