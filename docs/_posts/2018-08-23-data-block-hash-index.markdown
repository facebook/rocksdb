---
title: Improving Point-Lookup Performance in RocksDB Using Data Block Hash Index
layout: post
author: fgwu
category: blog
---
RocksDB does a binary search when performing point lookup the keys in data blocks. However, in order to find the right location where the key may reside, multiple key parsing and comparison are needed. Each binary search branching triggers CPU cache miss, causing much CPU utilization. We have seen that this binary search takes up considerable CPU in production use-cases.

A HashIndex is designed and implemented in RocksDB to improve the CPU efficiency of point lookup. `db_bench` evaluation shows that at an overhead of 4.6% more space, the CPU utilization of point lookup function is reduced by 21.8% and the overall throughput is increased by 10% under in-memory workloads.

# How to use it
It is disabled by default unless `BlockBasedTableOptions::data_block_index_type` is set to `data_block_index_type = kDataBlockBinaryAndHash`. The hash table utilization ratio is adjustable using `BlockBasedTableOptions::data_block_hash_table_util_ratio`.

```
// the definitions can be found in include/rocksdb/table.h

// The index type that will be used for the data block.
enum DataBlockIndexType : char {
  kDataBlockBinarySearch = 0,  // traditional block type
  kDataBlockBinaryAndHash = 1, // additional hash index
};

DataBlockIndexType data_block_index_type = kDataBlockBinarySearch;

// #entries/#buckets. It is valid only when data_block_hash_index_type is
// kDataBlockBinaryAndHash.
double data_block_hash_table_util_ratio = 0.75;

```


# Data Block Hash Index Design
RocksDB does a binary search when performing point lookup the keys in data blocks.
![](/static/images/data-block-hash-index/block-format-binary-seek.png)

We implement hash map at the end of the block to index the key into reduce the CPU utilization of the point lookup.
![](/static/images/data-block-hash-index/block-format-hash-index.png)

The hash index is just an array of pointers pointing to the binary seek index.

Each array element is considered as a hash bucket when storing the location of a key (or more precisely, the restart index of the restart interval where the key resides). When multiple keys happen to hash into the same bucket (hash collision), we just mark the bucket as “collision”. So that when later querying on that key, a hash table lookup can find there was a hash collision happened and can fall back to the traditional binary seek to find the location of the key.

We define hash table utilization ratio as the #keys/#buckets. If a utilization ratio is 0.5 and there are 100 buckets, 50 keys are stored in the bucket. The trade-off between the space and CPU can be tuned by the hash table util ratio. The less the util ratio, the less hash collision, and the less chance for a point lookup falls back to binary seek (fall back ratio) due to the collision. So a small util ratio has more benefit to reduce the CPU time but increase space overhead.

Space overhead depends on the util ratio. Each bucket is a `uint8_t`  (i.e. one byte). For a util ratio of 1, the space overhead is 1Byte per key, the fall back ratio observed is ~52%.

![](/static/images/data-block-hash-index/hash-index-data-structure.png)

# Things that Needs Attention
### Customized Comparator

Hash index will hash different keys (keys with different content, or byte sequence) into different hash values. This assumes the comparator will not treat different keys as equal if they have different content. 

The default bytewise comparator orders the keys in alphabetical order and works well with hash index, as different keys will never be regarded as equal. However, some specially crafted comparators will do. For example, a StringToIntComparator can convert strings into integer, and use the integer to perform the comparison. Key string “16” and “0x10” is equal to each other as seen by this StringToIntComparator, but they probably hash to different value. Later queries to one form of the key will not be able to find one been stored in the other format.

We add a new function member to the comparator interface: 
```
virtual bool CanKeysWithDifferentByteContentsBeEqual() const { return true; }
```
Every comparator implementation should override this function and specify the behavior of the comparator. If a comparator can regard different keys equal, the function returns true, and the hash index feature is not enabled, and vice versa.

NOTE: to use the hash index feature, one should 1) have a comparator that can never treat different keys as equal; and 2) override the CanKeysWithDifferentByteContentsBeEqual() function to return false, so the hash index can be enabled.


### Util Ratio's Impact on Data Block Cache

Adding hash index to the end of the data block esstentially takes up the data block cache space, making the effective data lbock cache size smaller and inceasing the data block cache miss ratio. Therefore, a very small util ratio will result in a large data block cache miss ratio, the extra I/O may drag down the thoughput gain achieved by the hash index lookup. Besides, when compression is enabled, cache mission also incurs data block decompression, which is CPU-consuming. Therefore the CPU may increase in this case.  The best util ratio depends on workloads, cache to data ratio, disk bandwith/latency etc. In our experiment we found util ratio = 1 ~ 0.5 is a good range to explore that brings both CPU and throughtput gains.

# Limitations

As we use uint8_t to store binary seek index, i.e. restart interval index, the total number to restart intervals cannot be more than 253 (we reserved  255 and 254 as special flags). For blocks having larger number of restart intervals, hash index will not be created and the point lookup will be done by traditional binary seek.

Hash index only support point lookup. Range query, iteration, etc. are still handled by binary seek.

DataBlockHashIndex only supports point lookup. We do not support range lookup. Range lookup request will be fall back to BinarySeek.

We have a limit set of supported record types:

```
kPutRecord,          <=== supported
kDeleteRecord,       <=== supported
kSingleDeleteRecord, <=== supported
kTypeBlobIndex,      <=== supported
```

For records not supported, the searching process will fall back to the traditional binary seek. 





