RocksDB does a binary search when performing point lookup the keys in data blocks. However, in order to find the right location where the key may reside, multiple key parsing and comparison are needed. Each binary search branching triggers CPU cache miss, causing much CPU utilization. We have seen that this binary search takes up considerable CPU in production use-cases.

A hash index is designed and implemented in RocksDB data blocks to improve the CPU efficiency of point lookup. Benchmarks with `db_bench`  show the CPU utilization of one of the main functions in the point lookup code path, `DataBlockIter::Seek()`, is reduced by 21.8%, and the overall RocksDB throughput is increased by 10% under purely cached workloads, at an overhead of 4.6% more space.

### How to use it
Two new options are added as part of this feature: `BlockBasedTableOptions::data_block_index_type` and `BlockBasedTableOptions::data_block_hash_table_util_ratio`.

The hash index is disabled by default unless `BlockBasedTableOptions::data_block_index_type` is set to `data_block_index_type = kDataBlockBinaryAndHash`. The hash table utilization ratio is adjustable using `BlockBasedTableOptions::data_block_hash_table_util_ratio`, which is valid only if `data_block_index_type = kDataBlockBinaryAndHash`.


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

### Things that Need Attention

**Customized Comparator**

Hash index will hash different keys (keys with different content, or byte sequence) into different hash values. This assumes the comparator will not treat different keys as equal if they have different content. 

The default bytewise comparator orders the keys in alphabetical order and works well with hash index, as different keys will never be regarded as equal. However, some specially crafted comparators will do. For example, say, a `StringToIntComparator` can convert a string into an integer, and use the integer to perform the comparison. Key string “16” and “0x10” is equal to each other as seen by this `StringToIntComparator`, but they probably hash to different value. Later queries to one form of the key will not be able to find the existing key been stored in the other format.

We add a new function member to the comparator interface: 

```
virtual bool CanKeysWithDifferentByteContentsBeEqual() const { return true; }
```


Every comparator implementation should override this function and specify the behavior of the comparator. If a comparator can regard different keys equal, the function returns true, and as a result the hash index feature will not be enabled, and vice versa.

NOTE: to use the hash index feature, one should 1) have a comparator that can never treat different keys as equal; and 2) override the `CanKeysWithDifferentByteContentsBeEqual()` function to return `false`, so the hash index can be enabled.


**Util Ratio's Impact on Data Block Cache**

Adding the hash index to the end of the data block essentially takes up the data block cache space, making the effective data block cache size smaller and increasing the data block cache miss ratio. Therefore, a very small util ratio will result in a large data block cache miss ratio, and the extra I/O may drag down the throughput gain achieved by the hash index lookup. Besides, when compression is enabled, cache miss also incurs data block decompression, which is CPU-consuming. Therefore the CPU may even increase if using a too small util ratio. The best util ratio depends on workloads, cache to data ratio, disk bandwidth/latency etc. In our experiment, we found util ratio = 0.5 ~ 1 is a good range to explore that brings both CPU and throughput gains.


### Limitations

As we use `uint8_t` to store binary seek index, i.e. restart interval index, the total number of restart intervals cannot be more than 253 (we reserved  255 and 254 as special flags). For blocks having a larger number of restart intervals, the hash index will not be created and the point lookup will be done by traditional binary seek.

Data block hash index only supports point lookup. We do not support range lookup. Range lookup request will fall back to BinarySeek.

RocksDB supports many types of records, such as `Put`, `Delete`, `Merge`, etc (visit [here](https://github.com/facebook/rocksdb/wiki/rocksdb-basics) for more information). Currently we only support `Put` and `Delete`, but not `Merge`. Internally we have a limited set of supported record types:


```
kPutRecord,          <=== supported
kDeleteRecord,       <=== supported
kSingleDeleteRecord, <=== supported
kTypeBlobIndex,      <=== supported
```

For records not supported, the searching process will fall back to the traditional binary seek. 
