An `index` block contains one entry per data block, where the key is a string `>=` last key in that data block and `<` the first key in the successive data block. The value is the `BlockHandle` (file offset and length) for the data block.

### Partitioned Index
If [kTwoLevelIndexSearch](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters) is used as IndexType, the `index` block is a 2nd level index on index partitions, i.e., each entry points to another `index` block that contains one entry per data block. In this case, the format will be

    [index block - 1st level]
    [index block - 1st level]
    ...
    [index block - 1st level]
    [index block - 2nd level]

### Key in index blocks
As described above, the key stored for a block is between the last key of the block and the first key of the next block. There are usually a range of potential keys met this condition. Choosing a smaller one can reduce the index size. If `BlockBasedTableOptions.index_shortening` is set to `kShortenSeparators` or `kShortenSeparatorsAndSuccessor`, the last key of the block, and the first key of the next block will be passed to Comparator::FindShortestSeparator() to find out the shortest separator key. This function is implemented in builtin byte-wise and reverse byte-wise comparators. User comparators need to implement the function to take advantage this feature.

Similarly the index key for the last block is determined by Comparator::FindShortSuccessor(), which provides any key that is greater or equal to the last key of the last block. Users also need to implement this function for customized comparator to take advantage of the memory saving feature.

The default value for the option is `kShortenSeparators`, which shortens index key for all blocks but the last one. It's because that the last key has minimal impacts to index size while can have positive impact of preventing the last data blocks to be read.

Value `kNoShortening` can also be used together with `index_type = kBinarySearchWithFirstKey` to prevent reading some blocks using a special function, which is explained below.

### Individual Index Block
Up to RocksDB version 5.14, `BlockBasedTableOptions::format_version`=2, the format of index and data blocks are the same, where the index blocks use same key format of <`user_key`,`seq`> but special values, <`offset`,`size`>, that point to data blocks. Different from data blocks, the option controlling restart block size is `BlockBasedTableOptions.index_block_restart_interval`, rather than `BlockBasedTableOptions.block_restart_interval`. The default value is 1, rather than 16 for data blocks. So the default is relatively memory costly. Setting the value to 8 or 16 can usually shrink index block size by half, but the CPU overhead might increase based on workloads.
`format_version=`3,4 further optimized size, yet forward-incompatible format for index blocks.
- `format_version`=3 (Since RocksDB 5.15): In most of the cases the sequence number `seq` is not necessary for keys in the index blocks. In such cases, this `format_version` skips encoding the sequence number and sets `index_key_is_user_key` in TableProperties, which is used by the reader to know how to decode the index block.
- `format_version`=4 (Since RocksDB 5.16): Changes the format of index blocks by delta encoding the index values, which are the block handles. This saves the encoding of `BlockHandle::offset` of the non-head index entries in each restart interval. If used, `TableProperties::index_value_is_delta_encoded` is set, which is used by the reader to know how to decode the index block.  The format of each key is (shared_size, non_shared_size, shared, non_shared). The format of each value, i.e., block handle, is {Varint64(offset), Varint64(size)} whenever the shared_size is 0, which included the first entry in each restart point. Otherwise the format is Varsignedint64(delta-sz), where delta-sz = size from current block handle - size from previous block handle.

The index format in `format_version=4` would be as follows:

    restart_point   0: k, v (off, sz), k, v (delta-sz), ..., k, v (delta-sz)
    restart_point   1: k, v (off, sz), k, v (delta-sz), ..., k, v (delta-sz)
    ...
    restart_point n-1: k, v (off, sz), k, v (delta-sz), ..., k, v (delta-sz)
    where, k is key, v is value, and its encoding is in parenthesis.

The format applies to the case when `index_type != kBinarySearchWithFirstKey`. The `kBinarySearchWithFirstKey` case is described in the next section.

### index_type == kBinarySearchWithFirstKey
The feature of `index_type == kBinarySearchWithFirstKey` is to allow RocksDB to see first key of a data block without reading it from the disk. With this feature, RocksDB knows the first key of the block, so it can defer reading the block until a value is actually needed. This can effectively prevent I/O and other overhead for some special workloads. For example, during range scans, when RocksDB merges key-values from multiple sources using a heap structure, reading a data block from an SST file can be delayed until the block actually reaches the top of the heap (and can be avoided completely if it never reaches the top).

If this option is used, for each entry in the index block, following the `BlockHandle` (offset, size) part, the first key of the block is stored, in the form of length prefixed string. For example, version_format = 5 will take the format as:

    restart_point   0:
        k (block_key), v (block_offset, block_size, size_of_first_key, first_key)
        k (block_key), v (delta_size, size_of_first_key, first_key)
        k (block_key), v (delta_size, size_of_first_key, first_key)
    restart_point   1:
        k (block_key), v (block_offset, block_size, size_of_first_key, first_key)
        k (block_key), v (delta_size, size_of_first_key, first_key)
        k (block_key), v (delta_size, size_of_first_key, first_key)
    ...
It's similar to other format versions and restart block size.