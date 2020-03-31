---
title: format_version 4
layout: post
author: maysamyabandeh
category: blog
---

The data blocks in RocksDB consist of a sequence of key/values pairs sorted by key, where the pairs are grouped into _restart intervals_ specified by `block_restart_interval`. Up to RocksDB version 5.14, where the latest and default value of `BlockBasedTableOptions::format_version` is 2, the format of index and data blocks are the same: index blocks use the same key format of <`user_key`,`seq`> and encode pointers to data blocks, <`offset`,`size`>, to a byte string and use them as values. The only difference is that the index blocks use `index_block_restart_interval` for the size of _restart intervals_. `format_version=`3,4 offer more optimized, backward-compatible, yet forward-incompatible format for index blocks. 

### Pros

Using `format_version`=4 significantly reduces the index block size, in some cases around 4-5x. This frees more space in block cache, which would result in higher hit rate for data and filter blocks, or offer the same performance with a smaller block cache size.

### Cons

Being _forward-incompatible_ means that if you enable `format_version=`4 you cannot downgrade to a RocksDB version lower than 5.16.

### How to use it?

- `BlockBasedTableOptions::format_version` = 4
- `BlockBasedTableOptions::index_block_restart_interval` = 16

### What is format_version 3?
(Since RocksDB 5.15) In most cases, the sequence number `seq` is not necessary for keys in the index blocks. In such cases, `format_version`=3 skips encoding the sequence number and sets `index_key_is_user_key` in TableProperties, which is used by the reader to know how to decode the index block.

### What is format_version 4?
(Since RocksDB 5.16) Changes the format of index blocks by delta encoding the index values, which are the block handles. This saves the encoding of `BlockHandle::offset` of the non-head index entries in each restart interval. If used, `TableProperties::index_value_is_delta_encoded` is set, which is used by the reader to know how to decode the index block.  The format of each key is (shared_size, non_shared_size, shared, non_shared). The format of each value, i.e., block handle, is (offset, size) whenever the shared_size is 0, which included the first entry in each restart point. Otherwise the format is delta-size = block handle size - size of last block handle.

The index format in `format_version=4` would be as follows:

    restart_point   0: k, v (off, sz), k, v (delta-sz), ..., k, v (delta-sz)
    restart_point   1: k, v (off, sz), k, v (delta-sz), ..., k, v (delta-sz)
    ...
    restart_point n-1: k, v (off, sz), k, v (delta-sz), ..., k, v (delta-sz)
    where, k is key, v is value, and its encoding is in parenthesis.

