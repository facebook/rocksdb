// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <memory>
#include <stdint.h>

#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace rocksdb {

struct Options;
struct EnvOptions;

using std::unique_ptr;
class Status;
class RandomAccessFile;
class WritableFile;
class Table;
class TableBuilder;

// IndexedTable requires fixed length key, configured as a constructor
// parameter of the factory class. Output file format:
// +--------------------------------------------+  <= key1 offset
// | key1            | value_size (4 bytes) |   |
// +----------------------------------------+   |
// | value1                                     |
// |                                            |
// +----------------------------------------+---+  <= key2 offset
// | key2            | value_size (4 bytes) |   |
// +----------------------------------------+   |
// | value2                                     |
// |                                            |
// |        ......                              |
// +-----------------+--------------------------+   <= index_block_offset
// | key1            | key1 offset (8 bytes)    |
// +-----------------+--------------------------+
// | key2            | key2 offset (8 bytes)    |
// +-----------------+--------------------------+
// | key3            | key3 offset (8 bytes)    |
// +-----------------+--------------------------+
// |        ......                              |
// +-----------------+------------+-------------+
class PlainTableFactory: public TableFactory {
public:
  ~PlainTableFactory() {
  }
  // user_key_size is the length of the user key. key_prefix_len is the
  // length of the prefix used for im-memory indexes. bloom_num_bits is
  // number of bits is used for bloom filer per key. hash_table_ratio is
  // the desired ultilization of the hash table used for prefix hashing.
  // hash_table_ratio = number of prefixes / #buckets in the hash table
  PlainTableFactory(int user_key_size, int key_prefix_len,
                    int bloom_num_bits = 0, double hash_table_ratio = 0.75) :
      user_key_size_(user_key_size), key_prefix_len_(key_prefix_len),
      bloom_num_bits_(bloom_num_bits), hash_table_ratio_(hash_table_ratio) {
  }
  const char* Name() const override {
    return "PlainTable";
  }
  Status GetTableReader(const Options& options, const EnvOptions& soptions,
                        unique_ptr<RandomAccessFile> && file,
                        uint64_t file_size,
                        unique_ptr<TableReader>* table) const override;

  TableBuilder* GetTableBuilder(const Options& options, WritableFile* file,
                                CompressionType compression_type) const
                                    override;
private:
  int user_key_size_;
  int key_prefix_len_;
  int bloom_num_bits_;
  double hash_table_ratio_;
};

}  // namespace rocksdb
