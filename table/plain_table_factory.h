// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
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
// +-------------+-----------------+
// | version     | user_key_length |
// +------------++------------------------------+  <= key1 offset
// | [key_size] |  key1       | value_size  |   |
// +------------+-------------+-------------+   |
// | value1                                     |
// |                                            |
// +----------------------------------------+---+  <= key2 offset
// | [key_size] |  key2       | value_size  |   |
// +------------+-------------+-------------+   |
// | value2                                     |
// |                                            |
// |        ......                              |
// +-----------------+--------------------------+
// If user_key_length = kPlainTableVariableLength, it means the key is variable
// length, there will be an extra field for key size encoded before every key.
class PlainTableFactory : public TableFactory {
 public:
  ~PlainTableFactory() {}
  // user_key_size is the length of the user key. If it is set to be
  // kPlainTableVariableLength, then it means variable length. Otherwise, all
  // the keys need to have the fix length of this value. bloom_bits_per_key is
  // number of bits used for bloom filer per key. hash_table_ratio is
  // the desired utilization of the hash table used for prefix hashing.
  // hash_table_ratio = number of prefixes / #buckets in the hash table
  // hash_table_ratio = 0 means skip hash table but only replying on binary
  // search.
  // index_sparseness determines index interval for keys
  // inside the same prefix. It will be the maximum number of linear search
  // required after hash and binary search.
  // index_sparseness = 0 means index for every key.
  // huge_page_tlb_size determines whether to allocate hash indexes from huge
  // page TLB and the page size if allocating from there. See comments of
  // Arena::AllocateAligned() for details.
  explicit PlainTableFactory(uint32_t user_key_len = kPlainTableVariableLength,
                             int bloom_bits_per_key = 0,
                             double hash_table_ratio = 0.75,
                             size_t index_sparseness = 16,
                             size_t huge_page_tlb_size = 0)
      : user_key_len_(user_key_len),
        bloom_bits_per_key_(bloom_bits_per_key),
        hash_table_ratio_(hash_table_ratio),
        index_sparseness_(index_sparseness),
        huge_page_tlb_size_(huge_page_tlb_size) {}
  const char* Name() const override { return "PlainTable"; }
  Status NewTableReader(const Options& options, const EnvOptions& soptions,
                        const InternalKeyComparator& internal_comparator,
                        unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
                        unique_ptr<TableReader>* table) const override;
  TableBuilder* NewTableBuilder(const Options& options,
                                const InternalKeyComparator& icomparator,
                                WritableFile* file,
                                CompressionType compression_type) const
      override;

  static const char kValueTypeSeqId0 = 0xFF;

 private:
  uint32_t user_key_len_;
  int bloom_bits_per_key_;
  double hash_table_ratio_;
  size_t index_sparseness_;
  size_t huge_page_tlb_size_;
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
