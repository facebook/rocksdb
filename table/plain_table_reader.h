// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <unordered_map>
#include <memory>
#include <stdint.h>
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"

namespace rocksdb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;
class TableReader;

using std::unique_ptr;
using std::unordered_map;

// Based on following output file format:
// +-------------+
// | version     |
// +-------------+------------------------------+  <= key1_data_offset
// | key1            | value_size (4 bytes) |   |
// +----------------------------------------+   |
// | value1                                     |
// |                                            |
// +----------------------------------------+---+  <= key2_data_offset
// | key2            | value_size (4 bytes) |   |
// +----------------------------------------+   |
// | value2                                     |
// |                                            |
// |        ......                              |
// +-----------------+--------------------------+   <= index_block_offset
// | key1            | key1 offset (8 bytes)    |
// +-----------------+--------------------------+   <= key2_index_offset
// | key2            | key2 offset (8 bytes)    |
// +-----------------+--------------------------+   <= key3_index_offset
// | key3            | key3 offset (8 bytes)    |
// +-----------------+--------------------------+   <= key4_index_offset
// |        ......                              |
// +-----------------+------------+-------------+
// When opening the output file, IndexedTableReader creates a hash table
// from key prefixes to offset of the output file. IndexedTable will decide
// whether it points to the data offset of the first key with the key prefix
// or the offset of it. If there are too many keys share this prefix, it will
// create a binary search-able index from the suffix to offset on disk.
//
// The implementation of IndexedTableReader requires output file is mmaped
class PlainTableReader: public TableReader {
public:
  static Status Open(const Options& options, const EnvOptions& soptions,
                     unique_ptr<RandomAccessFile> && file, uint64_t file_size,
                     unique_ptr<TableReader>* table, const int user_key_size,
                     const int key_prefix_len, const int bloom_num_bits,
                     double hash_table_ratio);

  bool PrefixMayMatch(const Slice& internal_prefix);

  Iterator* NewIterator(const ReadOptions&);

  Status Get(
      const ReadOptions&, const Slice& key, void* arg,
      bool (*handle_result)(void* arg, const Slice& k, const Slice& v, bool),
      void (*mark_key_may_exist)(void*) = nullptr);

  uint64_t ApproximateOffsetOf(const Slice& key);

  bool TEST_KeyInCache(const ReadOptions& options, const Slice& key);

  void SetupForCompaction();

  TableProperties& GetTableProperties() {
    return table_properties_;
  }

  PlainTableReader(
      const EnvOptions& storage_options,
      uint64_t file_size,
      int user_key_size,
      int key_prefix_len,
      int bloom_num_bits,
      double hash_table_ratio,
      const TableProperties& table_properties);
  ~PlainTableReader();

private:
  uint32_t* hash_table_ = nullptr;
  int hash_table_size_;
  std::string sub_index_;

  Options options_;
  const EnvOptions& soptions_;
  Status status_;
  unique_ptr<RandomAccessFile> file_;

  Slice file_data_;
  uint32_t version_;
  uint32_t file_size_;
  const size_t user_key_size_;
  const size_t key_prefix_len_;
  const double hash_table_ratio_;
  const FilterPolicy* filter_policy_;
  std::string filter_str_;
  Slice filter_slice_;

  TableProperties table_properties_;
  uint32_t data_start_offset_;
  uint32_t data_end_offset_;

  static const size_t kNumInternalBytes = 8;
  static const uint32_t kSubIndexMask = 0x80000000;
  static const size_t kOffsetLen = sizeof(uint32_t);

  inline size_t GetInternalKeyLength() {
    return user_key_size_ + kNumInternalBytes;
  }

  friend class TableCache;
  friend class PlainTableIterator;

  // Populate the internal indexes. It must be called before
  // any query to the table.
  // This query will populate the hash table hash_table_, the second
  // level of indexes sub_index_ and bloom filter filter_slice_ if enabled.
  Status PopulateIndex();

  // Check bloom filter to see whether it might contain this prefix
  bool MayHavePrefix(const Slice& target_prefix);

  // Read the key and value at offset to key and value.
  // tmp_slice is a tmp slice.
  // return next_offset as the offset for the next key.
  Status Next(uint32_t offset, Slice* key, Slice* value, uint32_t& next_offset);
  // Get file offset for key target.
  // return value prefix_matched is set to true if the offset is confirmed
  // for a key with the same prefix as target.
  uint32_t GetOffset(const Slice& target, bool& prefix_matched);

  // No copying allowed
  explicit PlainTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};

// Iterator to iterate IndexedTable
class PlainTableIterator: public Iterator {
public:
  explicit PlainTableIterator(PlainTableReader* table);
  ~PlainTableIterator();

  bool Valid() const;

  void SeekToFirst();

  void SeekToLast();

  void Seek(const Slice& target);

  void Next();

  void Prev();

  Slice key() const;

  Slice value() const;

  Status status() const;

private:
  PlainTableReader* table_;
  uint32_t offset_;
  uint32_t next_offset_;
  Slice key_;
  Slice value_;
  Status status_;
  // No copying allowed
  PlainTableIterator(const PlainTableIterator&) = delete;
  void operator=(const Iterator&) = delete;
};

}  // namespace rocksdb
