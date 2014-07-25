//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef ROCKSDB_LITE
#include <string>
#include <memory>

#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "table/table_reader.h"

namespace rocksdb {

class Arena;
class TableReader;

class CuckooTableReader: public TableReader {
 public:
  CuckooTableReader(
      const Options& options,
      std::unique_ptr<RandomAccessFile>&& file,
      uint64_t file_size,
      uint64_t (*GetSliceHash)(const Slice&, uint32_t, uint64_t));
  ~CuckooTableReader() {}

  std::shared_ptr<const TableProperties> GetTableProperties() const {
    return table_props_;
  }

  Status status() const { return status_; }

  Status Get(
      const ReadOptions& readOptions, const Slice& key, void* handle_context,
      bool (*result_handler)(void* arg, const ParsedInternalKey& k,
                             const Slice& v),
      void (*mark_key_may_exist_handler)(void* handle_context) = nullptr);

  Iterator* NewIterator(const ReadOptions&, Arena* arena = nullptr);

  // Following methods are not implemented for Cuckoo Table Reader
  uint64_t ApproximateOffsetOf(const Slice& key) { return 0; }
  void SetupForCompaction() {}
  void Prepare(const Slice& target) {}
  // End of methods not implemented.

 private:
  std::unique_ptr<RandomAccessFile> file_;
  Slice file_data_;
  const uint64_t file_size_;
  bool is_last_level_;
  std::shared_ptr<const TableProperties> table_props_;
  Status status_;
  uint32_t num_hash_fun_;
  std::string unused_key_;
  uint32_t key_length_;
  uint32_t value_length_;
  uint32_t bucket_length_;
  uint64_t num_buckets_;
  uint64_t (*GetSliceHash)(const Slice& s, uint32_t index,
      uint64_t max_num_buckets);
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
