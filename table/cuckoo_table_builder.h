//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE
#include <stdint.h>
#include <limits>
#include <string>
#include <utility>
#include <vector>
#include "rocksdb/status.h"
#include "table/table_builder.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "util/autovector.h"

namespace rocksdb {

class CuckooTableBuilder: public TableBuilder {
 public:
  CuckooTableBuilder(
      WritableFile* file, double max_hash_table_ratio,
      uint32_t max_num_hash_func, uint32_t max_search_depth,
      const Comparator* user_comparator, uint32_t cuckoo_block_size,
      uint64_t (*get_slice_hash)(const Slice&, uint32_t, uint64_t));

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~CuckooTableBuilder() {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override { return status_; }

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

 private:
  struct CuckooBucket {
    CuckooBucket()
      : vector_idx(kMaxVectorIdx), make_space_for_key_call_id(0) {}
    uint32_t vector_idx;
    // This number will not exceed kvs_.size() + max_num_hash_func_.
    // We assume number of items is <= 2^32.
    uint32_t make_space_for_key_call_id;
  };
  static const uint32_t kMaxVectorIdx = std::numeric_limits<int32_t>::max();

  bool MakeSpaceForKey(
      const autovector<uint64_t>& hash_vals,
      const uint64_t call_id,
      std::vector<CuckooBucket>* buckets,
      uint64_t* bucket_id);
  Status MakeHashTable(std::vector<CuckooBucket>* buckets);

  uint32_t num_hash_func_;
  WritableFile* file_;
  const double max_hash_table_ratio_;
  const uint32_t max_num_hash_func_;
  const uint32_t max_search_depth_;
  const uint32_t cuckoo_block_size_;
  uint64_t hash_table_size_;
  bool is_last_level_file_;
  Status status_;
  std::vector<std::pair<std::string, std::string>> kvs_;
  TableProperties properties_;
  bool has_seen_first_key_;
  const Comparator* ucomp_;
  uint64_t (*get_slice_hash_)(const Slice& s, uint32_t index,
    uint64_t max_num_buckets);
  std::string largest_user_key_ = "";
  std::string smallest_user_key_ = "";

  bool closed_;  // Either Finish() or Abandon() has been called.

  // No copying allowed
  CuckooTableBuilder(const CuckooTableBuilder&) = delete;
  void operator=(const CuckooTableBuilder&) = delete;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
