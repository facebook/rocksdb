//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE
#include <stdint.h>
#include <string>
#include <vector>
#include "rocksdb/status.h"
#include "table/table_builder.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "util/autovector.h"

namespace rocksdb {

struct CuckooBucket {
  CuckooBucket(): is_empty(true) {}
  Slice key;
  Slice value;
  bool is_empty;
};

class CuckooTableBuilder: public TableBuilder {
 public:
  CuckooTableBuilder(
      WritableFile* file, unsigned int fixed_key_length,
      unsigned int fixed_value_length, double hash_table_ratio,
      unsigned int file_size, unsigned int max_num_hash_table,
      unsigned int max_search_depth,
      unsigned int (*GetSliceHash)(const Slice&, unsigned int,
        unsigned int));

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~CuckooTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

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
  bool MakeSpaceForKey(const Slice& key, unsigned int* bucket_id,
      autovector<unsigned int> hash_vals);

  unsigned int num_hash_table_;
  WritableFile* file_;
  const unsigned int key_length_;
  const unsigned int value_length_;
  const unsigned int bucket_size_;
  const double hash_table_ratio_;
  const unsigned int max_num_buckets_;
  const unsigned int max_num_hash_table_;
  const unsigned int max_search_depth_;
  Status status_;
  std::vector<CuckooBucket> buckets_;
  bool is_last_level_file_ = true;
  TableProperties properties_;
  unsigned int (*GetSliceHash)(const Slice& s, unsigned int index,
    unsigned int max_num_buckets);
  std::string unused_user_key_ = "";
  std::string prev_key_;

  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  // No copying allowed
  CuckooTableBuilder(const CuckooTableBuilder&) = delete;
  void operator=(const CuckooTableBuilder&) = delete;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
