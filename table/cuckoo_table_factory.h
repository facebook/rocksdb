// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include "rocksdb/table.h"

namespace rocksdb {

extern uint64_t GetSliceMurmurHash(const Slice& s, uint32_t index,
    uint64_t max_num_buckets);

// Cuckoo Table is designed for applications that require fast point lookups
// but not fast range scans.
//
// Some assumptions:
// - Key length and Value length are fixed.
// - Does not support Snapshot.
// - Does not support Merge operations.
// - Only supports Bytewise comparators.
class CuckooTableFactory : public TableFactory {
 public:
  CuckooTableFactory(double hash_table_ratio, uint32_t max_search_depth)
    : hash_table_ratio_(hash_table_ratio),
      max_search_depth_(max_search_depth) {}
  ~CuckooTableFactory() {}

  const char* Name() const override { return "CuckooTable"; }

  Status NewTableReader(
      const Options& options, const EnvOptions& soptions,
      const InternalKeyComparator& internal_comparator,
      unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
      unique_ptr<TableReader>* table) const override;

  TableBuilder* NewTableBuilder(const Options& options,
      const InternalKeyComparator& icomparator, WritableFile* file,
      CompressionType compression_type) const override;

  // Sanitizes the specified DB Options.
  Status SanitizeDBOptions(DBOptions* db_opts) const override {
    return Status::OK();
  }

  std::string GetPrintableTableOptions() const override;

 private:
  const double hash_table_ratio_;
  const uint32_t max_search_depth_;
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
