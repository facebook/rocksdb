// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include "rocksdb/table.h"
#include "util/murmurhash.h"

namespace rocksdb {

const uint32_t kCuckooMurmurSeedMultiplier = 816922183;
static inline uint64_t CuckooHash(
    const Slice& user_key, uint32_t hash_cnt, uint64_t table_size_minus_one,
    uint64_t (*get_slice_hash)(const Slice&, uint32_t, uint64_t)) {
#ifndef NDEBUG
  // This part is used only in unit tests.
  if (get_slice_hash != nullptr) {
    return get_slice_hash(user_key, hash_cnt, table_size_minus_one + 1);
  }
#endif
  return MurmurHash(user_key.data(), user_key.size(),
      kCuckooMurmurSeedMultiplier * hash_cnt) & table_size_minus_one;
}

// Cuckoo Table is designed for applications that require fast point lookups
// but not fast range scans.
//
// Some assumptions:
// - Key length and Value length are fixed.
// - Does not support Snapshot.
// - Does not support Merge operations.
class CuckooTableFactory : public TableFactory {
 public:
  CuckooTableFactory(double hash_table_ratio, uint32_t max_search_depth,
      uint32_t cuckoo_block_size)
    : hash_table_ratio_(hash_table_ratio),
      max_search_depth_(max_search_depth),
      cuckoo_block_size_(cuckoo_block_size) {}
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
  Status SanitizeDBOptions(const DBOptions* db_opts) const override {
    return Status::OK();
  }

  std::string GetPrintableTableOptions() const override;

 private:
  const double hash_table_ratio_;
  const uint32_t max_search_depth_;
  const uint32_t cuckoo_block_size_;
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
