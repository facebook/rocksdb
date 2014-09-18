// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include "rocksdb/table.h"
#include "util/murmurhash.h"
#include "rocksdb/options.h"

namespace rocksdb {

const uint32_t kCuckooMurmurSeedMultiplier = 816922183;
static inline uint64_t CuckooHash(
    const Slice& user_key, uint32_t hash_cnt, uint64_t table_size_minus_one,
    bool identity_as_first_hash,
    uint64_t (*get_slice_hash)(const Slice&, uint32_t, uint64_t)) {
#ifndef NDEBUG
  // This part is used only in unit tests.
  if (get_slice_hash != nullptr) {
    return get_slice_hash(user_key, hash_cnt, table_size_minus_one + 1);
  }
#endif
  if (hash_cnt == 0 && identity_as_first_hash) {
    return (*reinterpret_cast<const int64_t*>(user_key.data())) &
           table_size_minus_one;
  }
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
  explicit CuckooTableFactory(const CuckooTableOptions& table_options)
    : table_options_(table_options) {}
  ~CuckooTableFactory() {}

  const char* Name() const override { return "CuckooTable"; }

  Status NewTableReader(
      const ImmutableCFOptions& ioptions, const EnvOptions& env_options,
      const InternalKeyComparator& internal_comparator,
      unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
      unique_ptr<TableReader>* table) const override;

  TableBuilder* NewTableBuilder(const ImmutableCFOptions& options,
      const InternalKeyComparator& icomparator, WritableFile* file,
      const CompressionType, const CompressionOptions&) const override;

  // Sanitizes the specified DB Options.
  Status SanitizeDBOptions(const DBOptions* db_opts) const override {
    return Status::OK();
  }

  std::string GetPrintableTableOptions() const override;

 private:
  const CuckooTableOptions table_options_;
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
