//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "utilities/blob_db/blob_index.h"

namespace rocksdb {
namespace blob_db {

// CompactionFilter to delete expired blob index from base DB.
class BlobIndexCompactionFilter : public CompactionFilter {
 public:
  explicit BlobIndexCompactionFilter(uint64_t current_time)
      : current_time_(current_time) {}

  virtual const char* Name() const override {
    return "BlobIndexCompactionFilter";
  }

  // Filter expired blob indexes regardless of snapshots.
  virtual bool IgnoreSnapshots() const override { return true; }

  virtual Decision FilterV2(int /*level*/, const Slice& /*key*/,
                            ValueType value_type, const Slice& value,
                            std::string* /*new_value*/,
                            std::string* /*skip_until*/) const override {
    if (value_type != kBlobIndex) {
      return Decision::kKeep;
    }
    BlobIndex blob_index;
    Status s = blob_index.DecodeFrom(value);
    if (!s.ok()) {
      // Unable to decode blob index. Keeping the value.
      return Decision::kKeep;
    }
    if (blob_index.HasTTL() && blob_index.expiration() <= current_time_) {
      // Expired
      return Decision::kRemove;
    }
    return Decision::kKeep;
  }

 private:
  const uint64_t current_time_;
};

class BlobIndexCompactionFilterFactory : public CompactionFilterFactory {
 public:
  explicit BlobIndexCompactionFilterFactory(Env* env) : env_(env) {}

  virtual const char* Name() const override {
    return "BlobIndexCompactionFilterFactory";
  }

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    int64_t current_time = 0;
    Status s = env_->GetCurrentTime(&current_time);
    if (!s.ok()) {
      return nullptr;
    }
    assert(current_time >= 0);
    return std::unique_ptr<CompactionFilter>(
        new BlobIndexCompactionFilter(static_cast<uint64_t>(current_time)));
  }

 private:
  Env* env_;
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
