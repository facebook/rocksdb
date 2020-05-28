//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/flush_block_policy.h"

namespace ROCKSDB_NAMESPACE {

// FlushBlockEveryKeyPolicy currently used only in tests.

class FlushBlockEveryKeyPolicy : public FlushBlockPolicy {
 public:
  bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
    if (!start_) {
      start_ = true;
      return false;
    }
    return true;
  }

 private:
  bool start_ = false;
};

class FlushBlockEveryKeyPolicyFactory : public FlushBlockPolicyFactory {
 public:
  explicit FlushBlockEveryKeyPolicyFactory() {}

  const char* Name() const override {
    return "FlushBlockEveryKeyPolicyFactory";
  }

  FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& /*data_block_builder*/) const override {
    return new FlushBlockEveryKeyPolicy;
  }
};

}  // namespace ROCKSDB_NAMESPACE
