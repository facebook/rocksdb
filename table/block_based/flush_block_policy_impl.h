//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
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

  static const char* kClassName() { return "FlushBlockEveryKeyPolicyFactory"; }
  const char* Name() const override { return kClassName(); }

  FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& /*data_block_builder*/) const override {
    return new FlushBlockEveryKeyPolicy;
  }
};

// For internal use, policy that is stateless after creation, meaning it can
// be safely re-targeted to another block builder.
class RetargetableFlushBlockPolicy : public FlushBlockPolicy {
 public:
  explicit RetargetableFlushBlockPolicy(const BlockBuilder& data_block_builder)
      : data_block_builder_(&data_block_builder) {}

  void Retarget(const BlockBuilder& data_block_builder) {
    data_block_builder_ = &data_block_builder;
  }

 protected:
  const BlockBuilder* data_block_builder_;
};

std::unique_ptr<RetargetableFlushBlockPolicy> NewFlushBlockBySizePolicy(
    const uint64_t size, const int deviation,
    const BlockBuilder& data_block_builder);

}  // namespace ROCKSDB_NAMESPACE
