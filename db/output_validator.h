//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/hash.h"

namespace rocksdb {
// A class that validates key/value that is inserted to an SST file.
// Pass every key/value of the file using OutputValidator::Add()
// and the class validates key order and optionally calculate a hash
// of all the key and value.
class OutputValidator {
 public:
  explicit OutputValidator(const InternalKeyComparator& _icmp,
                           bool _enable_order_check, bool _enable_hash) :
  icmp_(_icmp), enable_order_check_(_enable_order_check), enable_hash_(_enable_hash) {}
  Status Add(const Slice& key, const Slice& value) {
    if (enable_hash_) {
      // Generate a rolling 64-bit hash of the key and values
      paranoid_hash_ =
          Hash64(key.data(), key.size(), paranoid_hash_);
      paranoid_hash_ =
          Hash64(value.data(), value.size(), paranoid_hash_);
    }
    if (enable_order_check_) {
      TEST_SYNC_POINT_CALLBACK("OutputValidator::Add:order_check", /*arg=*/nullptr);
      if (key.size() < 8) {
        return Status::Corruption("Compaction try to write a key without internal bytes.");      
      }
      // prev_key_ starts with empty.
      if (!prev_key_.empty() && icmp_.Compare(key, prev_key_) < 0) {
        return Status::Corruption("Compaction sees out-of-order keys.");
      }
      prev_key_.assign(key.data(), key.size());
    }
    return Status::OK();
  }
  uint64_t GetHash() const { return paranoid_hash_; }
private:
  const InternalKeyComparator& icmp_;
  std::string prev_key_;
  uint64_t paranoid_hash_ = 0;
  bool enable_order_check_;
  bool enable_hash_;
};
} // namespace rocksdb
