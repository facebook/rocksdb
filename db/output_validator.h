//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "db/dbformat.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {
// A class that validates key/value that is inserted to an SST file.
// Pass every key/value of the file using OutputValidator::Add()
// and the class validates key order and optionally calculate a hash
// of all the key and value.
class OutputValidator {
 public:
  explicit OutputValidator(const InternalKeyComparator& icmp,
                           bool enable_order_check, bool enable_hash)
      : icmp_(icmp),
        enable_order_check_(enable_order_check),
        enable_hash_(enable_hash) {}

  // Add a key to the KV sequence, and return whether the key follows
  // criteria, e.g. key is ordered.
  Status Add(const Slice& key, const Slice& value);

  // Compare result of two key orders are the same. It can be used
  // to compare the keys inserted into a file, and what is read back.
  // Return true if the validation passes.
  bool CompareValidator(const OutputValidator& other_validator) {
    return GetHash() == other_validator.GetHash();
  }

 private:
  // Not (yet) intended to be persisted, so subject to change
  // without notice between releases.
  uint64_t GetHash() const { return paranoid_hash_; }

  const InternalKeyComparator& icmp_;
  std::string prev_key_;
  uint64_t paranoid_hash_ = 0;
  bool enable_order_check_;
  bool enable_hash_;
};
}  // namespace ROCKSDB_NAMESPACE
