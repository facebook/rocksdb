//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/sst_partitioner.h"

#include <algorithm>

namespace ROCKSDB_NAMESPACE {

PartitionerResult SstPartitionerFixedPrefix::ShouldPartition(
    const PartitionerRequest& request) {
  Slice last_key_fixed(*request.prev_user_key);
  if (last_key_fixed.size() > len_) {
    last_key_fixed.size_ = len_;
  }
  Slice current_key_fixed(*request.current_user_key);
  if (current_key_fixed.size() > len_) {
    current_key_fixed.size_ = len_;
  }
  return last_key_fixed.compare(current_key_fixed) != 0 ? kRequired
                                                        : kNotRequired;
}

bool SstPartitionerFixedPrefix::CanDoTrivialMove(
    const Slice& smallest_user_key, const Slice& largest_user_key) {
  return ShouldPartition(PartitionerRequest(smallest_user_key, largest_user_key,
                                            0)) == kNotRequired;
}

std::unique_ptr<SstPartitioner>
SstPartitionerFixedPrefixFactory::CreatePartitioner(
    const SstPartitioner::Context& /* context */) const {
  return std::unique_ptr<SstPartitioner>(new SstPartitionerFixedPrefix(len_));
}

std::shared_ptr<SstPartitionerFactory> NewSstPartitionerFixedPrefixFactory(
    size_t prefix_len) {
  return std::make_shared<SstPartitionerFixedPrefixFactory>(prefix_len);
}

}  // namespace ROCKSDB_NAMESPACE
