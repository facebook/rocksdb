//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/range_del_block.h"

#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

RangeDelBlockBuilder::RangeDelBlockBuilder(size_t timestamp_size,
                                           bool persist_user_defined_timestamps)
    : strip_timestamp_size_(persist_user_defined_timestamps ? 0
                                                            : timestamp_size),
      block_builder_(
          1 /* block_restart_interval */, true /* use_delta_encoding */,
          false /* use_value_delta_encoding */,
          BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
          0.75 /* data_block_hash_table_util_ratio */, timestamp_size,
          persist_user_defined_timestamps, false /* is_user_key */,
          false /* use_separated_kv_storage */, /*statistics=*/nullptr,
          /*uniform_cv_threshold=*/-1.0, /*use_common_prefix=*/false) {}

void RangeDelBlockBuilder::Add(const Slice& internal_start_key,
                               const Slice& end_user_key) {
  Slice persisted_end = end_user_key;
  if (strip_timestamp_size_ > 0) {
    persisted_end =
        StripTimestampFromUserKey(end_user_key, strip_timestamp_size_);
  }
  block_builder_.Add(internal_start_key, persisted_end);
}

}  // namespace ROCKSDB_NAMESPACE
