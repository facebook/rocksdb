// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include "utilities/flink/flink_compaction_filter.h"

namespace rocksdb {
namespace flink {

const char* FlinkCompactionFilter::Name() const {
  return "FlinkCompactionFilter";
}

CompactionFilter::Decision FlinkCompactionFilter::FilterV2(
    int /*level*/, const Slice& /*key*/, ValueType value_type,
    const Slice& existing_value, std::string* /*new_value*/,
    std::string* /*skip_until*/) const {
  const char* data = existing_value.data();

  // too short value to have timestamp at all
  if (existing_value.size() < TIMESTAMP_BYTE_SIZE) {
    return  Decision::kKeep;
  }

  const bool value_state = state_type_ == StateType::Value and value_type == ValueType::kValue;
  const bool list_entry = state_type_ == StateType::List and value_type == ValueType::kMergeOperand;
  const bool map_entry = state_type_ == StateType::Map and value_type == ValueType::kValue;

  Decision decision = Decision::kKeep;

  if (value_state or list_entry) {
    decision = Decide(data, 0);
  } else if (map_entry) {
    // too short value to have timestamp for map entry at all
    if (existing_value.size() < TIMESTAMP_BYTE_SIZE) {
      decision = Decision::kKeep;
    } else {
      decision = Decide(data, FLINK_MAP_STATE_NULL_BYTE_OFFSET); // skip (non-)null flag for map entry
    }
  }

  return decision;
}

CompactionFilter::Decision FlinkCompactionFilter::Decide(const char* ts_bytes, std::size_t offset) const {
  int64_t timestamp = DeserializeTimestamp(ts_bytes, offset);
  const int64_t ttlWithoutOverflow = timestamp > 0 ? std::min(JAVA_MAX_LONG - timestamp, ttl_) : ttl_;
  const int64_t currentTimestamp = time_provider_->CurrentTimestamp();
  return timestamp + ttlWithoutOverflow <= currentTimestamp ? Decision::kRemove : Decision ::kKeep;
}
}  // namespace flink
}  // namespace rocksdb
