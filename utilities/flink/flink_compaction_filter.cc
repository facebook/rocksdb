// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <iostream>
#include "utilities/flink/flink_compaction_filter.h"

namespace rocksdb {
namespace flink {

const char* FlinkCompactionFilter::Name() const {
  return "FlinkCompactionFilter";
}

CompactionFilter::Decision FlinkCompactionFilter::FilterV2(
    int /*level*/, const Slice& key, ValueType value_type,
    const Slice& existing_value, std::string* /*new_value*/,
    std::string* /*skip_until*/) const {
  const Config config = *(config_.load());
  const char* data = existing_value.data();

  Debug(logger_, "Call FlinkCompactionFilter::FilterV2");
  Debug(logger_, "Key: %s, Data: %s, Value type: %d",
    key.ToString().c_str(), existing_value.ToString(true).c_str(), value_type);
  Debug(logger_, "Config: state type %d, ttl %d ms, useSystemTime %d, timestamp_offset %d",
    config.state_type_, config.ttl_, config.useSystemTime_, config.timestamp_offset_);

  // too short value to have timestamp at all
  const bool tooShortValue = existing_value.size() < config.timestamp_offset_ + TIMESTAMP_BYTE_SIZE;

  const StateType state_type = config.state_type_;
  const bool value_state = state_type == StateType::Value && value_type == ValueType::kValue;
  const bool list_entry = state_type == StateType::List && value_type == ValueType::kMergeOperand;
  const bool map_entry = state_type == StateType::Map && value_type == ValueType::kValue;
  const bool toDecide = value_state || list_entry || map_entry;

  Decision decision = !tooShortValue && toDecide ?
    Decide(data, config.timestamp_offset_, config.ttl_, config.useSystemTime_) : Decision::kKeep;
  Debug(logger_, "Decision: %d", decision);
  return decision;
}

CompactionFilter::Decision FlinkCompactionFilter::Decide(
    const char* ts_bytes, std::size_t timestamp_offset, int64_t ttl, bool useSystemTime) const {
  int64_t timestamp = DeserializeTimestamp(ts_bytes, timestamp_offset);
  const int64_t ttlWithoutOverflow = timestamp > 0 ? std::min(JAVA_MAX_LONG - timestamp, ttl) : ttl;
  const int64_t currentTimestamp = CurrentTimestamp(useSystemTime);
  Debug(logger_, "Last access timestamp: %d ms, ttlWithoutOverflow: %d ms, Current timestamp: %d ms",
    timestamp, ttlWithoutOverflow, currentTimestamp);
  return timestamp + ttlWithoutOverflow <= currentTimestamp ? Decision::kRemove : Decision ::kKeep;
}
}  // namespace flink
}  // namespace rocksdb
