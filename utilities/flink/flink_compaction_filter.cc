// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <iostream>
#include "utilities/flink/flink_compaction_filter.h"

namespace rocksdb {
namespace flink {

int64_t DeserializeTimestamp(const char *src, std::size_t offset) {
  uint64_t result = 0;
  for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
    result |= static_cast<uint64_t>(static_cast<unsigned char>(src[offset + i]))
            << ((sizeof(int64_t) - 1 - i) * BITS_PER_BYTE);
  }
  return static_cast<int64_t>(result);
}

CompactionFilter::Decision Decide(
        const char* ts_bytes,
        const int64_t ttl,
        const std::size_t timestamp_offset,
        const int64_t current_timestamp,
        const std::shared_ptr<Logger> &logger) {
  int64_t timestamp = DeserializeTimestamp(ts_bytes, timestamp_offset);
  const int64_t ttlWithoutOverflow = timestamp > 0 ? std::min(JAVA_MAX_LONG - timestamp, ttl) : ttl;
  Debug(logger.get(), "Last access timestamp: %ld ms, ttlWithoutOverflow: %ld ms, Current timestamp: %ld ms",
        timestamp, ttlWithoutOverflow, current_timestamp);
  return timestamp + ttlWithoutOverflow <= current_timestamp ?
         CompactionFilter::Decision::kRemove : CompactionFilter::Decision::kKeep;
}

FlinkCompactionFilter::ConfigHolder::ConfigHolder() : config_(const_cast<FlinkCompactionFilter::Config*>(&DISABLED_CONFIG)) {};

FlinkCompactionFilter::ConfigHolder::~ConfigHolder() {
  Config* config = config_.load();
  if (config != &DISABLED_CONFIG) {
    delete config;
  }
}

// at the moment Flink configures filters (can be already created) only once when user creates state
// otherwise it can lead to ListElementFilter leak in Config
// or race between its delete in Configure() and usage in FilterV2()
// the method returns true if it was configured before
bool FlinkCompactionFilter::ConfigHolder::Configure(Config* config) {
  bool not_configured = GetConfig() == &DISABLED_CONFIG;
  if (not_configured) {
    assert(config->query_time_after_num_entries_ >= 0);
    config_ = config;
  }
  return not_configured;
}

FlinkCompactionFilter::Config* FlinkCompactionFilter::ConfigHolder::GetConfig() {
  return config_.load();
}

std::size_t FlinkCompactionFilter::FixedListElementFilter::NextUnexpiredOffset(
        const Slice& list, int64_t ttl, int64_t current_timestamp) const {
  std::size_t offset = 0;
  while (offset < list.size()) {
    Decision decision = Decide(list.data(), ttl, offset + timestamp_offset_, current_timestamp, logger_);
    if (decision != Decision::kKeep) {
      std::size_t new_offset = offset + fixed_size_;
      if (new_offset >= JAVA_MAX_SIZE || new_offset < offset) {
        return JAVA_MAX_SIZE;
      }
      offset = new_offset;
    } else {
      break;
    }
  }
  return offset;
}

const char* FlinkCompactionFilter::Name() const {
  return "FlinkCompactionFilter";
}

FlinkCompactionFilter::FlinkCompactionFilter(std::shared_ptr<ConfigHolder> config_holder,
                                             std::unique_ptr<TimeProvider> time_provider) :
        FlinkCompactionFilter(std::move(config_holder), std::move(time_provider), nullptr) {};

FlinkCompactionFilter::FlinkCompactionFilter(std::shared_ptr<ConfigHolder> config_holder,
                                             std::unique_ptr<TimeProvider> time_provider,
                                             std::shared_ptr<Logger> logger) :
        config_holder_(std::move(config_holder)),
        time_provider_(std::move(time_provider)),
        logger_(std::move(logger)),
        config_cached_(const_cast<Config*>(&DISABLED_CONFIG)) {};

inline void FlinkCompactionFilter::InitConfigIfNotYet() const {
  const_cast<FlinkCompactionFilter*>(this)->config_cached_ =
          config_cached_ == &DISABLED_CONFIG ? config_holder_->GetConfig() : config_cached_;
}

CompactionFilter::Decision FlinkCompactionFilter::FilterV2(
    int /*level*/, const Slice& key, ValueType value_type,
    const Slice& existing_value, std::string* new_value,
    std::string* /*skip_until*/) const {
  InitConfigIfNotYet();
  CreateListElementFilterIfNull();
  UpdateCurrentTimestampIfStale();

  const char* data = existing_value.data();

  Debug(logger_.get(),
    "Call FlinkCompactionFilter::FilterV2 - Key: %s, Data: %s, Value type: %d, "
    "State type: %d, TTL: %d ms, timestamp_offset: %d",
    key.ToString().c_str(), existing_value.ToString(true).c_str(), value_type,
    config_cached_->state_type_, config_cached_->ttl_, config_cached_->timestamp_offset_);

  // too short value to have timestamp at all
  const bool tooShortValue = existing_value.size() < config_cached_->timestamp_offset_ + TIMESTAMP_BYTE_SIZE;

  const StateType state_type = config_cached_->state_type_;
  const bool value_or_merge = value_type == ValueType::kValue || value_type == ValueType::kMergeOperand;
  const bool value_state = state_type == StateType::Value && value_type == ValueType::kValue;
  const bool list_entry = state_type == StateType::List && value_or_merge;
  const bool toDecide = value_state || list_entry;
  const bool list_filter = list_entry && list_element_filter_;

  Decision decision = Decision::kKeep;
  if (!tooShortValue && toDecide) {
    decision = list_filter ?
            ListDecide(existing_value, new_value) :
            Decide(data, config_cached_->ttl_, config_cached_->timestamp_offset_, current_timestamp_, logger_);
  }
  Debug(logger_.get(), "Decision: %d", decision);
  return decision;
}

CompactionFilter::Decision FlinkCompactionFilter::ListDecide(
        const Slice& existing_value, std::string* new_value) const {
  std::size_t offset = 0;
  if (offset < existing_value.size()) {
    Decision decision = Decide(existing_value.data(), config_cached_->ttl_, offset + config_cached_->timestamp_offset_, current_timestamp_, logger_);
    if (decision != Decision::kKeep) {
      offset = ListNextUnexpiredOffset(existing_value, offset, config_cached_->ttl_);
      if (offset >= JAVA_MAX_SIZE) {
        return Decision::kKeep;
      }
    }
  }
  if (offset >= existing_value.size()) {
    return Decision::kRemove;
  } else if (offset > 0) {
    SetUnexpiredListValue(existing_value, offset, new_value);
    return Decision::kChangeValue;
  }
  return Decision::kKeep;
}

std::size_t FlinkCompactionFilter::ListNextUnexpiredOffset(
        const Slice &existing_value, size_t offset, int64_t ttl) const {
  std::size_t new_offset = list_element_filter_->NextUnexpiredOffset(existing_value, ttl, current_timestamp_);
  if (new_offset >= JAVA_MAX_SIZE || new_offset < offset) {
    Error(logger_.get(), "Wrong next offset in list filter: %d -> %d",
          offset, new_offset);
    new_offset = JAVA_MAX_SIZE;
  } else {
    Debug(logger_.get(), "Next unexpired offset: %d -> %d",
          offset, new_offset);
  }
  return new_offset;
}

void FlinkCompactionFilter::SetUnexpiredListValue(
        const Slice& existing_value, std::size_t offset, std::string* new_value) const {
  new_value->clear();
  auto new_value_char = existing_value.data() + offset;
  auto new_value_size = existing_value.size() - offset;
  new_value->assign(new_value_char, new_value_size);
  Logger* logger = logger_.get();
  if (logger && logger->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    Slice new_value_slice = Slice(new_value_char, new_value_size);
    Debug(logger, "New list value: %s", new_value_slice.ToString(true).c_str());
  }
}
}  // namespace flink
}  // namespace rocksdb
