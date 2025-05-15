//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/table_properties_collectors/compact_for_tiering_collector.h"

#include <sstream>

#include "db/seqno_to_time_mapping.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
const std::string
    CompactForTieringCollector::kNumEligibleLastLevelEntriesPropertyName =
        "rocksdb.eligible.last.level.entries";
const std::string
    CompactForTieringCollector::kAverageDataUnixWriteTimePropertyName =
        "rocksdb.data.unix.write.time.average";
const std::string
    CompactForTieringCollector::kMaxDataUnixWriteTimePropertyName =
        "rocksdb.data.unix.write.time.max";
const std::string
    CompactForTieringCollector::kMinDataUnixWriteTimePropertyName =
        "rocksdb.data.unix.write.time.min";
const std::string
    CompactForTieringCollector::kNumInfinitelyOldEntriesPropertyName =
        "rocksdb.num.infinitely.old.entries";
const std::string
    CompactForTieringCollector::kNumWriteTimeAggregatedEntriesPropertyName =
        "rocksdb.num.write.time.aggregated.entries";
const std::string
    CompactForTieringCollector::kNumWriteTimeUntrackedEntriesPropertyName =
        "rocksdb.num.write.time.untracked.entries";

CompactForTieringCollector::CompactForTieringCollector(
    SequenceNumber last_level_inclusive_max_seqno_threshold,
    double compaction_trigger_ratio, bool track_data_write_time)
    : last_level_inclusive_max_seqno_threshold_(
          last_level_inclusive_max_seqno_threshold),
      compaction_trigger_ratio_(compaction_trigger_ratio),
      track_data_write_time_(track_data_write_time) {
  if (compaction_trigger_ratio_ > 0 && compaction_trigger_ratio_ <= 1) {
    track_eligible_entries_ = true;
    mark_need_compaction_for_eligible_entries_ = true;
  } else if (compaction_trigger_ratio_ > 1) {
    track_eligible_entries_ = true;
  }
  assert(track_eligible_entries_ || track_data_write_time_);
}

Status CompactForTieringCollector::AddUserKeyWithWriteTime(
    const Slice& /*key*/, const Slice& value, EntryType type,
    SequenceNumber seq, uint64_t unix_write_time, uint64_t /*file_size*/) {
  TrackEligibleEntries(value, type, seq);
  TrackWriteTimeMetric(unix_write_time);
  return Status::OK();
}

void CompactForTieringCollector::TrackEligibleEntries(const Slice& value,
                                                      EntryType type,
                                                      SequenceNumber seq) {
  if (!track_eligible_entries_) {
    return;
  }
  if (mark_need_compaction_for_eligible_entries_) {
    total_entries_counter_ += 1;
  }
  SequenceNumber seq_for_check = seq;
  if (type == kEntryTimedPut) {
    seq_for_check = ParsePackedValueForSeqno(value);
  }
  if (seq_for_check < last_level_inclusive_max_seqno_threshold_) {
    last_level_eligible_entries_counter_++;
  }
}

void CompactForTieringCollector::TrackWriteTimeMetric(
    uint64_t unix_write_time) {
  if (!WriteTimeTrackingEnabled()) {
    return;
  }
  if (unix_write_time == TablePropertiesCollector::kUnknownUnixWriteTime) {
    write_time_untracked_entries_++;
  } else if (unix_write_time ==
             TablePropertiesCollector::kInfinitelyOldUnixWriteTime) {
    write_time_infinitely_old_entries_++;
  } else {
    write_time_aggregated_entries_++;
    min_data_write_time_ = std::min(min_data_write_time_, unix_write_time);
    max_data_write_time_ = std::max(max_data_write_time_, unix_write_time);
    average_data_write_time_ =
        !average_data_write_time_.has_value()
            ? unix_write_time
            : (unix_write_time + average_data_write_time_.value() *
                                     (write_time_aggregated_entries_ - 1)) /
                  write_time_aggregated_entries_;
  }
}

Status CompactForTieringCollector::Finish(UserCollectedProperties* properties) {
  assert(!finish_called_);
  if (mark_need_compaction_for_eligible_entries_ &&
      last_level_eligible_entries_counter_ >=
          compaction_trigger_ratio_ * total_entries_counter_) {
    assert(track_eligible_entries_);
    need_compaction_ = true;
  }
  InitializeWithWriteTimeProperties(properties);
  AddEligibleEntriesProperty(properties);
  finish_called_ = true;
  return Status::OK();
}

UserCollectedProperties CompactForTieringCollector::GetReadableProperties()
    const {
  UserCollectedProperties properties;
  InitializeWithWriteTimeProperties(&properties);
  AddEligibleEntriesProperty(&properties);
  return properties;
}

void CompactForTieringCollector::InitializeWithWriteTimeProperties(
    UserCollectedProperties* properties) const {
  if (!WriteTimeTrackingEnabled()) {
    return;
  }
  *properties = UserCollectedProperties{
      {kAverageDataUnixWriteTimePropertyName,
       std::to_string(
           write_time_aggregated_entries_ != 0
               ? static_cast<uint64_t>(average_data_write_time_.value())
               : TablePropertiesCollector::kUnknownUnixWriteTime)},
      {kMaxDataUnixWriteTimePropertyName,
       std::to_string(write_time_aggregated_entries_ != 0
                          ? max_data_write_time_
                          : TablePropertiesCollector::kUnknownUnixWriteTime)},
      {kMinDataUnixWriteTimePropertyName,
       std::to_string(write_time_aggregated_entries_ != 0
                          ? min_data_write_time_
                          : TablePropertiesCollector::kUnknownUnixWriteTime)},
      {kNumInfinitelyOldEntriesPropertyName,
       std::to_string(write_time_infinitely_old_entries_)},
      {kNumWriteTimeAggregatedEntriesPropertyName,
       std::to_string(write_time_aggregated_entries_)},
      {kNumWriteTimeUntrackedEntriesPropertyName,
       std::to_string(write_time_untracked_entries_)},
  };
}

void CompactForTieringCollector::AddEligibleEntriesProperty(
    UserCollectedProperties* properties) const {
  if (!track_eligible_entries_) {
    return;
  }
  properties->insert({kNumEligibleLastLevelEntriesPropertyName,
                      std::to_string(last_level_eligible_entries_counter_)});
}

bool CompactForTieringCollector::NeedCompact() const {
  return need_compaction_;
}

bool CompactForTieringCollector::WriteTimeTrackingEnabled() const {
  return track_data_write_time_;
}

void CompactForTieringCollector::Reset() {
  last_level_eligible_entries_counter_ = 0;
  total_entries_counter_ = 0;
  finish_called_ = false;
  need_compaction_ = false;
}

Status CompactForTieringCollector::
    GetDataCollectionUnixWriteTimeInfoFromUserProperties(
        const UserCollectedProperties& user_props,
        std::unique_ptr<DataCollectionUnixWriteTimeInfo>* file_info) {
  std::optional<uint64_t> min_write_time;
  std::optional<uint64_t> max_write_time;
  std::optional<uint64_t> average_write_time;
  std::optional<uint64_t> num_entries_infinitely_old;
  std::optional<uint64_t> num_entries_write_time_aggregated;
  std::optional<uint64_t> num_entries_write_time_untracked;
  auto iter = user_props.find(
      CompactForTieringCollector::kAverageDataUnixWriteTimePropertyName);
  if (iter != user_props.end()) {
    average_write_time = std::stoull(iter->second);
  }
  iter = user_props.find(
      CompactForTieringCollector::kMinDataUnixWriteTimePropertyName);
  if (iter != user_props.end()) {
    min_write_time = std::stoull(iter->second);
  }
  iter = user_props.find(
      CompactForTieringCollector::kMaxDataUnixWriteTimePropertyName);
  if (iter != user_props.end()) {
    max_write_time = std::stoull(iter->second);
  }
  iter = user_props.find(
      CompactForTieringCollector::kNumInfinitelyOldEntriesPropertyName);
  if (iter != user_props.end()) {
    num_entries_infinitely_old = std::stoull(iter->second);
  }
  iter = user_props.find(
      CompactForTieringCollector::kNumWriteTimeAggregatedEntriesPropertyName);
  if (iter != user_props.end()) {
    num_entries_write_time_aggregated = std::stoull(iter->second);
  }
  iter = user_props.find(
      CompactForTieringCollector::kNumWriteTimeUntrackedEntriesPropertyName);
  if (iter != user_props.end()) {
    num_entries_write_time_untracked = std::stoull(iter->second);
  }
  if (min_write_time.has_value() && max_write_time.has_value() &&
      average_write_time.has_value() &&
      num_entries_infinitely_old.has_value() &&
      num_entries_write_time_aggregated.has_value() &&
      num_entries_write_time_untracked.has_value()) {
    *file_info = std::make_unique<DataCollectionUnixWriteTimeInfo>(
        min_write_time.value(), max_write_time.value(),
        average_write_time.value(), num_entries_infinitely_old.value(),
        num_entries_write_time_aggregated.value(),
        num_entries_write_time_untracked.value());
    return Status::OK();
  }
  file_info->reset();
  return Status::InvalidArgument("Missing data write time user property");
}

TablePropertiesCollector*
CompactForTieringCollectorFactory::CreateTablePropertiesCollector(
    TablePropertiesCollectorFactory::Context context) {
  double compaction_trigger_ratio = GetCompactionTriggerRatio();
  bool track_data_write_time = GetTrackDataWriteTime();
  if (!track_data_write_time &&
      (compaction_trigger_ratio <= 0 ||
       context.level_at_creation == context.num_levels - 1 ||
       context.last_level_inclusive_max_seqno_threshold ==
           kMaxSequenceNumber)) {
    return nullptr;
  }
  // For files on the last level, just track write time if it's enabled, do not
  // attempt to trigger compaction for it.
  if (context.level_at_creation == context.num_levels - 1) {
    compaction_trigger_ratio = 0;
  }
  return new CompactForTieringCollector(
      context.last_level_inclusive_max_seqno_threshold,
      compaction_trigger_ratio, track_data_write_time);
}

static std::unordered_map<std::string, OptionTypeInfo>
    on_compact_for_tiering_type_info = {
        {"compaction_trigger_ratio",
         {0, OptionType::kUnknown, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever | OptionTypeFlags::kMutable,
          [](const ConfigOptions&, const std::string&, const std::string& value,
             void* addr) {
            auto* factory =
                static_cast<CompactForTieringCollectorFactory*>(addr);
            factory->SetCompactionTriggerRatio(ParseDouble(value));
            return Status::OK();
          },
          [](const ConfigOptions&, const std::string&, const void* addr,
             std::string* value) {
            const auto* factory =
                static_cast<const CompactForTieringCollectorFactory*>(addr);
            *value = std::to_string(factory->GetCompactionTriggerRatio());
            return Status::OK();
          },
          nullptr}},
        {"track_data_write_time",
         {0, OptionType::kUnknown, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever | OptionTypeFlags::kMutable,
          [](const ConfigOptions&, const std::string&, const std::string& value,
             void* addr) {
            auto* factory =
                static_cast<CompactForTieringCollectorFactory*>(addr);
            factory->SetTrackDataWriteTime(ParseBoolean("", value));
            return Status::OK();
          },
          [](const ConfigOptions&, const std::string&, const void* addr,
             std::string* value) {
            const auto* factory =
                static_cast<const CompactForTieringCollectorFactory*>(addr);
            *value = std::to_string(factory->GetTrackDataWriteTime());
            return Status::OK();
          },
          nullptr}},

};

CompactForTieringCollectorFactory::CompactForTieringCollectorFactory(
    double compaction_trigger_ratio, bool track_data_write_time)
    : compaction_trigger_ratio_(compaction_trigger_ratio),
      track_data_write_time_(track_data_write_time) {
  RegisterOptions("", this, &on_compact_for_tiering_type_info);
}

std::string CompactForTieringCollectorFactory::ToString() const {
  std::ostringstream cfg;
  cfg << Name()
      << ", compaction trigger ratio:" << compaction_trigger_ratio_.load()
      << std::endl;
  return cfg.str();
}

std::shared_ptr<CompactForTieringCollectorFactory>
NewCompactForTieringCollectorFactory(double compaction_trigger_ratio,
                                     bool track_data_write_time) {
  return std::make_shared<CompactForTieringCollectorFactory>(
      compaction_trigger_ratio, track_data_write_time);
}

Status GetDataCollectionUnixWriteTimeInfoForFile(
    const std::shared_ptr<const TableProperties>& table_properties,
    std::unique_ptr<DataCollectionUnixWriteTimeInfo>* file_info) {
  return CompactForTieringCollector::
      GetDataCollectionUnixWriteTimeInfoFromUserProperties(
          table_properties->user_collected_properties, file_info);
}

Status GetDataCollectionUnixWriteTimeInfoForLevels(
    const std::vector<std::unique_ptr<
        TablePropertiesCollection>>& /* levels_table_properties */,
    std::vector<
        std::unique_ptr<DataCollectionUnixWriteTimeInfo>>* /* levels_info */) {
  return Status::NotSupported();
}

}  // namespace ROCKSDB_NAMESPACE
