//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <optional>

#include "rocksdb/utilities/table_properties_collectors.h"

namespace ROCKSDB_NAMESPACE {

// A user property collector that marks a SST file as need-compaction when for
// the tiering use case. See documentation for
// `CompactForTieringCollectorFactory`.
class CompactForTieringCollector : public TablePropertiesCollector {
 public:
  static const std::string kNumEligibleLastLevelEntriesPropertyName;
  static const std::string kAverageDataUnixWriteTimePropertyName;
  static const std::string kMaxDataUnixWriteTimePropertyName;
  static const std::string kMinDataUnixWriteTimePropertyName;
  static const std::string kNumInfinitelyOldEntriesPropertyName;
  static const std::string kNumWriteTimeAggregatedEntriesPropertyName;
  static const std::string kNumWriteTimeUntrackedEntriesPropertyName;

  CompactForTieringCollector(
      SequenceNumber last_level_inclusive_max_seqno_threshold,
      double compaction_trigger_ratio, bool collect_data_age_stats);

  Status AddUserKeyWithWriteTime(const Slice& key, const Slice& value,
                                 EntryType type, SequenceNumber seq,
                                 uint64_t unix_write_time,
                                 uint64_t file_size) override;

  Status Finish(UserCollectedProperties* properties) override;

  UserCollectedProperties GetReadableProperties() const override;

  const char* Name() const override { return "CompactForTieringCollector"; }

  bool NeedCompact() const override;

  bool WriteTimeTrackingEnabled() const override;

 private:
  void Reset();

  void TrackEligibleEntries(const Slice& value, EntryType type, SequenceNumber seq);

  void TrackWriteTimeMetric(uint64_t unix_write_time);

  void InitializeWithWriteTimeProperties(
      UserCollectedProperties* properties) const;

  void AddEligibleEntriesProperty(UserCollectedProperties* properties) const;

  SequenceNumber last_level_inclusive_max_seqno_threshold_;
  double compaction_trigger_ratio_;
  bool track_eligible_entries_{false};
  bool mark_need_compaction_for_eligible_entries_{false};
  size_t last_level_eligible_entries_counter_ = 0;
  size_t total_entries_counter_ = 0;
  bool finish_called_ = false;
  bool need_compaction_ = false;
  bool track_data_write_time_ = false;
  uint64_t write_time_aggregated_entries_ = 0;
  uint64_t write_time_untracked_entries_ = 0;
  uint64_t write_time_infinitely_old_entries_ = 0;
  uint64_t min_data_write_time_ =
      TablePropertiesCollector::kUnknownUnixWriteTime;
  uint64_t max_data_write_time_ =
      TablePropertiesCollector::kInfinitelyOldUnixWriteTime;
  std::optional<uint64_t> average_data_write_time_ = std::nullopt;
};
}  // namespace ROCKSDB_NAMESPACE
