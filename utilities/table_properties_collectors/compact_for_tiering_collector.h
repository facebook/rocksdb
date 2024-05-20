//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/utilities/table_properties_collectors.h"

namespace ROCKSDB_NAMESPACE {

// A user property collector that marks a SST file as need-compaction when for
// the tiering use case, it observes, at least "compaction_trigger"  number of
// data entries that are already eligible to be placed on the last level but are
// not yet on the last level.
class CompactForTieringCollector : public TablePropertiesCollector {
 public:
  static const std::string kNumEligibleLastLevelEntriesPropertyName;

  CompactForTieringCollector(
      int level_at_creation, int num_levels,
      SequenceNumber last_level_inclusive_max_seqno_threshold_,
      size_t compaction_trigger, bool enabled);

  Status AddUserKey(const Slice& key, const Slice& value, EntryType type,
                    SequenceNumber seq, uint64_t file_size) override;

  Status Finish(UserCollectedProperties* properties) override;

  UserCollectedProperties GetReadableProperties() const override;

  const char* Name() const override { return "CompactForTieringCollector"; }

  bool NeedCompact() const override;

 private:
  void Reset();

  int level_at_creation_;
  int num_levels_;
  SequenceNumber last_level_inclusive_max_seqno_threshold_;
  size_t compaction_trigger_;
  size_t last_level_eligible_entries_counter_ = 0;
  bool finish_called_ = false;
  bool need_compaction_ = false;
  bool enabled_;
};
}  // namespace ROCKSDB_NAMESPACE
