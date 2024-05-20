//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <atomic>
#include <memory>

#include "rocksdb/table_properties.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// A factory of a table property collector that marks a SST
// file as need-compaction when it observe at least "D" deletion
// entries in any "N" consecutive entries or the ratio of tombstone
// entries in the whole file >= the specified deletion ratio.
class CompactOnDeletionCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  // A factory of a table property collector that marks a SST
  // file as need-compaction when it observe at least "D" deletion
  // entries in any "N" consecutive entries, or the ratio of tombstone
  // entries >= deletion_ratio.
  //
  // @param sliding_window_size "N"
  // @param deletion_trigger "D"
  // @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
  //     based on deletion ratio.
  CompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                    size_t deletion_trigger,
                                    double deletion_ratio);

  ~CompactOnDeletionCollectorFactory() {}

  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  // Change the value of sliding_window_size "N"
  // Setting it to 0 disables the delete triggered compaction
  void SetWindowSize(size_t sliding_window_size) {
    sliding_window_size_.store(sliding_window_size);
  }
  size_t GetWindowSize() const { return sliding_window_size_.load(); }

  // Change the value of deletion_trigger "D"
  void SetDeletionTrigger(size_t deletion_trigger) {
    deletion_trigger_.store(deletion_trigger);
  }

  size_t GetDeletionTrigger() const { return deletion_trigger_.load(); }
  // Change deletion ratio.
  // @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
  //     based on deletion ratio.
  void SetDeletionRatio(double deletion_ratio) {
    deletion_ratio_.store(deletion_ratio);
  }

  double GetDeletionRatio() const { return deletion_ratio_.load(); }
  static const char* kClassName() { return "CompactOnDeletionCollector"; }
  const char* Name() const override { return kClassName(); }

  std::string ToString() const override;

 private:
  std::atomic<size_t> sliding_window_size_;
  std::atomic<size_t> deletion_trigger_;
  std::atomic<double> deletion_ratio_;
};

// Creates a factory of a table property collector that marks a SST
// file as need-compaction when it observe at least "D" deletion
// entries in any "N" consecutive entries, or the ratio of tombstone
// entries >= deletion_ratio.
//
// @param sliding_window_size "N". Note that this number will be
//     round up to the smallest multiple of 128 that is no less
//     than the specified size.
// @param deletion_trigger "D".  Note that even when "N" is changed,
//     the specified number for "D" will not be changed.
// @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
//     based on deletion ratio. Disabled by default.
std::shared_ptr<CompactOnDeletionCollectorFactory>
NewCompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                     size_t deletion_trigger,
                                     double deletion_ratio = 0);

// A factory of a table property collector that marks a SST file as
// need-compaction when for the tiering use case, it observes, at least "D" data
// entries that are already eligible to be placed on the last level but are not
// yet on the last level. "D" is the compaction trigger threshold set via the
// initial construction or later via `SetCompactionTrigger` API.
// When tiering is disabled for a column family, no observation is made, the SST
// file won't be marked as need-compaction either.
class CompactForTieringCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  // @param enabled. Whether to enable the collectors to observe and mark files
  // as need compaction.
  // @param compaction_triggers. Mapping from column family id to compaction
  // trigger. Not set it for a column family, or set the compaction trigger to 0
  // effectively will only collect stats and write a user property in sst file
  // Set it to something other than 0 will also mark the file as need compaction
  // if the number of observed eligible entries is equal to or higher than the
  // trigger.
  // No observation will be made if a column family that does not enable
  // tiering, regardless of the configured compaction trigger.
  CompactForTieringCollectorFactory(
      const std::map<uint32_t, size_t>& compaction_triggers, bool enabled);

  ~CompactForTieringCollectorFactory() {}

  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  // Update the value of existing compaction triggers or add compaction triggers
  // for new column families.
  void SetCompactionTriggers(
      const std::map<uint32_t, size_t>& compaction_triggers) {
    WriteLock _(&rwlock_);
    for (const auto& [cf_id, compaction_trigger] : compaction_triggers) {
      auto iter = compaction_triggers_.find(cf_id);
      if (iter == compaction_triggers_.end()) {
        compaction_triggers_.emplace(cf_id, compaction_trigger);
      } else {
        iter->second = compaction_trigger;
      }
    }
  }

  // Update the value of existing compaction trigger or add one for a new
  // column family.
  void SetCompactionTrigger(uint32_t cf_id, size_t compaction_trigger) {
    WriteLock _(&rwlock_);
    auto iter = compaction_triggers_.find(cf_id);
    if (iter == compaction_triggers_.end()) {
      compaction_triggers_.emplace(cf_id, compaction_trigger);
    } else {
      iter->second = compaction_trigger;
    }
  }

  const std::map<uint32_t, size_t>& GetCompactionTriggers() const {
    ReadLock _(&rwlock_);
    return compaction_triggers_;
  }

  size_t GetCompactionTrigger(uint32_t cf_id) const {
    ReadLock _(&rwlock_);
    auto iter = compaction_triggers_.find(cf_id);
    if (iter == compaction_triggers_.end()) {
      return 0;
    }
    return iter->second;
  }

  // To support dynamically enable / disable all the collectors.
  void SetEnabled(bool enabled) {
    WriteLock _(&rwlock_);
    enabled_ = enabled;
  }

  bool GetEnabled() const {
    ReadLock _(&rwlock_);
    return enabled_;
  }

  static const char* kClassName() { return "CompactForTieringCollector"; }
  const char* Name() const override { return kClassName(); }

  std::string ToString() const override;

 private:
  mutable port::RWMutex rwlock_;  // synchronization mutex
  std::map<uint32_t, size_t> compaction_triggers_;
  bool enabled_;
};

std::shared_ptr<CompactForTieringCollectorFactory>
NewCompactForTieringCollectorFactory(
    const std::map<uint32_t, size_t>& compaction_triggers, bool enabled);
}  // namespace ROCKSDB_NAMESPACE
