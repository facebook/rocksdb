//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <atomic>
#include <memory>

#include "rocksdb/table_properties.h"

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
// need-compaction when for the tiering use case, it observes, among all the
// data entries, the ratio of entries that are already eligible to be placed on
// the last level but are not yet on the last level is equal to or higher than
// the configured `compaction_trigger_ratio_`.
// 1) Setting the ratio to be equal to or smaller than 0 disables this collector
// 2) Setting the ratio to be within (0, 1] will write the number of
//     observed eligible entries into a user property and marks a file as
//     need-compaction when aforementioned condition is met.
// 3) Setting the ratio to be higher than 1 can be used to just writes the user
//    table property, and not mark any file as need compaction.
// For a column family that does not enable tiering feature, even if an
// effective configuration is provided, this collector is still disabled.
class CompactForTieringCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  // @param compaction_trigger_ratio: the triggering threshold for the ratio of
  // eligible entries to the total number of entries. See class documentation
  // for what entry is eligible.
  CompactForTieringCollectorFactory(double compaction_trigger_ratio);

  ~CompactForTieringCollectorFactory() {}

  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  void SetCompactionTriggerRatio(double new_ratio) {
    compaction_trigger_ratio_.store(new_ratio);
  }

  double GetCompactionTriggerRatio() const {
    return compaction_trigger_ratio_.load();
  }

  static const char* kClassName() { return "CompactForTieringCollector"; }
  const char* Name() const override { return kClassName(); }

  std::string ToString() const override;

 private:
  std::atomic<double> compaction_trigger_ratio_;
};

std::shared_ptr<CompactForTieringCollectorFactory>
NewCompactForTieringCollectorFactory(double compaction_trigger_ratio);
}  // namespace ROCKSDB_NAMESPACE
