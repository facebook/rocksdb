//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// This file defines a collection of statistics collectors.
#pragma once

#include "rocksdb/table_stats.h"

#include <memory>
#include <string>
#include <vector>

namespace rocksdb {

struct InternalKeyTableStatsNames {
  static const std::string kDeletedKeys;
};

// Collecting the statistics for internal keys. Visible only by internal
// rocksdb modules.
class InternalKeyStatsCollector : public TableStatsCollector {
 public:
  virtual Status Add(const Slice& key, const Slice& value);
  virtual Status Finish(TableStats::UserCollectedStats* stats);
  virtual const char* Name() const { return "InternalKeyStatsCollector"; }

 private:
  uint64_t deleted_keys_ = 0;
};

// When rocksdb creates a new table, it will encode all "user keys" into
// "internal keys", which contains meta information of a given entry.
//
// This class extracts user key from the encoded internal key when Add() is
// invoked.
class UserKeyTableStatsCollector : public TableStatsCollector {
 public:
  explicit UserKeyTableStatsCollector(TableStatsCollector* collector):
    UserKeyTableStatsCollector(
        std::shared_ptr<TableStatsCollector>(collector)
    ) {
  }

  explicit UserKeyTableStatsCollector(
      std::shared_ptr<TableStatsCollector> collector) : collector_(collector) {
  }
  virtual ~UserKeyTableStatsCollector() { }
  virtual Status Add(const Slice& key, const Slice& value);
  virtual Status Finish(TableStats::UserCollectedStats* stats);
  virtual const char* Name() const { return collector_->Name(); }

 protected:
  std::shared_ptr<TableStatsCollector> collector_;
};

}  // namespace rocksdb
