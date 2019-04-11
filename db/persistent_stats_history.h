// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/stats_history.h"

namespace rocksdb {

// 16 digit microseconds timestamp => [Sep 9, 2001 ~ Nov 20, 2286]
extern const int kTimestampStringMinimumLength;

class PersistentStatsHistoryIterator final : public StatsHistoryIterator {
 public:
  PersistentStatsHistoryIterator(uint64_t start_time, uint64_t end_time,
                                 DBImpl* db_impl)
      : time_(0),
        start_time_(start_time),
        end_time_(end_time),
        valid_(true),
        db_impl_(db_impl) {
    AdvanceIteratorByTime(start_time_, end_time_);
  }
  ~PersistentStatsHistoryIterator() override;
  bool Valid() const override;
  Status status() const override;

  void Next() override;
  uint64_t GetStatsTime() const override;

  const std::map<std::string, uint64_t>& GetStatsMap() const override;

 private:
  // advance the iterator to the next stats history record with timestamp
  // between [start_time, end_time)
  void AdvanceIteratorByTime(uint64_t start_time, uint64_t end_time);

  // No copying allowed
  PersistentStatsHistoryIterator(const PersistentStatsHistoryIterator&) =
      delete;
  void operator=(const PersistentStatsHistoryIterator&) = delete;
  PersistentStatsHistoryIterator(PersistentStatsHistoryIterator&&) = delete;
  PersistentStatsHistoryIterator& operator=(PersistentStatsHistoryIterator&&) =
      delete;

  uint64_t time_;
  uint64_t start_time_;
  uint64_t end_time_;
  std::map<std::string, uint64_t> stats_map_;
  Status status_;
  bool valid_;
  DBImpl* db_impl_;
};

}  // namespace rocksdb
