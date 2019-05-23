// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/db_impl/db_impl.h"
#include "rocksdb/stats_history.h"

namespace rocksdb {

extern const int kMicrosInSecond;
extern const char* kVersionKeyString;
extern const int kPersistentStatsVersion;

class PersistentStatsHistoryIterator final : public StatsHistoryIterator {
 public:
  PersistentStatsHistoryIterator(uint64_t start_time, uint64_t end_time,
                                 DBImpl* db_impl)
      : time_(0),
        start_time_(start_time),
        end_time_(end_time),
        valid_(true),
        version_(0),
        db_impl_(db_impl) {
    AdvanceIteratorByTime(start_time_, end_time_);
  }
  ~PersistentStatsHistoryIterator() override;
  bool Valid() const override;
  Status status() const override;

  void Next() override;
  uint64_t GetStatsTime() const override;
  int GetVersion() const override { return version_; }

  const std::map<std::string, uint64_t>& GetStatsMap() const override;

  // Encode timestamp and stats key into buf
  // Format: timestamp(10 digit) + '#' + key
  // Total length of encoded key will be capped at 100 bytes
  static int EncodeKey(uint64_t timestamp, const std::string& key, int size,
                       char* buf);

  // Encode timestamp and version key into buf
  // Format: timestamp(10 digit) + '#' + kVersionKeyString
  // Total length of encoded key will be capped at 100 bytes
  static int EncodeVersionKey(uint64_t timestamp, int size, char* buf);
  // Seek to timestamp_str + '#' + kVersionKeyString and read the version
  // number of the persitent stats
  // If version number matches, reset iter to timestamp_str
  static int DecodeVersionNumber(const char* timestamp_str, Iterator* iter);

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
  int version_;
  DBImpl* db_impl_;
};

}  // namespace rocksdb
