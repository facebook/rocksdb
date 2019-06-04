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
extern const char* kFormatVersionKeyString;
extern const char* kReaderVersionKeyString;
extern const int kStatsCFMinimumFormatVersion;
extern const int kStatsCFCurrentFormatVersion;
extern const int kPersistentStatsReaderVersion;

enum StatsVersionKeyType : uint32_t {
  kFormatVersion = 1,
  kReaderVersion = 2,
  kKeyTypeMax = 3
};

// Encode format/reader version key to preallocated buf
// Returns 0 if failed, otherwise returns number of bytes encoded
size_t EncodePersistentStatsVersionKey(char* buf, StatsVersionKeyType type);

// Read the version number from persitent stats cf depending on type provided
// Retuns version number on success, or negative number on failure
int DecodePersistentStatsVersionNumber(DBImpl* db,
                                       StatsVersionKeyType type);

// Encode timestamp and stats key into buf
// Format: timestamp(10 digit) + '#' + key
// Total length of encoded key will be capped at 100 bytes
int EncodePersistentStatsKey(uint64_t timestamp, const std::string& key,
                             int size, char* buf);

class PersistentStatsHistoryIterator final : public StatsHistoryIterator {
 public:
  PersistentStatsHistoryIterator(uint64_t start_time, uint64_t end_time,
                                 DBImpl* db_impl)
      : time_(0),
        start_time_(start_time),
        end_time_(end_time),
        valid_(true),
        format_version_(0),
        db_impl_(db_impl) {
    AdvanceIteratorByTime(start_time_, end_time_);
  }
  ~PersistentStatsHistoryIterator() override;
  bool Valid() const override;
  Status status() const override;

  void Next() override;
  uint64_t GetStatsTime() const override;
  int GetFormatVersion() const override { return format_version_; }

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
  int format_version_;
  DBImpl* db_impl_;
};

}  // namespace rocksdb
