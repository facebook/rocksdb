// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// //  This source code is licensed under both the GPLv2 (found in the
// //  COPYING file in the root directory) and Apache 2.0 License
// //  (found in the LICENSE.Apache file in the root directory).
// // Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// // Use of this source code is governed by a BSD-style license that can be
// // found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <map>
#include <string>

// #include "db/db_impl.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace rocksdb {

class DBImpl;

enum AggregationOps : uint32_t {
  AVG = 0,
  SUM,
  MIN,
  MAX,
  COUNT,
  COUNT_DISTINCT,
  P25,
  P50,
  P75,
  P90,
  P99,

  AGGREGATION_ENUM_MAX,
};

// Options that controls GetStatsHistory calls
struct GetStatsOptions {
  uint64_t start_time;
  uint64_t end_time;
  // list of counters to query, call GetSupportedStatsCounters to get full list
  std::vector<Tickers> tickers;
  // aggregation operation like avg, sum, min, max, count, percentiles, etc
  AggregationOps aggr_ops;
};

class StatsHistoryIterator {
public:
  StatsHistoryIterator(bool /*in_memory*/, GetStatsOptions& /*stats_opts*/,
                       DBImpl* /*db_impl*/);
  bool Valid();

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  void SeekToFirst();

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  void Next(); // advance to next stats history

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  uint64_t key(); // stats history creation timestamp

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  // stats history map saved at time in `key()`
  const std::map<std::string, std::string>& value() const;

  // If an error has occurred, return it.  Else return an ok status.
  Status status();

  // Advance the iterator to any valid timestamp >= time_start
  void AdvanceIteratorByTime(uint64_t start_time, uint64_t end_time);
private:
  uint64_t key_;
  std::map<std::string, std::string> value_;
  Status status_;
  bool in_memory_;
  bool valid_;
  GetStatsOptions stats_opts_;
  DBImpl* db_impl_;

  // No copying allowed
  StatsHistoryIterator(const StatsHistoryIterator&);
  void operator=(const StatsHistoryIterator&);
};


}  // namespace rocksdb
