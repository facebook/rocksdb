// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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
  // list of counters to query, call GetSupportedStatsCounters to get full list
  std::vector<Tickers> tickers;
  // aggregation operation like avg, sum, min, max, count, percentiles, etc
  AggregationOps aggr_ops;
};

class StatsHistoryIterator {
public:
  StatsHistoryIterator() {}
  virtual ~StatsHistoryIterator() {}

  virtual bool Valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // virtual StatsRecord& GetStatsRecord() = 0; // stats history creation timestamp

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual uint64_t key() const = 0; // stats history creation timestamp

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  // stats history map saved at time in `key()`
  virtual const std::map<std::string, std::string>& value() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const = 0;

private:

  // No copying allowed
  StatsHistoryIterator(const StatsHistoryIterator&);
  void operator=(const StatsHistoryIterator&);
};


}  // namespace rocksdb
