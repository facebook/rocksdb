//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <cinttypes>

#include "monitoring/histogram.h"
#include "monitoring/statistics_impl.h"
#include "port/likely.h"
#include "rocksdb/statistics.h"

namespace ROCKSDB_NAMESPACE {

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
std::shared_ptr<Statistics> CreateDBStatistics() {
  return std::make_shared<StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>>(nullptr);
}

// Utility functions
inline void RecordInHistogram(Statistics* statistics, uint32_t histogram_type,
                              uint64_t value) {
  if (statistics) {
    statistics->recordInHistogram(histogram_type, value);
  }
}

inline void RecordTimeToHistogram(Statistics* statistics,
                                  uint32_t histogram_type, uint64_t value) {
  if (statistics) {
    statistics->reportTimeToHistogram(histogram_type, value);
  }
}

inline void RecordTick(Statistics* statistics, uint32_t ticker_type,
                       uint64_t count = 1) {
  if (statistics) {
    statistics->recordTick(ticker_type, count);
  }
}

inline void SetTickerCount(Statistics* statistics, uint32_t ticker_type,
                           uint64_t count) {
  if (statistics) {
    statistics->setTickerCount(ticker_type, count);
  }
}

}  // namespace ROCKSDB_NAMESPACE
