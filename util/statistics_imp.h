//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include "rocksdb/statistics.h"

namespace rocksdb {

// Utility functions
inline void RecordTick(Statistics* statistics,
                       Tickers ticker,
                       uint64_t count = 1) {
  assert(HistogramsNameMap.size() == HISTOGRAM_ENUM_MAX);
  assert(TickersNameMap.size() == TICKER_ENUM_MAX);
  if (statistics) {
    statistics->recordTick(ticker, count);
  }
}

inline void SetTickerCount(Statistics* statistics,
                           Tickers ticker,
                           uint64_t count) {
  assert(HistogramsNameMap.size() == HISTOGRAM_ENUM_MAX);
  assert(TickersNameMap.size() == TICKER_ENUM_MAX);
  if (statistics) {
    statistics->setTickerCount(ticker, count);
  }
}

}
