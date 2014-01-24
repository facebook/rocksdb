//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include "rocksdb/statistics.h"
#include "util/histogram.h"
#include "util/mutexlock.h"

#include <vector>
#include <atomic>

#define UNLIKELY(val) (__builtin_expect((val), 0))

namespace rocksdb {

class StatisticsImpl : public Statistics {
 public:
  StatisticsImpl();
  virtual ~StatisticsImpl();

  virtual long getTickerCount(Tickers tickerType);
  virtual void setTickerCount(Tickers tickerType, uint64_t count);
  virtual void recordTick(Tickers tickerType, uint64_t count);
  virtual void measureTime(Histograms histogramType, uint64_t value);
  virtual void histogramData(Histograms histogramType,
                             HistogramData* const data);

 private:
  std::vector<std::atomic_uint_fast64_t> tickers_;
  std::vector<HistogramImpl> histograms_;
};

// Utility functions
inline void MeasureTime(Statistics* statistics, Histograms histogramType,
                        uint64_t value) {
  if (statistics) {
    statistics->measureTime(histogramType, value);
  }
}

inline void RecordTick(Statistics* statistics, Tickers ticker,
                       uint64_t count = 1) {
  if (statistics) {
    statistics->recordTick(ticker, count);
  }
}

inline void SetTickerCount(Statistics* statistics, Tickers ticker,
                           uint64_t count) {
  if (statistics) {
    statistics->setTickerCount(ticker, count);
  }
}
}
