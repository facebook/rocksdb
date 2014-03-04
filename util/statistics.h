//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include "rocksdb/statistics.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "port/likely.h"

#include <vector>
#include <atomic>


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
  struct Ticker {
    Ticker() : value(uint_fast64_t()) {}

    std::atomic_uint_fast64_t value;
    // Pad the structure to make it size of 64 bytes. A plain array of
    // std::atomic_uint_fast64_t results in huge performance degradataion
    // due to false sharing.
    char padding[64 - sizeof(std::atomic_uint_fast64_t)];
  };

  Ticker tickers_[TICKER_ENUM_MAX] __attribute__((aligned(64)));
  HistogramImpl histograms_[HISTOGRAM_ENUM_MAX] __attribute__((aligned(64)));
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
