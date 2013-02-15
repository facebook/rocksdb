// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef LEVELDB_STORAGE_DB_DB_STATISTICS_H_
#define LEVELDB_STORAGE_DB_DB_STATISTICS_H_

#include <cassert>
#include <stdlib.h>
#include <vector>

#include "leveldb/statistics.h"
#include "util/histogram.h"
#include "port/port.h"
#include "util/mutexlock.h"


namespace leveldb {

class DBStatistics: public Statistics {
 public:
  DBStatistics() : allTickers_(TICKER_ENUM_MAX),
                   allHistograms_(HISTOGRAM_ENUM_MAX) { }

  virtual ~DBStatistics() {}

  virtual void incNumFileOpens() {
    MutexLock l(&mu_);
    numFileOpens_++;
  }

  virtual void incNumFileCloses() {
    MutexLock l(&mu_);
    numFileCloses_++;
  }

  virtual void incNumFileErrors() {
    MutexLock l(&mu_);
    numFileErrors_++;
  }

  virtual long getTickerCount(Tickers tickerType) {
    assert(tickerType < TICKER_ENUM_MAX);
    return allTickers_[tickerType].getCount();
  }

  virtual void recordTick(Tickers tickerType, uint64_t count) {
    assert(tickerType < TICKER_ENUM_MAX);
    allTickers_[tickerType].recordTick(count);
  }

  virtual void measureTime(Histograms histogramType, uint64_t value) {
    assert(histogramType < HISTOGRAM_ENUM_MAX);
    allHistograms_[histogramType].Add(value);
  }

  virtual void measureTime(Histograms histogramType, double value) {
    assert(histogramType < HISTOGRAM_ENUM_MAX);
    allHistograms_[histogramType].Add(value);
  }

  virtual void histogramData(Histograms histogramType,
                             HistogramData * const data) {
    assert(histogramType < HISTOGRAM_ENUM_MAX);
    allHistograms_[histogramType].Data(data);
  }

  port::Mutex mu_;
  std::vector<Ticker> allTickers_;
  std::vector<HistogramImpl> allHistograms_;
};
} // namespace leveldb

#endif // LEVELDB_STORAGE_DB_DB_STATISTICS_H_


