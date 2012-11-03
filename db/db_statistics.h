// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cassert>
#include <stdlib.h>
#include <vector>

#include "leveldb/statistics.h"
#include "port/port.h"
#include "util/mutexlock.h"
namespace leveldb {

class DBStatistics: public Statistics {
 public:
  DBStatistics() : allTickers_(TICKER_ENUM_MAX) { }

  void incNumFileOpens() {
    MutexLock l(&mu_);
    numFileOpens_++;
  }

  void incNumFileCloses() {
    MutexLock l(&mu_);
    numFileCloses_++;
  }

  void incNumFileErrors() {
    MutexLock l(&mu_);
    numFileErrors_++;
  }

  long getTickerCount(Tickers tickerType) {
    assert(tickerType < MAX_NO_TICKERS);
    return allTickers_[tickerType].getCount();
  }

  void recordTick(Tickers tickerType) {
    assert(tickerType < MAX_NO_TICKERS);
    allTickers_[tickerType].recordTick();
  }

 private:
  port::Mutex mu_;
  std::vector<Ticker> allTickers_;
};
}


