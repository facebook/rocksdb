//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include "rocksdb/statistics.h"

#include <vector>
#include <atomic>
#include <string>

#include "port/likely.h"
#include "port/port.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/thread_local.h"

namespace rocksdb {

enum TickersInternal : uint32_t {
  INTERNAL_TICKER_ENUM_START = TICKER_ENUM_MAX,
  INTERNAL_TICKER_ENUM_MAX
};

enum HistogramsInternal : uint32_t {
  INTERNAL_HISTOGRAM_START = HISTOGRAM_ENUM_MAX,
  INTERNAL_HISTOGRAM_ENUM_MAX
};


class StatisticsImpl : public Statistics {
 public:
  StatisticsImpl(std::shared_ptr<Statistics> stats,
                 bool enable_internal_stats);
  virtual ~StatisticsImpl();

  virtual uint64_t getTickerCount(uint32_t ticker_type) const override;
  virtual void histogramData(uint32_t histogram_type,
                             HistogramData* const data) const override;
  std::string getHistogramString(uint32_t histogram_type) const override;

  virtual void setTickerCount(uint32_t ticker_type, uint64_t count) override;
  virtual void recordTick(uint32_t ticker_type, uint64_t count) override;
  virtual void measureTime(uint32_t histogram_type, uint64_t value) override;

  virtual std::string ToString() const override;
  virtual bool HistEnabledForType(uint32_t type) const override;

 private:
  std::shared_ptr<Statistics> stats_shared_;
  Statistics* stats_;
  bool enable_internal_stats_;
  // Synchronizes setTickerCount()/getTickerCount() operations so partially
  // completed setTickerCount() won't be visible.
  mutable port::Mutex aggregate_lock_;

  // Holds data maintained by each thread for implementing tickers.
  struct ThreadTickerInfo {
    std::atomic_uint_fast64_t value;
    // During teardown, value will be summed into *merged_sum.
    std::atomic_uint_fast64_t* merged_sum;

    ThreadTickerInfo(uint_fast64_t _value,
                     std::atomic_uint_fast64_t* _merged_sum)
        : value(_value), merged_sum(_merged_sum) {}
  };

  struct Ticker {
    Ticker()
        : thread_value(new ThreadLocalPtr(&mergeThreadValue)), merged_sum(0) {}
    // Holds thread-specific pointer to ThreadTickerInfo
    std::unique_ptr<ThreadLocalPtr> thread_value;
    // Sum of thread-specific values for tickers that have been reset due to
    // thread termination or ThreadLocalPtr destruction. Also, this is used by
    // setTickerCount() to conveniently change the global value by setting this
    // while simultaneously zeroing all thread-local values.
    std::atomic_uint_fast64_t merged_sum;

    static void mergeThreadValue(void* ptr) {
      auto info_ptr = static_cast<ThreadTickerInfo*>(ptr);
      *info_ptr->merged_sum += info_ptr->value;
      delete info_ptr;
    }
  };

  // Returns the info for this tickerType/thread. It sets a new info with zeroed
  // counter if none exists.
  ThreadTickerInfo* getThreadTickerInfo(uint32_t tickerType);

  Ticker tickers_[INTERNAL_TICKER_ENUM_MAX];
  // Attributes expand to nothing depending on the platform
  __declspec(align(64))
  HistogramImpl histograms_[INTERNAL_HISTOGRAM_ENUM_MAX]
      __attribute__((aligned(64)));
};

// Utility functions
inline void MeasureTime(Statistics* statistics, uint32_t histogram_type,
                        uint64_t value) {
  if (statistics) {
    statistics->measureTime(histogram_type, value);
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

}
