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
  virtual uint64_t getAndResetTickerCount(uint32_t ticker_type) override;
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

  // Holds data maintained by each thread for implementing histograms.
  struct ThreadHistogramInfo {
    HistogramImpl value;
    // During teardown, value will be merged into *merged_hist while holding
    // *merge_lock, which also syncs with the merges necessary for reads.
    HistogramImpl* merged_hist;
    port::Mutex* merge_lock;

    ThreadHistogramInfo(HistogramImpl* _merged_hist, port::Mutex* _merge_lock)
        : value(), merged_hist(_merged_hist), merge_lock(_merge_lock) {}
  };

  // Holds global data for implementing tickers.
  struct TickerInfo {
    TickerInfo()
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

  // Holds global data for implementing histograms.
  struct HistogramInfo {
    HistogramInfo()
        : merged_hist(),
          merge_lock(),
          thread_value(new ThreadLocalPtr(&mergeThreadValue)) {}
    // Merged thread-specific values for histograms that have been reset due to
    // thread termination or ThreadLocalPtr destruction. Note these must be
    // destroyed after thread_value since its destructor accesses them.
    HistogramImpl merged_hist;
    mutable port::Mutex merge_lock;
    // Holds thread-specific pointer to ThreadHistogramInfo
    std::unique_ptr<ThreadLocalPtr> thread_value;

    static void mergeThreadValue(void* ptr) {
      auto info_ptr = static_cast<ThreadHistogramInfo*>(ptr);
      {
        MutexLock lock(info_ptr->merge_lock);
        info_ptr->merged_hist->Merge(info_ptr->value);
      }
      delete info_ptr;
    }

    // Returns a histogram that merges all histograms (thread-specific and
    // previously merged ones).
    std::unique_ptr<HistogramImpl> getMergedHistogram() const;
  };

  // Returns the info for this tickerType/thread. It sets a new info with zeroed
  // counter if none exists.
  ThreadTickerInfo* getThreadTickerInfo(uint32_t ticker_type);
  // Returns the info for this histogramType/thread. It sets a new histogram
  // with zeroed data if none exists.
  ThreadHistogramInfo* getThreadHistogramInfo(uint32_t histogram_type);

  TickerInfo tickers_[INTERNAL_TICKER_ENUM_MAX];
  HistogramInfo histograms_[INTERNAL_HISTOGRAM_ENUM_MAX];
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
