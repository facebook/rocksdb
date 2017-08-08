//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "monitoring/statistics.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include "rocksdb/statistics.h"
#include "port/likely.h"
#include <algorithm>
#include <cstdio>

namespace rocksdb {

std::shared_ptr<Statistics> CreateDBStatistics(bool use_histogram_windowing) {
  if (use_histogram_windowing) {
    return std::make_shared<StatisticsImpl<HistogramWindowingImpl>>(nullptr,
           false);
  }
  return std::make_shared<StatisticsImpl<HistogramImpl>>(nullptr, false);
}

template <class T>
StatisticsImpl<T>::StatisticsImpl(std::shared_ptr<Statistics> stats,
                               bool enable_internal_stats)
    : stats_(std::move(stats)), enable_internal_stats_(enable_internal_stats) {}

template <class T>
StatisticsImpl<T>::~StatisticsImpl() {}

template <class T>
uint64_t StatisticsImpl<T>::getTickerCount(uint32_t tickerType) const {
  MutexLock lock(&aggregate_lock_);
  return getTickerCountLocked(tickerType);
}

template <class T>
uint64_t StatisticsImpl<T>::getTickerCountLocked(uint32_t tickerType) const {
  assert(
    enable_internal_stats_ ?
      tickerType < INTERNAL_TICKER_ENUM_MAX :
      tickerType < TICKER_ENUM_MAX);
  uint64_t res = 0;
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res += per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType];
  }
  return res;
}

template <class T>
void StatisticsImpl<T>::histogramData(uint32_t histogramType,
                                   HistogramData* const data) const {
  MutexLock lock(&aggregate_lock_);
  getHistogramImplLocked(histogramType)->Data(data);
}

template <class T>
std::unique_ptr<T> StatisticsImpl<T>::getHistogramImplLocked(
    uint32_t histogramType) const {
  assert(
    enable_internal_stats_ ?
      histogramType < INTERNAL_HISTOGRAM_ENUM_MAX :
      histogramType < HISTOGRAM_ENUM_MAX);
  std::unique_ptr<T> res_hist(new T());
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res_hist->Merge(
        per_core_stats_.AccessAtCore(core_idx)->histograms_[histogramType]);
  }
  return res_hist;
}

template <class T>
std::string StatisticsImpl<T>::getHistogramString(uint32_t histogramType)
const {
  MutexLock lock(&aggregate_lock_);
  return getHistogramImplLocked(histogramType)->ToString();
}

template <class T>
void StatisticsImpl<T>::setTickerCount(uint32_t tickerType, uint64_t count) {
  {
    MutexLock lock(&aggregate_lock_);
    setTickerCountLocked(tickerType, count);
  }
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->setTickerCount(tickerType, count);
  }
}

template <class T>
void StatisticsImpl<T>::setTickerCountLocked(uint32_t tickerType,
    uint64_t count) {
  assert(enable_internal_stats_ ? tickerType < INTERNAL_TICKER_ENUM_MAX
                                : tickerType < TICKER_ENUM_MAX);
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    if (core_idx == 0) {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = count;
    } else {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = 0;
    }
  }
}

template <class T>
uint64_t StatisticsImpl<T>::getAndResetTickerCount(uint32_t tickerType) {
  uint64_t sum = 0;
  {
    MutexLock lock(&aggregate_lock_);
    assert(enable_internal_stats_ ? tickerType < INTERNAL_TICKER_ENUM_MAX
                                  : tickerType < TICKER_ENUM_MAX);
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      sum +=
          per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType].exchange(
              0, std::memory_order_relaxed);
    }
  }
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->setTickerCount(tickerType, 0);
  }
  return sum;
}

template <class T>
void StatisticsImpl<T>::recordTick(uint32_t tickerType, uint64_t count) {
  assert(
    enable_internal_stats_ ?
      tickerType < INTERNAL_TICKER_ENUM_MAX :
      tickerType < TICKER_ENUM_MAX);
  per_core_stats_.Access()->tickers_[tickerType].fetch_add(
      count, std::memory_order_relaxed);
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->recordTick(tickerType, count);
  }
}

template <class T>
void StatisticsImpl<T>::measureTime(uint32_t histogramType, uint64_t value) {
  assert(
    enable_internal_stats_ ?
      histogramType < INTERNAL_HISTOGRAM_ENUM_MAX :
      histogramType < HISTOGRAM_ENUM_MAX);
  per_core_stats_.Access()->histograms_[histogramType].Add(value);
  if (stats_ && histogramType < HISTOGRAM_ENUM_MAX) {
    stats_->measureTime(histogramType, value);
  }
}

template <class T>
Status StatisticsImpl<T>::Reset() {
  MutexLock lock(&aggregate_lock_);
  for (uint32_t i = 0; i < TICKER_ENUM_MAX; ++i) {
    setTickerCountLocked(i, 0);
  }
  for (uint32_t i = 0; i < HISTOGRAM_ENUM_MAX; ++i) {
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      per_core_stats_.AccessAtCore(core_idx)->histograms_[i].Clear();
    }
  }
  return Status::OK();
}

namespace {

// a buffer size used for temp string buffers
const int kTmpStrBufferSize = 200;

} // namespace

template <class T>
std::string StatisticsImpl<T>::ToString() const {
  MutexLock lock(&aggregate_lock_);
  std::string res;
  res.reserve(20000);
  for (const auto& t : TickersNameMap) {
    if (t.first < TICKER_ENUM_MAX || enable_internal_stats_) {
      char buffer[kTmpStrBufferSize];
      snprintf(buffer, kTmpStrBufferSize, "%s COUNT : %" PRIu64 "\n",
               t.second.c_str(), getTickerCountLocked(t.first));
      res.append(buffer);
    }
  }
  for (const auto& h : HistogramsNameMap) {
    if (h.first < HISTOGRAM_ENUM_MAX || enable_internal_stats_) {
      char buffer[kTmpStrBufferSize];
      HistogramData hData;
      getHistogramImplLocked(h.first)->Data(&hData);
      snprintf(
          buffer, kTmpStrBufferSize,
          "%s statistics Percentiles :=> 50 : %f 95 : %f 99 : %f 100 : %f\n",
          h.second.c_str(), hData.median, hData.percentile95,
          hData.percentile99, hData.max);
      res.append(buffer);
    }
  }
  res.shrink_to_fit();
  return res;
}

template <class T>
bool StatisticsImpl<T>::HistEnabledForType(uint32_t type) const {
  if (LIKELY(!enable_internal_stats_)) {
    return type < HISTOGRAM_ENUM_MAX;
  }
  return true;
}

template <class T>
double StatisticsImpl<T>::getHistogramPercentile(uint32_t type,
    double percentile) const {
  assert(percentile > 0 && percentile < 100);
  MutexLock lock(&aggregate_lock_);
  return getHistogramImplLocked(type)->Percentile(percentile);
}

template <class T>
uint64_t StatisticsImpl<T>::getHistogramSum(uint32_t type) const {
  MutexLock lock(&aggregate_lock_);
  return getHistogramImplLocked(type)->sum();
}

} // namespace rocksdb
