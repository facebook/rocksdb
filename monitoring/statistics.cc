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

std::shared_ptr<Statistics> CreateDBStatistics() {
  return std::make_shared<StatisticsImpl>(nullptr, false);
}

StatisticsImpl::StatisticsImpl(std::shared_ptr<Statistics> stats,
                               bool enable_internal_stats)
    : stats_(std::move(stats)), enable_internal_stats_(enable_internal_stats) {}

StatisticsImpl::~StatisticsImpl() {}

uint64_t StatisticsImpl::getTickerCount(uint32_t tickerType) const {
  MutexLock lock(&aggregate_lock_);
  return getTickerCountLocked(tickerType);
}

uint64_t StatisticsImpl::getTickerCountLocked(uint32_t tickerType) const {
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

void StatisticsImpl::histogramData(uint32_t histogramType,
                                   HistogramData* const data) const {
  MutexLock lock(&aggregate_lock_);
  getHistogramImplLocked(histogramType)->Data(data);
}

std::unique_ptr<HistogramImpl> StatisticsImpl::getHistogramImplLocked(
    uint32_t histogramType) const {
  assert(
    enable_internal_stats_ ?
      histogramType < INTERNAL_HISTOGRAM_ENUM_MAX :
      histogramType < HISTOGRAM_ENUM_MAX);
  std::unique_ptr<HistogramImpl> res_hist(new HistogramImpl());
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res_hist->Merge(
        per_core_stats_.AccessAtCore(core_idx)->histograms_[histogramType]);
  }
  return res_hist;
}

std::string StatisticsImpl::getHistogramString(uint32_t histogramType) const {
  MutexLock lock(&aggregate_lock_);
  return getHistogramImplLocked(histogramType)->ToString();
}

void StatisticsImpl::setTickerCount(uint32_t tickerType, uint64_t count) {
  {
    MutexLock lock(&aggregate_lock_);
    setTickerCountLocked(tickerType, count);
  }
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->setTickerCount(tickerType, count);
  }
}

void StatisticsImpl::setTickerCountLocked(uint32_t tickerType, uint64_t count) {
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

uint64_t StatisticsImpl::getAndResetTickerCount(uint32_t tickerType) {
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

void StatisticsImpl::recordTick(uint32_t tickerType, uint64_t count) {
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

void StatisticsImpl::measureTime(uint32_t histogramType, uint64_t value) {
  assert(
    enable_internal_stats_ ?
      histogramType < INTERNAL_HISTOGRAM_ENUM_MAX :
      histogramType < HISTOGRAM_ENUM_MAX);
  per_core_stats_.Access()->histograms_[histogramType].Add(value);
  if (stats_ && histogramType < HISTOGRAM_ENUM_MAX) {
    stats_->measureTime(histogramType, value);
  }
}

Status StatisticsImpl::Reset() {
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

std::string StatisticsImpl::ToString() const {
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
      // don't handle failures - buffer should always be big enough and arguments
      // should be provided correctly
      int ret = snprintf(
          buffer, kTmpStrBufferSize,
          "%s P50 : %f P95 : %f P99 : %f P100 : %f COUNT : %" PRIu64 " SUM : %"
          PRIu64 "\n", h.second.c_str(), hData.median, hData.percentile95,
          hData.percentile99, hData.max, hData.count, hData.sum);
      if (ret < 0 || ret >= kTmpStrBufferSize) {
        assert(false);
        continue;
      }
      res.append(buffer);
    }
  }
  res.shrink_to_fit();
  return res;
}

bool StatisticsImpl::HistEnabledForType(uint32_t type) const {
  if (LIKELY(!enable_internal_stats_)) {
    return type < HISTOGRAM_ENUM_MAX;
  }
  return true;
}

} // namespace rocksdb
