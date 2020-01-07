//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <cstdio>
#include <map>
#include <string>
#include <vector>

#include "monitoring/histogram.h"
#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/options_type.h"
#include "util/core_local.h"
#include "util/mutexlock.h"

#ifdef __clang__
#define ROCKSDB_FIELD_UNUSED __attribute__((__unused__))
#else
#define ROCKSDB_FIELD_UNUSED
#endif  // __clang__

#ifndef STRINGIFY
#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#endif

namespace ROCKSDB_NAMESPACE {

template <uint32_t TICKER_MAX = TICKER_ENUM_MAX,
          uint32_t HISTOGRAM_MAX = HISTOGRAM_ENUM_MAX>
class StatisticsImpl : public Statistics {
 public:
  StatisticsImpl(std::shared_ptr<Statistics> stats);
  virtual ~StatisticsImpl();
  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "BasicStatistics"; }

  virtual uint64_t getTickerCount(uint32_t ticker_type) const override;
  virtual void histogramData(uint32_t histogram_type,
                             HistogramData* const data) const override;
  std::string getHistogramString(uint32_t histogram_type) const override;

  virtual void setTickerCount(uint32_t ticker_type, uint64_t count) override;
  virtual uint64_t getAndResetTickerCount(uint32_t ticker_type) override;
  virtual void recordTick(uint32_t ticker_type, uint64_t count) override;
  // The function is implemented for now for backward compatibility reason.
  // In case a user explicitly calls it, for example, they may have a wrapped
  // Statistics object, passing the call to recordTick() into here, nothing
  // will break.
  void measureTime(uint32_t histogramType, uint64_t time) override {
    recordInHistogram(histogramType, time);
  }
  virtual void recordInHistogram(uint32_t histogram_type,
                                 uint64_t value) override;

  virtual Status Reset() override;
  virtual std::string ToString() const override;
  virtual bool getTickerMap(std::map<std::string, uint64_t>*) const override;
  virtual bool HistEnabledForType(uint32_t type) const override;

  const Customizable* Inner() const override { return stats_.get(); }

 private:
  // If non-nullptr, forwards updates to the object pointed to by `stats_`.
  std::shared_ptr<Statistics> stats_;
  // Synchronizes anything that operates across other cores' local data,
  // such that operations like Reset() can be performed atomically.
  mutable port::Mutex aggregate_lock_;

  // The ticker/histogram data are stored in this structure, which we will store
  // per-core. It is cache-aligned, so tickers/histograms belonging to different
  // cores can never share the same cache line.
  //
  // Alignment attributes expand to nothing depending on the platform
  struct ALIGN_AS(CACHE_LINE_SIZE) StatisticsData {
    StatisticsData() : tickers_{{0}} {};
    std::atomic_uint_fast64_t tickers_[TICKER_MAX];
    HistogramImpl histograms_[HISTOGRAM_MAX];
#ifndef HAVE_ALIGNED_NEW
    char padding[(CACHE_LINE_SIZE -
                  (TICKER_MAX * sizeof(std::atomic_uint_fast64_t) +
                   HISTOGRAM_MAX * sizeof(HistogramImpl)) %
                      CACHE_LINE_SIZE)] ROCKSDB_FIELD_UNUSED;
#endif
    void* operator new(size_t s) { return port::cacheline_aligned_alloc(s); }
    void* operator new[](size_t s) { return port::cacheline_aligned_alloc(s); }
    void operator delete(void* p) { port::cacheline_aligned_free(p); }
    void operator delete[](void* p) { port::cacheline_aligned_free(p); }
  };

  static_assert(sizeof(StatisticsData) % CACHE_LINE_SIZE == 0,
                "Expected " TOSTRING(CACHE_LINE_SIZE) "-byte aligned");

  CoreLocalArray<StatisticsData> per_core_stats_;

  uint64_t getTickerCountLocked(uint32_t ticker_type) const;
  std::unique_ptr<HistogramImpl> getHistogramImplLocked(
      uint32_t histogram_type) const;
  void setTickerCountLocked(uint32_t ticker_type, uint64_t count);
};

static std::unordered_map<std::string, OptionTypeInfo> stats_type_info = {
#ifndef ROCKSDB_LITE
    {"inner", OptionTypeInfo::AsCustomSharedPtr<Statistics>(
                  0, OptionVerificationType::kByNameAllowFromNull,
                  OptionTypeFlags::kCompareNever)},
#endif  // !ROCKSDB_LITE
};

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::StatisticsImpl(
    std::shared_ptr<Statistics> stats)
    : stats_(std::move(stats)) {
  RegisterOptions("StatisticsOptions", &stats_, &stats_type_info);
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::~StatisticsImpl() {}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
uint64_t StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::getTickerCount(
    uint32_t tickerType) const {
  MutexLock lock(&aggregate_lock_);
  return getTickerCountLocked(tickerType);
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
uint64_t StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::getTickerCountLocked(
    uint32_t tickerType) const {
  assert(tickerType < TICKER_MAX);
  uint64_t res = 0;
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res += per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType];
  }
  return res;
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
void StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::histogramData(
    uint32_t histogramType, HistogramData* const data) const {
  MutexLock lock(&aggregate_lock_);
  getHistogramImplLocked(histogramType)->Data(data);
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
std::unique_ptr<HistogramImpl>
StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::getHistogramImplLocked(
    uint32_t histogramType) const {
  assert(histogramType < HISTOGRAM_MAX);
  std::unique_ptr<HistogramImpl> res_hist(new HistogramImpl());
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res_hist->Merge(
        per_core_stats_.AccessAtCore(core_idx)->histograms_[histogramType]);
  }
  return res_hist;
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
std::string StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::getHistogramString(
    uint32_t histogramType) const {
  MutexLock lock(&aggregate_lock_);
  return getHistogramImplLocked(histogramType)->ToString();
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
void StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::setTickerCount(
    uint32_t tickerType, uint64_t count) {
  {
    MutexLock lock(&aggregate_lock_);
    setTickerCountLocked(tickerType, count);
  }
  if (stats_ && tickerType < TICKER_MAX) {
    stats_->setTickerCount(tickerType, count);
  }
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
void StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::setTickerCountLocked(
    uint32_t tickerType, uint64_t count) {
  assert(tickerType < TICKER_MAX);
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    if (core_idx == 0) {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = count;
    } else {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = 0;
    }
  }
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
uint64_t StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::getAndResetTickerCount(
    uint32_t tickerType) {
  uint64_t sum = 0;
  {
    MutexLock lock(&aggregate_lock_);
    assert(tickerType < TICKER_MAX);
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      sum +=
          per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType].exchange(
              0, std::memory_order_relaxed);
    }
  }
  if (stats_ && tickerType < TICKER_MAX) {
    stats_->setTickerCount(tickerType, 0);
  }
  return sum;
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
void StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::recordTick(uint32_t tickerType,
                                                           uint64_t count) {
  if (get_stats_level() <= StatsLevel::kExceptTickers) {
    return;
  }
  if (tickerType < TICKER_MAX) {
    per_core_stats_.Access()->tickers_[tickerType].fetch_add(
        count, std::memory_order_relaxed);
    if (stats_) {
      stats_->recordTick(tickerType, count);
    }
  } else {
    assert(false);
  }
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
void StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::recordInHistogram(
    uint32_t histogramType, uint64_t value) {
  assert(histogramType < HISTOGRAM_MAX);
  if (get_stats_level() <= StatsLevel::kExceptHistogramOrTimers) {
    return;
  }
  per_core_stats_.Access()->histograms_[histogramType].Add(value);
  if (stats_ && histogramType < HISTOGRAM_MAX) {
    stats_->recordInHistogram(histogramType, value);
  }
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
Status StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::Reset() {
  MutexLock lock(&aggregate_lock_);
  for (uint32_t i = 0; i < TICKER_MAX; ++i) {
    setTickerCountLocked(i, 0);
  }
  for (uint32_t i = 0; i < HISTOGRAM_MAX; ++i) {
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      per_core_stats_.AccessAtCore(core_idx)->histograms_[i].Clear();
    }
  }
  return Status::OK();
}

namespace {

// a buffer size used for temp string buffers
const int kTmpStrBufferSize = 200;

}  // namespace

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
std::string StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::ToString() const {
  MutexLock lock(&aggregate_lock_);
  std::string res;
  res.reserve(20000);
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_MAX);
    char buffer[kTmpStrBufferSize];
    snprintf(buffer, kTmpStrBufferSize, "%s COUNT : %" PRIu64 "\n",
             t.second.c_str(), getTickerCountLocked(t.first));
    res.append(buffer);
  }
  for (const auto& h : HistogramsNameMap) {
    assert(h.first < HISTOGRAM_MAX);
    char buffer[kTmpStrBufferSize];
    HistogramData hData;
    getHistogramImplLocked(h.first)->Data(&hData);
    // don't handle failures - buffer should always be big enough and arguments
    // should be provided correctly
    int ret =
        snprintf(buffer, kTmpStrBufferSize,
                 "%s P50 : %f P95 : %f P99 : %f P100 : %f COUNT : %" PRIu64
                 " SUM : %" PRIu64 "\n",
                 h.second.c_str(), hData.median, hData.percentile95,
                 hData.percentile99, hData.max, hData.count, hData.sum);
    if (ret < 0 || ret >= kTmpStrBufferSize) {
      assert(false);
      continue;
    }
    res.append(buffer);
  }
  res.shrink_to_fit();
  return res;
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
bool StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::getTickerMap(
    std::map<std::string, uint64_t>* stats_map) const {
  assert(stats_map);
  if (!stats_map) return false;
  stats_map->clear();
  MutexLock lock(&aggregate_lock_);
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_MAX);
    (*stats_map)[t.second.c_str()] = getTickerCountLocked(t.first);
  }
  return true;
}

template <uint32_t TICKER_MAX, uint32_t HISTOGRAM_MAX>
bool StatisticsImpl<TICKER_MAX, HISTOGRAM_MAX>::HistEnabledForType(
    uint32_t type) const {
  return type < HISTOGRAM_MAX;
}

}  // namespace ROCKSDB_NAMESPACE
