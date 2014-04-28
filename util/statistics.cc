//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "util/statistics.h"
#include "rocksdb/statistics.h"
#include <algorithm>
#include <cstdio>

namespace rocksdb {

std::shared_ptr<Statistics> CreateDBStatistics() {
  return std::make_shared<StatisticsImpl>();
}

StatisticsImpl::StatisticsImpl() {}

StatisticsImpl::~StatisticsImpl() {}

long StatisticsImpl::getTickerCount(Tickers tickerType) {
  assert(tickerType < TICKER_ENUM_MAX);
  return tickers_[tickerType].value;
}

void StatisticsImpl::setTickerCount(Tickers tickerType, uint64_t count) {
  assert(tickerType < TICKER_ENUM_MAX);
  tickers_[tickerType].value = count;
}

void StatisticsImpl::recordTick(Tickers tickerType, uint64_t count) {
  assert(tickerType < TICKER_ENUM_MAX);
  tickers_[tickerType].value += count;
}

void StatisticsImpl::measureTime(Histograms histogramType, uint64_t value) {
  assert(histogramType < HISTOGRAM_ENUM_MAX);
  histograms_[histogramType].Add(value);
}

void StatisticsImpl::histogramData(Histograms histogramType,
                                   HistogramData* const data) {
  assert(histogramType < HISTOGRAM_ENUM_MAX);
  histograms_[histogramType].Data(data);
}

namespace {

// a buffer size used for temp string buffers
const int kBufferSize = 200;

std::string HistogramToString (
    Statistics* dbstats,
    const Histograms& histogram_type,
    const std::string& name) {

  char buffer[kBufferSize];
  HistogramData histogramData;
  dbstats->histogramData(histogram_type, &histogramData);
  snprintf(
      buffer,
      kBufferSize,
      "%s statistics Percentiles :=> 50 : %f 95 : %f 99 : %f\n",
      name.c_str(),
      histogramData.median,
      histogramData.percentile95,
      histogramData.percentile99
  );
  return std::string(buffer);
};

std::string TickerToString(Statistics* dbstats, const Tickers& ticker,
                           const std::string& name) {
  char buffer[kBufferSize];
  snprintf(buffer, kBufferSize, "%s COUNT : %ld\n",
            name.c_str(), dbstats->getTickerCount(ticker));
  return std::string(buffer);
};
} // namespace

std::string Statistics::ToString() {
  std::string res;
  res.reserve(20000);
  for (const auto& t : TickersNameMap) {
    res.append(TickerToString(this, t.first, t.second));
  }
  for (const auto& h : HistogramsNameMap) {
    res.append(HistogramToString(this, h.first, h.second));
  }
  res.shrink_to_fit();
  return res;
}

} // namespace rocksdb
