// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
#define STORAGE_LEVELDB_INCLUDE_STATISTICS_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <memory>

namespace leveldb {

/**
 * Keep adding ticker's here.
 * Any ticker should have a value less than TICKER_ENUM_MAX.
 * Add a new ticker by assigning it the current value of TICKER_ENUM_MAX
 * And incrementing TICKER_ENUM_MAX.
 */
enum Tickers {
  BLOCK_CACHE_MISS = 0,
  BLOCK_CACHE_HIT = 1,
  BLOOM_FILTER_USEFUL = 2, // no. of times bloom filter has avoided file reads.
  /**
   * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
   * There are 3 reasons currently.
   */
  COMPACTION_KEY_DROP_NEWER_ENTRY = 3, // key was written with a newer value.
  COMPACTION_KEY_DROP_OBSOLETE = 4, // The key is obsolete.
  COMPACTION_KEY_DROP_USER = 5, // user compaction function has dropped the key.
  // Number of keys written to the database via the Put and Write call's
  NUMBER_KEYS_WRITTEN = 6,
  // Number of Keys read,
  NUMBER_KEYS_READ = 7,
  // Bytes written / read
  BYTES_WRITTEN = 8,
  BYTES_READ = 9,
  NO_FILE_CLOSES = 10,
  NO_FILE_OPENS = 11,
  NO_FILE_ERRORS = 12,
  TICKER_ENUM_MAX = 13,
};


/**
 * Keep adding histogram's here.
 * Any histogram whould have value less than HISTOGRAM_ENUM_MAX
 * Add a new Histogram by assigning it the current value of HISTOGRAM_ENUM_MAX
 * And increment HISTOGRAM_ENUM_MAX
 */
enum Histograms {
  DB_GET = 0,
  DB_WRITE = 1,
  COMPACTION_TIME = 2,
  HISTOGRAM_ENUM_MAX = 3,
};

struct HistogramData {
  double median;
  double percentile95;
  double percentile99;
  double average;
  double standard_deviation;
};


class Histogram {
 public:
  // clear's the histogram
  virtual void Clear() = 0;
  virtual ~Histogram();
  // Add a value to be recorded in the histogram.
  virtual void Add(uint64_t value) = 0;
  virtual void Add(double value) = 0;

  virtual std::string ToString() const = 0;

  // Get statistics
  virtual double Median() const = 0;
  virtual double Percentile(double p) const = 0;
  virtual double Average() const = 0;
  virtual double StandardDeviation() const = 0;
  virtual void Data(HistogramData * const data) const = 0;

};

/**
 * A dumb ticker which keeps incrementing through its life time.
 * Not thread safe. Locking is currently managed by external leveldb lock
 */
class Ticker {
 public:
  Ticker() : count_(0) { }

  inline void recordTick() {
    count_++;
  }

  inline void recordTick(int count) {
    count_ += count;
  }

  inline uint64_t getCount() {
    return count_;
  }

 private:
  std::atomic_uint_fast64_t count_;
};

// Analyze the performance of a db
class Statistics {
 public:

  virtual long getTickerCount(Tickers tickerType) = 0;
  virtual void recordTick(Tickers tickerType, uint64_t count = 0) = 0;
  virtual void measureTime(Histograms histogramType, uint64_t count) = 0;
  virtual void measureTime(Histograms histogramType, double count) = 0;

  virtual void histogramData(Histograms type, HistogramData * const data) = 0;

};

// Ease of Use functions
inline void RecordTick(std::shared_ptr<Statistics> statistics,
                       Tickers ticker,
                       uint64_t count = 1) {
  if (statistics) {
    statistics->recordTick(ticker, count);
  }
}
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
