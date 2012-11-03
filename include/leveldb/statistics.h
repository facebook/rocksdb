// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
#define STORAGE_LEVELDB_INCLUDE_STATISTICS_H_

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
  TICKER_ENUM_MAX = 2,
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

  inline uint64_t getCount() {
    return count_;
  }

 private:
  uint64_t count_;

};

// Analyze the performance of a db
class Statistics {
 public:
  // Create an Statistics object with default values for all fields.
  Statistics() : numFileOpens_(0), numFileCloses_(0),
                 numFileErrors_(0) {}

  virtual void incNumFileOpens() = 0;
  virtual void incNumFileCloses() = 0;
  virtual void incNumFileErrors() = 0;

  virtual long getNumFileOpens() { return numFileOpens_;}
  virtual long getNumFileCloses() { return numFileCloses_;}
  virtual long getNumFileErrors() { return numFileErrors_;}
  virtual ~Statistics() {}

  virtual long getTickerCount(Tickers tickerType) = 0;
  virtual void recordTick(Tickers tickerType) = 0;

 protected:
  long numFileOpens_;
  long numFileCloses_;
  long numFileErrors_;
};

// Ease of Use functions
inline void RecordTick(Statistics* const statistics, Tickers ticker) {
  if (statistics != NULL) {
    statistics->recordTick(ticker);
  }
};
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
