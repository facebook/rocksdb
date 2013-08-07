// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
#define STORAGE_LEVELDB_INCLUDE_STATISTICS_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <memory>
#include <vector>

namespace leveldb {

/**
 * Keep adding ticker's here.
 * Any ticker should have a value less than TICKER_ENUM_MAX.
 * Add a new ticker by assigning it the current value of TICKER_ENUM_MAX
 * Add a string representation in TickersNameMap below.
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
  // Time system had to wait to do LO-L1 compactions
  STALL_L0_SLOWDOWN_MICROS = 13,
  // Time system had to wait to move memtable to L1.
  STALL_MEMTABLE_COMPACTION_MICROS = 14,
  // write throttle because of too many files in L0
  STALL_L0_NUM_FILES_MICROS = 15,
  RATE_LIMIT_DELAY_MILLIS = 16,

  NO_ITERATORS = 17, // number of iterators currently open

  // Number of MultiGet calls, keys read, and bytes read
  NUMBER_MULTIGET_CALLS = 18,
  NUMBER_MULTIGET_KEYS_READ = 19,
  NUMBER_MULTIGET_BYTES_READ = 20,

  NUMBER_FILTERED_DELETES = 21,

  TICKER_ENUM_MAX = 22
};

const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
  { BLOCK_CACHE_MISS, "rocksdb.block.cache.miss" },
  { BLOCK_CACHE_HIT, "rocksdb.block.cache.hit" },
  { BLOOM_FILTER_USEFUL, "rocksdb.bloom.filter.useful" },
  { COMPACTION_KEY_DROP_NEWER_ENTRY, "rocksdb.compaction.key.drop.new" },
  { COMPACTION_KEY_DROP_OBSOLETE, "rocksdb.compaction.key.drop.obsolete" },
  { COMPACTION_KEY_DROP_USER, "rocksdb.compaction.key.drop.user" },
  { NUMBER_KEYS_WRITTEN, "rocksdb.number.keys.written" },
  { NUMBER_KEYS_READ, "rocksdb.number.keys.read" },
  { BYTES_WRITTEN, "rocksdb.bytes.written" },
  { BYTES_READ, "rocksdb.bytes.read" },
  { NO_FILE_CLOSES, "rocksdb.no.file.closes" },
  { NO_FILE_OPENS, "rocksdb.no.file.opens" },
  { NO_FILE_ERRORS, "rocksdb.no.file.errors" },
  { STALL_L0_SLOWDOWN_MICROS, "rocksdb.l0.slowdown.micros" },
  { STALL_MEMTABLE_COMPACTION_MICROS, "rocksdb.memtable.compaction.micros" },
  { STALL_L0_NUM_FILES_MICROS, "rocksdb.l0.num.files.stall.micros" },
  { RATE_LIMIT_DELAY_MILLIS, "rocksdb.rate.limit.delay.millis" },
  { NO_ITERATORS, "rocksdb.num.iterators" },
  { NUMBER_MULTIGET_CALLS, "rocksdb.number.multiget.get" },
  { NUMBER_MULTIGET_KEYS_READ, "rocksdb.number.multiget.keys.read" },
  { NUMBER_MULTIGET_BYTES_READ, "rocksdb.number.multiget.bytes.read" },
  { NUMBER_FILTERED_DELETES, "rocksdb.number.deletes.filtered" }
};

/**
 * Keep adding histogram's here.
 * Any histogram whould have value less than HISTOGRAM_ENUM_MAX
 * Add a new Histogram by assigning it the current value of HISTOGRAM_ENUM_MAX
 * Add a string representation in HistogramsNameMap below
 * And increment HISTOGRAM_ENUM_MAX
 */
enum Histograms {
  DB_GET = 0,
  DB_WRITE = 1,
  COMPACTION_TIME = 2,
  TABLE_SYNC_MICROS = 3,
  COMPACTION_OUTFILE_SYNC_MICROS = 4,
  WAL_FILE_SYNC_MICROS = 5,
  MANIFEST_FILE_SYNC_MICROS = 6,
  // TIME SPENT IN IO DURING TABLE OPEN
  TABLE_OPEN_IO_MICROS = 7,
  DB_MULTIGET = 8,
  READ_BLOCK_COMPACTION_MICROS = 9,
  READ_BLOCK_GET_MICROS = 10,
  WRITE_RAW_BLOCK_MICROS = 11,

  STALL_L0_SLOWDOWN_COUNT = 12,
  STALL_MEMTABLE_COMPACTION_COUNT = 13,
  STALL_L0_NUM_FILES_COUNT = 14,
  HARD_RATE_LIMIT_DELAY_COUNT = 15,
  SOFT_RATE_LIMIT_DELAY_COUNT = 16,
  NUM_FILES_IN_SINGLE_COMPACTION = 17,
  HISTOGRAM_ENUM_MAX = 18
};

const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap = {
  { DB_GET, "rocksdb.db.get.micros" },
  { DB_WRITE, "rocksdb.db.write.micros" },
  { COMPACTION_TIME, "rocksdb.compaction.times.micros" },
  { TABLE_SYNC_MICROS, "rocksdb.table.sync.micros" },
  { COMPACTION_OUTFILE_SYNC_MICROS, "rocksdb.compaction.outfile.sync.micros" },
  { WAL_FILE_SYNC_MICROS, "rocksdb.wal.file.sync.micros" },
  { MANIFEST_FILE_SYNC_MICROS, "rocksdb.manifest.file.sync.micros" },
  { TABLE_OPEN_IO_MICROS, "rocksdb.table.open.io.micros" },
  { DB_MULTIGET, "rocksdb.db.multiget.micros" },
  { READ_BLOCK_COMPACTION_MICROS, "rocksdb.read.block.compaction.micros" },
  { READ_BLOCK_GET_MICROS, "rocksdb.read.block.get.micros" },
  { WRITE_RAW_BLOCK_MICROS, "rocksdb.write.raw.block.micros" },
  { STALL_L0_SLOWDOWN_COUNT, "rocksdb.l0.slowdown.count"},
  { STALL_MEMTABLE_COMPACTION_COUNT, "rocksdb.memtable.compaction.count"},
  { STALL_L0_NUM_FILES_COUNT, "rocksdb.num.files.stall.count"},
  { HARD_RATE_LIMIT_DELAY_COUNT, "rocksdb.hard.rate.limit.delay.count"},
  { SOFT_RATE_LIMIT_DELAY_COUNT, "rocksdb.soft.rate.limit.delay.count"},
  { NUM_FILES_IN_SINGLE_COMPACTION, "rocksdb.numfiles.in.singlecompaction" }
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
  virtual void measureTime(Histograms histogramType, uint64_t time) = 0;

  virtual void histogramData(Histograms type, HistogramData * const data) = 0;
  // String representation of the statistic object.
  std::string ToString();
};

// Create a concrete DBStatistics object
std::shared_ptr<Statistics> CreateDBStatistics();

// Ease of Use functions
inline void RecordTick(std::shared_ptr<Statistics> statistics,
                       Tickers ticker,
                       uint64_t count = 1) {
  assert(HistogramsNameMap.size() == HISTOGRAM_ENUM_MAX);
  assert(TickersNameMap.size() == TICKER_ENUM_MAX);
  if (statistics) {
    statistics->recordTick(ticker, count);
  }
}
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
