// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_ROCKSDB_INCLUDE_STATISTICS_H_
#define STORAGE_ROCKSDB_INCLUDE_STATISTICS_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <memory>
#include <vector>

namespace rocksdb {

/**
 * Keep adding ticker's here.
 *  1. Any ticker should be added before TICKER_ENUM_MAX.
 *  2. Add a readable string in TickersNameMap below for the newly added ticker.
 */
enum Tickers {
  // total block cache misses
  // REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
  //                               BLOCK_CACHE_FILTER_MISS +
  //                               BLOCK_CACHE_DATA_MISS;
  BLOCK_CACHE_MISS,
  // total block cache hit
  // REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
  //                              BLOCK_CACHE_FILTER_HIT +
  //                              BLOCK_CACHE_DATA_HIT;
  BLOCK_CACHE_HIT,
  // # of blocks added to block cache.
  BLOCK_CACHE_ADD,
  // # of times cache miss when accessing index block from block cache.
  BLOCK_CACHE_INDEX_MISS,
  // # of times cache hit when accessing index block from block cache.
  BLOCK_CACHE_INDEX_HIT,
  // # of times cache miss when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_MISS,
  // # of times cache hit when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_HIT,
  // # of times cache miss when accessing data block from block cache.
  BLOCK_CACHE_DATA_MISS,
  // # of times cache hit when accessing data block from block cache.
  BLOCK_CACHE_DATA_HIT,
  // # of times bloom filter has avoided file reads.
  BLOOM_FILTER_USEFUL,

  // # of memtable hits.
  MEMTABLE_HIT,
  // # of memtable misses.
  MEMTABLE_MISS,

  /**
   * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
   * There are 3 reasons currently.
   */
  COMPACTION_KEY_DROP_NEWER_ENTRY, // key was written with a newer value.
  COMPACTION_KEY_DROP_OBSOLETE, // The key is obsolete.
  COMPACTION_KEY_DROP_USER, // user compaction function has dropped the key.

  // Number of keys written to the database via the Put and Write call's
  NUMBER_KEYS_WRITTEN,
  // Number of Keys read,
  NUMBER_KEYS_READ,
  // Number keys updated, if inplace update is enabled
  NUMBER_KEYS_UPDATED,
  // Bytes written / read
  BYTES_WRITTEN,
  BYTES_READ,
  NO_FILE_CLOSES,
  NO_FILE_OPENS,
  NO_FILE_ERRORS,
  // Time system had to wait to do LO-L1 compactions
  STALL_L0_SLOWDOWN_MICROS,
  // Time system had to wait to move memtable to L1.
  STALL_MEMTABLE_COMPACTION_MICROS,
  // write throttle because of too many files in L0
  STALL_L0_NUM_FILES_MICROS,
  RATE_LIMIT_DELAY_MILLIS,

  NO_ITERATORS, // number of iterators currently open

  // Number of MultiGet calls, keys read, and bytes read
  NUMBER_MULTIGET_CALLS,
  NUMBER_MULTIGET_KEYS_READ,
  NUMBER_MULTIGET_BYTES_READ,

  // Number of deletes records that were not required to be
  // written to storage because key does not exist
  NUMBER_FILTERED_DELETES,
  NUMBER_MERGE_FAILURES,
  SEQUENCE_NUMBER,

  // number of times bloom was checked before creating iterator on a
  // file, and the number of times the check was useful in avoiding
  // iterator creation (and thus likely IOPs).
  BLOOM_FILTER_PREFIX_CHECKED,
  BLOOM_FILTER_PREFIX_USEFUL,

  // Number of times we had to reseek inside an iteration to skip
  // over large number of keys with same userkey.
  NUMBER_OF_RESEEKS_IN_ITERATION,

  // Record the number of calls to GetUpadtesSince. Useful to keep track of
  // transaction log iterator refreshes
  GET_UPDATES_SINCE_CALLS,

  BLOCK_CACHE_COMPRESSED_MISS, // miss in the compressed block cache
  BLOCK_CACHE_COMPRESSED_HIT,  // hit in the compressed block cache

  WAL_FILE_SYNCED,  // Number of times WAL sync is done
  WAL_FILE_BYTES,   // Number of bytes written to WAL

  // Writes can be processed by requesting thread or by the thread at the
  // head of the writers queue.
  WRITE_DONE_BY_SELF,
  WRITE_DONE_BY_OTHER,

  WRITE_WITH_WAL,      // Number of Write calls that request WAL

  COMPACT_READ_BYTES,  // Bytes read during compaction
  COMPACT_WRITE_BYTES, // Bytes written during compaction

  TICKER_ENUM_MAX
};

// The order of items listed in  Tickers should be the same as
// the order listed in TickersNameMap
const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
  { BLOCK_CACHE_MISS, "rocksdb.block.cache.miss" },
  { BLOCK_CACHE_HIT, "rocksdb.block.cache.hit" },
  { BLOCK_CACHE_ADD, "rocksdb.block.cache.add" },
  { BLOCK_CACHE_INDEX_MISS, "rocksdb.block.cache.index.miss" },
  { BLOCK_CACHE_INDEX_HIT, "rocksdb.block.cache.index.hit" },
  { BLOCK_CACHE_FILTER_MISS, "rocksdb.block.cache.filter.miss" },
  { BLOCK_CACHE_FILTER_HIT, "rocksdb.block.cache.filter.hit" },
  { BLOCK_CACHE_DATA_MISS, "rocksdb.block.cache.data.miss" },
  { BLOCK_CACHE_DATA_HIT, "rocksdb.block.cache.data.hit" },
  { BLOOM_FILTER_USEFUL, "rocksdb.bloom.filter.useful" },
  { MEMTABLE_HIT, "rocksdb.memtable.hit" },
  { MEMTABLE_MISS, "rocksdb.memtable.miss" },
  { COMPACTION_KEY_DROP_NEWER_ENTRY, "rocksdb.compaction.key.drop.new" },
  { COMPACTION_KEY_DROP_OBSOLETE, "rocksdb.compaction.key.drop.obsolete" },
  { COMPACTION_KEY_DROP_USER, "rocksdb.compaction.key.drop.user" },
  { NUMBER_KEYS_WRITTEN, "rocksdb.number.keys.written" },
  { NUMBER_KEYS_READ, "rocksdb.number.keys.read" },
  { NUMBER_KEYS_UPDATED, "rocksdb.number.keys.updated" },
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
  { NUMBER_FILTERED_DELETES, "rocksdb.number.deletes.filtered" },
  { NUMBER_MERGE_FAILURES, "rocksdb.number.merge.failures" },
  { SEQUENCE_NUMBER, "rocksdb.sequence.number" },
  { BLOOM_FILTER_PREFIX_CHECKED, "rocksdb.bloom.filter.prefix.checked" },
  { BLOOM_FILTER_PREFIX_USEFUL, "rocksdb.bloom.filter.prefix.useful" },
  { NUMBER_OF_RESEEKS_IN_ITERATION, "rocksdb.number.reseeks.iteration" },
  { GET_UPDATES_SINCE_CALLS, "rocksdb.getupdatessince.calls" },
  { BLOCK_CACHE_COMPRESSED_MISS, "rocksdb.block.cachecompressed.miss" },
  { BLOCK_CACHE_COMPRESSED_HIT, "rocksdb.block.cachecompressed.hit" },
  { WAL_FILE_SYNCED, "rocksdb.wal.synced" },
  { WAL_FILE_BYTES, "rocksdb.wal.bytes" },
  { WRITE_DONE_BY_SELF, "rocksdb.write.self" },
  { WRITE_DONE_BY_OTHER, "rocksdb.write.other" },
  { WRITE_WITH_WAL, "rocksdb.write.wal" },
  { COMPACT_READ_BYTES, "rocksdb.compact.read.bytes" },
  { COMPACT_WRITE_BYTES, "rocksdb.compact.write.bytes" },
};

/**
 * Keep adding histogram's here.
 * Any histogram whould have value less than HISTOGRAM_ENUM_MAX
 * Add a new Histogram by assigning it the current value of HISTOGRAM_ENUM_MAX
 * Add a string representation in HistogramsNameMap below
 * And increment HISTOGRAM_ENUM_MAX
 */
enum Histograms {
  DB_GET,
  DB_WRITE,
  COMPACTION_TIME,
  TABLE_SYNC_MICROS,
  COMPACTION_OUTFILE_SYNC_MICROS,
  WAL_FILE_SYNC_MICROS,
  MANIFEST_FILE_SYNC_MICROS,
  // TIME SPENT IN IO DURING TABLE OPEN
  TABLE_OPEN_IO_MICROS,
  DB_MULTIGET,
  READ_BLOCK_COMPACTION_MICROS,
  READ_BLOCK_GET_MICROS,
  WRITE_RAW_BLOCK_MICROS,

  STALL_L0_SLOWDOWN_COUNT,
  STALL_MEMTABLE_COMPACTION_COUNT,
  STALL_L0_NUM_FILES_COUNT,
  HARD_RATE_LIMIT_DELAY_COUNT,
  SOFT_RATE_LIMIT_DELAY_COUNT,
  NUM_FILES_IN_SINGLE_COMPACTION,
  HISTOGRAM_ENUM_MAX,
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
  { NUM_FILES_IN_SINGLE_COMPACTION, "rocksdb.numfiles.in.singlecompaction" },
};

struct HistogramData {
  double median;
  double percentile95;
  double percentile99;
  double average;
  double standard_deviation;
};

// Analyze the performance of a db
class Statistics {
 public:
  virtual ~Statistics() {}

  virtual long getTickerCount(Tickers tickerType) = 0;
  virtual void recordTick(Tickers tickerType, uint64_t count = 0) = 0;
  virtual void setTickerCount(Tickers tickerType, uint64_t count) = 0;
  virtual void measureTime(Histograms histogramType, uint64_t time) = 0;

  virtual void histogramData(Histograms type, HistogramData* const data) = 0;
  // String representation of the statistic object.
  std::string ToString();
};

// Create a concrete DBStatistics object
std::shared_ptr<Statistics> CreateDBStatistics();

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_STATISTICS_H_
