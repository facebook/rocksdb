// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include "leveldb/cache.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

using std::unique_ptr;

namespace leveldb {

struct BlockContents;
class Comparator;
class BlockMetrics;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

  // Creates an iterator on the block that knows which file and block it
  // belongs to.
  Iterator* NewIterator(const Comparator* comparator,
                        uint64_t file_number,
                        uint64_t block_offset);

  // Creates a new iterator that keeps track of accesses. When this iterator is
  // deleted it frees the cache handle and passes the metrics to the cache
  // specified.
  // REQUIRES: cache, cache_handle, metrics_handler must be non-NULL
  Iterator* NewMetricsIterator(const Comparator* comparator,
                               uint64_t file_number,
                               uint64_t block_offset,
                               Cache* cache,
                               Cache::Handle* cache_handle,
                               void* metrics_handler);

  // Returns true if iter is a Block iterator and also knows that which file
  // and block it belongs to.
  static bool GetBlockIterInfo(const Iterator* iter,
                               uint64_t& file_number,
                               uint64_t& block_offset,
                               uint32_t& restart_index,
                               uint32_t& restart_offset);

 private:
  uint32_t NumRestarts() const;

  const char* data_;
  size_t size_;
  uint32_t restart_offset_;     // Offset in data_ of restart array
  bool owned_;                  // Block owns data_[]

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);

  class Iter;
  class MetricsIter;
};

// This class records metrics for a given block. This object however gets
// attached to an iterator of a block and not the block itself as Block objects
// are immutable and code relies on this property wrt thread safety.
//
// We don't solve the problem of thread safety with a lock as we wish to avoid
// introducing extra mutexes (and their associated overhead) and since
// iterators have to be protected by an external lock already if they want to
// be used by multiple threads.
//
// Currently, BlockMetrics are recorded in Block::MetricsIter. Table receives a
// pointer to the BlockMetrics instance. That BlockMetrics instance is passed
// to the Cache instance by adding a cleanup function that passes it on. The
// Cache passes on the metrics (in batches) to the DBImpl instance that
// registered itself with the Cache for receiving metrics.
//
// DBImpl indicates to Table that it wishes to have metrics by setting
// ReadOptions.metrics_handler to itself as opposed to leaving it NULL.
class BlockMetrics {
 public:
  BlockMetrics(uint64_t file_number, uint64_t block_offset,
               uint32_t num_restarts, uint32_t bytes_per_restart);

  // Clears and puts the DB key for the file_number-block_offset-pair in
  // *db_key.
  static void CreateDBKey(uint64_t file_number, uint64_t block_offset,
                          std::string* db_key);

  // Creates a BlockMetrics object from the DB key and value. Returns NULL if
  // either/both are invalid.
  static BlockMetrics* Create(const std::string& db_key,
                              const std::string& db_value);

  // Creates a BlockMetrics object from the DB value. Returns NULL if
  // the DB value is invalid.
  static BlockMetrics* Create(uint64_t file_number, uint64_t block_offset,
                              const std::string& db_value);

  // Record that there was an access to an element in the restart block with
  // index "restart_index", with offset "restart_offset" from the beginning of
  // the restart block.
  void RecordAccess(uint32_t restart_index, uint32_t restart_offset);

  // Returns true if the record is considered hot.
  bool IsHot(uint32_t restart_index, uint32_t restart_offset) const;

  // Returns the key to use when storing in the metrics database.
  std::string GetDBKey() const;
  // Returns the value to use when storing in the metrics database.
  std::string GetDBValue() const;

  // Returns true if bm represents metrics for the same block.
  bool IsCompatible(const BlockMetrics* bm) const;

  // Returns true if the given file_number and block_offset match this block's.
  bool IsSameBlock(uint64_t file_number, uint64_t block_offset) const;

  // Joins the metrics from the other metrics into this one.
  // REQUIRES: this->IsCompatible(bm);
  void Join(const BlockMetrics* bm);

 private:
  BlockMetrics(uint64_t file_number, uint64_t block_offset,
               uint32_t num_restarts, uint32_t bytes_per_restart,
               const std::string& data);

  uint64_t file_number_;
  uint64_t block_offset_;
  uint32_t num_restarts_;
  uint32_t bytes_per_restart_;

  // We use a fixed size metrics_ array as dynamic allocations more than double
  // the time taking to create a BlockMetrics object.
  static const size_t kBlockMetricsSize = 32;
  unsigned char metrics_[kBlockMetricsSize];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
