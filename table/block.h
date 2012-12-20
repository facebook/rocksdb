// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace leveldb {

struct BlockContents;
class Comparator;
class BlockMetrics;
class BlockMetricsHandler;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

  // Creates a new iterator that keeps track of accesses.
  //
  // Creates a BlockMetrics object on the heap and sets metrics to it.
  // The caller is responsible for freeing this object.
  // REQUIRES: metrics must be non-NULL
  Iterator* NewMetricsIterator(const Comparator* comparator,
                               uint64_t file_number,
                               uint64_t block_offset,
                               BlockMetrics** metrics);

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

class DB;
class WriteBatch;
class ReadOptions;
class BlockMetrics {
 public:
  BlockMetrics(uint64_t file_number, uint64_t block_offset,
               uint32_t num_restarts, uint32_t bytes_per_restart);
  ~BlockMetrics();

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

  // Joins the metrics from the other metrics into this one.
  // REQUIRES: this->IsCompatible(bm);
  void Join(const BlockMetrics* bm);

 private:
  friend class Block;

  BlockMetrics(uint64_t file_number, uint64_t block_offset,
               uint32_t num_restarts, uint32_t bytes_per_restart,
               const std::string& data);

  uint64_t file_number_;
  uint64_t block_offset_;
  uint32_t num_restarts_;
  uint32_t bytes_per_restart_;
  char* metrics_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
