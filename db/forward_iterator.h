//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include <vector>
#include <queue>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "db/dbformat.h"
#include "util/arena.h"

namespace rocksdb {

class DBImpl;
class Env;
struct SuperVersion;
class ColumnFamilyData;
class LevelIterator;
struct FileMetaData;

class MinIterComparator {
 public:
  explicit MinIterComparator(const Comparator* comparator) :
    comparator_(comparator) {}

  bool operator()(Iterator* a, Iterator* b) {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }
 private:
  const Comparator* comparator_;
};

typedef std::priority_queue<Iterator*,
          std::vector<Iterator*>,
          MinIterComparator> MinIterHeap;

/**
 * ForwardIterator is a special type of iterator that only supports Seek()
 * and Next(). It is expected to perform better than TailingIterator by
 * removing the encapsulation and making all information accessible within
 * the iterator. At the current implementation, snapshot is taken at the
 * time Seek() is called. The Next() followed do not see new values after.
 */
class ForwardIterator : public Iterator {
 public:
  ForwardIterator(DBImpl* db, const ReadOptions& read_options,
                  ColumnFamilyData* cfd, SuperVersion* current_sv = nullptr);
  virtual ~ForwardIterator();

  void SeekToLast() override {
    status_ = Status::NotSupported("ForwardIterator::SeekToLast()");
    valid_ = false;
  }
  void Prev() override {
    status_ = Status::NotSupported("ForwardIterator::Prev");
    valid_ = false;
  }

  virtual bool Valid() const override;
  void SeekToFirst() override;
  virtual void Seek(const Slice& target) override;
  virtual void Next() override;
  virtual Slice key() const override;
  virtual Slice value() const override;
  virtual Status status() const override;

 private:
  void Cleanup(bool release_sv);
  void RebuildIterators(bool refresh_sv);
  void ResetIncompleteIterators();
  void SeekInternal(const Slice& internal_key, bool seek_to_first);
  void UpdateCurrent();
  bool NeedToSeekImmutable(const Slice& internal_key);
  uint32_t FindFileInRange(
    const std::vector<FileMetaData*>& files, const Slice& internal_key,
    uint32_t left, uint32_t right);

  DBImpl* const db_;
  const ReadOptions read_options_;
  ColumnFamilyData* const cfd_;
  const SliceTransform* const prefix_extractor_;
  const Comparator* user_comparator_;
  MinIterHeap immutable_min_heap_;

  SuperVersion* sv_;
  Iterator* mutable_iter_;
  std::vector<Iterator*> imm_iters_;
  std::vector<Iterator*> l0_iters_;
  std::vector<LevelIterator*> level_iters_;
  Iterator* current_;
  // internal iterator status
  Status status_;
  Status immutable_status_;
  bool valid_;

  IterKey prev_key_;
  bool is_prev_set_;
  bool is_prev_inclusive_;
  Arena arena_;
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
