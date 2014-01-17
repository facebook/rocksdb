//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"

namespace rocksdb {

class DBImpl;

/**
 * TailingIterator is a special type of iterator that doesn't use an (implicit)
 * snapshot. In other words, it can be used to read data that was added to the
 * db after the iterator had been created.
 *
 * TailingIterator is optimized for sequential reading. It doesn't support
 * Prev() and SeekToLast() operations.
 */
class TailingIterator : public Iterator {
 public:
  TailingIterator(DBImpl* db, const ReadOptions& options,
                  const Comparator* comparator);
  virtual ~TailingIterator() {}

  virtual bool Valid() const override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Seek(const Slice& target) override;
  virtual void Next() override;
  virtual void Prev() override;
  virtual Slice key() const override;
  virtual Slice value() const override;
  virtual Status status() const override;

 private:
  DBImpl* const db_;
  const ReadOptions options_;
  const Comparator* const comparator_;
  uint64_t version_number_;

  // TailingIterator merges the contents of the two iterators below (one using
  // mutable memtable contents only, other over SSTs and immutable memtables).
  // See DBIter::GetTailingIteratorPair().
  std::unique_ptr<Iterator> mutable_;
  std::unique_ptr<Iterator> immutable_;

  // points to either mutable_ or immutable_
  Iterator* current_;

  // key that precedes immutable iterator's current key
  std::string prev_key_;

  // unless prev_set is true, prev_key/prev_head is not valid and shouldn't be
  // used; reset by createIterators()
  bool is_prev_set_;

  // prev_key_ was set by SeekImmutable(), which means that the interval of
  // keys covered by immutable_ is [prev_key_, current], i.e. it includes the
  // left endpoint
  bool is_prev_inclusive_;

  // internal iterator status
  Status status_;

  // check if this iterator's version matches DB's version
  bool IsCurrentVersion() const;

  // check if SeekImmutable() is needed due to target having a different prefix
  // than prev_key_ (used when options.prefix_seek is set)
  bool IsSamePrefix(const Slice& target) const;

  // creates mutable_ and immutable_ iterators and updates version_number_
  void CreateIterators();

  // set current_ to be one of the iterators with the smallest key
  void UpdateCurrent();

  // seek on immutable_ and update prev_key
  void SeekImmutable(const Slice& target);
};

}  // namespace rocksdb
