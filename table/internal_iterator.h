// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <string>
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/status.h"

namespace rocksdb {

class PinnedIteratorsManager;

class InternalIterator : public Cleanable {
 public:
  InternalIterator() {}
  virtual ~InternalIterator() {}

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that at or past target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) = 0;

  // Position at the first key in the source that at or before target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or before target.
  virtual void SeekForPrev(const Slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const = 0;

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: !AtEnd() && !AtStart()
  virtual Slice value() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  // If non-blocking IO is requested and this operation cannot be
  // satisfied without doing some IO, then this returns Status::Incomplete().
  virtual Status status() const = 0;

  // Pass the PinnedIteratorsManager to the Iterator, most Iterators dont
  // communicate with PinnedIteratorsManager so default implementation is no-op
  // but for Iterators that need to communicate with PinnedIteratorsManager
  // they will implement this function and use the passed pointer to communicate
  // with PinnedIteratorsManager.
  virtual void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {}

  // If true, this means that the Slice returned by key() is valid as long as
  // PinnedIteratorsManager::ReleasePinnedData is not called and the
  // Iterator is not deleted.
  //
  // IsKeyPinned() is guaranteed to always return true if
  //  - Iterator is created with ReadOptions::pin_data = true
  //  - DB tables were created with BlockBasedTableOptions::use_delta_encoding
  //    set to false.
  virtual bool IsKeyPinned() const { return false; }

  // If true, this means that the Slice returned by value() is valid as long as
  // PinnedIteratorsManager::ReleasePinnedData is not called and the
  // Iterator is not deleted.
  virtual bool IsValuePinned() const { return false; }

  virtual Status GetProperty(std::string prop_name, std::string* prop) {
    return Status::NotSupported("");
  }

 protected:
  void SeekForPrevImpl(const Slice& target, const Comparator* cmp) {
    Seek(target);
    if (!Valid()) {
      SeekToLast();
    }
    while (Valid() && cmp->Compare(target, key()) < 0) {
      Prev();
    }
  }

 private:
  // No copying allowed
  InternalIterator(const InternalIterator&) = delete;
  InternalIterator& operator=(const InternalIterator&) = delete;
};

// Return an empty iterator (yields nothing).
extern InternalIterator* NewEmptyInternalIterator();

// Return an empty iterator with the specified status.
extern InternalIterator* NewErrorInternalIterator(const Status& status);

}  // namespace rocksdb
