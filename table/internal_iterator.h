// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <string>
#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/status.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

class PinnedIteratorsManager;

struct IterateResult {
  Slice key;
  bool may_be_out_of_upper_bound;
};

template <class TValue>
class InternalIteratorBase : public Cleanable {
 public:
  InternalIteratorBase() {}

  // No copying allowed
  InternalIteratorBase(const InternalIteratorBase&) = delete;
  InternalIteratorBase& operator=(const InternalIteratorBase&) = delete;

  virtual ~InternalIteratorBase() {}

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  // Always returns false if !status().ok().
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
  // All Seek*() methods clear any error status() that the iterator had prior to
  // the call; after the seek, status() indicates only the error (if any) that
  // happened during the seek, not any past errors.
  // 'target' contains user timestamp if timestamp is enabled.
  virtual void Seek(const Slice& target) = 0;

  // Position at the first key in the source that at or before target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or before target.
  virtual void SeekForPrev(const Slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the next entry in the source, and return result. Iterator
  // implementation should override this method to help methods inline better,
  // or when MayBeOutOfUpperBound() is non-trivial.
  // REQUIRES: Valid()
  virtual bool NextAndGetResult(IterateResult* result) {
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      // Default may_be_out_of_upper_bound to true to avoid unnecessary virtual
      // call. If an implementation has non-trivial MayBeOutOfUpperBound(),
      // it should also override NextAndGetResult().
      result->may_be_out_of_upper_bound = true;
      assert(MayBeOutOfUpperBound());
    }
    return is_valid;
  }

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const = 0;

  // Return user key for the current entry.
  // REQUIRES: Valid()
  virtual Slice user_key() const { return ExtractUserKey(key()); }

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual TValue value() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  // If non-blocking IO is requested and this operation cannot be
  // satisfied without doing some IO, then this returns Status::Incomplete().
  virtual Status status() const = 0;

  // True if the iterator is invalidated because it reached a key that is above
  // the iterator upper bound. Used by LevelIterator to decide whether it should
  // stop or move on to the next file.
  // Important: if iterator reached the end of the file without encountering any
  // keys above the upper bound, IsOutOfBound() must return false.
  virtual bool IsOutOfBound() { return false; }

  // Keys return from this iterator can be smaller than iterate_lower_bound.
  virtual bool MayBeOutOfLowerBound() { return true; }

  // Keys return from this iterator can be larger or equal to
  // iterate_upper_bound.
  virtual bool MayBeOutOfUpperBound() { return true; }

  // Pass the PinnedIteratorsManager to the Iterator, most Iterators don't
  // communicate with PinnedIteratorsManager so default implementation is no-op
  // but for Iterators that need to communicate with PinnedIteratorsManager
  // they will implement this function and use the passed pointer to communicate
  // with PinnedIteratorsManager.
  virtual void SetPinnedItersMgr(PinnedIteratorsManager* /*pinned_iters_mgr*/) {
  }

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

  virtual Status GetProperty(std::string /*prop_name*/, std::string* /*prop*/) {
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

  bool is_mutable_;
};

using InternalIterator = InternalIteratorBase<Slice>;

// Return an empty iterator (yields nothing).
template <class TValue = Slice>
extern InternalIteratorBase<TValue>* NewEmptyInternalIterator();

// Return an empty iterator with the specified status.
template <class TValue = Slice>
extern InternalIteratorBase<TValue>* NewErrorInternalIterator(
    const Status& status);

// Return an empty iterator with the specified status, allocated arena.
template <class TValue = Slice>
extern InternalIteratorBase<TValue>* NewErrorInternalIterator(
    const Status& status, Arena* arena);

}  // namespace ROCKSDB_NAMESPACE
