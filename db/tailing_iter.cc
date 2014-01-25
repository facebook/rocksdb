//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/tailing_iter.h"

#include <string>
#include <utility>
#include "db/db_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

namespace rocksdb {

TailingIterator::TailingIterator(DBImpl* db, const ReadOptions& options,
                                 const Comparator* comparator)
    : db_(db), options_(options), comparator_(comparator),
      version_number_(0), current_(nullptr),
      status_(Status::InvalidArgument("Seek() not called on this iterator")) {}

bool TailingIterator::Valid() const {
  return current_ != nullptr;
}

void TailingIterator::SeekToFirst() {
  if (!IsCurrentVersion()) {
    CreateIterators();
  }

  mutable_->SeekToFirst();
  immutable_->SeekToFirst();
  UpdateCurrent();
}

void TailingIterator::Seek(const Slice& target) {
  if (!IsCurrentVersion()) {
    CreateIterators();
  }

  mutable_->Seek(target);

  // We maintain the interval (prev_key_, immutable_->key()] such that there
  // are no records with keys within that range in immutable_ other than
  // immutable_->key(). Since immutable_ can't change in this version, we don't
  // need to do a seek if 'target' belongs to that interval (i.e. immutable_ is
  // already at the correct position)!
  //
  // If options.prefix_seek is used and immutable_ is not valid, seek if target
  // has a different prefix than prev_key.
  //
  // prev_key_ is updated by Next(). SeekImmutable() sets prev_key_ to
  // 'target' -- in this case, prev_key_ is included in the interval, so
  // prev_inclusive_ has to be set.

  if (!is_prev_set_ ||
      comparator_->Compare(prev_key_, target) >= !is_prev_inclusive_ ||
      (immutable_->Valid() &&
       comparator_->Compare(target, immutable_->key()) > 0) ||
      (options_.prefix_seek && !IsSamePrefix(target))) {
    SeekImmutable(target);
  }

  UpdateCurrent();
}

void TailingIterator::Next() {
  assert(Valid());

  if (!IsCurrentVersion()) {
    // save the current key, create new iterators and then seek
    std::string current_key = key().ToString();
    Slice key_slice(current_key.data(), current_key.size());

    CreateIterators();
    Seek(key_slice);

    if (!Valid() || key().compare(key_slice) != 0) {
      // record with current_key no longer exists
      return;
    }

  } else if (current_ == immutable_.get()) {
    // immutable iterator is advanced -- update prev_key_
    prev_key_ = key().ToString();
    is_prev_inclusive_ = false;
    is_prev_set_ = true;
  }

  current_->Next();
  UpdateCurrent();
}

Slice TailingIterator::key() const {
  assert(Valid());
  return current_->key();
}

Slice TailingIterator::value() const {
  assert(Valid());
  return current_->value();
}

Status TailingIterator::status() const {
  if (!status_.ok()) {
    return status_;
  } else if (!mutable_->status().ok()) {
    return mutable_->status();
  } else {
    return immutable_->status();
  }
}

void TailingIterator::Prev() {
  status_ = Status::NotSupported("This iterator doesn't support Prev()");
}

void TailingIterator::SeekToLast() {
  status_ = Status::NotSupported("This iterator doesn't support SeekToLast()");
}

void TailingIterator::CreateIterators() {
  std::pair<Iterator*, Iterator*> iters =
    db_->GetTailingIteratorPair(options_, &version_number_);

  assert(iters.first && iters.second);

  mutable_.reset(iters.first);
  immutable_.reset(iters.second);
  current_ = nullptr;
  is_prev_set_ = false;
}

void TailingIterator::UpdateCurrent() {
  current_ = nullptr;

  if (mutable_->Valid()) {
    current_ = mutable_.get();
  }
  if (immutable_->Valid() &&
      (current_ == nullptr ||
       comparator_->Compare(immutable_->key(), current_->key()) < 0)) {
    current_ = immutable_.get();
  }

  if (!status_.ok()) {
    // reset status that was set by Prev() or SeekToLast()
    status_ = Status::OK();
  }
}

bool TailingIterator::IsCurrentVersion() const {
  return mutable_ != nullptr && immutable_ != nullptr &&
    version_number_ == db_->CurrentVersionNumber();
}

bool TailingIterator::IsSamePrefix(const Slice& target) const {
  const SliceTransform* extractor = db_->options_.prefix_extractor;

  assert(extractor);
  assert(is_prev_set_);

  return extractor->Transform(target)
    .compare(extractor->Transform(prev_key_)) == 0;
}

void TailingIterator::SeekImmutable(const Slice& target) {
  prev_key_ = target.ToString();
  is_prev_inclusive_ = true;
  is_prev_set_ = true;

  immutable_->Seek(target);
}

}  // namespace rocksdb
