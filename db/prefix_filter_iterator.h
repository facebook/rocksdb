// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Wrap an underlying iterator, but exclude any results not starting
// with a given prefix.  Seeking to keys not beginning with the prefix
// is invalid, and SeekToLast is not implemented (that would be
// non-trivial), but otherwise this iterator will behave just like the
// underlying iterator would if there happened to be no non-matching
// keys in the dataset.

#ifndef STORAGE_LEVELDB_DB_PREFIX_FILTER_ITERATOR_H_
#define STORAGE_LEVELDB_DB_PREFIX_FILTER_ITERATOR_H_

#include "leveldb/iterator.h"

namespace leveldb {

class PrefixFilterIterator : public Iterator {
 private:
  Iterator* iter_;
  const Slice &prefix_;
  const SliceTransform *prefix_extractor_;
  Status status_;

 public:
  PrefixFilterIterator(Iterator* iter,
                       const Slice &prefix,
                       const SliceTransform* prefix_extractor)
                             : iter_(iter), prefix_(prefix),
                               prefix_extractor_(prefix_extractor),
                               status_(Status::OK()) {
    if (prefix_extractor == nullptr) {
      status_ = Status::InvalidArgument("A prefix filter may not be used "
                                        "unless a function is also defined "
                                        "for extracting prefixes");
    } else if (!prefix_extractor_->InRange(prefix)) {
      status_ = Status::InvalidArgument("Must provide a slice for prefix which"
                                        "is a prefix for some key");
    }
  }
  ~PrefixFilterIterator() {
    delete iter_;
  }
  Slice key() const { return iter_->key(); }
  Slice value() const { return iter_->value(); }
  Status status() const {
    if (!status_.ok()) {
      return status_;
    }
    return iter_->status();
  }
  void Next() { iter_->Next(); }
  void Prev() { iter_->Prev(); }
  void Seek(const Slice& k) {
    if (prefix_extractor_->Transform(k) == prefix_) {
      iter_->Seek(k);
    } else {
      status_ = Status::InvalidArgument("Seek must begin with target prefix");
    }
  }
  void SeekToFirst() {
    Seek(prefix_);
  }
  void SeekToLast() {
    status_ = Status::NotSupported("SeekToLast is incompatible with prefixes");
  }
  bool Valid() const {
    return (status_.ok() && iter_->Valid() &&
            prefix_extractor_->Transform(iter_->key()) == prefix_);
  }
};

}  // namespace leveldb

#endif
