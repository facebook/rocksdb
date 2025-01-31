//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <string>

#include "rocksdb/iterator.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/secondary_index.h"
#include "utilities/secondary_index/secondary_index_helper.h"

namespace ROCKSDB_NAMESPACE {

// A simple iterator that can be used to query a secondary index (that is, find
// the primary keys for a given search target). Can be used as-is or as a
// building block for more complex iterators.
class SecondaryIndexIterator : public Iterator {
 public:
  SecondaryIndexIterator(const SecondaryIndex* index,
                         std::unique_ptr<Iterator>&& underlying_it)
      : index_(index), underlying_it_(std::move(underlying_it)) {
    assert(index_);
    assert(underlying_it_);
  }

  bool Valid() const override {
    return status_.ok() && underlying_it_->Valid() &&
           underlying_it_->key().starts_with(prefix_);
  }

  void SeekToFirst() override {
    status_ = Status::NotSupported(
        "SeekToFirst is not supported for secondary index iterators");
  }

  void SeekToLast() override {
    status_ = Status::NotSupported(
        "SeekToLast is not supported for secondary index iterators");
  }

  void Seek(const Slice& target) override {
    status_ = Status::OK();

    std::variant<Slice, std::string> prefix = target;

    const Status s = index_->FinalizeSecondaryKeyPrefix(&prefix);
    if (!s.ok()) {
      status_ = s;
      return;
    }

    prefix_ = SecondaryIndexHelper::AsString(prefix);

    // FIXME: this works for BytewiseComparator but not for all comparators in
    // general
    underlying_it_->Seek(prefix_);
  }

  void SeekForPrev(const Slice& /* target */) override {
    status_ = Status::NotSupported(
        "SeekForPrev is not supported for secondary index iterators");
  }

  void Next() override {
    assert(Valid());

    underlying_it_->Next();
  }

  void Prev() override {
    assert(Valid());

    underlying_it_->Prev();
  }

  bool PrepareValue() override {
    assert(Valid());

    return underlying_it_->PrepareValue();
  }

  Status status() const override {
    if (!status_.ok()) {
      return status_;
    }

    return underlying_it_->status();
  }

  Slice key() const override {
    assert(Valid());

    Slice key = underlying_it_->key();
    key.remove_prefix(prefix_.size());

    return key;
  }

  Slice value() const override {
    assert(Valid());

    return underlying_it_->value();
  }

  const WideColumns& columns() const override {
    assert(Valid());

    return underlying_it_->columns();
  }

  Slice timestamp() const override {
    assert(Valid());

    return underlying_it_->timestamp();
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return underlying_it_->GetProperty(std::move(prop_name), prop);
  }

 private:
  const SecondaryIndex* index_;
  std::unique_ptr<Iterator> underlying_it_;
  Status status_;
  std::string prefix_;
};

}  // namespace ROCKSDB_NAMESPACE
