//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/iterator.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {
// An iterator wrapper class used to wrap an `InternalIterator` created by API
// `TableReader::NewIterator`. The `InternalIterator` should be allocated with
// the default allocator, not on an arena.
// NOTE: Callers should ensure the wrapped `InternalIterator*` is a valid
// pointer before constructing a `TableIterator` with it.
class TableIterator : public Iterator {
  void reset(InternalIterator* iter) noexcept {
    if (iter_ != nullptr) {
      delete iter_;
    }
    iter_ = iter;
  }

 public:
  explicit TableIterator(InternalIterator* iter) : iter_(iter) {}

  TableIterator(const TableIterator&) = delete;
  TableIterator& operator=(const TableIterator&) = delete;

  TableIterator(TableIterator&& o) noexcept {
    iter_ = o.iter_;
    o.iter_ = nullptr;
  }

  TableIterator& operator=(TableIterator&& o) noexcept {
    reset(o.iter_);
    o.iter_ = nullptr;
    return *this;
  }

  InternalIterator* operator->() { return iter_; }
  InternalIterator* get() { return iter_; }

  ~TableIterator() override { reset(nullptr); }

  bool Valid() const override { return iter_->Valid(); }
  void SeekToFirst() override { return iter_->SeekToFirst(); }
  void SeekToLast() override { return iter_->SeekToLast(); }
  void Seek(const Slice& target) override { return iter_->Seek(target); }
  void SeekForPrev(const Slice& target) override {
    return iter_->SeekForPrev(target);
  }
  void Next() override { return iter_->Next(); }
  void Prev() override { return iter_->Prev(); }
  Slice key() const override { return iter_->key(); }
  Slice value() const override { return iter_->value(); }
  Status status() const override { return iter_->status(); }
  Status GetProperty(std::string /*prop_name*/,
                     std::string* /*prop*/) override {
    assert(false);
    return Status::NotSupported("TableIterator does not support GetProperty.");
  }

 private:
  InternalIterator* iter_;
};
}  // namespace ROCKSDB_NAMESPACE
