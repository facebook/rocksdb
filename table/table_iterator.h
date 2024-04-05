//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/iterator.h"
#include "table/internal_iterator.h"
#include "rocksdb/utilities/types_util.h"

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
  explicit TableIterator(InternalIterator* iter, Options* options) : iter_(iter), options_(options) {}

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
  void Seek(const Slice& target) override {
      std::string seek_key_buf;
      ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::GetInternalKeyForSeek(target, options_->comparator,
                                                                             &seek_key_buf);
      assert(s.ok());
      if (!s.ok()) {
        status_ = s;
      }
      iter_->Seek(seek_key_buf);
  }
  void SeekForPrev(const Slice& target) override {
      std::string seek_key_buf;
      ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::GetInternalKeyForSeekForPrev(target, options_->comparator,
                                                                                    &seek_key_buf);
      if(s.ok()) {
          status_ = s;
      }
      iter_->SeekForPrev(target);
  }

  void Next() override {
      iter_->Next();
      init_key();
  }

  void Prev() override {
      iter_->Prev();
      init_key();
  }

  Slice key() const override { return ikey->user_key;}
  Slice value() const override { return iter_->value(); }
  Status status() const override {
      if (!status_.ok()) {
          return status_;
      }
      return iter_->status();
  }
  Status GetProperty(std::string /*prop_name*/,
                     std::string* /*prop*/) override {
    assert(false);
    return Status::NotSupported("TableIterator does not support GetProperty.");
  }

  uint64_t SequenceNumber() {
    return ikey->sequence;
  }

  ValueType type() {
      return ikey->type;
  }

 private:
  InternalIterator* iter_;
  Options* options_;
  ParsedInternalKey* ikey;
  Status status_;
  void init_key() {
      ParseInternalKey(iter_->key(), ikey, true /* log_err_key */);
  }
};
}  // namespace ROCKSDB_NAMESPACE
