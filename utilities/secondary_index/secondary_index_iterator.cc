//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/secondary_index.h"
#include "utilities/secondary_index/secondary_index_helper.h"

namespace ROCKSDB_NAMESPACE {

SecondaryIndexIterator::SecondaryIndexIterator(
    const SecondaryIndex* index, std::unique_ptr<Iterator>&& underlying_it)
    : index_(index), underlying_it_(std::move(underlying_it)) {
  assert(index_);
  assert(underlying_it_);
}

bool SecondaryIndexIterator::Valid() const {
  return status_.ok() && underlying_it_->Valid() &&
         underlying_it_->key().starts_with(prefix_);
}

Status SecondaryIndexIterator::status() const {
  if (!status_.ok()) {
    return status_;
  }

  return underlying_it_->status();
}

void SecondaryIndexIterator::Seek(const Slice& target) {
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

void SecondaryIndexIterator::Next() {
  assert(Valid());

  underlying_it_->Next();
}

void SecondaryIndexIterator::Prev() {
  assert(Valid());

  underlying_it_->Prev();
}

bool SecondaryIndexIterator::PrepareValue() {
  assert(Valid());

  return underlying_it_->PrepareValue();
}

Slice SecondaryIndexIterator::key() const {
  assert(Valid());

  Slice key = underlying_it_->key();
  key.remove_prefix(prefix_.size());

  return key;
}

Slice SecondaryIndexIterator::value() const {
  assert(Valid());

  return underlying_it_->value();
}

const WideColumns& SecondaryIndexIterator::columns() const {
  assert(Valid());

  return underlying_it_->columns();
}

Slice SecondaryIndexIterator::timestamp() const {
  assert(Valid());

  return underlying_it_->timestamp();
}

Status SecondaryIndexIterator::GetProperty(std::string prop_name,
                                           std::string* prop) const {
  return underlying_it_->GetProperty(std::move(prop_name), prop);
}

}  // namespace ROCKSDB_NAMESPACE
