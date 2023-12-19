//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/multi_cf_iterator.h"

namespace ROCKSDB_NAMESPACE {

void MultiCfIterator::SeekToFirst() {
  Reset();
  int i = 0;
  for (auto& iter : iterators_) {
    auto& cfh = cfhs_[i];
    iter->SeekToFirst();
    if (iter->Valid()) {
      assert(iter->status().ok());
      min_heap_.push(
          MultiCfIteratorInfo{.iterator = iter, .cfh = cfh, .order = i});
    } else {
      considerStatus(iter->status());
    }
    ++i;
  }
}

void MultiCfIterator::Next() {
  assert(Valid());

  auto* current = min_heap_.top().iterator;
  std::string current_key_copy =
      std::string(current->key().data(), current->key().size());
  while (!min_heap_.empty() &&
         comparator_->Compare(current->key(), current_key_copy) == 0) {
    current->Next();
    if (current->Valid()) {
      assert(current->status().ok());
      min_heap_.replace_top(min_heap_.top());
    } else {
      considerStatus(current->status());
      min_heap_.pop();
    }
    if (!min_heap_.empty()) {
      current = min_heap_.top().iterator;
    }
  }
}

class EmptyMultiCfIterator : public MultiCfIterator {
 public:
  explicit EmptyMultiCfIterator(const Status& s) : status_(s) {}
  bool Valid() const override { return false; }
  void Seek(const Slice& /*target*/) override {}
  void SeekForPrev(const Slice& /*target*/) override {}
  void SeekToFirst() override {}
  void SeekToLast() override {}
  void Next() override { assert(false); }
  void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  Slice value() const override {
    assert(false);
    return Slice();
  }
  Status status() const override { return status_; }

 private:
  Status status_;
};

MultiCfIterator* NewEmptyMultiColumnFamilyIterator() {
  return new EmptyMultiCfIterator(Status::OK());
}

MultiCfIterator* NewErrorMultiColumnFamilyIterator(const Status& status) {
  return new EmptyMultiCfIterator(status);
}

MultiCfIterator* NewMultiColumnFamilyIterator(
    const Comparator* comparator,
    const std::vector<ColumnFamilyHandle*>& column_families,
    const std::vector<Iterator*>& child_iterators) {
  MultiCfIterator* iterator =
      new MultiCfIterator(comparator, column_families, child_iterators);
  return iterator;
}

}  // namespace ROCKSDB_NAMESPACE
