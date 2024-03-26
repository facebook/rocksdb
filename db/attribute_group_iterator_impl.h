//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <variant>

#include "db/multi_cf_iterator_impl.h"
#include "rocksdb/attribute_groups.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "util/heap.h"
#include "util/overload.h"

namespace ROCKSDB_NAMESPACE {

class AttributeGroupIteratorImpl : public AttributeGroupIterator {
 public:
  AttributeGroupIteratorImpl(
      const Comparator* comparator,
      const std::vector<ColumnFamilyHandle*>& column_families,
      const std::vector<Iterator*>& child_iterators)
      : impl_(comparator, column_families, child_iterators) {}
  ~AttributeGroupIteratorImpl() override {}

  // No copy allowed
  AttributeGroupIteratorImpl(const AttributeGroupIteratorImpl&) = delete;
  AttributeGroupIteratorImpl& operator=(const AttributeGroupIteratorImpl&) =
      delete;

  bool Valid() const override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void Next() override;
  void Prev() override;
  Slice key() const override;
  Status status() const override;

  AttributeGroups attribute_groups() const override {
    // TODO - Implement
    assert(false);
    return kNoAttributeGroups;
  }

 private:
  MultiCfIteratorImpl impl_;
};

class EmptyAttributeGroupIterator : public AttributeGroupIterator {
 public:
  explicit EmptyAttributeGroupIterator(const Status& s) : status_(s) {}
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
  Status status() const override { return status_; }

  AttributeGroups attribute_groups() const override {
    return kNoAttributeGroups;
  }

 private:
  Status status_;
};

inline std::unique_ptr<AttributeGroupIterator> NewAttributeGroupErrorIterator(
    const Status& status) {
  return std::make_unique<EmptyAttributeGroupIterator>(status);
}

}  // namespace ROCKSDB_NAMESPACE
