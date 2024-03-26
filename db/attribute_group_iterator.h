//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <variant>

#include "db/multi_cf_iterator_impl.h"
#include "multi_cf_iterator_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "util/heap.h"
#include "util/overload.h"

namespace ROCKSDB_NAMESPACE {

// UNDER CONSTRUCTION - DO NOT USE
// A cross-column-family iterator that collects and returns attribute groups for
// each key in order provided by comparator
class AttributeGroupIterator : public IteratorBase {
 public:
  AttributeGroupIterator(
      const Comparator* comparator,
      const std::vector<ColumnFamilyHandle*>& column_families,
      const std::vector<Iterator*>& child_iterators)
      : impl_(comparator, column_families, child_iterators) {}
  ~AttributeGroupIterator() override {}

  // No copy allowed
  AttributeGroupIterator(const AttributeGroupIterator&) = delete;
  AttributeGroupIterator& operator=(const AttributeGroupIterator&) = delete;

  bool Valid() const override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void Next() override;
  void Prev() override;
  Slice key() const override;
  Status status() const override;

  AttributeGroups attribute_groups() const {
    // TODO - Implement
    assert(false);
    return kNoAttributeGroups;
  }

 private:
  MultiCfIteratorImpl impl_;
};

}  // namespace ROCKSDB_NAMESPACE
