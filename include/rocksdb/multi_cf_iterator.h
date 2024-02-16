//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class Iterator;
class ColumnFamilyHandle;
struct ReadOptions;

// UNDER CONSTRUCTION - DO NOT USE
// Return a cross-column-family iterator from a consistent database state.
// When the same key is present in multiple column families, the iterator
// selects the value or columns from the first column family containing the
// key, in the order specified by the `column_families` parameter.
class MultiCfIterator : public Iterator {
 public:
  MultiCfIterator() {}
  ~MultiCfIterator() override {}

  // No copy allowed
  MultiCfIterator(const MultiCfIterator&) = delete;
  MultiCfIterator& operator=(const MultiCfIterator&) = delete;

  virtual const AttributeGroups& attribute_groups() const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
