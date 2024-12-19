// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class RemoveEmptyValueCompactionFilter : public CompactionFilter {
 public:
  static const char* kClassName() { return "RemoveEmptyValueCompactionFilter"; }

  const char* Name() const override { return kClassName(); }

  bool Filter(int level, const Slice& key, const Slice& existing_value,
              std::string* new_value, bool* value_changed) const override;
};

}  // namespace ROCKSDB_NAMESPACE
