// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#ifndef ROCKSDB_LITE
#pragma once
#include <string>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace rocksdb {

// An interface for converting a slice to a readable string
class SliceFormatter {
 public:
  virtual ~SliceFormatter() {}
  virtual std::string Format(const Slice& s) const = 0;
};

// Options for customizing ldb tool (beyond the DB Options)
struct LDBOptions {
  // Create LDBOptions with default values for all fields
  LDBOptions();

  // Key formatter that converts a slice to a readable string.
  // Default: Slice::ToString()
  std::shared_ptr<SliceFormatter> key_formatter;
};

class LDBTool {
 public:
  void Run(
      int argc, char** argv, Options db_options = Options(),
      const LDBOptions& ldb_options = LDBOptions(),
      const std::vector<ColumnFamilyDescriptor>* column_families = nullptr);
};

} // namespace rocksdb

#endif  // ROCKSDB_LITE
