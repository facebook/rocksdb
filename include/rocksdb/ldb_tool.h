// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#ifndef STORAGE_ROCKSDB_INCLUDE_LDB_TOOL_H
#define STORAGE_ROCKSDB_INCLUDE_LDB_TOOL_H
#include "rocksdb/options.h"

namespace rocksdb {

class LDBTool {
 public:
  void Run(int argc, char** argv, Options = Options());
};

} // namespace rocksdb

#endif // STORAGE_ROCKSDB_INCLUDE_LDB_TOOL_H
