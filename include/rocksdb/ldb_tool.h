// Copyright 2008-present Facebook. All Rights Reserved.
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
