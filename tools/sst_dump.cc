//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/sst_dump_tool.h"

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::SSTDumpTool tool;
  return tool.Run(argc, argv);
}
