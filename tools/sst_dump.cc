//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "rocksdb/sst_dump_tool.h"

int main(int argc, char** argv) {
  rocksdb::SSTDumpTool tool;
  tool.Run(argc, argv);
  return 0;
}
