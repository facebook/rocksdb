// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "rocksdb/ldb_tool.h"

int main(int argc, char** argv) {
  leveldb::LDBTool tool;
  tool.Run(argc, argv);
  return 0;
}
