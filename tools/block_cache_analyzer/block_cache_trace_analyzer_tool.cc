//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE
#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else  // GFLAGS
#include "tools/block_cache_analyzer/block_cache_trace_analyzer.h"
int main(int argc, char** argv) {
  return ROCKSDB_NAMESPACE::block_cache_trace_analyzer_tool(argc, argv);
}
#endif  // GFLAGS
#else   // ROCKSDB_LITE
#include <stdio.h>
int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "Not supported in lite mode.\n");
  return 1;
}
#endif  // ROCKSDB_LITE
