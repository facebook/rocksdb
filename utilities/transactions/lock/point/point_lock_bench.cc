//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else
#include "rocksdb/point_lock_bench_tool.h"
int main(int argc, char** argv) {
  return ROCKSDB_NAMESPACE::point_lock_bench_tool(argc, argv);
}
#endif  // GFLAGS
