// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#ifdef ROCKSDB_LITE
int main() {
  fprintf("Not supported in lite mode.\n");
  return 1;
}
#else
#ifdef GFLAGS
#ifdef LIBZBD
int zenfs_tool(int argc, char** argv);
int main(int argc, char** argv) { return zenfs_tool(argc, argv); }
#else
int main() {
  fprintf(stderr, "Please install libzbd to run the zenfs tool\n");
  return 1;
}
#endif  // LIBZBD
#else
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#endif  // GFLAGS
#endif  // ROCKSDB_LITE
