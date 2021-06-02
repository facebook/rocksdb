//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !(defined GFLAGS) || defined(ROCKSDB_LITE)

#include <cstdio>
int main() {
#ifndef GFLAGS
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
#endif
#ifdef ROCKSDB_LITE
  fprintf(stderr, "DbUndumpTool is not supported in ROCKSDB_LITE\n");
#endif
  return 1;
}

#else

#include "rocksdb/convenience.h"
#include "rocksdb/db_dump_tool.h"
#include "util/gflags_compat.h"

DEFINE_string(dump_location, "", "Path to the dump file that will be loaded");
DEFINE_string(db_path, "", "Path to the db that we will undump the file into");
DEFINE_bool(compact, false, "Compact the db after loading the dumped file");
DEFINE_string(db_options, "",
              "Options string used to open the database that will be loaded");

int main(int argc, char **argv) {
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_db_path == "" || FLAGS_dump_location == "") {
    fprintf(stderr, "Please set --db_path and --dump_location\n");
    return 1;
  }

  ROCKSDB_NAMESPACE::UndumpOptions undump_options;
  undump_options.db_path = FLAGS_db_path;
  undump_options.dump_location = FLAGS_dump_location;
  undump_options.compact_db = FLAGS_compact;

  ROCKSDB_NAMESPACE::Options db_options;
  if (FLAGS_db_options != "") {
    ROCKSDB_NAMESPACE::Options parsed_options;
    ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::GetOptionsFromString(
        db_options, FLAGS_db_options, &parsed_options);
    if (!s.ok()) {
      fprintf(stderr, "Cannot parse provided db_options\n");
      return 1;
    }
    db_options = parsed_options;
  }

  ROCKSDB_NAMESPACE::DbUndumpTool tool;
  if (!tool.Run(undump_options, db_options)) {
    return 1;
  }
  return 0;
}
#endif  // !(defined GFLAGS) || defined(ROCKSDB_LITE)
