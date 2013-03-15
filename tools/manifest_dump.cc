// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/storage_options.h"

static int verbose = 0;
static int output_hex = 0;

using namespace leveldb;

//
// Takes a manifest file and dumps out all metedata
//
int main(int argc, char** argv) {

  // parse command line options
  size_t n;
  char junk;
  int foundfile = 0;
  std::string manifestfile;
  for (int i = 1; i < argc; i++) {
    std::string param(argv[i]);
    if ((n = param.find("--file=")) != std::string::npos) {
      manifestfile = param.substr(strlen("--file="));
      foundfile = 1;
    } else if (sscanf(argv[i], "--verbose=%ld%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      verbose = n;
    } else if (param == "--output_hex") {
      output_hex = n;
    } else {
      fprintf(stderr, "Unknown or badly-formatted option: %s\n",
              argv[i]);
      abort();
    }
  }
  if (!foundfile) {
    fprintf(stderr,
      "%s [--verbose=0|1] [--output_hex] [--file=pathname of manifest file]\n",
      argv[0]);
    abort();
  }

  if (verbose) {
    printf("Processing Manifest file %s\n", manifestfile.c_str());
  }

  Options options;
  StorageOptions sopt;
  std::string file(manifestfile);
  std::string dbname("dummy");
  TableCache* tc = new TableCache(dbname, &options, sopt, 10);
  const InternalKeyComparator* cmp = new InternalKeyComparator(options.comparator);

  VersionSet* versions = new VersionSet(dbname, &options, sopt,
                                   tc, cmp);
  Status s = versions->DumpManifest(options, file, verbose, output_hex);
  if (!s.ok()) {
    printf("Error in processing file %s %s\n", manifestfile.c_str(),
           s.ToString().c_str());
  }
  if (verbose) {
    printf("Processing Manifest file %s done\n", manifestfile.c_str());
  }
}
