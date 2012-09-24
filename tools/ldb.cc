// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string>
#include <iostream>
#include <sstream>

#include "leveldb/db.h"
#include "leveldb/iterator.h"

std::string HexToString(std::string str) {
  std::string parsed;
  for (int i = 0; i < str.length(); ) {
    int c;
    sscanf(str.c_str() + i, "%2X", &c);
    parsed.push_back(c);
    i += 2;
  }
  return parsed;
}

static void print_help() {
  fprintf(stderr,
      "db_dump "
      "--start=[START_KEY] "
      "--end=[END_KEY] "
      "--max_keys=[NUM] "
      "--hex "
      "--count_only "
      "--stats "
      "[PATH]\n");
}

int main(int argc, char** argv) {
  std::string db_path;
  std::string start;
  std::string end;
  uint64_t max_keys = -1;
  bool print_stats = false;
  bool count_only = false;
  uint64_t count = 0;
  bool hex = false;

  // Parse command line args
  char junk;
  uint64_t n;
  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--start=", 8) == 0) {
      start= argv[i] + 8;
    } else if (strncmp(argv[i], "--end=", 6) == 0) {
      end = argv[i] + 6;
    } else if (sscanf(argv[i], "--max_keys=%ld%c", &n, &junk) == 1) {
      max_keys = n;
    } else if (strncmp(argv[i], "--stats", 7) == 0) {
      print_stats = true;
    } else if (strncmp(argv[i], "--count_only", 12) == 0) {
      count_only = true;
    } else if (strncmp(argv[i], "--hex", 5) == 0) {
      hex = true;
    } else if (i == (argc - 1)) {
      db_path = argv[i];
    } else {
      print_help();
      exit(1);
    }
  }

  if (hex) {
    start = HexToString(start);
    end = HexToString(end);
  }

  if (db_path.empty()) {
    print_help();
    exit(1);
  }

  // Open DB
  leveldb::Options options;
  leveldb::DB *db;
  leveldb::Status status = leveldb::DB::Open(options, db_path, &db);

  if (!status.ok()) {
    fprintf(stderr, "%s\n", status.ToString().c_str());
    exit(1);
  }

  // Print DB stats if desired
  if (print_stats) {
    std::string stats;
    if (db->GetProperty("leveldb.stats", &stats)) {
      fprintf(stdout, "%s\n", stats.c_str());
    }
  }

  // Setup key iterator
  leveldb::Iterator* iter = db->NewIterator(leveldb::ReadOptions());
  status = iter->status();
  if (!status.ok()) {
    fprintf(stderr, "%s\n", status.ToString().c_str());
    delete db;
    exit(1);
  }

  for (iter->Seek(start); iter->Valid(); iter->Next()) {
    // If end marker was specified, we stop before it
    if (!end.empty() && (iter->key().ToString() >= end))
      break;

    // Terminate if maximum number of keys have been dumped
    if (max_keys == 0)
      break;

    --max_keys;
    ++count;

    if (!count_only) {
      if (hex) {
        std::string str = iter->key().ToString();
        for (int i = 0; i < str.length(); ++i) {
          fprintf(stdout, "%X", str[i]);
        }
        fprintf(stdout, " ==> ");
        str = iter->value().ToString();
        for (int i = 0; i < str.length(); ++i) {
          fprintf(stdout, "%X", str[i]);
        }
        fprintf(stdout, "\n");
      } else {
        fprintf(stdout, "%s ==> %s\n",
            iter->key().ToString().c_str(),
            iter->value().ToString().c_str());
      }
    }
  }

  fprintf(stdout, "Keys in range: %d\n", count);

  // Clean up
  delete iter;
  delete db;

  return 0;
}

