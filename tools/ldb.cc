// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string>
#include <iostream>
#include <sstream>
#include <stdlib.h>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

std::string HexToString(const std::string& str) {
  std::string parsed;
  for (int i = 0; i < str.length(); ) {
    int c;
    sscanf(str.c_str() + i, "%2X", &c);
    parsed.push_back(c);
    i += 2;
  }
  return parsed;
}


static void print_usage() {
  fprintf(stderr,
    "ldb [compact|dump] "
    "--db-path=database_path "
    "[--from=START KEY] "
    "[--to=END KEY ] "
    "[--max_keys=[NUM] (only for dump)] "
    "[--hex ] "
    "[--count_only (only for dump)] "
    "[--stats (only for dump) ] \n");
}


static void safe_open_db(const std::string& dbname, leveldb::DB** db) {
  leveldb::Options options;
  options.create_if_missing = false;
  leveldb::Status status = leveldb::DB::Open(options, dbname, db);

  if(!status.ok()) {
    fprintf(
      stderr,
      "Could not open db at %s\nERROR: %s",
      dbname.data(),
      status.ToString().data()
    );
    exit(1);
  }
}


static void dump_db(
  const std::string& db_path,
  std::string& start,
  std::string& end,
  int64_t max_keys,
  const bool hex,
  const bool print_stats,
  const bool count_only
) {
    // Parse command line args
  uint64_t count = 0;


  if (hex) {
    start = HexToString(start);
    end = HexToString(end);
  }

  leveldb::DB *db;
  safe_open_db(db_path, &db);

  if (print_stats) {
    std::string stats;
    if (db->GetProperty("leveldb.stats", &stats)) {
      fprintf(stdout, "%s\n", stats.c_str());
    }
  }

  // Setup key iterator
  leveldb::Iterator* iter = db->NewIterator(leveldb::ReadOptions());
  leveldb::Status status = iter->status();
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

  fprintf(stdout, "Keys in range: %lld\n", (long long) count);

  // Clean up
  delete iter;
  delete db;
}

static void compact(
  const std::string dbname,
  std::string from,
  std::string to,
  const bool hex
) {

  leveldb::DB* db;
  safe_open_db(dbname, &db);

  if(hex) {
    from = HexToString(from);
    to = HexToString(to);
  }

  leveldb::Slice* begin = from.empty() ? NULL : new leveldb::Slice(from);
  leveldb::Slice* end = to.empty() ? NULL : new leveldb::Slice(to);
  db->CompactRange(begin, end);
  delete db;
}

int main(int argc, char** argv) {

  enum {
    DUMP, COMPACT
  } command;

  if (argc < 2) {
    print_usage();
    exit(1);
  }

  size_t n;

  const std::string dbnameKey = "--db-path=";
  const std::string toKey = "--to=";
  const std::string fromKey = "--from=";
  std::string dbname;
  bool dbnameFound = false;
  std::string from;
  std::string to;
  int64_t temp;
  int64_t max_keys = -1;

  bool print_stats = false;
  bool count_only = false;
  bool hex = false;
  char junk;


  std::string commandString = argv[1];
  if (commandString == "dump") {
    command = DUMP;
  } else if (commandString == "compact") {
    command = COMPACT;
  } else {
    print_usage();
    exit(1);
  }

  for (int i = 2; i < argc; i++) {
    std::string param(argv[i]);
    if ((n = param.find(dbnameKey)) != std::string::npos) {
      dbname = param.substr(dbnameKey.size());
      dbnameFound = true;
    } else if ((n = param.find(fromKey)) != std::string::npos) {
      from = param.substr(fromKey.size());
    } else if ((n = param.find(toKey)) != std::string::npos) {
      to = param.substr(toKey.size());
    } else if (sscanf(argv[i], "--max_keys=%ld%c", &temp, &junk) == 1) {
      max_keys = temp;
    } else if (strncmp(argv[i], "--stats", 7) == 0) {
      print_stats = true;
    } else if (strncmp(argv[i], "--count_only", 12) == 0) {
      count_only = true;
    } else if (strncmp(argv[i], "--hex", 5) == 0) {
      hex = true;
    } else {
      print_usage();
      exit(1);
    }
  }

  if (!dbnameFound || dbname.empty()) {
    fprintf(stderr, "DB path required. See help\n");
    print_usage();
    exit(1);
  }

  switch(command) {
    case DUMP:
      dump_db(dbname, from, to, max_keys, hex, print_stats, count_only);
      break;
    case COMPACT:
      compact(dbname, from, to, hex);
      break;
    default:
      print_usage();
      exit(1);
  }

  return 0;
}
