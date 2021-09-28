//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// this is a simple micro-benchmark for compare ribbon filter vs. other filter
// for more comprehensive, please check the dedicate util/filter_bench.
#include <benchmark/benchmark.h>

#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

static DB* db;

static void Write(benchmark::State &state) {
  // setup DB
  if (state.thread_index == 0) {
    Options options;
    options.create_if_missing = true;

    std::string db_name = "/Users/zjay/ws/tmp/bench";
    Status s = DB::Open(options, db_name, &db);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
  }

  auto rnd = Random(12345);
  for (auto _ : state) {
    auto wo = WriteOptions();
    wo.disableWAL = false;
    auto s = db->Put(wo, rnd.RandomString(10), rnd.RandomString(100));
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }
//  if(state.thread_index == 0) {
  db->Close();
//  }
}

BENCHMARK(Write)->Threads(100);

}  // namespace ROCKSDB_NAMESPACE

BENCHMARK_MAIN();
