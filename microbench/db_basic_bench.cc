//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// this is a simple micro-benchmark for compare ribbon filter vs. other filter
// for more comprehensive, please check the dedicate util/filter_bench.
#include <benchmark/benchmark.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

static void DBOpen(benchmark::State& state) {
  // create DB
  DB* db;
  Options options;
  auto env = Env::Default();
  std::string db_path;
  auto s = env->GetTestDirectory(&db_path);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }
  std::string db_name = db_path + "/bench_dbopen";

  DestroyDB(db_name, options);

  options.create_if_missing = true;
  s = DB::Open(options, db_name, &db);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }
  db->Close();

  options.create_if_missing = false;

  auto rnd = Random(12345);

  for (auto _ : state) {
    s = DB::Open(options, db_name, &db);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    state.PauseTiming();
    auto wo = WriteOptions();
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 100; j++) {
        s = db->Put(wo, rnd.RandomString(10), rnd.RandomString(100));
        if (!s.ok()) {
          state.SkipWithError(s.ToString().c_str());
        }
      }
      s = db->Flush(FlushOptions());
    }
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    s = db->Close();
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    state.ResumeTiming();
  }
  DestroyDB(db_name, options);
}

BENCHMARK(DBOpen)->Iterations(200);  // specify iteration number as the db size
                                     // is impacted by iteration number

static void DBClose(benchmark::State& state) {
  // create DB
  DB* db;
  Options options;
  auto env = Env::Default();
  std::string db_path;
  auto s = env->GetTestDirectory(&db_path);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }
  std::string db_name = db_path + "/bench_dbclose";

  DestroyDB(db_name, options);

  options.create_if_missing = true;
  s = DB::Open(options, db_name, &db);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }
  db->Close();

  options.create_if_missing = false;

  auto rnd = Random(12345);

  for (auto _ : state) {
    state.PauseTiming();
    s = DB::Open(options, db_name, &db);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    auto wo = WriteOptions();
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 100; j++) {
        s = db->Put(wo, rnd.RandomString(10), rnd.RandomString(100));
        if (!s.ok()) {
          state.SkipWithError(s.ToString().c_str());
        }
      }
      s = db->Flush(FlushOptions());
    }
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    state.ResumeTiming();
    s = db->Close();
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }
  DestroyDB(db_name, options);
}

BENCHMARK(DBClose)->Iterations(200);  // specify iteration number as the db size
                                      // is impacted by iteration number

}  // namespace ROCKSDB_NAMESPACE

BENCHMARK_MAIN();
