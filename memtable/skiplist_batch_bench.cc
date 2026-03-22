//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <numeric>
#include <vector>

#include "memory/arena.h"
#include "memtable/inlineskiplist.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

struct BenchComparator {
  using DecodedType = uint64_t;

  static DecodedType decode_key(const char* b) {
    uint64_t id;
    memcpy(&id, b, sizeof(uint64_t));
    return id;
  }

  int operator()(const char* a, const char* b) const {
    uint64_t id_a, id_b;
    memcpy(&id_a, a, sizeof(uint64_t));
    memcpy(&id_b, b, sizeof(uint64_t));
    if (id_a < id_b) return -1;
    if (id_a > id_b) return +1;
    return 0;
  }

  int operator()(const char* a, const DecodedType b) const {
    uint64_t id_a;
    memcpy(&id_a, a, sizeof(uint64_t));
    if (id_a < b) return -1;
    if (id_a > b) return +1;
    return 0;
  }
};

struct BenchResult {
  const char* name;
  size_t num_keys;
  size_t batch_size;
  double duration_ms;
  double throughput_mops;
};

void RunSingleInsertBench(size_t num_keys, size_t key_size, bool random_keys,
                          BenchResult* result) {
  Arena arena;
  BenchComparator cmp;
  InlineSkipList<BenchComparator> list(cmp, &arena);
  Random rnd(42);

  // Pre-generate key IDs
  std::vector<uint64_t> key_ids(num_keys);
  if (random_keys) {
    for (size_t i = 0; i < num_keys; i++) {
      key_ids[i] = static_cast<uint64_t>(rnd.Next()) << 32 | rnd.Next();
    }
  } else {
    for (size_t i = 0; i < num_keys; i++) {
      key_ids[i] = i;
    }
  }

  auto start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < num_keys; i++) {
    char* buf = list.AllocateKey(key_size);
    memset(buf, 0, key_size);
    memcpy(buf, &key_ids[i], sizeof(uint64_t));
    list.Insert(buf);
  }
  auto end = std::chrono::high_resolution_clock::now();

  double ms =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count() /
      1000.0;
  result->name = random_keys ? "Single (random)" : "Single (sequential)";
  result->num_keys = num_keys;
  result->batch_size = 1;
  result->duration_ms = ms;
  result->throughput_mops = num_keys / ms / 1000.0;
}

void RunBatchInsertBench(size_t num_keys, size_t key_size, size_t batch_size,
                         bool random_keys, BenchResult* result) {
  Arena arena;
  BenchComparator cmp;
  InlineSkipList<BenchComparator> list(cmp, &arena);
  Random rnd(42);

  // Pre-generate key IDs
  std::vector<uint64_t> key_ids(num_keys);
  if (random_keys) {
    for (size_t i = 0; i < num_keys; i++) {
      key_ids[i] = static_cast<uint64_t>(rnd.Next()) << 32 | rnd.Next();
    }
  } else {
    for (size_t i = 0; i < num_keys; i++) {
      key_ids[i] = i;
    }
  }

  auto start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < num_keys; i += batch_size) {
    size_t current_batch = std::min(batch_size, num_keys - i);
    std::vector<const char*> batch_keys(current_batch);
    for (size_t j = 0; j < current_batch; j++) {
      char* buf = list.AllocateKey(key_size);
      memset(buf, 0, key_size);
      memcpy(buf, &key_ids[i + j], sizeof(uint64_t));
      batch_keys[j] = buf;
    }
    list.InsertBatch(batch_keys.data(), current_batch);
  }
  auto end = std::chrono::high_resolution_clock::now();

  double ms =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count() /
      1000.0;
  result->name = random_keys ? "Batch (random)" : "Batch (sequential)";
  result->num_keys = num_keys;
  result->batch_size = batch_size;
  result->duration_ms = ms;
  result->throughput_mops = num_keys / ms / 1000.0;
}

void PrintResults(const std::vector<BenchResult>& results,
                  double baseline_ms) {
  printf("%-25s %8s %10s %12s %10s\n", "Method", "Batch", "Time(ms)",
         "Mops/s", "Speedup");
  printf("%-25s %8s %10s %12s %10s\n", "-------------------------", "--------",
         "----------", "------------", "----------");
  for (const auto& r : results) {
    printf("%-25s %8zu %10.1f %12.3f %9.2fx\n", r.name, r.batch_size,
           r.duration_ms, r.throughput_mops, baseline_ms / r.duration_ms);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main() {
  using namespace ROCKSDB_NAMESPACE;

  const size_t num_keys = 1000000;
  std::vector<size_t> key_sizes = {64, 256, 1024};
  std::vector<size_t> batch_sizes = {1, 8, 32, 128, 512};

  printf("Skiplist Batch Insert Benchmark\n");
  printf("================================\n");
  printf("Keys: %zu\n\n", num_keys);

  for (size_t key_size : key_sizes) {
    for (bool random : {false, true}) {
      printf("\nKey size: %zu bytes, %s keys\n", key_size,
             random ? "random" : "sequential");
      printf("-------------------------------------------\n");

      std::vector<BenchResult> results;

      // Baseline: single insert
      BenchResult single_result;
      RunSingleInsertBench(num_keys, key_size, random, &single_result);
      results.push_back(single_result);
      double baseline_ms = single_result.duration_ms;

      // Batch inserts with different sizes
      for (size_t bs : batch_sizes) {
        if (bs == 1) continue;  // Skip batch_size=1 (same as single)
        BenchResult batch_result;
        RunBatchInsertBench(num_keys, key_size, bs, random, &batch_result);
        results.push_back(batch_result);
      }

      PrintResults(results, baseline_ms);
    }
  }

  return 0;
}
