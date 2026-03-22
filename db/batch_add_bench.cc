//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <chrono>
#include <iostream>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "util/coding.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class BatchAddBenchmark {
 public:
  BatchAddBenchmark()
      : num_entries_(5000000),
        value_size_(200),
        db_path_("/tmp/batch_add_bench") {}

  ~BatchAddBenchmark() {
    if (db_) {
      delete db_;
      db_ = nullptr;
    }
    DestroyDB(db_path_, Options());
  }

  void Run() {
    std::cout << "BatchAdd Benchmark\n";
    std::cout << "==================\n";
    std::cout << "Entries: " << num_entries_ << "\n";
    std::cout << "Value size: " << value_size_ << " bytes\n\n";

    std::cout << "=== Sorted Key Insertion ===\n";
    RunComparison(false);

    std::cout << "\n";

    std::cout << "=== Random Key Insertion ===\n";
    RunComparison(true);
  }

  void RunComparison(bool random_order) {
    std::cout << "Running with individual Add calls...\n";
    auto time_add = RunWithAdd(false, random_order);
    std::cout << "Time: " << time_add << " ms\n";
    std::cout << "Throughput: " << (num_entries_ * 1000.0 / time_add)
              << " ops/sec\n\n";

    std::cout << "Running with BatchAdd API (use_batch_add=true)...\n";
    auto time_batch_add = RunWithAdd(true, random_order);
    std::cout << "Time: " << time_batch_add << " ms\n";
    std::cout << "Throughput: " << (num_entries_ * 1000.0 / time_batch_add)
              << " ops/sec\n\n";

    double speedup = static_cast<double>(time_add) / time_batch_add;
    std::cout << "Speedup: " << speedup << "x\n";
  }

 private:
  int64_t RunWithAdd(bool use_batch_add, bool random_order) {
    if (db_) {
      delete db_;
      db_ = nullptr;
    }
    DestroyDB(db_path_, Options());

    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 10ULL * 1024 * 1024 * 1024;  // 10GB
    options.allow_concurrent_memtable_write = false;

    Status s = DB::Open(options, db_path_, &db_);
    if (!s.ok()) {
      std::cerr << "Failed to open DB: " << s.ToString() << "\n";
      return -1;
    }

    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(num_entries_);
    values.reserve(num_entries_);

    Random rnd(12345);
    std::string value(value_size_, 'x');

    for (int i = 0; i < num_entries_; ++i) {
      char key_buf[16];
      snprintf(key_buf, sizeof(key_buf), "key%08d", i);
      keys.emplace_back(key_buf);

      for (int j = 0; j < value_size_; ++j) {
        value[j] = static_cast<char>('a' + (rnd.Next() % 26));
      }
      values.emplace_back(value);
    }

    if (random_order) {
      // Fisher-Yates shuffle using RocksDB Random
      for (int i = num_entries_ - 1; i > 0; --i) {
        int j = static_cast<int>(rnd.Next() % (i + 1));
        std::swap(keys[i], keys[j]);
        std::swap(values[i], values[j]);
      }
    }

    WriteOptions write_options;
    write_options.disableWAL = true;
    write_options.use_batch_add = use_batch_add;

    auto start = std::chrono::high_resolution_clock::now();

    const int batch_size = 1000;
    for (int i = 0; i < num_entries_; i += batch_size) {
      WriteBatch batch;
      int end = std::min(i + batch_size, num_entries_);

      for (int j = i; j < end; ++j) {
        s = batch.Put(keys[j], values[j]);
        if (!s.ok()) {
          std::cerr << "Failed to Put: " << s.ToString() << "\n";
          return -1;
        }
      }

      s = db_->Write(write_options, &batch);
      if (!s.ok()) {
        std::cerr << "Failed to Write: " << s.ToString() << "\n";
        return -1;
      }
    }

    auto e = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(e - start);

    return duration.count();
  }

  int num_entries_;
  int value_size_;
  std::string db_path_;
  DB* db_ = nullptr;
};

}  // namespace ROCKSDB_NAMESPACE

int main() {
  ROCKSDB_NAMESPACE::BatchAddBenchmark bench;
  bench.Run();
  return 0;
}
