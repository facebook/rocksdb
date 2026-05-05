//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef OS_WIN
#include <unistd.h>
#endif  // !OS_WIN

#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "table/multiget_context.h"

namespace ROCKSDB_NAMESPACE {
namespace {

constexpr size_t kNumTableKeys = 16 * 1024;
constexpr size_t kMaxBenchmarkBatchSize = 1024;
constexpr size_t kValueSize = 32;

std::string MakeKey(uint64_t value) {
  char buf[32];
  snprintf(buf, sizeof(buf), "key%016" PRIu64, value);
  return std::string(buf);
}

std::string MakeMissingKey(uint64_t value) {
  char buf[32];
  snprintf(buf, sizeof(buf), "missing%016" PRIu64, value);
  return std::string(buf);
}

std::string MakeValue(uint64_t value) {
  std::string result = "value";
  result.append(std::to_string(value));
  result.resize(kValueSize, 'x');
  return result;
}

uint64_t ProcessId() {
#ifndef OS_WIN
  return static_cast<uint64_t>(getpid());
#else
  return 0;
#endif
}

char FilePathSeparator() {
#ifdef OS_WIN
  return '\\';
#else
  return '/';
#endif
}

std::vector<Slice> ToSlices(const std::vector<std::string>& keys) {
  std::vector<Slice> slices;
  slices.reserve(keys.size());
  for (const auto& key : keys) {
    slices.emplace_back(key);
  }
  return slices;
}

void MakeLookupKeys(size_t batch_size, int hit_rate_pct,
                    std::vector<std::string>* storage) {
  storage->clear();
  storage->reserve(batch_size);
  for (size_t i = 0; i < batch_size; ++i) {
    const bool hit =
        hit_rate_pct == 100 ||
        (hit_rate_pct != 0 && static_cast<int>(i % 100) < hit_rate_pct);
    if (hit) {
      storage->emplace_back(MakeKey((i * 131) % kNumTableKeys));
    } else {
      storage->emplace_back(MakeMissingKey(i));
    }
  }
}

void SstFileReaderBenchArguments(benchmark::internal::Benchmark* b) {
  for (int batch_size :
       {1, 8, 32, 128, static_cast<int>(kMaxBenchmarkBatchSize)}) {
    for (int hit_rate_pct : {0, 10, 100}) {
      b->Args({batch_size, hit_rate_pct});
    }
  }
  b->ArgNames({"batch_size", "hit_rate_pct"});
}

class SstFileReaderBenchFixture {
 public:
  SstFileReaderBenchFixture(size_t batch_size, int hit_rate_pct)
      : options_(MakeOptions()), reader_(options_) {
    Status s = PrepareFileName();
    if (s.ok()) {
      s = BuildSstFile();
    }
    if (s.ok()) {
      s = reader_.Open(file_name_);
    }
    if (s.ok()) {
      MakeLookupKeys(batch_size, hit_rate_pct, &lookup_storage_);
      lookup_keys_ = ToSlices(lookup_storage_);
      may_match_results_.reset(new bool[batch_size]);
      PrepareMultiGetChunks();
      WarmFilter();
    }
    status_ = s;
  }

  ~SstFileReaderBenchFixture() {
    if (!file_name_.empty()) {
      options_.env->DeleteFile(file_name_).PermitUncheckedError();
    }
  }

  SstFileReaderBenchFixture(const SstFileReaderBenchFixture&) = delete;
  SstFileReaderBenchFixture& operator=(const SstFileReaderBenchFixture&) =
      delete;
  SstFileReaderBenchFixture(SstFileReaderBenchFixture&&) = delete;
  SstFileReaderBenchFixture& operator=(SstFileReaderBenchFixture&&) = delete;

  Status status() const { return status_; }

  void RunMayMatch() {
    reader_.MayMatch(read_options_, lookup_keys_.data(), lookup_keys_.size(),
                     may_match_results_.get());
    benchmark::DoNotOptimize(may_match_results_.get());
  }

  void RunMultiGet() {
    for (size_t i = 0; i < multiget_keys_.size(); ++i) {
      std::vector<Status> statuses =
          reader_.MultiGet(read_options_, multiget_keys_[i], &values_[i]);
      benchmark::DoNotOptimize(statuses.data());
      benchmark::DoNotOptimize(values_[i].data());
    }
  }

  size_t batch_size() const { return lookup_keys_.size(); }

 private:
  static Options MakeOptions() {
    Options options;
    options.compression = kNoCompression;
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(16, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    return options;
  }

  Status PrepareFileName() {
    std::string test_dir;
    Status s = options_.env->GetTestDirectory(&test_dir);
    if (!s.ok()) {
      return s;
    }
    file_name_ = test_dir + FilePathSeparator() + "sst_file_reader_bench_" +
                 std::to_string(ProcessId()) + "_" +
                 std::to_string(reinterpret_cast<uintptr_t>(this)) + ".sst";
    options_.env->DeleteFile(file_name_).PermitUncheckedError();
    return Status::OK();
  }

  Status BuildSstFile() {
    SstFileWriter writer(EnvOptions(), options_);
    Status s = writer.Open(file_name_);
    for (uint64_t i = 0; s.ok() && i < kNumTableKeys; ++i) {
      s = writer.Put(MakeKey(i), MakeValue(i));
    }
    if (s.ok()) {
      s = writer.Finish();
    }
    return s;
  }

  void PrepareMultiGetChunks() {
    for (size_t base = 0; base < lookup_keys_.size();
         base += MultiGetContext::MAX_BATCH_SIZE) {
      const size_t chunk_size = std::min<size_t>(
          MultiGetContext::MAX_BATCH_SIZE, lookup_keys_.size() - base);
      multiget_keys_.emplace_back(lookup_keys_.begin() + base,
                                  lookup_keys_.begin() + base + chunk_size);
      values_.emplace_back();
      values_.back().reserve(chunk_size);
    }
  }

  void WarmFilter() {
    reader_.MayMatch(read_options_, lookup_keys_.data(), lookup_keys_.size(),
                     may_match_results_.get());
  }

  Options options_;
  ReadOptions read_options_;
  SstFileReader reader_;
  std::string file_name_;
  Status status_;
  std::vector<std::string> lookup_storage_;
  std::vector<Slice> lookup_keys_;
  std::unique_ptr<bool[]> may_match_results_;
  std::vector<std::vector<Slice>> multiget_keys_;
  std::vector<std::vector<PinnableSlice>> values_;
};

void BM_SstFileReaderMayMatch(benchmark::State& state) {
  SstFileReaderBenchFixture fixture(static_cast<size_t>(state.range(0)),
                                    static_cast<int>(state.range(1)));
  if (!fixture.status().ok()) {
    state.SkipWithError(fixture.status().ToString().c_str());
    return;
  }

  for (auto _ : state) {
    fixture.RunMayMatch();
  }
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(fixture.batch_size()));
}

void BM_SstFileReaderMultiGet(benchmark::State& state) {
  SstFileReaderBenchFixture fixture(static_cast<size_t>(state.range(0)),
                                    static_cast<int>(state.range(1)));
  if (!fixture.status().ok()) {
    state.SkipWithError(fixture.status().ToString().c_str());
    return;
  }

  for (auto _ : state) {
    fixture.RunMultiGet();
  }
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(fixture.batch_size()));
}

BENCHMARK(BM_SstFileReaderMayMatch)->Apply(SstFileReaderBenchArguments);
BENCHMARK(BM_SstFileReaderMultiGet)->Apply(SstFileReaderBenchArguments);

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

BENCHMARK_MAIN();
