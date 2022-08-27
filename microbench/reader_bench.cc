//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "benchmark/benchmark.h"
#include "microbench/bench_util.h"
#include "rocksdb/db.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

static void BlockBasedTableGet(benchmark::State& state) {
  Options options;
  const MutableCFOptions moptions(options);
  InternalKeyComparator comparator(options.comparator);
  ImmutableOptions ioptions(options);
  const uint64_t kNumKeys = 10 * 1024;

  // generate file
  auto env = Env::Default();
  auto fs = env->GetFileSystem();
  std::string db_path;
  Status s = env->GetTestDirectory(&db_path);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  Random rand(301);
  std::string fname =
      db_path + kFilePathSeparator + "block-based-table-multi-get-bench";

  FileOptions foptions;
  std::unique_ptr<FSWritableFile> f;
  s = fs->NewWritableFile(fname, foptions, &f, nullptr);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  auto writer =
      std::make_unique<WritableFileWriter>(std::move(f), fname, EnvOptions());

  IntTblPropCollectorFactories factories;
  TableBuilderOptions table_builder_options(
      ioptions, moptions, comparator, &factories, kNoCompression,
      CompressionOptions(), 0, kDefaultColumnFamilyName, -1);
  std::unique_ptr<TableBuilder> table_builder(
      options.table_factory->NewTableBuilder(table_builder_options,
                                             writer.get()));

  KeyGenerator sequential_key_gen(kNumKeys);
  for (uint64_t i = 0; i < kNumKeys; i++) {
    Slice key = sequential_key_gen.Next();
    InternalKey internal_key(key, 0, ValueType::kTypeValue);
    table_builder->Add(internal_key.Encode(), rand.HumanReadableString(100));
  }

  s = table_builder->Finish();
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  s = writer->Close();
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  // create file reader
  std::unique_ptr<FSRandomAccessFile> file;
  s = fs->NewRandomAccessFile(fname, FileOptions(), &file, nullptr);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  auto reader = std::make_unique<RandomAccessFileReader>(
      std::move(file), fname, env->GetSystemClock().get());

  std::unique_ptr<TableReader> table;

  uint64_t file_size = 0;
  s = env->GetFileSize(fname, &file_size);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  TableReaderOptions table_reader_options = TableReaderOptions(
      ImmutableOptions(), moptions.prefix_extractor, EnvOptions(), comparator);
  s = options.table_factory->NewTableReader(ReadOptions(), table_reader_options,
                                            std::move(reader), file_size,
                                            &table, true);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, Slice(), nullptr, nullptr,
                         nullptr, nullptr, true, nullptr, nullptr);

  Random rnd(301);
  KeyGenerator rand_key_gen(&rnd, kNumKeys);

  for (auto _ : state) {
    Slice key = rand_key_gen.Next();
    InternalKey internal_key(key, 0, ValueType::kTypeValue);
    s = table->Get(ReadOptions(), internal_key.Encode(), &get_context,
                   moptions.prefix_extractor.get());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
  }
}

BENCHMARK(BlockBasedTableGet)->Iterations(1000000);

static void RandomAccessFileReaderRead(benchmark::State& state) {
  bool enable_statistics = state.range(0);
  constexpr int kFileNum = 10;
  auto env = Env::Default();
  auto fs = env->GetFileSystem();
  std::string db_path;
  Status s = env->GetTestDirectory(&db_path);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  // Setup multiple `RandomAccessFileReader`s with different parameters to be
  // used for test
  Random rand(301);
  std::string fname_base =
      db_path + kFilePathSeparator + "random-access-file-reader-read";
  std::vector<std::unique_ptr<RandomAccessFileReader>> readers;
  auto statistics_share = CreateDBStatistics();
  Statistics* statistics = enable_statistics ? statistics_share.get() : nullptr;
  for (int i = 0; i < kFileNum; i++) {
    std::string fname = fname_base + std::to_string(i);
    std::string content = rand.RandomString(kDefaultPageSize);
    std::unique_ptr<WritableFile> tgt_file;
    env->NewWritableFile(fname, &tgt_file, EnvOptions());
    s = tgt_file->Append(content);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }

    s = tgt_file->Close();
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }

    std::unique_ptr<FSRandomAccessFile> f;
    s = fs->NewRandomAccessFile(fname, FileOptions(), &f, nullptr);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }

    int rand_num = rand.Next() % 3;
    auto temperature = rand_num == 0   ? Temperature::kUnknown
                       : rand_num == 1 ? Temperature::kWarm
                                       : Temperature::kCold;
    readers.emplace_back(new RandomAccessFileReader(
        std::move(f), fname, env->GetSystemClock().get(), nullptr, statistics,
        0, nullptr, nullptr, {}, temperature, rand_num == 1));
  }

  IOOptions io_options;
  std::unique_ptr<char[]> scratch(new char[2048]);
  Slice result;
  uint64_t idx = 0;
  for (auto _ : state) {
    s = readers[idx++ % kFileNum]->Read(io_options, 0, kDefaultPageSize / 3,
                                        &result, scratch.get(), nullptr,
                                        Env::IO_TOTAL);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }

  // clean up
  for (int i = 0; i < kFileNum; i++) {
    std::string fname = fname_base + std::to_string(i);
    env->DeleteFile(fname).PermitUncheckedError();  // ignore return, okay to fail cleanup
  }
}

BENCHMARK(RandomAccessFileReaderRead)
    ->Iterations(1000000)
    ->Arg(0)
    ->Arg(1)
    ->ArgName("enable_statistics");

std::string GenerateKey(int primary_key, int secondary_key, int padding_size,
                        Random* rnd) {
  char buf[50];
  char* p = &buf[0];
  snprintf(buf, sizeof(buf), "%6d%4d", primary_key, secondary_key);
  std::string k(p);
  if (padding_size) {
    k += rnd->RandomString(padding_size);
  }

  return k;
}

void GenerateRandomKVs(std::vector<std::string>* keys,
                       std::vector<std::string>* values, const int from,
                       const int len, const int step = 1,
                       const int padding_size = 0,
                       const int keys_share_prefix = 1) {
  Random rnd(302);

  // generate different prefix
  for (int i = from; i < from + len; i += step) {
    // generating keys that share the prefix
    for (int j = 0; j < keys_share_prefix; ++j) {
      keys->emplace_back(GenerateKey(i, j, padding_size, &rnd));
      // 100 bytes values
      values->emplace_back(rnd.RandomString(100));
    }
  }
}

static void DataBlockSeek(benchmark::State& state) {
  Random rnd(301);
  Options options = Options();

  BlockBuilder builder(16, true, false,
                       BlockBasedTableOptions::kDataBlockBinarySearch);

  int num_records = 500;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  GenerateRandomKVs(&keys, &values, 0, num_records);

  for (int i = 0; i < num_records; i++) {
    std::string ukey(keys[i] + "1");
    InternalKey ikey(ukey, 0, kTypeValue);
    builder.Add(ikey.Encode().ToString(), values[i]);
  }

  Slice rawblock = builder.Finish();

  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents));

  SetPerfLevel(kEnableTime);
  uint64_t total = 0;
  for (auto _ : state) {
    DataBlockIter* iter = reader.NewDataIterator(options.comparator,
                                                 kDisableGlobalSequenceNumber);
    uint32_t index = rnd.Uniform(static_cast<int>(num_records));
    std::string ukey(keys[index] + "1");
    InternalKey ikey(ukey, 0, kTypeValue);
    get_perf_context()->Reset();
    bool may_exist = iter->SeekForGet(ikey.Encode().ToString());
    if (!may_exist) {
      state.SkipWithError("key not found");
    }
    total += get_perf_context()->block_seek_nanos;
    delete iter;
  }
  state.counters["seek_ns"] = benchmark::Counter(
      static_cast<double>(total), benchmark::Counter::kAvgIterations);
}

BENCHMARK(DataBlockSeek)->Iterations(1000000);


}  // namespace ROCKSDB_NAMESPACE

BENCHMARK_MAIN();
