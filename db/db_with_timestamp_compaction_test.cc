//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

namespace {
std::string Key1(uint64_t key) {
  std::string ret;
  PutFixed64(&ret, key);
  std::reverse(ret.begin(), ret.end());
  return ret;
}

std::string Timestamp(uint64_t ts) {
  std::string ret;
  PutFixed64(&ret, ts);
  return ret;
}
}  // anonymous namespace

class TimestampCompatibleCompactionTest : public DBTestBase {
 public:
  TimestampCompatibleCompactionTest()
      : DBTestBase("ts_compatible_compaction_test", /*env_do_fsync=*/true) {}

  std::string Get(const std::string& key, uint64_t ts) {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(ts);
    Slice ts_slice = ts_str;
    read_opts.timestamp = &ts_slice;
    std::string value;
    Status s = db_->Get(read_opts, key, &value);
    if (s.IsNotFound()) {
      value.assign("NOT_FOUND");
    } else if (!s.ok()) {
      value.assign(s.ToString());
    }
    return value;
  }
};

TEST_F(TimestampCompatibleCompactionTest, UserKeyCrossFileBoundary) {
  Options options = CurrentOptions();
  options.env = env_;
  options.compaction_style = kCompactionStyleLevel;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.level0_file_num_compaction_trigger = 3;
  constexpr size_t kNumKeysPerFile = 101;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        const auto* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_NE(nullptr, compaction);
        ASSERT_EQ(0, compaction->start_level());
        ASSERT_EQ(1, compaction->num_input_levels());
        // Check that all 3 L0 ssts are picked for level compaction.
        ASSERT_EQ(3, compaction->num_input_files(0));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // Write a L0 with keys 0, 1, ..., 99 with ts from 100 to 199.
  uint64_t ts = 100;
  uint64_t key = 0;
  WriteOptions write_opts;
  for (; key < kNumKeysPerFile - 1; ++key, ++ts) {
    std::string ts_str = Timestamp(ts);
    ASSERT_OK(
        db_->Put(write_opts, Key1(key), ts_str, "foo_" + std::to_string(key)));
  }
  // Write another L0 with keys 99 with newer ts.
  ASSERT_OK(Flush());
  uint64_t saved_read_ts1 = ts++;
  key = 99;
  for (int i = 0; i < 4; ++i, ++ts) {
    std::string ts_str = Timestamp(ts);
    ASSERT_OK(
        db_->Put(write_opts, Key1(key), ts_str, "bar_" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  uint64_t saved_read_ts2 = ts++;
  // Write another L0 with keys 99, 100, 101, ..., 150
  for (; key <= 150; ++key, ++ts) {
    std::string ts_str = Timestamp(ts);
    ASSERT_OK(
        db_->Put(write_opts, Key1(key), ts_str, "foo1_" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  // Wait for compaction to finish
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  uint64_t read_ts = ts;
  ASSERT_EQ("foo_99", Get(Key1(99), saved_read_ts1));
  ASSERT_EQ("bar_99", Get(Key1(99), saved_read_ts2));
  ASSERT_EQ("foo1_99", Get(Key1(99), read_ts));
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(TimestampCompatibleCompactionTest, MultipleSubCompactions) {
  Options options = CurrentOptions();
  options.env = env_;
  options.compaction_style = kCompactionStyleUniversal;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.level0_file_num_compaction_trigger = 3;
  options.max_subcompactions = 3;
  options.target_file_size_base = 1024;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  uint64_t ts = 100;
  uint64_t key = 0;
  WriteOptions write_opts;

  // Write keys 0, 1, ..., 499 with ts from 100 to 599.
  {
    for (; key <= 499; ++key, ++ts) {
      std::string ts_str = Timestamp(ts);
      ASSERT_OK(db_->Put(write_opts, Key1(key), ts_str,
                         "foo_" + std::to_string(key)));
    }
  }

  // Write keys 500, ..., 999 with ts from 600 to 1099.
  {
    for (; key <= 999; ++key, ++ts) {
      std::string ts_str = Timestamp(ts);
      ASSERT_OK(db_->Put(write_opts, Key1(key), ts_str,
                         "foo_" + std::to_string(key)));
    }
    ASSERT_OK(Flush());
  }

  // Wait for compaction to finish
  {
    ASSERT_OK(dbfull()->RunManualCompaction(
        static_cast_with_check<ColumnFamilyHandleImpl>(
            db_->DefaultColumnFamily())
            ->cfd(),
        0 /* input_level */, 1 /* output_level */, CompactRangeOptions(),
        nullptr /* begin */, nullptr /* end */, true /* exclusive */,
        true /* disallow_trivial_move */,
        std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
        "" /*trim_ts*/));
  }

  // Check stats to make sure multiple subcompactions were scheduled for
  // boundaries not to be nullptr.
  {
    HistogramData num_sub_compactions;
    options.statistics->histogramData(NUM_SUBCOMPACTIONS_SCHEDULED,
                                      &num_sub_compactions);
    ASSERT_GT(num_sub_compactions.sum, 1);
  }

  for (key = 0; key <= 999; ++key) {
    ASSERT_EQ("foo_" + std::to_string(key), Get(Key1(key), ts));
  }
}

class TestFilePartitioner : public SstPartitioner {
 public:
  explicit TestFilePartitioner() {}
  ~TestFilePartitioner() override {}

  const char* Name() const override { return "TestFilePartitioner"; }
  PartitionerResult ShouldPartition(
      const PartitionerRequest& /*request*/) override {
    return PartitionerResult::kRequired;
  }
  bool CanDoTrivialMove(const Slice& /*smallest_user_key*/,
                        const Slice& /*largest_user_key*/) override {
    return false;
  }
};

class TestFilePartitionerFactory : public SstPartitionerFactory {
 public:
  explicit TestFilePartitionerFactory() {}
  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /*context*/) const override {
    std::unique_ptr<SstPartitioner> ret =
        std::make_unique<TestFilePartitioner>();
    return ret;
  }
  const char* Name() const override { return "TestFilePartitionerFactory"; }
};

#ifndef ROCKSDB_LITE
TEST_F(TimestampCompatibleCompactionTest, CompactFilesRangeCheckL0) {
  Options options = CurrentOptions();
  options.env = env_;
  options.sst_partitioner_factory =
      std::make_shared<TestFilePartitionerFactory>();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  constexpr int kNumFiles = 10;
  constexpr int kKeysPerFile = 2;
  const std::string user_key = "foo";
  constexpr uint64_t start_ts = 10000;

  uint64_t cur_ts = start_ts;
  for (int k = 0; k < kNumFiles; ++k) {
    for (int i = 0; i < kKeysPerFile; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), user_key, Timestamp(cur_ts),
                         "v" + std::to_string(i)));
      ++cur_ts;
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  std::vector<std::string> input_files{};
  {
    std::vector<std::string> files;
    ASSERT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& f : files) {
      uint64_t file_num = 0;
      FileType file_type = FileType::kWalFile;
      if (!ParseFileName(f, &file_num, &file_type) ||
          file_type != FileType::kTableFile) {
        continue;
      }
      input_files.emplace_back(f);
    }
    // sorting here by name, which also happens to sort by generation date.
    std::sort(input_files.begin(), input_files.end());
    assert(kNumFiles == input_files.size());
    std::vector<std::string> tmp;
    tmp.emplace_back(input_files[input_files.size() / 2]);
    input_files.swap(tmp);
  }

  {
    std::vector<std::string> output_file_names;
    CompactionJobInfo compaction_job_info;
    ASSERT_OK(db_->CompactFiles(CompactionOptions(), input_files,
                                /*output_level=*/1, /*output_path_id=*/-1,
                                &output_file_names, &compaction_job_info));
    // We expect the L0 files older than the original provided input were all
    // included in the compaction.
    ASSERT_EQ(static_cast<size_t>(kNumFiles / 2 + 1),
              compaction_job_info.input_files.size());
  }
}

TEST_F(TimestampCompatibleCompactionTest, CompactFilesRangeCheckL1) {
  Options options = CurrentOptions();
  options.env = env_;
  options.sst_partitioner_factory =
      std::make_shared<TestFilePartitionerFactory>();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();

  constexpr int kNumFiles = 4;
  options.level0_file_num_compaction_trigger = kNumFiles;

  DestroyAndReopen(options);

  constexpr int kKeysPerFile = 2;
  const std::string user_key = "foo";
  constexpr uint64_t start_ts = 10000;

  uint64_t cur_ts = start_ts;
  // Generate some initial files in both L0 and L1.
  for (int k = 0; k < kNumFiles; ++k) {
    for (int i = 0; i < kKeysPerFile; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), user_key, Timestamp(cur_ts),
                         "v" + std::to_string(i)));
      ++cur_ts;
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(0, NumTableFilesAtLevel(/*level=*/0, /*cf=*/0));
  ASSERT_EQ(kNumFiles * kKeysPerFile,
            NumTableFilesAtLevel(/*level=*/1, /*cf=*/0));

  constexpr int additional_l0s = 2;
  for (int i = 0; i < additional_l0s; ++i, ++cur_ts) {
    ASSERT_OK(db_->Put(WriteOptions(), user_key, Timestamp(cur_ts), "v"));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_EQ(additional_l0s, NumTableFilesAtLevel(/*level=*/0, /*cf=*/0));

  std::vector<std::string> inputs;
  {
    std::vector<LiveFileMetaData> fmetas;
    db_->GetLiveFilesMetaData(&fmetas);
    bool included_one_l1 = false;
    for (const auto& meta : fmetas) {
      if (meta.level == 0) {
        inputs.emplace_back(meta.relative_filename);
      } else if (!included_one_l1) {
        inputs.emplace_back(meta.relative_filename);
        included_one_l1 = true;
      }
    }
  }
  ASSERT_EQ(static_cast<size_t>(3), inputs.size());
  {
    std::vector<std::string> output_file_names;
    CompactionJobInfo compaction_job_info;

    ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, /*output_level=*/1,
                                /*output_path_id=*/-1, &output_file_names,
                                &compaction_job_info));
    ASSERT_EQ(kNumFiles * kKeysPerFile + 2, output_file_names.size());
    ASSERT_EQ(kNumFiles * kKeysPerFile + 2,
              static_cast<int>(compaction_job_info.input_files.size()));
  }
}

TEST_F(TimestampCompatibleCompactionTest, EmptyCompactionOutput) {
  Options options = CurrentOptions();
  options.env = env_;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);

  std::string ts_str = Timestamp(1);
  WriteOptions wopts;
  ASSERT_OK(
      db_->DeleteRange(wopts, db_->DefaultColumnFamily(), "k1", "k3", ts_str));
  ASSERT_OK(Flush());

  ts_str = Timestamp(3);
  Slice ts = ts_str;
  CompactRangeOptions cro;
  // range tombstone will be dropped during compaction
  cro.full_history_ts_low = &ts;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
}
#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
