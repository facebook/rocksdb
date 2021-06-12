//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "db/db_test_util.h"
#include "port/stack_trace.h"

namespace ROCKSDB_NAMESPACE {

class MyTestCompactionService : public CompactionService {
 public:
  MyTestCompactionService(const std::string& db_path,
                          std::shared_ptr<FileSystem> fs, Options& options)
      : db_path_(db_path), fs_(fs), options_(options) {}

  static const char* kClassName() { return "MyTestCompactionService"; }

  const char* Name() const override { return kClassName(); }

  CompactionServiceJobStatus Start(const std::string& compaction_service_input,
                                   int job_id) override {
    InstrumentedMutexLock l(&mutex_);
    jobs_.emplace(job_id, compaction_service_input);
    CompactionServiceJobStatus s = CompactionServiceJobStatus::kSuccess;
    TEST_SYNC_POINT_CALLBACK("MyTestCompactionService::Start::End", &s);
    return s;
  }

  CompactionServiceJobStatus WaitForComplete(
      int job_id, std::string* compaction_service_result) override {
    std::string compaction_input;
    {
      InstrumentedMutexLock l(&mutex_);
      auto i = jobs_.find(job_id);
      if (i == jobs_.end()) {
        return CompactionServiceJobStatus::kFailure;
      }
      compaction_input = std::move(i->second);
      jobs_.erase(i);
    }

    CompactionServiceOptionsOverride options_override;
    options_override.env = options_.env;
    options_override.file_checksum_gen_factory =
        options_.file_checksum_gen_factory;
    options_override.comparator = options_.comparator;
    options_override.merge_operator = options_.merge_operator;
    options_override.compaction_filter = options_.compaction_filter;
    options_override.compaction_filter_factory =
        options_.compaction_filter_factory;
    options_override.prefix_extractor = options_.prefix_extractor;
    options_override.table_factory = options_.table_factory;
    options_override.sst_partitioner_factory = options_.sst_partitioner_factory;

    Status s = DB::OpenAndCompact(
        db_path_, db_path_ + "/" + ROCKSDB_NAMESPACE::ToString(job_id),
        compaction_input, compaction_service_result, options_override);
    TEST_SYNC_POINT_CALLBACK("MyTestCompactionService::WaitForComplete::End",
                             compaction_service_result);
    compaction_num_.fetch_add(1);
    if (s.ok()) {
      return CompactionServiceJobStatus::kSuccess;
    } else {
      return CompactionServiceJobStatus::kFailure;
    }
  }

  int GetCompactionNum() { return compaction_num_.load(); }

 private:
  InstrumentedMutex mutex_;
  std::atomic_int compaction_num_{0};
  std::map<int, std::string> jobs_;
  const std::string db_path_;
  std::shared_ptr<FileSystem> fs_;
  Options options_;
};

class CompactionServiceTest : public DBTestBase {
 public:
  explicit CompactionServiceTest()
      : DBTestBase("compaction_service_test", true) {}

 protected:
  void GenerateTestData() {
    // Generate 20 files @ L2
    for (int i = 0; i < 20; i++) {
      for (int j = 0; j < 10; j++) {
        int key_id = i * 10 + j;
        ASSERT_OK(Put(Key(key_id), "value" + ToString(key_id)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);

    // Generate 10 files @ L1 overlap with all 20 files @ L2
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        int key_id = i * 20 + j * 2;
        ASSERT_OK(Put(Key(key_id), "value_new" + ToString(key_id)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(1);
    ASSERT_EQ(FilesPerLevel(), "0,10,20");
  }

  void VerifyTestData() {
    for (int i = 0; i < 200; i++) {
      auto result = Get(Key(i));
      if (i % 2) {
        ASSERT_EQ(result, "value" + ToString(i));
      } else {
        ASSERT_EQ(result, "value_new" + ToString(i));
      }
    }
  }
};

TEST_F(CompactionServiceTest, BasicCompactions) {
  Options options = CurrentOptions();
  options.env = env_;
  options.compaction_service = std::make_shared<MyTestCompactionService>(
      dbname_, env_->GetFileSystem(), options);

  DestroyAndReopen(options);

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + ToString(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + ToString(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + ToString(i));
    } else {
      ASSERT_EQ(result, "value_new" + ToString(i));
    }
  }
  auto my_cs =
      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());
  ASSERT_GE(my_cs->GetCompactionNum(), 1);

  // Test failed compaction
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::CompactWithoutInstallation::End", [&](void* status) {
        // override job status
        Status* s = static_cast<Status*>(status);
        *s = Status::Aborted("MyTestCompactionService failed to compact!");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s;
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      s = Put(Key(key_id), "value_new" + ToString(key_id));
      if (s.IsAborted()) {
        break;
      }
    }
    if (s.IsAborted()) {
      break;
    }
    s = Flush();
    if (s.IsAborted()) {
      break;
    }
    s = dbfull()->TEST_WaitForCompact();
    if (s.IsAborted()) {
      break;
    }
  }
  ASSERT_TRUE(s.IsAborted());
}

TEST_F(CompactionServiceTest, ManualCompaction) {
  Options options = CurrentOptions();
  options.env = env_;
  options.disable_auto_compactions = true;
  options.compaction_service = std::make_shared<MyTestCompactionService>(
      dbname_, env_->GetFileSystem(), options);
  DestroyAndReopen(options);
  GenerateTestData();

  auto my_cs =
      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  start_str = Key(120);
  start = start_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  end_str = Key(92);
  end = end_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();
}

TEST_F(CompactionServiceTest, FailedToStart) {
  Options options = CurrentOptions();
  options.env = env_;
  options.disable_auto_compactions = true;
  options.compaction_service = std::make_shared<MyTestCompactionService>(
      dbname_, env_->GetFileSystem(), options);
  DestroyAndReopen(options);
  GenerateTestData();

  SyncPoint::GetInstance()->SetCallBack(
      "MyTestCompactionService::Start::End", [&](void* status) {
        // override job status
        auto s = static_cast<CompactionServiceJobStatus*>(status);
        *s = CompactionServiceJobStatus::kFailure;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
}

TEST_F(CompactionServiceTest, InvalidResult) {
  Options options = CurrentOptions();
  options.env = env_;
  options.disable_auto_compactions = true;
  options.compaction_service = std::make_shared<MyTestCompactionService>(
      dbname_, env_->GetFileSystem(), options);
  DestroyAndReopen(options);
  GenerateTestData();

  SyncPoint::GetInstance()->SetCallBack(
      "MyTestCompactionService::WaitForComplete::End", [&](void* result) {
        // override job status
        auto result_str = static_cast<std::string*>(result);
        *result_str = "Invalid Str";
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_FALSE(s.ok());
}

// TODO: support sub-compaction
TEST_F(CompactionServiceTest, DISABLED_SubCompaction) {
  Options options = CurrentOptions();
  options.env = env_;
  options.max_subcompactions = 10;
  options.target_file_size_base = 1 << 10;  // 1KB
  options.disable_auto_compactions = true;
  options.compaction_service = std::make_shared<MyTestCompactionService>(
      dbname_, env_->GetFileSystem(), options);

  DestroyAndReopen(options);
  GenerateTestData();

  auto cro = CompactRangeOptions();
  cro.max_subcompactions = 10;
  db_->CompactRange(cro, nullptr, nullptr);
}

class PartialDeleteCompactionFilter : public CompactionFilter {
 public:
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& key, ValueType /*value_type*/,
      const Slice& /*existing_value*/, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    int i = std::stoi(key.ToString().substr(3));
    if (i > 5 && i <= 105) {
      return CompactionFilter::Decision::kRemove;
    }
    return CompactionFilter::Decision::kKeep;
  }

  const char* Name() const override { return "PartialDeleteCompactionFilter"; }
};

TEST_F(CompactionServiceTest, CompactionFilter) {
  Options options = CurrentOptions();
  options.env = env_;
  auto delete_comp_filter = PartialDeleteCompactionFilter();
  options.compaction_filter = &delete_comp_filter;
  options.compaction_service = std::make_shared<MyTestCompactionService>(
      dbname_, env_->GetFileSystem(), options);

  DestroyAndReopen(options);

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + ToString(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + ToString(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i > 5 && i <= 105) {
      ASSERT_EQ(result, "NOT_FOUND");
    } else if (i % 2) {
      ASSERT_EQ(result, "value" + ToString(i));
    } else {
      ASSERT_EQ(result, "value_new" + ToString(i));
    }
  }
  auto my_cs =
      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
}

TEST_F(CompactionServiceTest, Snapshot) {
  Options options = CurrentOptions();
  options.env = env_;
  options.compaction_service = std::make_shared<MyTestCompactionService>(
      dbname_, env_->GetFileSystem(), options);

  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(1), "value1"));
  ASSERT_OK(Put(Key(2), "value1"));
  const Snapshot* s1 = db_->GetSnapshot();
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(1), "value2"));
  ASSERT_OK(Put(Key(3), "value2"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  auto my_cs =
      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
  ASSERT_EQ("value1", Get(Key(1), s1));
  ASSERT_EQ("value2", Get(Key(1)));
  db_->ReleaseSnapshot(s1);
}

TEST_F(CompactionServiceTest, ConcurrentCompaction) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;
  options.env = env_;
  options.compaction_service = std::make_shared<MyTestCompactionService>(
      dbname_, env_->GetFileSystem(), options);
  options.max_background_jobs = 20;

  DestroyAndReopen(options);
  GenerateTestData();

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);

  std::vector<std::thread> threads;
  for (const auto& file : meta.levels[1].files) {
    threads.push_back(std::thread([&]() {
      std::string fname = file.db_path + "/" + file.name;
      ASSERT_OK(db_->CompactFiles(CompactionOptions(), {fname}, 2));
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + ToString(i));
    } else {
      ASSERT_EQ(result, "value_new" + ToString(i));
    }
  }
  auto my_cs =
      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());
  ASSERT_EQ(my_cs->GetCompactionNum(), 10);
  ASSERT_EQ(FilesPerLevel(), "0,0,10");
}

}  // namespace ROCKSDB_NAMESPACE

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED as CompactionService is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
