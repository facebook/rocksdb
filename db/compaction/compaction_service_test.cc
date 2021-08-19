//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "db/db_test_util.h"
#include "port/stack_trace.h"

namespace ROCKSDB_NAMESPACE {

class TestCompactionServiceBase {
 public:
  virtual int GetCompactionNum() = 0;

  void OverrideStartStatus(CompactionServiceJobStatus s) {
    is_override_start_status = true;
    override_start_status = std::move(s);
  }

  void OverrideWaitResult(std::string str) {
    is_override_wait_result = true;
    override_wait_result = std::move(str);
  }

  void ResetOverride() {
    is_override_wait_result = false;
    is_override_start_status = false;
  }

  virtual ~TestCompactionServiceBase(){};

 protected:
  bool is_override_start_status = false;
  CompactionServiceJobStatus override_start_status;
  bool is_override_wait_result = false;
  std::string override_wait_result;
};

class MyTestCompactionServiceLegacy : public CompactionService,
                                      public TestCompactionServiceBase {
 public:
  MyTestCompactionServiceLegacy(
      const std::string& db_path, Options& options,
      std::shared_ptr<Statistics> statistics = nullptr)
      : db_path_(db_path), options_(options), statistics_(statistics) {}

  static const char* kClassName() { return "MyTestCompactionServiceLegacy"; }

  const char* Name() const override { return kClassName(); }

  CompactionServiceJobStatus Start(const std::string& compaction_service_input,
                                   uint64_t job_id) override {
    InstrumentedMutexLock l(&mutex_);
    jobs_.emplace(job_id, compaction_service_input);
    CompactionServiceJobStatus s = CompactionServiceJobStatus::kSuccess;
    if (is_override_start_status) {
      return override_start_status;
    }
    return s;
  }

  CompactionServiceJobStatus WaitForComplete(
      uint64_t job_id, std::string* compaction_service_result) override {
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
    options_override.statistics = statistics_;

    Status s = DB::OpenAndCompact(
        db_path_, db_path_ + "/" + ROCKSDB_NAMESPACE::ToString(job_id),
        compaction_input, compaction_service_result, options_override);
    if (is_override_wait_result) {
      *compaction_service_result = override_wait_result;
    }
    compaction_num_.fetch_add(1);
    if (s.ok()) {
      return CompactionServiceJobStatus::kSuccess;
    } else {
      return CompactionServiceJobStatus::kFailure;
    }
  }

  int GetCompactionNum() override { return compaction_num_.load(); }

 private:
  InstrumentedMutex mutex_;
  std::atomic_int compaction_num_{0};
  std::map<uint64_t, std::string> jobs_;
  const std::string db_path_;
  Options options_;
  std::shared_ptr<Statistics> statistics_;
};

class MyTestCompactionService : public CompactionService,
                                public TestCompactionServiceBase {
 public:
  MyTestCompactionService(const std::string& db_path, Options& options,
                          std::shared_ptr<Statistics> statistics = nullptr)
      : db_path_(db_path), options_(options), statistics_(statistics) {}

  static const char* kClassName() { return "MyTestCompactionService"; }

  const char* Name() const override { return kClassName(); }

  CompactionServiceJobStatus Start(const CompactionServiceJobInfo& info,
                                   const std::string& compaction_service_input,
                                   uint64_t job_id) override {
    InstrumentedMutexLock l(&mutex_);
    assert(info.db_name.compare(db_path_) == 0);
    jobs_.emplace(job_id, compaction_service_input);
    CompactionServiceJobStatus s = CompactionServiceJobStatus::kSuccess;
    if (is_override_start_status) {
      return override_start_status;
    }
    return s;
  }

  CompactionServiceJobStatus WaitForComplete(
      const CompactionServiceJobInfo& info, uint64_t job_id,
      std::string* compaction_service_result) override {
    std::string compaction_input;
    assert(info.db_name.compare(db_path_) == 0);
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
    options_override.statistics = statistics_;

    Status s = DB::OpenAndCompact(
        db_path_, db_path_ + "/" + ROCKSDB_NAMESPACE::ToString(job_id),
        compaction_input, compaction_service_result, options_override);
    if (is_override_wait_result) {
      *compaction_service_result = override_wait_result;
    }
    compaction_num_.fetch_add(1);
    if (s.ok()) {
      return CompactionServiceJobStatus::kSuccess;
    } else {
      return CompactionServiceJobStatus::kFailure;
    }
  }

  int GetCompactionNum() override { return compaction_num_.load(); }

 private:
  InstrumentedMutex mutex_;
  std::atomic_int compaction_num_{0};
  std::map<uint64_t, std::string> jobs_;
  const std::string db_path_;
  Options options_;
  std::shared_ptr<Statistics> statistics_;
};

template <class T>
class CCTest : public DBTestBase {
 public:
  explicit CCTest() : DBTestBase("compaction_service_test", true) {}

  T GetCS() {}
};

typedef testing::Types<MyTestCompactionService, MyTestCompactionServiceLegacy>
    MyTest;
TYPED_TEST_CASE(CCTest,  // Instance name
                MyTest);

TYPED_TEST(CCTest, Test1) { std::cout << "HI" << std::endl; }

template <class T>
class CompactionServiceTest : public DBTestBase {
 public:
  explicit CompactionServiceTest()
      : DBTestBase("compaction_service_test", true) {}

 protected:
  void ReopenWithCompactionService() {
    Options options = CurrentOptions();
    options.env = env_;
    primary_statistics_ = CreateDBStatistics();
    options.statistics = primary_statistics_;
    compactor_statistics_ = CreateDBStatistics();
    compaction_service_ = std::make_shared<T>(dbname_, options, compactor_statistics_);
    options.compaction_service = compaction_service_;
    DestroyAndReopen(options);
  }

  Statistics* GetCompactorStatistics() {
    return primary_statistics_.get();
  }

  Statistics* GetPrimaryStatistics() {
    return compactor_statistics_.get();
  }

  T* GetCompactionService() {
    return compaction_service_.get();
  }

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

 private:
  std::shared_ptr<Statistics> compactor_statistics_;
  std::shared_ptr<Statistics> primary_statistics_;
  std::shared_ptr<T> compaction_service_;
};

typedef testing::Types<MyTestCompactionService, MyTestCompactionServiceLegacy> MyTestCompactionServiceTypes;
TYPED_TEST_CASE(CompactionServiceTest, MyTestCompactionServiceTypes);

TYPED_TEST(CompactionServiceTest, BasicCompactions) {
  CompactionServiceTest<TypeParam>::ReopenWithCompactionService();

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(CompactionServiceTest<TypeParam>::Put(CompactionServiceTest<TypeParam>::Key(key_id), "value" + ToString(key_id)));
    }
    ASSERT_OK(CompactionServiceTest<TypeParam>::Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(CompactionServiceTest<TypeParam>::Put(CompactionServiceTest<TypeParam>::Key(key_id), "value_new" + ToString(key_id)));
    }
    ASSERT_OK(CompactionServiceTest<TypeParam>::Flush());
  }
  ASSERT_OK(CompactionServiceTest<TypeParam>::dbfull()->TEST_WaitForCompact());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = CompactionServiceTest<TypeParam>::Get(CompactionServiceTest<TypeParam>::Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + ToString(i));
    } else {
      ASSERT_EQ(result, "value_new" + ToString(i));
    }
  }
  auto my_cs = CompactionServiceTest<TypeParam>::GetCompactionService();
  Statistics* compactor_statistics = CompactionServiceTest<TypeParam>::GetCompactorStatistics();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);

  // make sure the compaction statistics is only recorded on remote side
//  ASSERT_GE(
//      compactor_statistics->getTickerCount(COMPACTION_KEY_DROP_NEWER_ENTRY), 1);
  Statistics* primary_statistics = CompactionServiceTest<TypeParam>::GetPrimaryStatistics();
//  ASSERT_EQ(primary_statistics->getTickerCount(COMPACTION_KEY_DROP_NEWER_ENTRY),
//            0);

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
      s = CompactionServiceTest<TypeParam>::Put(CompactionServiceTest<TypeParam>::Key(key_id), "value_new" + ToString(key_id));
      if (s.IsAborted()) {
        break;
      }
    }
    if (s.IsAborted()) {
      break;
    }
    s = CompactionServiceTest<TypeParam>::Flush();
    if (s.IsAborted()) {
      break;
    }
    s = CompactionServiceTest<TypeParam>::dbfull()->TEST_WaitForCompact();
    if (s.IsAborted()) {
      break;
    }
  }
  ASSERT_TRUE(s.IsAborted());
}

TYPED_TEST(CompactionServiceTest, ManualCompaction) {
  Options options = CompactionServiceTest<TypeParam>::CurrentOptions();
  options.env = CompactionServiceTest<TypeParam>::env_;
  options.disable_auto_compactions = true;
  options.compaction_service =
      std::make_shared<MyTestCompactionService>(CompactionServiceTest<TypeParam>::dbname_, options);
  CompactionServiceTest<TypeParam>::DestroyAndReopen(options);
  CompactionServiceTest<TypeParam>::GenerateTestData();

  auto my_cs =
      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());

  std::string start_str = CompactionServiceTest<TypeParam>::Key(15);
  std::string end_str = CompactionServiceTest<TypeParam>::Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(CompactionServiceTest<TypeParam>::db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  CompactionServiceTest<TypeParam>::VerifyTestData();

  start_str = CompactionServiceTest<TypeParam>::Key(120);
  start = start_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(CompactionServiceTest<TypeParam>::db_->CompactRange(CompactRangeOptions(), &start, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  CompactionServiceTest<TypeParam>::VerifyTestData();

  end_str = CompactionServiceTest<TypeParam>::Key(92);
  end = end_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(CompactionServiceTest<TypeParam>::db_->CompactRange(CompactRangeOptions(), nullptr, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  CompactionServiceTest<TypeParam>::VerifyTestData();

  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(CompactionServiceTest<TypeParam>::db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  CompactionServiceTest<TypeParam>::VerifyTestData();
}
//
//TYPED_TEST(CompactionServiceTest, FailedToStart) {
//  Options options = CurrentOptions();
//  options.env = env_;
//  options.disable_auto_compactions = true;
//  options.compaction_service =
//      std::make_shared<MyTestCompactionService>(dbname_, options);
//  DestroyAndReopen(options);
//  GenerateTestData();
//
//  auto my_cs = dynamic_cast<TestCompactionServiceBase*>(
//      options.compaction_service.get());
//  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kFailure);
//
//  std::string start_str = Key(15);
//  std::string end_str = Key(45);
//  Slice start(start_str);
//  Slice end(end_str);
//  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
//  ASSERT_TRUE(s.IsIncomplete());
//}
//
//TYPED_TEST(CompactionServiceTest, InvalidResult) {
//  Options options = CurrentOptions();
//  options.env = env_;
//  options.disable_auto_compactions = true;
//  options.compaction_service =
//      std::make_shared<MyTestCompactionService>(dbname_, options);
//  DestroyAndReopen(options);
//  GenerateTestData();
//
//  auto my_cs = dynamic_cast<TestCompactionServiceBase*>(
//      options.compaction_service.get());
//  my_cs->OverrideWaitResult("Invalid Str");
//
//  std::string start_str = Key(15);
//  std::string end_str = Key(45);
//  Slice start(start_str);
//  Slice end(end_str);
//  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
//  ASSERT_FALSE(s.ok());
//}
//
//TYPED_TEST(CompactionServiceTest, SubCompaction) {
//  Options options = CurrentOptions();
//  options.env = env_;
//  options.max_subcompactions = 10;
//  options.target_file_size_base = 1 << 10;  // 1KB
//  options.disable_auto_compactions = true;
//  options.compaction_service =
//      std::make_shared<MyTestCompactionService>(dbname_, options);
//
//  DestroyAndReopen(options);
//  GenerateTestData();
//  VerifyTestData();
//
//  auto my_cs =
//      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());
//  int compaction_num_before = my_cs->GetCompactionNum();
//
//  auto cro = CompactRangeOptions();
//  cro.max_subcompactions = 10;
//  Status s = db_->CompactRange(cro, nullptr, nullptr);
//  ASSERT_OK(s);
//  VerifyTestData();
//  int compaction_num = my_cs->GetCompactionNum() - compaction_num_before;
//  // make sure there's sub-compaction by checking the compaction number
//  ASSERT_GE(compaction_num, 2);
//}
//
//class PartialDeleteCompactionFilter : public CompactionFilter {
// public:
//  CompactionFilter::Decision FilterV2(
//      int /*level*/, const Slice& key, ValueType /*value_type*/,
//      const Slice& /*existing_value*/, std::string* /*new_value*/,
//      std::string* /*skip_until*/) const override {
//    int i = std::stoi(key.ToString().substr(3));
//    if (i > 5 && i <= 105) {
//      return CompactionFilter::Decision::kRemove;
//    }
//    return CompactionFilter::Decision::kKeep;
//  }
//
//  const char* Name() const override { return "PartialDeleteCompactionFilter"; }
//};
//
//TYPED_TEST(CompactionServiceTest, CompactionFilter) {
//  Options options = CurrentOptions();
//  options.env = env_;
//  std::unique_ptr<CompactionFilter> delete_comp_filter(
//      new PartialDeleteCompactionFilter());
//  options.compaction_filter = delete_comp_filter.get();
//  options.compaction_service =
//      std::make_shared<MyTestCompactionService>(dbname_, options);
//
//  DestroyAndReopen(options);
//
//  for (int i = 0; i < 20; i++) {
//    for (int j = 0; j < 10; j++) {
//      int key_id = i * 10 + j;
//      ASSERT_OK(Put(Key(key_id), "value" + ToString(key_id)));
//    }
//    ASSERT_OK(Flush());
//  }
//
//  for (int i = 0; i < 10; i++) {
//    for (int j = 0; j < 10; j++) {
//      int key_id = i * 20 + j * 2;
//      ASSERT_OK(Put(Key(key_id), "value_new" + ToString(key_id)));
//    }
//    ASSERT_OK(Flush());
//  }
//  ASSERT_OK(dbfull()->TEST_WaitForCompact());
//
//  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
//
//  // verify result
//  for (int i = 0; i < 200; i++) {
//    auto result = Get(Key(i));
//    if (i > 5 && i <= 105) {
//      ASSERT_EQ(result, "NOT_FOUND");
//    } else if (i % 2) {
//      ASSERT_EQ(result, "value" + ToString(i));
//    } else {
//      ASSERT_EQ(result, "value_new" + ToString(i));
//    }
//  }
//  auto my_cs =
//      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());
//  ASSERT_GE(my_cs->GetCompactionNum(), 1);
//}
//
//TYPED_TEST(CompactionServiceTest, Snapshot) {
//  Options options = CurrentOptions();
//  options.env = env_;
//  options.compaction_service =
//      std::make_shared<MyTestCompactionService>(dbname_, options);
//
//  DestroyAndReopen(options);
//
//  ASSERT_OK(Put(Key(1), "value1"));
//  ASSERT_OK(Put(Key(2), "value1"));
//  const Snapshot* s1 = db_->GetSnapshot();
//  ASSERT_OK(Flush());
//
//  ASSERT_OK(Put(Key(1), "value2"));
//  ASSERT_OK(Put(Key(3), "value2"));
//  ASSERT_OK(Flush());
//
//  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
//  auto my_cs =
//      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());
//  ASSERT_GE(my_cs->GetCompactionNum(), 1);
//  ASSERT_EQ("value1", Get(Key(1), s1));
//  ASSERT_EQ("value2", Get(Key(1)));
//  db_->ReleaseSnapshot(s1);
//}
//
//TYPED_TEST(CompactionServiceTest, ConcurrentCompaction) {
//  Options options = CurrentOptions();
//  options.level0_file_num_compaction_trigger = 100;
//  options.env = env_;
//  options.compaction_service =
//      std::make_shared<MyTestCompactionService>(dbname_, options);
//  options.max_background_jobs = 20;
//
//  DestroyAndReopen(options);
//  GenerateTestData();
//
//  ColumnFamilyMetaData meta;
//  db_->GetColumnFamilyMetaData(&meta);
//
//  std::vector<std::thread> threads;
//  for (const auto& file : meta.levels[1].files) {
//    threads.push_back(std::thread([&]() {
//      std::string fname = file.db_path + "/" + file.name;
//      ASSERT_OK(db_->CompactFiles(CompactionOptions(), {fname}, 2));
//    }));
//  }
//
//  for (auto& thread : threads) {
//    thread.join();
//  }
//  ASSERT_OK(dbfull()->TEST_WaitForCompact());
//
//  // verify result
//  for (int i = 0; i < 200; i++) {
//    auto result = Get(Key(i));
//    if (i % 2) {
//      ASSERT_EQ(result, "value" + ToString(i));
//    } else {
//      ASSERT_EQ(result, "value_new" + ToString(i));
//    }
//  }
//  auto my_cs =
//      dynamic_cast<MyTestCompactionService*>(options.compaction_service.get());
//  ASSERT_EQ(my_cs->GetCompactionNum(), 10);
//  ASSERT_EQ(FilesPerLevel(), "0,0,10");
//}

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
