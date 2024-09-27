//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "table/unique_id_impl.h"

namespace ROCKSDB_NAMESPACE {

class MyTestCompactionService : public CompactionService {
 public:
  MyTestCompactionService(
      std::string db_path, Options& options,
      std::shared_ptr<Statistics>& statistics,
      std::vector<std::shared_ptr<EventListener>>& listeners,
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
          table_properties_collector_factories)
      : db_path_(std::move(db_path)),
        options_(options),
        statistics_(statistics),
        start_info_("na", "na", "na", 0, Env::TOTAL, CompactionReason::kUnknown,
                    false, false, false),
        wait_info_("na", "na", "na", 0, Env::TOTAL, CompactionReason::kUnknown,
                   false, false, false),
        listeners_(listeners),
        table_properties_collector_factories_(
            std::move(table_properties_collector_factories)) {}

  static const char* kClassName() { return "MyTestCompactionService"; }

  const char* Name() const override { return kClassName(); }

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override {
    InstrumentedMutexLock l(&mutex_);
    start_info_ = info;
    assert(info.db_name == db_path_);
    std::string unique_id = Env::Default()->GenerateUniqueId();
    jobs_.emplace(unique_id, compaction_service_input);
    infos_.emplace(unique_id, info);
    CompactionServiceScheduleResponse response(
        unique_id, is_override_start_status_
                       ? override_start_status_
                       : CompactionServiceJobStatus::kSuccess);
    return response;
  }

  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override {
    std::string compaction_input;
    {
      InstrumentedMutexLock l(&mutex_);
      auto job_index = jobs_.find(scheduled_job_id);
      if (job_index == jobs_.end()) {
        return CompactionServiceJobStatus::kFailure;
      }
      compaction_input = std::move(job_index->second);
      jobs_.erase(job_index);

      auto info_index = infos_.find(scheduled_job_id);
      if (info_index == infos_.end()) {
        return CompactionServiceJobStatus::kFailure;
      }
      wait_info_ = std::move(info_index->second);
      infos_.erase(info_index);
    }
    if (is_override_wait_status_) {
      return override_wait_status_;
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
    if (!listeners_.empty()) {
      options_override.listeners = listeners_;
    }

    if (!table_properties_collector_factories_.empty()) {
      options_override.table_properties_collector_factories =
          table_properties_collector_factories_;
    }

    OpenAndCompactOptions options;
    options.canceled = &canceled_;

    Status s =
        DB::OpenAndCompact(options, db_path_, db_path_ + "/" + scheduled_job_id,
                           compaction_input, result, options_override);
    {
      InstrumentedMutexLock l(&mutex_);
      if (is_override_wait_result_) {
        *result = override_wait_result_;
      }
      result_ = *result;
    }
    compaction_num_.fetch_add(1);
    if (s.ok()) {
      return CompactionServiceJobStatus::kSuccess;
    } else {
      return CompactionServiceJobStatus::kFailure;
    }
  }

  void OnInstallation(const std::string& /*scheduled_job_id*/,
                      CompactionServiceJobStatus status) override {
    final_updated_status_ = status;
  }

  int GetCompactionNum() { return compaction_num_.load(); }

  CompactionServiceJobInfo GetCompactionInfoForStart() { return start_info_; }
  CompactionServiceJobInfo GetCompactionInfoForWait() { return wait_info_; }

  void OverrideStartStatus(CompactionServiceJobStatus s) {
    is_override_start_status_ = true;
    override_start_status_ = s;
  }

  void OverrideWaitStatus(CompactionServiceJobStatus s) {
    is_override_wait_status_ = true;
    override_wait_status_ = s;
  }

  void OverrideWaitResult(std::string str) {
    is_override_wait_result_ = true;
    override_wait_result_ = std::move(str);
  }

  void ResetOverride() {
    is_override_wait_result_ = false;
    is_override_start_status_ = false;
    is_override_wait_status_ = false;
  }

  void SetCanceled(bool canceled) { canceled_ = canceled; }

  void GetResult(CompactionServiceResult* deserialized) {
    CompactionServiceResult::Read(result_, deserialized).PermitUncheckedError();
  }

  CompactionServiceJobStatus GetFinalCompactionServiceJobStatus() {
    return final_updated_status_.load();
  }

 private:
  InstrumentedMutex mutex_;
  std::atomic_int compaction_num_{0};
  std::map<std::string, std::string> jobs_;
  std::map<std::string, CompactionServiceJobInfo> infos_;
  const std::string db_path_;
  Options options_;
  std::shared_ptr<Statistics> statistics_;
  CompactionServiceJobInfo start_info_;
  CompactionServiceJobInfo wait_info_;
  bool is_override_start_status_ = false;
  CompactionServiceJobStatus override_start_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_status_ = false;
  CompactionServiceJobStatus override_wait_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_result_ = false;
  std::string result_;
  std::string override_wait_result_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories_;
  std::atomic_bool canceled_{false};
  std::atomic<CompactionServiceJobStatus> final_updated_status_{
      CompactionServiceJobStatus::kUseLocal};
};

class CompactionServiceTest : public DBTestBase {
 public:
  explicit CompactionServiceTest()
      : DBTestBase("compaction_service_test", true) {}

 protected:
  void ReopenWithCompactionService(Options* options) {
    options->env = env_;
    primary_statistics_ = CreateDBStatistics();
    options->statistics = primary_statistics_;
    compactor_statistics_ = CreateDBStatistics();

    compaction_service_ = std::make_shared<MyTestCompactionService>(
        dbname_, *options, compactor_statistics_, remote_listeners,
        remote_table_properties_collector_factories);
    options->compaction_service = compaction_service_;
    DestroyAndReopen(*options);
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, *options);
  }

  Statistics* GetCompactorStatistics() { return compactor_statistics_.get(); }

  Statistics* GetPrimaryStatistics() { return primary_statistics_.get(); }

  MyTestCompactionService* GetCompactionService() {
    CompactionService* cs = compaction_service_.get();
    return static_cast_with_check<MyTestCompactionService>(cs);
  }

  void GenerateTestData(bool move_files_manually = false) {
    // Generate 20 files @ L2 Per CF
    for (int cf_id = 0; cf_id < static_cast<int>(handles_.size()); cf_id++) {
      for (int i = 0; i < 20; i++) {
        for (int j = 0; j < 10; j++) {
          int key_id = i * 10 + j;
          ASSERT_OK(Put(cf_id, Key(key_id), "value" + std::to_string(key_id)));
        }
        ASSERT_OK(Flush(cf_id));
      }
      if (move_files_manually) {
        MoveFilesToLevel(2, cf_id);
      }

      // Generate 10 files @ L1 overlap with all 20 files @ L2
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
          int key_id = i * 20 + j * 2;
          ASSERT_OK(
              Put(cf_id, Key(key_id), "value_new" + std::to_string(key_id)));
        }
        ASSERT_OK(Flush(cf_id));
      }
      if (move_files_manually) {
        MoveFilesToLevel(1, cf_id);
        ASSERT_EQ(FilesPerLevel(cf_id), "0,10,20");
      }
    }
  }

  void VerifyTestData() {
    for (int cf_id = 0; cf_id < static_cast<int>(handles_.size()); cf_id++) {
      for (int i = 0; i < 200; i++) {
        auto result = Get(cf_id, Key(i));
        if (i % 2) {
          ASSERT_EQ(result, "value" + std::to_string(i));
        } else {
          ASSERT_EQ(result, "value_new" + std::to_string(i));
        }
      }
    }
  }

  std::vector<std::shared_ptr<EventListener>> remote_listeners;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      remote_table_properties_collector_factories;

 private:
  std::shared_ptr<Statistics> compactor_statistics_;
  std::shared_ptr<Statistics> primary_statistics_;
  std::shared_ptr<CompactionService> compaction_service_;
};

TEST_F(CompactionServiceTest, BasicCompactions) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  Statistics* primary_statistics = GetPrimaryStatistics();
  Statistics* compactor_statistics = GetCompactorStatistics();

  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  VerifyTestData();

  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
  ASSERT_EQ(CompactionServiceJobStatus::kSuccess,
            my_cs->GetFinalCompactionServiceJobStatus());

  // make sure the compaction statistics is only recorded on the remote side
  ASSERT_GE(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES), 1);
  ASSERT_GE(compactor_statistics->getTickerCount(COMPACT_READ_BYTES), 1);
  ASSERT_EQ(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES), 0);
  // even with remote compaction, primary host still needs to read SST files to
  // `verify_table()`.
  ASSERT_GE(primary_statistics->getTickerCount(COMPACT_READ_BYTES), 1);
  // all the compaction write happens on the remote side
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES));
  ASSERT_GE(primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 1);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_READ_BYTES),
            primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES));
  // compactor is already the remote side, which doesn't have remote
  ASSERT_EQ(compactor_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 0);
  ASSERT_EQ(compactor_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            0);

  // Test failed compaction
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::CompactWithoutInstallation::End", [&](void* status) {
        // override job status
        auto s = static_cast<Status*>(status);
        *s = Status::Aborted("MyTestCompactionService failed to compact!");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s;
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      s = Put(Key(key_id), "value_new" + std::to_string(key_id));
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

  // Test re-open and successful unique id verification
  std::atomic_int verify_passed{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::PassedVerifyUniqueId", [&](void* arg) {
        // override job status
        auto id = static_cast<UniqueId64x2*>(arg);
        assert(*id != kNullUniqueId64x2);
        verify_passed++;
      });
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "cf_1", "cf_2", "cf_3"},
                           options);
  ASSERT_GT(verify_passed, 0);
  CompactionServiceResult result;
  my_cs->GetResult(&result);
  if (s.IsAborted()) {
    ASSERT_NOK(result.status);
  } else {
    ASSERT_OK(result.status);
  }
  ASSERT_TRUE(result.stats.is_remote_compaction);
  Close();
}

TEST_F(CompactionServiceTest, ManualCompaction) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

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

  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_TRUE(result.stats.is_remote_compaction);
}

TEST_F(CompactionServiceTest, CancelCompactionOnRemoteSide) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  // Test cancel compaction at the beginning
  my_cs->SetCanceled(true);
  auto s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
  // compaction number is not increased
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num);
  VerifyTestData();

  // Test cancel compaction in progress
  ReopenWithCompactionService(&options);
  GenerateTestData();
  my_cs = GetCompactionService();
  my_cs->SetCanceled(false);

  std::atomic_bool cancel_issued{false};
  SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():Inprogress",
                                        [&](void* /*arg*/) {
                                          cancel_issued = true;
                                          my_cs->SetCanceled(true);
                                        });

  SyncPoint::GetInstance()->EnableProcessing();

  s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(cancel_issued);
  // compaction number is not increased
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num);
  VerifyTestData();
}

TEST_F(CompactionServiceTest, FailedToStart) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();

  auto my_cs = GetCompactionService();
  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kFailure);

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
}

TEST_F(CompactionServiceTest, InvalidResult) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();

  auto my_cs = GetCompactionService();
  my_cs->OverrideWaitResult("Invalid Str");

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_FALSE(s.ok());
  ASSERT_EQ(CompactionServiceJobStatus::kFailure,
            my_cs->GetFinalCompactionServiceJobStatus());
}

TEST_F(CompactionServiceTest, SubCompaction) {
  Options options = CurrentOptions();
  options.max_subcompactions = 10;
  options.target_file_size_base = 1 << 10;  // 1KB
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  int compaction_num_before = my_cs->GetCompactionNum();

  auto cro = CompactRangeOptions();
  cro.max_subcompactions = 10;
  Status s = db_->CompactRange(cro, nullptr, nullptr);
  ASSERT_OK(s);
  VerifyTestData();
  int compaction_num = my_cs->GetCompactionNum() - compaction_num_before;
  // make sure there's sub-compaction by checking the compaction number
  ASSERT_GE(compaction_num, 2);
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
  std::unique_ptr<CompactionFilter> delete_comp_filter(
      new PartialDeleteCompactionFilter());
  options.compaction_filter = delete_comp_filter.get();
  ReopenWithCompactionService(&options);
  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i > 5 && i <= 105) {
      ASSERT_EQ(result, "NOT_FOUND");
    } else if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
}

TEST_F(CompactionServiceTest, Snapshot) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put(Key(1), "value1"));
  ASSERT_OK(Put(Key(2), "value1"));
  const Snapshot* s1 = db_->GetSnapshot();
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(1), "value2"));
  ASSERT_OK(Put(Key(3), "value2"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
  ASSERT_EQ("value1", Get(Key(1), s1));
  ASSERT_EQ("value2", Get(Key(1)));
  db_->ReleaseSnapshot(s1);
}

TEST_F(CompactionServiceTest, ConcurrentCompaction) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;
  options.max_background_jobs = 20;
  ReopenWithCompactionService(&options);
  GenerateTestData(true);

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);

  std::vector<std::thread> threads;
  for (const auto& file : meta.levels[1].files) {
    threads.emplace_back([&]() {
      std::string fname = file.db_path + "/" + file.name;
      ASSERT_OK(db_->CompactFiles(CompactionOptions(), {fname}, 2));
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify result
  VerifyTestData();
  auto my_cs = GetCompactionService();
  ASSERT_EQ(my_cs->GetCompactionNum(), 10);
  ASSERT_EQ(FilesPerLevel(), "0,0,10");
}

TEST_F(CompactionServiceTest, CompactionInfo) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  auto my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_GE(comp_num, 1);

  CompactionServiceJobInfo info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(dbname_, info.db_name);
  std::string db_id, db_session_id;
  ASSERT_OK(db_->GetDbIdentity(db_id));
  ASSERT_EQ(db_id, info.db_id);
  ASSERT_OK(db_->GetDbSessionId(db_session_id));
  ASSERT_EQ(db_session_id, info.db_session_id);
  ASSERT_EQ(Env::LOW, info.priority);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(dbname_, info.db_name);
  ASSERT_EQ(db_id, info.db_id);
  ASSERT_EQ(db_session_id, info.db_session_id);
  ASSERT_EQ(Env::LOW, info.priority);

  // Test priority USER
  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  SstFileMetaData file = meta.levels[1].files[0];
  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {file.db_path + "/" + file.name}, 2));
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(Env::USER, info.priority);
  ASSERT_EQ(CompactionReason::kManualCompaction, info.compaction_reason);
  ASSERT_EQ(true, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(Env::USER, info.priority);
  ASSERT_EQ(CompactionReason::kManualCompaction, info.compaction_reason);
  ASSERT_EQ(true, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);

  // Test priority BOTTOM
  env_->SetBackgroundThreads(1, Env::BOTTOM);
  // This will set bottommost_level = true but is_full_compaction = false
  options.num_levels = 2;
  ReopenWithCompactionService(&options);
  my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(CompactionReason::kLevelL0FilesNum, info.compaction_reason);
  ASSERT_EQ(false, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  ASSERT_EQ(Env::BOTTOM, info.priority);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(Env::BOTTOM, info.priority);
  ASSERT_EQ(CompactionReason::kLevelL0FilesNum, info.compaction_reason);
  ASSERT_EQ(false, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);

  // Test Non-Bottommost Level
  options.num_levels = 4;
  ReopenWithCompactionService(&options);
  my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());

  for (int i = 0; i < options.level0_file_num_compaction_trigger; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value_new_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(false, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(false, info.bottommost_level);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(false, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(false, info.bottommost_level);

  // Test Full Compaction + Bottommost Level
  options.num_levels = 6;
  ReopenWithCompactionService(&options);
  my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value_new_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(true, info.is_manual_compaction);
  ASSERT_EQ(true, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  ASSERT_EQ(CompactionReason::kManualCompaction, info.compaction_reason);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(true, info.is_manual_compaction);
  ASSERT_EQ(true, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  ASSERT_EQ(CompactionReason::kManualCompaction, info.compaction_reason);
}

TEST_F(CompactionServiceTest, FallbackLocalAuto) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  auto my_cs = GetCompactionService();
  Statistics* compactor_statistics = GetCompactorStatistics();
  Statistics* primary_statistics = GetPrimaryStatistics();
  uint64_t compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  uint64_t primary_write_bytes =
      primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kUseLocal);

  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  VerifyTestData();

  ASSERT_EQ(my_cs->GetCompactionNum(), 0);

  // make sure the compaction statistics is only recorded on the local side
  ASSERT_EQ(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 0);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES), 0);
}

TEST_F(CompactionServiceTest, FallbackLocalManual) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  Statistics* compactor_statistics = GetCompactorStatistics();
  Statistics* primary_statistics = GetPrimaryStatistics();
  uint64_t compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  uint64_t primary_write_bytes =
      primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  // re-enable remote compaction
  my_cs->ResetOverride();
  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  // make sure the compaction statistics is only recorded on the remote side
  ASSERT_GT(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES));
  ASSERT_EQ(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);

  // return run local again with API WaitForComplete
  my_cs->OverrideWaitStatus(CompactionServiceJobStatus::kUseLocal);
  start_str = Key(120);
  start = start_str;
  comp_num = my_cs->GetCompactionNum();
  compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  primary_write_bytes = primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, nullptr));
  ASSERT_EQ(my_cs->GetCompactionNum(),
            comp_num);  // no remote compaction is run
  // make sure the compaction statistics is only recorded on the local side
  ASSERT_EQ(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_write_bytes);

  // verify result after 2 manual compactions
  VerifyTestData();
}

TEST_F(CompactionServiceTest, RemoteEventListener) {
  class RemoteEventListenerTest : public EventListener {
   public:
    const char* Name() const override { return "RemoteEventListenerTest"; }

    void OnSubcompactionBegin(const SubcompactionJobInfo& info) override {
      auto result = on_going_compactions.emplace(info.job_id);
      ASSERT_TRUE(result.second);  // make sure there's no duplication
      compaction_num++;
      EventListener::OnSubcompactionBegin(info);
    }
    void OnSubcompactionCompleted(const SubcompactionJobInfo& info) override {
      auto num = on_going_compactions.erase(info.job_id);
      ASSERT_TRUE(num == 1);  // make sure the compaction id exists
      EventListener::OnSubcompactionCompleted(info);
    }
    void OnTableFileCreated(const TableFileCreationInfo& info) override {
      ASSERT_EQ(on_going_compactions.count(info.job_id), 1);
      file_created++;
      EventListener::OnTableFileCreated(info);
    }
    void OnTableFileCreationStarted(
        const TableFileCreationBriefInfo& info) override {
      ASSERT_EQ(on_going_compactions.count(info.job_id), 1);
      file_creation_started++;
      EventListener::OnTableFileCreationStarted(info);
    }

    bool ShouldBeNotifiedOnFileIO() override {
      file_io_notified++;
      return EventListener::ShouldBeNotifiedOnFileIO();
    }

    std::atomic_uint64_t file_io_notified{0};
    std::atomic_uint64_t file_creation_started{0};
    std::atomic_uint64_t file_created{0};

    std::set<int> on_going_compactions;  // store the job_id
    std::atomic_uint64_t compaction_num{0};
  };

  auto listener = new RemoteEventListenerTest();
  remote_listeners.emplace_back(listener);

  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // check the events are triggered
  ASSERT_TRUE(listener->file_io_notified > 0);
  ASSERT_TRUE(listener->file_creation_started > 0);
  ASSERT_TRUE(listener->file_created > 0);
  ASSERT_TRUE(listener->compaction_num > 0);
  ASSERT_TRUE(listener->on_going_compactions.empty());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
}

TEST_F(CompactionServiceTest, TablePropertiesCollector) {
  const static std::string kUserPropertyName = "TestCount";

  class TablePropertiesCollectorTest : public TablePropertiesCollector {
   public:
    Status Finish(UserCollectedProperties* properties) override {
      *properties = UserCollectedProperties{
          {kUserPropertyName, std::to_string(count_)},
      };
      return Status::OK();
    }

    UserCollectedProperties GetReadableProperties() const override {
      return UserCollectedProperties();
    }

    const char* Name() const override { return "TablePropertiesCollectorTest"; }

    Status AddUserKey(const Slice& /*user_key*/, const Slice& /*value*/,
                      EntryType /*type*/, SequenceNumber /*seq*/,
                      uint64_t /*file_size*/) override {
      count_++;
      return Status::OK();
    }

   private:
    uint32_t count_ = 0;
  };

  class TablePropertiesCollectorFactoryTest
      : public TablePropertiesCollectorFactory {
   public:
    TablePropertiesCollector* CreateTablePropertiesCollector(
        TablePropertiesCollectorFactory::Context /*context*/) override {
      return new TablePropertiesCollectorTest();
    }

    const char* Name() const override {
      return "TablePropertiesCollectorFactoryTest";
    }
  };

  auto factory = new TablePropertiesCollectorFactoryTest();
  remote_table_properties_collector_factories.emplace_back(factory);

  const int kNumSst = 3;
  const int kLevel0Trigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kLevel0Trigger;
  ReopenWithCompactionService(&options);

  // generate a few SSTs locally which should not have user property
  for (int i = 0; i < kNumSst; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }

  TablePropertiesCollection fname_to_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&fname_to_props));
  for (const auto& file_props : fname_to_props) {
    auto properties = file_props.second->user_collected_properties;
    auto it = properties.find(kUserPropertyName);
    ASSERT_EQ(it, properties.end());
  }

  // trigger compaction
  for (int i = kNumSst; i < kLevel0Trigger; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_OK(db_->GetPropertiesOfAllTables(&fname_to_props));

  bool has_user_property = false;
  for (const auto& file_props : fname_to_props) {
    auto properties = file_props.second->user_collected_properties;
    auto it = properties.find(kUserPropertyName);
    if (it != properties.end()) {
      has_user_property = true;
      ASSERT_GT(std::stoi(it->second), 0);
    }
  }
  ASSERT_TRUE(has_user_property);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
