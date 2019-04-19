//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl_secondary.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "util/fault_injection_test_env.h"
#include "util/sync_point.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
class DBSecondaryTest : public DBTestBase {
 public:
  DBSecondaryTest()
      : DBTestBase("/db_secondary_test"),
        secondary_path_(),
        handles_secondary_(),
        db_secondary_(nullptr) {
    secondary_path_ =
        test::PerThreadDBPath(env_, "/db_secondary_test_secondary");
  }

  ~DBSecondaryTest() override {
    CloseSecondary();
    if (getenv("KEEP_DB") != nullptr) {
      fprintf(stdout, "Secondary DB is still at %s\n", secondary_path_.c_str());
    } else {
      Options options;
      options.env = env_;
      EXPECT_OK(DestroyDB(secondary_path_, options));
    }
  }

 protected:
  Status ReopenAsSecondary(const Options& options) {
    return DB::OpenAsSecondary(options, dbname_, secondary_path_, &db_);
  }

  void OpenSecondary(const Options& options);

  void OpenSecondaryWithColumnFamilies(
      const std::vector<std::string>& column_families, const Options& options);

  void CloseSecondary() {
    for (auto h : handles_secondary_) {
      db_secondary_->DestroyColumnFamilyHandle(h);
    }
    handles_secondary_.clear();
    delete db_secondary_;
    db_secondary_ = nullptr;
  }

  DBImplSecondary* db_secondary_full() {
    return static_cast<DBImplSecondary*>(db_secondary_);
  }

  void CheckFileTypeCounts(const std::string& dir, int expected_log,
                           int expected_sst, int expected_manifest) const;

  std::string secondary_path_;
  std::vector<ColumnFamilyHandle*> handles_secondary_;
  DB* db_secondary_;
};

void DBSecondaryTest::OpenSecondary(const Options& options) {
  Status s =
      DB::OpenAsSecondary(options, dbname_, secondary_path_, &db_secondary_);
  ASSERT_OK(s);
}

void DBSecondaryTest::OpenSecondaryWithColumnFamilies(
    const std::vector<std::string>& column_families, const Options& options) {
  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  for (const auto& cf_name : column_families) {
    cf_descs.emplace_back(cf_name, options);
  }
  Status s = DB::OpenAsSecondary(options, dbname_, secondary_path_, cf_descs,
                                 &handles_secondary_, &db_secondary_);
  ASSERT_OK(s);
}

void DBSecondaryTest::CheckFileTypeCounts(const std::string& dir,
                                          int expected_log, int expected_sst,
                                          int expected_manifest) const {
  std::vector<std::string> filenames;
  env_->GetChildren(dir, &filenames);

  int log_cnt = 0, sst_cnt = 0, manifest_cnt = 0;
  for (auto file : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(file, &number, &type)) {
      log_cnt += (type == kLogFile);
      sst_cnt += (type == kTableFile);
      manifest_cnt += (type == kDescriptorFile);
    }
  }
  ASSERT_EQ(expected_log, log_cnt);
  ASSERT_EQ(expected_sst, sst_cnt);
  ASSERT_EQ(expected_manifest, manifest_cnt);
}

TEST_F(DBSecondaryTest, ReopenAsSecondary) {
  Options options;
  options.env = env_;
  Reopen(options);
  ASSERT_OK(Put("foo", "foo_value"));
  ASSERT_OK(Put("bar", "bar_value"));
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  Close();

  ASSERT_OK(ReopenAsSecondary(options));
  ASSERT_EQ("foo_value", Get("foo"));
  ASSERT_EQ("bar_value", Get("bar"));
  ReadOptions ropts;
  ropts.verify_checksums = true;
  auto db1 = static_cast<DBImplSecondary*>(db_);
  ASSERT_NE(nullptr, db1);
  Iterator* iter = db1->NewIterator(ropts);
  ASSERT_NE(nullptr, iter);
  size_t count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (0 == count) {
      ASSERT_EQ("bar", iter->key().ToString());
      ASSERT_EQ("bar_value", iter->value().ToString());
    } else if (1 == count) {
      ASSERT_EQ("foo", iter->key().ToString());
      ASSERT_EQ("foo_value", iter->value().ToString());
    }
    ++count;
  }
  delete iter;
  ASSERT_EQ(2, count);
}

TEST_F(DBSecondaryTest, OpenAsSecondary) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ReadOptions ropts;
  ropts.verify_checksums = true;
  const auto verify_db_func = [&](const std::string& foo_val,
                                  const std::string& bar_val) {
    std::string value;
    ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
    ASSERT_EQ(foo_val, value);
    ASSERT_OK(db_secondary_->Get(ropts, "bar", &value));
    ASSERT_EQ(bar_val, value);
    Iterator* iter = db_secondary_->NewIterator(ropts);
    ASSERT_NE(nullptr, iter);
    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ(foo_val, iter->value().ToString());
    iter->Seek("bar");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bar", iter->key().ToString());
    ASSERT_EQ(bar_val, iter->value().ToString());
    size_t count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ++count;
    }
    ASSERT_EQ(2, count);
    delete iter;
  };

  verify_db_func("foo_value2", "bar_value2");

  ASSERT_OK(Put("foo", "new_foo_value"));
  ASSERT_OK(Put("bar", "new_bar_value"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db_func("new_foo_value", "new_bar_value");
}

TEST_F(DBSecondaryTest, OpenAsSecondaryWALTailing) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
  }
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  ReadOptions ropts;
  ropts.verify_checksums = true;
  const auto verify_db_func = [&](const std::string& foo_val,
                                  const std::string& bar_val) {
    std::string value;
    ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
    ASSERT_EQ(foo_val, value);
    ASSERT_OK(db_secondary_->Get(ropts, "bar", &value));
    ASSERT_EQ(bar_val, value);
    Iterator* iter = db_secondary_->NewIterator(ropts);
    ASSERT_NE(nullptr, iter);
    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ(foo_val, iter->value().ToString());
    iter->Seek("bar");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bar", iter->key().ToString());
    ASSERT_EQ(bar_val, iter->value().ToString());
    size_t count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ++count;
    }
    ASSERT_EQ(2, count);
    delete iter;
  };

  verify_db_func("foo_value2", "bar_value2");
}

TEST_F(DBSecondaryTest, OpenWithNonExistColumnFamily) {
  Options options;
  options.env = env_;
  CreateAndReopenWithCF({"pikachu"}, options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options1);
  cf_descs.emplace_back("pikachu", options1);
  cf_descs.emplace_back("eevee", options1);
  Status s = DB::OpenAsSecondary(options1, dbname_, secondary_path_, cf_descs,
                                 &handles_secondary_, &db_secondary_);
  ASSERT_NOK(s);
}

TEST_F(DBSecondaryTest, OpenWithSubsetOfColumnFamilies) {
  Options options;
  options.env = env_;
  CreateAndReopenWithCF({"pikachu"}, options);
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);
  ASSERT_EQ(0, handles_secondary_.size());
  ASSERT_NE(nullptr, db_secondary_);

  ASSERT_OK(Put(0 /*cf*/, "foo", "foo_value"));
  ASSERT_OK(Put(1 /*cf*/, "foo", "foo_value"));
  ASSERT_OK(Flush(0 /*cf*/));
  ASSERT_OK(Flush(1 /*cf*/));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
  ASSERT_EQ("foo_value", value);
}

TEST_F(DBSecondaryTest, SwitchToNewManifestDuringOpen) {
  Options options;
  options.env = env_;
  Reopen(options);
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency(
      {{"ReactiveVersionSet::MaybeSwitchManifest:AfterGetCurrentManifestPath:0",
        "VersionSet::ProcessManifestWrites:BeforeNewManifest"},
       {"VersionSet::ProcessManifestWrites:AfterNewManifest",
        "ReactiveVersionSet::MaybeSwitchManifest:AfterGetCurrentManifestPath:"
        "1"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // Make sure db calls RecoverLogFiles so as to trigger a manifest write,
  // which causes the db to switch to a new MANIFEST upon start.
  port::Thread ro_db_thread([&]() {
    Options options1;
    options1.env = env_;
    options1.max_open_files = -1;
    OpenSecondary(options1);
    CloseSecondary();
  });
  Reopen(options);
  ro_db_thread.join();
}

TEST_F(DBSecondaryTest, MissingTableFileDuringOpen) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i != options.level0_file_num_compaction_trigger; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  ASSERT_OK(db_secondary_->Get(ropts, "bar", &value));
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  Iterator* iter = db_secondary_->NewIterator(ropts);
  ASSERT_NE(nullptr, iter);
  iter->Seek("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar", iter->key().ToString());
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            iter->value().ToString());
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            iter->value().ToString());
  size_t count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ++count;
  }
  ASSERT_EQ(2, count);
  delete iter;
}

TEST_F(DBSecondaryTest, MissingTableFile) {
  int table_files_not_exist = 0;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "ReactiveVersionSet::ReadAndApply:AfterLoadTableHandlers",
      [&](void* arg) {
        Status s = *reinterpret_cast<Status*>(arg);
        if (s.IsPathNotFound()) {
          ++table_files_not_exist;
        } else if (!s.ok()) {
          assert(false);  // Should not reach here
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  for (int i = 0; i != options.level0_file_num_compaction_trigger; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_NE(nullptr, db_secondary_full());
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_NOK(db_secondary_->Get(ropts, "foo", &value));
  ASSERT_NOK(db_secondary_->Get(ropts, "bar", &value));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_EQ(options.level0_file_num_compaction_trigger, table_files_not_exist);
  ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  ASSERT_OK(db_secondary_->Get(ropts, "bar", &value));
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  Iterator* iter = db_secondary_->NewIterator(ropts);
  ASSERT_NE(nullptr, iter);
  iter->Seek("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar", iter->key().ToString());
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            iter->value().ToString());
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            iter->value().ToString());
  size_t count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ++count;
  }
  ASSERT_EQ(2, count);
  delete iter;
}

TEST_F(DBSecondaryTest, PrimaryDropColumnFamily) {
  Options options;
  options.env = env_;
  const std::string kCfName1 = "pikachu";
  CreateAndReopenWithCF({kCfName1}, options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({kCfName1}, options1);
  ASSERT_EQ(2, handles_secondary_.size());

  ASSERT_OK(Put(1 /*cf*/, "foo", "foo_val_1"));
  ASSERT_OK(Flush(1 /*cf*/));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_OK(db_secondary_->Get(ropts, handles_secondary_[1], "foo", &value));
  ASSERT_EQ("foo_val_1", value);

  ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
  Close();
  CheckFileTypeCounts(dbname_, 1, 0, 1);
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  value.clear();
  ASSERT_OK(db_secondary_->Get(ropts, handles_secondary_[1], "foo", &value));
  ASSERT_EQ("foo_val_1", value);
}

TEST_F(DBSecondaryTest, SwitchManifest) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  const int kNumFiles = options.level0_file_num_compaction_trigger - 1;
  // Keep it smaller than 10 so that key0, key1, ..., key9 are sorted as 0, 1,
  // ..., 9.
  const int kNumKeys = 10;
  // Create two sst
  for (int i = 0; i != kNumFiles; ++i) {
    for (int j = 0; j != kNumKeys; ++j) {
      ASSERT_OK(Put("key" + std::to_string(j), "value_" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
  }

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  const auto& range_scan_db = [&]() {
    ReadOptions tmp_ropts;
    tmp_ropts.total_order_seek = true;
    tmp_ropts.verify_checksums = true;
    std::unique_ptr<Iterator> iter(db_secondary_->NewIterator(tmp_ropts));
    int cnt = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++cnt) {
      ASSERT_EQ("key" + std::to_string(cnt), iter->key().ToString());
      ASSERT_EQ("value_" + std::to_string(kNumFiles - 1),
                iter->value().ToString());
    }
  };

  range_scan_db();

  // While secondary instance still keeps old MANIFEST open, we close primary,
  // restart primary, performs full compaction, close again, restart again so
  // that next time secondary tries to catch up with primary, the secondary
  // will skip the MANIFEST in middle.
  Reopen(options);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  Reopen(options);
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  range_scan_db();
}
#endif  //! ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
