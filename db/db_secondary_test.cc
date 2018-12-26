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
  DBSecondaryTest() : DBTestBase("/db_secondary_test"), secondary_dbname_() {
    secondary_dbname_ =
        test::PerThreadDBPath(env_, "/db_secondary_test_secondary");
  }

  ~DBSecondaryTest() {
    if (getenv("KEEP_DB") != nullptr) {
      fprintf(stdout, "Secondary DB is still at %s\n",
              secondary_dbname_.c_str());
    } else {
      Options options;
      options.env = env_;
      EXPECT_OK(DestroyDB(secondary_dbname_, options));
    }
  }

 protected:
  Status ReopenAsSecondary(const Options& options) {
    return DB::OpenAsSecondary(options, dbname_, secondary_dbname_, &db_);
  }

  std::string secondary_dbname_;
};

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
  Close();
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
  DB* db_secondary = nullptr;
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  Status s =
      DB::OpenAsSecondary(options1, dbname_, secondary_dbname_, &db_secondary);
  ASSERT_OK(s);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ReadOptions ropts;
  ropts.verify_checksums = true;
  const auto verify_db_func = [&](const std::string& foo_val,
                                  const std::string& bar_val) {
    std::string value;
    ASSERT_OK(db_secondary->Get(ropts, "foo", &value));
    ASSERT_EQ(foo_val, value);
    ASSERT_OK(db_secondary->Get(ropts, "bar", &value));
    ASSERT_EQ(bar_val, value);
    Iterator* iter = db_secondary->NewIterator(ropts);
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

  ASSERT_OK(
      static_cast<DBImplSecondary*>(db_secondary)->TryCatchUpWithPrimary());
  verify_db_func("new_foo_value", "new_bar_value");

  delete db_secondary;
  Close();
}

TEST_F(DBSecondaryTest, SwitchToNewManifestDuringOpen) {
  Options options;
  options.env = env_;
  Reopen(options);
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::MaybeSwitchManifest:AfterGetCurrentManifestPath:0",
        "VersionSet::ProcessManifestWrites:BeforeNewManifest"},
       {"VersionSet::ProcessManifestWrites:AfterNewManifest",
        "VersionSet::MaybeSwitchManifest:AfterGetCurrentManifestPath:1"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // Make sure db calls RecoverLogFiles so as to trigger a manifest write,
  // which causes the db to switch to a new MANIFEST upon start.
  port::Thread ro_db_thread([&]() {
    DB* db_secondary = nullptr;
    Options options1;
    options1.env = env_;
    options1.max_open_files = -1;
    Status s = DB::OpenAsSecondary(options1, dbname_, secondary_dbname_,
                                   &db_secondary);
    ASSERT_OK(s);
    delete db_secondary;
  });
  Reopen(options);
  ro_db_thread.join();
  Close();
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
  DB* db1 = nullptr;
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  Status s = DB::OpenAsSecondary(options1, dbname_, secondary_dbname_, &db1);
  ASSERT_OK(s);
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_OK(db1->Get(ropts, "foo", &value));
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  ASSERT_OK(db1->Get(ropts, "bar", &value));
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  Iterator* iter = db1->NewIterator(ropts);
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
  delete db1;
  Close();
}

TEST_F(DBSecondaryTest, MissingTableFile) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);

  DB* db1 = nullptr;
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  Status s = DB::OpenAsSecondary(options1, dbname_, secondary_dbname_, &db1);
  ASSERT_OK(s);

  for (int i = 0; i != options.level0_file_num_compaction_trigger; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  auto db_secondary = static_cast<DBImplSecondary*>(db1);
  ASSERT_NE(nullptr, db_secondary);
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_NOK(db_secondary->Get(ropts, "foo", &value));
  ASSERT_NOK(db_secondary->Get(ropts, "bar", &value));

  ASSERT_OK(db_secondary->TryCatchUpWithPrimary());
  ASSERT_OK(db_secondary->Get(ropts, "foo", &value));
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  ASSERT_OK(db_secondary->Get(ropts, "bar", &value));
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  Iterator* iter = db1->NewIterator(ropts);
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
  delete db1;
  Close();
}
#endif  //! ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
