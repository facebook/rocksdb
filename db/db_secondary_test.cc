//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl/db_impl_secondary.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE
class DBSecondaryTest : public DBTestBase {
 public:
  DBSecondaryTest()
      : DBTestBase("/db_secondary_test", /*env_do_fsync=*/true),
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

  Status TryOpenSecondary(const Options& options);

  void OpenSecondaryWithColumnFamilies(
      const std::vector<std::string>& column_families, const Options& options);

  void CloseSecondary() {
    for (auto h : handles_secondary_) {
      ASSERT_OK(db_secondary_->DestroyColumnFamilyHandle(h));
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
  ASSERT_OK(TryOpenSecondary(options));
}

Status DBSecondaryTest::TryOpenSecondary(const Options& options) {
  Status s =
      DB::OpenAsSecondary(options, dbname_, secondary_path_, &db_secondary_);
  return s;
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
  ASSERT_OK(env_->GetChildren(dir, &filenames));

  int log_cnt = 0, sst_cnt = 0, manifest_cnt = 0;
  for (auto file : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(file, &number, &type)) {
      log_cnt += (type == kWalFile);
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

TEST_F(DBSecondaryTest, SimpleInternalCompaction) {
  Options options;
  options.env = env_;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  CompactionServiceInput input;

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  for (auto& file : meta.levels[0].files) {
    ASSERT_EQ(0, meta.levels[0].level);
    input.input_files.push_back(file.name);
  }
  ASSERT_EQ(input.input_files.size(), 3);

  input.output_level = 1;
  Close();

  options.max_open_files = -1;
  OpenSecondary(options);
  auto cfh = db_secondary_->DefaultColumnFamily();

  CompactionServiceResult result;
  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(cfh, input,
                                                                 &result));

  ASSERT_EQ(result.output_files.size(), 1);
  InternalKey smallest, largest;
  smallest.DecodeFrom(result.output_files[0].smallest_internal_key);
  largest.DecodeFrom(result.output_files[0].largest_internal_key);
  ASSERT_EQ(smallest.user_key().ToString(), "bar");
  ASSERT_EQ(largest.user_key().ToString(), "foo");
  ASSERT_EQ(result.output_level, 1);
  ASSERT_EQ(result.output_path, this->secondary_path_);
  ASSERT_EQ(result.num_output_records, 2);
  ASSERT_GT(result.bytes_written, 0);
  ASSERT_OK(result.status);
}

TEST_F(DBSecondaryTest, InternalCompactionMultiLevels) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Reopen(options);
  const int kRangeL2 = 10;
  const int kRangeL1 = 30;
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i * kRangeL2), "value" + ToString(i)));
    ASSERT_OK(Put(Key((i + 1) * kRangeL2 - 1), "value" + ToString(i)));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Put(Key(i * kRangeL1), "value" + ToString(i)));
    ASSERT_OK(Put(Key((i + 1) * kRangeL1 - 1), "value" + ToString(i)));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(1);
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put(Key(i * 30), "value" + ToString(i)));
    ASSERT_OK(Put(Key(i * 30 + 50), "value" + ToString(i)));
    ASSERT_OK(Flush());
  }

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);

  // pick 2 files on level 0 for compaction, which has 3 overlap files on L1
  CompactionServiceInput input1;
  input1.input_files.push_back(meta.levels[0].files[2].name);
  input1.input_files.push_back(meta.levels[0].files[3].name);
  input1.input_files.push_back(meta.levels[1].files[0].name);
  input1.input_files.push_back(meta.levels[1].files[1].name);
  input1.input_files.push_back(meta.levels[1].files[2].name);

  input1.output_level = 1;

  options.max_open_files = -1;
  Close();

  OpenSecondary(options);
  auto cfh = db_secondary_->DefaultColumnFamily();
  CompactionServiceResult result;
  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(cfh, input1,
                                                                 &result));
  ASSERT_OK(result.status);

  // pick 2 files on level 1 for compaction, which has 6 overlap files on L2
  CompactionServiceInput input2;
  input2.input_files.push_back(meta.levels[1].files[1].name);
  input2.input_files.push_back(meta.levels[1].files[2].name);
  for (int i = 3; i < 9; i++) {
    input2.input_files.push_back(meta.levels[2].files[i].name);
  }

  input2.output_level = 2;
  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(cfh, input2,
                                                                 &result));
  ASSERT_OK(result.status);

  CloseSecondary();

  // delete all l2 files, without update manifest
  for (auto& file : meta.levels[2].files) {
    ASSERT_OK(env_->DeleteFile(dbname_ + file.name));
  }
  OpenSecondary(options);
  cfh = db_secondary_->DefaultColumnFamily();
  Status s = db_secondary_full()->TEST_CompactWithoutInstallation(cfh, input2,
                                                                  &result);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(result.status);

  // TODO: L0 -> L1 compaction should success, currently version is not built
  // if files is missing.
  //  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(cfh,
  //  input1, &result));
}

TEST_F(DBSecondaryTest, InternalCompactionCompactedFiles) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  CompactionServiceInput input;

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  for (auto& file : meta.levels[0].files) {
    ASSERT_EQ(0, meta.levels[0].level);
    input.input_files.push_back(file.name);
  }
  ASSERT_EQ(input.input_files.size(), 3);

  input.output_level = 1;

  // trigger compaction to delete the files for secondary instance compaction
  ASSERT_OK(Put("foo", "foo_value" + std::to_string(3)));
  ASSERT_OK(Put("bar", "bar_value" + std::to_string(3)));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  Close();

  options.max_open_files = -1;
  OpenSecondary(options);
  auto cfh = db_secondary_->DefaultColumnFamily();

  CompactionServiceResult result;
  Status s =
      db_secondary_full()->TEST_CompactWithoutInstallation(cfh, input, &result);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(result.status);
}

TEST_F(DBSecondaryTest, InternalCompactionMissingFiles) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  CompactionServiceInput input;

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  for (auto& file : meta.levels[0].files) {
    ASSERT_EQ(0, meta.levels[0].level);
    input.input_files.push_back(file.name);
  }
  ASSERT_EQ(input.input_files.size(), 3);

  input.output_level = 1;

  Close();

  ASSERT_OK(env_->DeleteFile(dbname_ + input.input_files[0]));

  options.max_open_files = -1;
  OpenSecondary(options);
  auto cfh = db_secondary_->DefaultColumnFamily();

  CompactionServiceResult result;
  Status s =
      db_secondary_full()->TEST_CompactWithoutInstallation(cfh, input, &result);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(result.status);

  input.input_files.erase(input.input_files.begin());

  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(cfh, input,
                                                                 &result));
  ASSERT_OK(result.status);
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

namespace {
class TraceFileEnv : public EnvWrapper {
 public:
  explicit TraceFileEnv(Env* _target) : EnvWrapper(_target) {}
  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& env_options) override {
    class TracedRandomAccessFile : public RandomAccessFile {
     public:
      TracedRandomAccessFile(std::unique_ptr<RandomAccessFile>&& target,
                             std::atomic<int>& counter)
          : target_(std::move(target)), files_closed_(counter) {}
      ~TracedRandomAccessFile() override {
        files_closed_.fetch_add(1, std::memory_order_relaxed);
      }
      Status Read(uint64_t offset, size_t n, Slice* result,
                  char* scratch) const override {
        return target_->Read(offset, n, result, scratch);
      }

     private:
      std::unique_ptr<RandomAccessFile> target_;
      std::atomic<int>& files_closed_;
    };
    Status s = target()->NewRandomAccessFile(f, r, env_options);
    if (s.ok()) {
      r->reset(new TracedRandomAccessFile(std::move(*r), files_closed_));
    }
    return s;
  }

  int files_closed() const {
    return files_closed_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<int> files_closed_{0};
};
}  // namespace

TEST_F(DBSecondaryTest, SecondaryCloseFiles) {
  Options options;
  options.env = env_;
  options.max_open_files = 1;
  options.disable_auto_compactions = true;
  Reopen(options);
  Options options1;
  std::unique_ptr<Env> traced_env(new TraceFileEnv(env_));
  options1.env = traced_env.get();
  OpenSecondary(options1);

  static const auto verify_db = [&]() {
    std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));
    std::unique_ptr<Iterator> iter2(db_secondary_->NewIterator(ReadOptions()));
    for (iter1->SeekToFirst(), iter2->SeekToFirst();
         iter1->Valid() && iter2->Valid(); iter1->Next(), iter2->Next()) {
      ASSERT_EQ(iter1->key(), iter2->key());
      ASSERT_EQ(iter1->value(), iter2->value());
    }
    ASSERT_FALSE(iter1->Valid());
    ASSERT_FALSE(iter2->Valid());
  };

  ASSERT_OK(Put("a", "value"));
  ASSERT_OK(Put("c", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db();

  ASSERT_OK(Put("b", "value"));
  ASSERT_OK(Put("d", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db();

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_EQ(2, static_cast<TraceFileEnv*>(traced_env.get())->files_closed());

  Status s = db_secondary_->SetDBOptions({{"max_open_files", "-1"}});
  ASSERT_TRUE(s.IsNotSupported());
  CloseSecondary();
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

  ASSERT_OK(Put("foo", "new_foo_value"));
  ASSERT_OK(Put("bar", "new_bar_value"));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db_func("new_foo_value", "new_bar_value");

  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "new_foo_value_1"));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db_func("new_foo_value_1", "new_bar_value");
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

TEST_F(DBSecondaryTest, SwitchManifestTwice) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Reopen(options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  ASSERT_OK(Put("0", "value0"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  std::string value;
  ReadOptions ropts;
  ropts.verify_checksums = true;
  ASSERT_OK(db_secondary_->Get(ropts, "0", &value));
  ASSERT_EQ("value0", value);

  Reopen(options);
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
  Reopen(options);
  ASSERT_OK(Put("0", "value1"));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  ASSERT_OK(db_secondary_->Get(ropts, "0", &value));
  ASSERT_EQ("value1", value);
}

TEST_F(DBSecondaryTest, DISABLED_SwitchWAL) {
  const int kNumKeysPerMemtable = 1;
  Options options;
  options.env = env_;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(kNumKeysPerMemtable));
  Reopen(options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  const auto& verify_db = [](DB* db1, DB* db2) {
    ASSERT_NE(nullptr, db1);
    ASSERT_NE(nullptr, db2);
    ReadOptions read_opts;
    read_opts.verify_checksums = true;
    std::unique_ptr<Iterator> it1(db1->NewIterator(read_opts));
    std::unique_ptr<Iterator> it2(db2->NewIterator(read_opts));
    it1->SeekToFirst();
    it2->SeekToFirst();
    for (; it1->Valid() && it2->Valid(); it1->Next(), it2->Next()) {
      ASSERT_EQ(it1->key(), it2->key());
      ASSERT_EQ(it1->value(), it2->value());
    }
    ASSERT_FALSE(it1->Valid());
    ASSERT_FALSE(it2->Valid());

    for (it1->SeekToFirst(); it1->Valid(); it1->Next()) {
      std::string value;
      ASSERT_OK(db2->Get(read_opts, it1->key(), &value));
      ASSERT_EQ(it1->value(), value);
    }
    for (it2->SeekToFirst(); it2->Valid(); it2->Next()) {
      std::string value;
      ASSERT_OK(db1->Get(read_opts, it2->key(), &value));
      ASSERT_EQ(it2->value(), value);
    }
  };
  for (int k = 0; k != 16; ++k) {
    ASSERT_OK(Put("key" + std::to_string(k), "value" + std::to_string(k)));
    ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
    verify_db(dbfull(), db_secondary_);
  }
}

TEST_F(DBSecondaryTest, DISABLED_SwitchWALMultiColumnFamilies) {
  const int kNumKeysPerMemtable = 1;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallFlush:ContextCleanedUp",
        "DBSecondaryTest::SwitchWALMultipleColumnFamilies:BeforeCatchUp"}});
  SyncPoint::GetInstance()->EnableProcessing();
  const std::string kCFName1 = "pikachu";
  Options options;
  options.env = env_;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(kNumKeysPerMemtable));
  CreateAndReopenWithCF({kCFName1}, options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({kCFName1}, options1);
  ASSERT_EQ(2, handles_secondary_.size());

  const auto& verify_db = [](DB* db1,
                             const std::vector<ColumnFamilyHandle*>& handles1,
                             DB* db2,
                             const std::vector<ColumnFamilyHandle*>& handles2) {
    ASSERT_NE(nullptr, db1);
    ASSERT_NE(nullptr, db2);
    ReadOptions read_opts;
    read_opts.verify_checksums = true;
    ASSERT_EQ(handles1.size(), handles2.size());
    for (size_t i = 0; i != handles1.size(); ++i) {
      std::unique_ptr<Iterator> it1(db1->NewIterator(read_opts, handles1[i]));
      std::unique_ptr<Iterator> it2(db2->NewIterator(read_opts, handles2[i]));
      it1->SeekToFirst();
      it2->SeekToFirst();
      for (; it1->Valid() && it2->Valid(); it1->Next(), it2->Next()) {
        ASSERT_EQ(it1->key(), it2->key());
        ASSERT_EQ(it1->value(), it2->value());
      }
      ASSERT_FALSE(it1->Valid());
      ASSERT_FALSE(it2->Valid());

      for (it1->SeekToFirst(); it1->Valid(); it1->Next()) {
        std::string value;
        ASSERT_OK(db2->Get(read_opts, handles2[i], it1->key(), &value));
        ASSERT_EQ(it1->value(), value);
      }
      for (it2->SeekToFirst(); it2->Valid(); it2->Next()) {
        std::string value;
        ASSERT_OK(db1->Get(read_opts, handles1[i], it2->key(), &value));
        ASSERT_EQ(it2->value(), value);
      }
    }
  };
  for (int k = 0; k != 8; ++k) {
    for (int j = 0; j < 2; ++j) {
      ASSERT_OK(Put(0 /*cf*/, "key" + std::to_string(k),
                    "value" + std::to_string(k)));
      ASSERT_OK(Put(1 /*cf*/, "key" + std::to_string(k),
                    "value" + std::to_string(k)));
    }
    TEST_SYNC_POINT(
        "DBSecondaryTest::SwitchWALMultipleColumnFamilies:BeforeCatchUp");
    ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
    verify_db(dbfull(), handles_, db_secondary_, handles_secondary_);
    SyncPoint::GetInstance()->ClearTrace();
  }
}

TEST_F(DBSecondaryTest, CatchUpAfterFlush) {
  const int kNumKeysPerMemtable = 16;
  Options options;
  options.env = env_;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(kNumKeysPerMemtable));
  Reopen(options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  WriteOptions write_opts;
  WriteBatch wb;
  ASSERT_OK(wb.Put("key0", "value0"));
  ASSERT_OK(wb.Put("key1", "value1"));
  ASSERT_OK(dbfull()->Write(write_opts, &wb));
  ReadOptions read_opts;
  std::unique_ptr<Iterator> iter1(db_secondary_->NewIterator(read_opts));
  iter1->Seek("key0");
  ASSERT_FALSE(iter1->Valid());
  iter1->Seek("key1");
  ASSERT_FALSE(iter1->Valid());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  iter1->Seek("key0");
  ASSERT_FALSE(iter1->Valid());
  iter1->Seek("key1");
  ASSERT_FALSE(iter1->Valid());
  ASSERT_OK(iter1->status());
  std::unique_ptr<Iterator> iter2(db_secondary_->NewIterator(read_opts));
  iter2->Seek("key0");
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ("value0", iter2->value());
  iter2->Seek("key1");
  ASSERT_TRUE(iter2->Valid());
  ASSERT_OK(iter2->status());
  ASSERT_EQ("value1", iter2->value());

  {
    WriteBatch wb1;
    ASSERT_OK(wb1.Put("key0", "value01"));
    ASSERT_OK(wb1.Put("key1", "value11"));
    ASSERT_OK(dbfull()->Write(write_opts, &wb1));
  }

  {
    WriteBatch wb2;
    ASSERT_OK(wb2.Put("key0", "new_value0"));
    ASSERT_OK(wb2.Delete("key1"));
    ASSERT_OK(dbfull()->Write(write_opts, &wb2));
  }

  ASSERT_OK(Flush());

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  std::unique_ptr<Iterator> iter3(db_secondary_->NewIterator(read_opts));
  // iter3 should not see value01 and value11 at all.
  iter3->Seek("key0");
  ASSERT_TRUE(iter3->Valid());
  ASSERT_EQ("new_value0", iter3->value());
  iter3->Seek("key1");
  ASSERT_FALSE(iter3->Valid());
  ASSERT_OK(iter3->status());
}

TEST_F(DBSecondaryTest, CheckConsistencyWhenOpen) {
  bool called = false;
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Reopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::CheckConsistency:AfterFirstAttempt", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        called = true;
        auto* s = reinterpret_cast<Status*>(arg);
        ASSERT_NOK(*s);
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::CheckConsistency:AfterGetLiveFilesMetaData",
        "BackgroundCallCompaction:0"},
       {"DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles",
        "DBImpl::CheckConsistency:BeforeGetFileSize"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("a", "value0"));
  ASSERT_OK(Put("c", "value0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "value1"));
  ASSERT_OK(Put("d", "value1"));
  ASSERT_OK(Flush());
  port::Thread thread([this]() {
    Options opts;
    opts.env = env_;
    opts.max_open_files = -1;
    OpenSecondary(opts);
  });
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  thread.join();
  ASSERT_TRUE(called);
}

TEST_F(DBSecondaryTest, StartFromInconsistent) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistencyBeforeReturn", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        *(reinterpret_cast<Status*>(arg)) =
            Status::Corruption("Inject corruption");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Options options1;
  options1.env = env_;
  Status s = TryOpenSecondary(options1);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(DBSecondaryTest, InconsistencyDuringCatchUp) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Flush());

  Options options1;
  options1.env = env_;
  OpenSecondary(options1);

  {
    std::string value;
    ASSERT_OK(db_secondary_->Get(ReadOptions(), "foo", &value));
    ASSERT_EQ("value", value);
  }

  ASSERT_OK(Put("bar", "value1"));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistencyBeforeReturn", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        *(reinterpret_cast<Status*>(arg)) =
            Status::Corruption("Inject corruption");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = db_secondary_->TryCatchUpWithPrimary();
  ASSERT_TRUE(s.IsCorruption());
}
#endif  //! ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
