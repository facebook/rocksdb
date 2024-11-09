//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Syncpoint prevents us building and running tests in release
#include "rocksdb/utilities/checkpoint.h"

#ifndef OS_WIN
#include <unistd.h>
#endif
#include <cstdlib>
#include <iostream>
#include <thread>
#include <utility>

#include "db/db_impl/db_impl.h"
#include "file/file_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
class CheckpointTest : public testing::Test {
 protected:
  // Sequence of option configurations to try
  enum OptionConfig {
    kDefault = 0,
  };
  int option_config_;

 public:
  std::string dbname_;
  std::string alternative_wal_dir_;
  Env* env_;
  DB* db_;
  Options last_options_;
  std::vector<ColumnFamilyHandle*> handles_;
  std::string snapshot_name_;
  std::string export_path_;
  ColumnFamilyHandle* cfh_reverse_comp_;
  ExportImportFilesMetaData* metadata_;

  CheckpointTest() : env_(Env::Default()) {
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->SetBackgroundThreads(1, Env::HIGH);
    dbname_ = test::PerThreadDBPath(env_, "checkpoint_test");
    alternative_wal_dir_ = dbname_ + "/wal";
    auto options = CurrentOptions();
    auto delete_options = options;
    delete_options.wal_dir = alternative_wal_dir_;
    EXPECT_OK(DestroyDB(dbname_, delete_options));
    // Destroy it for not alternative WAL dir is used.
    EXPECT_OK(DestroyDB(dbname_, options));
    db_ = nullptr;
    snapshot_name_ = test::PerThreadDBPath(env_, "snapshot");
    std::string snapshot_tmp_name = snapshot_name_ + ".tmp";
    EXPECT_OK(DestroyDB(snapshot_name_, options));
    test::DeleteDir(env_, snapshot_name_);
    EXPECT_OK(DestroyDB(snapshot_tmp_name, options));
    test::DeleteDir(env_, snapshot_tmp_name);
    Reopen(options);
    export_path_ = test::PerThreadDBPath("/export");
    DestroyDir(env_, export_path_).PermitUncheckedError();
    cfh_reverse_comp_ = nullptr;
    metadata_ = nullptr;
  }

  ~CheckpointTest() override {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({});
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    if (cfh_reverse_comp_) {
      EXPECT_OK(db_->DestroyColumnFamilyHandle(cfh_reverse_comp_));
      cfh_reverse_comp_ = nullptr;
    }
    if (metadata_) {
      delete metadata_;
      metadata_ = nullptr;
    }
    Close();
    Options options;
    options.db_paths.emplace_back(dbname_, 0);
    options.db_paths.emplace_back(dbname_ + "_2", 0);
    options.db_paths.emplace_back(dbname_ + "_3", 0);
    options.db_paths.emplace_back(dbname_ + "_4", 0);
    EXPECT_OK(DestroyDB(dbname_, options));
    EXPECT_OK(DestroyDB(snapshot_name_, options));
    DestroyDir(env_, export_path_).PermitUncheckedError();
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    options.env = env_;
    options.create_if_missing = true;
    return options;
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const Options& options) {
    ColumnFamilyOptions cf_opts(options);
    size_t cfi = handles_.size();
    handles_.resize(cfi + cfs.size());
    for (const auto& cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
    }
  }

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const Options& options) {
    CreateColumnFamilies(cfs, options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    ReopenWithColumnFamilies(cfs_plus_default, options);
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const std::vector<Options>& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const Options& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const std::vector<Options>& options) {
    Close();
    EXPECT_EQ(cfs.size(), options.size());
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.emplace_back(cfs[i], options[i]);
    }
    DBOptions db_opts = DBOptions(options[0]);
    return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const Options& options) {
    Close();
    std::vector<Options> v_opts(cfs.size(), options);
    return TryReopenWithColumnFamilies(cfs, v_opts);
  }

  void Reopen(const Options& options) { ASSERT_OK(TryReopen(options)); }

  void CompactAll() {
    for (auto h : handles_) {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), h, nullptr, nullptr));
    }
  }

  void Close() {
    for (auto h : handles_) {
      delete h;
    }
    handles_.clear();
    delete db_;
    db_ = nullptr;
  }

  void DestroyAndReopen(const Options& options) {
    // Destroy using last options
    Destroy(last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(const Options& options) {
    Close();
    ASSERT_OK(DestroyDB(dbname_, options));
  }

  Status ReadOnlyReopen(const Options& options) {
    return DB::OpenForReadOnly(options, dbname_, &db_);
  }

  Status ReadOnlyReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const Options& options) {
    std::vector<ColumnFamilyDescriptor> column_families;
    for (const auto& cf : cfs) {
      column_families.emplace_back(cf, options);
    }
    return DB::OpenForReadOnly(options, dbname_, column_families, &handles_,
                               &db_);
  }

  Status TryReopen(const Options& options) {
    Close();
    last_options_ = options;
    return DB::Open(options, dbname_, &db_);
  }

  Status Flush(int cf = 0) {
    if (cf == 0) {
      return db_->Flush(FlushOptions());
    } else {
      return db_->Flush(FlushOptions(), handles_[cf]);
    }
  }

  Status Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, k, v);
  }

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, handles_[cf], k, v);
  }

  Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

  Status Delete(int cf, const std::string& k) {
    return db_->Delete(WriteOptions(), handles_[cf], k);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  std::string Get(int cf, const std::string& k,
                  const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, handles_[cf], k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  int NumTableFilesAtLevel(int level) {
    std::string property;
    EXPECT_TRUE(db_->GetProperty(
        "rocksdb.num-files-at-level" + std::to_string(level), &property));
    return atoi(property.c_str());
  }
};

TEST_F(CheckpointTest, GetSnapshotLink) {
  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    options = CurrentOptions();
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));

    // Create a database
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, log_size_for_flush));
    ASSERT_OK(Put(key, "v2"));
    ASSERT_EQ("v2", Get(key));
    ASSERT_OK(Flush());
    ASSERT_EQ("v2", Get(key));
    // Open snapshot and verify contents while DB is running
    options.create_if_missing = false;
    ASSERT_OK(DB::Open(options, snapshot_name_, &snapshotDB));
    ASSERT_OK(snapshotDB->Get(roptions, key, &result));
    ASSERT_EQ("v1", result);
    delete snapshotDB;
    snapshotDB = nullptr;
    delete db_;
    db_ = nullptr;

    // Destroy original DB
    ASSERT_OK(DestroyDB(dbname_, options));

    // Open snapshot and verify contents
    options.create_if_missing = false;
    dbname_ = snapshot_name_;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_EQ("v1", Get(key));
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    delete checkpoint;

    // Restore DB name
    dbname_ = test::PerThreadDBPath(env_, "db_test");
  }
}

TEST_F(CheckpointTest, CheckpointWithBlob) {
  // Create a database with a blob file
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  ASSERT_OK(Put(key, blob));
  ASSERT_OK(Flush());

  // Create a checkpoint
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));

  std::unique_ptr<Checkpoint> checkpoint_guard(checkpoint);

  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));

  // Make sure it contains the blob file
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(snapshot_name_, &files));

  bool blob_file_found = false;
  for (const auto& file : files) {
    uint64_t number = 0;
    FileType type = kWalFile;

    if (ParseFileName(file, &number, &type) && type == kBlobFile) {
      blob_file_found = true;
      break;
    }
  }

  ASSERT_TRUE(blob_file_found);

  // Make sure the checkpoint can be opened and the blob value read
  options.create_if_missing = false;
  DB* checkpoint_db = nullptr;
  ASSERT_OK(DB::Open(options, snapshot_name_, &checkpoint_db));

  std::unique_ptr<DB> checkpoint_db_guard(checkpoint_db);

  PinnableSlice value;
  ASSERT_OK(checkpoint_db->Get(
      ReadOptions(), checkpoint_db->DefaultColumnFamily(), key, &value));

  ASSERT_EQ(value, blob);
}

TEST_F(CheckpointTest, ExportColumnFamilyWithLinks) {
  // Create a database
  auto options = CurrentOptions();
  options.create_if_missing = true;
  CreateAndReopenWithCF({}, options);

  // Helper to verify the number of files in metadata and export dir
  auto verify_files_exported = [&](const ExportImportFilesMetaData& metadata,
                                   int num_files_expected) {
    ASSERT_EQ(metadata.files.size(), num_files_expected);
    std::vector<std::string> subchildren;
    ASSERT_OK(env_->GetChildren(export_path_, &subchildren));
    ASSERT_EQ(subchildren.size(), num_files_expected);
  };

  // Test DefaultColumnFamily
  {
    const auto key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));

    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));

    // Export the Tables and verify
    ASSERT_OK(checkpoint->ExportColumnFamily(db_->DefaultColumnFamily(),
                                             export_path_, &metadata_));
    verify_files_exported(*metadata_, 1);
    ASSERT_EQ(metadata_->db_comparator_name, options.comparator->Name());
    ASSERT_OK(DestroyDir(env_, export_path_));
    delete metadata_;
    metadata_ = nullptr;

    // Check again after compaction
    CompactAll();
    ASSERT_OK(Put(key, "v2"));
    ASSERT_OK(checkpoint->ExportColumnFamily(db_->DefaultColumnFamily(),
                                             export_path_, &metadata_));
    verify_files_exported(*metadata_, 2);
    ASSERT_EQ(metadata_->db_comparator_name, options.comparator->Name());
    ASSERT_OK(DestroyDir(env_, export_path_));
    delete metadata_;
    metadata_ = nullptr;
    delete checkpoint;
  }

  // Test non default column family with non default comparator
  {
    auto cf_options = CurrentOptions();
    cf_options.comparator = ReverseBytewiseComparator();
    ASSERT_OK(db_->CreateColumnFamily(cf_options, "yoyo", &cfh_reverse_comp_));

    const auto key = std::string("foo");
    ASSERT_OK(db_->Put(WriteOptions(), cfh_reverse_comp_, key, "v1"));

    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));

    // Export the Tables and verify
    ASSERT_OK(checkpoint->ExportColumnFamily(cfh_reverse_comp_, export_path_,
                                             &metadata_));
    verify_files_exported(*metadata_, 1);
    ASSERT_EQ(metadata_->db_comparator_name,
              ReverseBytewiseComparator()->Name());
    delete checkpoint;
  }
}

TEST_F(CheckpointTest, ExportColumnFamilyNegativeTest) {
  // Create a database
  auto options = CurrentOptions();
  options.create_if_missing = true;
  CreateAndReopenWithCF({}, options);

  const auto key = std::string("foo");
  ASSERT_OK(Put(key, "v1"));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));

  // Export onto existing directory
  ASSERT_OK(env_->CreateDirIfMissing(export_path_));
  ASSERT_EQ(checkpoint->ExportColumnFamily(db_->DefaultColumnFamily(),
                                           export_path_, &metadata_),
            Status::InvalidArgument("Specified export_dir exists"));
  ASSERT_OK(DestroyDir(env_, export_path_));

  // Export with invalid directory specification
  export_path_ = "";
  ASSERT_EQ(checkpoint->ExportColumnFamily(db_->DefaultColumnFamily(),
                                           export_path_, &metadata_),
            Status::InvalidArgument("Specified export_dir invalid"));
  delete checkpoint;
}

TEST_F(CheckpointTest, CheckpointCF) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three", "four", "five"}, options);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointTest::CheckpointCF:2", "DBImpl::FlushAllColumnFamilies:2"},
       {"DBImpl::FlushAllColumnFamilies:1", "CheckpointTest::CheckpointCF:1"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  ASSERT_OK(Put(2, "two", "two"));
  ASSERT_OK(Put(3, "three", "three"));
  ASSERT_OK(Put(4, "four", "four"));
  ASSERT_OK(Put(5, "five", "five"));

  DB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;

  // Take a snapshot
  ROCKSDB_NAMESPACE::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:1");
  ASSERT_OK(Put(0, "Default", "Default1"));
  ASSERT_OK(Put(1, "one", "eleven"));
  ASSERT_OK(Put(2, "two", "twelve"));
  ASSERT_OK(Put(3, "three", "thirteen"));
  ASSERT_OK(Put(4, "four", "fourteen"));
  ASSERT_OK(Put(5, "five", "fifteen"));
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:2");
  t.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(Put(1, "one", "twentyone"));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  ASSERT_OK(Put(3, "three", "twentythree"));
  ASSERT_OK(Put(4, "four", "twentyfour"));
  ASSERT_OK(Put(5, "five", "twentyfive"));
  ASSERT_OK(Flush());

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs = {kDefaultColumnFamilyName, "one", "two", "three", "four", "five"};
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.emplace_back(cfs[i], options);
  }
  ASSERT_OK(DB::Open(options, snapshot_name_, column_families, &cphandles,
                     &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default1", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("eleven", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  for (auto h : cphandles) {
    delete h;
  }
  cphandles.clear();
  delete snapshotDB;
  snapshotDB = nullptr;
}

TEST_F(CheckpointTest, CheckpointCFNoFlush) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three", "four", "five"}, options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(2, "two", "two"));

  DB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;

  // Take a snapshot
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void* /*arg*/) {
        // Flush should never trigger.
        FAIL();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, 1000000));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  delete checkpoint;
  ASSERT_OK(Put(1, "one", "two"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  Close();
  EXPECT_OK(DestroyDB(dbname_, options));

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs = {kDefaultColumnFamilyName, "one", "two", "three", "four", "five"};
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.emplace_back(cfs[i], options);
  }
  ASSERT_OK(DB::Open(options, snapshot_name_, column_families, &cphandles,
                     &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("one", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  ASSERT_EQ("two", result);
  for (auto h : cphandles) {
    delete h;
  }
  cphandles.clear();
  delete snapshotDB;
  snapshotDB = nullptr;
}

TEST_F(CheckpointTest, CurrentFileModifiedWhileCheckpointing) {
  Options options = CurrentOptions();
  options.max_manifest_file_size = 0;  // always rollover manifest for file add
  Reopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {// Get past the flush in the checkpoint thread before adding any keys to
       // the db so the checkpoint thread won't hit the WriteManifest
       // syncpoints.
       {"CheckpointImpl::CreateCheckpoint:FlushDone",
        "CheckpointTest::CurrentFileModifiedWhileCheckpointing:PrePut"},
       // Roll the manifest during checkpointing right after live files are
       // snapshotted.
       {"CheckpointImpl::CreateCheckpoint:SavedLiveFiles1",
        "VersionSet::LogAndApply:WriteManifest"},
       {"VersionSet::LogAndApply:WriteManifestDone",
        "CheckpointImpl::CreateCheckpoint:SavedLiveFiles2"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing:PrePut");
  ASSERT_OK(Put("Default", "Default1"));
  ASSERT_OK(Flush());
  t.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  DB* snapshotDB;
  // Successful Open() implies that CURRENT pointed to the manifest in the
  // checkpoint.
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshotDB));
  delete snapshotDB;
  snapshotDB = nullptr;
}

TEST_F(CheckpointTest, CurrentFileModifiedWhileCheckpointing2PC) {
  Close();
  const std::string dbname = test::PerThreadDBPath("transaction_testdb");
  ASSERT_OK(DestroyDB(dbname, CurrentOptions()));
  test::DeleteDir(env_, dbname);

  Options options = CurrentOptions();
  options.allow_2pc = true;
  // allow_2pc is implicitly set with tx prepare
  // options.allow_2pc = true;
  TransactionDBOptions txn_db_options;
  TransactionDB* txdb;
  Status s = TransactionDB::Open(options, txn_db_options, dbname, &txdb);
  ASSERT_OK(s);
  ColumnFamilyHandle* cfa;
  ColumnFamilyHandle* cfb;
  ColumnFamilyOptions cf_options;
  ASSERT_OK(txdb->CreateColumnFamily(cf_options, "CFA", &cfa));

  WriteOptions write_options;
  // Insert something into CFB so lots of log files will be kept
  // before creating the checkpoint.
  ASSERT_OK(txdb->CreateColumnFamily(cf_options, "CFB", &cfb));
  ASSERT_OK(txdb->Put(write_options, cfb, "", ""));

  ReadOptions read_options;
  std::string value;
  TransactionOptions txn_options;
  Transaction* txn = txdb->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);
  ASSERT_EQ(txdb->GetTransactionByName("xid"), txn);

  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);
  s = txn->Put(cfa, Slice("foocfa"), Slice("barcfa"));
  ASSERT_OK(s);
  // Writing prepare into middle of first WAL, then flush WALs many times
  for (int i = 1; i <= 100000; i++) {
    Transaction* tx = txdb->BeginTransaction(write_options, txn_options);
    ASSERT_OK(tx->SetName("x"));
    ASSERT_OK(tx->Put(Slice(std::to_string(i)), Slice("val")));
    ASSERT_OK(tx->Put(cfa, Slice("aaa"), Slice("111")));
    ASSERT_OK(tx->Prepare());
    ASSERT_OK(tx->Commit());
    if (i % 10000 == 0) {
      ASSERT_OK(txdb->Flush(FlushOptions()));
    }
    if (i == 88888) {
      ASSERT_OK(txn->Prepare());
    }
    delete tx;
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointImpl::CreateCheckpoint:SavedLiveFiles1",
        "CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PreCommit"},
       {"CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PostCommit",
        "CheckpointImpl::CreateCheckpoint:SavedLiveFiles2"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ROCKSDB_NAMESPACE::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(txdb, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PreCommit");
  ASSERT_OK(txn->Commit());
  delete txn;
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PostCommit");
  t.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // No more than two logs files should exist.
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(snapshot_name_, &files));
  int num_log_files = 0;
  for (auto& file : files) {
    uint64_t num;
    FileType type;
    WalFileType log_type;
    if (ParseFileName(file, &num, &type, &log_type) && type == kWalFile) {
      num_log_files++;
    }
  }
  // One flush after preapare + one outstanding file before checkpoint + one log
  // file generated after checkpoint.
  ASSERT_LE(num_log_files, 3);

  TransactionDB* snapshotDB;
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  column_families.emplace_back("CFA", ColumnFamilyOptions());
  column_families.emplace_back("CFB", ColumnFamilyOptions());
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  ASSERT_OK(TransactionDB::Open(options, txn_db_options, snapshot_name_,
                                column_families, &cf_handles, &snapshotDB));
  ASSERT_OK(snapshotDB->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");
  ASSERT_OK(snapshotDB->Get(read_options, cf_handles[1], "foocfa", &value));
  ASSERT_EQ(value, "barcfa");

  delete cfa;
  delete cfb;
  delete cf_handles[0];
  delete cf_handles[1];
  delete cf_handles[2];
  delete snapshotDB;
  snapshotDB = nullptr;
  delete txdb;
}

TEST_F(CheckpointTest, CheckpointInvalidDirectoryName) {
  for (std::string checkpoint_dir : {"", "/", "////"}) {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_TRUE(
        checkpoint->CreateCheckpoint(checkpoint_dir).IsInvalidArgument());
    delete checkpoint;
  }
}

TEST_F(CheckpointTest, CheckpointWithParallelWrites) {
  // When run with TSAN, this exposes the data race fixed in
  // https://github.com/facebook/rocksdb/pull/3603
  ASSERT_OK(Put("key1", "val1"));
  port::Thread thread([this]() { ASSERT_OK(Put("key2", "val2")); });
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  thread.join();
}

class CheckpointTestWithWalParams
    : public CheckpointTest,
      public testing::WithParamInterface<
          std::tuple<uint64_t, bool, bool, bool>> {
 public:
  uint64_t GetLogSizeForFlush() { return std::get<0>(GetParam()); }
  bool GetWalsInManifest() { return std::get<1>(GetParam()); }
  bool GetManualWalFlush() { return std::get<2>(GetParam()); }
  bool GetBackgroundCloseInactiveWals() { return std::get<3>(GetParam()); }
};

INSTANTIATE_TEST_CASE_P(NormalWalParams, CheckpointTestWithWalParams,
                        ::testing::Combine(::testing::Values(0U, 100000000U),
                                           ::testing::Bool(), ::testing::Bool(),
                                           ::testing::Values(false)));

INSTANTIATE_TEST_CASE_P(DeprecatedWalParams, CheckpointTestWithWalParams,
                        ::testing::Values(std::make_tuple(100000000U, true,
                                                          false, true)));

TEST_P(CheckpointTestWithWalParams, CheckpointWithUnsyncedDataDropped) {
  Options options = CurrentOptions();
  options.max_write_buffer_number = 4;
  options.track_and_verify_wals_in_manifest = GetWalsInManifest();
  options.manual_wal_flush = GetManualWalFlush();
  options.background_close_inactive_wals = GetBackgroundCloseInactiveWals();
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));

  if (options.background_close_inactive_wals) {
    // Disable this hygiene check when the fix is disabled
    fault_fs->SetAllowLinkOpenFile();
  }

  options.env = fault_fs_env.get();
  Reopen(options);
  ASSERT_OK(Put("key1", "val1"));
  if (GetLogSizeForFlush() > 0) {
    // When not flushing memtable for checkpoint, this is the simplest way
    // to get
    // * one inactive WAL, synced
    // * one inactive WAL, not synced, and
    // * one active WAL, not synced
    // with a single thread, so that we have at least one that can be hard
    // linked, etc.
    ASSERT_OK(static_cast_with_check<DBImpl>(db_)->PauseBackgroundWork());
    ASSERT_OK(static_cast_with_check<DBImpl>(db_)->TEST_SwitchMemtable());
    ASSERT_OK(db_->SyncWAL());
  }
  ASSERT_OK(Put("key2", "val2"));
  if (GetLogSizeForFlush() > 0) {
    ASSERT_OK(static_cast_with_check<DBImpl>(db_)->TEST_SwitchMemtable());
  }
  ASSERT_OK(Put("key3", "val3"));
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, GetLogSizeForFlush()));
  delete checkpoint;
  ASSERT_OK(fault_fs->DropUnsyncedFileData());
  // make sure it's openable even though whatever data that wasn't synced got
  // dropped.
  options.env = env_;
  DB* snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "key1", &get_result));
  ASSERT_EQ("val1", get_result);
  ASSERT_OK(snapshot_db->Get(read_opts, "key2", &get_result));
  ASSERT_EQ("val2", get_result);
  ASSERT_OK(snapshot_db->Get(read_opts, "key3", &get_result));
  ASSERT_EQ("val3", get_result);
  delete snapshot_db;
  delete db_;
  db_ = nullptr;
}

TEST_F(CheckpointTest, CheckpointOptionsFileFailedToPersist) {
  // Regression test for a bug where checkpoint failed on a DB where persisting
  // OPTIONS file failed and the DB was opened with
  // `fail_if_options_file_error == false`.
  Options options = CurrentOptions();
  options.fail_if_options_file_error = false;
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(FileSystem::Default());

  // Setup `FaultInjectionTestFS` and `SyncPoint` callbacks to fail one
  // operation when inside the OPTIONS file persisting code.
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kWrite, 7 /* seed*/, 1 /* one_in */,
      false /* retryable */, false /* has_data_loss*/);
  SyncPoint::GetInstance()->SetCallBack(
      "PersistRocksDBOptions:start", [fault_fs](void* /* arg */) {
        fault_fs->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataWrite);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "FaultInjectionTestFS::InjectMetadataWriteError:Injected",
      [fault_fs](void* /* arg */) {
        fault_fs->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataWrite);
      });
  options.env = fault_fs_env.get();
  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);
  ASSERT_OK(Put("key1", "val1"));
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;

  // Make sure it's usable.
  options.env = env_;
  DB* snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "key1", &get_result));
  ASSERT_EQ("val1", get_result);
  delete snapshot_db;
  delete db_;
  db_ = nullptr;
}

TEST_F(CheckpointTest, CheckpointReadOnlyDB) {
  ASSERT_OK(Put("foo", "foo_value"));
  ASSERT_OK(Flush());
  Close();
  Options options = CurrentOptions();
  ASSERT_OK(ReadOnlyReopen(options));
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  checkpoint = nullptr;
  Close();
  DB* snapshot_db = nullptr;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "foo", &get_result));
  ASSERT_EQ("foo_value", get_result);
  delete snapshot_db;
}

TEST_F(CheckpointTest, CheckpointWithLockWAL) {
  Options options = CurrentOptions();
  ASSERT_OK(Put("foo", "foo_value"));

  ASSERT_OK(db_->LockWAL());

  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  checkpoint = nullptr;

  ASSERT_OK(db_->UnlockWAL());
  Close();

  DB* snapshot_db = nullptr;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "foo", &get_result));
  ASSERT_EQ("foo_value", get_result);
  delete snapshot_db;
}

TEST_F(CheckpointTest, CheckpointReadOnlyDBWithMultipleColumnFamilies) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  for (int i = 0; i != 3; ++i) {
    ASSERT_OK(Put(i, "foo", "foo_value"));
    ASSERT_OK(Flush(i));
  }
  Close();
  Status s = ReadOnlyReopenWithColumnFamilies(
      {kDefaultColumnFamilyName, "pikachu", "eevee"}, options);
  ASSERT_OK(s);
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  checkpoint = nullptr;
  Close();

  std::vector<ColumnFamilyDescriptor> column_families{
      {kDefaultColumnFamilyName, options},
      {"pikachu", options},
      {"eevee", options}};
  DB* snapshot_db = nullptr;
  std::vector<ColumnFamilyHandle*> snapshot_handles;
  s = DB::Open(options, snapshot_name_, column_families, &snapshot_handles,
               &snapshot_db);
  ASSERT_OK(s);
  ReadOptions read_opts;
  for (int i = 0; i != 3; ++i) {
    std::string get_result;
    s = snapshot_db->Get(read_opts, snapshot_handles[i], "foo", &get_result);
    ASSERT_OK(s);
    ASSERT_EQ("foo_value", get_result);
  }

  for (auto snapshot_h : snapshot_handles) {
    delete snapshot_h;
  }
  snapshot_handles.clear();
  delete snapshot_db;
}

TEST_F(CheckpointTest, CheckpointWithDbPath) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_ + "_2", 0);
  Reopen(options);
  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Flush());
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  // Currently not supported
  ASSERT_TRUE(checkpoint->CreateCheckpoint(snapshot_name_).IsNotSupported());
  delete checkpoint;
}

TEST_F(CheckpointTest, CheckpointWithArchievedLog) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"WalManager::ArchiveWALFile",
        "CheckpointTest:CheckpointWithArchievedLog"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Options options = CurrentOptions();
  options.WAL_ttl_seconds = 3600;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", std::string(1024 * 1024, 'a')));
  // flush and archive the first log
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", std::string(1024, 'a')));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  TEST_SYNC_POINT("CheckpointTest:CheckpointWithArchievedLog");
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, 1024 * 1024));
  // unflushed log size < 1024 * 1024 < total file size including archived log,
  // so flush shouldn't occur, there is only one file at level 0
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  delete checkpoint;
  checkpoint = nullptr;

  DB* snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "key1", &get_result));
  ASSERT_EQ(std::string(1024 * 1024, 'a'), get_result);
  get_result.clear();
  ASSERT_OK(snapshot_db->Get(read_opts, "key2", &get_result));
  ASSERT_EQ(std::string(1024, 'a'), get_result);
  delete snapshot_db;
}

class CheckpointDestroyTest : public CheckpointTest,
                              public testing::WithParamInterface<bool> {};

TEST_P(CheckpointDestroyTest, DisableEnableSlowDeletion) {
  bool slow_deletion = GetParam();
  Options options = CurrentOptions();
  options.num_levels = 2;
  options.disable_auto_compactions = true;
  Status s;
  options.sst_file_manager.reset(NewSstFileManager(
      options.env, options.info_log, "", slow_deletion ? 1024 * 1024 : 0,
      false /* delete_existing_trash */, &s, 1));
  ASSERT_OK(s);
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "a"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("bar", "b"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("bar", "val" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);
  ASSERT_EQ(NumTableFilesAtLevel(1), 2);

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));

  delete checkpoint;
  checkpoint = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 2);

  DB* snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "foo", &get_result));
  ASSERT_EQ("a", get_result);
  ASSERT_OK(snapshot_db->Get(read_opts, "bar", &get_result));
  ASSERT_EQ("val9", get_result);
  delete snapshot_db;

  // Make sure original obsolete files for hard linked files are all deleted.
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_);
  db_impl->TEST_DeleteObsoleteFiles();
  auto sfm = static_cast_with_check<SstFileManagerImpl>(
      options.sst_file_manager.get());
  ASSERT_NE(nullptr, sfm);
  sfm->WaitForEmptyTrash();
  // SST file 2-12 for "bar" will be compacted into one file on L1 during the
  // compaction  after checkpoint is created. SST file 1 on L1: foo, seq:
  // 1 (hard links is 1 after checkpoint destroy)
  std::atomic<int> bg_delete_sst{0};
  std::atomic<int> fg_delete_sst{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile::cb", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto file_name = *static_cast<std::string*>(arg);
        if (file_name.size() >= 4 &&
            file_name.compare(file_name.size() - 4, 4, ".sst") == 0) {
          fg_delete_sst.fetch_add(1);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile::cb", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto file_name = *static_cast<std::string*>(arg);
        if (file_name.size() >= 10 &&
            file_name.compare(file_name.size() - 10, 10, ".sst.trash") == 0) {
          bg_delete_sst.fetch_add(1);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(DestroyDB(snapshot_name_, options));
  if (slow_deletion) {
    ASSERT_EQ(fg_delete_sst, 1);
    ASSERT_EQ(bg_delete_sst, 11);
  } else {
    ASSERT_EQ(fg_delete_sst, 12);
  }

  ASSERT_EQ("a", Get("foo"));
  ASSERT_EQ("val9", Get("bar"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}
INSTANTIATE_TEST_CASE_P(CheckpointDestroyTest, CheckpointDestroyTest,
                        ::testing::Values(true, false));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
