//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/table_properties.h"
#include "table/block_based_table_factory.h"
#include "table/plain_table_factory.h"
#include "util/hash.h"
#include "util/hash_linklist_rep.h"
#include "utilities/merge_operators.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/rate_limiter.h"
#include "util/statistics.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/sync_point.h"
#include "util/testutil.h"

#ifndef ROCKSDB_LITE

namespace rocksdb {

class EventListenerTest : public testing::Test {
 public:
  EventListenerTest() {
    dbname_ = test::TmpDir() + "/listener_test";
    EXPECT_OK(DestroyDB(dbname_, Options()));
    db_ = nullptr;
    Reopen();
  }

  ~EventListenerTest() {
    Close();
    Options options;
    options.db_paths.emplace_back(dbname_, 0);
    options.db_paths.emplace_back(dbname_ + "_2", 0);
    options.db_paths.emplace_back(dbname_ + "_3", 0);
    options.db_paths.emplace_back(dbname_ + "_4", 0);
    EXPECT_OK(DestroyDB(dbname_, options));
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const ColumnFamilyOptions* options = nullptr) {
    ColumnFamilyOptions cf_opts;
    cf_opts = ColumnFamilyOptions(Options());
    size_t cfi = handles_.size();
    handles_.resize(cfi + cfs.size());
    for (auto cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
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

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const Options* options = nullptr) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const Options* options = nullptr) {
    Close();
    Options opts = (options == nullptr) ? Options() : *options;
    std::vector<const Options*> v_opts(cfs.size(), &opts);
    return TryReopenWithColumnFamilies(cfs, v_opts);
  }

  Status TryReopenWithColumnFamilies(
      const std::vector<std::string>& cfs,
      const std::vector<const Options*>& options) {
    Close();
    EXPECT_EQ(cfs.size(), options.size());
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(cfs[i], *options[i]));
    }
    DBOptions db_opts = DBOptions(*options[0]);
    return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
  }

  Status TryReopen(Options* options = nullptr) {
    Close();
    Options opts;
    if (options != nullptr) {
      opts = *options;
    } else {
      opts.create_if_missing = true;
    }

    return DB::Open(opts, dbname_, &db_);
  }

  void Reopen(Options* options = nullptr) {
    ASSERT_OK(TryReopen(options));
  }

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const Options* options = nullptr) {
    CreateColumnFamilies(cfs, options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    ReopenWithColumnFamilies(cfs_plus_default, options);
  }

  DBImpl* dbfull() {
    return reinterpret_cast<DBImpl*>(db_);
  }

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, handles_[cf], k, v);
  }

  Status Flush(int cf = 0) {
    FlushOptions opt = FlushOptions();
    opt.wait = true;
    if (cf == 0) {
      return db_->Flush(opt);
    } else {
      return db_->Flush(opt, handles_[cf]);
    }
  }

  DB* db_;
  std::string dbname_;
  std::vector<ColumnFamilyHandle*> handles_;
};

class TestCompactionListener : public EventListener {
 public:
  void OnCompactionCompleted(DB *db, const CompactionJobInfo& ci) override {
    compacted_dbs_.push_back(db);
  }

  std::vector<DB*> compacted_dbs_;
};

TEST_F(EventListenerTest, OnSingleDBCompactionTest) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 100;
  const int kNumL0Files = 4;

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base = options.target_file_size_base * 2;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;
  options.enable_thread_tracking = true;
  options.level0_file_num_compaction_trigger = kNumL0Files;

  TestCompactionListener* listener = new TestCompactionListener();
  options.listeners.emplace_back(listener);
  std::vector<std::string> cf_names = {
      "pikachu", "ilya", "muromec", "dobrynia",
      "nikitich", "alyosha", "popovich"};
  CreateAndReopenWithCF(cf_names, &options);
  ASSERT_OK(Put(1, "pikachu", std::string(90000, 'p')));
  ASSERT_OK(Put(2, "ilya", std::string(90000, 'i')));
  ASSERT_OK(Put(3, "muromec", std::string(90000, 'm')));
  ASSERT_OK(Put(4, "dobrynia", std::string(90000, 'd')));
  ASSERT_OK(Put(5, "nikitich", std::string(90000, 'n')));
  ASSERT_OK(Put(6, "alyosha", std::string(90000, 'a')));
  ASSERT_OK(Put(7, "popovich", std::string(90000, 'p')));
  for (size_t i = 1; i < 8; ++i) {
    ASSERT_OK(Flush(static_cast<int>(i)));
    const Slice kStart = "a";
    const Slice kEnd = "z";
    ASSERT_OK(dbfull()->CompactRange(handles_[i], &kStart, &kEnd));
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }

  ASSERT_EQ(listener->compacted_dbs_.size(), cf_names.size());
  for (size_t i = 0; i < cf_names.size(); ++i) {
    ASSERT_EQ(listener->compacted_dbs_[i], db_);
  }
}

class TestFlushListener : public EventListener {
 public:
  void OnFlushCompleted(
      DB* db, const std::string& name,
      const std::string& file_path,
      bool triggered_writes_slowdown,
      bool triggered_writes_stop) override {
    flushed_dbs_.push_back(db);
    flushed_column_family_names_.push_back(name);
    if (triggered_writes_slowdown) {
      slowdown_count++;
    }
    if (triggered_writes_stop) {
      stop_count++;
    }
  }

  std::vector<std::string> flushed_column_family_names_;
  std::vector<DB*> flushed_dbs_;
  int slowdown_count;
  int stop_count;
};

TEST_F(EventListenerTest, OnSingleDBFlushTest) {
  Options options;
  options.write_buffer_size = 100000;
  TestFlushListener* listener = new TestFlushListener();
  options.listeners.emplace_back(listener);
  std::vector<std::string> cf_names = {
      "pikachu", "ilya", "muromec", "dobrynia",
      "nikitich", "alyosha", "popovich"};
  CreateAndReopenWithCF(cf_names, &options);

  ASSERT_OK(Put(1, "pikachu", std::string(90000, 'p')));
  ASSERT_OK(Put(2, "ilya", std::string(90000, 'i')));
  ASSERT_OK(Put(3, "muromec", std::string(90000, 'm')));
  ASSERT_OK(Put(4, "dobrynia", std::string(90000, 'd')));
  ASSERT_OK(Put(5, "nikitich", std::string(90000, 'n')));
  ASSERT_OK(Put(6, "alyosha", std::string(90000, 'a')));
  ASSERT_OK(Put(7, "popovich", std::string(90000, 'p')));
  for (size_t i = 1; i < 8; ++i) {
    ASSERT_OK(Flush(static_cast<int>(i)));
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(listener->flushed_dbs_.size(), i);
    ASSERT_EQ(listener->flushed_column_family_names_.size(), i);
  }

  // make sure call-back functions are called in the right order
  for (size_t i = 0; i < cf_names.size(); ++i) {
    ASSERT_EQ(listener->flushed_dbs_[i], db_);
    ASSERT_EQ(listener->flushed_column_family_names_[i], cf_names[i]);
  }
}

TEST_F(EventListenerTest, MultiCF) {
  Options options;
  options.write_buffer_size = 100000;
  TestFlushListener* listener = new TestFlushListener();
  options.listeners.emplace_back(listener);
  std::vector<std::string> cf_names = {
      "pikachu", "ilya", "muromec", "dobrynia",
      "nikitich", "alyosha", "popovich"};
  CreateAndReopenWithCF(cf_names, &options);

  ASSERT_OK(Put(1, "pikachu", std::string(90000, 'p')));
  ASSERT_OK(Put(2, "ilya", std::string(90000, 'i')));
  ASSERT_OK(Put(3, "muromec", std::string(90000, 'm')));
  ASSERT_OK(Put(4, "dobrynia", std::string(90000, 'd')));
  ASSERT_OK(Put(5, "nikitich", std::string(90000, 'n')));
  ASSERT_OK(Put(6, "alyosha", std::string(90000, 'a')));
  ASSERT_OK(Put(7, "popovich", std::string(90000, 'p')));
  for (size_t i = 1; i < 8; ++i) {
    ASSERT_OK(Flush(static_cast<int>(i)));
    ASSERT_EQ(listener->flushed_dbs_.size(), i);
    ASSERT_EQ(listener->flushed_column_family_names_.size(), i);
  }

  // make sure call-back functions are called in the right order
  for (size_t i = 0; i < cf_names.size(); i++) {
    ASSERT_EQ(listener->flushed_dbs_[i], db_);
    ASSERT_EQ(listener->flushed_column_family_names_[i], cf_names[i]);
  }
}

TEST_F(EventListenerTest, MultiDBMultiListeners) {
  std::vector<TestFlushListener*> listeners;
  const int kNumDBs = 5;
  const int kNumListeners = 10;
  for (int i = 0; i < kNumListeners; ++i) {
    listeners.emplace_back(new TestFlushListener());
  }

  std::vector<std::string> cf_names = {
      "pikachu", "ilya", "muromec", "dobrynia",
      "nikitich", "alyosha", "popovich"};

  Options options;
  options.create_if_missing = true;
  for (int i = 0; i < kNumListeners; ++i) {
    options.listeners.emplace_back(listeners[i]);
  }
  DBOptions db_opts(options);
  ColumnFamilyOptions cf_opts(options);

  std::vector<DB*> dbs;
  std::vector<std::vector<ColumnFamilyHandle *>> vec_handles;

  for (int d = 0; d < kNumDBs; ++d) {
    ASSERT_OK(DestroyDB(dbname_ + ToString(d), options));
    DB* db;
    std::vector<ColumnFamilyHandle*> handles;
    ASSERT_OK(DB::Open(options, dbname_ + ToString(d), &db));
    for (size_t c = 0; c < cf_names.size(); ++c) {
      ColumnFamilyHandle* handle;
      db->CreateColumnFamily(cf_opts, cf_names[c], &handle);
      handles.push_back(handle);
    }

    vec_handles.push_back(std::move(handles));
    dbs.push_back(db);
  }

  for (int d = 0; d < kNumDBs; ++d) {
    for (size_t c = 0; c < cf_names.size(); ++c) {
      ASSERT_OK(dbs[d]->Put(WriteOptions(), vec_handles[d][c],
                cf_names[c], cf_names[c]));
    }
  }

  for (size_t c = 0; c < cf_names.size(); ++c) {
    for (int d = 0; d < kNumDBs; ++d) {
      ASSERT_OK(dbs[d]->Flush(FlushOptions(), vec_handles[d][c]));
      reinterpret_cast<DBImpl*>(dbs[d])->TEST_WaitForFlushMemTable();
    }
  }

  for (auto* listener : listeners) {
    int pos = 0;
    for (size_t c = 0; c < cf_names.size(); ++c) {
      for (int d = 0; d < kNumDBs; ++d) {
        ASSERT_EQ(listener->flushed_dbs_[pos], dbs[d]);
        ASSERT_EQ(listener->flushed_column_family_names_[pos], cf_names[c]);
        pos++;
      }
    }
  }

  for (auto handles : vec_handles) {
    for (auto h : handles) {
      delete h;
    }
    handles.clear();
  }
  vec_handles.clear();

  for (auto db : dbs) {
    delete db;
  }
}

TEST_F(EventListenerTest, DisableBGCompaction) {
  Options options;
  TestFlushListener* listener = new TestFlushListener();
  const int kSlowdownTrigger = 5;
  const int kStopTrigger = 10;
  options.level0_slowdown_writes_trigger = kSlowdownTrigger;
  options.level0_stop_writes_trigger = kStopTrigger;
  options.listeners.emplace_back(listener);
  // BG compaction is disabled.  Number of L0 files will simply keeps
  // increasing in this test.
  options.compaction_style = kCompactionStyleNone;
  options.compression = kNoCompression;
  options.write_buffer_size = 100000;  // Small write buffer

  CreateAndReopenWithCF({"pikachu"}, &options);
  WriteOptions wopts;
  wopts.timeout_hint_us = 100000;
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  // keep writing until writes are forced to stop.
  for (int i = 0; static_cast<int>(cf_meta.file_count) < kStopTrigger; ++i) {
    Put(1, ToString(i), std::string(100000, 'x'), wopts);
    db_->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  }
  ASSERT_GE(listener->slowdown_count, kStopTrigger - kSlowdownTrigger);
  ASSERT_GE(listener->stop_count, 1);
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

