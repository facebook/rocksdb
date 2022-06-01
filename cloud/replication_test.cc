// Copyright (c) 2017 Rockset

#include <algorithm>
#include <chrono>
#include <cinttypes>

#include "cloud/filename.h"
#include "db/db_impl/db_impl.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class Listener : public ReplicationLogListener {
 public:
  Listener(port::Mutex* log_records_mutex,
           std::vector<ReplicationLogRecord>* log_records)
      : log_records_mutex_(log_records_mutex), log_records_(log_records) {}

  enum State { OPEN, RECOVERY, TAILING };

  void setState(State state) { state_ = state; }

  std::string OnReplicationLogRecord(ReplicationLogRecord record) override {
    // We should't be producing replication log records during open
    assert(state_ != OPEN);
    if (state_ == RECOVERY) {
      return "";
    }
    assert(state_ == TAILING);
    {
      MutexLock lock(log_records_mutex_);
      log_records_->push_back(std::move(record));
      return std::to_string(log_records_->size() - 1);
    }
  }

 private:
  port::Mutex* log_records_mutex_;
  std::vector<ReplicationLogRecord>* log_records_;
  State state_{OPEN};
};

class FollowerEnv : public EnvWrapper {
 public:
  FollowerEnv(std::string leader_path)
      : EnvWrapper(Env::Default()), leader_path_(std::move(leader_path)) {}

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override {
    return EnvWrapper::NewRandomAccessFile(mapFilename(fname), result, options);
  };

  Status GetFileSize(const std::string& fname, uint64_t* file_size) override {
    return EnvWrapper::GetFileSize(mapFilename(fname), file_size);
  }

 private:
  std::string mapFilename(const std::string& fname) {
    if (IsSstFile(fname)) {
      return leader_path_ + "/" + basename(fname);
    }
    return fname;
  }

  std::string leader_path_;
};

int getPersistedSequence(DB* db) {
  std::string out;
  auto s = db->GetPersistedReplicationSequence(&out);
  assert(s.ok());
  if (out.empty()) {
    return -1;
  }
  return std::atoi(out.c_str());
}

int getMemtableEntries(DB* db) {
  uint64_t v;
  auto ok = db->GetIntProperty(DB::Properties::kNumEntriesActiveMemTable, &v);
  assert(ok);
  return (int)v;
}

int getLevel0FileCount(DB* db) {
  std::string v;
  auto ok = db->GetProperty(DB::Properties::kNumFilesAtLevelPrefix + "0", &v);
  assert(ok);
  return std::atoi(v.c_str());
}

class ReplicationTest : public testing::Test {
 public:
  ReplicationTest()
      : test_dir_(test::TmpDir()), follower_env_(test_dir_ + "/leader") {
    DestroyDir(Env::Default(), test_dir_ + "/leader");
    DestroyDir(Env::Default(), test_dir_ + "/follower");
    Env::Default()->CreateDirIfMissing(test_dir_);
  }
  ~ReplicationTest() {
    DestroyDir(Env::Default(), test_dir_ + "/leader");
    DestroyDir(Env::Default(), test_dir_ + "/follower");
  }

  using ColumnFamilyMap =
      std::unordered_map<std::string, std::unique_ptr<ColumnFamilyHandle>>;
  const ColumnFamilyMap& leaderColumnFamilies() const { return leader_cfs_; }
  const ColumnFamilyMap& followerColumnFamilies() const {
    return follower_cfs_;
  }

  ColumnFamilyHandle* leaderCF(const std::string& name) const {
    auto pos = leader_cfs_.find(name);
    assert(pos != leader_cfs_.end());
    return pos->second.get();
  }
  ColumnFamilyHandle* followerCF(const std::string& name) const {
    auto pos = follower_cfs_.find(name);
    assert(pos != follower_cfs_.end());
    return pos->second.get();
  }

  DB* openLeader();
  void closeLeader() {
    leader_cfs_.clear();
    leader_db_.reset();
  }

  DB* openFollower();

  void closeFollower() {
    follower_cfs_.clear();
    follower_db_.reset();
  }

  Options leaderOptions() const;

  // Returns the number of log records applied
  size_t catchUpFollower();

  WriteOptions wo() const {
    WriteOptions w;
    w.disableWAL = true;
    return w;
  }

  void createColumnFamily(std::string name);
  void deleteColumnFamily(std::string name);

 private:
  std::string test_dir_;
  FollowerEnv follower_env_;

  port::Mutex log_records_mutex_;
  std::vector<ReplicationLogRecord> log_records_;
  int followerSequence_{0};

  std::unique_ptr<DB> leader_db_;
  ColumnFamilyMap leader_cfs_;
  std::unique_ptr<DB> follower_db_;
  ColumnFamilyMap follower_cfs_;
};

Options ReplicationTest::leaderOptions() const {
  Options options;
  options.create_if_missing = true;
  options.atomic_flush = true;
  options.avoid_flush_during_shutdown = true;
  options.write_buffer_size = 100 << 10;
  options.target_file_size_base = 500 << 10;
  options.max_background_jobs = 4;
  options.max_open_files = 500;
  options.max_bytes_for_level_base = 1 << 20;
  return options;
}

DB* ReplicationTest::openLeader() {
  bool firstOpen = log_records_.empty();
  auto dbname = test_dir_ + "/leader";

  std::vector<std::string> cf_names;
  auto s = DB::ListColumnFamilies(Options(), dbname, &cf_names);
  assert(firstOpen == !s.ok());
  if (!s.ok()) {
    cf_names.push_back(kDefaultColumnFamilyName);
  }

  auto options = leaderOptions();
  auto listener =
      std::make_shared<Listener>(&log_records_mutex_, &log_records_);
  options.replication_log_listener = listener;

  listener->setState(firstOpen ? Listener::TAILING : Listener::OPEN);

  std::vector<ColumnFamilyDescriptor> column_families;
  for (auto& name : cf_names) {
    column_families.emplace_back(name, options);
  }

  std::vector<ColumnFamilyHandle*> handles;
  DB* db;
  // open DB
  s = DB::Open(options, dbname, column_families, &handles, &db);
  assert(s.ok());
  leader_db_.reset(db);
  // Follower will sometimes need to access deleted files
  db->DisableFileDeletions();

  for (auto& h : handles) {
    bool inserted = leader_cfs_.try_emplace(h->GetName(), h).second;
    assert(inserted);
  }

  if (!firstOpen) {
    MutexLock lock(&log_records_mutex_);
    listener->setState(Listener::RECOVERY);
    // recover leader
    DB::ApplyReplicationLogRecordInfo info;
    auto leaderSeq = getPersistedSequence(db) + 1;
    for (; leaderSeq < (int)log_records_.size(); ++leaderSeq) {
      s = db->ApplyReplicationLogRecord(log_records_[leaderSeq], &info);
      assert(s.ok());
    }
    listener->setState(Listener::TAILING);
  }

  return db;
}

DB* ReplicationTest::openFollower() {
  auto dbname = test_dir_ + "/follower";

  std::vector<std::string> cf_names;
  auto s = DB::ListColumnFamilies(Options(), dbname, &cf_names);
  if (!s.ok()) {
    cf_names.push_back(kDefaultColumnFamilyName);
  }

  auto options = leaderOptions();
  options.env = &follower_env_;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 100 << 20;

  std::vector<ColumnFamilyDescriptor> column_families;
  for (auto& name : cf_names) {
    column_families.emplace_back(name, options);
  }

  std::vector<ColumnFamilyHandle*> handles;
  DB* db;
  // open DB
  s = DB::Open(options, dbname, column_families, &handles, &db);
  assert(s.ok());
  follower_db_.reset(db);
  followerSequence_ = getPersistedSequence(db) + 1;

  for (auto& h : handles) {
    auto inserted = follower_cfs_.try_emplace(h->GetName(), h).second;
    assert(inserted);
  }

  return db;
}

size_t ReplicationTest::catchUpFollower() {
  MutexLock lock(&log_records_mutex_);
  DB::ApplyReplicationLogRecordInfo info;
  size_t ret = 0;
  for (; followerSequence_ < (int)log_records_.size(); ++followerSequence_) {
    auto s = follower_db_->ApplyReplicationLogRecord(
        log_records_[followerSequence_], &info);
    assert(s.ok());
    ++ret;
  }
  for (auto& cf : info.added_column_families) {
    auto inserted =
        follower_cfs_.try_emplace(cf->GetName(), std::move(cf)).second;
    assert(inserted);
  }
  for (auto& d : info.deleted_column_families) {
    bool found = false;
    for (auto& [name, cf] : follower_cfs_) {
      if (cf->GetID() == d) {
        found = true;
        follower_cfs_.erase(name);
        break;
      }
    }
    assert(found);
  }
  return ret;
}

void ReplicationTest::createColumnFamily(std::string name) {
  ColumnFamilyOptions options(leaderOptions());
  ColumnFamilyHandle* h;
  auto s = leader_db_->CreateColumnFamily(options, name, &h);
  assert(s.ok());
  auto inserted = leader_cfs_.try_emplace(std::move(name), h).second;
  assert(inserted);
}

void ReplicationTest::deleteColumnFamily(std::string name) {
  auto cf = leader_cfs_.find(name);
  assert(cf != leader_cfs_.end());
  auto s = leader_db_->DropColumnFamily(cf->second.get());
  assert(s.ok());
  leader_cfs_.erase(cf);
}

TEST_F(ReplicationTest, Simple) {
  auto leader = openLeader();
  auto follower = openFollower();

  std::string val;

  // Insert key1
  ASSERT_OK(leader->Put(wo(), "key1", "val1"));
  // Catch up follower, expect key1 to propagate
  ASSERT_TRUE(follower->Get(ReadOptions(), "key1", &val).IsNotFound());
  EXPECT_EQ(catchUpFollower(), 1);
  ASSERT_OK(follower->Get(ReadOptions(), "key1", &val));
  EXPECT_EQ(val, "val1");

  // Flush
  EXPECT_EQ(getPersistedSequence(leader), -1);
  ASSERT_OK(leader->Flush(FlushOptions()));
  EXPECT_EQ(getPersistedSequence(leader), 1);

  // Catch up follower, expect flush to propagate
  EXPECT_EQ(getPersistedSequence(follower), -1);
  EXPECT_EQ(getMemtableEntries(follower), 1);
  EXPECT_EQ(catchUpFollower(), 2);
  EXPECT_EQ(getPersistedSequence(follower), 1);
  EXPECT_EQ(getMemtableEntries(follower), 0);

  ASSERT_OK(follower->Get(ReadOptions(), "key1", &val));
  EXPECT_EQ(val, "val1");

  // Create 3 new files, compaction happens.
  // (insert a-z into the first file to prevent trivial move and actually
  // trigger a compaction)
  ASSERT_OK(leader->Put(wo(), "a", "a"));
  ASSERT_OK(leader->Put(wo(), "z", "z"));
  ASSERT_OK(leader->Put(wo(), "key2", "val2"));
  ASSERT_OK(leader->Flush(FlushOptions()));
  ASSERT_OK(leader->Put(wo(), "key3", "val3"));
  ASSERT_OK(leader->Flush(FlushOptions()));
  ASSERT_OK(leader->Put(wo(), "key4", "val4"));
  ASSERT_OK(leader->Flush(FlushOptions()));
  // Wait until compaction finishes
  while (getLevel0FileCount(leader) > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Catch up follower, compaction propagates
  EXPECT_EQ(catchUpFollower(), 12);
  EXPECT_EQ(getLevel0FileCount(follower), 0);

  ASSERT_OK(follower->Get(ReadOptions(), "key2", &val));
  EXPECT_EQ(val, "val2");
  ASSERT_OK(follower->Get(ReadOptions(), "key3", &val));
  EXPECT_EQ(val, "val3");
  ASSERT_OK(follower->Get(ReadOptions(), "key4", &val));
  EXPECT_EQ(val, "val4");

  // Manual compaction
  ASSERT_OK(leader->Put(wo(), "key5", "val5"));
  ASSERT_OK(leader->Flush(FlushOptions()));
  ASSERT_OK(leader->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  EXPECT_EQ(catchUpFollower(), 4);
  EXPECT_EQ(getMemtableEntries(follower), 0);
  EXPECT_EQ(getLevel0FileCount(follower), 0);

  ASSERT_OK(follower->Get(ReadOptions(), "key5", &val));
  EXPECT_EQ(val, "val5");

  ASSERT_OK(leader->Put(wo(), "key6", "val6"));
  EXPECT_EQ(catchUpFollower(), 1);
  ASSERT_OK(follower->Get(ReadOptions(), "key6", &val));
  EXPECT_EQ(val, "val6");

  // Reopen follower
  closeFollower();
  follower = openFollower();
  // Memtable is empty, need to catch up
  ASSERT_TRUE(follower->Get(ReadOptions(), "key6", &val).IsNotFound());
  EXPECT_EQ(catchUpFollower(), 3);
  ASSERT_OK(follower->Get(ReadOptions(), "key6", &val));
  EXPECT_EQ(val, "val6");

  // Reopen leader
  closeLeader();
  leader = openLeader();
  ASSERT_OK(leader->Get(ReadOptions(), "key1", &val));
  EXPECT_EQ(val, "val1");
  ASSERT_OK(leader->Get(ReadOptions(), "key6", &val));
  EXPECT_EQ(val, "val6");

  ASSERT_OK(leader->Put(wo(), "key7", "val7"));
  EXPECT_EQ(catchUpFollower(), 1);
  ASSERT_OK(follower->Get(ReadOptions(), "key7", &val));
  EXPECT_EQ(val, "val7");
  ASSERT_OK(leader->Flush(FlushOptions()));
  EXPECT_EQ(catchUpFollower(), 2);
  ASSERT_OK(follower->Get(ReadOptions(), "key7", &val));
  EXPECT_EQ(val, "val7");
}

TEST_F(ReplicationTest, MultiColumnFamily) {
  std::string val;
  auto leader = openLeader();
  auto follower = openFollower();

  auto cf = [](int i) { return "cf" + std::to_string(i); };

  for (int i = 0; i < 10; ++i) {
    createColumnFamily(cf(i));
  }

  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(leaderColumnFamilies().count(cf(i)), 1);
  }

  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(followerColumnFamilies().count(cf(i)), 0);
  }
  catchUpFollower();
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(followerColumnFamilies().count(cf(i)), 1);
  }

  // Spray some writes
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_OK(leader->Put(wo(), leaderCF(cf(i % 10)), "key" + std::to_string(i),
                          "val" + std::to_string(i)));
  }
  catchUpFollower();
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_OK(follower->Get(ReadOptions(), followerCF(cf(i % 10)),
                            "key" + std::to_string(i), &val));
    EXPECT_EQ(val, "val" + std::to_string(i));
  }

  // Atomic flush, will flush all column families
  ASSERT_OK(leader->Flush(FlushOptions()));

  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_OK(follower->Get(ReadOptions(), followerCF(cf(i % 10)),
                            "key" + std::to_string(i), &val));
    EXPECT_EQ(val, "val" + std::to_string(i));
  }

  // Compact cf3
  ASSERT_OK(leader->Put(wo(), leaderCF(cf(3)), "a", "a"));
  ASSERT_OK(leader->Flush(FlushOptions()));
  ASSERT_OK(leader->Put(wo(), leaderCF(cf(3)), "z", "z"));
  ASSERT_OK(leader->Flush(FlushOptions()));
  ASSERT_OK(leader->CompactRange(CompactRangeOptions(), leaderCF(cf(3)),
                                 nullptr, nullptr));

  catchUpFollower();
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_OK(follower->Get(ReadOptions(), followerCF(cf(i % 10)),
                            "key" + std::to_string(i), &val));
    EXPECT_EQ(val, "val" + std::to_string(i));
  }

  // Reopen follower
  closeFollower();
  follower = openFollower();
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_OK(follower->Get(ReadOptions(), followerCF(cf(i % 10)),
                            "key" + std::to_string(i), &val));
    EXPECT_EQ(val, "val" + std::to_string(i));
  }

  // Reopen leader
  closeLeader();
  leader = openLeader();
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_OK(leader->Get(ReadOptions(), leaderCF(cf(i % 10)),
                          "key" + std::to_string(i), &val));
    EXPECT_EQ(val, "val" + std::to_string(i));
  }

  deleteColumnFamily(cf(9));
  EXPECT_EQ(leaderColumnFamilies().count(cf(9)), 0);
  catchUpFollower();
  EXPECT_EQ(followerColumnFamilies().count(cf(9)), 0);

  // reopen, make sure the cf is still deleted
  closeLeader();
  leader = openLeader();
  closeFollower();
  catchUpFollower();
  follower = openFollower();
  for (int i = 0; i < 9; ++i) {
    EXPECT_EQ(leaderColumnFamilies().count(cf(i)), 1);
    EXPECT_EQ(followerColumnFamilies().count(cf(i)), 1);
  }
  EXPECT_EQ(leaderColumnFamilies().count(cf(9)), 0);
  EXPECT_EQ(followerColumnFamilies().count(cf(9)), 0);
}

TEST_F(ReplicationTest, Stress) {
  std::string val;
  auto leader = openLeader();
  auto follower = openFollower();

  auto cf = [](int i) { return "cf" + std::to_string(i); };

  constexpr auto kThreadCount = 16;
  constexpr auto kColumnFamilyCount = 3;
  constexpr auto kMaxKey = 200000;
  constexpr auto kWritesPerThread = 200000;

  for (int i = 0; i < kColumnFamilyCount; ++i) {
    createColumnFamily(cf(i));
  }

  auto do_writes = [&](int n) {
    auto rand = Random::GetTLSInstance();
    while (n > 0) {
      auto cfi = rand->Uniform(kColumnFamilyCount);
      rocksdb::WriteBatch wb;
      for (size_t i = 0; i < 3; ++i) {
        --n;
        wb.Put(leaderCF(cf(cfi)), std::to_string(rand->Uniform(kMaxKey)),
               std::to_string(rand->Next()));
      }
      ASSERT_OK(leader->Write(wo(), &wb));
    }
  };

  auto verify_equal = [&]() {
    for (int i = 0; i < kColumnFamilyCount; ++i) {
      auto itrLeader = std::unique_ptr<Iterator>(
          leader->NewIterator(ReadOptions(), leaderCF(cf(i))));
      auto itrFollower = std::unique_ptr<Iterator>(
          follower->NewIterator(ReadOptions(), followerCF(cf(i))));
      itrLeader->SeekToFirst();
      itrFollower->SeekToFirst();
      while (itrLeader->Valid() && itrFollower->Valid()) {
        ASSERT_EQ(itrLeader->key(), itrFollower->key());
        ASSERT_EQ(itrLeader->value(), itrFollower->value());
        itrLeader->Next();
        itrFollower->Next();
      }
      ASSERT_TRUE(!itrLeader->Valid() && !itrFollower->Valid());
    }
  };

  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreadCount; ++i) {
    threads.emplace_back([&]() { do_writes(kWritesPerThread); });
  }
  for (auto& t : threads) {
    t.join();
  }
  ASSERT_OK(
      static_cast_with_check<DBImpl>(leader)->TEST_WaitForBackgroundWork());

  catchUpFollower();

  verify_equal();

  // Reopen leader
  closeLeader();
  leader = openLeader();
  ASSERT_OK(leader->Flush(FlushOptions()));

  verify_equal();

  // Reopen follower
  closeFollower();
  follower = openFollower();
  catchUpFollower();

  verify_equal();
}

}  //  namespace ROCKSDB_NAMESPACE

// A black-box test for the cloud wrapper around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
