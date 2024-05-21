// Copyright (c) 2017 Rockset

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <optional>

#include "cloud/filename.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

using LogRecordsVector =
    std::vector<std::pair<ReplicationLogRecord, std::string>>;

class TestReplicationEpochExtractor : public ReplicationEpochExtractor {
 public:
  uint64_t EpochOfReplicationSequence(Slice replication_seq) override {
    uint64_t epoch;
    assert(GetFixed64(&replication_seq, &epoch));
    return epoch;
  }
};

class Listener : public ReplicationLogListener {
 public:
  Listener(port::Mutex* log_records_mutex, LogRecordsVector* log_records)
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
      std::string replication_sequence;
      PutFixed64(&replication_sequence, epoch_);
      PutFixed64(&replication_sequence, log_records_->size());
      log_records_->emplace_back(std::move(record), replication_sequence);
      return replication_sequence;
    }
  }

  void UpdateEpoch(uint64_t epoch) { epoch_ = epoch; }

 private:
  port::Mutex* log_records_mutex_;
  LogRecordsVector* log_records_;
  State state_{OPEN};
  uint64_t epoch_{0};
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
  auto outSlice = Slice(out);
  uint64_t epoch{0}, sequence{0};
  auto ok = GetFixed64(&outSlice, &epoch);
  assert(ok);
  ok = GetFixed64(&outSlice, &sequence);
  assert(ok);
  return (int)sequence;
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

Status countWalFiles(Env* env, const std::string& path, size_t* out) {
  *out = 0;
  std::vector<std::string> files;
  auto s = env->GetChildren(path, &files);
  if (!s.ok()) {
    return s;
  }
  for (const std::string& file : files) {
    uint64_t number = 0;
    FileType type = kInfoLogFile;
    if (!ParseFileName(file, &number, &type)) {
      continue;
    }
    if (type == kWalFile) {
      ++(*out);
    }
  }
  return Status::OK();
}

class ReplicationTest : public testing::Test {
 public:
  ReplicationTest()
      : test_dir_(test::TmpDir()), follower_env_(test_dir_ + "/leader") {
    auto base_env = Env::Default();
    DestroyDir(base_env, test_dir_ + "/leader");
    DestroyDir(base_env, test_dir_ + "/follower");
    base_env->CreateDirIfMissing(test_dir_);
    base_env->NewLogger(test::TmpDir(base_env) + "/replication-test.log",
                        &info_log_);
    info_log_->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);
  }
  ~ReplicationTest() {
    if (getenv("KEEP_DB")) {
      printf("DB is still at %s\n", test_dir_.c_str());
    } else {
      DestroyDir(Env::Default(), test_dir_ + "/leader");
      DestroyDir(Env::Default(), test_dir_ + "/follower");
    }
  }

  std::string leaderPath() const { return test_dir_ + "/leader"; }

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

  ColumnFamilyData* leaderCFD(const std::string& name) const {
    auto* handle = leaderCF(name);
    return static_cast_with_check<ColumnFamilyHandleImpl>(handle)->cfd();
  }
  ColumnFamilyData* followerCFD(const std::string& name) const {
    auto* handle = followerCF(name);
    return static_cast_with_check<ColumnFamilyHandleImpl>(handle)->cfd();
  }

  DB* openLeader() { return openLeader(leaderOptions()); }
  DB* openLeader(Options options, uint64_t snapshot_replication_epoch = 1);
  void closeLeader() {
    leader_cfs_.clear();
    leader_db_.reset();
  }

  DB* openFollower() { return openFollower(leaderOptions()); }
  DB* openFollower(Options options);

  void closeFollower() {
    follower_cfs_.clear();
    follower_db_.reset();
  }

  Options leaderOptions() const;

  // catching up follower for `num_records`. If `num_records` not specified,
  // it will catch up until end of log
  //
  // Returns the number of log records applied
  size_t catchUpFollower(std::optional<size_t> num_records = std::nullopt);

  WriteOptions wo() const {
    WriteOptions w;
    w.disableWAL = true;
    return w;
  }

  void createColumnFamily(std::string name);
  void deleteColumnFamily(std::string name);

  DB* currentLeader() const { return leader_db_.get(); }

  DBImpl* leaderFull() const {
    return static_cast_with_check<DBImpl>(currentLeader());
  }

  DB* currentFollower() const { return follower_db_.get(); }

  DBImpl* followerFull() const {
    return static_cast_with_check<DBImpl>(currentFollower());
  }

  // Check that CFs in leader and follower have the same next_log_num and
  // replication_sequence for all unflushed memtables
  void verifyNextLogNumAndReplSeqConsistency() const {
    for (const auto& cf : leader_cfs_) {
      verifyNextLogNumAndReplSeqConsistency(cf.first);
    }
  }

  void verifyNextLogNumAndReplSeqConsistency(const std::string& name) const {
    auto leader_cfd = leaderCFD(name);
    auto follower_cfd = followerCFD(name);
    ASSERT_TRUE(leader_cfd != nullptr && follower_cfd != nullptr);
    ASSERT_EQ(leader_cfd->imm()->NumNotFlushed(),
              follower_cfd->imm()->NumNotFlushed());
    ASSERT_EQ(leader_cfd->imm()->NumFlushed(),
              follower_cfd->imm()->NumFlushed());
    auto leader_lognums_and_repl_seq =
        leader_cfd->imm()->TEST_GetNextLogNumAndReplSeq();
    auto follower_lognums_and_repl_seq =
        follower_cfd->imm()->TEST_GetNextLogNumAndReplSeq();
    ASSERT_EQ(leader_lognums_and_repl_seq.size(),
              follower_lognums_and_repl_seq.size());
    for (size_t i = 0; i < leader_lognums_and_repl_seq.size(); i++) {
      ASSERT_EQ(leader_lognums_and_repl_seq[i],
                follower_lognums_and_repl_seq[i]);
    }
  }

  std::vector<std::string> getAllKeys(DB* db, ColumnFamilyHandle* cf) {
    auto itr = std::unique_ptr<Iterator>(db->NewIterator(ReadOptions(), cf));

    itr->SeekToFirst();
    std::vector<std::string> keys;
    while (itr->Valid()) {
      keys.push_back(itr->key().ToString());
      itr->Next();
    }
    return keys;
  }

  // verify that the current log structured merge tree of two CFs to be the same
  void verifyLSMTEqual(ColumnFamilyHandle* h1, ColumnFamilyHandle* h2) {
    auto cf1 = static_cast_with_check<ColumnFamilyHandleImpl>(h1)->cfd(),
         cf2 = static_cast_with_check<ColumnFamilyHandleImpl>(h2)->cfd();
    ASSERT_EQ(cf1->NumberLevels(), cf2->NumberLevels())
        << h1->GetName() << ", " << h2->GetName();

    for (int level = 0; level < cf1->NumberLevels(); level++) {
      auto files1 = cf1->current()->storage_info()->LevelFiles(level),
           files2 = cf2->current()->storage_info()->LevelFiles(level);
      ASSERT_EQ(files1.size(), files2.size())
          << "mismatched number of files at level: " << level
          << " between cf: " << cf1->GetName() << " and cf: " << cf2->GetName();
      for (size_t i = 0; i < files1.size(); i++) {
        auto f1 = files1[i], f2 = files2[i];
        ASSERT_EQ(f1->fd.file_size, f2->fd.file_size);
        ASSERT_EQ(f1->fd.smallest_seqno, f2->fd.smallest_seqno);
        ASSERT_EQ(f1->fd.largest_seqno, f2->fd.largest_seqno);
        ASSERT_EQ(f1->epoch_number, f2->epoch_number);
        ASSERT_EQ(f1->file_checksum, f2->file_checksum);
        ASSERT_EQ(f1->unique_id, f2->unique_id);
      }
    }
  }

  void verifyReplicationEpochsEqual() {
    auto leader = leaderFull(), follower = followerFull();
    auto leaderEpochs = leader->GetVersionSet()->TEST_GetReplicationEpochSet(),
         followerEpochs =
             follower->GetVersionSet()->TEST_GetReplicationEpochSet();
    ASSERT_EQ(leaderEpochs, followerEpochs);
  }

  void verifyEqual() {
    ASSERT_EQ(leader_cfs_.size(), follower_cfs_.size());
    auto leader = leader_db_.get(), follower = follower_db_.get();
    for (auto& [name, cf1] : leader_cfs_) {
      auto cf2 = followerCF(name);
      verifyNextLogNumAndReplSeqConsistency(name);
      verifyLSMTEqual(cf1.get(), cf2);

      auto itrLeader = std::unique_ptr<Iterator>(
          leader->NewIterator(ReadOptions(), cf1.get()));
      auto itrFollower =
          std::unique_ptr<Iterator>(follower->NewIterator(ReadOptions(), cf2));
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
  }

 protected:
  void UpdateLeaderEpoch(uint64_t epoch) {
    // assuming leader db is opened
    assert(leader_db_ && listener_);
    leader_db_->UpdateReplicationEpoch(epoch);
    listener_->UpdateEpoch(epoch);
  }

  std::shared_ptr<Logger> info_log_;
  bool replicate_epoch_number_{true};
  bool consistency_check_on_epoch_replication{true};
  void resetFollowerSequence(int new_seq) { followerSequence_ = new_seq; }

 private:
  std::string test_dir_;
  FollowerEnv follower_env_;

  port::Mutex log_records_mutex_;
  LogRecordsVector log_records_;
  int followerSequence_{0};

  std::unique_ptr<DB> leader_db_;
  ColumnFamilyMap leader_cfs_;
  std::unique_ptr<DB> follower_db_;
  ColumnFamilyMap follower_cfs_;
  std::shared_ptr<Listener> listener_;
  uint64_t snapshot_replication_epoch_{0};
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
  options.info_log = info_log_;
  options.replication_epoch_extractor =
      std::make_shared<TestReplicationEpochExtractor>();
  return options;
}

DB* ReplicationTest::openLeader(Options options, uint64_t snapshot_replication_epoch) {
  bool firstOpen = log_records_.empty();
  auto dbname = test_dir_ + "/leader";

  std::vector<std::string> cf_names;
  auto s = DB::ListColumnFamilies(Options(), dbname, &cf_names);
  assert(firstOpen == !s.ok());
  if (!s.ok()) {
    cf_names.push_back(kDefaultColumnFamilyName);
  }

  listener_ = std::make_shared<Listener>(&log_records_mutex_, &log_records_);
  options.replication_log_listener = listener_;

  listener_->setState(firstOpen ? Listener::TAILING : Listener::OPEN);

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

  snapshot_replication_epoch_ = snapshot_replication_epoch;
  if (!firstOpen) {
    MutexLock lock(&log_records_mutex_);
    listener_->setState(Listener::RECOVERY);
    // recover leader
    DB::ApplyReplicationLogRecordInfo info;
    auto leaderSeq = getPersistedSequence(db) + 1;
    for (; leaderSeq < (int)log_records_.size(); ++leaderSeq) {
      s = db->ApplyReplicationLogRecord(
          log_records_[leaderSeq].first, log_records_[leaderSeq].second,
          [this](Slice) { return ColumnFamilyOptions(leaderOptions()); },
          snapshot_replication_epoch_, &info, DB::AR_EVICT_OBSOLETE_FILES);
      assert(s.ok());
      assert(!info.diverged_manifest_writes);
    }
    listener_->setState(Listener::TAILING);
  }

  return db;
}

DB* ReplicationTest::openFollower(Options options) {
  auto dbname = test_dir_ + "/follower";

  std::vector<std::string> cf_names;
  auto s = DB::ListColumnFamilies(Options(), dbname, &cf_names);
  if (!s.ok()) {
    cf_names.push_back(kDefaultColumnFamilyName);
  }

  options.env = &follower_env_;
  options.disable_auto_compactions = true;
  // write buffer size of follower is much smaller than leader to help verify
  // that disable_auto_flush works as expected
  options.write_buffer_size = 10 << 10;
  options.disable_auto_flush = true;

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

size_t ReplicationTest::catchUpFollower(std::optional<size_t> num_records) {
  MutexLock lock(&log_records_mutex_);
  DB::ApplyReplicationLogRecordInfo info;
  size_t ret = 0;
  unsigned flags = DB::AR_EVICT_OBSOLETE_FILES;
  for (; followerSequence_ < (int)log_records_.size(); ++followerSequence_) {
    if (num_records && ret >= *num_records) {
      break;
    }
    auto s = follower_db_->ApplyReplicationLogRecord(
        log_records_[followerSequence_].first,
        log_records_[followerSequence_].second,
        [this](Slice) {
          return ColumnFamilyOptions(follower_db_->GetOptions());
        },
        snapshot_replication_epoch_,
        &info, flags);
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

  uint64_t followerManifestUpdateSeq;
  uint64_t leaderManifestUpdateSeq;
  follower->GetManifestUpdateSequence(&followerManifestUpdateSeq);
  leader->GetManifestUpdateSequence(&leaderManifestUpdateSeq);
  EXPECT_EQ(followerManifestUpdateSeq, 14);
  EXPECT_EQ(leaderManifestUpdateSeq, 14);
}

TEST_F(ReplicationTest, ReproSYS3320) {
  auto leader = openLeader();
  auto follower = openFollower();

  leader->SetOptions({{"disable_auto_compactions", "false"}});

  // Insert key1
  ASSERT_OK(leader->Put(wo(), "key1", "val1"));

  // Flush
  ASSERT_OK(leader->Flush(FlushOptions()));

  // Catch up follower, expect flush to propagate
  EXPECT_EQ(catchUpFollower(), 4);

  uint64_t max_file_number = 0;
  std::vector<LiveFileMetaData> files;
  follower->GetLiveFilesMetaData(&files);
  for (auto& f : files) {
    max_file_number = std::max(max_file_number, TableFileNameToNumber(f.name));
  }

  ASSERT_LT(max_file_number, follower->GetNextFileNumber());
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

  {
    // Make sure that CF5 was also flushed, even though it wasn't in the list of
    // CFs in the call to Flush() above
    ReadOptions ro;
    ro.read_tier = kPersistedTier;
    ASSERT_OK(leader->Get(ro, leaderCF(cf(5)), "key5", &val));
    EXPECT_EQ(val, "val5");
  }

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
  {  // expect only one WAL
    size_t walFileCount{0};
    ASSERT_OK(countWalFiles(Env::Default(), leaderPath(), &walFileCount));
    EXPECT_EQ(1, walFileCount);
  }
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

// Test that we never switch empty memtables on both leader and follower
TEST_F(ReplicationTest, SwitchEmptyMemTable) {
  auto openOptions = leaderOptions();
  openOptions.max_write_buffer_number = 3;

  auto leader = openLeader(openOptions);
  openFollower(openOptions);

  auto leaderFull = static_cast_with_check<DBImpl>(leader);
  leaderFull->PauseBackgroundWork();

  // Empty memtable is not expected to be switched
  FlushOptions flushOpts;
  flushOpts.wait = false;
  leader->Flush(flushOpts);
  catchUpFollower();
  ASSERT_EQ(0, leaderCFD("default")->imm()->NumNotFlushed());
  ASSERT_EQ(0, followerCFD("default")->imm()->NumNotFlushed());

  leader->Put(wo(), "key1", "val1");
  leader->Flush(flushOpts);
  catchUpFollower();

  ASSERT_EQ(1, leaderCFD("default")->imm()->NumNotFlushed());
  ASSERT_EQ(1, followerCFD("default")->imm()->NumNotFlushed());

  // Empty memtable is not expected to be switched
  leader->Flush(flushOpts);
  catchUpFollower();
  ASSERT_EQ(1, leaderCFD("default")->imm()->NumNotFlushed());
  ASSERT_EQ(1, followerCFD("default")->imm()->NumNotFlushed());
}

// Reproduces SYS-3423
TEST_F(ReplicationTest, FollowerDeletesMemtables) {
  auto leader = openLeader();
  auto follower = openFollower();

  auto cf = [](int i) { return "cf" + std::to_string(i); };

  for (int i = 0; i < 5; ++i) {
    createColumnFamily(cf(i));
  }

  // Fill up column families in order
  for (int cfi = 0; cfi < 5; cfi++) {
    for (size_t i = 0; i < 1000; ++i) {
      ASSERT_OK(leader->Put(wo(), leaderCF(cf(cfi)), "key" + std::to_string(i),
                            "val" + std::to_string(i)));
    }
    ASSERT_OK(leader->Flush(FlushOptions()));
  }
  catchUpFollower();

  {
    auto fdb = static_cast_with_check<DBImpl>(leader);
    auto cfs = fdb->GetVersionSet()->GetColumnFamilySet();
    for (auto cfd : *cfs) {
      // Everything is flushed immutable memtables shouldn't use any memory
      EXPECT_EQ(0, *cfd->imm()->current_memory_usage());
    }
  }

  uint64_t leaderValue{0};
  ASSERT_TRUE(leader->GetAggregatedIntProperty(
      DB::Properties::kLiveSstFilesSize, &leaderValue));
  uint64_t followerValue{0};
  ASSERT_TRUE(follower->GetAggregatedIntProperty(
      DB::Properties::kLiveSstFilesSize, &followerValue));
  EXPECT_EQ(leaderValue, followerValue);
}

// Reproduces SYS-4535
TEST_F(ReplicationTest, MultiCFFlushCorrectReplicationSequence) {
  auto leaderOpenOptions = leaderOptions();

  leaderOpenOptions.write_buffer_size = 64 << 10;
  leaderOpenOptions.max_write_buffer_number = 4;
  auto leader = openLeader(leaderOpenOptions);

  auto leaderFull = static_cast_with_check<DBImpl>(leader);
  leaderFull->PauseBackgroundWork();

  createColumnFamily("cf");

  // Add one key to the non-default column family
  ASSERT_OK(leader->Put(wo(), leaderCF("cf"), "cfkey", "cfvalue"));

  constexpr auto kKeyCount = 4000;
  for (size_t i = 0; i < kKeyCount; ++i) {
    ASSERT_OK(leader->Put(wo(), "key" + std::to_string(i),
                          "val" + std::to_string(i)));
  }
  closeLeader();

  leaderOpenOptions.disable_auto_flush = true;
  leader = openLeader(leaderOpenOptions);

  leader->SetOptions({{"disable_auto_flush", "false"}});
  FlushOptions fo;
  fo.wait = true;
  fo.allow_write_stall = true;
  ASSERT_OK(leader->Flush(fo));

  closeLeader();

  leader = openLeader(leaderOpenOptions);

  std::string val;
  for (size_t i = 0; i < kKeyCount; ++i) {
    ASSERT_OK(leader->Get(ReadOptions(), "key" + std::to_string(i), &val));
    EXPECT_EQ(val, "val" + std::to_string(i));
  }
}

class TestEventListener : public EventListener {
 public:
  explicit TestEventListener(ReplicationTest* testInstance)
      : testInstance_(testInstance) {}
  void OnFlushCompleted(DB*, const FlushJobInfo& info) override {
    ASSERT_EQ(info.smallest_seqno, info.largest_seqno);
    if (info.smallest_seqno == seq1) {  // the first memtable is flushed
      testInstance_->catchUpFollower();
      ASSERT_EQ(1, testInstance_->leaderCFD("default")->imm()->NumNotFlushed());
      ASSERT_EQ(1,
                testInstance_->followerCFD("default")->imm()->NumNotFlushed());
      testInstance_->verifyNextLogNumAndReplSeqConsistency();
    } else if (info.smallest_seqno == seq2) {  // the second memtable is flushed
      testInstance_->catchUpFollower();
      ASSERT_EQ(0, testInstance_->leaderCFD("default")->imm()->NumNotFlushed());
      ASSERT_EQ(0,
                testInstance_->followerCFD("default")->imm()->NumNotFlushed());
    }
  }

  std::atomic<SequenceNumber> seq1{0};
  std::atomic<SequenceNumber> seq2{0};

 private:
  ReplicationTest* testInstance_;
};

// Verifies next_log_num of memtables in `imm` is consistent between leader and
// follower.
//
// When a memtable is flushed on leader, and follower gets the manifest updates,
// follower will try to remove all the flushed memtables. This test verifies
// that follower only removes the flushed memtables
//
// We first create two unflushed memtables on both leader and follower, then
// flush the memtables one by one. Whenever a memtable is flushed, we catch the
// event and check the next log number consistency between leader and follower
TEST_F(ReplicationTest, NextLogNumConsistency) {
  auto leaderOpenOptions = leaderOptions();

  // We need to maintain at most 3 memtables(1 active, 2 unflushed)
  leaderOpenOptions.max_write_buffer_number = 3;

  // event listener helps catch the flush complete events
  std::shared_ptr<TestEventListener> eventListener =
      std::make_shared<TestEventListener>(this);
  leaderOpenOptions.listeners.push_back(eventListener);
  auto leader = openLeader(leaderOpenOptions);
  auto leaderFull = static_cast_with_check<DBImpl>(leader);
  leaderFull->PauseBackgroundWork();

  // Following flushes will create two unflushed memtables, we rely on the
  // sequence number to figure out which memtable is flushed
  leader->Put(wo(), "key1", "val1");
  eventListener->seq1 = leader->GetLatestSequenceNumber();
  FlushOptions flushOpts;
  flushOpts.wait = false;
  leader->Flush(flushOpts);
  leader->Put(wo(), "key2", "val2");
  eventListener->seq2 = leader->GetLatestSequenceNumber();
  leader->Flush(flushOpts);

  auto followerOpenOptions = leaderOptions();
  followerOpenOptions.max_write_buffer_number = 3;

  openFollower(followerOpenOptions);
  catchUpFollower();

  for (auto cfd : {leaderCFD("default"), followerCFD("default")}) {
    ASSERT_EQ(2, cfd->imm()->NumNotFlushed());
  }
  verifyNextLogNumAndReplSeqConsistency();

  leaderFull->ContinueBackgroundWork();
  leaderFull->TEST_WaitForBackgroundWork();
}

// Verify the file number consistency between leader and follower
TEST_F(ReplicationTest, FileNumConsistency) {
  auto kMaxWritebufferNum = 3;
  auto leaderOpenOptions = leaderOptions();
  leaderOpenOptions.max_write_buffer_number = kMaxWritebufferNum;
  auto followerOpenOptions = leaderOptions();
  followerOpenOptions.max_write_buffer_number = kMaxWritebufferNum;

  auto leader = openLeader(leaderOpenOptions);
  auto follower = openFollower(followerOpenOptions);

  EXPECT_EQ(leader->GetNextFileNumber(), follower->GetNextFileNumber());

  leader->Put(wo(), "key1", "val1");
  catchUpFollower();

  EXPECT_EQ(leader->GetNextFileNumber(), follower->GetNextFileNumber());

  // First time we flush, so we will create a new manifest file
  leader->Flush({});
  // Follower will create new manifest file as well.
  catchUpFollower();

  // Rolling manifest file on follower causes the next file number
  // on follower to be greater than the file number on leader.
  // Next manifest write/memtable switch will reset the file number
  EXPECT_LT(leader->GetNextFileNumber(), follower->GetNextFileNumber());

  // next flush reset file number
  leader->Put(wo(), "key2", "val2");
  leader->Flush({});
  catchUpFollower();
  EXPECT_EQ(leader->GetNextFileNumber(), follower->GetNextFileNumber());

  leader->PauseBackgroundWork();

  leader->Put(wo(), "key2", "val2");
  catchUpFollower();

  EXPECT_EQ(leader->GetNextFileNumber(), follower->GetNextFileNumber());

  FlushOptions flushOpts;
  flushOpts.wait = false;
  leader->Flush(flushOpts);
  catchUpFollower();

  // file number consistent for mem switch
  EXPECT_EQ(leader->GetNextFileNumber(), follower->GetNextFileNumber());

  leader->ContinueBackgroundWork();

  auto leaderFull = static_cast_with_check<DBImpl>(leader);
  leaderFull->TEST_WaitForBackgroundWork();
  catchUpFollower();

  EXPECT_EQ(leader->GetNextFileNumber(), follower->GetNextFileNumber());
}

// Verify that when follower tries to catch up, the file number doesn't
// go backwards
TEST_F(ReplicationTest, NextFileNumDoNotGoBackwards) {
  auto leader = openLeader(leaderOptions());
  leader->Put(wo(), "key1", "val1");
  leader->Flush({});
  auto follower = openFollower(leaderOptions());
  catchUpFollower();
  auto next_file_number = follower->GetNextFileNumber();

  // catching up again from last memtable switch record
  resetFollowerSequence(1);

  // memtable switch
  ASSERT_GT(catchUpFollower(1), 0);
  EXPECT_EQ(follower->GetNextFileNumber(), next_file_number);
  // manifest write
  ASSERT_GT(catchUpFollower(1), 0);
  EXPECT_EQ(follower->GetNextFileNumber(), next_file_number);

  leader->Put(wo(), "key2", "val2");
  leader->Flush({});
  catchUpFollower();

  EXPECT_GT(follower->GetNextFileNumber(), next_file_number);
}

TEST_F(ReplicationTest, LogNumberDontGoBackwards) {
  auto leader = openLeader(), follower = openFollower();

  ASSERT_OK(leader->Put(wo(), "k1", "v1"));
  ASSERT_OK(leader->Flush({}));
  catchUpFollower();

  leader->PauseBackgroundWork();

  leader->Put(wo(), "k2", "v2");
  FlushOptions flushOpts;
  flushOpts.wait = false;
  flushOpts.allow_write_stall = true;
  leader->Flush(flushOpts);

  catchUpFollower();
  auto leaderFull = static_cast_with_check<DBImpl>(leader);
  auto followerFull = static_cast_with_check<DBImpl>(follower);

  auto logNum = followerFull->TEST_GetCurrentLogNumber();
  auto minLogNumberToKeep =
      followerFull->GetVersionSet()->min_log_number_to_keep();
  EXPECT_GE(logNum, minLogNumberToKeep);

  leader->Put(wo(), "k3", "v3");
  leader->Flush(flushOpts);

  catchUpFollower();
  logNum = followerFull->TEST_GetCurrentLogNumber();
  minLogNumberToKeep = followerFull->GetVersionSet()->min_log_number_to_keep();
  EXPECT_GE(logNum, minLogNumberToKeep);

  leader->ContinueBackgroundWork();
  leaderFull->TEST_WaitForBackgroundWork();

  // kManifestWrite
  EXPECT_EQ(catchUpFollower(1), 1);

  EXPECT_GE(followerFull->TEST_GetCurrentLogNumber(), logNum);
  logNum = followerFull->TEST_GetCurrentLogNumber();
  minLogNumberToKeep = followerFull->GetVersionSet()->min_log_number_to_keep();
  EXPECT_GE(logNum, minLogNumberToKeep);

  // kManifestWrite
  EXPECT_EQ(catchUpFollower(), 1);

  EXPECT_GE(followerFull->TEST_GetCurrentLogNumber(), logNum);
  logNum = followerFull->TEST_GetCurrentLogNumber();
  minLogNumberToKeep = followerFull->GetVersionSet()->min_log_number_to_keep();
  EXPECT_GE(logNum, minLogNumberToKeep);
}

// Memtable switch record won't be generated if all memtables are empty
TEST_F(ReplicationTest, NoMemSwitchRecordIfEmpty) {
  auto leader = openLeader();
  openFollower();

  auto cf = [](int i) { return "cf" + std::to_string(i); };

  for (int i = 0; i < 10; ++i) {
    createColumnFamily(cf(i));
  }

  EXPECT_GT(catchUpFollower(), 0);

  ASSERT_OK(leader->Flush({}));
  // no mem switch record
  EXPECT_EQ(catchUpFollower(), 0);
}

TEST_F(ReplicationTest, EvictObsoleteFiles) {
  auto leader = openLeader();
  leader->EnableFileDeletions();
  auto followerOptions = leaderOptions();
  followerOptions.disable_delete_obsolete_files_on_open = true;
  auto follower = openFollower(followerOptions);

  ASSERT_OK(leader->Put(wo(), "key0", "val0"));
  ASSERT_OK(leader->Flush(FlushOptions()));
  EXPECT_EQ(catchUpFollower(), 3);
  ASSERT_OK(leader->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ(catchUpFollower(), 1);

  ASSERT_OK(leader->Put(wo(), "key1", "val1"));
  ASSERT_OK(leader->Put(wo(), "key3", "val3"));
  ASSERT_OK(leader->Flush(FlushOptions()));
  EXPECT_EQ(catchUpFollower(), 4);
  ASSERT_OK(leader->Put(wo(), "key2", "val2"));
  ASSERT_OK(leader->Flush(FlushOptions()));
  EXPECT_EQ(catchUpFollower(), 3);

  ASSERT_OK(leader->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ(catchUpFollower(), 1);

  EXPECT_EQ(
      2,
      static_cast_with_check<DBImpl>(leader)->TEST_table_cache()->GetUsage());
  EXPECT_EQ(
      2,
      static_cast_with_check<DBImpl>(follower)->TEST_table_cache()->GetUsage());
}

TEST_F(ReplicationTest, Stress) {
  std::string val;
  auto leader = openLeader();
  openFollower();

  auto cf = [](int i) { return "cf" + std::to_string(i); };

  constexpr auto kThreadCount = 16;
  constexpr auto kColumnFamilyCount = 3;
  constexpr auto kMaxKey = 200000;
  constexpr auto kWritesPerThread = 200000;

  for (int i = 0; i < kColumnFamilyCount; ++i) {
    createColumnFamily(cf(i));
  }

  auto do_writes = [&]() {
    auto writes_per_thread = [&](int n) {
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

    std::vector<std::thread> threads;
    for (size_t i = 0; i < kThreadCount; ++i) {
      threads.emplace_back([&]() { writes_per_thread(kWritesPerThread); });
    }
    for (auto& t : threads) {
      t.join();
    }

    ASSERT_OK(leaderFull()->TEST_WaitForBackgroundWork());
  };

  auto verifyNextEpochNumber = [&]() {
    for (int i = 0; i < kColumnFamilyCount; i++) {
      auto cf1 = leaderCFD(cf(i)), cf2 = followerCFD(cf(i));
      ASSERT_EQ(cf1->GetNextEpochNumber(), cf2->GetNextEpochNumber());
    }
  };

  do_writes();

  catchUpFollower();
  verifyEqual();
  verifyNextEpochNumber();

  ROCKS_LOG_INFO(info_log_, "reopen leader");

  // Reopen leader
  closeLeader();
  leader = openLeader();
  // memtable might not be empty after reopening leader, since we recover
  // replication log when opening it.
  ASSERT_OK(leader->Flush({}));
  ASSERT_OK(leaderFull()->TEST_WaitForBackgroundWork());
  catchUpFollower();
  verifyEqual();

  do_writes();

  ROCKS_LOG_INFO(info_log_, "reopen follower");

  // Reopen follower
  closeFollower();
  openFollower();
  catchUpFollower();

  verifyEqual();
  verifyNextEpochNumber();
}

TEST_F(ReplicationTest, DeleteRange) {
  auto leader = openLeader();
  openFollower();
  auto cf = [](int i) { return "cf" + std::to_string(i); };
  constexpr auto kColumnFamilyCount = 2;
  constexpr auto kNumKeys = 100;
  for (int i = 0; i < kColumnFamilyCount; ++i) {
    createColumnFamily(cf(i));
  }

  auto writeKey = [&](const std::string& key) {
    rocksdb::WriteBatch wb;
    for (int j = 0; j < kColumnFamilyCount; j++) {
      wb.Put(leaderCF(cf(j)), key, key);
    }
    ASSERT_OK(leader->Write(wo(), &wb));
  };

  for (int i = 0; i < kNumKeys / 2; i++) {
    writeKey(std::to_string(i));
  }

  leader->Flush({});

  for (int i = kNumKeys / 2; i < kNumKeys; i++) {
    writeKey(std::to_string(i));
  }

  ASSERT_OK(leader->DeleteRange(wo(), leaderCF(cf(0)), "2", "3"));
  ASSERT_OK(leader->DeleteRange(wo(), leaderCF(cf(0)), "5", "6"));

  catchUpFollower();

  EXPECT_EQ(getAllKeys(leader, leaderCF(cf(0))).size(), 78);
  verifyEqual();

  leader->Flush({});
  catchUpFollower();

  EXPECT_EQ(getAllKeys(leader, leaderCF(cf(0))).size(), 78);
  verifyEqual();
}

TEST_F(ReplicationTest, EpochNumberSimple) {
  auto options = leaderOptions();
  options.disable_auto_compactions = true;
  auto leader = openLeader();
  openFollower();

  ASSERT_OK(leader->Put(wo(), "k1", "v1"));
  ASSERT_OK(leader->Flush({}));
  catchUpFollower();

  ASSERT_OK(leader->Put(wo(), "k1", "v2"));
  ASSERT_OK(leader->Flush({}));
  auto leaderFull = static_cast_with_check<DBImpl>(leader);
  ASSERT_OK(leaderFull->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));

  catchUpFollower();

  verifyEqual();
}

TEST_F(ReplicationTest, SuperSnapshot) {
  auto options = leaderOptions();
  options.disable_auto_compactions = true;
  auto leader = openLeader();
  auto follower = openFollower();

  createColumnFamily("cf1");

  ASSERT_OK(leader->Put(wo(), "k1", "v1"));
  ASSERT_OK(leader->Put(wo(), leaderCF("cf1"), "cf1k1", "cf1v1"));
  ASSERT_OK(leader->Flush({}));
  catchUpFollower();

  std::vector<ColumnFamilyHandle*> cf;
  cf.push_back(follower->DefaultColumnFamily());
  cf.push_back(followerCF("cf1"));
  std::vector<const Snapshot*> snapshots;
  ASSERT_OK(follower->GetSuperSnapshots(cf, &snapshots));

  ASSERT_OK(leader->Put(wo(), "k1", "v2"));
  ASSERT_OK(leader->Put(wo(), leaderCF("cf1"), "cf1k1", "cf1v2"));
  ASSERT_OK(leader->Flush({}));
  auto leaderFull = static_cast_with_check<DBImpl>(leader);
  ASSERT_OK(leaderFull->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
  ASSERT_OK(leaderFull->TEST_CompactRange(0, nullptr, nullptr, leaderCF("cf1"),
                                          true));

  catchUpFollower();

  ReadOptions ro;
  std::string val;
  ASSERT_OK(follower->Get(ro, "k1", &val));
  EXPECT_EQ(val, "v2");
  ro.snapshot = snapshots[0];
  ASSERT_OK(follower->Get(ro, "k1", &val));
  EXPECT_EQ(val, "v1");

  auto iter = std::unique_ptr<rocksdb::Iterator>(
      follower->NewIterator(ro, follower->DefaultColumnFamily()));
  iter->SeekToFirst();
  EXPECT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "k1");
  EXPECT_EQ(iter->value(), "v1");
  iter->Next();
  EXPECT_FALSE(iter->Valid());
  iter.reset();

  ro.snapshot = nullptr;
  ASSERT_OK(follower->Get(ro, followerCF("cf1"), "cf1k1", &val));
  EXPECT_EQ(val, "cf1v2");
  ro.snapshot = snapshots[1];
  ASSERT_OK(follower->Get(ro, followerCF("cf1"), "cf1k1", &val));
  EXPECT_EQ(val, "cf1v1");

  // Test MultiGet
  std::vector<Slice> keys;
  keys.push_back("cf1k1");
  keys.push_back("missing");
  std::vector<ColumnFamilyHandle*> cfs;
  cfs.push_back(followerCF("cf1"));
  cfs.push_back(followerCF("cf1"));
  std::vector<std::string> vals;

  // Multiget on cf1, snapshot
  ro.snapshot = snapshots[1];
  auto statuses = follower->MultiGet(ro, cfs, keys, &vals);
  ASSERT_OK(statuses[0]);
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_EQ(vals[0], "cf1v1");

  // Multiget on cf1, no snapshot
  ro.snapshot = nullptr;
  statuses = follower->MultiGet(ro, cfs, keys, &vals);
  ASSERT_OK(statuses[0]);
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_EQ(vals[0], "cf1v2");

  // Multiget on default column family, snapshot, pinnable values
  ro.snapshot = snapshots[0];
  keys[0] = "k1";
  std::vector<PinnableSlice> pinnableValues;
  pinnableValues.resize(2);
  follower->MultiGet(ro, follower->DefaultColumnFamily(), 2, keys.data(),
                     pinnableValues.data(), statuses.data(), true);
  ASSERT_OK(statuses[0]);
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_EQ(pinnableValues[0], "v1");
  pinnableValues.clear();

  // Column family <-> snapshot mismatch
  ASSERT_FALSE(follower->Get(ro, followerCF("cf1"), "cf1k1", &val).ok());

  follower->ReleaseSnapshot(snapshots[0]);
  follower->ReleaseSnapshot(snapshots[1]);
}

TEST_F(ReplicationTest, ReplicationEpochs) {
  auto options = leaderOptions();
  options.disable_auto_compactions = true;
  auto leader = openLeader(options);
  openFollower();

  ASSERT_TRUE(
      leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().empty());
  ASSERT_TRUE(
      followerFull()->GetVersionSet()->TEST_GetReplicationEpochSet().empty());

  auto cf = [](int i) { return "cf" + std::to_string(i); };

  createColumnFamily(cf(0));

  ASSERT_OK(leader->Put(wo(), leaderCF(cf(0)), "k1", "v1"));
  ASSERT_OK(leader->Flush(FlushOptions()));

  catchUpFollower();

  ASSERT_TRUE(
      leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().empty());
  ASSERT_TRUE(
      followerFull()->GetVersionSet()->TEST_GetReplicationEpochSet().empty());

  UpdateLeaderEpoch(2);

  ASSERT_TRUE(
      leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().empty());

  ASSERT_OK(leader->Put(wo(), leaderCF(cf(0)), "k2", "v2"));
  ASSERT_OK(leader->Flush(FlushOptions(), leaderCF(cf(0))));

  catchUpFollower();

  ASSERT_EQ(leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().size(),
            1);
  verifyReplicationEpochsEqual();

  UpdateLeaderEpoch(3);
  ASSERT_OK(leader->Put(wo(), leaderCF(cf(0)), "k3", "v3"));
  ASSERT_OK(leader->Flush(FlushOptions(), leaderCF(cf(0))));

  catchUpFollower();
  ASSERT_EQ(leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().size(),
            1);
  verifyReplicationEpochsEqual();

  closeLeader();
  leader = openLeader(options);

  ASSERT_EQ(leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().size(),
            1);
}

TEST_F(ReplicationTest, MaxNumReplicationEpochs) {
  auto options = leaderOptions();
  options.disable_auto_compactions = true;
  options.disable_auto_flush = true;
  // maintain at most two replication epochs in the set
  options.max_num_replication_epochs = 2;
  auto leader = openLeader(options, 1 /* snapshot_replication_epoch */);
  openFollower(options);

  auto cf = [](int i) { return "cf" + std::to_string(i); };

  createColumnFamily(cf(0));

  UpdateLeaderEpoch(2);

  ASSERT_OK(leader->Put(wo(), leaderCF(cf(0)), "k1", "v1"));
  ASSERT_OK(leader->Flush(FlushOptions(), leaderCF(cf(0))));

  ASSERT_EQ(leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().size(),
            1);

  catchUpFollower();
  verifyReplicationEpochsEqual();

  UpdateLeaderEpoch(3);
  // generate some manifest writes without changing persisted replication
  // sequence
  ASSERT_OK(leader->SetOptions(leaderCF(cf(0)), {{"max_write_buffer_number", "3"}}));

  ASSERT_EQ(leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().size(),
            2);
  
  UpdateLeaderEpoch(4);
  // generate some manifest writes without changing persisted replication
  // sequence
  ASSERT_OK(leader->SetOptions(leaderCF(cf(0)), {{"max_write_buffer_number", "2"}}));

  ASSERT_EQ(leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().size(),
            2);
  ASSERT_EQ(leaderFull()
                ->GetVersionSet()
                ->TEST_GetReplicationEpochSet()
                .GetSmallestEpoch(),
            3);

  catchUpFollower();
  verifyReplicationEpochsEqual();

  closeLeader();
  leader = openLeader(options, 4 /* snapshot_replication_epoch */);
  ASSERT_EQ(leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().size(),
            2);
  ASSERT_EQ(leaderFull()->GetVersionSet()->TEST_GetReplicationEpochSet().GetSmallestEpoch(),
            3);
}

}  //  namespace ROCKSDB_NAMESPACE

// A black-box test for the cloud wrapper around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
