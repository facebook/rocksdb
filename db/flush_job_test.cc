//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <map>
#include <string>

#include "db/column_family.h"
#include "db/flush_job.h"
#include "db/version_set.h"
#include "rocksdb/cache.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/mock_table.h"
#include "util/file_reader_writer.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

// TODO(icanadi) Mock out everything else:
// 1. VersionSet
// 2. Memtable
class FlushJobTest : public testing::Test {
 public:
  FlushJobTest()
      : env_(Env::Default()),
        dbname_(test::TmpDir() + "/flush_job_test"),
        options_(),
        db_options_(options_),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        versions_(new VersionSet(dbname_, &db_options_, env_options_,
                                 table_cache_.get(), &write_buffer_manager_,
                                 &write_controller_)),
        shutting_down_(false),
        mock_table_factory_(new mock::MockTableFactory()) {
    EXPECT_OK(env_->CreateDirIfMissing(dbname_));
    db_options_.db_paths.emplace_back(dbname_,
                                      std::numeric_limits<uint64_t>::max());
    db_options_.statistics = rocksdb::CreateDBStatistics();
    // TODO(icanadi) Remove this once we mock out VersionSet
    NewDB();
    std::vector<ColumnFamilyDescriptor> column_families;
    cf_options_.table_factory = mock_table_factory_;
    column_families.emplace_back(kDefaultColumnFamilyName, cf_options_);

    EXPECT_OK(versions_->Recover(column_families, false));
  }

  void NewDB() {
    VersionEdit new_db;
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::string manifest = DescriptorFileName(dbname_, 1);
    unique_ptr<WritableFile> file;
    Status s = env_->NewWritableFile(
        manifest, &file, env_->OptimizeForManifestWrite(env_options_));
    ASSERT_OK(s);
    unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(file), EnvOptions()));
    {
      log::Writer log(std::move(file_writer), 0, false);
      std::string record;
      new_db.EncodeTo(&record);
      s = log.AddRecord(record);
    }
    ASSERT_OK(s);
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1, nullptr);
  }

  Env* env_;
  std::string dbname_;
  EnvOptions env_options_;
  Options options_;
  ImmutableDBOptions db_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  ColumnFamilyOptions cf_options_;
  std::unique_ptr<VersionSet> versions_;
  InstrumentedMutex mutex_;
  std::atomic<bool> shutting_down_;
  std::shared_ptr<mock::MockTableFactory> mock_table_factory_;
};

TEST_F(FlushJobTest, Empty) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  EventLogger event_logger(db_options_.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;  // not relavant
  FlushJob flush_job(
      dbname_, versions_->GetColumnFamilySet()->GetDefault(), db_options_,
      *cfd->GetLatestMutableCFOptions(), env_options_, versions_.get(), &mutex_,
      &shutting_down_, {}, kMaxSequenceNumber, snapshot_checker, &job_context,
      nullptr, nullptr, nullptr, kNoCompression, nullptr, &event_logger, false);
  {
    InstrumentedMutexLock l(&mutex_);
    flush_job.PickMemTable();
    ASSERT_OK(flush_job.Run());
  }
  job_context.Clean();
}

TEST_F(FlushJobTest, NonEmpty) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto new_mem = cfd->ConstructNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                           kMaxSequenceNumber);
  new_mem->Ref();
  auto inserted_keys = mock::MakeMockFile();
  // Test data:
  //   seqno [    1,    2 ... 8998, 8999, 9000, 9001, 9002 ... 9999 ]
  //   key   [ 1001, 1002 ... 9998, 9999,    0,    1,    2 ...  999 ]
  //   range-delete "9995" -> "9999" at seqno 10000
  for (int i = 1; i < 10000; ++i) {
    std::string key(ToString((i + 1000) % 10000));
    std::string value("value" + key);
    new_mem->Add(SequenceNumber(i), kTypeValue, key, value);
    if ((i + 1000) % 10000 < 9995) {
      InternalKey internal_key(key, SequenceNumber(i), kTypeValue);
      inserted_keys.insert({internal_key.Encode().ToString(), value});
    }
  }
  new_mem->Add(SequenceNumber(10000), kTypeRangeDeletion, "9995", "9999a");
  InternalKey internal_key("9995", SequenceNumber(10000), kTypeRangeDeletion);
  inserted_keys.insert({internal_key.Encode().ToString(), "9999a"});

  autovector<MemTable*> to_delete;
  cfd->imm()->Add(new_mem, &to_delete);
  for (auto& m : to_delete) {
    delete m;
  }

  EventLogger event_logger(db_options_.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;  // not relevant
  FlushJob flush_job(dbname_, versions_->GetColumnFamilySet()->GetDefault(),
                     db_options_, *cfd->GetLatestMutableCFOptions(),
                     env_options_, versions_.get(), &mutex_, &shutting_down_,
                     {}, kMaxSequenceNumber, snapshot_checker, &job_context,
                     nullptr, nullptr, nullptr, kNoCompression,
                     db_options_.statistics.get(), &event_logger, true);

  HistogramData hist;
  FileMetaData fd;
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(nullptr, &fd));
  mutex_.Unlock();
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);

  ASSERT_EQ(ToString(0), fd.smallest.user_key().ToString());
  ASSERT_EQ("9999a",
            fd.largest.user_key().ToString());  // range tombstone end key
  ASSERT_EQ(1, fd.smallest_seqno);
  ASSERT_EQ(10000, fd.largest_seqno);  // range tombstone seqnum 10000
  mock_table_factory_->AssertSingleFile(inserted_keys);
  job_context.Clean();
}

TEST_F(FlushJobTest, Snapshots) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto new_mem = cfd->ConstructNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                           kMaxSequenceNumber);

  std::vector<SequenceNumber> snapshots;
  std::set<SequenceNumber> snapshots_set;
  int keys = 10000;
  int max_inserts_per_keys = 8;

  Random rnd(301);
  for (int i = 0; i < keys / 2; ++i) {
    snapshots.push_back(rnd.Uniform(keys * (max_inserts_per_keys / 2)) + 1);
    snapshots_set.insert(snapshots.back());
  }
  std::sort(snapshots.begin(), snapshots.end());

  new_mem->Ref();
  SequenceNumber current_seqno = 0;
  auto inserted_keys = mock::MakeMockFile();
  for (int i = 1; i < keys; ++i) {
    std::string key(ToString(i));
    int insertions = rnd.Uniform(max_inserts_per_keys);
    for (int j = 0; j < insertions; ++j) {
      std::string value(test::RandomHumanReadableString(&rnd, 10));
      auto seqno = ++current_seqno;
      new_mem->Add(SequenceNumber(seqno), kTypeValue, key, value);
      // a key is visible only if:
      // 1. it's the last one written (j == insertions - 1)
      // 2. there's a snapshot pointing at it
      bool visible = (j == insertions - 1) ||
                     (snapshots_set.find(seqno) != snapshots_set.end());
      if (visible) {
        InternalKey internal_key(key, seqno, kTypeValue);
        inserted_keys.insert({internal_key.Encode().ToString(), value});
      }
    }
  }

  autovector<MemTable*> to_delete;
  cfd->imm()->Add(new_mem, &to_delete);
  for (auto& m : to_delete) {
    delete m;
  }

  EventLogger event_logger(db_options_.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;  // not relavant
  FlushJob flush_job(dbname_, versions_->GetColumnFamilySet()->GetDefault(),
                     db_options_, *cfd->GetLatestMutableCFOptions(),
                     env_options_, versions_.get(), &mutex_, &shutting_down_,
                     snapshots, kMaxSequenceNumber, snapshot_checker,
                     &job_context, nullptr, nullptr, nullptr, kNoCompression,
                     db_options_.statistics.get(), &event_logger, true);
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run());
  mutex_.Unlock();
  mock_table_factory_->AssertSingleFile(inserted_keys);
  HistogramData hist;
  db_options_.statistics->histogramData(FLUSH_TIME, &hist);
  ASSERT_GT(hist.average, 0.0);
  job_context.Clean();
}

class BatchFlushJobTest : public testing::Test {
  class SequenceNumberWrapper {
   public:
    SequenceNumberWrapper(): seq_(0ULL) {}
    SequenceNumber Get() {
      SequenceNumber ret = seq_++;
      return ret;
    }
   private:
    SequenceNumber seq_;
  };

 public:
  BatchFlushJobTest()
    : env_(Env::Default()),
      dbname_(test::TmpDir() + "/batch_flush_job_test"),
      table_cache_(NewLRUCache(50000, 16)),
      shutting_down_(false),
      mock_table_factory_(new mock::MockTableFactory()) {
    options_.atomic_flush = true;
    db_options_.reset(new ImmutableDBOptions(options_));
    write_buffer_manager_.reset(
        new WriteBufferManager(db_options_->db_write_buffer_size));
    versions_.reset(new VersionSet(dbname_, db_options_.get(), env_options_,
        table_cache_.get(), write_buffer_manager_.get(), &write_controller_));

    EXPECT_OK(env_->CreateDirIfMissing(dbname_));
    db_options_->db_paths.emplace_back(dbname_,
        std::numeric_limits<uint64_t>::max());
    db_options_->statistics = rocksdb::CreateDBStatistics();
    NewDB();
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i != kNumberOfColumnFamilies; ++i) {
      cf_options_list_.emplace_back(ColumnFamilyOptions());
      cf_options_list_[i].table_factory = mock_table_factory_;
    }

    column_families.emplace_back(kDefaultColumnFamilyName, cf_options_list_[0]);
    for (size_t i = 1; i != kNumberOfColumnFamilies; ++i) {
      column_families.emplace_back("cf" + ToString(i),
          cf_options_list_[i]);
    }

    EXPECT_OK(versions_->Recover(column_families, false));
    EXPECT_TRUE(versions_->GetColumnFamilySet()->NumberOfColumnFamilies() ==
        kNumberOfColumnFamilies);
  }

  void NewDB() {
    VersionEdit new_db;
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(seqno_.Get());

    // Create non-default column families.
    std::vector<VersionEdit> new_cfs(kNumberOfColumnFamilies - 1);
    for (size_t i = 1; i != kNumberOfColumnFamilies; ++i) {
      new_cfs[i-1].AddColumnFamily("cf" + ToString(i));
      new_cfs[i-1].SetColumnFamily(static_cast<int>(i));
      new_cfs[i-1].SetLogNumber(0);
      new_cfs[i-1].SetNextFile(2);
      new_cfs[i-1].SetLastSequence(seqno_.Get());
    }

    const std::string manifest = DescriptorFileName(dbname_, 1);
    unique_ptr<WritableFile> file;
    Status s = env_->NewWritableFile(manifest, &file,
        env_->OptimizeForManifestWrite(env_options_));
    ASSERT_OK(s);
    unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(file), EnvOptions()));
    {
      log::Writer log(std::move(file_writer), 0, false);
      std::string record;
      new_db.EncodeTo(&record);
      ASSERT_OK(log.AddRecord(record));

      for (size_t i = 1; i != kNumberOfColumnFamilies; ++i) {
        std::string tmp_record;
        new_cfs[i-1].EncodeTo(&tmp_record);
        ASSERT_OK(log.AddRecord(tmp_record));
      }
    }
    ASSERT_OK(SetCurrentFile(env_, dbname_, 1, nullptr));
  }

  static const size_t kNumberOfColumnFamilies = 3;
  Env* env_;
  std::string dbname_;
  EnvOptions env_options_;
  Options options_;
  std::unique_ptr<ImmutableDBOptions> db_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  std::unique_ptr<WriteBufferManager> write_buffer_manager_;
  std::vector<ColumnFamilyOptions> cf_options_list_;
  std::unique_ptr<VersionSet> versions_;
  InstrumentedMutex mutex_;
  std::atomic<bool> shutting_down_;
  std::shared_ptr<mock::MockTableFactory> mock_table_factory_;
  SequenceNumberWrapper seqno_;
};

TEST_F(BatchFlushJobTest, MultiColumnFamiliesVersionEdits) {
  std::vector<ColumnFamilyData*> cfds;
  std::vector<MutableCFOptions> mutable_cf_options_list;
  std::vector<uint64_t> memtable_ids;
  autovector<stl_wrappers::KVMap> mock_files;
  autovector<std::pair<SequenceNumber, SequenceNumber>> seq_pairs;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    mock_files.emplace_back(mock::MakeMockFile());
    auto& mock_file = mock_files.back();
    cfds.push_back(cfd);
    mutable_cf_options_list.push_back(*cfd->GetLatestMutableCFOptions());
    auto new_mem = cfd->ConstructNewMemtable(mutable_cf_options_list.back(),
        kMaxSequenceNumber);
    new_mem->Ref();
    SequenceNumber smallest_seq(0), largest_seq(0);
    for (int i = 0; i != 10000; ++i) {
      std::string key(ToString((i + 1000) % 10000));
      std::string value("value" + key);
      SequenceNumber curr_seq = seqno_.Get();
      if (i == 0) {
        smallest_seq = curr_seq;
      } else if (i == 9999) {
        largest_seq = curr_seq;
      }
      new_mem->Add(curr_seq, kTypeValue, key, value);
      InternalKey internal_key(key, SequenceNumber(curr_seq), kTypeValue);
      mock_file.insert({internal_key.Encode().ToString(), value});
    }
    seq_pairs.emplace_back(smallest_seq, largest_seq);

    autovector<MemTable*> to_delete;
    cfd->imm()->Add(new_mem, &to_delete);
    memtable_ids.emplace_back(cfd->imm()->GetLatestMemTableID());
    for (auto m : to_delete) {
      delete m;
    }
  }

  std::vector<Directory*> output_file_directories(cfds.size(), nullptr);
  std::vector<CompressionType> compression_types(cfds.size(), kNoCompression);
  std::vector<bool> measure_io_stats(cfds.size(), true);
  EventLogger event_logger(db_options_->info_log.get());
  JobContext job_context(0);
  BatchFlushJob flush_job(dbname_, cfds, *db_options_, mutable_cf_options_list,
      memtable_ids, env_options_, versions_.get(), &mutex_, &shutting_down_,
      {} /* existing_snapshots */,
      kMaxSequenceNumber /* earliest_write_conflict_snapshot */,
      nullptr /* snapshot_checker */, &job_context,
      nullptr /* log_buffer */, nullptr /* db_directory */,
      output_file_directories, compression_types,
      db_options_->statistics.get(), &event_logger, measure_io_stats);
  std::vector<FileMetaData> file_meta(cfds.size());
  mutex_.Lock();
  flush_job.PickMemTable();
  ASSERT_OK(flush_job.Run(nullptr, &file_meta));
  mutex_.Unlock();
  for (size_t i = 0; i != kNumberOfColumnFamilies; ++i) {
    ASSERT_EQ(ToString(0), file_meta[i].smallest.user_key().ToString());
    ASSERT_EQ(ToString(9999), file_meta[i].largest.user_key().ToString());
    ASSERT_EQ(seq_pairs[i].first, file_meta[i].smallest_seqno);
    ASSERT_EQ(seq_pairs[i].second, file_meta[i].largest_seqno);
  }
  job_context.Clean();
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
