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
  SnapshotChecker* snapshot_checker = nullptr;  // not relavant
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

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
