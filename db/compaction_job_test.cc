//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <map>
#include <string>

#include "db/compaction_job.h"
#include "db/column_family.h"
#include "db/version_set.h"
#include "db/writebuffer.h"
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "util/file_reader_writer.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "table/mock_table.h"

namespace rocksdb {

namespace {
void VerifyInitializationOfCompactionJobStats(
      const CompactionJobStats& compaction_job_stats) {
#if !defined(IOS_CROSS_COMPILE)
  ASSERT_EQ(compaction_job_stats.elapsed_micros, 0U);

  ASSERT_EQ(compaction_job_stats.num_input_records, 0U);
  ASSERT_EQ(compaction_job_stats.num_input_files, 0U);
  ASSERT_EQ(compaction_job_stats.num_input_files_at_output_level, 0U);

  ASSERT_EQ(compaction_job_stats.num_output_records, 0U);
  ASSERT_EQ(compaction_job_stats.num_output_files, 0U);

  ASSERT_EQ(compaction_job_stats.is_manual_compaction, false);

  ASSERT_EQ(compaction_job_stats.total_input_bytes, 0U);
  ASSERT_EQ(compaction_job_stats.total_output_bytes, 0U);

  ASSERT_EQ(compaction_job_stats.total_input_raw_key_bytes, 0U);
  ASSERT_EQ(compaction_job_stats.total_input_raw_value_bytes, 0U);

  ASSERT_EQ(compaction_job_stats.smallest_output_key_prefix[0], 0);
  ASSERT_EQ(compaction_job_stats.largest_output_key_prefix[0], 0);

  ASSERT_EQ(compaction_job_stats.num_records_replaced, 0U);

  ASSERT_EQ(compaction_job_stats.num_input_deletion_records, 0U);
  ASSERT_EQ(compaction_job_stats.num_expired_deletion_records, 0U);

  ASSERT_EQ(compaction_job_stats.num_corrupt_keys, 0U);
#endif  // !defined(IOS_CROSS_COMPILE)
}

}  // namespace

// TODO(icanadi) Make it simpler once we mock out VersionSet
class CompactionJobTest : public testing::Test {
 public:
  CompactionJobTest()
      : env_(Env::Default()),
        dbname_(test::TmpDir() + "/compaction_job_test"),
        mutable_cf_options_(Options(), ImmutableCFOptions(Options())),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_(db_options_.db_write_buffer_size),
        versions_(new VersionSet(dbname_, &db_options_, env_options_,
                                 table_cache_.get(), &write_buffer_,
                                 &write_controller_)),
        shutting_down_(false),
        mock_table_factory_(new mock::MockTableFactory()) {
    EXPECT_OK(env_->CreateDirIfMissing(dbname_));
    db_options_.db_paths.emplace_back(dbname_,
                                      std::numeric_limits<uint64_t>::max());
    NewDB();
    std::vector<ColumnFamilyDescriptor> column_families;
    cf_options_.table_factory = mock_table_factory_;
    column_families.emplace_back(kDefaultColumnFamilyName, cf_options_);

    EXPECT_OK(versions_->Recover(column_families, false));
  }

  std::string GenerateFileName(uint64_t file_number) {
    FileMetaData meta;
    std::vector<DbPath> db_paths;
    db_paths.emplace_back(dbname_, std::numeric_limits<uint64_t>::max());
    meta.fd = FileDescriptor(file_number, 0, 0);
    return TableFileName(db_paths, meta.fd.GetNumber(), meta.fd.GetPathId());
  }

  // Corrupts key by changing the type
  void CorruptKey(InternalKey* ikey) {
    std::string keystr = ikey->Encode().ToString();
    keystr[keystr.size() - 8] = kTypeLogData;
    ikey->DecodeFrom(Slice(keystr.data(), keystr.size()));
  }

  // returns expected result after compaction
  mock::MockFileContents CreateTwoFiles(bool gen_corrupted_keys) {
    mock::MockFileContents expected_results;
    const int kKeysPerFile = 10000;
    const int kCorruptKeysPerFile = 200;
    const int kMatchingKeys = kKeysPerFile / 2;
    SequenceNumber sequence_number = 0;

    auto corrupt_id = [&](int id) {
      return gen_corrupted_keys && id > 0 && id <= kCorruptKeysPerFile;
    };

    for (int i = 0; i < 2; ++i) {
      mock::MockFileContents contents;
      SequenceNumber smallest_seqno = 0, largest_seqno = 0;
      InternalKey smallest, largest;
      for (int k = 0; k < kKeysPerFile; ++k) {
        auto key = ToString(i * kMatchingKeys + k);
        auto value = ToString(i * kKeysPerFile + k);
        InternalKey internal_key(key, ++sequence_number, kTypeValue);
        // This is how the key will look like once it's written in bottommost
        // file
        InternalKey bottommost_internal_key(key, 0, kTypeValue);
        if (corrupt_id(k)) {
          CorruptKey(&internal_key);
          CorruptKey(&bottommost_internal_key);
        }
        if (k == 0) {
          smallest = internal_key;
          smallest_seqno = sequence_number;
        } else if (k == kKeysPerFile - 1) {
          largest = internal_key;
          largest_seqno = sequence_number;
        }
        contents.insert({internal_key.Encode().ToString(), value});
        if (i == 1 || k < kMatchingKeys || corrupt_id(k - kMatchingKeys)) {
          expected_results.insert(
              {bottommost_internal_key.Encode().ToString(), value});
        }
      }

      uint64_t file_number = versions_->NewFileNumber();
      EXPECT_OK(mock_table_factory_->CreateMockTable(
          env_, GenerateFileName(file_number), std::move(contents)));

      VersionEdit edit;
      edit.AddFile(0, file_number, 0, 10, smallest, largest, smallest_seqno,
                   largest_seqno, false);

      mutex_.Lock();
      versions_->LogAndApply(versions_->GetColumnFamilySet()->GetDefault(),
                             mutable_cf_options_, &edit, &mutex_);
      mutex_.Unlock();
    }
    versions_->SetLastSequence(sequence_number + 1);
    return expected_results;
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
      log::Writer log(std::move(file_writer));
      std::string record;
      new_db.EncodeTo(&record);
      s = log.AddRecord(record);
    }
    ASSERT_OK(s);
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1, nullptr);
  }

  void RunCompaction(const std::vector<FileMetaData*>& files) {
    auto cfd = versions_->GetColumnFamilySet()->GetDefault();

    CompactionInputFiles compaction_input_files;
    compaction_input_files.level = 0;
    for (auto file : files) {
      compaction_input_files.files.push_back(file);
    }
    Compaction compaction(cfd->current()->storage_info(),
                          *cfd->GetLatestMutableCFOptions(),
                          {compaction_input_files}, 1, 1024 * 1024, 10, 0,
                          kNoCompression, {});
    compaction.SetInputVersion(cfd->current());

    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options_.info_log.get());
    mutex_.Lock();
    EventLogger event_logger(db_options_.info_log.get());
    CompactionJob compaction_job(
        0, &compaction, db_options_, env_options_, versions_.get(),
        &shutting_down_, &log_buffer, nullptr, nullptr, nullptr, {},
        table_cache_, &event_logger, false, dbname_, &compaction_job_stats_);

    VerifyInitializationOfCompactionJobStats(compaction_job_stats_);

    compaction_job.Prepare();
    mutex_.Unlock();
    ASSERT_OK(compaction_job.Run());
    mutex_.Lock();
    Status s;
    compaction_job.Install(&s, *cfd->GetLatestMutableCFOptions(), &mutex_);
    ASSERT_OK(s);
    mutex_.Unlock();

    ASSERT_GE(compaction_job_stats_.elapsed_micros, 0U);
    ASSERT_EQ(compaction_job_stats_.num_input_files, files.size());
    ASSERT_EQ(compaction_job_stats_.num_output_files, 1U);
  }

  Env* env_;
  std::string dbname_;
  EnvOptions env_options_;
  MutableCFOptions mutable_cf_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  DBOptions db_options_;
  ColumnFamilyOptions cf_options_;
  WriteBuffer write_buffer_;
  std::unique_ptr<VersionSet> versions_;
  InstrumentedMutex mutex_;
  std::atomic<bool> shutting_down_;
  std::shared_ptr<mock::MockTableFactory> mock_table_factory_;
  CompactionJobStats compaction_job_stats_;
};

TEST_F(CompactionJobTest, Simple) {
  auto expected_results = CreateTwoFiles(false);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto files = cfd->current()->storage_info()->LevelFiles(0);
  ASSERT_EQ(2U, files.size());

  RunCompaction(files);
  mock_table_factory_->AssertLatestFile(expected_results);
}

TEST_F(CompactionJobTest, SimpleCorrupted) {
  auto expected_results = CreateTwoFiles(true);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto files = cfd->current()->storage_info()->LevelFiles(0);

  RunCompaction(files);
  ASSERT_EQ(compaction_job_stats_.num_corrupt_keys, 400U);
  mock_table_factory_->AssertLatestFile(expected_results);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
