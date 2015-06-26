//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <map>
#include <string>

#include "db/flush_job.h"
#include "db/column_family.h"
#include "db/version_set.h"
#include "db/writebuffer.h"
#include "rocksdb/cache.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "table/mock_table.h"

namespace rocksdb {

// TODO(icanadi) Mock out everything else:
// 1. VersionSet
// 2. Memtable
class FlushJobTest : public testing::Test {
 public:
  FlushJobTest()
      : env_(Env::Default()),
        dbname_(test::TmpDir() + "/flush_job_test"),
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
    {
      log::Writer log(std::move(file));
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
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  DBOptions db_options_;
  WriteBuffer write_buffer_;
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
  FlushJob flush_job(dbname_, versions_->GetColumnFamilySet()->GetDefault(),
                     db_options_, *cfd->GetLatestMutableCFOptions(),
                     env_options_, versions_.get(), &mutex_, &shutting_down_,
                     SequenceNumber(), &job_context, nullptr, nullptr, nullptr,
                     kNoCompression, nullptr, &event_logger);
  ASSERT_OK(flush_job.Run());
  job_context.Clean();
}

TEST_F(FlushJobTest, NonEmpty) {
  JobContext job_context(0);
  auto cfd = versions_->GetColumnFamilySet()->GetDefault();
  auto new_mem = cfd->ConstructNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                           kMaxSequenceNumber);
  new_mem->Ref();
  std::map<std::string, std::string> inserted_keys;
  for (int i = 1; i < 10000; ++i) {
    std::string key(ToString(i));
    std::string value("value" + ToString(i));
    new_mem->Add(SequenceNumber(i), kTypeValue, key, value);
    InternalKey internal_key(key, SequenceNumber(i), kTypeValue);
    inserted_keys.insert({internal_key.Encode().ToString(), value});
  }

  autovector<MemTable*> to_delete;
  cfd->imm()->Add(new_mem, &to_delete);
  for (auto& m : to_delete) {
    delete m;
  }

  EventLogger event_logger(db_options_.info_log.get());
  FlushJob flush_job(dbname_, versions_->GetColumnFamilySet()->GetDefault(),
                     db_options_, *cfd->GetLatestMutableCFOptions(),
                     env_options_, versions_.get(), &mutex_, &shutting_down_,
                     SequenceNumber(), &job_context, nullptr, nullptr, nullptr,
                     kNoCompression, nullptr, &event_logger);
  mutex_.Lock();
  ASSERT_OK(flush_job.Run());
  mutex_.Unlock();
  mock_table_factory_->AssertSingleFile(inserted_keys);
  job_context.Clean();
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
