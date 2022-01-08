//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

class DBBlobCorruptionTest : public DBTestBase {
 protected:
  DBBlobCorruptionTest()
      : DBTestBase("db_blob_corruption_test", /* env_do_fsync */ false) {}

  void Corrupt(FileType filetype, int offset, int bytes_to_corrupt) {
    // Pick file to corrupt
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    uint64_t number;
    FileType type;
    std::string fname;
    uint64_t picked_number = kInvalidBlobFileNumber;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) && type == filetype &&
          number > picked_number) {  // Pick latest file
        fname = dbname_ + "/" + filenames[i];
        picked_number = number;
      }
    }
    ASSERT_TRUE(!fname.empty()) << filetype;
    ASSERT_OK(test::CorruptFile(env_, fname, offset, bytes_to_corrupt));
  }
};

#ifndef ROCKSDB_LITE
TEST_F(DBBlobCorruptionTest, VerifyWholeBlobFileChecksum) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  options.file_checksum_gen_factory =
      ROCKSDB_NAMESPACE::GetFileChecksumGenCrc32cFactory();
  Reopen(options);

  ASSERT_OK(Put(Slice("key_1"), Slice("blob_value_1")));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Slice("key_2"), Slice("blob_value_2")));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));
  Close();

  Corrupt(kBlobFile, 0, 2);

  ASSERT_OK(TryReopen(options));

  int count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::VerifyFullFileChecksum:mismatch", [&](void* arg) {
        const Status* s = static_cast<Status*>(arg);
        ASSERT_NE(s, nullptr);
        ++count;
        ASSERT_NOK(*s);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(db_->VerifyFileChecksums(ReadOptions()).IsCorruption());
  ASSERT_EQ(1, count);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // !ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
