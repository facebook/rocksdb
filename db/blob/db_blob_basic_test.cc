//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_index.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBBlobBasicTest : public DBTestBase {
 protected:
  DBBlobBasicTest()
      : DBTestBase("/db_blob_basic_test", /* env_do_fsync */ false) {}
};

TEST_F(DBBlobBasicTest, GetBlob) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get(key), blob_value);

  // Try again with no I/O allowed. The table and the necessary blocks should
  // already be in their respective caches; however, the blob itself can only be
  // read from the blob file, so the read should return Incomplete.
  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result)
                  .IsIncomplete());
}

TEST_F(DBBlobBasicTest, GetBlob_CorruptIndex) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";

  // Fake a corrupt blob index.
  const std::string blob_index("foobar");

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

TEST_F(DBBlobBasicTest, GetBlob_InlinedTTLIndex) {
  constexpr uint64_t min_blob_size = 10;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob[] = "short";
  static_assert(sizeof(short) - 1 < min_blob_size,
                "Blob too long to be inlined");

  // Fake an inlined TTL blob index.
  std::string blob_index;

  constexpr uint64_t expiration = 1234567890;

  BlobIndex::EncodeInlinedTTL(&blob_index, expiration, blob);

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

TEST_F(DBBlobBasicTest, GetBlob_IndexWithInvalidFileNumber) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";

  // Fake a blob index referencing a non-existent blob file.
  std::string blob_index;

  constexpr uint64_t blob_file_number = 1000;
  constexpr uint64_t offset = 1234;
  constexpr uint64_t size = 5678;

  BlobIndex::EncodeBlob(&blob_index, blob_file_number, offset, size,
                        kNoCompression);

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

class DBBlobBasicIOErrorTest : public DBBlobBasicTest,
                               public testing::WithParamInterface<std::string> {
 protected:
  DBBlobBasicIOErrorTest() : sync_point_(GetParam()) {
    fault_injection_env_.reset(new FaultInjectionTestEnv(env_));
  }
  ~DBBlobBasicIOErrorTest() { Close(); }

  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DBBlobBasicTest, DBBlobBasicIOErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::OpenFile:NewRandomAccessFile",
                            "BlobFileReader::GetBlob:ReadFromFile"}));

TEST_P(DBBlobBasicIOErrorTest, GetBlob_IOError) {
  Options options;
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
