//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_index.h"
#include "db/db_test_util.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

enum class WriteBatchOpType {
  kPut = 0,
  kDelete,
  kSingleDelete,
  kDeleteRange,
  kMerge,
  kBlobIndex,
  kNum,
};

// Integer addition is needed for `::testing::Range()` to take the enum type.
WriteBatchOpType operator+(WriteBatchOpType lhs, const int rhs) {
  using T = std::underlying_type<WriteBatchOpType>::type;
  return static_cast<WriteBatchOpType>(static_cast<T>(lhs) + rhs);
}

class DbKvChecksumTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<WriteBatchOpType, char>> {
 public:
  DbKvChecksumTest()
      : DBTestBase("db_kv_checksum_test", /*env_do_fsync=*/false) {
    op_type_ = std::get<0>(GetParam());
    corrupt_byte_addend_ = std::get<1>(GetParam());
  }

  std::pair<WriteBatch, Status> GetWriteBatch(ColumnFamilyHandle* cf_handle) {
    Status s;
    WriteBatch wb(0 /* reserved_bytes */, 0 /* max_bytes */,
                  8 /* protection_bytes_per_entry */, 0 /* default_cf_ts_sz */);
    switch (op_type_) {
      case WriteBatchOpType::kPut:
        s = wb.Put(cf_handle, "key", "val");
        break;
      case WriteBatchOpType::kDelete:
        s = wb.Delete(cf_handle, "key");
        break;
      case WriteBatchOpType::kSingleDelete:
        s = wb.SingleDelete(cf_handle, "key");
        break;
      case WriteBatchOpType::kDeleteRange:
        s = wb.DeleteRange(cf_handle, "begin", "end");
        break;
      case WriteBatchOpType::kMerge:
        s = wb.Merge(cf_handle, "key", "val");
        break;
      case WriteBatchOpType::kBlobIndex: {
        // TODO(ajkr): use public API once available.
        uint32_t cf_id;
        if (cf_handle == nullptr) {
          cf_id = 0;
        } else {
          cf_id = cf_handle->GetID();
        }

        std::string blob_index;
        BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210,
                                    "val");

        s = WriteBatchInternal::PutBlobIndex(&wb, cf_id, "key", blob_index);
        break;
      }
      case WriteBatchOpType::kNum:
        assert(false);
    }
    return {std::move(wb), std::move(s)};
  }

  void CorruptNextByteCallBack(void* arg) {
    Slice encoded = *static_cast<Slice*>(arg);
    if (entry_len_ == std::numeric_limits<size_t>::max()) {
      // We learn the entry size on the first attempt
      entry_len_ = encoded.size();
    }
    // All entries should be the same size
    assert(entry_len_ == encoded.size());
    char* buf = const_cast<char*>(encoded.data());
    buf[corrupt_byte_offset_] += corrupt_byte_addend_;
    ++corrupt_byte_offset_;
  }

  bool MoreBytesToCorrupt() { return corrupt_byte_offset_ < entry_len_; }

 protected:
  WriteBatchOpType op_type_;
  char corrupt_byte_addend_;
  size_t corrupt_byte_offset_ = 0;
  size_t entry_len_ = std::numeric_limits<size_t>::max();
};

std::string GetTestNameSuffix(
    ::testing::TestParamInfo<std::tuple<WriteBatchOpType, char>> info) {
  std::ostringstream oss;
  switch (std::get<0>(info.param)) {
    case WriteBatchOpType::kPut:
      oss << "Put";
      break;
    case WriteBatchOpType::kDelete:
      oss << "Delete";
      break;
    case WriteBatchOpType::kSingleDelete:
      oss << "SingleDelete";
      break;
    case WriteBatchOpType::kDeleteRange:
      oss << "DeleteRange";
      break;
    case WriteBatchOpType::kMerge:
      oss << "Merge";
      break;
    case WriteBatchOpType::kBlobIndex:
      oss << "BlobIndex";
      break;
    case WriteBatchOpType::kNum:
      assert(false);
  }
  oss << "Add"
      << static_cast<int>(static_cast<unsigned char>(std::get<1>(info.param)));
  return oss.str();
}

INSTANTIATE_TEST_CASE_P(
    DbKvChecksumTest, DbKvChecksumTest,
    ::testing::Combine(::testing::Range(static_cast<WriteBatchOpType>(0),
                                        WriteBatchOpType::kNum),
                       ::testing::Values(2, 103, 251)),
    GetTestNameSuffix);

TEST_P(DbKvChecksumTest, MemTableAddCorrupted) {
  // This test repeatedly attempts to write `WriteBatch`es containing a single
  // entry of type `op_type_`. Each attempt has one byte corrupted in its
  // memtable entry by adding `corrupt_byte_addend_` to its original value. The
  // test repeats until an attempt has been made on each byte in the encoded
  // memtable entry. All attempts are expected to fail with `Status::Corruption`
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::Add:Encoded",
      std::bind(&DbKvChecksumTest::CorruptNextByteCallBack, this,
                std::placeholders::_1));

  while (MoreBytesToCorrupt()) {
    // Failed memtable insert always leads to read-only mode, so we have to
    // reopen for every attempt.
    Options options = CurrentOptions();
    if (op_type_ == WriteBatchOpType::kMerge) {
      options.merge_operator = MergeOperators::CreateStringAppendOperator();
    }
    Reopen(options);

    SyncPoint::GetInstance()->EnableProcessing();
    auto batch_and_status = GetWriteBatch(nullptr /* cf_handle */);
    ASSERT_OK(batch_and_status.second);
    ASSERT_TRUE(
        db_->Write(WriteOptions(), &batch_and_status.first).IsCorruption());
    SyncPoint::GetInstance()->DisableProcessing();

    // In case the above callback is not invoked, this test will run
    // numeric_limits<size_t>::max() times until it reports an error (or will
    // exhaust disk space). Added this assert to report error early.
    ASSERT_TRUE(entry_len_ < std::numeric_limits<size_t>::max());
  }
}

TEST_P(DbKvChecksumTest, MemTableAddWithColumnFamilyCorrupted) {
  // This test repeatedly attempts to write `WriteBatch`es containing a single
  // entry of type `op_type_` to a non-default column family. Each attempt has
  // one byte corrupted in its memtable entry by adding `corrupt_byte_addend_`
  // to its original value. The test repeats until an attempt has been made on
  // each byte in the encoded memtable entry. All attempts are expected to fail
  // with `Status::Corruption`.
  Options options = CurrentOptions();
  if (op_type_ == WriteBatchOpType::kMerge) {
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
  }
  CreateAndReopenWithCF({"pikachu"}, options);
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::Add:Encoded",
      std::bind(&DbKvChecksumTest::CorruptNextByteCallBack, this,
                std::placeholders::_1));

  while (MoreBytesToCorrupt()) {
    // Failed memtable insert always leads to read-only mode, so we have to
    // reopen for every attempt.
    ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);

    SyncPoint::GetInstance()->EnableProcessing();
    auto batch_and_status = GetWriteBatch(handles_[1]);
    ASSERT_OK(batch_and_status.second);
    ASSERT_TRUE(
        db_->Write(WriteOptions(), &batch_and_status.first).IsCorruption());
    SyncPoint::GetInstance()->DisableProcessing();

    // In case the above callback is not invoked, this test will run
    // numeric_limits<size_t>::max() times until it reports an error (or will
    // exhaust disk space). Added this assert to report error early.
    ASSERT_TRUE(entry_len_ < std::numeric_limits<size_t>::max());
  }
}

TEST_P(DbKvChecksumTest, NoCorruptionCase) {
  // If this test fails, we may have found a piece of malfunctioned hardware
  auto batch_and_status = GetWriteBatch(nullptr);
  ASSERT_OK(batch_and_status.second);
  ASSERT_OK(batch_and_status.first.VerifyChecksum());
}

TEST_P(DbKvChecksumTest, WriteToWALCorrupted) {
  // This test repeatedly attempts to write `WriteBatch`es containing a single
  // entry of type `op_type_`. Each attempt has one byte corrupted by adding
  // `corrupt_byte_addend_` to its original value. The test repeats until an
  // attempt has been made on each byte in the encoded write batch. All attempts
  // are expected to fail with `Status::Corruption`
  Options options = CurrentOptions();
  if (op_type_ == WriteBatchOpType::kMerge) {
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
  }
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:log_entry",
      std::bind(&DbKvChecksumTest::CorruptNextByteCallBack, this,
                std::placeholders::_1));
  // First 8 bytes are for sequence number which is not protected in write batch
  corrupt_byte_offset_ = 8;

  while (MoreBytesToCorrupt()) {
    // Corrupted write batch leads to read-only mode, so we have to
    // reopen for every attempt.
    Reopen(options);
    auto log_size_pre_write = dbfull()->TEST_total_log_size();

    SyncPoint::GetInstance()->EnableProcessing();
    auto batch_and_status = GetWriteBatch(nullptr /* cf_handle */);
    ASSERT_OK(batch_and_status.second);
    ASSERT_TRUE(
        db_->Write(WriteOptions(), &batch_and_status.first).IsCorruption());
    // Confirm that nothing was written to WAL
    ASSERT_EQ(log_size_pre_write, dbfull()->TEST_total_log_size());
    ASSERT_TRUE(dbfull()->TEST_GetBGError().IsCorruption());
    SyncPoint::GetInstance()->DisableProcessing();

    // In case the above callback is not invoked, this test will run
    // numeric_limits<size_t>::max() times until it reports an error (or will
    // exhaust disk space). Added this assert to report error early.
    ASSERT_TRUE(entry_len_ < std::numeric_limits<size_t>::max());
  }
}

TEST_P(DbKvChecksumTest, WriteToWALWithColumnFamilyCorrupted) {
  // This test repeatedly attempts to write `WriteBatch`es containing a single
  // entry of type `op_type_`. Each attempt has one byte corrupted by adding
  // `corrupt_byte_addend_` to its original value. The test repeats until an
  // attempt has been made on each byte in the encoded write batch. All attempts
  // are expected to fail with `Status::Corruption`
  Options options = CurrentOptions();
  if (op_type_ == WriteBatchOpType::kMerge) {
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
  }
  CreateAndReopenWithCF({"pikachu"}, options);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:log_entry",
      std::bind(&DbKvChecksumTest::CorruptNextByteCallBack, this,
                std::placeholders::_1));
  // First 8 bytes are for sequence number which is not protected in write batch
  corrupt_byte_offset_ = 8;

  while (MoreBytesToCorrupt()) {
    // Corrupted write batch leads to read-only mode, so we have to
    // reopen for every attempt.
    ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
    auto log_size_pre_write = dbfull()->TEST_total_log_size();

    SyncPoint::GetInstance()->EnableProcessing();
    auto batch_and_status = GetWriteBatch(handles_[1]);
    ASSERT_OK(batch_and_status.second);
    ASSERT_TRUE(
        db_->Write(WriteOptions(), &batch_and_status.first).IsCorruption());
    // Confirm that nothing was written to WAL
    ASSERT_EQ(log_size_pre_write, dbfull()->TEST_total_log_size());
    ASSERT_TRUE(dbfull()->TEST_GetBGError().IsCorruption());
    SyncPoint::GetInstance()->DisableProcessing();

    // In case the above callback is not invoked, this test will run
    // numeric_limits<size_t>::max() times until it reports an error (or will
    // exhaust disk space). Added this assert to report error early.
    ASSERT_TRUE(entry_len_ < std::numeric_limits<size_t>::max());
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
