//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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
      : DBTestBase("/db_kv_checksum_test", /*env_do_fsync=*/false) {
    op_type_ = std::get<0>(GetParam());
    corrupt_byte_addend_ = std::get<1>(GetParam());
  }

 protected:
  WriteBatchOpType op_type_;
  char corrupt_byte_addend_;
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
  // entry of type `op_type_`. Each attempt has one byte corrupted by adding
  // `corrupt_byte_addend_` to its original value. The test repeats until an
  // attempt has been made on each byte in the encoded memtable entry. All
  // attempts are expected to fail with `Status::Corruption`.
  size_t corrupt_byte_offset = 0;
  size_t memtable_entry_len = port::kMaxSizet;
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::Add:Encoded", [&](void* arg) {
        Slice encoded = *static_cast<Slice*>(arg);
        if (memtable_entry_len == port::kMaxSizet) {
          // We learn the entry size on the first attempt
          memtable_entry_len = encoded.size();
        }
        // All entries should be the same size
        assert(memtable_entry_len == encoded.size());
        char* buf = const_cast<char*>(encoded.data());
        buf[corrupt_byte_offset] += corrupt_byte_addend_;
      });

  while (corrupt_byte_offset < memtable_entry_len) {
    // Failed memtable insert always leads to read-only mode, so we have to
    // reopen for every attempt.
    Options options = CurrentOptions();
    if (op_type_ == WriteBatchOpType::kMerge) {
      options.merge_operator = MergeOperators::CreateStringAppendOperator();
    }
    Reopen(options);
    SyncPoint::GetInstance()->EnableProcessing();

    WriteBatch wb(0 /* reserved_bytes */, 0 /* max_bytes */, 0 /* ts_sz */,
                  true /* _protected */);
    switch (op_type_) {
      case WriteBatchOpType::kPut:
        ASSERT_OK(wb.Put("key", "val"));
        break;
      case WriteBatchOpType::kDelete:
        ASSERT_OK(wb.Delete("key"));
        break;
      case WriteBatchOpType::kSingleDelete:
        ASSERT_OK(wb.SingleDelete("key"));
        break;
      case WriteBatchOpType::kDeleteRange:
        ASSERT_OK(wb.DeleteRange("begin", "end"));
        break;
      case WriteBatchOpType::kMerge:
        ASSERT_OK(wb.Merge("key", "val"));
        break;
      case WriteBatchOpType::kBlobIndex:
        // TODO(ajkr): use public API once available.
        ASSERT_OK(WriteBatchInternal::PutBlobIndex(
            &wb, 0 /* column_family_id */, "key", "val"));
        break;
      case WriteBatchOpType::kNum:
        assert(false);
    }
    ASSERT_TRUE(db_->Write(WriteOptions(), &wb).IsCorruption());
    SyncPoint::GetInstance()->DisableProcessing();
    ++corrupt_byte_offset;
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
