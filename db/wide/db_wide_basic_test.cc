//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <array>
#include <sstream>
#include <string>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBWideBasicTest : public DBTestBase {
 protected:
  DBWideBasicTest()
      : DBTestBase("db_wide_basic_test", /* env_do_fsync */ false) {}
};

TEST_F(DBWideBasicTest, Entity) {
  Options options = GetDefaultOptions();

  constexpr char key[] = "key";
  WideColumns columns{{"foo", "bar"}, {"hello", "world"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));

  // Try reading from memtable
  {
    PinnableSlice result;
    ASSERT_TRUE(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
            .IsNotSupported());
  }

  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsNotSupported());
  }

  options.avoid_flush_during_recovery = true;
  Reopen(options);

  // Try reading after recovery
  {
    PinnableSlice result;
    ASSERT_TRUE(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
            .IsNotSupported());
  }

  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsNotSupported());
  }

  ASSERT_OK(Flush());

  // Try reading from storage
  {
    PinnableSlice result;
    ASSERT_TRUE(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
            .IsNotSupported());
  }

  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsNotSupported());
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
