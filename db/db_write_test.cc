//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#include <memory>
#include <thread>
#include <vector>
#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "port/stack_trace.h"
#include "util/sync_point.h"

namespace rocksdb {

// Test variations of WriteImpl.
class DBWriteTest : public DBTestBase, public testing::WithParamInterface<int> {
 public:
  DBWriteTest() : DBTestBase("/db_write_test") {}

  void Open() { DBTestBase::Reopen(GetOptions(GetParam())); }
};

// Sequence number should be return through input write batch.
TEST_P(DBWriteTest, ReturnSeuqneceNumber) {
  Random rnd(4422);
  Open();
  for (int i = 0; i < 100; i++) {
    WriteBatch batch;
    batch.Put("key" + ToString(i), test::RandomHumanReadableString(&rnd, 10));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(),
              WriteBatchInternal::Sequence(&batch));
  }
}

TEST_P(DBWriteTest, ReturnSeuqneceNumberMultiThreaded) {
  constexpr size_t kThreads = 16;
  constexpr size_t kNumKeys = 1000;
  Open();
  ASSERT_EQ(0, dbfull()->GetLatestSequenceNumber());
  // Check each sequence is used once and only once.
  std::vector<std::atomic_flag> flags(kNumKeys * kThreads + 1);
  for (size_t i = 0; i < flags.size(); i++) {
    flags[i].clear();
  }
  auto writer = [&](size_t id) {
    Random rnd(4422 + static_cast<uint32_t>(id));
    for (size_t k = 0; k < kNumKeys; k++) {
      WriteBatch batch;
      batch.Put("key" + ToString(id) + "-" + ToString(k),
                test::RandomHumanReadableString(&rnd, 10));
      ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
      SequenceNumber sequence = WriteBatchInternal::Sequence(&batch);
      ASSERT_GT(sequence, 0);
      ASSERT_LE(sequence, kNumKeys * kThreads);
      // The sequence isn't consumed by someone else.
      ASSERT_FALSE(flags[sequence].test_and_set());
    }
  };
  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreads; i++) {
    threads.emplace_back(writer, i);
  }
  for (size_t i = 0; i < kThreads; i++) {
    threads[i].join();
  }
}

INSTANTIATE_TEST_CASE_P(DBWriteTestInstance, DBWriteTest,
                        testing::Values(DBTestBase::kDefault,
                                        DBTestBase::kPipelinedWrite));

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
