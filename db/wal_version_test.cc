// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/wal_version.h"

#include "port/port.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

TEST(WalSet, AddDeleteReset) {
  WalSet wals;
  ASSERT_TRUE(wals.GetWals().empty());

  for (WalNumber log_number = 1; log_number <= 10; log_number++) {
    wals.AddWal(WalAddition(log_number));
  }
  ASSERT_EQ(wals.GetWals().size(), 10);

  for (WalNumber log_number = 1; log_number <= 5; log_number++) {
    wals.DeleteWal(WalDeletion(log_number));
  }
  ASSERT_EQ(wals.GetWals().size(), 5);

  WalNumber expected_log_number = 6;
  for (auto it : wals.GetWals()) {
    WalNumber log_number = it.first;
    ASSERT_EQ(log_number, expected_log_number++);
  }

  wals.Reset();
  ASSERT_TRUE(wals.GetWals().empty());
}

TEST(WalSet, Overwrite) {
  constexpr WalNumber kNumber = 100;
  constexpr uint64_t kBytes = 200;
  WalSet wals;
  wals.AddWal(WalAddition(kNumber));
  ASSERT_TRUE(!wals.GetWals().at(kNumber).HasSize());
  wals.AddWal(WalAddition(kNumber, WalMetadata(kBytes)));
  ASSERT_TRUE(wals.GetWals().at(kNumber).HasSize());
  ASSERT_EQ(wals.GetWals().at(kNumber).GetSizeInBytes(), kBytes);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
