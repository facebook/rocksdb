// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/wal_edit.h"

#include "port/port.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

TEST(WalSet, AddDeleteReset) {
  WalSet wals;
  ASSERT_TRUE(wals.GetWals().empty());

  // Create WAL 1 - 10.
  for (WalNumber log_number = 1; log_number <= 10; log_number++) {
    wals.AddWal(WalAddition(log_number));
  }
  ASSERT_EQ(wals.GetWals().size(), 10);

  // Close WAL 1 - 5.
  for (WalNumber log_number = 1; log_number <= 5; log_number++) {
    wals.AddWal(WalAddition(log_number, WalMetadata(100)));
  }
  ASSERT_EQ(wals.GetWals().size(), 10);

  // Delete WAL 1 - 5.
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
  ASSERT_FALSE(wals.GetWals().at(kNumber).HasSize());
  wals.AddWal(WalAddition(kNumber, WalMetadata(kBytes)));
  ASSERT_TRUE(wals.GetWals().at(kNumber).HasSize());
  ASSERT_EQ(wals.GetWals().at(kNumber).GetSizeInBytes(), kBytes);
}

TEST(WalSet, CreateTwice) {
  constexpr WalNumber kNumber = 100;
  WalSet wals;
  ASSERT_OK(wals.AddWal(WalAddition(kNumber)));
  Status s = wals.AddWal(WalAddition(kNumber));
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("WAL 100 is created more than once") !=
              std::string::npos);
}

TEST(WalSet, CloseTwice) {
  constexpr WalNumber kNumber = 100;
  constexpr uint64_t kBytes = 200;
  WalSet wals;
  ASSERT_OK(wals.AddWal(WalAddition(kNumber)));
  ASSERT_OK(wals.AddWal(WalAddition(kNumber, WalMetadata(kBytes))));
  Status s = wals.AddWal(WalAddition(kNumber, WalMetadata(kBytes)));
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("WAL 100 is closed more than once") !=
              std::string::npos);
}

TEST(WalSet, CloseBeforeCreate) {
  constexpr WalNumber kNumber = 100;
  constexpr uint64_t kBytes = 200;
  WalSet wals;
  Status s = wals.AddWal(WalAddition(kNumber, WalMetadata(kBytes)));
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("WAL 100 is not created before closing") !=
              std::string::npos);
}

TEST(WalSet, CreateAfterClose) {
  constexpr WalNumber kNumber = 100;
  constexpr uint64_t kBytes = 200;
  WalSet wals;
  ASSERT_OK(wals.AddWal(WalAddition(kNumber)));
  ASSERT_OK(wals.AddWal(WalAddition(kNumber, WalMetadata(kBytes))));
  Status s = wals.AddWal(WalAddition(kNumber));
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("WAL 100 is created more than once") !=
              std::string::npos);
}

TEST(WalSet, DeleteNonExistingWal) {
  constexpr WalNumber kNonExistingNumber = 100;
  WalSet wals;
  Status s = wals.DeleteWal(WalDeletion(kNonExistingNumber));
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("WAL 100 must exist before deletion") !=
              std::string::npos);
}

TEST(WalSet, DeleteNonClosedWal) {
  constexpr WalNumber kNonClosedWalNumber = 100;
  WalSet wals;
  ASSERT_OK(wals.AddWal(WalAddition(kNonClosedWalNumber)));
  Status s = wals.DeleteWal(WalDeletion(kNonClosedWalNumber));
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("WAL 100 must be closed before deletion") !=
              std::string::npos);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
