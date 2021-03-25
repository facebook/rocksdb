// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/wal_edit.h"

#include "db/db_test_util.h"
#include "file/file_util.h"
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

  // Delete WAL 1 - 5.
  wals.DeleteWalsBefore(6);
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
  ASSERT_FALSE(wals.GetWals().at(kNumber).HasSyncedSize());
  wals.AddWal(WalAddition(kNumber, WalMetadata(kBytes)));
  ASSERT_TRUE(wals.GetWals().at(kNumber).HasSyncedSize());
  ASSERT_EQ(wals.GetWals().at(kNumber).GetSyncedSizeInBytes(), kBytes);
}

TEST(WalSet, SmallerSyncedSize) {
  constexpr WalNumber kNumber = 100;
  constexpr uint64_t kBytes = 100;
  WalSet wals;
  ASSERT_OK(wals.AddWal(WalAddition(kNumber, WalMetadata(kBytes))));
  Status s = wals.AddWal(WalAddition(kNumber, WalMetadata(0)));
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(
      s.ToString().find(
          "WAL 100 must not have smaller synced size than previous one") !=
      std::string::npos);
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

TEST(WalSet, DeleteAllWals) {
  constexpr WalNumber kMaxWalNumber = 10;
  WalSet wals;
  for (WalNumber i = 1; i <= kMaxWalNumber; i++) {
    wals.AddWal(WalAddition(i));
  }
  ASSERT_OK(wals.DeleteWalsBefore(kMaxWalNumber + 1));
}

TEST(WalSet, AddObsoleteWal) {
  constexpr WalNumber kNumber = 100;
  WalSet wals;
  ASSERT_OK(wals.DeleteWalsBefore(kNumber + 1));
  ASSERT_OK(wals.AddWal(WalAddition(kNumber)));
  ASSERT_TRUE(wals.GetWals().empty());
}

TEST(WalSet, MinWalNumberToKeep) {
  constexpr WalNumber kNumber = 100;
  WalSet wals;
  ASSERT_EQ(wals.GetMinWalNumberToKeep(), 0);
  ASSERT_OK(wals.DeleteWalsBefore(kNumber));
  ASSERT_EQ(wals.GetMinWalNumberToKeep(), kNumber);
  ASSERT_OK(wals.DeleteWalsBefore(kNumber - 1));
  ASSERT_EQ(wals.GetMinWalNumberToKeep(), kNumber);
  ASSERT_OK(wals.DeleteWalsBefore(kNumber + 1));
  ASSERT_EQ(wals.GetMinWalNumberToKeep(), kNumber + 1);
}

class WalSetTest : public DBTestBase {
 public:
  WalSetTest() : DBTestBase("WalSetTest", /* env_do_fsync */ true) {}

  void SetUp() override {
    test_dir_ = test::PerThreadDBPath("wal_set_test");
    ASSERT_OK(env_->CreateDir(test_dir_));
  }

  void TearDown() override {
    EXPECT_OK(DestroyDir(env_, test_dir_));
    logs_on_disk_.clear();
    wals_.Reset();
  }

  void CreateWalOnDisk(WalNumber number, const std::string& fname,
                       uint64_t size_bytes) {
    std::unique_ptr<WritableFile> f;
    std::string fpath = Path(fname);
    ASSERT_OK(env_->NewWritableFile(fpath, &f, EnvOptions()));
    std::string content(size_bytes, '0');
    ASSERT_OK(f->Append(content));
    ASSERT_OK(f->Close());

    logs_on_disk_[number] = fpath;
  }

  void AddWalToWalSet(WalNumber number, uint64_t size_bytes) {
    // Create WAL.
    ASSERT_OK(wals_.AddWal(WalAddition(number)));
    // Close WAL.
    WalMetadata wal(size_bytes);
    ASSERT_OK(wals_.AddWal(WalAddition(number, wal)));
  }

  Status CheckWals() const { return wals_.CheckWals(env_, logs_on_disk_); }

 private:
  std::string test_dir_;
  std::unordered_map<WalNumber, std::string> logs_on_disk_;
  WalSet wals_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }
};

TEST_F(WalSetTest, CheckEmptyWals) { ASSERT_OK(CheckWals()); }

TEST_F(WalSetTest, CheckWals) {
  for (int number = 1; number < 10; number++) {
    uint64_t size = rand() % 100;
    std::stringstream ss;
    ss << "log" << number;
    std::string fname = ss.str();
    CreateWalOnDisk(number, fname, size);
    // log 0 - 5 are obsolete.
    if (number > 5) {
      AddWalToWalSet(number, size);
    }
  }
  ASSERT_OK(CheckWals());
}

TEST_F(WalSetTest, CheckMissingWals) {
  for (int number = 1; number < 10; number++) {
    uint64_t size = rand() % 100;
    AddWalToWalSet(number, size);
    // logs with even number are missing from disk.
    if (number % 2) {
      std::stringstream ss;
      ss << "log" << number;
      std::string fname = ss.str();
      CreateWalOnDisk(number, fname, size);
    }
  }

  Status s = CheckWals();
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  // The first log with even number is missing.
  std::stringstream expected_err;
  expected_err << "Missing WAL with log number: " << 2;
  ASSERT_TRUE(s.ToString().find(expected_err.str()) != std::string::npos)
      << s.ToString();
}

TEST_F(WalSetTest, CheckWalsWithShrinkedSize) {
  for (int number = 1; number < 10; number++) {
    uint64_t size = rand() % 100 + 1;
    AddWalToWalSet(number, size);
    // logs with even number have shrinked size.
    std::stringstream ss;
    ss << "log" << number;
    std::string fname = ss.str();
    CreateWalOnDisk(number, fname, (number % 2) ? size : size - 1);
  }

  Status s = CheckWals();
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  // The first log with even number has wrong size.
  std::stringstream expected_err;
  expected_err << "Size mismatch: WAL (log number: " << 2 << ")";
  ASSERT_TRUE(s.ToString().find(expected_err.str()) != std::string::npos)
      << s.ToString();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
