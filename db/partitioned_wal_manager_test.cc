//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Tests for PartitionedWALManager.

#include "db/partitioned_wal_manager.h"

#include <atomic>
#include <set>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class PartitionedWALManagerTest : public testing::Test {
 protected:
  std::string test_dir_;
  std::unique_ptr<Env> env_;
  std::shared_ptr<FileSystem> fs_;
  DBOptions db_options_;
  std::unique_ptr<ImmutableDBOptions> immutable_db_options_;

  void SetUp() override {
    env_.reset(MockEnv::Create(Env::Default()));
    fs_ = env_->GetFileSystem();
    test_dir_ = test::PerThreadDBPath("partitioned_wal_manager_test");
    ASSERT_OK(env_->CreateDirIfMissing(test_dir_));

    db_options_.env = env_.get();
    db_options_.wal_dir = test_dir_;
    db_options_.create_if_missing = true;
    immutable_db_options_ = std::make_unique<ImmutableDBOptions>(db_options_);
  }

  void TearDown() override {
    // Clean up test directory
    std::vector<std::string> files;
    if (env_->GetChildren(test_dir_, &files).ok()) {
      for (const auto& f : files) {
        env_->DeleteFile(test_dir_ + "/" + f).PermitUncheckedError();
      }
    }
    env_->DeleteDir(test_dir_).PermitUncheckedError();
  }

  std::unique_ptr<PartitionedWALManager> CreateManager(
      uint32_t num_partitions) {
    return std::make_unique<PartitionedWALManager>(
        fs_.get(), *immutable_db_options_, test_dir_, num_partitions);
  }
};

TEST_F(PartitionedWALManagerTest, OpenCreatesWriters) {
  const uint32_t kNumPartitions = 4;
  auto manager = CreateManager(kNumPartitions);

  // Initially not open
  EXPECT_FALSE(manager->IsOpen());
  EXPECT_EQ(manager->GetNumPartitions(), kNumPartitions);

  // Open the manager
  ASSERT_OK(manager->Open(100));
  EXPECT_TRUE(manager->IsOpen());
  EXPECT_EQ(manager->GetCurrentLogNumber(), 100);

  // Verify all writers are available
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    auto* writer = manager->GetWriter(i);
    ASSERT_NE(writer, nullptr) << "Writer " << i << " should not be null";
    EXPECT_EQ(writer->GetLogNumber(), 100);
  }

  // Out of range partition should return nullptr
  EXPECT_EQ(manager->GetWriter(kNumPartitions), nullptr);
  EXPECT_EQ(manager->GetWriter(kNumPartitions + 1), nullptr);

  // Verify partition files exist
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    std::string fname =
        PartitionedWALManager::PartitionFileName(test_dir_, 100, i);
    EXPECT_OK(env_->FileExists(fname)) << "File should exist: " << fname;
  }

  // Double open should fail
  EXPECT_TRUE(manager->Open(200).IsInvalidArgument());

  // Close the manager
  ASSERT_OK(manager->CloseAll(WriteOptions()));
  EXPECT_FALSE(manager->IsOpen());
}

TEST_F(PartitionedWALManagerTest, RoundRobinPartitionSelection) {
  const uint32_t kNumPartitions = 4;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Test round-robin selection
  for (uint32_t round = 0; round < 3; round++) {
    for (uint32_t i = 0; i < kNumPartitions; i++) {
      uint32_t partition = manager->SelectPartition();
      EXPECT_EQ(partition, i)
          << "Round " << round << ", expected partition " << i;
    }
  }

  // Close
  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALManagerTest, RoundRobinConcurrent) {
  const uint32_t kNumPartitions = 4;
  const int kNumThreads = 8;
  const int kSelectionsPerThread = 1000;

  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  std::vector<std::thread> threads;
  std::atomic<int> counts[4] = {{0}, {0}, {0}, {0}};

  for (int t = 0; t < kNumThreads; t++) {
    threads.emplace_back([&]() {
      for (int i = 0; i < kSelectionsPerThread; i++) {
        uint32_t partition = manager->SelectPartition();
        ASSERT_LT(partition, kNumPartitions);
        counts[partition].fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Total selections should be kNumThreads * kSelectionsPerThread
  int total = 0;
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    total += counts[i].load();
  }
  EXPECT_EQ(total, kNumThreads * kSelectionsPerThread);

  // Each partition should get roughly equal number of selections
  int expected_per_partition =
      (kNumThreads * kSelectionsPerThread) / kNumPartitions;
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    EXPECT_EQ(counts[i].load(), expected_per_partition)
        << "Partition " << i << " should have " << expected_per_partition
        << " selections";
  }

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALManagerTest, RotateWALFiles) {
  const uint32_t kNumPartitions = 3;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(100));

  // Write to each partition
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    auto* writer = manager->GetWriter(i);
    ASSERT_NE(writer, nullptr);
    std::string body = "test body for partition " + std::to_string(i);
    log::PartitionedLogWriter::WriteResult result;
    ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
    EXPECT_EQ(result.wal_number, 100);
  }

  // Rotate to new log number
  ASSERT_OK(manager->RotateWALFiles(200));
  EXPECT_EQ(manager->GetCurrentLogNumber(), 200);
  EXPECT_TRUE(manager->IsOpen());

  // Writers should be new ones with new log number
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    auto* writer = manager->GetWriter(i);
    ASSERT_NE(writer, nullptr);
    EXPECT_EQ(writer->GetLogNumber(), 200);
  }

  // Write to the new files
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    auto* writer = manager->GetWriter(i);
    std::string body = "new body for partition " + std::to_string(i);
    log::PartitionedLogWriter::WriteResult result;
    ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
    EXPECT_EQ(result.wal_number, 200);
  }

  // Both old and new files should exist
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    std::string old_fname =
        PartitionedWALManager::PartitionFileName(test_dir_, 100, i);
    std::string new_fname =
        PartitionedWALManager::PartitionFileName(test_dir_, 200, i);
    EXPECT_OK(env_->FileExists(old_fname))
        << "Old file should exist: " << old_fname;
    EXPECT_OK(env_->FileExists(new_fname))
        << "New file should exist: " << new_fname;
  }

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALManagerTest, DeleteObsoleteFiles) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);

  // Create files with multiple log numbers
  ASSERT_OK(manager->Open(100));
  ASSERT_OK(manager->RotateWALFiles(200));
  ASSERT_OK(manager->RotateWALFiles(300));
  ASSERT_OK(manager->CloseAll(WriteOptions()));

  // Verify all files exist
  for (uint64_t log_num : {100, 200, 300}) {
    for (uint32_t i = 0; i < kNumPartitions; i++) {
      std::string fname =
          PartitionedWALManager::PartitionFileName(test_dir_, log_num, i);
      EXPECT_OK(env_->FileExists(fname)) << "File should exist: " << fname;
    }
  }

  // Reopen with latest log number
  manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(400));

  // Delete files older than 250 (should delete 100 and 200)
  ASSERT_OK(manager->DeleteObsoleteFiles(250));

  // 100 and 200 should be deleted
  for (uint64_t log_num : {100, 200}) {
    for (uint32_t i = 0; i < kNumPartitions; i++) {
      std::string fname =
          PartitionedWALManager::PartitionFileName(test_dir_, log_num, i);
      EXPECT_TRUE(env_->FileExists(fname).IsNotFound())
          << "File should be deleted: " << fname;
    }
  }

  // 300 should still exist
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    std::string fname =
        PartitionedWALManager::PartitionFileName(test_dir_, 300, i);
    EXPECT_OK(env_->FileExists(fname)) << "File should exist: " << fname;
  }

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALManagerTest, SyncAll) {
  const uint32_t kNumPartitions = 3;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Write to all partitions
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    auto* writer = manager->GetWriter(i);
    std::string body = "test body " + std::to_string(i);
    log::PartitionedLogWriter::WriteResult result;
    ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
  }

  // Sync all
  ASSERT_OK(manager->SyncAll(WriteOptions()));

  // Close
  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALManagerTest, PartitionFileNameParsing) {
  // Test file name generation
  std::string fname =
      PartitionedWALManager::PartitionFileName("/path/to/wal", 123, 0);
  EXPECT_EQ(fname, "/path/to/wal/PARTITIONED_000123_P0.log");

  fname = PartitionedWALManager::PartitionFileName("/wal", 999999, 15);
  EXPECT_EQ(fname, "/wal/PARTITIONED_999999_P15.log");

  // Test parsing
  uint64_t log_number;
  uint32_t partition_id;

  EXPECT_TRUE(PartitionedWALManager::ParsePartitionFileName(
      "PARTITIONED_000123_P0.log", &log_number, &partition_id));
  EXPECT_EQ(log_number, 123);
  EXPECT_EQ(partition_id, 0);

  EXPECT_TRUE(PartitionedWALManager::ParsePartitionFileName(
      "PARTITIONED_999999_P15.log", &log_number, &partition_id));
  EXPECT_EQ(log_number, 999999);
  EXPECT_EQ(partition_id, 15);

  // Test with larger numbers
  EXPECT_TRUE(PartitionedWALManager::ParsePartitionFileName(
      "PARTITIONED_1234567890_P100.log", &log_number, &partition_id));
  EXPECT_EQ(log_number, 1234567890);
  EXPECT_EQ(partition_id, 100);

  // Invalid file names
  EXPECT_FALSE(PartitionedWALManager::ParsePartitionFileName(
      "PARTITIONED_123.log", &log_number, &partition_id));
  EXPECT_FALSE(PartitionedWALManager::ParsePartitionFileName(
      "000123.log", &log_number, &partition_id));
  EXPECT_FALSE(PartitionedWALManager::ParsePartitionFileName(
      "PARTITIONED_abc_P0.log", &log_number, &partition_id));
  EXPECT_FALSE(PartitionedWALManager::ParsePartitionFileName(
      "other_file.sst", &log_number, &partition_id));
}

TEST_F(PartitionedWALManagerTest, ZeroPartitionsError) {
  auto manager = CreateManager(0);
  EXPECT_TRUE(manager->Open(1).IsInvalidArgument());
}

TEST_F(PartitionedWALManagerTest, OperationsWhenNotOpen) {
  auto manager = CreateManager(4);

  // Operations before open should fail or return null
  EXPECT_EQ(manager->GetWriter(0), nullptr);
  EXPECT_TRUE(manager->SyncAll(WriteOptions()).IsInvalidArgument());
  EXPECT_TRUE(manager->RotateWALFiles(100).IsInvalidArgument());

  // Close when not open is OK
  EXPECT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALManagerTest, WriteToPartitions) {
  const uint32_t kNumPartitions = 4;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Write to each partition and verify
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    auto* writer = manager->GetWriter(i);
    ASSERT_NE(writer, nullptr);

    std::string body = "body for partition " + std::to_string(i);
    log::PartitionedLogWriter::WriteResult result;
    ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));

    EXPECT_EQ(result.wal_number, 1);
    EXPECT_EQ(result.offset, 0);  // First record in each file
    EXPECT_EQ(result.record_count, 1);
    EXPECT_GT(result.size, 0);
  }

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALManagerTest, MultipleRotations) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Perform multiple rotations
  for (uint64_t log_num = 2; log_num <= 10; log_num++) {
    // Write before rotation
    for (uint32_t i = 0; i < kNumPartitions; i++) {
      auto* writer = manager->GetWriter(i);
      std::string body = "body " + std::to_string(log_num - 1);
      log::PartitionedLogWriter::WriteResult result;
      ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
      EXPECT_EQ(result.wal_number, log_num - 1);
    }

    // Rotate
    ASSERT_OK(manager->RotateWALFiles(log_num));
    EXPECT_EQ(manager->GetCurrentLogNumber(), log_num);
    EXPECT_TRUE(manager->IsOpen());

    // Verify new writers have correct log number
    for (uint32_t i = 0; i < kNumPartitions; i++) {
      auto* writer = manager->GetWriter(i);
      ASSERT_NE(writer, nullptr);
      EXPECT_EQ(writer->GetLogNumber(), log_num);
    }
  }

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
