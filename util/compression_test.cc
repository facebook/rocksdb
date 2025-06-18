//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Testing the features of auto skip compression manager
//
// ***********************************************************************
// EXPERIMENTAL - subject to change while under development
// ***********************************************************************

#include <cstdlib>
#include <memory>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/flush_block_policy.h"
#include "table/block_based/block_builder.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class AutoSkipTestFlushBlockPolicy : public FlushBlockPolicy {
 public:
  explicit AutoSkipTestFlushBlockPolicy(const int window,
                                        const BlockBuilder& data_block_builder,
                                        std::shared_ptr<Statistics> statistics)
      : window_(window),
        num_keys_(0),
        data_block_builder_(data_block_builder),
        statistics_(statistics) {}

  bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
    auto nth_window = num_keys_ / window_;
    if (data_block_builder_.empty()) {
      // First key in this block
      return false;
    }
    // Check every window
    if (num_keys_ % window_ == 0) {
      auto set_exploration = [&](void* arg) {
        bool* exploration = static_cast<bool*>(arg);
        *exploration = true;
      };
      auto unset_exploration = [&](void* arg) {
        bool* exploration = static_cast<bool*>(arg);
        *exploration = false;
      };
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      // We force exploration to set the predicted rejection ratio for odd
      // window and then test that the prediction is exploited in the even
      // window
      if (nth_window % 2 == 0) {
        SyncPoint::GetInstance()->SetCallBack(
            "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
            set_exploration);
      } else {
        SyncPoint::GetInstance()->SetCallBack(
            "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
            unset_exploration);
      }
      SyncPoint::GetInstance()->EnableProcessing();

      auto compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
      auto bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
      auto rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
      auto total = compressed_count + rejected_count + bypassed_count;
      int rejection_percentage, bypassed_percentage, compressed_percentage;
      if (total != 0) {
        rejection_percentage = static_cast<int>(rejected_count * 100 / total);
        bypassed_percentage = static_cast<int>(bypassed_count * 100 / total);
        compressed_percentage =
            static_cast<int>(compressed_count * 100 / total);
        // use nth window to detect test cases and set the expected
        switch (nth_window) {
          case 1:
            // In first window we only explore and thus here we verify that the
            // correct prediction has been made by the end of the window
            // Since 6 of 10 blocks are compression unfriendly, the predicted
            // rejection ratio should be 60%
            EXPECT_EQ(rejection_percentage, 60);
            EXPECT_EQ(bypassed_percentage, 0);
            EXPECT_EQ(compressed_percentage, 40);
            break;
          case 2:
            // With the rejection ratio set to 0.6 all the blocks should be
            // bypassed in next window
            EXPECT_EQ(rejection_percentage, 0);
            EXPECT_EQ(bypassed_percentage, 100);
            EXPECT_EQ(compressed_percentage, 0);
            break;
          case 3:
            // In third window we only explore and verify that the correct
            // prediction has been made by the end of the window
            // since 4 of 10 blocks are compression ufriendly, the predicted
            // rejection ratio should be 40%
            EXPECT_EQ(rejection_percentage, 40);
            EXPECT_EQ(bypassed_percentage, 0);
            EXPECT_EQ(compressed_percentage, 60);
            break;
          case 4:
            // With the rejection ratio set to 0.4 all the blocks should be
            // attempted to be compressed
            // 6 of 10 blocks are compression unfriendly and thus should be
            // rejected 4 of 10 blocks are compression friendly and thus should
            // be compressed
            EXPECT_EQ(rejection_percentage, 60);
            EXPECT_EQ(bypassed_percentage, 0);
            EXPECT_EQ(compressed_percentage, 40);
        }
      }
    }
    num_keys_++;
    return true;
  }
  uint64_t PopStat(Tickers t) { return statistics_->getAndResetTickerCount(t); }

 private:
  int window_;
  int num_keys_;
  const BlockBuilder& data_block_builder_;
  std::shared_ptr<Statistics> statistics_;
};

class AutoSkipTestFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  explicit AutoSkipTestFlushBlockPolicyFactory(
      const int window, std::shared_ptr<Statistics> statistics)
      : window_(window), statistics_(statistics) {}

  virtual const char* Name() const override {
    return "AutoSkipTestFlushBlockPolicyFactory";
  }

  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& data_block_builder) const override {
    (void)data_block_builder;
    return new AutoSkipTestFlushBlockPolicy(window_, data_block_builder,
                                            statistics_);
  }

 private:
  int window_;
  std::shared_ptr<Statistics> statistics_;
};

class DBAutoSkip : public DBTestBase {
 public:
  Options options;
  Random rnd_;
  int key_index_;
  DBAutoSkip()
      : DBTestBase("db_auto_skip", /*env_do_fsync=*/true),
        options(CurrentOptions()),
        rnd_(231),
        key_index_(0) {
    options.compression_manager =
        CreateAutoSkipCompressionManager(GetDefaultBuiltinCompressionManager());
    auto statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.statistics = statistics;
    options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
    BlockBasedTableOptions bbto;
    bbto.enable_index_compression = false;
    bbto.flush_block_policy_factory.reset(
        new AutoSkipTestFlushBlockPolicyFactory(10, statistics));
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);
  }

  bool CompressionFriendlyPut(const int no_of_kvs, const int size_of_value) {
    auto value = std::string(size_of_value, 'A');
    for (int i = 0; i < no_of_kvs; ++i) {
      auto status = Put(Key(key_index_), value);
      EXPECT_EQ(status.ok(), true);
      key_index_++;
    }
    return true;
  }
  bool CompressionUnfriendlyPut(const int no_of_kvs, const int size_of_value) {
    auto value = rnd_.RandomBinaryString(size_of_value);
    for (int i = 0; i < no_of_kvs; ++i) {
      auto status = Put(Key(key_index_), value);
      EXPECT_EQ(status.ok(), true);
      key_index_++;
    }
    return true;
  }
};

TEST_F(DBAutoSkip, AutoSkipCompressionManager) {
  if (GetSupportedCompressions().size() > 1) {
    const int kValueSize = 20000;
    // This will set the rejection ratio to 60%
    CompressionUnfriendlyPut(6, kValueSize);
    CompressionFriendlyPut(4, kValueSize);
    // This will verify all the data block compressions are bypassed based on
    // previous prediction
    CompressionUnfriendlyPut(6, kValueSize);
    CompressionFriendlyPut(4, kValueSize);
    // This will set the rejection ratio to 40%
    CompressionUnfriendlyPut(4, kValueSize);
    CompressionFriendlyPut(6, kValueSize);
    // This will verify all the data block compression are attempted based on
    // previous prediction
    // Compression will be rejected for 6 compression unfriendly blocks
    // Compression will be accepted for 4 compression friendly blocks
    CompressionUnfriendlyPut(6, kValueSize);
    CompressionFriendlyPut(4, kValueSize);
    // Extra block write to ensure that the all above cases are checked
    CompressionFriendlyPut(6, kValueSize);
    CompressionFriendlyPut(4, kValueSize);
    ASSERT_OK(Flush());
  }
}
}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
