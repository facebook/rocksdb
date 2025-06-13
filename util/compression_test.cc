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
#include "util/auto_skip_compressor.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class MyFlushBlockPolicy : public FlushBlockPolicy {
 public:
  explicit MyFlushBlockPolicy(const int window,
                              const BlockBuilder& data_block_builder,
                              std::shared_ptr<Statistics> statistics)
      : window_(window),
        num_keys_(0),
        data_block_builder_(data_block_builder),
        statistics_(statistics) {}

  bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
    auto multiple_of_10 = num_keys_ / window_;
    if (data_block_builder_.empty()) {
      // First key in this block
      return false;
    }
    // Check every 10 keys
    if (num_keys_ % window_ == 0) {
      // get stats
      auto set_exploration = [&](void* arg) {
        bool* exploration = static_cast<bool*>(arg);
        *exploration = true;
      };
      auto unset_exploration = [&](void* arg) {
        bool* exploration = static_cast<bool*>(arg);
        *exploration = false;
      };
      // Test 1: Just explore and check correct rejection ratio is predicted
      // (6/10
      // -> dataset not favourable for compression)
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      if (multiple_of_10 % 2 == 0) {
        SyncPoint::GetInstance()->SetCallBack(
            "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
            set_exploration);
        fprintf(stdout, "exploration turned on\n");
      } else {
        SyncPoint::GetInstance()->SetCallBack(
            "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
            unset_exploration);
        fprintf(stdout, "exploitation turned on\n");
      }
      SyncPoint::GetInstance()->EnableProcessing();

      auto compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
      auto bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
      auto rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
      auto total = compressed_count + rejected_count + bypassed_count;
      int rejection_percentage, bypassed_percentage, compressed_percentage;
      if (total != 0) {
        rejection_percentage = rejected_count * 100 / total;
        bypassed_percentage = bypassed_count * 100 / total;
        compressed_percentage = compressed_count * 100 / total;
      }
      // use mulitple of 10 to get correct assertion
      switch (multiple_of_10) {
        case 1:
          EXPECT_EQ(rejection_percentage, 60);
          EXPECT_EQ(bypassed_percentage, 0);
          EXPECT_EQ(compressed_percentage, 40);
          break;
        case 2:
          EXPECT_EQ(rejection_percentage, 0);
          EXPECT_EQ(bypassed_percentage, 100);
          EXPECT_EQ(compressed_percentage, 0);
          break;
        case 3:
          EXPECT_EQ(rejection_percentage, 40);
          EXPECT_EQ(bypassed_percentage, 0);
          EXPECT_EQ(compressed_percentage, 60);
          break;
        case 4:
          EXPECT_EQ(rejection_percentage, 60);
          EXPECT_EQ(bypassed_percentage, 0);
          EXPECT_EQ(compressed_percentage, 40);
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

class MyFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  explicit MyFlushBlockPolicyFactory(const int window,
                                     std::shared_ptr<Statistics> statistics)
      : window_(window), statistics_(statistics) {}

  virtual const char* Name() const override {
    return "MyFlushBlockPolicyFactory";
  }

  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& data_block_builder) const override {
    (void)data_block_builder;
    return new MyFlushBlockPolicy(window_, data_block_builder, statistics_);
  }

 private:
  int window_;
  std::shared_ptr<Statistics> statistics_;
};

class DBAutoSkip : public DBTestBase {
 public:
  Options options;
  Random rnd_;
  int key_cursor_ = 0;
  DBAutoSkip()
      : DBTestBase("db_auto_skip", /*env_do_fsync=*/true),
        options(CurrentOptions()),
        rnd_(231) {
    options.compression_manager =
        CreateAutoSkipCompressionManager(GetDefaultBuiltinCompressionManager());
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
    BlockBasedTableOptions bbto;
    bbto.enable_index_compression = false;
    bbto.flush_block_policy_factory.reset(
        new MyFlushBlockPolicyFactory(10, options.statistics));
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);
  }
  uint64_t PopStat(Tickers t) {
    return options.statistics->getAndResetTickerCount(t);
  }
  bool CompressionFriendlyPut(const int no_of_kvs, const int size_of_value) {
    auto value = std::string(size_of_value, 'A');
    for (int i = 0; i < no_of_kvs; ++i) {
      auto status = Put(Key(key_cursor_), value);
      EXPECT_EQ(status.ok(), true);
      key_cursor_++;
    }
    return true;
  }
  bool CompressionUnfriendlyPut(const int no_of_kvs, const int size_of_value) {
    auto value = rnd_.RandomBinaryString(size_of_value);
    for (int i = 0; i < no_of_kvs; ++i) {
      auto status = Put(Key(key_cursor_), value);
      EXPECT_EQ(status.ok(), true);
      key_cursor_++;
    }
    return true;
  }
};

// test case just to make sure auto compression manager is working
TEST_F(DBAutoSkip, AutoSkipCompressionManager) {
  if (GetSupportedCompressions().size() > 1) {
    // AutoSkipCompressionManager starts with rejection ratio 0 i.e.
    // compression enabled enough data to change the decision
    const int kValueSize = 20000;
    // Set the rejection ratio to 60%
    CompressionUnfriendlyPut(6, kValueSize);
    CompressionFriendlyPut(4, kValueSize);
    // All the keys should be bypassed
    CompressionUnfriendlyPut(6, kValueSize);
    CompressionFriendlyPut(4, kValueSize);
    // Set rejection ratio to 40%
    CompressionUnfriendlyPut(4, kValueSize);
    CompressionFriendlyPut(6, kValueSize);
    // Compression attempted 6 rejected 4 accepted
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
