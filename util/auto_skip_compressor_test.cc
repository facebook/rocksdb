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

#include "util/auto_skip_compressor.h"

#include <cstdlib>
#include <memory>

#include "db/db_test_util.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBAutoSkip : public DBTestBase {
 public:
  Options options;
  DBAutoSkip()
      : DBTestBase("db_auto_skip", /*env_do_fsync=*/true),
        options(CurrentOptions()) {
    options.compression = kZSTD;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
    BlockBasedTableOptions bbto;
    bbto.enable_index_compression = false;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    auto mgr = std::make_shared<AutoSkipCompressorManager>(
        GetDefaultBuiltinCompressionManager());
    options.compression_manager = mgr;
    DestroyAndReopen(options);
  }
  uint64_t PopStat(Tickers t) {
    return options.statistics->getAndResetTickerCount(t);
  };
};

// test case just to make sure auto compression manager is working
TEST_F(DBAutoSkip, AutoSkipCompressionManagerEnablesDisablesCompression) {
  // Start with predicted rejection percentage of 0 i.e. compression enabled
  Random rnd(301);
  std::string value;
  constexpr int kCount = 1000;  // high enough to get window size of compression
  // enough data to change the decision
  auto count = 0;
  for (int i = 0; i < kCount; ++i) {
    value = rnd.RandomBinaryString(20000);
    ASSERT_OK(Put(Key(count + i), value));
    ASSERT_EQ(Get(Key(count + i)), value);
  }
  ASSERT_OK(Flush());
  auto compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
  auto bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  auto rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  count = count + kCount;
  // should stick with the not optimize decision
  for (int i = 0; i < kCount; ++i) {
    value = rnd.RandomBinaryString(20000);
    ASSERT_OK(Put(Key(count + i), value));
    ASSERT_EQ(Get(Key(count + i)), value);
  }
  ASSERT_OK(Flush());
  // Test the compression is disabled
  // Make sure that Compressor output is properly calculated as bypassed
  compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
  bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  auto rejection_ratio =
      rejected_count * 100 / (compressed_count + rejected_count);
  EXPECT_GT(rejection_ratio, 50);
  auto bypassed_rate = bypassed_count * 100 /
                       (compressed_count + rejected_count + bypassed_count);
  EXPECT_GT(bypassed_rate, 50);
  count = count + kCount;

  // Test the compression is enabled when passing highly compressible data
  value = std::string(20000, 'A');
  for (int i = 0; i < kCount; ++i) {
    ASSERT_OK(Put(Key(count + i), value));
    ASSERT_EQ(Get(Key(count + i)), value);
  }
  ASSERT_OK(Flush());
  compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
  bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  count = count + kCount;
  // should stick with the compression decision
  for (int i = 0; i < kCount; ++i) {
    ASSERT_OK(Put(Key(count + i), value));
    ASSERT_EQ(Get(Key(count + i)), value);
  }
  ASSERT_OK(Flush());
  // Test the compression is disabled
  compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
  bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  rejection_ratio = rejected_count * 100 / (compressed_count + rejected_count);
  EXPECT_LT(rejection_ratio, 50);
  bypassed_rate = bypassed_count * 100 /
                  (compressed_count + rejected_count + bypassed_count);
  EXPECT_LT(bypassed_rate, 50);
}
TEST(WindowBasedRejectionPredictorTest, CorrectPrediction) {
  WindowBasedRejectionPredictor model_(10);
  CompressionOptions opts;
  opts.max_compressed_bytes_per_kb = 1000;
  std::string uncompressed_data("A", 10000);
  Slice block_data(uncompressed_data);
  std::string compressed_data("A", 500);
  // Test ability to cold start
  model_.SetPrediction(20);
  auto prediction = model_.Predict();
  EXPECT_EQ(prediction, 20);
  // set window 10, 5 rejection , 5 compression -> should get predicted
  // 5 bypass
  for (auto i = 0; i < 5; i++) {
    model_.Record(block_data, &compressed_data, opts);
  }
  // rejection of 0.5 after 5 rejection
  for (auto i = 0; i < 5; i++) {
    model_.Record(block_data, &uncompressed_data, opts);
  }
  prediction = model_.Predict();
  EXPECT_EQ(prediction, 50);
  // 8 compressed
  for (auto i = 0; i < 8; i++) {
    model_.Record(block_data, &compressed_data, opts);
  }
  // 2 rejection
  for (auto i = 0; i < 2; i++) {
    model_.Record(block_data, &uncompressed_data, opts);
  }
  prediction = model_.Predict();
  EXPECT_EQ(prediction, 20);
  // 2 compressed
  for (auto i = 0; i < 2; i++) {
    model_.Record(block_data, &compressed_data, opts);
  }
  // 8 rejection
  for (auto i = 0; i < 8; i++) {
    model_.Record(block_data, &uncompressed_data, opts);
  }
  prediction = model_.Predict();
  EXPECT_EQ(prediction, 80);
}
TEST(AutoSkipCompressor, ExplorationRate) {
  // Create AutoSkipCompressor object
  CompressionOptions opts;
  opts.max_compressed_bytes_per_kb = 1000;
  AutoSkipCompressorWrapper compressor(opts, kZSTD);
  compressor.SetMinExplorationPercentage(10);

  // Create a new Predictor object with Predict function always returning 100
  class CustomPredictor : public RejectionRatioPredictor {
   public:
    int Predict() const override { return 100; }
  };
  compressor.predictor_ = std::make_unique<CustomPredictor>();

  // Run the compress 100 times
  std::string value = std::string("A", 1000);
  auto compression_count_ = 0;
  auto bypass_count_ = 0;
  auto rejection_count_ = 0;
  for (int i = 0; i < 100; ++i) {
    std::string compressed_output;
    CompressionType out_compression_type;
    Compressor::ManagedWorkingArea wa;
    Status status = compressor.CompressBlock(Slice(value), &compressed_output,
                                             &out_compression_type, &wa);
    ASSERT_OK(status);
    if (out_compression_type == kNoCompression) {
      if (compressed_output.empty()) {
        bypass_count_++;
      } else {
        rejection_count_++;
      }
    } else {
      compression_count_++;
    }
  }

  EXPECT_GE(bypass_count_, 80);
  EXPECT_LE(compression_count_, 20);
  EXPECT_EQ(rejection_count_, 0);
  compressor.SetMinExplorationPercentage(0);
  compression_count_ = 0;
  bypass_count_ = 0;
  rejection_count_ = 0;
  for (int i = 0; i < 100; ++i) {
    std::string compressed_output;
    CompressionType out_compression_type;
    Compressor::ManagedWorkingArea wa;
    Status status = compressor.CompressBlock(Slice(value), &compressed_output,
                                             &out_compression_type, &wa);
    ASSERT_OK(status);
    if (out_compression_type == kNoCompression) {
      if (compressed_output.empty()) {
        bypass_count_++;
      } else {
        rejection_count_++;
      }
    } else {
      compression_count_++;
    }
  }
  EXPECT_GE(bypass_count_, 95);
}

}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
