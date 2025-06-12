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
#include "test_util/testutil.h"
#include "util/auto_skip_compressor.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

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
    // AutoSkipCompressionManager starts with rejection ratio 0 i.e. compression
    // enabled
    // enough data to change the decision
    const int kValueSize = 20000;
    auto set_exploration = [&](void* arg) {
      bool* exploration = static_cast<bool*>(arg);
      *exploration = true;
    };
    // auto unset_exploration = [&](void* arg) {
    //   bool* exploration = static_cast<bool*>(arg);
    //   *exploration = false;
    // };
    // Test 1: Just explore and check correct rejection ratio is predicted (6/10
    // -> dataset not favourable for compression)
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->SetCallBack(
        "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
        set_exploration);
    SyncPoint::GetInstance()->EnableProcessing();
    CompressionUnfriendlyPut(6, kValueSize);
    CompressionFriendlyPut(4, kValueSize);
    ASSERT_OK(Flush());

    auto compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
    auto bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
    auto rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
    auto total = compressed_count + rejected_count + bypassed_count;
    auto rejection_percentage = rejected_count * 100 / total;
    auto bypassed_percentage = bypassed_count * 100 / total;
    auto compressed_percentage = compressed_count * 100 / total;
    EXPECT_EQ(rejection_percentage, 60);
    EXPECT_EQ(bypassed_percentage, 0);
    EXPECT_EQ(compressed_percentage, 40);
    // Test 2: Use the model prediction to make decision to not compress the
    // blocks
    // SyncPoint::GetInstance()->DisableProcessing();
    // SyncPoint::GetInstance()->ClearAllCallBacks();
    // SyncPoint::GetInstance()->SetCallBack(
    //     "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
    //     unset_exploration);
    // SyncPoint::GetInstance()->EnableProcessing();
    // Test 3: Force the model to explore and thus make sure the rejection ratio
    // is predicted to be 4/10

    CompressionUnfriendlyPut(4, kValueSize);
    CompressionFriendlyPut(6, kValueSize);
    ASSERT_OK(Flush());
    // Make sure that Compressor output is properly calculated as bypassed
    compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
    bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
    rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
    total = compressed_count + rejected_count + bypassed_count;
    rejection_percentage = rejected_count * 100 / total;
    bypassed_percentage = bypassed_count * 100 / total;
    compressed_percentage = compressed_count * 100 / total;
    EXPECT_EQ(rejection_percentage, 0);
    EXPECT_EQ(bypassed_percentage, 100);
    EXPECT_EQ(compressed_percentage, 0);
  }
}
/**
TEST(CompressionRejectionProbabilityPredictorTest,
     UsesLastWindowSizeDataForPrediction) {
  CompressionRejectionProbabilityPredictor predictor_(10);
  CompressionOptions opts;
  opts.max_compressed_bytes_per_kb = 1000;
  std::string uncompressed_data(10000, 'A');
  Slice block_data(uncompressed_data);
  std::string compressed_data(500, 'A');
  // Test ability to cold start
  predictor_.TEST_SetPrediction(20);
  auto prediction = predictor_.Predict();
  EXPECT_EQ(prediction, 20);
  // set window 10, 5 rejection , 5 compression -> should get predicted
  // 5 bypass
  for (auto i = 0; i < 5; i++) {
    predictor_.Record(block_data, &compressed_data, opts);
  }
  // rejection of 0.5 after 5 rejection
  for (auto i = 0; i < 5; i++) {
    predictor_.Record(block_data, &uncompressed_data, opts);
  }
  prediction = predictor_.Predict();
  EXPECT_EQ(prediction, 50);
  // 8 compressed
  for (auto i = 0; i < 8; i++) {
    predictor_.Record(block_data, &compressed_data, opts);
  }
  // 2 rejection
  for (auto i = 0; i < 2; i++) {
    predictor_.Record(block_data, &uncompressed_data, opts);
  }
  prediction = predictor_.Predict();
  EXPECT_EQ(prediction, 20);
  // 2 compressed
  for (auto i = 0; i < 2; i++) {
    predictor_.Record(block_data, &compressed_data, opts);
  }
  // 8 rejection
  for (auto i = 0; i < 8; i++) {
    predictor_.Record(block_data, &uncompressed_data, opts);
  }
  prediction = predictor_.Predict();
  EXPECT_EQ(prediction, 80);
}
**/
}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
