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
#include "port/stack_trace.h"
#include "test_util/testutil.h"
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
    options.auto_tune = true;
    // options.compression = kZSTD;
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
      Put(Key(key_cursor_), value);
      key_cursor_++;
    }
    return true;
  }
  bool CompressionUnfriendlyPut(const int no_of_kvs, const int size_of_value) {
    auto value = rnd_.RandomBinaryString(size_of_value);
    for (int i = 0; i < no_of_kvs; ++i) {
      Put(Key(key_cursor_), value);
      key_cursor_++;
    }
    return true;
  }
};

// test case just to make sure auto compression manager is working
TEST_F(DBAutoSkip, AutoSkipCompressionManager) {
  // AutoSkipCompressionManager starts with rejection ratio 0 i.e. compression
  // enabled
  constexpr int kCount =
      5000;  // high enough for compression manager to register the changes
  // enough data to change the decision
  const int kValueSize = 20000;
  CompressionUnfriendlyPut(kCount, kValueSize);
  ASSERT_OK(Flush());
  // reset the following stats to zero
  PopStat(NUMBER_BLOCK_COMPRESSED);
  PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  // should stick with the not compressing decision
  CompressionUnfriendlyPut(kCount, kValueSize);
  ASSERT_OK(Flush());
  // Test the compression is disabled
  // Make sure that Compressor output is properly calculated as bypassed
  auto compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
  auto bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  auto rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  auto rejection_ratio =
      rejected_count * 100 / (compressed_count + rejected_count);
  EXPECT_GT(rejection_ratio, 50);
  auto bypassed_rate = bypassed_count * 100 /
                       (compressed_count + rejected_count + bypassed_count);
  EXPECT_GT(bypassed_rate, 50);

  // Test the compression is enabled when passing highly compressible data
  CompressionFriendlyPut(kCount, kValueSize);
  ASSERT_OK(Flush());
  PopStat(NUMBER_BLOCK_COMPRESSED);
  PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  // should stick with the compression decision
  CompressionFriendlyPut(kCount, kValueSize);
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

}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
