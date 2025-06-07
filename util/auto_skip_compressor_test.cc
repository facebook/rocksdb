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

#include <atomic>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>

#include "db/db_test_util.h"
#include "db/read_callback.h"
#include "db/version_edit.h"
#include "env/fs_readonly.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/experimental.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/trace_record_result.h"
#include "rocksdb/utilities/replayer.h"
#include "rocksdb/wal_filter.h"
#include "test_util/testutil.h"
#include "util/defer.h"
#include "util/random.h"
#include "util/simple_mixed_compressor.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBAutoSkip : public DBTestBase {
 public:
  DBAutoSkip() : DBTestBase("db_auto_skip", /*env_do_fsync=*/true) {
    Options options = CurrentOptions();
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
};
// test case just to make sure auto compression manager is working
// TEST_F(DBAutoSkip, AutoSkipCompressionManagerAliveTest) {
//   Random rnd(301);
//   std::vector<std::string> values;
//   constexpr int kCount = 13;
//   for (int i = 0; i < kCount; ++i) {
//     std::string value;
//     if (i == 6) {
//       // One non-compressible block
//       value = rnd.RandomBinaryString(20000);
//     } else {
//       test::CompressibleString(&rnd, 0.1, 20000, &value);
//     }
//     values.push_back(value);
//     ASSERT_OK(Put(Key(i), value));
//     ASSERT_EQ(Get(Key(i)), value);
//   }
//   ASSERT_OK(Flush());

//   // Ensure well-formed for reads
//   for (int i = 0; i < kCount; ++i) {
//     ASSERT_NE(Get(Key(i)), "NOT_FOUND");
//     ASSERT_EQ(Get(Key(i)), values[i]);
//   }

//   // Write test code to
// }
// test case just to make sure auto compression manager is working
TEST_F(DBAutoSkip, AutoSkipCompressionManagerEnablesCompression) {
  // Start with prediction of 0
  Random rnd(301);
  Options options = CurrentOptions();
  std::vector<std::string> values;
  std::string value("A", 20000);
  // test::CompressibleString(&rnd, 0.1, 20000, &value);
  constexpr int kCount = 100;
  auto PopStat = [&](Tickers t) -> uint64_t {
    return options.statistics->getAndResetTickerCount(t);
  };
  for (int i = 0; i < kCount; ++i) {
    values.push_back(value);
    ASSERT_OK(Put(Key(i), value));
    ASSERT_EQ(Get(Key(i)), value);
  }
  ASSERT_OK(Flush());
  auto compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
  auto bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  auto rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  // should stick with the not optimize decision
  for (int i = 0; i < kCount; ++i) {
    value = rnd.RandomBinaryString(20000);
    ASSERT_OK(Put(Key(i + kCount), value));
    ASSERT_EQ(Get(Key(i + kCount)), value);
  }
  ASSERT_OK(Flush());
  // Ensure well-formed for reads
  compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
  bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  auto rejection_ratio = rejected_count / (compressed_count + bypassed_count);
  EXPECT_LT(rejection_ratio, 0.5);
}

// test case just to make sure auto compression manager is working
TEST_F(DBAutoSkip, AutoSkipCompressionManagerDisablesCompression) {
  // Start with prediction of 1
  Random rnd(301);
  Options options = CurrentOptions();
  std::string value;
  constexpr int kCount = 100;
  auto PopStat = [&](Tickers t) -> uint64_t {
    return options.statistics->getAndResetTickerCount(t);
  };
  // enough data to change the decision
  for (int i = 0; i < kCount; ++i) {
    value = rnd.RandomBinaryString(20000);
    ASSERT_OK(Put(Key(i), value));
    ASSERT_EQ(Get(Key(i)), value);
  }
  ASSERT_OK(Flush());
  auto compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
  auto bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  auto rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  // should stick with the not optimize decision
  for (int i = 0; i < kCount; ++i) {
    value = rnd.RandomBinaryString(20000);
    ASSERT_OK(Put(Key(i), value));
    ASSERT_EQ(Get(Key(i)), value);
  }
  ASSERT_OK(Flush());
  // Test the compression is disabled
  compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
  bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
  rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
  auto rejection_ratio = rejected_count / (compressed_count + bypassed_count);
  EXPECT_GT(rejection_ratio, 0.5);
}
TEST(WindowRejectionModelTest, CorrectPrediction) {
  WindowRejectionModel model_(10);
  // Test ability to cold start
  model_.SetPrediction(20);
  auto prediction = model_.Predict();
  EXPECT_EQ(prediction, 20);
  // set window 10, 5 rejection , 5 compression -> should get predicted
  // 5 bypass
  for (auto i = 0; i < 5; i++) {
    std::string compressed_string("blah");
    CompressionType type_ = kZSTD;
    model_.Record(&compressed_string, &type_);
  }
  // rejection of 0.5 after 5 rejection
  for (auto i = 0; i < 5; i++) {
    std::string compressed_string("blah");
    CompressionType type_ = kNoCompression;
    model_.Record(&compressed_string, &type_);
  }
  prediction = model_.Predict();
  EXPECT_EQ(prediction, 50);
  // 8 compressed
  for (auto i = 0; i < 8; i++) {
    std::string compressed_string("blah");
    CompressionType type_ = kZSTD;
    model_.Record(&compressed_string, &type_);
  }
  // 2 rejection
  for (auto i = 0; i < 2; i++) {
    std::string compressed_string("blah");
    CompressionType type_ = kNoCompression;
    model_.Record(&compressed_string, &type_);
  }
  prediction = model_.Predict();
  EXPECT_EQ(prediction, 20);
  // 2 compressed
  for (auto i = 0; i < 2; i++) {
    std::string compressed_string("blah");
    CompressionType type_ = kZSTD;
    model_.Record(&compressed_string, &type_);
  }
  // 8 rejection
  for (auto i = 0; i < 8; i++) {
    std::string compressed_string("blah");
    CompressionType type_ = kNoCompression;
    model_.Record(&compressed_string, &type_);
  }
  prediction = model_.Predict();
  EXPECT_EQ(prediction, 80);
}
}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
