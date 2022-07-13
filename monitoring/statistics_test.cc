//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/statistics.h"

#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_type.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class StatisticsTest : public testing::Test {};

// Sanity check to make sure that contents and order of TickersNameMap
// match Tickers enum
TEST_F(StatisticsTest, SanityTickers) {
  EXPECT_EQ(static_cast<size_t>(Tickers::TICKER_ENUM_MAX),
            TickersNameMap.size());

  for (uint32_t t = 0; t < Tickers::TICKER_ENUM_MAX; t++) {
    auto pair = TickersNameMap[static_cast<size_t>(t)];
    ASSERT_EQ(pair.first, t) << "Miss match at " << pair.second;
  }
}

// Sanity check to make sure that contents and order of HistogramsNameMap
// match Tickers enum
TEST_F(StatisticsTest, SanityHistograms) {
  EXPECT_EQ(static_cast<size_t>(Histograms::HISTOGRAM_ENUM_MAX),
            HistogramsNameMap.size());

  for (uint32_t h = 0; h < Histograms::HISTOGRAM_ENUM_MAX; h++) {
    auto pair = HistogramsNameMap[static_cast<size_t>(h)];
    ASSERT_EQ(pair.first, h) << "Miss match at " << pair.second;
  }
}

TEST_F(StatisticsTest, NoNameStats) {
  static std::unordered_map<std::string, OptionTypeInfo> no_name_opt_info = {
#ifndef ROCKSDB_LITE
      {"inner",
       OptionTypeInfo::AsCustomSharedPtr<Statistics>(
           0, OptionVerificationType::kByName,
           OptionTypeFlags::kAllowNull | OptionTypeFlags::kCompareNever)},
#endif  // ROCKSDB_LITE
  };

  class DefaultNameStatistics : public Statistics {
   public:
    DefaultNameStatistics(const std::shared_ptr<Statistics>& stats = nullptr)
        : inner(stats) {
      RegisterOptions("", &inner, &no_name_opt_info);
    }

    uint64_t getTickerCount(uint32_t /*tickerType*/) const override {
      return 0;
    }
    void histogramData(uint32_t /*type*/,
                       HistogramData* const /*data*/) const override {}
    void recordTick(uint32_t /*tickerType*/, uint64_t /*count*/) override {}
    void setTickerCount(uint32_t /*tickerType*/, uint64_t /*count*/) override {}
    uint64_t getAndResetTickerCount(uint32_t /*tickerType*/) override {
      return 0;
    }
    std::shared_ptr<Statistics> inner;
  };
  ConfigOptions options;
  options.ignore_unsupported_options = false;
  auto stats = std::make_shared<DefaultNameStatistics>();
  ASSERT_STREQ(stats->Name(), "");
#ifndef ROCKSDB_LITE
  ASSERT_EQ("", stats->ToString(
                    options));  // A stats with no name with have no options...
  ASSERT_OK(stats->ConfigureFromString(options, "inner="));
  ASSERT_EQ("", stats->ToString(
                    options));  // A stats with no name with have no options...
  ASSERT_NE(stats->inner, nullptr);
  ASSERT_NE("", stats->inner->ToString(options));  // ... even if it does...
#endif                                             // ROCKSDB_LITE
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
