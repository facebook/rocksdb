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
    options.compression_manager = CreateAutoSkipCompressionManager();
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

class CostAwareTestFlushBlockPolicy : public FlushBlockPolicy {
 public:
  explicit CostAwareTestFlushBlockPolicy(const int window,
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
      auto set_exploration = [](bool value) {
        return [value](void* arg) {
          bool* exploration = static_cast<bool*>(arg);
          *exploration = value;
          // fprintf(stderr, "set exploration: %d\n", value);
        };
      };
      auto set_compression_type = [](size_t type) {
        return [type](void* arg) {
          size_t* compression_type = static_cast<size_t*>(arg);
          *compression_type = type;
          // fprintf(stderr, "set compression type: %lu\n", type);
        };
      };
      auto set_compression_level = [](size_t level) {
        return [level](void* arg) {
          size_t* compression_level = static_cast<size_t*>(arg);
          *compression_level = level;
          // fprintf(stderr, "set compression level: %lu\n", level);
        };
      };
      auto set_compress_size_with_delay = [](size_t size, int milliseconds) {
        return [size, milliseconds](void* arg) {
          size_t* compress_size = static_cast<size_t*>(arg);
          *compress_size = size;
          // fprintf(stderr, "set compression size: %lu time second: %d\n",
          // size, milliseconds);
          std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
        };
      };
      auto get_compress_size = [&](void* arg) {
        size_t* compress_size = static_cast<size_t*>(arg);
        output_size_ = *compress_size;
        // fprintf(stderr, "get compression size: %lu\n", *compress_size);
      };
      auto get_cpu_time = [&](void* arg) {
        size_t* time = static_cast<size_t*>(arg);
        cpu_time_ = *time;
        // fprintf(stderr, "get cpu time: %lu\n", *time);
      };
      auto get_cpu_predictor = [&](void* arg) {
        auto predictor = static_cast<CPUUtilPredictor*>(arg);
        predicted_cpu_time_ = predictor->Predict();
        // fprintf(stderr, "predicted cpu time: %lu\n", predicted_cpu_time_);
      };
      auto get_io_predictor = [&](void* arg) {
        auto predictor = static_cast<IOCostPredictor*>(arg);
        predicted_io_bytes = predictor->Predict();
        // fprintf(stderr, "predicted io cost: %lu\n", predicted_io_bytes);
      };
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();

      // use nth window to detect test cases and set the expected
      switch (nth_window) {
        case 0:
          // Set exploration to true and compression type to zstd and
          // compression level to 2
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::exploitOrExplore",
              set_exploration(true));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::"
              "SelectCompressionType",
              set_compression_type(6));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::"
              "SelectCompressionLevel",
              set_compression_level(2));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlockAndRecord::"
              "DelaySetCompressedOutputSize",
              set_compress_size_with_delay(100, 1000));
          break;
        case 1:
          // Verify that the Mocked cpu cost and io cost are predicted correctly
          EXPECT_EQ(output_size_, 100);
          EXPECT_GT(cpu_time_, 1000 * 1000);
          EXPECT_EQ(predicted_io_bytes, 100);
          EXPECT_GT(predicted_cpu_time_, 1000 * 1000);
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::exploitOrExplore",
              set_exploration(false));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::"
              "SelectCompressionType",
              set_compression_type(6));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::"
              "SelectCompressionLevel",
              set_compression_level(2));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlockAndRecord::"
              "DelaySetCompressedOutputSize",
              set_compress_size_with_delay(100, 1000));
          break;
        case 2:
          // Set exploration to true and compression type to  and compression
          // level to 2
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::exploitOrExplore",
              set_exploration(true));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::"
              "SelectCompressionType",
              set_compression_type(4));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::"
              "SelectCompressionLevel",
              set_compression_level(2));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlockAndRecord::"
              "DelaySetCompressedOutputSize",
              set_compress_size_with_delay(1000, 2000));
          break;
        case 3:
          // Verify that the Mocked cpu cost and io cost are predicted correctly
          EXPECT_EQ(output_size_, 1000);
          EXPECT_GT(cpu_time_, 2000 * 1000);
          EXPECT_EQ(predicted_io_bytes, 1000);
          EXPECT_GT(predicted_cpu_time_, 2000 * 1000);
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::exploitOrExplore",
              set_exploration(false));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::"
              "SelectCompressionType",
              set_compression_type(4));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlock::"
              "SelectCompressionLevel",
              set_compression_level(2));
          SyncPoint::GetInstance()->SetCallBack(
              "CostAwareCompressor::CompressBlockAndRecord::"
              "DelaySetCompressedOutputSize",
              set_compress_size_with_delay(1000, 2000));
          break;
      }
      // Add syncpoint to get the cpu and io cost
      SyncPoint::GetInstance()->SetCallBack(
          "CostAwareCompressor::CompressBlockAndRecord::"
          "GetCompressedOutputSize",
          get_compress_size);
      SyncPoint::GetInstance()->SetCallBack(
          "CostAwareCompressor::CompressBlockAndRecord::"
          "GetCPUTime",
          get_cpu_time);
      SyncPoint::GetInstance()->SetCallBack(
          "CostAwareCompressor::CompressBlockAndRecord::"
          "GetCPUPredictor",
          get_cpu_predictor);
      SyncPoint::GetInstance()->SetCallBack(
          "CostAwareCompressor::CompressBlockAndRecord::"
          "GetIOPredictor",
          get_io_predictor);
      SyncPoint::GetInstance()->EnableProcessing();
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

  static size_t cpu_time_, predicted_cpu_time_, predicted_io_bytes;
  static size_t output_size_;
};
size_t CostAwareTestFlushBlockPolicy::cpu_time_ = 0,
       CostAwareTestFlushBlockPolicy::predicted_cpu_time_ = 0,
       CostAwareTestFlushBlockPolicy::predicted_io_bytes = 0,
       CostAwareTestFlushBlockPolicy::output_size_ = 0;
class CostAwareTestFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  explicit CostAwareTestFlushBlockPolicyFactory(
      const int window, std::shared_ptr<Statistics> statistics)
      : window_(window), statistics_(statistics) {}

  virtual const char* Name() const override {
    return "CostAwareTestFlushBlockPolicyFactory";
  }

  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& data_block_builder) const override {
    (void)data_block_builder;
    return new CostAwareTestFlushBlockPolicy(window_, data_block_builder,
                                             statistics_);
  }

 private:
  int window_;
  std::shared_ptr<Statistics> statistics_;
};

class DBCPUIOPredictor : public DBTestBase {
 public:
  Options options;
  Random rnd_;
  int key_index_;
  DBCPUIOPredictor()
      : DBTestBase("db_cpuio_skip", /*env_do_fsync=*/true),
        options(CurrentOptions()),
        rnd_(231),
        key_index_(0) {
    options.compression_manager = CreateCostAwareCompressionManager();
    auto statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.statistics = statistics;
    options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
    BlockBasedTableOptions bbto;
    bbto.enable_index_compression = false;
    bbto.flush_block_policy_factory.reset(
        new CostAwareTestFlushBlockPolicyFactory(10, statistics));
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
// FIXME: the test is failing the assertion in auto_skip_compressor.cc
// when run on nightly build in build-linux-arm-test-full mode [1].
//
// [1]
// auto_skip_compressor.cc:101: Assertion `preferred != kNoCompression' failed.
TEST_F(DBAutoSkip, DISABLED_AutoSkipCompressionManager) {
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
TEST_F(DBCPUIOPredictor, CUPIOAwareCompressorManager) {
  // making sure that the compression is supported
  if (GetSupportedCompressions().size() > 1) {
    const int kValueSize = 20000;
    // This denotes the first window
    // Mocked to have specific cpu utilization and io cost
    CompressionUnfriendlyPut(6, kValueSize);
    CompressionFriendlyPut(4, kValueSize);
    // In this window we verify the correct prediction has been made
    CompressionUnfriendlyPut(6, kValueSize);
    CompressionFriendlyPut(4, kValueSize);
    // In this winodw we mock for another alogrithm and compression level to
    // have specific cpu utilization and io cost
    CompressionUnfriendlyPut(4, kValueSize);
    CompressionFriendlyPut(6, kValueSize);
    // In this window we verify the correct prediction has been made
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
