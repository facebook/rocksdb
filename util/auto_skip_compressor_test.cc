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
  DBAutoSkip() : DBTestBase("db_auto_skip", /*env_do_fsync=*/true) {}
};
TEST_F(DBAutoSkip, AutoSkipCompressionManager) {
  {
    // Test that we can use a custom CompressionManager to wrap the built-in
    // CompressionManager, thus adopting a custom *strategy* based on existing
    // algorithms. This will "mark" some blocks (in their contents) as "do not
    // compress", i.e. no attempt to compress, and some blocks as "reject
    // compression", i.e. compression attempted but rejected because of ratio
    // or otherwise. These cases are distinguishable for statistics that
    // approximate "wasted effort".
    static std::string kDoNotCompress = "do_not_compress";
    static std::string kRejectCompression = "reject_compression";

    struct MyCompressor : public CompressorWrapper {
      using CompressorWrapper::CompressorWrapper;

      Status CompressBlock(Slice uncompressed_data,
                           std::string* compressed_output,
                           CompressionType* out_compression_type,
                           ManagedWorkingArea* working_area) override {
        auto begin = uncompressed_data.data();
        auto end = uncompressed_data.data() + uncompressed_data.size();
        if (std::search(begin, end, kDoNotCompress.begin(),
                        kDoNotCompress.end()) != end) {
          // Do not attempt compression
          EXPECT_EQ(*out_compression_type, kNoCompression);
          return Status::OK();
        } else if (std::search(begin, end, kRejectCompression.begin(),
                               kRejectCompression.end()) != end) {
          // Simulate attempted & rejected compression
          *compressed_output = "blah";
          EXPECT_EQ(*out_compression_type, kNoCompression);
          return Status::OK();
        } else {
          return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                         out_compression_type, working_area);
        }
      }
    };
    struct MyManager : public CompressionManagerWrapper {
      using CompressionManagerWrapper::CompressionManagerWrapper;
      const char* Name() const override { return wrapped_->Name(); }
      std::unique_ptr<Compressor> GetCompressorForSST(
          const FilterBuildingContext& context, const CompressionOptions& opts,
          CompressionType preferred) override {
        return std::make_unique<MyCompressor>(
            wrapped_->GetCompressorForSST(context, opts, preferred));
      }
    };
    auto mgr =
        std::make_shared<MyManager>(GetDefaultBuiltinCompressionManager());

    for (CompressionType type : GetSupportedCompressions()) {
      for (bool use_wrapper : {false, true}) {
        if (type == kNoCompression) {
          continue;
        }
        SCOPED_TRACE("Compression type: " + std::to_string(type) +
                     (use_wrapper ? " with " : " no ") + "wrapper");

        Options options = CurrentOptions();
        options.compression = type;
        options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
        options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
        BlockBasedTableOptions bbto;
        bbto.enable_index_compression = false;
        options.table_factory.reset(NewBlockBasedTableFactory(bbto));
        options.compression_manager = use_wrapper ? mgr : nullptr;
        DestroyAndReopen(options);

        auto PopStat = [&](Tickers t) -> uint64_t {
          return options.statistics->getAndResetTickerCount(t);
        };

        Random rnd(301);
        constexpr int kCount = 13;

        // Highly compressible blocks, except 1 non-compressible. Half of the
        // compressible are morked for bypass and 1 marked for rejection. Values
        // are large enough to ensure just 1 k-v per block.
        for (int i = 0; i < kCount; ++i) {
          std::string value;
          if (i == 6) {
            // One non-compressible block
            value = rnd.RandomBinaryString(20000);
          } else {
            test::CompressibleString(&rnd, 0.1, 20000, &value);
            if ((i % 2) == 0) {
              // Half for bypass
              value += kDoNotCompress;
            } else if (i == 7) {
              // One for rejection
              value += kRejectCompression;
            }
          }
          ASSERT_OK(Put(Key(i), value));
        }
        ASSERT_OK(Flush());

        if (use_wrapper) {
          EXPECT_EQ(kCount / 2 - 1, PopStat(NUMBER_BLOCK_COMPRESSED));
          EXPECT_EQ(kCount / 2, PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED));
          EXPECT_EQ(1 + 1, PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED));
        } else {
          EXPECT_EQ(kCount - 1, PopStat(NUMBER_BLOCK_COMPRESSED));
          EXPECT_EQ(0, PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED));
          EXPECT_EQ(1, PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED));
        }

        // Ensure well-formed for reads
        for (int i = 0; i < kCount; ++i) {
          ASSERT_NE(Get(Key(i)), "NOT_FOUND");
        }
        ASSERT_EQ(Get(Key(kCount)), "NOT_FOUND");
      }
    }
  }
}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
