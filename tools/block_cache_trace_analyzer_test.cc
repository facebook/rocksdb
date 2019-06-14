//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <map>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "tools/block_cache_trace_analyzer.h"
#include "trace_replay/block_cache_tracer.h"

namespace rocksdb {

namespace {
const uint64_t kBlockSize = 1024;
const std::string kBlockKeyPrefix = "test-block-";
const uint32_t kCFId = 0;
const uint32_t kLevel = 1;
const uint64_t kSSTStoringEvenKeys = 100;
const uint64_t kSSTStoringOddKeys = 101;
const std::string kRefKeyPrefix = "test-get-";
const uint64_t kNumKeysInBlock = 1024;
}  // namespace

class BlockCacheTracerTest : public testing::Test {
 public:
  BlockCacheTracerTest() {
    test_path_ = test::PerThreadDBPath("block_cache_tracer_test");
    env_ = rocksdb::Env::Default();
    EXPECT_OK(env_->CreateDir(test_path_));
    trace_file_path_ = test_path_ + "/block_cache_trace";
  }

  ~BlockCacheTracerTest() override {
    if (getenv("KEEP_DB")) {
      printf("The trace file is still at %s\n", trace_file_path_.c_str());
      return;
    }
    EXPECT_OK(env_->DeleteFile(trace_file_path_));
    EXPECT_OK(env_->DeleteDir(test_path_));
  }

  BlockCacheLookupCaller GetCaller(uint32_t key_id) {
    uint32_t n = key_id % 5;
    switch (n) {
      case 0:
        return BlockCacheLookupCaller::kPrefetch;
      case 1:
        return BlockCacheLookupCaller::kCompaction;
      case 2:
        return BlockCacheLookupCaller::kUserGet;
      case 3:
        return BlockCacheLookupCaller::kUserMGet;
      case 4:
        return BlockCacheLookupCaller::kUserIterator;
    }
    // This cannot happend.
    assert(false);
    return BlockCacheLookupCaller::kUserGet;
  }

  void WriteBlockAccess(BlockCacheTraceWriter* writer, uint32_t from_key_id,
                        TraceType block_type, uint32_t nblocks) {
    assert(writer);
    for (uint32_t i = 0; i < nblocks; i++) {
      uint32_t key_id = from_key_id + i;
      BlockCacheTraceRecord record;
      record.block_type = block_type;
      record.block_size = kBlockSize + key_id;
      record.block_key = kBlockKeyPrefix + std::to_string(key_id);
      record.access_timestamp = env_->NowMicros();
      record.cf_id = kCFId;
      record.cf_name = kDefaultColumnFamilyName;
      record.caller = GetCaller(key_id);
      record.level = kLevel;
      if (key_id % 2 == 0) {
        record.sst_fd_number = kSSTStoringEvenKeys;
      } else {
        record.sst_fd_number = kSSTStoringOddKeys;
      }
      record.is_cache_hit = Boolean::kFalse;
      record.no_insert = Boolean::kFalse;
      // Provide these fields for all block types.
      // The writer should only write these fields for data blocks and the
      // caller is either GET or MGET.
      record.referenced_key = kRefKeyPrefix + std::to_string(key_id);
      record.referenced_key_exist_in_block = Boolean::kTrue;
      record.num_keys_in_block = kNumKeysInBlock;
      ASSERT_OK(writer->WriteBlockAccess(
          record, record.block_key, record.cf_name, record.referenced_key));
    }
  }

  void AssertBlockAccessInfo(
      uint32_t key_id, TraceType type,
      const std::map<std::string, BlockAccessInfo>& block_access_info_map) {
    auto key_id_str = kBlockKeyPrefix + std::to_string(key_id);
    ASSERT_TRUE(block_access_info_map.find(key_id_str) !=
                block_access_info_map.end());
    auto& block_access_info = block_access_info_map.find(key_id_str)->second;
    ASSERT_EQ(1, block_access_info.num_accesses);
    ASSERT_EQ(kBlockSize + key_id, block_access_info.block_size);
    ASSERT_GT(block_access_info.first_access_time, 0);
    ASSERT_GT(block_access_info.last_access_time, 0);
    ASSERT_EQ(1, block_access_info.caller_num_access_map.size());
    BlockCacheLookupCaller expected_caller = GetCaller(key_id);
    ASSERT_TRUE(block_access_info.caller_num_access_map.find(expected_caller) !=
                block_access_info.caller_num_access_map.end());
    ASSERT_EQ(
        1,
        block_access_info.caller_num_access_map.find(expected_caller)->second);

    if ((expected_caller == BlockCacheLookupCaller::kUserGet ||
         expected_caller == BlockCacheLookupCaller::kUserMGet) &&
        type == TraceType::kBlockTraceDataBlock) {
      ASSERT_EQ(kNumKeysInBlock, block_access_info.num_keys);
      ASSERT_EQ(1, block_access_info.key_num_access_map.size());
      ASSERT_EQ(0, block_access_info.non_exist_key_num_access_map.size());
      ASSERT_EQ(1, block_access_info.num_referenced_key_exist_in_block);
    }
  }

  Env* env_;
  EnvOptions env_options_;
  std::string trace_file_path_;
  std::string test_path_;
};

TEST_F(BlockCacheTracerTest, MixedBlocks) {
  {
    // Generate a trace file containing a mix of blocks.
    // It contains two SST files with 25 blocks of odd numbered block_key in
    // kSSTStoringOddKeys and 25 blocks of even numbered blocks_key in
    // kSSTStoringEvenKeys.
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    BlockCacheTraceWriter writer(env_, trace_opt, std::move(trace_writer));
    ASSERT_OK(writer.WriteHeader());
    // Write blocks of different types.
    WriteBlockAccess(&writer, 0, TraceType::kBlockTraceUncompressionDictBlock,
                     10);
    WriteBlockAccess(&writer, 10, TraceType::kBlockTraceDataBlock, 10);
    WriteBlockAccess(&writer, 20, TraceType::kBlockTraceFilterBlock, 10);
    WriteBlockAccess(&writer, 30, TraceType::kBlockTraceIndexBlock, 10);
    WriteBlockAccess(&writer, 40, TraceType::kBlockTraceRangeDeletionBlock, 10);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }

  {
    // Verify trace file is generated correctly.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, header.rocksdb_major_version);
    ASSERT_EQ(kMinorVersion, header.rocksdb_minor_version);
    // Read blocks.
    BlockCacheTraceAnalyzer analyzer(trace_file_path_);
    // The analyzer ends when it detects an incomplete access record.
    ASSERT_EQ(Status::Incomplete(""), analyzer.Analyze());
    const uint64_t expected_num_cfs = 1;
    std::vector<uint64_t> expected_fds{kSSTStoringOddKeys, kSSTStoringEvenKeys};
    const std::vector<TraceType> expected_types{
        TraceType::kBlockTraceUncompressionDictBlock,
        TraceType::kBlockTraceDataBlock, TraceType::kBlockTraceFilterBlock,
        TraceType::kBlockTraceIndexBlock,
        TraceType::kBlockTraceRangeDeletionBlock};
    const uint64_t expected_num_keys_per_type = 5;

    auto& stats = analyzer.TEST_cf_aggregates_map();
    ASSERT_EQ(expected_num_cfs, stats.size());
    ASSERT_TRUE(stats.find(kDefaultColumnFamilyName) != stats.end());
    auto& cf_stats = stats.find(kDefaultColumnFamilyName)->second;
    ASSERT_EQ(expected_fds.size(), cf_stats.fd_aggregates_map.size());
    for (auto fd_id : expected_fds) {
      ASSERT_TRUE(cf_stats.fd_aggregates_map.find(fd_id) !=
                  cf_stats.fd_aggregates_map.end());
      ASSERT_EQ(kLevel, cf_stats.fd_aggregates_map.find(fd_id)->second.level);
      auto& block_type_aggregates_map = cf_stats.fd_aggregates_map.find(fd_id)
                                            ->second.block_type_aggregates_map;
      ASSERT_EQ(expected_types.size(), block_type_aggregates_map.size());
      uint32_t key_id = 0;
      for (auto type : expected_types) {
        ASSERT_TRUE(block_type_aggregates_map.find(type) !=
                    block_type_aggregates_map.end());
        auto& block_access_info_map =
            block_type_aggregates_map.find(type)->second.block_access_info_map;
        // Each block type has 5 blocks.
        ASSERT_EQ(expected_num_keys_per_type, block_access_info_map.size());
        for (uint32_t i = 0; i < 10; i++) {
          // Verify that odd numbered blocks are stored in kSSTStoringOddKeys
          // and even numbered blocks are stored in kSSTStoringEvenKeys.
          auto key_id_str = kBlockKeyPrefix + std::to_string(key_id);
          if (fd_id == kSSTStoringOddKeys) {
            if (key_id % 2 == 1) {
              AssertBlockAccessInfo(key_id, type, block_access_info_map);
            } else {
              ASSERT_TRUE(block_access_info_map.find(key_id_str) ==
                          block_access_info_map.end());
            }
          } else {
            if (key_id % 2 == 1) {
              ASSERT_TRUE(block_access_info_map.find(key_id_str) ==
                          block_access_info_map.end());
            } else {
              AssertBlockAccessInfo(key_id, type, block_access_info_map);
            }
          }
          key_id++;
        }
      }
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
