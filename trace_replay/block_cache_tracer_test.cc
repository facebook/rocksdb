//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/block_cache_tracer.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace rocksdb {

namespace {
const uint64_t kBlockSize = 1024;
const std::string kBlockKeyPrefix = "test-block-";
const uint32_t kCFId = 0;
const uint32_t kLevel = 1;
const uint64_t kSSTFDNumber = 100;
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
    assert(false);
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
      record.sst_fd_number = kSSTFDNumber + key_id;
      record.is_cache_hit = Boolean::kFalse;
      record.no_insert = Boolean::kFalse;
      // Provide these fields for all block types.
      // The writer should only write these fields for data blocks and the
      // caller is either GET or MGET.
      record.referenced_key = kRefKeyPrefix + std::to_string(key_id);
      record.is_referenced_key_exist_in_block = Boolean::kTrue;
      record.num_keys_in_block = kNumKeysInBlock;
      ASSERT_OK(writer->WriteBlockAccess(record));
    }
  }

  void VerifyAccess(BlockCacheTraceReader* reader, uint32_t from_key_id,
                    TraceType block_type, uint32_t nblocks) {
    assert(reader);
    for (uint32_t i = 0; i < nblocks; i++) {
      uint32_t key_id = from_key_id + i;
      BlockCacheTraceRecord record;
      ASSERT_OK(reader->ReadAccess(&record));
      ASSERT_EQ(block_type, record.block_type);
      ASSERT_EQ(kBlockSize + key_id, record.block_size);
      ASSERT_EQ(kBlockKeyPrefix + std::to_string(key_id), record.block_key);
      ASSERT_EQ(kCFId, record.cf_id);
      ASSERT_EQ(kDefaultColumnFamilyName, record.cf_name);
      ASSERT_EQ(GetCaller(key_id), record.caller);
      ASSERT_EQ(kLevel, record.level);
      ASSERT_EQ(kSSTFDNumber + key_id, record.sst_fd_number);
      ASSERT_EQ(Boolean::kFalse, record.is_cache_hit);
      ASSERT_EQ(Boolean::kFalse, record.no_insert);
      if (block_type == TraceType::kBlockTraceDataBlock &&
          (record.caller == BlockCacheLookupCaller::kUserGet ||
           record.caller == BlockCacheLookupCaller::kUserMGet)) {
        ASSERT_EQ(kRefKeyPrefix + std::to_string(key_id),
                  record.referenced_key);
        ASSERT_EQ(Boolean::kTrue, record.is_referenced_key_exist_in_block);
        ASSERT_EQ(kNumKeysInBlock, record.num_keys_in_block);
        continue;
      }
      ASSERT_EQ("", record.referenced_key);
      ASSERT_EQ(Boolean::kFalse, record.is_referenced_key_exist_in_block);
      ASSERT_EQ(0, record.num_keys_in_block);
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
    VerifyAccess(&reader, 0, TraceType::kBlockTraceUncompressionDictBlock, 10);
    VerifyAccess(&reader, 10, TraceType::kBlockTraceDataBlock, 10);
    VerifyAccess(&reader, 20, TraceType::kBlockTraceFilterBlock, 10);
    VerifyAccess(&reader, 30, TraceType::kBlockTraceIndexBlock, 10);
    VerifyAccess(&reader, 40, TraceType::kBlockTraceRangeDeletionBlock, 10);
    // Read one more record should report an error.
    BlockCacheTraceRecord record;
    ASSERT_NOK(reader.ReadAccess(&record));
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
