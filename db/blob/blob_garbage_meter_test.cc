//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_garbage_meter.h"

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/dbformat.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

TEST(BlobGarbageMeterTest, OneBlobInOut) {
  BlobGarbageMeter blob_garbage_meter;

  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  const InternalKey key(user_key, seq, kTypeBlobIndex);
  const Slice key_slice = key.Encode();

  constexpr uint64_t blob_file_number = 4;
  constexpr uint64_t offset = 5678;
  constexpr uint64_t size = 909090;
  constexpr CompressionType compression_type = kLZ4Compression;

  std::string value;
  BlobIndex::EncodeBlob(&value, blob_file_number, offset, size,
                        compression_type);

  const Slice value_slice(value);

  ASSERT_OK(blob_garbage_meter.ProcessInFlow(key_slice, value_slice));
  ASSERT_OK(blob_garbage_meter.ProcessOutFlow(key_slice, value_slice));

  const auto& flows = blob_garbage_meter.flows();
  ASSERT_EQ(flows.size(), 1);

  const auto it = flows.begin();
  ASSERT_EQ(it->first, blob_file_number);

  const auto& flow = it->second;
  ASSERT_TRUE(flow.IsValid());
  ASSERT_FALSE(flow.HasGarbage());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
