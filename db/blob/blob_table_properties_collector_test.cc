//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_table_properties_collector.h"

#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_stats.h"
#include "db/blob/blob_stats_collection.h"
#include "db/dbformat.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class BlobTablePropertiesCollectorTest : public testing::Test {
 public:
  using BlobStatsVec = std::vector<std::pair<uint64_t, BlobStats>>;

  static Status Decode(const std::string& encoded, BlobStatsVec* decoded) {
    assert(decoded);

    Slice input(encoded);

    return BlobStatsCollection::DecodeFrom(
        &input,
        [decoded](uint64_t blob_file_number, uint64_t count, uint64_t bytes) {
          decoded->emplace_back(blob_file_number, BlobStats(count, bytes));
        });
  }
};

TEST_F(BlobTablePropertiesCollectorTest, InternalAddOneAndFinish) {
  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  InternalKey key(user_key, seq, kTypeBlobIndex);
  const Slice key_slice = key.Encode();

  constexpr uint64_t blob_file_number = 4;
  constexpr uint64_t offset = 5678;
  constexpr uint64_t size = 909090;
  constexpr CompressionType compression_type = kLZ4Compression;

  std::string value;
  BlobIndex::EncodeBlob(&value, blob_file_number, offset, size,
                        compression_type);

  const Slice value_slice(value);

  constexpr uint64_t file_size = 1000000;

  BlobTablePropertiesCollector collector;

  ASSERT_OK(collector.InternalAdd(key_slice, value_slice, file_size));

  UserCollectedProperties user_props;

  ASSERT_OK(collector.Finish(&user_props));

  auto it = user_props.find(TablePropertiesNames::kBlobFileMapping);
  ASSERT_NE(it, user_props.end());

  BlobStatsVec stats;
  ASSERT_OK(Decode(it->second, &stats));

  ASSERT_EQ(stats.size(), 1);
  ASSERT_EQ(stats[0].first, blob_file_number);
  ASSERT_EQ(stats[0].second.GetCount(), 1);
  ASSERT_EQ(stats[0].second.GetBytes(),
            size + BlobLogRecord::CalculateAdjustmentForRecordHeader(
                       sizeof(user_key) - 1));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
