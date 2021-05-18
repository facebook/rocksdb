//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_table_properties_collector.h"

#include <algorithm>
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
  using BlobStatsPair = std::pair<uint64_t, BlobStats>;
  using BlobStatsVec = std::vector<BlobStatsPair>;

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

TEST_F(BlobTablePropertiesCollectorTest, InternalAddMultipleAndFinish) {
  BlobTablePropertiesCollector collector;

  constexpr char user_key[] = "user_key";
  constexpr uint64_t format_overhead =
      BlobLogRecord::CalculateAdjustmentForRecordHeader(sizeof(user_key) - 1);
  constexpr uint64_t file_size = 1000000;

  // Add a total of four blob references, with two pointing to file #10 and two
  // to file #11
  for (uint64_t i = 0; i < 4; ++i) {
    const SequenceNumber seq = 100 + i;

    InternalKey key(user_key, seq, kTypeBlobIndex);
    const Slice key_slice = key.Encode();

    const uint64_t blob_file_number = 10 + i / 2;
    const uint64_t offset = (i + 1) * 1000;
    const uint64_t size = (i + 1) * 100;

    std::string value;
    BlobIndex::EncodeBlob(&value, blob_file_number, offset, size,
                          kNoCompression);

    const Slice value_slice(value);

    ASSERT_OK(collector.InternalAdd(key_slice, value_slice, file_size));
  }

  UserCollectedProperties user_props;

  ASSERT_OK(collector.Finish(&user_props));

  auto it = user_props.find(TablePropertiesNames::kBlobFileMapping);
  ASSERT_NE(it, user_props.end());

  BlobStatsVec stats;
  ASSERT_OK(Decode(it->second, &stats));

  std::sort(stats.begin(), stats.end(),
            [](const BlobStatsPair& lhs, const BlobStatsPair& rhs) {
              return lhs.first < rhs.first;
            });

  ASSERT_EQ(stats.size(), 2);
  ASSERT_EQ(stats[0].first, 10);
  ASSERT_EQ(stats[0].second.GetCount(), 2);
  ASSERT_EQ(stats[0].second.GetBytes(), 300 + 2 * format_overhead);
  ASSERT_EQ(stats[1].first, 11);
  ASSERT_EQ(stats[1].second.GetCount(), 2);
  ASSERT_EQ(stats[1].second.GetBytes(), 700 + 2 * format_overhead);
}

TEST_F(BlobTablePropertiesCollectorTest, InternalAddPlainValueAndFinish) {
  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  InternalKey key(user_key, seq, kTypeValue);
  const Slice key_slice = key.Encode();

  constexpr char value[] = "value";
  const Slice value_slice(value);

  constexpr uint64_t file_size = 1000000;

  BlobTablePropertiesCollector collector;

  ASSERT_OK(collector.InternalAdd(key_slice, value_slice, file_size));

  UserCollectedProperties user_props;

  ASSERT_OK(collector.Finish(&user_props));

  auto it = user_props.find(TablePropertiesNames::kBlobFileMapping);
  ASSERT_EQ(it, user_props.end());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
