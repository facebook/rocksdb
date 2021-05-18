//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_stats.h"

#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_stats_collection.h"
#include "db/blob/blob_stats_record.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class BlobStatsTest : public testing::Test {
 public:
  static void TestEncodeDecode(const BlobStatsRecord& record) {
    std::string encoded;
    record.EncodeTo(&encoded);

    BlobStatsRecord decoded;
    Slice input(encoded);
    ASSERT_OK(decoded.DecodeFrom(&input));

    ASSERT_EQ(record.GetBlobFileNumber(), decoded.GetBlobFileNumber());
    ASSERT_EQ(record.GetCount(), decoded.GetCount());
    ASSERT_EQ(record.GetBytes(), decoded.GetBytes());
  }

  using BlobStatsVec = std::vector<std::pair<uint64_t, BlobStats>>;

  static void Encode(const BlobStatsVec& blob_stats_vec, std::string* encoded) {
    assert(encoded);

    BlobStatsCollection::EncodeTo(blob_stats_vec, encoded);
  }

  static Status Decode(const std::string& encoded, BlobStatsVec* decoded) {
    assert(decoded);

    Slice input(encoded);

    return BlobStatsCollection::DecodeFrom(
        &input,
        [decoded](uint64_t blob_file_number, uint64_t count, uint64_t bytes) {
          decoded->emplace_back(blob_file_number, BlobStats(count, bytes));
        });
  }

  static void TestEncodeDecode(const BlobStatsVec& blob_stats_vec) {
    std::string encoded;
    Encode(blob_stats_vec, &encoded);

    BlobStatsVec decoded;
    ASSERT_OK(Decode(encoded, &decoded));

    ASSERT_EQ(blob_stats_vec.size(), decoded.size());

    for (BlobStatsVec::size_type i = 0; i < blob_stats_vec.size(); ++i) {
      ASSERT_EQ(blob_stats_vec[i].first, decoded[i].first);
      ASSERT_EQ(blob_stats_vec[i].second.GetCount(),
                decoded[i].second.GetCount());
      ASSERT_EQ(blob_stats_vec[i].second.GetBytes(),
                decoded[i].second.GetBytes());
    }
  }
};

TEST_F(BlobStatsTest, EmptyRecord) {
  const BlobStatsRecord record;

  ASSERT_EQ(record.GetBlobFileNumber(), kInvalidBlobFileNumber);
  ASSERT_EQ(record.GetCount(), 0);
  ASSERT_EQ(record.GetBytes(), 0);

  TestEncodeDecode(record);
}

TEST_F(BlobStatsTest, NonEmptyRecord) {
  constexpr uint64_t blob_file_number = 123;
  constexpr uint64_t count = 54;
  constexpr uint64_t bytes = 9876;

  const BlobStatsRecord record(blob_file_number, count, bytes);

  ASSERT_EQ(record.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(record.GetCount(), count);
  ASSERT_EQ(record.GetBytes(), bytes);

  TestEncodeDecode(record);
}

TEST_F(BlobStatsTest, RecordDecodingErrors) {
  std::string str;
  Slice slice(str);

  BlobStatsRecord record;

  {
    const Status s = record.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "blob file number"));
  }

  constexpr uint64_t blob_file_number = 123;
  PutVarint64(&str, blob_file_number);
  slice = str;

  {
    const Status s = record.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "blob count"));
  }

  constexpr uint64_t blob_count = 4567;
  PutVarint64(&str, blob_count);
  slice = str;

  {
    const Status s = record.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "blob bytes"));
  }
}

TEST_F(BlobStatsTest, EmptyCollection) {
  const BlobStatsVec blob_stats_vec;

  TestEncodeDecode(blob_stats_vec);
}

TEST_F(BlobStatsTest, NonEmptyCollection) {
  const BlobStatsVec blob_stats_vec{{1, BlobStats(1111, 2222)},
                                    {23, BlobStats(4545, 676767)},
                                    {456, BlobStats(888888, 98765432)}};

  TestEncodeDecode(blob_stats_vec);
}

TEST_F(BlobStatsTest, CollectionDecodingErrors) {
  std::string str;
  BlobStatsVec blob_stats_vec;

  {
    const Status s = Decode(str, &blob_stats_vec);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "size"));
  }

  constexpr uint64_t size = 100;
  PutVarint64(&str, size);

  {
    const Status s = Decode(str, &blob_stats_vec);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "blob file number"));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
