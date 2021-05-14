//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_stats.h"

#include <cstring>
#include <string>

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
};

TEST_F(BlobStatsTest, EmptyRecord) {
  BlobStatsRecord record;

  ASSERT_EQ(record.GetBlobFileNumber(), kInvalidBlobFileNumber);
  ASSERT_EQ(record.GetCount(), 0);
  ASSERT_EQ(record.GetBytes(), 0);

  TestEncodeDecode(record);
}

TEST_F(BlobStatsTest, NonEmptyRecord) {
  constexpr uint64_t blob_file_number = 123;
  constexpr uint64_t count = 54;
  constexpr uint64_t bytes = 9876;

  BlobStatsRecord record(blob_file_number, count, bytes);

  ASSERT_EQ(record.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(record.GetCount(), count);
  ASSERT_EQ(record.GetBytes(), bytes);

  TestEncodeDecode(record);
}

TEST_F(BlobStatsTest, RecordDecodeErrors) {
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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
