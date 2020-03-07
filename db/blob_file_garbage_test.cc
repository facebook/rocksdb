//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob_file_garbage.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"

#include <cstdint>
#include <cstring>
#include <string>

namespace ROCKSDB_NAMESPACE {

class BlobFileStateTest : public testing::Test {
 public:
  template <class T>
  static void TestEncodeDecode(const T& t) {
    std::string encoded;
    t.EncodeTo(&encoded);

    T decoded;
    Slice input(encoded);
    ASSERT_OK(decoded.DecodeFrom(&input));

    ASSERT_EQ(t, decoded);
  }
};

TEST_F(BlobFileStateTest, Empty) {
  BlobFileGarbage blob_file_garbage;

  ASSERT_EQ(blob_file_garbage.GetBlobFileNumber(), kInvalidBlobFileNumber);
  ASSERT_EQ(blob_file_garbage.GetGarbageBlobCount(), 0);
  ASSERT_EQ(blob_file_garbage.GetGarbageBlobBytes(), 0);

  TestEncodeDecode(blob_file_garbage);
}

TEST_F(BlobFileStateTest, NonEmpty) {
  constexpr uint64_t blob_file_number = 123;
  constexpr uint64_t garbage_blob_count = 1;
  constexpr uint64_t garbage_blob_bytes = 9876;

  BlobFileGarbage blob_file_garbage(blob_file_number, garbage_blob_count,
                                    garbage_blob_bytes);

  ASSERT_EQ(blob_file_garbage.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_garbage.GetGarbageBlobCount(), garbage_blob_count);
  ASSERT_EQ(blob_file_garbage.GetGarbageBlobBytes(), garbage_blob_bytes);

  TestEncodeDecode(blob_file_garbage);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
