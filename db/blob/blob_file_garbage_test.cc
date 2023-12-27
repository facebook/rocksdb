//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_garbage.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileGarbageTest : public testing::Test {
 public:
  static void TestEncodeDecode(const BlobFileGarbage& blob_file_garbage) {
    std::string encoded;
    blob_file_garbage.EncodeTo(&encoded);

    BlobFileGarbage decoded;
    Slice input(encoded);
    ASSERT_OK(decoded.DecodeFrom(&input));

    ASSERT_EQ(blob_file_garbage, decoded);
  }
};

TEST_F(BlobFileGarbageTest, Empty) {
  BlobFileGarbage blob_file_garbage;

  ASSERT_EQ(blob_file_garbage.GetBlobFileNumber(), kInvalidBlobFileNumber);
  ASSERT_EQ(blob_file_garbage.GetGarbageBlobCount(), 0);
  ASSERT_EQ(blob_file_garbage.GetGarbageBlobBytes(), 0);

  TestEncodeDecode(blob_file_garbage);
}

TEST_F(BlobFileGarbageTest, NonEmpty) {
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

TEST_F(BlobFileGarbageTest, DecodeErrors) {
  std::string str;
  Slice slice(str);

  BlobFileGarbage blob_file_garbage;

  {
    const Status s = blob_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "blob file number"));
  }

  constexpr uint64_t blob_file_number = 123;
  PutVarint64(&str, blob_file_number);
  slice = str;

  {
    const Status s = blob_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "garbage blob count"));
  }

  constexpr uint64_t garbage_blob_count = 4567;
  PutVarint64(&str, garbage_blob_count);
  slice = str;

  {
    const Status s = blob_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "garbage blob bytes"));
  }

  constexpr uint64_t garbage_blob_bytes = 12345678;
  PutVarint64(&str, garbage_blob_bytes);
  slice = str;

  {
    const Status s = blob_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field tag"));
  }

  constexpr uint32_t custom_tag = 2;
  PutVarint32(&str, custom_tag);
  slice = str;

  {
    const Status s = blob_file_garbage.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field value"));
  }
}

TEST_F(BlobFileGarbageTest, ForwardCompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileGarbage::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_compatible_tag = 2;
        PutVarint32(output, forward_compatible_tag);

        PutLengthPrefixedSlice(output, "deadbeef");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t blob_file_number = 678;
  constexpr uint64_t garbage_blob_count = 9999;
  constexpr uint64_t garbage_blob_bytes = 100000000;

  BlobFileGarbage blob_file_garbage(blob_file_number, garbage_blob_count,
                                    garbage_blob_bytes);

  TestEncodeDecode(blob_file_garbage);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileGarbageTest, ForwardIncompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileGarbage::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_incompatible_tag = (1 << 6) + 1;
        PutVarint32(output, forward_incompatible_tag);

        PutLengthPrefixedSlice(output, "foobar");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t blob_file_number = 456;
  constexpr uint64_t garbage_blob_count = 100;
  constexpr uint64_t garbage_blob_bytes = 2000000;

  BlobFileGarbage blob_file_garbage(blob_file_number, garbage_blob_count,
                                    garbage_blob_bytes);

  std::string encoded;
  blob_file_garbage.EncodeTo(&encoded);

  BlobFileGarbage decoded_blob_file_addition;
  Slice input(encoded);
  const Status s = decoded_blob_file_addition.DecodeFrom(&input);

  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Forward incompatible"));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
