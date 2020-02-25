//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob_file_state.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"

#include <cstdint>
#include <cstring>
#include <string>

namespace ROCKSDB_NAMESPACE {

class BlobFileStateTest : public testing::Test {
 public:
  static void TestEncodeDecode(const BlobFileState& blob_file_state) {
    std::string encoded;
    blob_file_state.EncodeTo(&encoded);

    BlobFileState decoded;
    Slice input(encoded);
    ASSERT_OK(decoded.DecodeFrom(&input));

    ASSERT_EQ(blob_file_state, decoded);
  }
};

TEST_F(BlobFileStateTest, Empty) {
  BlobFileState blob_file_state;

  ASSERT_EQ(blob_file_state.GetBlobFileNumber(), kInvalidBlobFileNumber);
  ASSERT_EQ(blob_file_state.GetTotalBlobCount(), 0);
  ASSERT_EQ(blob_file_state.GetTotalBlobBytes(), 0);
  ASSERT_EQ(blob_file_state.GetGarbageBlobCount(), 0);
  ASSERT_EQ(blob_file_state.GetGarbageBlobBytes(), 0);
  ASSERT_TRUE(blob_file_state.IsObsolete());
  ASSERT_TRUE(blob_file_state.GetChecksumMethod().empty());
  ASSERT_TRUE(blob_file_state.GetChecksumValue().empty());

  TestEncodeDecode(blob_file_state);
}

TEST_F(BlobFileStateTest, NonEmpty) {
  constexpr uint64_t blob_file_number = 123;
  constexpr uint64_t total_blob_count = 2;
  constexpr uint64_t total_blob_bytes = 123456;
  constexpr uint64_t garbage_blob_count = 1;
  constexpr uint64_t garbage_blob_bytes = 9876;
  const std::string checksum_method("SHA1");
  const std::string checksum_value("bdb7f34a59dfa1592ce7f52e99f98c570c525cbd");

  BlobFileState blob_file_state(
      blob_file_number, total_blob_count, total_blob_bytes, garbage_blob_count,
      garbage_blob_bytes, checksum_method, checksum_value);

  ASSERT_EQ(blob_file_state.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_state.GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(blob_file_state.GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(blob_file_state.GetGarbageBlobCount(), garbage_blob_count);
  ASSERT_EQ(blob_file_state.GetGarbageBlobBytes(), garbage_blob_bytes);
  ASSERT_FALSE(blob_file_state.IsObsolete());
  ASSERT_EQ(blob_file_state.GetChecksumMethod(), checksum_method);
  ASSERT_EQ(blob_file_state.GetChecksumValue(), checksum_value);

  TestEncodeDecode(blob_file_state);
}

TEST_F(BlobFileStateTest, AddGarbageBlob) {
  constexpr uint64_t blob_file_number = 123;
  constexpr uint64_t total_blob_count = 2;
  constexpr uint64_t total_blob_bytes = 123456;
  const std::string checksum_method("MD5");
  const std::string checksum_value("d8f72233c67a68c5ec2bd51c6be7556e");

  BlobFileState blob_file_state(blob_file_number, total_blob_count,
                                total_blob_bytes, checksum_method,
                                checksum_value);

  ASSERT_EQ(blob_file_state.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_state.GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(blob_file_state.GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(blob_file_state.GetGarbageBlobCount(), 0);
  ASSERT_EQ(blob_file_state.GetGarbageBlobBytes(), 0);
  ASSERT_FALSE(blob_file_state.IsObsolete());
  ASSERT_EQ(blob_file_state.GetChecksumMethod(), checksum_method);
  ASSERT_EQ(blob_file_state.GetChecksumValue(), checksum_value);

  TestEncodeDecode(blob_file_state);

  blob_file_state.AddGarbageBlob(123000);

  ASSERT_EQ(blob_file_state.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_state.GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(blob_file_state.GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(blob_file_state.GetGarbageBlobCount(), 1);
  ASSERT_EQ(blob_file_state.GetGarbageBlobBytes(), 123000);
  ASSERT_FALSE(blob_file_state.IsObsolete());
  ASSERT_EQ(blob_file_state.GetChecksumMethod(), checksum_method);
  ASSERT_EQ(blob_file_state.GetChecksumValue(), checksum_value);

  TestEncodeDecode(blob_file_state);

  blob_file_state.AddGarbageBlob(456);

  ASSERT_EQ(blob_file_state.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_state.GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(blob_file_state.GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(blob_file_state.GetGarbageBlobCount(), total_blob_count);
  ASSERT_EQ(blob_file_state.GetGarbageBlobBytes(), total_blob_bytes);
  ASSERT_TRUE(blob_file_state.IsObsolete());
  ASSERT_EQ(blob_file_state.GetChecksumMethod(), checksum_method);
  ASSERT_EQ(blob_file_state.GetChecksumValue(), checksum_value);

  TestEncodeDecode(blob_file_state);
}

TEST_F(BlobFileStateTest, DecodeErrors) {
  std::string str;
  Slice slice(str);

  BlobFileState blob_file_state;

  {
    const Status s = blob_file_state.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "blob file number"));
  }

  constexpr uint64_t blob_file_number = 123;
  PutVarint64(&str, blob_file_number);
  slice = str;

  {
    const Status s = blob_file_state.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "total blob count"));
  }

  constexpr uint64_t total_blob_count = 4567;
  PutVarint64(&str, total_blob_count);
  slice = str;

  {
    const Status s = blob_file_state.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "total blob bytes"));
  }

  constexpr uint64_t total_blob_bytes = 12345678;
  PutVarint64(&str, total_blob_bytes);
  slice = str;

  {
    const Status s = blob_file_state.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "garbage blob count"));
  }

  constexpr uint64_t garbage_blob_count = 1234;
  PutVarint64(&str, garbage_blob_count);
  slice = str;

  {
    const Status s = blob_file_state.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "garbage blob bytes"));
  }

  constexpr uint64_t garbage_blob_bytes = 5678;
  PutVarint64(&str, garbage_blob_bytes);
  slice = str;

  {
    const Status s = blob_file_state.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "checksum method"));
  }

  constexpr char checksum_method[] = "SHA1";
  PutLengthPrefixedSlice(&str, checksum_method);
  slice = str;

  {
    const Status s = blob_file_state.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "checksum value"));
  }

  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";
  PutLengthPrefixedSlice(&str, checksum_value);
  slice = str;

  {
    const Status s = blob_file_state.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field tag"));
  }

  constexpr uint32_t custom_tag = 2;
  PutVarint32(&str, custom_tag);
  slice = str;

  {
    const Status s = blob_file_state.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field value"));
  }
}

TEST_F(BlobFileStateTest, ForwardCompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileState::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_compatible_tag = 2;
        PutVarint32(output, forward_compatible_tag);

        PutLengthPrefixedSlice(output, "deadbeef");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t blob_file_number = 678;
  constexpr uint64_t total_blob_count = 9999;
  constexpr uint64_t total_blob_bytes = 100000000;
  constexpr uint64_t garbage_blob_count = 3333;
  constexpr uint64_t garbage_blob_bytes = 2500000;
  const std::string checksum_method("CRC32");
  const std::string checksum_value("3d87ff57");

  BlobFileState blob_file_state(
      blob_file_number, total_blob_count, total_blob_bytes, garbage_blob_count,
      garbage_blob_bytes, checksum_method, checksum_value);

  TestEncodeDecode(blob_file_state);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileStateTest, ForwardIncompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileState::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_incompatible_tag = (1 << 6) + 1;
        PutVarint32(output, forward_incompatible_tag);

        PutLengthPrefixedSlice(output, "foobar");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t blob_file_number = 456;
  constexpr uint64_t total_blob_count = 100;
  constexpr uint64_t total_blob_bytes = 2000000;
  const std::string checksum_method("CRC32B");
  const std::string checksum_value("6dbdf23a");

  BlobFileState blob_file_state(blob_file_number, total_blob_count,
                                total_blob_bytes, checksum_method,
                                checksum_value);

  std::string encoded;
  blob_file_state.EncodeTo(&encoded);

  BlobFileState decoded_blob_file_state;
  Slice input(encoded);
  const Status s = decoded_blob_file_state.DecodeFrom(&input);

  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Forward incompatible"));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
