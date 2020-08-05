//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

static void TestEncodeDecode(const VersionEdit& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2);
  ASSERT_EQ(encoded, encoded2);
}

class VersionEditTest : public testing::Test {};

TEST_F(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;
  static const uint32_t kBig32Bit = 1ull << 30;

  VersionEdit edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(3, kBig + 300 + i, kBig32Bit + 400 + i, 0,
                 InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion),
                 kBig + 500 + i, kBig + 600 + i, false, kInvalidBlobFileNumber,
                 888, 678, "234", "crc32c");
    edit.DeleteFile(4, kBig + 700 + i);
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, EncodeDecodeNewFile4) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true, kInvalidBlobFileNumber,
               kUnknownOldestAncesterTime, kUnknownFileCreationTime,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName);
  edit.AddFile(4, 301, 3, 100, InternalKey("foo", kBig + 501, kTypeValue),
               InternalKey("zoo", kBig + 601, kTypeDeletion), kBig + 501,
               kBig + 601, false, kInvalidBlobFileNumber,
               kUnknownOldestAncesterTime, kUnknownFileCreationTime,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName);
  edit.AddFile(5, 302, 0, 100, InternalKey("foo", kBig + 502, kTypeValue),
               InternalKey("zoo", kBig + 602, kTypeDeletion), kBig + 502,
               kBig + 602, true, kInvalidBlobFileNumber, 666, 888,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName);
  edit.AddFile(5, 303, 0, 100, InternalKey("foo", kBig + 503, kTypeBlobIndex),
               InternalKey("zoo", kBig + 603, kTypeBlobIndex), kBig + 503,
               kBig + 603, true, 1001, kUnknownOldestAncesterTime,
               kUnknownFileCreationTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName);
  ;

  edit.DeleteFile(4, 700);

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);

  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto& new_files = parsed.GetNewFiles();
  ASSERT_TRUE(new_files[0].second.marked_for_compaction);
  ASSERT_TRUE(!new_files[1].second.marked_for_compaction);
  ASSERT_TRUE(new_files[2].second.marked_for_compaction);
  ASSERT_TRUE(new_files[3].second.marked_for_compaction);
  ASSERT_EQ(3u, new_files[0].second.fd.GetPathId());
  ASSERT_EQ(3u, new_files[1].second.fd.GetPathId());
  ASSERT_EQ(0u, new_files[2].second.fd.GetPathId());
  ASSERT_EQ(0u, new_files[3].second.fd.GetPathId());
  ASSERT_EQ(kInvalidBlobFileNumber,
            new_files[0].second.oldest_blob_file_number);
  ASSERT_EQ(kInvalidBlobFileNumber,
            new_files[1].second.oldest_blob_file_number);
  ASSERT_EQ(kInvalidBlobFileNumber,
            new_files[2].second.oldest_blob_file_number);
  ASSERT_EQ(1001, new_files[3].second.oldest_blob_file_number);
}

TEST_F(VersionEditTest, ForwardCompatibleNewFile4) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true, kInvalidBlobFileNumber,
               kUnknownOldestAncesterTime, kUnknownFileCreationTime,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName);
  edit.AddFile(4, 301, 3, 100, InternalKey("foo", kBig + 501, kTypeValue),
               InternalKey("zoo", kBig + 601, kTypeDeletion), kBig + 501,
               kBig + 601, false, kInvalidBlobFileNumber, 686, 868, "234",
               "crc32c");
  edit.DeleteFile(4, 700);

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);

  std::string encoded;

  // Call back function to add extra customized builds.
  bool first = true;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:NewFile4:CustomizeFields", [&](void* arg) {
        std::string* str = reinterpret_cast<std::string*>(arg);
        PutVarint32(str, 33);
        const std::string str1 = "random_string";
        PutLengthPrefixedSlice(str, str1);
        if (first) {
          first = false;
          PutVarint32(str, 22);
          const std::string str2 = "s";
          PutLengthPrefixedSlice(str, str2);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  edit.EncodeTo(&encoded);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(!first);
  auto& new_files = parsed.GetNewFiles();
  ASSERT_TRUE(new_files[0].second.marked_for_compaction);
  ASSERT_TRUE(!new_files[1].second.marked_for_compaction);
  ASSERT_EQ(3u, new_files[0].second.fd.GetPathId());
  ASSERT_EQ(3u, new_files[1].second.fd.GetPathId());
  ASSERT_EQ(1u, parsed.GetDeletedFiles().size());
}

TEST_F(VersionEditTest, NewFile4NotSupportedField) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true, kInvalidBlobFileNumber,
               kUnknownOldestAncesterTime, kUnknownFileCreationTime,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName);

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);

  std::string encoded;

  // Call back function to add extra customized builds.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:NewFile4:CustomizeFields", [&](void* arg) {
        std::string* str = reinterpret_cast<std::string*>(arg);
        const std::string str1 = "s";
        PutLengthPrefixedSlice(str, str1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  edit.EncodeTo(&encoded);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_NOK(s);
}

TEST_F(VersionEditTest, EncodeEmptyFile) {
  VersionEdit edit;
  edit.AddFile(0, 0, 0, 0, InternalKey(), InternalKey(), 0, 0, false,
               kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
               kUnknownFileCreationTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName);
  std::string buffer;
  ASSERT_TRUE(!edit.EncodeTo(&buffer));
}

TEST_F(VersionEditTest, ColumnFamilyTest) {
  VersionEdit edit;
  edit.SetColumnFamily(2);
  edit.AddColumnFamily("column_family");
  edit.SetMaxColumnFamily(5);
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetColumnFamily(3);
  edit.DropColumnFamily();
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, MinLogNumberToKeep) {
  VersionEdit edit;
  edit.SetMinLogNumberToKeep(13);
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetMinLogNumberToKeep(23);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, AtomicGroupTest) {
  VersionEdit edit;
  edit.MarkAtomicGroup(1);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, IgnorableField) {
  VersionEdit ve;
  std::string encoded;

  // Size of ignorable field is too large
  PutVarint32Varint64(&encoded, 2 /* kLogNumber */, 66);
  // This is a customized ignorable tag
  PutVarint32Varint64(&encoded,
                      0x2710 /* A field with kTagSafeIgnoreMask set */,
                      5 /* fieldlength 5 */);
  encoded += "abc";  // Only fills 3 bytes,
  ASSERT_NOK(ve.DecodeFrom(encoded));

  encoded.clear();
  // Error when seeing unidentified tag that is not ignorable
  PutVarint32Varint64(&encoded, 2 /* kLogNumber */, 66);
  // This is a customized ignorable tag
  PutVarint32Varint64(&encoded, 666 /* A field with kTagSafeIgnoreMask unset */,
                      3 /* fieldlength 3 */);
  encoded += "abc";  //  Fill 3 bytes
  PutVarint32Varint64(&encoded, 3 /* next file number */, 88);
  ASSERT_NOK(ve.DecodeFrom(encoded));

  // Safely ignore an identified but safely ignorable entry
  encoded.clear();
  PutVarint32Varint64(&encoded, 2 /* kLogNumber */, 66);
  // This is a customized ignorable tag
  PutVarint32Varint64(&encoded,
                      0x2710 /* A field with kTagSafeIgnoreMask set */,
                      3 /* fieldlength 3 */);
  encoded += "abc";  //  Fill 3 bytes
  PutVarint32Varint64(&encoded, 3 /* kNextFileNumber */, 88);

  ASSERT_OK(ve.DecodeFrom(encoded));

  ASSERT_TRUE(ve.HasLogNumber());
  ASSERT_TRUE(ve.HasNextFile());
  ASSERT_EQ(66, ve.GetLogNumber());
  ASSERT_EQ(88, ve.GetNextFile());
}

TEST_F(VersionEditTest, DbId) {
  VersionEdit edit;
  edit.SetDBId("ab34-cd12-435f-er00");
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetDBId("34ba-cd12-435f-er01");
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, BlobFileAdditionAndGarbage) {
  VersionEdit edit;

  const std::string checksum_method_prefix = "Hash";
  const std::string checksum_value_prefix = "Value";

  for (uint64_t blob_file_number = 1; blob_file_number <= 10;
       ++blob_file_number) {
    const uint64_t total_blob_count = blob_file_number << 10;
    const uint64_t total_blob_bytes = blob_file_number << 20;

    std::string checksum_method(checksum_method_prefix);
    AppendNumberTo(&checksum_method, blob_file_number);

    std::string checksum_value(checksum_value_prefix);
    AppendNumberTo(&checksum_value, blob_file_number);

    edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                     checksum_method, checksum_value);

    const uint64_t garbage_blob_count = total_blob_count >> 2;
    const uint64_t garbage_blob_bytes = total_blob_bytes >> 1;

    edit.AddBlobFileGarbage(blob_file_number, garbage_blob_count,
                            garbage_blob_bytes);
  }

  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, AddWalEncodeDecode) {
  VersionEdit edit;
  for (uint64_t log_number = 1; log_number <= 20; log_number++) {
    WalMetadata meta(rand() % 100);
    bool has_size = rand() % 2 == 0;
    if (has_size) {
      meta.SetSizeInBytes(rand() % 1000);
    }
    edit.AddWal(log_number, meta);
  }
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, AddWalDecodeBadLogNumber) {
  std::string encoded;
  PutVarint32(&encoded, Tag::kWalAddition);

  {
    // No log number.
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding WAL log number") !=
                std::string::npos)
        << s.ToString();
  }

  {
    // log number should be varint64,
    // but we only encode 128 which is not a valid representation of varint64.
    char c = 0;
    unsigned char* ptr = reinterpret_cast<unsigned char*>(&c);
    *ptr = 128;
    encoded.append(1, c);
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding WAL log number") !=
                std::string::npos)
        << s.ToString();
  }
}

TEST_F(VersionEditTest, AddWalDecodeBadTag) {
  constexpr WalNumber kLogNumber = 100;
  constexpr uint64_t kSizeInBytes = 100;

  std::string encoded_without_tag;
  PutVarint32(&encoded_without_tag, Tag::kWalAddition);
  PutVarint64(&encoded_without_tag, kLogNumber);

  {
    // No tag.
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded_without_tag);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding tag") != std::string::npos)
        << s.ToString();
  }

  {
    // Only has size tag, no terminate tag.
    std::string encoded_with_size = encoded_without_tag;
    PutVarint32(&encoded_with_size,
                static_cast<uint32_t>(WalAdditionTag::kSize));
    PutVarint64(&encoded_with_size, kSizeInBytes);
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded_with_size);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding tag") != std::string::npos)
        << s.ToString();
  }

  {
    // Only has terminate tag.
    std::string encoded_with_terminate = encoded_without_tag;
    PutVarint32(&encoded_with_terminate,
                static_cast<uint32_t>(WalAdditionTag::kTerminate));
    VersionEdit edit;
    ASSERT_OK(edit.DecodeFrom(encoded_with_terminate));
    auto& wal_addition = edit.GetWalAdditions()[0];
    ASSERT_EQ(wal_addition.GetLogNumber(), kLogNumber);
    ASSERT_FALSE(wal_addition.GetMetadata().HasSize());
  }
}

TEST_F(VersionEditTest, AddWalDecodeNoSize) {
  constexpr WalNumber kLogNumber = 100;

  std::string encoded;
  PutVarint32(&encoded, Tag::kWalAddition);
  PutVarint64(&encoded, kLogNumber);
  PutVarint32(&encoded, static_cast<uint32_t>(WalAdditionTag::kSize));
  // No real size after the size tag.

  {
    // Without terminate tag.
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding WAL file size") !=
                std::string::npos)
        << s.ToString();
  }

  {
    // With terminate tag.
    PutVarint32(&encoded, static_cast<uint32_t>(WalAdditionTag::kTerminate));
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded);
    ASSERT_TRUE(s.IsCorruption());
    // The terminate tag is misunderstood as the size.
    ASSERT_TRUE(s.ToString().find("Error decoding tag") != std::string::npos)
        << s.ToString();
  }
}

TEST_F(VersionEditTest, AddWalDebug) {
  constexpr int n = 2;
  constexpr std::array<uint64_t, n> kLogNumbers{{10, 20}};
  constexpr std::array<uint64_t, n> kSizeInBytes{{100, 200}};

  VersionEdit edit;
  for (int i = 0; i < n; i++) {
    edit.AddWal(kLogNumbers[i], WalMetadata(kSizeInBytes[i]));
  }

  const WalAdditions& wals = edit.GetWalAdditions();

  ASSERT_TRUE(edit.HasWalAddition());
  ASSERT_EQ(wals.size(), n);
  for (int i = 0; i < n; i++) {
    const WalAddition& wal = wals[i];
    ASSERT_EQ(wal.GetLogNumber(), kLogNumbers[i]);
    ASSERT_EQ(wal.GetMetadata().GetSizeInBytes(), kSizeInBytes[i]);
  }

  std::string expected_str = "VersionEdit {\n";
  for (int i = 0; i < n; i++) {
    std::stringstream ss;
    ss << "  WalAddition: log_number: " << kLogNumbers[i]
       << " size_in_bytes: " << kSizeInBytes[i] << "\n";
    expected_str += ss.str();
  }
  expected_str += "  ColumnFamily: 0\n}\n";
  ASSERT_EQ(edit.DebugString(true), expected_str);

  std::string expected_json = "{\"EditNumber\": 4, \"WalAdditions\": [";
  for (int i = 0; i < n; i++) {
    std::stringstream ss;
    ss << "{\"LogNumber\": " << kLogNumbers[i] << ", "
       << "\"SizeInBytes\": " << kSizeInBytes[i] << "}";
    if (i < n - 1) ss << ", ";
    expected_json += ss.str();
  }
  expected_json += "], \"ColumnFamily\": 0}";
  ASSERT_EQ(edit.DebugJSON(4, true), expected_json);
}

TEST_F(VersionEditTest, DeleteWalEncodeDecode) {
  VersionEdit edit;
  for (uint64_t log_number = 1; log_number <= 20; log_number++) {
    edit.DeleteWal(log_number);
  }
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, DeleteWalDebug) {
  constexpr int n = 2;
  constexpr std::array<uint64_t, n> kLogNumbers{{10, 20}};

  VersionEdit edit;
  for (int i = 0; i < n; i++) {
    edit.DeleteWal(kLogNumbers[i]);
  }

  const WalDeletions& wals = edit.GetWalDeletions();

  ASSERT_TRUE(edit.HasWalDeletion());
  ASSERT_EQ(wals.size(), n);
  for (int i = 0; i < n; i++) {
    const WalDeletion& wal = wals[i];
    ASSERT_EQ(wal.GetLogNumber(), kLogNumbers[i]);
  }

  std::string expected_str = "VersionEdit {\n";
  for (int i = 0; i < n; i++) {
    std::stringstream ss;
    ss << "  WalDeletion: log_number: " << kLogNumbers[i] << "\n";
    expected_str += ss.str();
  }
  expected_str += "  ColumnFamily: 0\n}\n";
  ASSERT_EQ(edit.DebugString(true), expected_str);

  std::string expected_json = "{\"EditNumber\": 4, \"WalDeletions\": [";
  for (int i = 0; i < n; i++) {
    std::stringstream ss;
    ss << "{\"LogNumber\": " << kLogNumbers[i] << "}";
    if (i < n - 1) ss << ", ";
    expected_json += ss.str();
  }
  expected_json += "], \"ColumnFamily\": 0}";
  ASSERT_EQ(edit.DebugJSON(4, true), expected_json);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
