//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include "table/block_based/index_builder.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

static std::string IKey(const std::string& user_key, uint64_t seq,
                        ValueType vt) {
  std::string encoded;
  AppendInternalKey(&encoded, ParsedInternalKey(user_key, seq, vt));
  return encoded;
}

static std::string Shorten(const std::string& s, const std::string& l) {
  std::string scratch;
  return ShortenedIndexBuilder::FindShortestInternalKeySeparator(
             *BytewiseComparator(), s, l, &scratch)
      .ToString();
}

static std::string ShortSuccessor(const std::string& s) {
  std::string scratch;
  return ShortenedIndexBuilder::FindShortInternalKeySuccessor(
             *BytewiseComparator(), s, &scratch)
      .ToString();
}

static void TestKey(const std::string& key, uint64_t seq, ValueType vt) {
  std::string encoded = IKey(key, seq, vt);

  Slice in(encoded);
  ParsedInternalKey decoded("", 0, kTypeValue);

  ASSERT_OK(ParseInternalKey(in, &decoded, true /* log_err_key */));
  ASSERT_EQ(key, decoded.user_key.ToString());
  ASSERT_EQ(seq, decoded.sequence);
  ASSERT_EQ(vt, decoded.type);

  ASSERT_NOK(ParseInternalKey(Slice("bar"), &decoded, true /* log_err_key */));
}

class FormatTest : public testing::Test {};

TEST_F(FormatTest, InternalKey_EncodeDecode) {
  const char* keys[] = {"", "k", "hello", "longggggggggggggggggggggg"};
  const uint64_t seq[] = {1,
                          2,
                          3,
                          (1ull << 8) - 1,
                          1ull << 8,
                          (1ull << 8) + 1,
                          (1ull << 16) - 1,
                          1ull << 16,
                          (1ull << 16) + 1,
                          (1ull << 32) - 1,
                          1ull << 32,
                          (1ull << 32) + 1};
  for (unsigned int k = 0; k < sizeof(keys) / sizeof(keys[0]); k++) {
    for (unsigned int s = 0; s < sizeof(seq) / sizeof(seq[0]); s++) {
      TestKey(keys[k], seq[s], kTypeValue);
      TestKey("hello", 1, kTypeDeletion);
    }
  }
}

TEST_F(FormatTest, InternalKeyShortSeparator) {
  // When user keys are same
  ASSERT_EQ(IKey("foo", 100, kTypeValue),
            Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 99, kTypeValue)));
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 101, kTypeValue)));
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 100, kTypeValue)));
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 100, kTypeDeletion)));

  // When user keys are misordered
  ASSERT_EQ(IKey("foo", 100, kTypeValue),
            Shorten(IKey("foo", 100, kTypeValue), IKey("bar", 99, kTypeValue)));

  // When user keys are different, but correctly ordered
  ASSERT_EQ(
      IKey("g", kMaxSequenceNumber, kValueTypeForSeek),
      Shorten(IKey("foo", 100, kTypeValue), IKey("hello", 200, kTypeValue)));

  ASSERT_EQ(IKey("ABC2", kMaxSequenceNumber, kValueTypeForSeek),
            Shorten(IKey("ABC1AAAAA", 100, kTypeValue),
                    IKey("ABC2ABB", 200, kTypeValue)));

  ASSERT_EQ(IKey("AAA2", kMaxSequenceNumber, kValueTypeForSeek),
            Shorten(IKey("AAA1AAA", 100, kTypeValue),
                    IKey("AAA2AA", 200, kTypeValue)));

  ASSERT_EQ(
      IKey("AAA2", kMaxSequenceNumber, kValueTypeForSeek),
      Shorten(IKey("AAA1AAA", 100, kTypeValue), IKey("AAA4", 200, kTypeValue)));

  ASSERT_EQ(
      IKey("AAA1B", kMaxSequenceNumber, kValueTypeForSeek),
      Shorten(IKey("AAA1AAA", 100, kTypeValue), IKey("AAA2", 200, kTypeValue)));

  ASSERT_EQ(IKey("AAA2", kMaxSequenceNumber, kValueTypeForSeek),
            Shorten(IKey("AAA1AAA", 100, kTypeValue),
                    IKey("AAA2A", 200, kTypeValue)));

  ASSERT_EQ(
      IKey("AAA1", 100, kTypeValue),
      Shorten(IKey("AAA1", 100, kTypeValue), IKey("AAA2", 200, kTypeValue)));

  // When start user key is prefix of limit user key
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foobar", 200, kTypeValue)));

  // When limit user key is prefix of start user key
  ASSERT_EQ(
      IKey("foobar", 100, kTypeValue),
      Shorten(IKey("foobar", 100, kTypeValue), IKey("foo", 200, kTypeValue)));
}

TEST_F(FormatTest, InternalKeyShortestSuccessor) {
  ASSERT_EQ(IKey("g", kMaxSequenceNumber, kValueTypeForSeek),
            ShortSuccessor(IKey("foo", 100, kTypeValue)));
  ASSERT_EQ(IKey("\xff\xff", 100, kTypeValue),
            ShortSuccessor(IKey("\xff\xff", 100, kTypeValue)));
}

TEST_F(FormatTest, IterKeyOperation) {
  IterKey k;
  const char p[] = "abcdefghijklmnopqrstuvwxyz";
  const char q[] = "0123456789";

  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string(""));

  k.TrimAppend(0, p, 3);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abc"));

  k.TrimAppend(1, p, 3);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("aabc"));

  k.TrimAppend(0, p, 26);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz"));

  k.TrimAppend(26, q, 10);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz0123456789"));

  k.TrimAppend(36, q, 1);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz01234567890"));

  k.TrimAppend(26, q, 1);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz0"));

  // Size going up, memory allocation is triggered
  k.TrimAppend(27, p, 26);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz0"
                        "abcdefghijklmnopqrstuvwxyz"));
}

TEST_F(FormatTest, IterKeyWithTimestampOperation) {
  IterKey k;
  k.SetUserKey("");
  const char p[] = "abcdefghijklmnopqrstuvwxyz";
  const char q[] = "0123456789";

  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string(""));

  size_t ts_sz = 8;
  std::string min_timestamp(ts_sz, static_cast<unsigned char>(0));
  k.TrimAppendWithTimestamp(0, p, 3, ts_sz);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            "abc" + min_timestamp);

  k.TrimAppendWithTimestamp(1, p, 3, ts_sz);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            "aabc" + min_timestamp);

  k.TrimAppendWithTimestamp(0, p, 26, ts_sz);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            "abcdefghijklmnopqrstuvwxyz" + min_timestamp);

  k.TrimAppendWithTimestamp(26, q, 10, ts_sz);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            "abcdefghijklmnopqrstuvwxyz0123456789" + min_timestamp);

  k.TrimAppendWithTimestamp(36, q, 1, ts_sz);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            "abcdefghijklmnopqrstuvwxyz01234567890" + min_timestamp);

  k.TrimAppendWithTimestamp(26, q, 1, ts_sz);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            "abcdefghijklmnopqrstuvwxyz0" + min_timestamp);

  k.TrimAppendWithTimestamp(27, p, 26, ts_sz);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            "abcdefghijklmnopqrstuvwxyz0"
            "abcdefghijklmnopqrstuvwxyz" +
                min_timestamp);
  // IterKey holds an internal key, the last 8 bytes hold the key footer, the
  // timestamp is expected to be added before the key footer.
  std::string key_without_ts = "keywithoutts";
  k.SetInternalKey(key_without_ts + min_timestamp + "internal");

  ASSERT_EQ(std::string(k.GetInternalKey().data(), k.GetInternalKey().size()),
            key_without_ts + min_timestamp + "internal");
  k.TrimAppendWithTimestamp(0, p, 10, ts_sz);
  ASSERT_EQ(std::string(k.GetInternalKey().data(), k.GetInternalKey().size()),
            "ab" + min_timestamp + "cdefghij");

  k.TrimAppendWithTimestamp(1, p, 8, ts_sz);
  ASSERT_EQ(std::string(k.GetInternalKey().data(), k.GetInternalKey().size()),
            "a" + min_timestamp + "abcdefgh");

  k.TrimAppendWithTimestamp(9, p, 3, ts_sz);
  ASSERT_EQ(std::string(k.GetInternalKey().data(), k.GetInternalKey().size()),
            "aabc" + min_timestamp + "defghabc");

  k.TrimAppendWithTimestamp(10, q, 10, ts_sz);
  ASSERT_EQ(std::string(k.GetInternalKey().data(), k.GetInternalKey().size()),
            "aabcdefgha01" + min_timestamp + "23456789");

  k.TrimAppendWithTimestamp(20, q, 1, ts_sz);
  ASSERT_EQ(std::string(k.GetInternalKey().data(), k.GetInternalKey().size()),
            "aabcdefgha012" + min_timestamp + "34567890");

  k.TrimAppendWithTimestamp(21, p, 26, ts_sz);
  ASSERT_EQ(
      std::string(k.GetInternalKey().data(), k.GetInternalKey().size()),
      "aabcdefgha01234567890abcdefghijklmnopqr" + min_timestamp + "stuvwxyz");
}

TEST_F(FormatTest, UpdateInternalKey) {
  std::string user_key("abcdefghijklmnopqrstuvwxyz");
  uint64_t new_seq = 0x123456;
  ValueType new_val_type = kTypeDeletion;

  std::string ikey;
  AppendInternalKey(&ikey, ParsedInternalKey(user_key, 100U, kTypeValue));
  size_t ikey_size = ikey.size();
  UpdateInternalKey(&ikey, new_seq, new_val_type);
  ASSERT_EQ(ikey_size, ikey.size());

  Slice in(ikey);
  ParsedInternalKey decoded;
  ASSERT_OK(ParseInternalKey(in, &decoded, true /* log_err_key */));
  ASSERT_EQ(user_key, decoded.user_key.ToString());
  ASSERT_EQ(new_seq, decoded.sequence);
  ASSERT_EQ(new_val_type, decoded.type);
}

TEST_F(FormatTest, RangeTombstoneSerializeEndKey) {
  RangeTombstone t("a", "b", 2);
  InternalKey k("b", 3, kTypeValue);
  const InternalKeyComparator cmp(BytewiseComparator());
  ASSERT_LT(cmp.Compare(t.SerializeEndKey(), k), 0);
}

TEST_F(FormatTest, PadInternalKeyWithMinTimestamp) {
  std::string orig_user_key = "foo";
  std::string orig_internal_key = IKey(orig_user_key, 100, kTypeValue);
  size_t ts_sz = 8;

  std::string key_buf;
  PadInternalKeyWithMinTimestamp(&key_buf, orig_internal_key, ts_sz);
  ParsedInternalKey key_with_timestamp;
  Slice in(key_buf);
  ASSERT_OK(ParseInternalKey(in, &key_with_timestamp, true /*log_err_key*/));

  std::string min_timestamp(ts_sz, static_cast<unsigned char>(0));
  ASSERT_EQ(orig_user_key + min_timestamp, key_with_timestamp.user_key);
  ASSERT_EQ(100, key_with_timestamp.sequence);
  ASSERT_EQ(kTypeValue, key_with_timestamp.type);
}

TEST_F(FormatTest, StripTimestampFromInternalKey) {
  std::string orig_user_key = "foo";
  size_t ts_sz = 8;
  std::string timestamp(ts_sz, static_cast<unsigned char>(0));
  orig_user_key.append(timestamp.data(), timestamp.size());
  std::string orig_internal_key = IKey(orig_user_key, 100, kTypeValue);

  std::string key_buf;
  StripTimestampFromInternalKey(&key_buf, orig_internal_key, ts_sz);
  ParsedInternalKey key_without_timestamp;
  Slice in(key_buf);
  ASSERT_OK(ParseInternalKey(in, &key_without_timestamp, true /*log_err_key*/));

  ASSERT_EQ("foo", key_without_timestamp.user_key);
  ASSERT_EQ(100, key_without_timestamp.sequence);
  ASSERT_EQ(kTypeValue, key_without_timestamp.type);
}

TEST_F(FormatTest, ReplaceInternalKeyWithMinTimestamp) {
  std::string orig_user_key = "foo";
  size_t ts_sz = 8;
  orig_user_key.append(ts_sz, static_cast<unsigned char>(1));
  std::string orig_internal_key = IKey(orig_user_key, 100, kTypeValue);

  std::string key_buf;
  ReplaceInternalKeyWithMinTimestamp(&key_buf, orig_internal_key, ts_sz);
  ParsedInternalKey new_key;
  Slice in(key_buf);
  ASSERT_OK(ParseInternalKey(in, &new_key, true /*log_err_key*/));

  std::string min_timestamp(ts_sz, static_cast<unsigned char>(0));
  size_t ukey_diff_offset = new_key.user_key.difference_offset(orig_user_key);
  ASSERT_EQ(min_timestamp,
            Slice(new_key.user_key.data() + ukey_diff_offset, ts_sz));
  ASSERT_EQ(orig_user_key.size(), new_key.user_key.size());
  ASSERT_EQ(100, new_key.sequence);
  ASSERT_EQ(kTypeValue, new_key.type);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
