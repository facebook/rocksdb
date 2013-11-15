//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

#include "util/testharness.h"

namespace rocksdb {

class Coding { };

TEST(Coding, Fixed32) {
  std::string s;
  for (uint32_t v = 0; v < 100000; v++) {
    PutFixed32(&s, v);
  }

  const char* p = s.data();
  for (uint32_t v = 0; v < 100000; v++) {
    uint32_t actual = DecodeFixed32(p);
    ASSERT_EQ(v, actual);
    p += sizeof(uint32_t);
  }
}

TEST(Coding, Fixed64) {
  std::string s;
  for (int power = 0; power <= 63; power++) {
    uint64_t v = static_cast<uint64_t>(1) << power;
    PutFixed64(&s, v - 1);
    PutFixed64(&s, v + 0);
    PutFixed64(&s, v + 1);
  }

  const char* p = s.data();
  for (int power = 0; power <= 63; power++) {
    uint64_t v = static_cast<uint64_t>(1) << power;
    uint64_t actual;
    actual = DecodeFixed64(p);
    ASSERT_EQ(v-1, actual);
    p += sizeof(uint64_t);

    actual = DecodeFixed64(p);
    ASSERT_EQ(v+0, actual);
    p += sizeof(uint64_t);

    actual = DecodeFixed64(p);
    ASSERT_EQ(v+1, actual);
    p += sizeof(uint64_t);
  }
}

// Test that encoding routines generate little-endian encodings
TEST(Coding, EncodingOutput) {
  std::string dst;
  PutFixed32(&dst, 0x04030201);
  ASSERT_EQ(4U, dst.size());
  ASSERT_EQ(0x01, static_cast<int>(dst[0]));
  ASSERT_EQ(0x02, static_cast<int>(dst[1]));
  ASSERT_EQ(0x03, static_cast<int>(dst[2]));
  ASSERT_EQ(0x04, static_cast<int>(dst[3]));

  dst.clear();
  PutFixed64(&dst, 0x0807060504030201ull);
  ASSERT_EQ(8U, dst.size());
  ASSERT_EQ(0x01, static_cast<int>(dst[0]));
  ASSERT_EQ(0x02, static_cast<int>(dst[1]));
  ASSERT_EQ(0x03, static_cast<int>(dst[2]));
  ASSERT_EQ(0x04, static_cast<int>(dst[3]));
  ASSERT_EQ(0x05, static_cast<int>(dst[4]));
  ASSERT_EQ(0x06, static_cast<int>(dst[5]));
  ASSERT_EQ(0x07, static_cast<int>(dst[6]));
  ASSERT_EQ(0x08, static_cast<int>(dst[7]));
}

TEST(Coding, Varint32) {
  std::string s;
  for (uint32_t i = 0; i < (32 * 32); i++) {
    uint32_t v = (i / 32) << (i % 32);
    PutVarint32(&s, v);
  }

  const char* p = s.data();
  const char* limit = p + s.size();
  for (uint32_t i = 0; i < (32 * 32); i++) {
    uint32_t expected = (i / 32) << (i % 32);
    uint32_t actual;
    const char* start = p;
    p = GetVarint32Ptr(p, limit, &actual);
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(VarintLength(actual), p - start);
  }
  ASSERT_EQ(p, s.data() + s.size());
}

TEST(Coding, Varint64) {
  // Construct the list of values to check
  std::vector<uint64_t> values;
  // Some special values
  values.push_back(0);
  values.push_back(100);
  values.push_back(~static_cast<uint64_t>(0));
  values.push_back(~static_cast<uint64_t>(0) - 1);
  for (uint32_t k = 0; k < 64; k++) {
    // Test values near powers of two
    const uint64_t power = 1ull << k;
    values.push_back(power);
    values.push_back(power-1);
    values.push_back(power+1);
  };

  std::string s;
  for (unsigned int i = 0; i < values.size(); i++) {
    PutVarint64(&s, values[i]);
  }

  const char* p = s.data();
  const char* limit = p + s.size();
  for (unsigned int i = 0; i < values.size(); i++) {
    ASSERT_TRUE(p < limit);
    uint64_t actual;
    const char* start = p;
    p = GetVarint64Ptr(p, limit, &actual);
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(values[i], actual);
    ASSERT_EQ(VarintLength(actual), p - start);
  }
  ASSERT_EQ(p, limit);

}

TEST(Coding, Varint32Overflow) {
  uint32_t result;
  std::string input("\x81\x82\x83\x84\x85\x11");
  ASSERT_TRUE(GetVarint32Ptr(input.data(), input.data() + input.size(), &result)
              == nullptr);
}

TEST(Coding, Varint32Truncation) {
  uint32_t large_value = (1u << 31) + 100;
  std::string s;
  PutVarint32(&s, large_value);
  uint32_t result;
  for (unsigned int len = 0; len < s.size() - 1; len++) {
    ASSERT_TRUE(GetVarint32Ptr(s.data(), s.data() + len, &result) == nullptr);
  }
  ASSERT_TRUE(
      GetVarint32Ptr(s.data(), s.data() + s.size(), &result) != nullptr);
  ASSERT_EQ(large_value, result);
}

TEST(Coding, Varint64Overflow) {
  uint64_t result;
  std::string input("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11");
  ASSERT_TRUE(GetVarint64Ptr(input.data(), input.data() + input.size(), &result)
              == nullptr);
}

TEST(Coding, Varint64Truncation) {
  uint64_t large_value = (1ull << 63) + 100ull;
  std::string s;
  PutVarint64(&s, large_value);
  uint64_t result;
  for (unsigned int len = 0; len < s.size() - 1; len++) {
    ASSERT_TRUE(GetVarint64Ptr(s.data(), s.data() + len, &result) == nullptr);
  }
  ASSERT_TRUE(
      GetVarint64Ptr(s.data(), s.data() + s.size(), &result) != nullptr);
  ASSERT_EQ(large_value, result);
}

TEST(Coding, Strings) {
  std::string s;
  PutLengthPrefixedSlice(&s, Slice(""));
  PutLengthPrefixedSlice(&s, Slice("foo"));
  PutLengthPrefixedSlice(&s, Slice("bar"));
  PutLengthPrefixedSlice(&s, Slice(std::string(200, 'x')));

  Slice input(s);
  Slice v;
  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &v));
  ASSERT_EQ("", v.ToString());
  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &v));
  ASSERT_EQ("foo", v.ToString());
  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &v));
  ASSERT_EQ("bar", v.ToString());
  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &v));
  ASSERT_EQ(std::string(200, 'x'), v.ToString());
  ASSERT_EQ("", input.ToString());
}

TEST(Coding, BitStream) {
  const int kNumBytes = 10;
  char bytes[kNumBytes+1];
  for (int i = 0; i < kNumBytes + 1; ++i) {
      bytes[i] = '\0';
  }

  // Simple byte aligned test.
  for (int i = 0; i < kNumBytes; ++i) {
    BitStreamPutInt(bytes, kNumBytes, i*8, 8, 255-i);

    ASSERT_EQ((unsigned char)bytes[i], (unsigned char)(255-i));
  }
  for (int i = 0; i < kNumBytes; ++i) {
    ASSERT_EQ(BitStreamGetInt(bytes, kNumBytes, i*8, 8), (uint32_t)(255-i));
  }
  ASSERT_EQ(bytes[kNumBytes], '\0');

  // Write and read back at strange offsets
  for (int i = 0; i < kNumBytes + 1; ++i) {
      bytes[i] = '\0';
  }
  for (int i = 0; i < kNumBytes; ++i) {
    BitStreamPutInt(bytes, kNumBytes, i*5+1, 4, (i * 7) % (1 << 4));
  }
  for (int i = 0; i < kNumBytes; ++i) {
    ASSERT_EQ(BitStreamGetInt(bytes, kNumBytes, i*5+1, 4),
              (uint32_t)((i * 7) % (1 << 4)));
  }
  ASSERT_EQ(bytes[kNumBytes], '\0');

  // Create 11011011 as a bit pattern
  for (int i = 0; i < kNumBytes + 1; ++i) {
      bytes[i] = '\0';
  }
  for (int i = 0; i < kNumBytes; ++i) {
    BitStreamPutInt(bytes, kNumBytes, i*8, 2, 3);
    BitStreamPutInt(bytes, kNumBytes, i*8+3, 2, 3);
    BitStreamPutInt(bytes, kNumBytes, i*8+6, 2, 3);

    ASSERT_EQ((unsigned char)bytes[i],
              (unsigned char)(3 + (3 << 3) + (3 << 6)));
  }
  ASSERT_EQ(bytes[kNumBytes], '\0');


  // Test large values
  for (int i = 0; i < kNumBytes + 1; ++i) {
      bytes[i] = '\0';
  }
  BitStreamPutInt(bytes, kNumBytes, 0, 64, (uint64_t)(-1));
  for (int i = 0; i < 64/8; ++i) {
    ASSERT_EQ((unsigned char)bytes[i],
              (unsigned char)(255));
  }
  ASSERT_EQ(bytes[64/8], '\0');


}

TEST(Coding, BitStreamConvenienceFuncs) {
  std::string bytes(1, '\0');

  // Check that independent changes to byte are preserved.
  BitStreamPutInt(&bytes, 0, 2, 3);
  BitStreamPutInt(&bytes, 3, 2, 3);
  BitStreamPutInt(&bytes, 6, 2, 3);
  ASSERT_EQ((unsigned char)bytes[0], (unsigned char)(3 + (3 << 3) + (3 << 6)));
  ASSERT_EQ(BitStreamGetInt(&bytes, 0, 2), 3u);
  ASSERT_EQ(BitStreamGetInt(&bytes, 3, 2), 3u);
  ASSERT_EQ(BitStreamGetInt(&bytes, 6, 2), 3u);
  Slice slice(bytes);
  ASSERT_EQ(BitStreamGetInt(&slice, 0, 2), 3u);
  ASSERT_EQ(BitStreamGetInt(&slice, 3, 2), 3u);
  ASSERT_EQ(BitStreamGetInt(&slice, 6, 2), 3u);

  // Test overlapping crossing over byte boundaries
  bytes = std::string(2, '\0');
  BitStreamPutInt(&bytes, 6, 4, 15);
  ASSERT_EQ((unsigned char)bytes[0], 3 << 6);
  ASSERT_EQ((unsigned char)bytes[1], 3);
  ASSERT_EQ(BitStreamGetInt(&bytes, 6, 4), 15u);
  slice = Slice(bytes);
  ASSERT_EQ(BitStreamGetInt(&slice, 6, 4), 15u);

  // Test 64-bit number
  bytes = std::string(64/8, '\0');
  BitStreamPutInt(&bytes, 0, 64, (uint64_t)(-1));
  ASSERT_EQ(BitStreamGetInt(&bytes, 0, 64), (uint64_t)(-1));
  slice = Slice(bytes);
  ASSERT_EQ(BitStreamGetInt(&slice, 0, 64), (uint64_t)(-1));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
