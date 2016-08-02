//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef ROCKSDB_LITE

#include <vector>
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/col_buf_decoder.h"
#include "utilities/col_buf_encoder.h"

namespace rocksdb {

class ColumnAwareEncodingTest : public testing::Test {
 public:
  ColumnAwareEncodingTest() {}

  ~ColumnAwareEncodingTest() {}
};

class ColumnAwareEncodingTestWithSize
    : public ColumnAwareEncodingTest,
      public testing::WithParamInterface<size_t> {
 public:
  ColumnAwareEncodingTestWithSize() {}

  ~ColumnAwareEncodingTestWithSize() {}

  static std::vector<size_t> GetValues() { return {4, 8}; }
};

INSTANTIATE_TEST_CASE_P(
    ColumnAwareEncodingTestWithSize, ColumnAwareEncodingTestWithSize,
    ::testing::ValuesIn(ColumnAwareEncodingTestWithSize::GetValues()));

TEST_P(ColumnAwareEncodingTestWithSize, NoCompressionEncodeDecode) {
  size_t col_size = GetParam();
  std::unique_ptr<ColBufEncoder> col_buf_encoder(
      new FixedLengthColBufEncoder(col_size, kColNoCompression, false, true));
  std::string str_buf;
  uint64_t base_val = 0x0102030405060708;
  uint64_t val = 0;
  memcpy(&val, &base_val, col_size);
  const int row_count = 4;
  for (int i = 0; i < row_count; ++i) {
    str_buf.append(reinterpret_cast<char*>(&val), col_size);
  }
  const char* str_buf_ptr = str_buf.c_str();
  for (int i = 0; i < row_count; ++i) {
    col_buf_encoder->Append(str_buf_ptr);
  }
  col_buf_encoder->Finish();
  const std::string& encoded_data = col_buf_encoder->GetData();
  // Check correctness of encoded string length
  ASSERT_EQ(row_count * col_size, encoded_data.size());

  const char* encoded_data_ptr = encoded_data.c_str();
  uint64_t expected_encoded_val;
  if (col_size == 8) {
    expected_encoded_val = 0x0807060504030201;
  } else if (col_size == 4) {
    expected_encoded_val = 0x08070605;
  }
  uint64_t encoded_val = 0;
  for (int i = 0; i < row_count; ++i) {
    memcpy(&encoded_val, encoded_data_ptr, col_size);
    // Check correctness of encoded value
    ASSERT_EQ(expected_encoded_val, encoded_val);
    encoded_data_ptr += col_size;
  }

  std::unique_ptr<ColBufDecoder> col_buf_decoder(
      new FixedLengthColBufDecoder(col_size, kColNoCompression, false, true));
  encoded_data_ptr = encoded_data.c_str();
  encoded_data_ptr += col_buf_decoder->Init(encoded_data_ptr);
  char* decoded_data = new char[100];
  char* decoded_data_base = decoded_data;
  for (int i = 0; i < row_count; ++i) {
    encoded_data_ptr +=
        col_buf_decoder->Decode(encoded_data_ptr, &decoded_data);
  }

  // Check correctness of decoded string length
  ASSERT_EQ(row_count * col_size, decoded_data - decoded_data_base);
  decoded_data = decoded_data_base;
  for (int i = 0; i < row_count; ++i) {
    uint64_t decoded_val;
    decoded_val = 0;
    memcpy(&decoded_val, decoded_data, col_size);
    // Check correctness of decoded value
    ASSERT_EQ(val, decoded_val);
    decoded_data += col_size;
  }
  delete[] decoded_data_base;
}

TEST_P(ColumnAwareEncodingTestWithSize, RleEncodeDecode) {
  size_t col_size = GetParam();
  std::unique_ptr<ColBufEncoder> col_buf_encoder(
      new FixedLengthColBufEncoder(col_size, kColRle, false, true));
  std::string str_buf;
  uint64_t base_val = 0x0102030405060708;
  uint64_t val = 0;
  memcpy(&val, &base_val, col_size);
  const int row_count = 4;
  for (int i = 0; i < row_count; ++i) {
    str_buf.append(reinterpret_cast<char*>(&val), col_size);
  }
  const char* str_buf_ptr = str_buf.c_str();
  for (int i = 0; i < row_count; ++i) {
    str_buf_ptr += col_buf_encoder->Append(str_buf_ptr);
  }
  col_buf_encoder->Finish();
  const std::string& encoded_data = col_buf_encoder->GetData();
  // Check correctness of encoded string length
  ASSERT_EQ(col_size + 1, encoded_data.size());

  const char* encoded_data_ptr = encoded_data.c_str();
  uint64_t encoded_val = 0;
  memcpy(&encoded_val, encoded_data_ptr, col_size);
  uint64_t expected_encoded_val;
  if (col_size == 8) {
    expected_encoded_val = 0x0807060504030201;
  } else if (col_size == 4) {
    expected_encoded_val = 0x08070605;
  }
  // Check correctness of encoded value
  ASSERT_EQ(expected_encoded_val, encoded_val);

  std::unique_ptr<ColBufDecoder> col_buf_decoder(
      new FixedLengthColBufDecoder(col_size, kColRle, false, true));
  char* decoded_data = new char[100];
  char* decoded_data_base = decoded_data;
  encoded_data_ptr += col_buf_decoder->Init(encoded_data_ptr);
  for (int i = 0; i < row_count; ++i) {
    encoded_data_ptr +=
        col_buf_decoder->Decode(encoded_data_ptr, &decoded_data);
  }
  // Check correctness of decoded string length
  ASSERT_EQ(decoded_data - decoded_data_base, row_count * col_size);
  decoded_data = decoded_data_base;
  for (int i = 0; i < row_count; ++i) {
    uint64_t decoded_val;
    decoded_val = 0;
    memcpy(&decoded_val, decoded_data, col_size);
    // Check correctness of decoded value
    ASSERT_EQ(val, decoded_val);
    decoded_data += col_size;
  }
  delete[] decoded_data_base;
}

TEST_P(ColumnAwareEncodingTestWithSize, DeltaEncodeDecode) {
  size_t col_size = GetParam();
  int row_count = 4;
  std::unique_ptr<ColBufEncoder> col_buf_encoder(
      new FixedLengthColBufEncoder(col_size, kColDeltaVarint, false, true));
  std::string str_buf;
  uint64_t base_val1 = 0x0102030405060708;
  uint64_t base_val2 = 0x0202030405060708;
  uint64_t val1 = 0, val2 = 0;
  memcpy(&val1, &base_val1, col_size);
  memcpy(&val2, &base_val2, col_size);
  const char* str_buf_ptr;
  for (int i = 0; i < row_count / 2; ++i) {
    str_buf = std::string(reinterpret_cast<char*>(&val1), col_size);
    str_buf_ptr = str_buf.c_str();
    col_buf_encoder->Append(str_buf_ptr);

    str_buf = std::string(reinterpret_cast<char*>(&val2), col_size);
    str_buf_ptr = str_buf.c_str();
    col_buf_encoder->Append(str_buf_ptr);
  }
  col_buf_encoder->Finish();
  const std::string& encoded_data = col_buf_encoder->GetData();
  // Check encoded string length
  int varint_len = 0;
  if (col_size == 8) {
    varint_len = 9;
  } else if (col_size == 4) {
    varint_len = 5;
  }
  // Check encoded string length: first value is original one (val - 0), the
  // coming three are encoded as 1, -1, 1, so they should take 1 byte in varint.
  ASSERT_EQ(varint_len + 3 * 1, encoded_data.size());

  std::unique_ptr<ColBufDecoder> col_buf_decoder(
      new FixedLengthColBufDecoder(col_size, kColDeltaVarint, false, true));
  char* decoded_data = new char[100];
  char* decoded_data_base = decoded_data;
  const char* encoded_data_ptr = encoded_data.c_str();
  encoded_data_ptr += col_buf_decoder->Init(encoded_data_ptr);
  for (int i = 0; i < row_count; ++i) {
    encoded_data_ptr +=
        col_buf_decoder->Decode(encoded_data_ptr, &decoded_data);
  }

  // Check correctness of decoded string length
  ASSERT_EQ(row_count * col_size, decoded_data - decoded_data_base);
  decoded_data = decoded_data_base;

  // Check correctness of decoded data
  for (int i = 0; i < row_count / 2; ++i) {
    uint64_t decoded_val = 0;
    memcpy(&decoded_val, decoded_data, col_size);
    ASSERT_EQ(val1, decoded_val);
    decoded_data += col_size;
    memcpy(&decoded_val, decoded_data, col_size);
    ASSERT_EQ(val2, decoded_val);
    decoded_data += col_size;
  }
  delete[] decoded_data_base;
}

TEST_F(ColumnAwareEncodingTest, ChunkBufEncodeDecode) {
  std::unique_ptr<ColBufEncoder> col_buf_encoder(
      new VariableChunkColBufEncoder(kColDict));
  std::string buf("12345678\377\1\0\0\0\0\0\0\0\376", 18);
  col_buf_encoder->Append(buf.c_str());
  col_buf_encoder->Finish();
  const std::string& encoded_data = col_buf_encoder->GetData();
  const char* str_ptr = encoded_data.c_str();

  std::unique_ptr<ColBufDecoder> col_buf_decoder(
      new VariableChunkColBufDecoder(kColDict));
  str_ptr += col_buf_decoder->Init(str_ptr);
  char* decoded_data = new char[100];
  char* decoded_data_base = decoded_data;
  col_buf_decoder->Decode(str_ptr, &decoded_data);
  for (size_t i = 0; i < buf.size(); ++i) {
    ASSERT_EQ(buf[i], decoded_data_base[i]);
  }
  delete[] decoded_data_base;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else

#include <cstdio>

int main() {
  fprintf(stderr,
          "SKIPPED as column aware encoding experiment is not enabled in "
          "ROCKSDB_LITE\n");
}
#endif  // ROCKSDB_LITE
