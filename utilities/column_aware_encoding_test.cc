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

TEST_F(ColumnAwareEncodingTest, NoCompressionEncodeDecode) {
  std::unique_ptr<ColBufEncoder> col_buf_encoder(
      new FixedLengthColBufEncoder(8, kColNoCompression, false, true));
  std::string str_buf;
  uint64_t val = 0x0102030405060708;
  for (int i = 0; i < 4; ++i) {
    str_buf.append(reinterpret_cast<char*>(&val), 8);
  }
  const char* str_buf_ptr = str_buf.c_str();
  for (int i = 0; i < 4; ++i) {
    col_buf_encoder->Append(str_buf_ptr);
  }
  col_buf_encoder->Finish();
  const std::string& encoded_data = col_buf_encoder->GetData();
  ASSERT_EQ(encoded_data.size(), 32);

  const char* encoded_data_ptr = encoded_data.c_str();
  uint64_t encoded_val;
  for (int i = 0; i < 4; ++i) {
    memcpy(&encoded_val, encoded_data_ptr, 8);
    ASSERT_EQ(encoded_val, 0x0807060504030201);
    encoded_data_ptr += 8;
  }

  std::unique_ptr<ColBufDecoder> col_buf_decoder(
      new FixedLengthColBufDecoder(8, kColNoCompression, false, true));
  encoded_data_ptr = encoded_data.c_str();
  encoded_data_ptr += col_buf_decoder->Init(encoded_data_ptr);
  char* decoded_data = new char[100];
  char* decoded_data_base = decoded_data;
  for (int i = 0; i < 4; ++i) {
    encoded_data_ptr +=
        col_buf_decoder->Decode(encoded_data_ptr, &decoded_data);
  }

  ASSERT_EQ(4 * 8, decoded_data - decoded_data_base);
  decoded_data = decoded_data_base;
  for (int i = 0; i < 4; ++i) {
    uint64_t decoded_val;
    decoded_val = 0;
    memcpy(&decoded_val, decoded_data, 8);
    ASSERT_EQ(decoded_val, val);
    decoded_data += 8;
  }
  delete[] decoded_data_base;
}

TEST_F(ColumnAwareEncodingTest, RleEncodeDecode) {
  std::unique_ptr<ColBufEncoder> col_buf_encoder(
      new FixedLengthColBufEncoder(8, kColRle, false, true));
  std::string str_buf;
  uint64_t val = 0x0102030405060708;
  for (int i = 0; i < 4; ++i) {
    str_buf.append(reinterpret_cast<char*>(&val), 8);
  }
  const char* str_buf_ptr = str_buf.c_str();
  for (int i = 0; i < 4; ++i) {
    str_buf_ptr += col_buf_encoder->Append(str_buf_ptr);
  }
  col_buf_encoder->Finish();
  const std::string& encoded_data = col_buf_encoder->GetData();
  ASSERT_EQ(encoded_data.size(), 9);

  const char* encoded_data_ptr = encoded_data.c_str();
  uint64_t encoded_val;
  memcpy(&encoded_val, encoded_data_ptr, 8);
  ASSERT_EQ(encoded_val, 0x0807060504030201);

  std::unique_ptr<ColBufDecoder> col_buf_decoder(
      new FixedLengthColBufDecoder(8, kColRle, false, true));
  char* decoded_data = new char[100];
  char* decoded_data_base = decoded_data;
  encoded_data_ptr += col_buf_decoder->Init(encoded_data_ptr);
  for (int i = 0; i < 4; ++i) {
    encoded_data_ptr +=
        col_buf_decoder->Decode(encoded_data_ptr, &decoded_data);
  }
  ASSERT_EQ(4 * 8, decoded_data - decoded_data_base);
  decoded_data = decoded_data_base;
  for (int i = 0; i < 4; ++i) {
    uint64_t decoded_val;
    decoded_val = 0;
    memcpy(&decoded_val, decoded_data, 8);
    ASSERT_EQ(decoded_val, val);
    decoded_data += 8;
  }
  delete[] decoded_data_base;
}

TEST_F(ColumnAwareEncodingTest, DeltaEncodeDecode) {
  std::unique_ptr<ColBufEncoder> col_buf_encoder(
      new FixedLengthColBufEncoder(8, kColDeltaVarint, false, true));
  std::string str_buf;
  uint64_t val = 0x0102030405060708;
  uint64_t val2 = 0x0202030405060708;
  const char* str_buf_ptr;
  for (int i = 0; i < 2; ++i) {
    str_buf = std::string(reinterpret_cast<char*>(&val), 8);
    str_buf_ptr = str_buf.c_str();
    col_buf_encoder->Append(str_buf_ptr);

    str_buf = std::string(reinterpret_cast<char*>(&val2), 8);
    str_buf_ptr = str_buf.c_str();
    col_buf_encoder->Append(str_buf_ptr);
  }
  col_buf_encoder->Finish();
  const std::string& encoded_data = col_buf_encoder->GetData();
  ASSERT_EQ(encoded_data.size(), 9 + 3);

  std::unique_ptr<ColBufDecoder> col_buf_decoder(
      new FixedLengthColBufDecoder(8, kColDeltaVarint, false, true));
  char* decoded_data = new char[100];
  char* decoded_data_base = decoded_data;
  const char* encoded_data_ptr = encoded_data.c_str();
  encoded_data_ptr += col_buf_decoder->Init(encoded_data_ptr);
  for (int i = 0; i < 4; ++i) {
    encoded_data_ptr +=
        col_buf_decoder->Decode(encoded_data_ptr, &decoded_data);
  }
  ASSERT_EQ(4 * 8, decoded_data - decoded_data_base);
  decoded_data = decoded_data_base;
  for (int i = 0; i < 2; ++i) {
    uint64_t decoded_val;
    memcpy(&decoded_val, decoded_data, 8);
    ASSERT_EQ(decoded_val, val);
    decoded_data += 8;
    memcpy(&decoded_val, decoded_data, 8);
    ASSERT_EQ(decoded_val, val2);
    decoded_data += 8;
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
