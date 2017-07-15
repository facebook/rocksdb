// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "util/coding.h"

namespace rocksdb {

enum ColCompressionType {
  kColNoCompression,
  kColRle,
  kColVarint,
  kColRleVarint,
  kColDeltaVarint,
  kColRleDeltaVarint,
  kColDict,
  kColRleDict
};

struct ColDeclaration;

// ColBufEncoder is a class to encode column buffers. It can be populated from a
// ColDeclaration. Each time it takes a column value into Append() method to
// encode the column and store it into an internal buffer. After all rows for
// this column are consumed, a Finish() should be called to add header and
// remaining data.
class ColBufEncoder {
 public:
  // Read a column, encode data and append into internal buffer.
  virtual size_t Append(const char *buf) = 0;
  virtual ~ColBufEncoder() = 0;
  // Get the internal column buffer. Should only be called after Finish().
  const std::string &GetData();
  // Finish encoding. Add header and remaining data.
  virtual void Finish() = 0;
  // Populate a ColBufEncoder from ColDeclaration.
  static ColBufEncoder *NewColBufEncoder(const ColDeclaration &col_declaration);

 protected:
  std::string buffer_;
  static inline bool IsRunLength(ColCompressionType type) {
    return type == kColRle || type == kColRleVarint ||
           type == kColRleDeltaVarint || type == kColRleDict;
  }
};

// Encoder for fixed length column buffer. In fixed length column buffer, the
// size of the column should not exceed 8 bytes.
// The following encodings are supported:
// Varint: Variable length integer. See util/coding.h for more details
// Rle (Run length encoding): encode a sequence of contiguous value as
// [run_value][run_length]. Can be combined with Varint
// Delta: Encode value to its delta with its adjacent entry. Use varint to
// possibly reduce stored bytes. Can be combined with Rle.
// Dictionary: Use a dictionary to record all possible values in the block and
// encode them with an ID started from 0. IDs are encoded as varint. A column
// with dictionary encoding will have a header to store all actual values,
// ordered by their dictionary value, and the data will be replaced by
// dictionary value. Can be combined with Rle.
class FixedLengthColBufEncoder : public ColBufEncoder {
 public:
  explicit FixedLengthColBufEncoder(
      size_t size, ColCompressionType col_compression_type = kColNoCompression,
      bool nullable = false, bool big_endian = false)
      : size_(size),
        col_compression_type_(col_compression_type),
        nullable_(nullable),
        big_endian_(big_endian),
        last_val_(0),
        run_length_(-1),
        run_val_(0) {}

  size_t Append(const char *buf) override;
  void Finish() override;
  ~FixedLengthColBufEncoder() {}

 private:
  size_t size_;
  ColCompressionType col_compression_type_;
  // If set as true, the input value can be null (represented as nullptr). When
  // nullable is true, use one more byte before actual value to indicate if the
  // current value is null.
  bool nullable_;
  // If set as true, input value will be treated as big endian encoded.
  bool big_endian_;

  // for encoding
  uint64_t last_val_;
  int16_t run_length_;
  uint64_t run_val_;
  // Map to store dictionary for dictionary encoding
  std::unordered_map<uint64_t, uint64_t> dictionary_;
  // Vector of dictionary keys.
  std::vector<uint64_t> dict_vec_;
};

// Long fixed length column buffer is a variant of fixed length buffer to hold
// fixed length buffer with more than 8 bytes. We do not support any special
// encoding schemes in LongFixedLengthColBufEncoder.
class LongFixedLengthColBufEncoder : public ColBufEncoder {
 public:
  LongFixedLengthColBufEncoder(size_t size, bool nullable)
      : size_(size), nullable_(nullable) {}
  size_t Append(const char *buf) override;
  void Finish() override;

  ~LongFixedLengthColBufEncoder() {}

 private:
  size_t size_;
  bool nullable_;
};

// Variable length column buffer holds a format of variable length column. In
// this format, a column is composed of one byte length k, followed by data with
// k bytes long data.
class VariableLengthColBufEncoder : public ColBufEncoder {
 public:
  size_t Append(const char *buf) override;
  void Finish() override;

  ~VariableLengthColBufEncoder() {}
};

// Variable chunk column buffer holds another format of variable length column.
// In this format, a column contains multiple chunks of data, each of which is
// composed of 8 bytes long data, and one byte as a mask to indicate whether we
// have more data to come. If no more data coming, the mask is set as 0xFF. If
// the chunk is the last chunk and has only k valid bytes, the mask is set as
// 0xFF - (8 - k).
class VariableChunkColBufEncoder : public VariableLengthColBufEncoder {
 public:
  size_t Append(const char *buf) override;
  void Finish() override;
  explicit VariableChunkColBufEncoder(ColCompressionType col_compression_type)
      : col_compression_type_(col_compression_type) {}
  VariableChunkColBufEncoder() : col_compression_type_(kColNoCompression) {}

 private:
  ColCompressionType col_compression_type_;
  // Map to store dictionary for dictionary encoding
  std::unordered_map<uint64_t, uint64_t> dictionary_;
  // Vector of dictionary keys.
  std::vector<uint64_t> dict_vec_;
};

// ColDeclaration declares a column's type, algorithm of column-aware encoding,
// and other column data like endian and nullability.
struct ColDeclaration {
  explicit ColDeclaration(
      std::string _col_type,
      ColCompressionType _col_compression_type = kColNoCompression,
      size_t _size = 0, bool _nullable = false, bool _big_endian = false)
      : col_type(_col_type),
        col_compression_type(_col_compression_type),
        size(_size),
        nullable(_nullable),
        big_endian(_big_endian) {}
  std::string col_type;
  ColCompressionType col_compression_type;
  size_t size;
  bool nullable;
  bool big_endian;
};

// KVPairColDeclarations is a class to hold column declaration of columns in
// key and value.
struct KVPairColDeclarations {
  std::vector<ColDeclaration> *key_col_declarations;
  std::vector<ColDeclaration> *value_col_declarations;
  ColDeclaration *value_checksum_declaration;
  KVPairColDeclarations(std::vector<ColDeclaration> *_key_col_declarations,
                        std::vector<ColDeclaration> *_value_col_declarations,
                        ColDeclaration *_value_checksum_declaration)
      : key_col_declarations(_key_col_declarations),
        value_col_declarations(_value_col_declarations),
        value_checksum_declaration(_value_checksum_declaration) {}
};

// Similar to KVPairDeclarations, KVPairColBufEncoders is used to hold column
// buffer encoders of all columns in key and value.
struct KVPairColBufEncoders {
  std::vector<std::unique_ptr<ColBufEncoder>> key_col_bufs;
  std::vector<std::unique_ptr<ColBufEncoder>> value_col_bufs;
  std::unique_ptr<ColBufEncoder> value_checksum_buf;

  explicit KVPairColBufEncoders(const KVPairColDeclarations &kvp_cd) {
    for (auto kcd : *kvp_cd.key_col_declarations) {
      key_col_bufs.emplace_back(
          std::move(ColBufEncoder::NewColBufEncoder(kcd)));
    }
    for (auto vcd : *kvp_cd.value_col_declarations) {
      value_col_bufs.emplace_back(
          std::move(ColBufEncoder::NewColBufEncoder(vcd)));
    }
    value_checksum_buf.reset(
        ColBufEncoder::NewColBufEncoder(*kvp_cd.value_checksum_declaration));
  }

  // Helper function to call Finish()
  void Finish() {
    for (auto &col_buf : key_col_bufs) {
      col_buf->Finish();
    }
    for (auto &col_buf : value_col_bufs) {
      col_buf->Finish();
    }
    value_checksum_buf->Finish();
  }
};
}  // namespace rocksdb
