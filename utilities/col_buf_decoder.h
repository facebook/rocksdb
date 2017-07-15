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
#include "utilities/col_buf_encoder.h"

namespace rocksdb {

struct ColDeclaration;

// ColBufDecoder is a class to decode column buffers. It can be populated from a
// ColDeclaration. Before starting decoding, a Init() method should be called.
// Each time it takes a column value into Decode() method.
class ColBufDecoder {
 public:
  virtual ~ColBufDecoder() = 0;
  virtual size_t Init(const char* src) { return 0; }
  virtual size_t Decode(const char* src, char** dest) = 0;
  static ColBufDecoder* NewColBufDecoder(const ColDeclaration& col_declaration);

 protected:
  std::string buffer_;
  static inline bool IsRunLength(ColCompressionType type) {
    return type == kColRle || type == kColRleVarint ||
           type == kColRleDeltaVarint || type == kColRleDict;
  }
};

class FixedLengthColBufDecoder : public ColBufDecoder {
 public:
  explicit FixedLengthColBufDecoder(
      size_t size, ColCompressionType col_compression_type = kColNoCompression,
      bool nullable = false, bool big_endian = false)
      : size_(size),
        col_compression_type_(col_compression_type),
        nullable_(nullable),
        big_endian_(big_endian) {}

  size_t Init(const char* src) override;
  size_t Decode(const char* src, char** dest) override;
  ~FixedLengthColBufDecoder() {}

 private:
  size_t size_;
  ColCompressionType col_compression_type_;
  bool nullable_;
  bool big_endian_;

  // for decoding
  std::vector<uint64_t> dict_vec_;
  uint64_t remain_runs_;
  uint64_t run_val_;
  uint64_t last_val_;
};

class LongFixedLengthColBufDecoder : public ColBufDecoder {
 public:
  LongFixedLengthColBufDecoder(size_t size, bool nullable)
      : size_(size), nullable_(nullable) {}

  size_t Decode(const char* src, char** dest) override;
  ~LongFixedLengthColBufDecoder() {}

 private:
  size_t size_;
  bool nullable_;
};

class VariableLengthColBufDecoder : public ColBufDecoder {
 public:
  size_t Decode(const char* src, char** dest) override;
  ~VariableLengthColBufDecoder() {}
};

class VariableChunkColBufDecoder : public VariableLengthColBufDecoder {
 public:
  size_t Init(const char* src) override;
  size_t Decode(const char* src, char** dest) override;
  explicit VariableChunkColBufDecoder(ColCompressionType col_compression_type)
      : col_compression_type_(col_compression_type) {}
  VariableChunkColBufDecoder() : col_compression_type_(kColNoCompression) {}

 private:
  ColCompressionType col_compression_type_;
  std::unordered_map<uint64_t, uint64_t> dictionary_;
  std::vector<uint64_t> dict_vec_;
};

struct KVPairColBufDecoders {
  std::vector<std::unique_ptr<ColBufDecoder>> key_col_bufs;
  std::vector<std::unique_ptr<ColBufDecoder>> value_col_bufs;
  std::unique_ptr<ColBufDecoder> value_checksum_buf;

  explicit KVPairColBufDecoders(const KVPairColDeclarations& kvp_cd) {
    for (auto kcd : *kvp_cd.key_col_declarations) {
      key_col_bufs.emplace_back(
          std::move(ColBufDecoder::NewColBufDecoder(kcd)));
    }
    for (auto vcd : *kvp_cd.value_col_declarations) {
      value_col_bufs.emplace_back(
          std::move(ColBufDecoder::NewColBufDecoder(vcd)));
    }
    value_checksum_buf.reset(
        ColBufDecoder::NewColBufDecoder(*kvp_cd.value_checksum_declaration));
  }
};
}  // namespace rocksdb
