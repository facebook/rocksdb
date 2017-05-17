// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "utilities/col_buf_encoder.h"
#include <cstring>
#include <string>
#include "port/port.h"

namespace rocksdb {

ColBufEncoder::~ColBufEncoder() {}

namespace {

inline uint64_t DecodeFixed64WithEndian(uint64_t val, bool big_endian,
                                        size_t size) {
  if (big_endian && port::kLittleEndian) {
    val = EndianTransform(val, size);
  } else if (!big_endian && !port::kLittleEndian) {
    val = EndianTransform(val, size);
  }
  return val;
}

}  // namespace

const std::string &ColBufEncoder::GetData() { return buffer_; }

ColBufEncoder *ColBufEncoder::NewColBufEncoder(
    const ColDeclaration &col_declaration) {
  if (col_declaration.col_type == "FixedLength") {
    return new FixedLengthColBufEncoder(
        col_declaration.size, col_declaration.col_compression_type,
        col_declaration.nullable, col_declaration.big_endian);
  } else if (col_declaration.col_type == "VariableLength") {
    return new VariableLengthColBufEncoder();
  } else if (col_declaration.col_type == "VariableChunk") {
    return new VariableChunkColBufEncoder(col_declaration.col_compression_type);
  } else if (col_declaration.col_type == "LongFixedLength") {
    return new LongFixedLengthColBufEncoder(col_declaration.size,
                                            col_declaration.nullable);
  }
  // Unrecognized column type
  return nullptr;
}

#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("shift")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif
size_t FixedLengthColBufEncoder::Append(const char *buf) {
  if (nullable_) {
    if (buf == nullptr) {
      buffer_.append(1, 0);
      return 0;
    } else {
      buffer_.append(1, 1);
    }
  }
  uint64_t read_val = 0;
  memcpy(&read_val, buf, size_);
  read_val = DecodeFixed64WithEndian(read_val, big_endian_, size_);

  // Determine write value
  uint64_t write_val = read_val;
  if (col_compression_type_ == kColDeltaVarint ||
      col_compression_type_ == kColRleDeltaVarint) {
    int64_t delta = read_val - last_val_;
    // Encode signed delta value
    delta = (delta << 1) ^ (delta >> 63);
    write_val = delta;
    last_val_ = read_val;
  } else if (col_compression_type_ == kColDict ||
             col_compression_type_ == kColRleDict) {
    auto iter = dictionary_.find(read_val);
    uint64_t dict_val;
    if (iter == dictionary_.end()) {
      // Add new entry to dictionary
      dict_val = dictionary_.size();
      dictionary_.insert(std::make_pair(read_val, dict_val));
      dict_vec_.push_back(read_val);
    } else {
      dict_val = iter->second;
    }
    write_val = dict_val;
  }

  // Write into buffer
  if (IsRunLength(col_compression_type_)) {
    if (run_length_ == -1) {
      // First element
      run_val_ = write_val;
      run_length_ = 1;
    } else if (write_val != run_val_) {
      // End of run
      // Write run value
      if (col_compression_type_ == kColRle) {
        buffer_.append(reinterpret_cast<char *>(&run_val_), size_);
      } else {
        PutVarint64(&buffer_, run_val_);
      }
      // Write run length
      PutVarint64(&buffer_, run_length_);
      run_val_ = write_val;
      run_length_ = 1;
    } else {
      run_length_++;
    }
  } else {  // non run-length encodings
    if (col_compression_type_ == kColNoCompression) {
      buffer_.append(reinterpret_cast<char *>(&write_val), size_);
    } else {
      PutVarint64(&buffer_, write_val);
    }
  }
  return size_;
}

void FixedLengthColBufEncoder::Finish() {
  if (col_compression_type_ == kColDict ||
      col_compression_type_ == kColRleDict) {
    std::string header;
    PutVarint64(&header, dict_vec_.size());
    // Put dictionary in the header
    for (auto item : dict_vec_) {
      PutVarint64(&header, item);
    }
    buffer_ = header + buffer_;
  }
  if (IsRunLength(col_compression_type_)) {
    // Finish last run value
    if (col_compression_type_ == kColRle) {
      buffer_.append(reinterpret_cast<char *>(&run_val_), size_);
    } else {
      PutVarint64(&buffer_, run_val_);
    }
    PutVarint64(&buffer_, run_length_);
  }
}

size_t LongFixedLengthColBufEncoder::Append(const char *buf) {
  if (nullable_) {
    if (buf == nullptr) {
      buffer_.append(1, 0);
      return 0;
    } else {
      buffer_.append(1, 1);
    }
  }
  buffer_.append(buf, size_);
  return size_;
}

void LongFixedLengthColBufEncoder::Finish() {}

size_t VariableLengthColBufEncoder::Append(const char *buf) {
  uint8_t length = 0;
  length = *buf;
  buffer_.append(buf, 1);
  buf += 1;
  buffer_.append(buf, length);
  return length + 1;
}

void VariableLengthColBufEncoder::Finish() {}

size_t VariableChunkColBufEncoder::Append(const char *buf) {
  const char *orig_buf = buf;
  uint8_t mark = 0xFF;
  size_t length = 0;
  std::string tmp_buffer;
  while (mark == 0xFF) {
    uint64_t val;
    memcpy(&val, buf, 8);
    buf += 8;
    mark = *buf;
    buf += 1;
    int8_t chunk_size = 8 - (0xFF - mark);
    if (col_compression_type_ == kColDict) {
      auto iter = dictionary_.find(val);
      uint64_t dict_val;
      if (iter == dictionary_.end()) {
        dict_val = dictionary_.size();
        dictionary_.insert(std::make_pair(val, dict_val));
        dict_vec_.push_back(val);
      } else {
        dict_val = iter->second;
      }
      PutVarint64(&tmp_buffer, dict_val);
    } else {
      tmp_buffer.append(reinterpret_cast<char *>(&val), chunk_size);
    }
    length += chunk_size;
  }

  PutVarint64(&buffer_, length);
  buffer_.append(tmp_buffer);
  return buf - orig_buf;
}

void VariableChunkColBufEncoder::Finish() {
  if (col_compression_type_ == kColDict) {
    std::string header;
    PutVarint64(&header, dict_vec_.size());
    for (auto item : dict_vec_) {
      PutVarint64(&header, item);
    }
    buffer_ = header + buffer_;
  }
}

}  // namespace rocksdb
