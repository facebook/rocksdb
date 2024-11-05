//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::WalFilter.

#pragma once

#include <jni.h>

#include <map>
#include <memory>
#include <string>

#include "rocksdb/write_batch.h"
#include "rocksjni/portal.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief Exception for write batch problems
 *
 * TODO make compatible with KVException
 *
 */
class WriteBatchJavaNativeException : public std::exception {
 public:
  static const int kStatusError = -1;  // generic error value

  /**
   * @brief Throw a KVException and a Java exception
   *
   * @param env JNI environment needed to create a Java exception
   * @param message content of the exception we will throw
   */
  static void ThrowNew(JNIEnv* env, const std::string& message) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, message);
    throw WriteBatchJavaNativeException(kStatusError);
  }

  static void ThrowNew(JNIEnv* env, const Status& status) {
    if (status.ok()) {
      return;
    }
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
    throw WriteBatchJavaNativeException(kStatusError);
  }

  WriteBatchJavaNativeException(jint code) : kCode_(code) {};
  WriteBatchJavaNativeException(const Status& status) : kCode_(status.code()) {};
  WriteBatchJavaNativeException(jint code, const std::string& message)
      : kCode_(code), message_(message) {};
  WriteBatchJavaNativeException(const std::string& message)
      : kCode_(kStatusError), message_(message) {};

  jint Code() const { return kCode_; }

  const std::string& Message() const { return message_; }

 private:
  jint kCode_;
  const std::string message_;
};
class WriteBatchJavaNative : public WriteBatch {
 public:
  // just copy the simplest WB constructor
  explicit WriteBatchJavaNative(size_t reserved_bytes = 0, size_t max_bytes = 0)
      : WriteBatch(reserved_bytes, max_bytes, 0, 0) {}

  WriteBatchJavaNative& Append(const Slice& slice);
};

class WriteBatchJavaNativeBuffer {
 private:
  Slice slice_;
  uint32_t pos;

 public:
  WriteBatchJavaNativeBuffer(const Slice& slice)
      : slice_(slice.data(), slice.size()),
        pos(sizeof(int64_t) + sizeof(int32_t)) {};

  /**
   * @brief the sequence value is a fixed field at the start of the buffer (header)
   *
   * @return jlong
   */
  jlong sequence() {
    jlong result = *reinterpret_cast<const jlong*>(slice_.data());
    return result;
  }

  jlong next_long() {
    jlong result = *reinterpret_cast<const jlong*>(slice_.data() + pos);
    pos += sizeof(jlong);
    return result;
  }

  jint next_int() {
    jint result = *reinterpret_cast<const jint*>(slice_.data() + pos);
    pos += sizeof(jint);
    return result;
  }

  jbyte next_byte() {
    jbyte result = *reinterpret_cast<const jbyte*>(slice_.data() + pos);
    pos += sizeof(jbyte);
    return result;
  }

  uint32_t next_varint32() {
    uint32_t result;
    const char *beyond = GetVarint32Ptr(slice_.data() + pos, slice_.data() + slice_.size(), &result);
    if (beyond == nullptr) {
      std::string msg;
      msg.append("next_varint32() extends beyond end of write batch buffer ");
      msg.append(" at ");
      msg.append(std::to_string(pos));
      msg.append(" in buffer of size ");
      msg.append(std::to_string(slice_.size()));
      throw WriteBatchJavaNativeException(msg);
    }
    pos = static_cast<uint32_t>(beyond - slice_.data());
    return result;
  }

  uint32_t current_pos() {
    return pos;
  }

  /**
   * @brief
   *
   * @param bytes_to_skip
   */
  void skip(const uint32_t bytes_to_skip) {
    if (pos + bytes_to_skip > slice_.size()) {
      throw WriteBatchJavaNativeException(
          "Skip beyond end of write batch buffer " +
          std::to_string(pos + bytes_to_skip) + " in " +
          std::to_string(slice_.size()));
    }
    pos += bytes_to_skip;
  }

  bool has_next() { return pos < slice_.size(); };

  const ROCKSDB_NAMESPACE::Slice slice() {
    uint32_t slice_len = next_varint32();
    const jbyte* slice_ptr = ptr();
    skip(slice_len);
    return Slice(reinterpret_cast<const char*>(slice_ptr), slice_len);
  }

  std::string slice_to_string(const ROCKSDB_NAMESPACE::Slice s) {
    return std::string("slice: ")
        .append(std::to_string(reinterpret_cast<uint64_t>(s.data())))
        .append("[")
        .append(std::to_string(s.size()))
        .append("]");
  }

  const jbyte* ptr() { return reinterpret_cast<const jbyte*>(slice_.data()) + pos; };

  void copy_write_batch_from_java(ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb);
};

}  // namespace ROCKSDB_NAMESPACE