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

namespace ROCKSDB_NAMESPACE {

/**
 * @brief Exception for write batch problems
 *
 * TODO make compatible with KVException
 *
 */
class WriteBatchJavaNativeException : public std::exception {
 public:
  static const int kStatusError =
      -2;  // there was some other error fetching the value for the key

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

  jint Code() const { return kCode_; }

 private:
  jint kCode_;
};
class WriteBatchJavaNative : public WriteBatch {
 public:
  // just copy the simplest WB constructor
  explicit WriteBatchJavaNative(size_t reserved_bytes = 0, size_t max_bytes = 0)
      : WriteBatch(reserved_bytes, max_bytes, 0, 0) {}
};

class WriteBatchJavaNativeBuffer {
 private:
  JNIEnv* env;
  jbyte* buf;
  jint buf_len;
  jint pos;

  const static int ALIGN = sizeof(int) - 1;

 public:
  WriteBatchJavaNativeBuffer(JNIEnv* _env, jbyte* _buf, jint _buf_pos,
                             jint _buf_len)
      : env(_env),
        buf(_buf),
        buf_len(_buf_len),
        pos(_buf_pos + sizeof(int64_t) + sizeof(int32_t)) {};

  /**
   * @brief the sequence is a fixed field at the start of the buffer (header)
   *
   * @return jlong
   */
  jlong sequence() {
    jlong result = *reinterpret_cast<jlong*>(buf);
    return result;
  }

  jlong next_long() {
    jlong result = *reinterpret_cast<jlong*>(buf + pos);
    pos += sizeof(jlong);
    return result;
  }

  jint next_int() {
    jint result = *reinterpret_cast<jint*>(buf + pos);
    pos += sizeof(jint);
    return result;
  }

  /**
   * @brief
   *
   * @param bytes_to_skip
   */
  void skip(const jint bytes_to_skip) {
    if (pos + bytes_to_skip > buf_len) {
      ROCKSDB_NAMESPACE::WriteBatchJavaNativeException::ThrowNew(
          env, "Skip beyond end of write batch buffer " +
                   std::to_string(pos + bytes_to_skip) + " in " +
                   std::to_string(buf_len));
    }
    pos += bytes_to_skip;
  }

  void skip_aligned(const jint bytes_to_skip) {
    return skip(align(bytes_to_skip));
  };

  bool has_next() { return pos < buf_len; };

  const ROCKSDB_NAMESPACE::Slice slice(jint slice_len) {
    jbyte* slice_ptr = ptr();
    skip_aligned(slice_len);
    return Slice(reinterpret_cast<char*>(slice_ptr), slice_len);
  }

  jbyte* ptr() { return buf + pos; };

  jint align(const jint val) { return (val + ALIGN) & ~ALIGN; }

  void copy_write_batch_from_java(ROCKSDB_NAMESPACE::WriteBatchJavaNative* wb);
};

}  // namespace ROCKSDB_NAMESPACE