// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines helper methods for Java API write methods
//

#pragma once

#include <jni.h>

#include <cstring>
#include <functional>
#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksjni/portal.h"

namespace ROCKSDB_NAMESPACE {

class KVHelperJNI {
 public:
  static const int kNotFound = -1;
  static const int kStatusError = -2;

  static bool DoWrite(JNIEnv* env, std::function<Status()> fn) {
    if (env->ExceptionCheck()) {
      return false;
    }
    auto status = fn();
    if (status.ok()) {
      return true;
    }
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
    return false;
  }

  template <typename TPinnableWrapper>
  static jint DoRead(JNIEnv* env, TPinnableWrapper& pinnable,
                     std::function<Status()> fn) {
    if (env->ExceptionCheck()) {
      return kStatusError;
    }
    auto status = fn();
    if (status.IsNotFound()) {
      return kNotFound;
    }
    if (!status.ok()) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
      // There's a Java exception, but C++ doesn't know that,
      // and it needs us to return something; it will be ignored by Java
      return kStatusError;
    }

    return pinnable.Retrieve();
  }
};

/**
 * @brief Construct a slice with the contents of a Java byte array
 *
 * The slice refers to an array into which the Java byte array's whole region is
 * copied
 */
class JByteArraySlice {
 public:
  JByteArraySlice(JNIEnv* env, const jbyteArray& jarray, const jint jarray_off,
                  const jint jarray_len)
      : array_(new jbyte[jarray_len]),
        slice_(reinterpret_cast<char*>(array_), jarray_len) {
    env->GetByteArrayRegion(jarray, jarray_off, jarray_len, array_);
    if (env->ExceptionCheck()) {
      slice_.clear();
      delete[] array_;
    }
  };

  ~JByteArraySlice() {
    slice_.clear();
    delete[] array_;
  };

  Slice& slice() { return slice_; }

 private:
  jbyte* array_;
  Slice slice_;
};

/**
 * @brief Construct a slice with the contents of a direct Java ByterBuffer
 *
 * The slice refers directly to the contents of the buffer, no copy is made.
 *
 */
class JByteBufferSlice {
 public:
  JByteBufferSlice(JNIEnv* env, const jobject& jbuffer, const jint jbuffer_off,
                   const jint jbuffer_len)
      : slice_(static_cast<char*>(env->GetDirectBufferAddress(jbuffer)) +
                   jbuffer_off,
               jbuffer_len) {
    if (env->ExceptionCheck()) {
      slice_.clear();
      return;
    }
    jlong capacity = env->GetDirectBufferCapacity(jbuffer);
    if (capacity < jbuffer_off + jbuffer_len) {
      auto message = "Direct buffer offset " + std::to_string(jbuffer_off) +
                     " + length " + std::to_string(jbuffer_len) +
                     " exceeds capacity " + std::to_string(capacity);
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, message);
      slice_.clear();
    }
  }

  ~JByteBufferSlice() { slice_.clear(); };

  Slice& slice() { return slice_; }

 private:
  Slice slice_;
 
};

/**
 * @brief Wrap a pinnable slice with a method to retrieve the contents back into
 * Java
 *
 * The Java Byte Array version sets the byte array's region from the slice
 */
class JByteArrayPinnableSlice {
 public:
  JByteArrayPinnableSlice(JNIEnv* env, const jbyteArray& jbuffer,
                          const jint jbuffer_off, const jint jbuffer_len)
      : env_(env),
        jbuffer_(jbuffer),
        jbuffer_off_(jbuffer_off),
        jbuffer_len_(jbuffer_len){};
  PinnableSlice& pinnable_slice() { return pinnable_slice_; }

  ~JByteArrayPinnableSlice() { pinnable_slice_.Reset(); };

  /**
   * @brief copy back contents of the pinnable slice into the Java ByteBuffer
   *
   * @return jint min of size of buffer and number of bytes in value for
   * requested key
   */
  jint Retrieve() {
    if (env_->ExceptionCheck()) {
      return ROCKSDB_NAMESPACE::KVHelperJNI::kStatusError;
    }
    const jint pinnable_len = static_cast<jint>(pinnable_slice_.size());
    const jint result_len = std::min(jbuffer_len_, pinnable_len);

    env_->SetByteArrayRegion(
        jbuffer_, jbuffer_off_, result_len,
        reinterpret_cast<const jbyte*>(pinnable_slice_.data()));
    pinnable_slice_.Reset();
    return result_len;
  };

 private:
  JNIEnv* env_;
  jbyteArray jbuffer_;
  jint jbuffer_off_;
  jint jbuffer_len_;
  PinnableSlice pinnable_slice_;
};

/**
 * @brief Wrap a pinnable slice with a method to retrieve the contents back into
 * Java
 *
 * The Java Byte Buffer version copies the memory of the buffer from the slice
 */
class JByteBufferPinnableSlice {
 public:
  JByteBufferPinnableSlice(JNIEnv* env, const jobject& jbuffer,
                           const jint jbuffer_off, const jint jbuffer_len)
      : env_(env),
        buffer_(static_cast<char*>(env->GetDirectBufferAddress(jbuffer)) +
                jbuffer_off),
        jbuffer_len_(jbuffer_len) {
    jlong capacity = env->GetDirectBufferCapacity(jbuffer);
    if (capacity < jbuffer_off + jbuffer_len) {
      auto message = "Direct buffer offset " + std::to_string(jbuffer_off) +
                     " + length " + std::to_string(jbuffer_len) +
                     " exceeds capacity " + std::to_string(capacity);
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, message);
    }
  };

  PinnableSlice& pinnable_slice() { return pinnable_slice_; }

  ~JByteBufferPinnableSlice() { pinnable_slice_.Reset(); };

  /**
   * @brief copy back contents of the pinnable slice into the Java ByteBuffer
   *
   * @return jint min of size of buffer and number of bytes in value for
   * requested key
   */
  jint Retrieve() {
    if (env_->ExceptionCheck()) {
      return ROCKSDB_NAMESPACE::KVHelperJNI::kStatusError;
    }
    const jint pinnable_len = static_cast<jint>(pinnable_slice_.size());
    const jint result_len = std::min(jbuffer_len_, pinnable_len);

    memcpy(buffer_, pinnable_slice_.data(), result_len);
    pinnable_slice_.Reset();
    return result_len;
  };

 private:
  JNIEnv* env_;
  char* buffer_;
  jint jbuffer_len_;
  PinnableSlice pinnable_slice_;
};

}  // namespace ROCKSDB_NAMESPACE
