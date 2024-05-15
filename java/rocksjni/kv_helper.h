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
#include <exception>
#include <functional>
#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksjni/portal.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief Exception class used to make the flow of key/value (Put(), Get(),
 * Merge(), ...) calls clearer.
 *
 * This class is used by Java API JNI methods in try { save/fetch } catch { ...
 * } style.
 *
 */
class KVException : public std::exception {
 public:
  // These values are expected on Java API calls to represent the result of a
  // Get() which has failed; a negative length is returned to indicate an error.
  static const int kNotFound = -1;  // the key was not found in RocksDB
  static const int kStatusError =
      -2;  // there was some other error fetching the value for the key

  /**
   * @brief Throw a KVException (and potentially a Java exception) if the
   * RocksDB status is "bad"
   *
   * @param env JNI environment needed to create a Java exception
   * @param status RocksDB status to examine
   */
  static void ThrowOnError(JNIEnv* env, const Status& status) {
    if (status.ok()) {
      return;
    }
    if (status.IsNotFound()) {
      // IsNotFound does not generate a Java Exception, any other bad status
      // does..
      throw KVException(kNotFound);
    }
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
    throw KVException(kStatusError);
  }

  /**
   * @brief Throw a KVException and a Java exception
   *
   * @param env JNI environment needed to create a Java exception
   * @param message content of the exception we will throw
   */
  static void ThrowNew(JNIEnv* env, const std::string& message) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, message);
    throw KVException(kStatusError);
  }

  /**
   * @brief Throw a KVException if there is already a Java exception in the JNI
   * enviroment
   *
   * @param env
   */
  static void ThrowOnError(JNIEnv* env) {
    if (env->ExceptionCheck()) {
      throw KVException(kStatusError);
    }
  }

  KVException(jint code) : kCode_(code){};

  virtual const char* what() const throw() {
    return "Exception raised by JNI. There may be a Java exception in the "
           "JNIEnv. Please check!";
  }

  jint Code() const { return kCode_; }

 private:
  jint kCode_;
};

/**
 * @brief Construct a slice with the contents of a Java byte array
 *
 * The slice refers to an array into which the Java byte array's whole region is
 * copied
 */
class JByteArraySlice {
 public:
  JByteArraySlice(JNIEnv* env, const jbyteArray& jarr, const jint jarr_off,
                  const jint jarr_len)
      : arr_(new jbyte[jarr_len]),
        slice_(reinterpret_cast<char*>(arr_), jarr_len) {
    env->GetByteArrayRegion(jarr, jarr_off, jarr_len, arr_);
    KVException::ThrowOnError(env);
  };

  ~JByteArraySlice() {
    slice_.clear();
    delete[] arr_;
  };

  Slice& slice() { return slice_; }

 private:
  jbyte* arr_;
  Slice slice_;
};

/**
 * @brief Construct a slice with the contents of a direct Java ByterBuffer
 *
 * The slice refers directly to the contents of the buffer, no copy is made.
 *
 */
class JDirectBufferSlice {
 public:
  JDirectBufferSlice(JNIEnv* env, const jobject& jbuffer,
                     const jint jbuffer_off, const jint jbuffer_len)
      : slice_(static_cast<char*>(env->GetDirectBufferAddress(jbuffer)) +
                   jbuffer_off,
               jbuffer_len) {
    KVException::ThrowOnError(env);
    jlong capacity = env->GetDirectBufferCapacity(jbuffer);
    if (capacity < jbuffer_off + jbuffer_len) {
      auto message = "Direct buffer offset " + std::to_string(jbuffer_off) +
                     " + length " + std::to_string(jbuffer_len) +
                     " exceeds capacity " + std::to_string(capacity);
      KVException::ThrowNew(env, message);
      slice_.clear();
    }
  }

  ~JDirectBufferSlice() { slice_.clear(); };

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
  /**
   * @brief Construct a new JByteArrayPinnableSlice object referring to an
   * existing  java byte buffer
   *
   * @param env
   * @param jbuffer
   * @param jbuffer_off
   * @param jbuffer_len
   */
  JByteArrayPinnableSlice(JNIEnv* env, const jbyteArray& jbuffer,
                          const jint jbuffer_off, const jint jbuffer_len)
      : env_(env),
        jbuffer_(jbuffer),
        jbuffer_off_(jbuffer_off),
        jbuffer_len_(jbuffer_len){};

  /**
   * @brief Construct an empty new JByteArrayPinnableSlice object
   *
   */
  JByteArrayPinnableSlice(JNIEnv* env) : env_(env){};

  PinnableSlice& pinnable_slice() { return pinnable_slice_; }

  ~JByteArrayPinnableSlice() { pinnable_slice_.Reset(); };

  /**
   * @brief copy back contents of the pinnable slice into the Java ByteBuffer
   *
   * @return jint min of size of buffer and number of bytes in value for
   * requested key
   */
  jint Fetch() {
    const jint pinnable_len = static_cast<jint>(pinnable_slice_.size());
    const jint result_len = std::min(jbuffer_len_, pinnable_len);
    env_->SetByteArrayRegion(
        jbuffer_, jbuffer_off_, result_len,
        reinterpret_cast<const jbyte*>(pinnable_slice_.data()));
    KVException::ThrowOnError(
        env_);  // exception thrown: ArrayIndexOutOfBoundsException

    return pinnable_len;
  };

  /**
   * @brief create a new Java buffer and copy the result into it
   *
   * @return jbyteArray the java buffer holding the result
   */
  jbyteArray NewByteArray() {
    const jint pinnable_len = static_cast<jint>(pinnable_slice_.size());
    jbyteArray jbuffer =
        ROCKSDB_NAMESPACE::JniUtil::createJavaByteArrayWithSizeCheck(
            env_, pinnable_slice_.data(), pinnable_len);
    KVException::ThrowOnError(env_);  // OutOfMemoryError

    return jbuffer;
  }

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
 * The Java Direct Buffer version copies the memory of the buffer from the slice
 */
class JDirectBufferPinnableSlice {
 public:
  JDirectBufferPinnableSlice(JNIEnv* env, const jobject& jbuffer,
                             const jint jbuffer_off, const jint jbuffer_len)
      : buffer_(static_cast<char*>(env->GetDirectBufferAddress(jbuffer)) +
                jbuffer_off),
        jbuffer_len_(jbuffer_len) {
    jlong capacity = env->GetDirectBufferCapacity(jbuffer);
    if (capacity < jbuffer_off + jbuffer_len) {
      auto message =
          "Invalid value argument. Capacity is less than requested region. "
          "offset " +
          std::to_string(jbuffer_off) + " + length " +
          std::to_string(jbuffer_len) + " exceeds capacity " +
          std::to_string(capacity);
      KVException::ThrowNew(env, message);
    }
  }

  PinnableSlice& pinnable_slice() { return pinnable_slice_; }

  ~JDirectBufferPinnableSlice() { pinnable_slice_.Reset(); };

  /**
   * @brief copy back contents of the pinnable slice into the Java DirectBuffer
   *
   * @return jint min of size of buffer and number of bytes in value for
   * requested key
   */
  jint Fetch() {
    const jint pinnable_len = static_cast<jint>(pinnable_slice_.size());
    const jint result_len = std::min(jbuffer_len_, pinnable_len);

    memcpy(buffer_, pinnable_slice_.data(), result_len);
    return pinnable_len;
  };

 private:
  char* buffer_;
  jint jbuffer_len_;
  PinnableSlice pinnable_slice_;
};

}  // namespace ROCKSDB_NAMESPACE
