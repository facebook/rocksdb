// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <jni.h>

#include <exception>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

/**
 * This exception should be used in situation when critical
 * JNI call failed. For example GetJvm, FindClass, ...
 */
class JniException : public std::exception {
 private:
  JniException() {}

 public:
  static void ThrowNew(JNIEnv* env, const std::string& message) {
    if (!env->ExceptionCheck()) {  // Throw Java exception only when there is no
                                   // exception.
      ROCKSDB_NAMESPACE::JniException::ThrowNew(env, message);
    }
    throw JniException();
  };

  static void ThrowIfTrue(JNIEnv* env, const std::string& message,
                          const bool condition) {
    if (condition) {
      if (!env->ExceptionCheck()) {  // Throw Java exception only when there is
                                     // no exception.
        ROCKSDB_NAMESPACE::JniException::ThrowNew(env, message);
      }
      throw JniException();
    }
  };

  /**
   * Check if there is Java exception, and then throw C++ exception.
   */
  static void checkAndThrowException(JNIEnv* env) {
    if (env->ExceptionCheck()) {
      throw JniException();
    }
  };

  virtual const char* what() const noexcept {
    return "Exception raised by JNI. There may be a Java exception in the "
           "JNIEnv. Please check!";
  };
};
}  // namespace ROCKSDB_NAMESPACE
