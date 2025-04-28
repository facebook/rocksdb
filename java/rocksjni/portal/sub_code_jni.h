// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#pragma once

#include <jni.h>

#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.Status.SubCode
class SubCodeJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Status.SubCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Status$SubCode");
  }

  /**
   * Get the Java Method: Status.SubCode#getValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getValueMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getValue", "()b");
    assert(mid != nullptr);
    return mid;
  }

  static ROCKSDB_NAMESPACE::Status::SubCode toCppSubCode(
      const jbyte jsub_code) {
    switch (jsub_code) {
      case 0x0:
        return ROCKSDB_NAMESPACE::Status::SubCode::kNone;
      case 0x1:
        return ROCKSDB_NAMESPACE::Status::SubCode::kMutexTimeout;
      case 0x2:
        return ROCKSDB_NAMESPACE::Status::SubCode::kLockTimeout;
      case 0x3:
        return ROCKSDB_NAMESPACE::Status::SubCode::kLockLimit;
      case 0x4:
        return ROCKSDB_NAMESPACE::Status::SubCode::kNoSpace;
      case 0x5:
        return ROCKSDB_NAMESPACE::Status::SubCode::kDeadlock;
      case 0x6:
        return ROCKSDB_NAMESPACE::Status::SubCode::kStaleFile;
      case 0x7:
        return ROCKSDB_NAMESPACE::Status::SubCode::kMemoryLimit;

      case 0x7F:
      default:
        return ROCKSDB_NAMESPACE::Status::SubCode::kNone;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
