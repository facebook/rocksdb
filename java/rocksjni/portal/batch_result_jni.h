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
#include "rocksjni/portal/common.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.TransactionLogIterator.BatchResult
class BatchResultJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionLogIterator.BatchResult
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(
        env, "org/rocksdb/TransactionLogIterator$BatchResult");
  }

  /**
   * Create a new Java org.rocksdb.TransactionLogIterator.BatchResult object
   * with the same properties as the provided C++ ROCKSDB_NAMESPACE::BatchResult
   * object
   *
   * @param env A pointer to the Java environment
   * @param batch_result The ROCKSDB_NAMESPACE::BatchResult object
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionLogIterator.BatchResult object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env,
                           ROCKSDB_NAMESPACE::BatchResult& batch_result) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(JJ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jbatch_result = env->NewObject(jclazz, mid, batch_result.sequence,
                                           batch_result.writeBatchPtr.get());
    if (jbatch_result == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      return nullptr;
    }

    batch_result.writeBatchPtr.release();
    return jbatch_result;
  }
};
}  // namespace ROCKSDB_NAMESPACE
