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
// The portal class for org.rocksdb.AbstractComparatorJniBridge
class AbstractComparatorJniBridge : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractComparatorJniBridge
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/AbstractComparatorJniBridge");
  }

  /**
   * Get the Java Method: Comparator#compareInternal
   *
   * @param env A pointer to the Java environment
   * @param jclazz the AbstractComparatorJniBridge class
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getCompareInternalMethodId(JNIEnv* env, jclass jclazz) {
    static jmethodID mid =
        env->GetStaticMethodID(jclazz, "compareInternal",
                               "(Lorg/rocksdb/AbstractComparator;Ljava/nio/"
                               "ByteBuffer;ILjava/nio/ByteBuffer;I)I");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortestSeparatorInternal
   *
   * @param env A pointer to the Java environment
   * @param jclazz the AbstractComparatorJniBridge class
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getFindShortestSeparatorInternalMethodId(JNIEnv* env,
                                                            jclass jclazz) {
    static jmethodID mid =
        env->GetStaticMethodID(jclazz, "findShortestSeparatorInternal",
                               "(Lorg/rocksdb/AbstractComparator;Ljava/nio/"
                               "ByteBuffer;ILjava/nio/ByteBuffer;I)I");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortSuccessorInternal
   *
   * @param env A pointer to the Java environment
   * @param jclazz the AbstractComparatorJniBridge class
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getFindShortSuccessorInternalMethodId(JNIEnv* env,
                                                         jclass jclazz) {
    static jmethodID mid = env->GetStaticMethodID(
        jclazz, "findShortSuccessorInternal",
        "(Lorg/rocksdb/AbstractComparator;Ljava/nio/ByteBuffer;I)I");
    assert(mid != nullptr);
    return mid;
  }
};
}  // namespace ROCKSDB_NAMESPACE
