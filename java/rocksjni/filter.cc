// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::FilterPolicy.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>

#include "include/org_rocksdb_BloomFilter.h"
#include "include/org_rocksdb_Filter.h"
#include "rocksdb/filter_policy.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_Filter
 * Method:    createFilterFromString
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_Filter_createFilterFromString__Ljava_lang_String_2(JNIEnv* env,
                                                                    jclass,
                                                                    jstring s) {
  return ROCKSDB_NAMESPACE::CustomizableJni::createSharedFromString<
      const ROCKSDB_NAMESPACE::FilterPolicy, ROCKSDB_NAMESPACE::FilterPolicy>(
      env, s);
}

/*
 * Class:     org_rocksdb_Filter
 * Method:    createFilterFromString
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_Filter_createFilterFromString__JLjava_lang_String_2(
    JNIEnv* env, jclass, jlong handle, jstring s) {
  return ROCKSDB_NAMESPACE::CustomizableJni::createSharedFromString<
      const ROCKSDB_NAMESPACE::FilterPolicy, ROCKSDB_NAMESPACE::FilterPolicy>(
      env, handle, s);
}

/*
 * Class:     org_rocksdb_Filter
 * Method:    getId
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_rocksdb_Filter_getId(JNIEnv* env, jobject,
                                                        jlong jhandle) {
  return ROCKSDB_NAMESPACE::CustomizableJni::getIdFromShared<
      const ROCKSDB_NAMESPACE::FilterPolicy>(env, jhandle);
}

/*
 * Class:     org_rocksdb_Filter
 * Method:    isInstanceOf
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_Filter_isInstanceOf(JNIEnv* env,
                                                                jobject,
                                                                jlong jhandle,
                                                                jstring s) {
  return ROCKSDB_NAMESPACE::CustomizableJni::isSharedInstanceOf<
      const ROCKSDB_NAMESPACE::FilterPolicy>(env, jhandle, s);
}
/*
 * Class:     org_rocksdb_BloomFilter
 * Method:    createBloomFilter
 * Signature: (DZ)J
 */
jlong Java_org_rocksdb_BloomFilter_createNewBloomFilter(JNIEnv* /*env*/,
                                                        jclass /*jcls*/,
                                                        jdouble bits_per_key) {
  auto* sptr_filter =
      new std::shared_ptr<const ROCKSDB_NAMESPACE::FilterPolicy>(
          ROCKSDB_NAMESPACE::NewBloomFilterPolicy(bits_per_key));
  return GET_CPLUSPLUS_POINTER(sptr_filter);
}

/*
 * Class:     org_rocksdb_Filter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Filter_disposeInternal(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<const ROCKSDB_NAMESPACE::FilterPolicy>*>(
          jhandle);
  delete handle;  // delete std::shared_ptr
}
