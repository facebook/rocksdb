// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::HyperClockCache.

#include <jni.h>

#include "cache/clock_cache.h"
#include "include/org_rocksdb_HyperClockCache.h"
#include "rocksjni/cplusplus_to_java_convert.h"

/*
 * Class:     org_rocksdb_HyperClockCache
 * Method:    newHyperClockCache
 * Signature: (JJIZ)J
 */
jlong Java_org_rocksdb_HyperClockCache_newHyperClockCache(
    JNIEnv*, jclass, jlong capacity, jlong estimatedEntryCharge,
    jint numShardBits, jboolean strictCapacityLimit) {
  ROCKSDB_NAMESPACE::HyperClockCacheOptions cacheOptions =
      ROCKSDB_NAMESPACE::HyperClockCacheOptions(
          capacity, estimatedEntryCharge, numShardBits, strictCapacityLimit);

  auto* cache = new std::shared_ptr<ROCKSDB_NAMESPACE::Cache>(
      cacheOptions.MakeSharedCache());
  return GET_CPLUSPLUS_POINTER(cache);
}

/*
 * Class:     org_rocksdb_HyperClockCache
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
void Java_org_rocksdb_HyperClockCache_disposeInternalJni(JNIEnv*, jclass,
                                                         jlong jhandle) {
  auto* hyper_clock_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  delete hyper_clock_cache;  // delete std::shared_ptr
}