// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::ClockCache.

#include <jni.h>

#include "cache/clock_cache.h"
#include "include/org_rocksdb_ClockCache.h"

/*
 * Class:     org_rocksdb_ClockCache
 * Method:    newClockCache
 * Signature: (JIZ)J
 */
jlong Java_org_rocksdb_ClockCache_newClockCache(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jcapacity, jint jnum_shard_bits,
    jboolean jstrict_capacity_limit) {
  auto* sptr_clock_cache = new std::shared_ptr<ROCKSDB_NAMESPACE::Cache>(
      ROCKSDB_NAMESPACE::NewClockCache(
          static_cast<size_t>(jcapacity), static_cast<int>(jnum_shard_bits),
          static_cast<bool>(jstrict_capacity_limit)));
  return reinterpret_cast<jlong>(sptr_clock_cache);
}

/*
 * Class:     org_rocksdb_ClockCache
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ClockCache_disposeInternal(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jhandle) {
  auto* sptr_clock_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  delete sptr_clock_cache;  // delete std::shared_ptr
}
