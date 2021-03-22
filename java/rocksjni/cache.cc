// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::Cache.

#include "rocksdb/cache.h"

#include <jni.h>

#include "include/org_rocksdb_Cache.h"

/*
 * Class:     org_rocksdb_Cache
 * Method:    getUsage
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Cache_getUsage(JNIEnv*, jclass, jlong jhandle) {
  auto* sptr_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  return static_cast<jlong>(sptr_cache->get()->GetUsage());
}

/*
 * Class:     org_rocksdb_Cache
 * Method:    getPinnedUsage
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Cache_getPinnedUsage(JNIEnv*, jclass, jlong jhandle) {
  auto* sptr_cache =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache>*>(jhandle);
  return static_cast<jlong>(sptr_cache->get()->GetPinnedUsage());
}
