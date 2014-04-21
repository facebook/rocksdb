// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for MemTables.

#include "include/org_rocksdb_HashSkipListMemTableConfig.h"
#include "include/org_rocksdb_HashLinkedListMemTableConfig.h"
#include "include/org_rocksdb_VectorMemTableConfig.h"
#include "include/org_rocksdb_SkipListMemTableConfig.h"
#include "rocksdb/memtablerep.h"

/*
 * Class:     org_rocksdb_HashSkipListMemTableConfig
 * Method:    newMemTableFactoryHandle
 * Signature: (JII)J
 */
jlong Java_org_rocksdb_HashSkipListMemTableConfig_newMemTableFactoryHandle(
    JNIEnv* env, jobject jobj, jlong jbucket_count,
    jint jheight, jint jbranching_factor) {
  return reinterpret_cast<jlong>(rocksdb::NewHashSkipListRepFactory(
      static_cast<size_t>(jbucket_count),
      static_cast<int32_t>(jheight),
      static_cast<int32_t>(jbranching_factor)));
}

/*
 * Class:     org_rocksdb_HashLinkedListMemTableConfig
 * Method:    newMemTableFactoryHandle
 * Signature: (J)J
 */
jlong Java_org_rocksdb_HashLinkedListMemTableConfig_newMemTableFactoryHandle(
    JNIEnv* env, jobject jobj, jlong jbucket_count) {
  return reinterpret_cast<jlong>(rocksdb::NewHashLinkListRepFactory(
       static_cast<size_t>(jbucket_count)));
}

/*
 * Class:     org_rocksdb_VectorMemTableConfig
 * Method:    newMemTableFactoryHandle
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorMemTableConfig_newMemTableFactoryHandle(
    JNIEnv* env, jobject jobj, jlong jreserved_size) {
  return reinterpret_cast<jlong>(new rocksdb::VectorRepFactory(
      static_cast<size_t>(jreserved_size)));
}

/*
 * Class:     org_rocksdb_SkipListMemTableConfig
 * Method:    newMemTableFactoryHandle0
 * Signature: ()J
 */
jlong Java_org_rocksdb_SkipListMemTableConfig_newMemTableFactoryHandle0(
    JNIEnv* env, jobject jobj) {
  return reinterpret_cast<jlong>(new rocksdb::SkipListFactory());
}
