// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include "api_columnfamilyhandle.h"
#include "include/org_rocksdb_ColumnFamilyHandle.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getName
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_ColumnFamilyHandle_getName(JNIEnv* env,
                                                       jobject /*jobj*/,
                                                       jlong jhandle) {
  auto* cfh = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jhandle);
  std::string cf_name = cfh->GetName();
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, cf_name);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getID
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyHandle_getID(JNIEnv* /*env*/,
                                               jobject /*jobj*/,
                                               jlong jhandle) {
  auto* cfh = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jhandle);
  const int32_t id = cfh->GetID();
  return static_cast<jint>(id);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getDescriptor
 * Signature: (J)Lorg/rocksdb/ColumnFamilyDescriptor;
 */
jobject Java_org_rocksdb_ColumnFamilyHandle_getDescriptor(JNIEnv* env,
                                                          jobject /*jobj*/,
                                                          jlong jhandle) {
  auto* cfh = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jhandle);
  ROCKSDB_NAMESPACE::ColumnFamilyDescriptor desc;
  ROCKSDB_NAMESPACE::Status s = cfh->GetDescriptor(&desc);
  if (s.ok()) {
    return ROCKSDB_NAMESPACE::ColumnFamilyDescriptorJni::construct(env, &desc);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyHandle_nativeClose(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong jhandle) {
  std::unique_ptr<APIColumnFamilyHandle> cfhAPI(
      reinterpret_cast<APIColumnFamilyHandle*>(jhandle));
  // All pointers in APIColumnFamilyHandle are weak, so there is nothing to do
  // here This may turn out to be the standard pattern.
  cfhAPI->check();
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    isLastReference
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyHandle_isLastReference(JNIEnv*, jobject,
                                                             jlong jhandle) {
  std::unique_ptr<APIColumnFamilyHandle> cfhAPI(
      reinterpret_cast<APIColumnFamilyHandle*>(jhandle));
  cfhAPI->check();
  const bool result = !cfhAPI->cfh.lock();
  cfhAPI.release();
  return result;
}
