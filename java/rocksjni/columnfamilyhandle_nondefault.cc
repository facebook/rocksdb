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

#include "api_columnfamilyhandle_nondefault.h"
#include "include/org_rocksdb_ColumnFamilyHandleNonDefault.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_ColumnFamilyHandleNonDefault
 * Method:    nativeClose
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyHandleNonDefault_nativeClose(JNIEnv*, jobject,
                                                               jlong handle) {
  std::unique_ptr<APIColumnFamilyHandle> cfhAPI(
      reinterpret_cast<APIColumnFamilyHandle*>(handle));
  // All pointers in APIColumnFamilyHandle are weak, so there is nothing to do
  // here on return, unique_ptr going out of scope will delete the handle
  cfhAPI->check("nativeClose()");
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandleNonDefault
 * Method:    isLastReference
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyHandleNonDefault_isLastReference(
    JNIEnv*, jobject, jlong handle) {
  auto* cfhAPI = reinterpret_cast<APIColumnFamilyHandle*>(handle);
  cfhAPI->check("isLastReference()");
  return !cfhAPI->cfh.lock();
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandleNonDefault
 * Method:    equalsByHandle
 * Signature: (JJ)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyHandleNonDefault_equalsByHandle(
    JNIEnv* env, jobject, jlong handle, jlong handle2) {
  auto cfh = APIColumnFamilyHandle::lock(env, handle);
  auto cfh2 = APIColumnFamilyHandle::lock(env, handle2);
  if (!cfh || !cfh2) {
    return false;
  }

  auto db = APIColumnFamilyHandle::lockDB(env, handle);
  auto db2 = APIColumnFamilyHandle::lockDB(env, handle2);
  if (!db || !db2) {
    return false;
  }

  if (cfh->GetID() != cfh2->GetID()) {
    return false;
  }

  return true;
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandleNonDefault
 * Method:    getName
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_ColumnFamilyHandleNonDefault_getName(JNIEnv* env,
                                                                 jobject,
                                                                 jlong handle) {
  auto cfh = APIColumnFamilyHandle::lock(env, handle);
  if (!cfh) {
    // exception has been raised / set up
    return nullptr;
  }

  std::string cf_name = cfh->GetName();
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, cf_name);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandleNonDefault
 * Method:    getID
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyHandleNonDefault_getID(JNIEnv* env, jobject,
                                                         jlong handle) {
  auto cfh = APIColumnFamilyHandle::lock(env, handle);
  if (!cfh) {
    return -1;
  }
  const int32_t id = cfh->GetID();
  return static_cast<jint>(id);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandleNonDefault
 * Method:    getDescriptor
 * Signature: (J)Lorg/rocksdb/ColumnFamilyDescriptor;
 */
jobject Java_org_rocksdb_ColumnFamilyHandleNonDefault_getDescriptor(
    JNIEnv* env, jobject, jlong handle) {
  auto cfh = APIColumnFamilyHandle::lock(env, handle);
  if (!cfh) {
    return nullptr;
  }
  ROCKSDB_NAMESPACE::ColumnFamilyDescriptor desc;
  ROCKSDB_NAMESPACE::Status s = cfh->GetDescriptor(&desc);
  if (s.ok()) {
    return ROCKSDB_NAMESPACE::ColumnFamilyDescriptorJni::construct(env, &desc);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}
