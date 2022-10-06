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

using API_CF = APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>;

/*
 * Class:     org_rocksdb_ColumnFamilyHandleNonDefault
 * Method:    nativeClose
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyHandle_nativeClose(JNIEnv*, jobject,
                                                     jlong handle) {
  std::unique_ptr<API_CF> cfhAPI(reinterpret_cast<API_CF*>(handle));
  // All pointers in APIColumnFamilyHandle are weak, so there is nothing to do
  // here on return, unique_ptr going out of scope will delete the handle
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    isDefaultColumnFamily
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyHandle_isDefaultColumnFamily(
    JNIEnv* env, jobject, jlong handle) {
  const auto& cfhAPI = *reinterpret_cast<API_CF*>(handle);
  const auto& rocksDB = cfhAPI.dbLock(env);
  if (!rocksDB) {
    // dbLock exception
    return false;
  }
  const auto& cfh = cfhAPI.cfhLock(env);
  if (!cfh) {
    // cfhLock exception
    return false;
  }

  return (rocksDB->DefaultColumnFamily() == cfh.get());
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getReferenceCounts
 * Signature: (J)[J
 */
jlongArray Java_org_rocksdb_ColumnFamilyHandle_getReferenceCounts(
    JNIEnv* env, jobject, jlong jhandle) {
  return APIBase::getReferenceCounts<API_CF>(env, jhandle);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandleNonDefault
 * Method:    equalsByHandle
 * Signature: (JJ)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyHandle_equalsByHandle(JNIEnv* env,
                                                            jobject,
                                                            jlong handle,
                                                            jlong handle2) {
  auto cfh = API_CF::lock(env, handle);
  auto cfh2 = API_CF::lock(env, handle2);
  if (!cfh || !cfh2) {
    return false;
  }

  auto db = API_CF::lockDB(env, handle);
  auto db2 = API_CF::lockDB(env, handle2);
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
jbyteArray Java_org_rocksdb_ColumnFamilyHandle_getName(JNIEnv* env, jobject,
                                                       jlong handle) {
  auto cfh = API_CF::lock(env, handle);
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
jint Java_org_rocksdb_ColumnFamilyHandle_getID(JNIEnv* env, jobject,
                                               jlong handle) {
  auto cfh = API_CF::lock(env, handle);
  if (!cfh) {
    return -1;
  }
  const int32_t id = cfh->GetID();
  return static_cast<jint>(id);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getDescriptor
 * Signature: (J)Lorg/rocksdb/ColumnFamilyDescriptor;
 */
jobject Java_org_rocksdb_ColumnFamilyHandle_getDescriptor(JNIEnv* env, jobject,
                                                          jlong handle) {
  auto cfh = API_CF::lock(env, handle);
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
