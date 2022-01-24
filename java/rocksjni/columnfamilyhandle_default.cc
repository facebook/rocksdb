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

#include "api_columnfamilyhandle_default.h"
#include "include/org_rocksdb_ColumnFamilyHandleDefault.h"

/*
 * Class:     org_rocksdb_ColumnFamilyHandleDefault
 * Method:    equalsByHandle
 * Signature: (JJ)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyHandleDefault_equalsByHandle(
    JNIEnv *env, jobject, jlong handle, jlong handle2) {
  auto cfhAPI = reinterpret_cast<APIColumnFamilyHandleDefault *>(handle);
  auto db = APIWeakDB::lockDB(env, handle);
  if (!db) {
    return false;
  }
  auto cfhAPI2 = reinterpret_cast<APIColumnFamilyHandleDefault *>(handle2);
  auto db2 = APIWeakDB::lockDB(env, handle);
  if (!db2) {
    return false;
  }
  if (cfhAPI->default_unowned != cfhAPI2->default_unowned) {
    return false;
  }
  if (db != db2) {
    return false;
  }

  return true;
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandleDefault
 * Method:    getName
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_ColumnFamilyHandleDefault_getName(JNIEnv *env,
                                                              jobject,
                                                              jlong handle) {
  auto db = APIWeakDB::lockDB(env, handle);
  if (!db) {
    return nullptr;
  }
  auto cfh = APIColumnFamilyHandleDefault::getCFH(env, handle);
  if (cfh == nullptr) {
    return nullptr;
  }

  std::string cf_name = cfh->GetName();
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, cf_name);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandleDefault
 * Method:    getID
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyHandleDefault_getID(JNIEnv *env, jobject,
                                                      jlong handle) {
  auto db = APIWeakDB::lockDB(env, handle);
  if (!db) {
    return -1;
  }
  auto cfh = APIColumnFamilyHandleDefault::getCFH(env, handle);
  if (cfh == nullptr) {
    return -1;
  }

  const int32_t id = cfh->GetID();
  return static_cast<jint>(id);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandleDefault
 * Method:    getDescriptor
 * Signature: (J)Lorg/rocksdb/ColumnFamilyDescriptor;
 */
jobject Java_org_rocksdb_ColumnFamilyHandleDefault_getDescriptor(JNIEnv *env,
                                                                 jobject,
                                                                 jlong handle) {
  auto db = APIWeakDB::lockDB(env, handle);
  if (!db) {
    return nullptr;
  }
  auto cfh = APIColumnFamilyHandleDefault::getCFH(env, handle);
  if (cfh == nullptr) {
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

/*
 * Class:     org_rocksdb_ColumnFamilyHandleDefault
 * Method:    nativeClose
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyHandleDefault_nativeClose(JNIEnv *, jobject,
                                                            jlong handle) {
  std::unique_ptr<APIColumnFamilyHandleDefault> cfhAPI(
      reinterpret_cast<APIColumnFamilyHandleDefault *>(handle));
  // All pointers in APIColumnFamilyHandle are weak, so there is nothing to do
  // here on return, unique_ptr going out of scope will delete the handle
  cfhAPI->check("nativeClose()");
}
