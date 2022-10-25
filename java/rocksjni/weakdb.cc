// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#include <jni.h>

#include "api_weakdb.h"
#include "include/org_rocksdb_WeakDB.h"

using API_Weak = APIWeakDB<ROCKSDB_NAMESPACE::DB>;

/*
 * @brief true iff there are other strong references to the RocksDB object
 * Class:     org_rocksdb_WeakDB
 * Method:    isOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WeakDB_isDatabaseOpen(JNIEnv *, jobject,
                                                jlong handle) {
  std::unique_ptr<API_Weak> weakDBAPI(reinterpret_cast<API_Weak *>(handle));
  auto lock = weakDBAPI->db.lock();
  bool result = !!lock;
  weakDBAPI.release();
  return result;
}

/*
 * Class:     org_rocksdb_WeakDB
 * Method:    nativeClose
 * Signature: (J)V
 */
void Java_org_rocksdb_WeakDB_nativeClose(JNIEnv *, jobject, jlong handle) {
  std::unique_ptr<API_Weak> weakDBAPI(reinterpret_cast<API_Weak *>(handle));
}

/*
 * Class:     org_rocksdb_WeakDB
 * Method:    getReferenceCounts
 * Signature: (J)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_rocksdb_WeakDB_getReferenceCounts(
    JNIEnv *env, jobject, jlong jhandle) {
  return APIBase::getReferenceCounts<API_Weak>(env, jhandle);
}

/*
 * @brief true iff there are other no strong references to the RocksDB object
 * Class:     org_rocksdb_WeakDB
 * Method:    isLastReference
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WeakDB_isLastReference(JNIEnv *, jobject,
                                                 jlong handle) {
  std::unique_ptr<API_Weak> weakDBAPI(reinterpret_cast<API_Weak *>(handle));
  auto lock = weakDBAPI->db.lock();
  bool result = !lock;
  weakDBAPI.release();
  return result;
}