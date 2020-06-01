// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for SstFileMetaData

#include <jni.h>

#include "include/org_rocksdb_SstFileMetaData.h"
#include "rocksdb/metadata.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    newSstFileMetaData
 * Signature: ()J
 */
jlong Java_org_rocksdb_SstFileMetaData_newSstFileMetaData(JNIEnv*, jclass) {
  auto* metadata = new ROCKSDB_NAMESPACE::SstFileMetaData();
  return reinterpret_cast<jlong>(metadata);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_SstFileMetaData_disposeInternal(JNIEnv*, jobject,
                                                      jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  delete metadata;
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    fileName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_SstFileMetaData_fileName(JNIEnv* env, jobject,
                                                  jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &metadata->name, false);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    path
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_SstFileMetaData_path(JNIEnv* env, jobject,
                                              jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &metadata->db_path,
                                                  false);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    size
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_size(JNIEnv*, jobject, jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->size);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    smallestSeqno
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_smallestSeqno(JNIEnv*, jobject,
                                                     jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->smallest_seqno);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    largestSeqno
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_largestSeqno(JNIEnv*, jobject,
                                                    jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->largest_seqno);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    smallestKey
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_SstFileMetaData_smallestKey(JNIEnv* env, jobject,
                                                        jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, metadata->smallestkey);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    largestKey
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_SstFileMetaData_largestKey(JNIEnv* env, jobject,
                                                       jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, metadata->largestkey);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    numReadsSampled
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_numReadsSampled(JNIEnv*, jobject,
                                                       jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->num_reads_sampled);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    beingCompacted
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_SstFileMetaData_beingCompacted(JNIEnv*, jobject,
                                                         jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jboolean>(metadata->being_compacted);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    numEntries
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_numEntries(JNIEnv*, jobject,
                                                  jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->num_entries);
}

/*
 * Class:     org_rocksdb_SstFileMetaData
 * Method:    numDeletions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileMetaData_numDeletions(JNIEnv*, jobject,
                                                    jlong jhandle) {
  auto* metadata =
      reinterpret_cast<ROCKSDB_NAMESPACE::SstFileMetaData*>(jhandle);
  return static_cast<jlong>(metadata->num_deletions);
}
